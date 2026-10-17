"""Extrinsic-marginalized likelihood as a function of intrinsic parameters.

``IntrinsicMarginalObjective(P)`` returns ln L marginalized over time, distance,
orbital phase and polarization (the JAX ILE fused kernel), and averaged over sky
position and inclination with a FIXED importance-sampled batch.  The batch is held
fixed between calls (common random numbers), so the objective is a deterministic,
smooth function of the intrinsic parameters, suitable for a hillclimber.  It is
not an evidence estimate: the sky/inclination average uses a few thousand points.

The waveform is whatever ``factored_likelihood.PrecomputeLikelihoodTerms`` builds
for ``P.approx`` (lalsimulation, CPU).  The marginalization runs on the JAX
backend (GPU if present).  ``JAXLikelihoodData`` is not a pytree, so the stock
wrappers close over one data object and recompile for each intrinsic point.  Here
the per-point arrays (Q, U, V) are jit ARGUMENTS substituted into a fixed
template, so the kernel compiles once per (modes, buffer length) shape.
"""
import copy
import time

import numpy as np

import lal
import lalsimulation as lalsim

import jax
import jax.numpy as jnp
from jax.scipy.special import logsumexp

from RIFT import lalsimutils
from . import core
from .wrapper import build_data_from_precompute

_PC = lal.PC_SI


def load_frame_data(cache_file, channel_names, psd_files, data_start_time,
                    data_end_time, srate, window_shape=0.0, psd_window_shape=0.0,
                    fmin_ifo=None):
    """Data and PSDs, read as in ``integrate_likelihood_extrinsic_jax.load_frames``.

    channel_names, psd_files: lists of 'IFO=value'.  fmin_ifo: dict IFO -> Hz.
    """
    fmin_ifo = fmin_ifo or {}
    deltaT = 1.0 / srate
    data_dict, psd_dict = {}, {}
    for inst, chan in (c.split("=") for c in channel_names):
        data_dict[inst] = lalsimutils.frame_data_to_non_herm_hoff(
            cache_file, inst + ":" + chan, start=data_start_time,
            stop=data_end_time, window_shape=window_shape, deltaT=deltaT)
    for inst, psdf in (c.split("=") for c in psd_files):
        psd = lalsimutils.get_psd_series_from_xmldoc(psdf, inst)
        psd = lalsimutils.resample_psd_series(psd, data_dict[inst].deltaF)
        if psd_window_shape > 0 or window_shape > 0:
            psd.data.data *= (
                lalsimutils.psd_windowing_factor(window_shape, len(data_dict[inst].data.data))
                / lalsimutils.psd_windowing_factor(psd_window_shape, len(psd.data.data)))
        if inst in fmin_ifo:
            freqs = psd.f0 + psd.deltaF * np.arange(psd.data.length)
            psd.data.data[freqs < fmin_ifo[inst]] = 0
        psd_dict[inst] = psd
    return data_dict, psd_dict


# ---------------------------------------------------------------------------
# Sky / inclination batch: prior and a reflected-kernel importance proposal
# ---------------------------------------------------------------------------
def _prior_draw(rng, n):
    ra = rng.uniform(0, 2 * np.pi, n)
    dec = np.arcsin(rng.uniform(-1, 1, n))
    incl = np.arccos(rng.uniform(-1, 1, n))
    return ra, dec, incl


def _to_unit(ra, dec, incl):
    """(ra, sin dec, cos incl): the prior is uniform on [0,2pi) x [-1,1]^2."""
    return np.stack([ra, np.sin(dec), np.cos(incl)], axis=-1)


class SkyBatch:
    """Fixed batch of (ra, dec, incl) with log importance weights ln(p/q).

    q = f_prior * prior + (1 - f_prior) * GMM, in u = (ra, sin dec, cos incl),
    where the prior is uniform.  The mixture (component count by BIC) is fitted to
    weight-resampled pilot points, so the separate sky modes of a two-detector
    network (and the incl <-> pi - incl mirror) each get their own component; a
    single kernel bandwidth would span them.  Draws wrap in ra and reflect at
    +-1; the density sums the 27 periodic/mirror images, so the weights are exact.
    """

    _LOG_PRIOR = -np.log(8 * np.pi)   # 1/(2pi * 2 * 2)

    def __init__(self, n, rng):
        self.n = int(n)
        self.rng = rng
        self.gmm = None          # (weights, means, covs)
        self.f_prior = 1.0
        self.ra, self.dec, self.incl = _prior_draw(rng, self.n)
        self.log_w = np.zeros(self.n)

    def _log_gmm(self, u):
        wts, mus, covs = self.gmm
        shifts = [np.array([a, 0, 0]) for a in (0.0, 2 * np.pi, -2 * np.pi)]
        out = []
        for wk, mu, cov in zip(wts, mus, covs):
            L = np.linalg.cholesky(cov)
            logdet = 2 * np.sum(np.log(np.diag(L)))
            terms = []
            for sh in shifts:
                for m1 in (0, 1, 2):
                    for m2 in (0, 1, 2):
                        v = u + sh
                        if m1: v = v.copy(); v[:, 1] = (2 if m1 == 1 else -2) - v[:, 1]
                        if m2: v = v.copy(); v[:, 2] = (2 if m2 == 1 else -2) - v[:, 2]
                        z = np.linalg.solve(L, (v - mu).T)
                        terms.append(-0.5 * np.sum(z ** 2, axis=0))
            out.append(np.log(wk) - 0.5 * logdet - 1.5 * np.log(2 * np.pi)
                       + logsumexp(np.array(terms), axis=0))
        return logsumexp(np.array(out), axis=0)

    def log_q(self, u):
        lp = self._LOG_PRIOR * np.ones(len(u))
        if self.gmm is None:
            return lp
        return np.logaddexp(np.log(self.f_prior) + lp,
                            np.log1p(-self.f_prior) + self._log_gmm(u))

    def adapt(self, ra, dec, incl, log_weight, n_resample=3000, max_components=4,
              f_prior=0.1, cov_inflate=1.5, **kw):
        """Refit q from pilot samples with log weights ln(L p / q_pilot)."""
        from sklearn.mixture import GaussianMixture
        u = _to_unit(ra, dec, incl)
        lw = np.asarray(log_weight) - np.max(log_weight)
        w = np.exp(lw); w /= w.sum()
        neff = 1.0 / np.sum(w ** 2)
        x = u[self.rng.choice(len(w), size=n_resample, p=w)]
        # cut the ra circle at its widest empty gap so no mode straddles 0/2pi
        r = np.sort(np.mod(x[:, 0], 2 * np.pi))
        gaps = np.diff(np.concatenate([r, [r[0] + 2 * np.pi]]))
        cut = r[np.argmax(gaps)] + 0.5 * np.max(gaps)
        x[:, 0] = np.mod(x[:, 0] - cut, 2 * np.pi) + cut
        jitter = self.rng.normal(scale=[0.01, 0.005, 0.01], size=x.shape)   # break exact duplicates
        x = x + jitter
        best, best_bic = None, np.inf
        for k in range(1, max_components + 1):
            g = GaussianMixture(k, covariance_type="full", reg_covar=1e-4,
                                random_state=int(self.rng.integers(1 << 31))).fit(x)
            b = g.bic(x)
            if b < best_bic:
                best, best_bic = g, b
        covs = best.covariances_ * cov_inflate ** 2
        self.gmm = (best.weights_, best.means_, covs)
        self.f_prior = float(f_prior)
        self.redraw()
        return dict(neff_pilot=float(neff), n_components=int(best.n_components))

    def redraw(self):
        n_prior = self.rng.binomial(self.n, self.f_prior) if self.gmm is not None else self.n
        u = _to_unit(*_prior_draw(self.rng, n_prior))
        if self.gmm is not None and n_prior < self.n:
            wts, mus, covs = self.gmm
            m = self.n - n_prior
            comp = self.rng.choice(len(wts), size=m, p=wts / wts.sum())
            u_k = np.empty((m, 3))
            for c in range(len(wts)):
                sel = comp == c
                if np.any(sel):
                    u_k[sel] = self.rng.multivariate_normal(mus[c], covs[c], size=int(sel.sum()))
            u_k[:, 0] = np.mod(u_k[:, 0], 2 * np.pi)
            for k in (1, 2):
                y = np.mod(u_k[:, k] + 1, 4)                      # reflect into [-1, 1]
                u_k[:, k] = np.where(y > 2, 4 - y, y) - 1
            u = np.concatenate([u, u_k])
        u[:, 0] = np.mod(u[:, 0], 2 * np.pi)
        self.ra = u[:, 0]
        self.dec = np.arcsin(np.clip(u[:, 1], -1, 1))
        self.incl = np.arccos(np.clip(u[:, 2], -1, 1))
        self.log_w = self._LOG_PRIOR - self.log_q(u)


# ---------------------------------------------------------------------------
# The objective
# ---------------------------------------------------------------------------
class IntrinsicMarginalObjective:
    """ln L(intrinsic), marginalized over time/distance/phase/psi, sky-averaged.

    Parameters
    ----------
    data_dict, psd_dict : as for the production precompute.
    P_base : ChooseWaveformParams holding approximant, fmin, fref, deltaT, deltaF.
    reduce : 'marginal' (log-mean over the sky batch, with importance weights) or
        'profile' (max over the batch).
    """

    def __init__(self, data_dict, psd_dict, P_base, event_time, d_min, d_max,
                 l_max=2, fmax=None, n_sky=1024, nphi=16, npsi=8, n_dist=128,
                 d_prior="euclidean", storage_window_half=0.15,
                 integration_window_half=0.075, interp=None, reduce="marginal",
                 waveform_kwargs=None, seed=0, verbose=False, analyticPSD_Q=False):
        self.data_dict, self.psd_dict = data_dict, psd_dict
        self.P_base = P_base.copy()
        self.event_time = float(event_time)
        self.l_max, self.fmax = int(l_max), fmax
        self.swh, self.iwh = float(storage_window_half), float(integration_window_half)
        self.interp = interp or core.JAX_INTERP_DEFAULT
        self.reduce = reduce
        self.waveform_kwargs = dict(waveform_kwargs or {})
        self.verbose = verbose
        self.analyticPSD_Q = analyticPSD_Q
        self.x_grid, self.log_w_grid = core.make_distance_grid(
            d_min, d_max, n_grid=n_dist, d_prior=d_prior)
        self.phi_grid = core.phi_ref_grid(nphi)
        self.psi_grid = core.psi_grid(npsi)
        self.rng = np.random.default_rng(seed)
        self.sky = SkyBatch(n_sky, self.rng)
        self._template = None
        self._key = None
        self._kernel = None
        self.n_calls = 0
        self.n_compiles = 0
        self.n_nonfinite = 0
        self.t_precompute = 0.0
        self.t_kernel = 0.0

    # -- per-point precompute -------------------------------------------------
    def _prepare(self, P):
        P = P.copy()
        for attr in ("approx", "fmin", "fref", "deltaT", "deltaF"):
            setattr(P, attr, getattr(self.P_base, attr))
        P.fmax = 0.0
        P.radec = True
        P.tref = self.event_time
        P.dist = core.DIST_MPC_REF * 1e6 * _PC
        P.phiref = P.psi = P.incl = 0.0
        return P

    def precompute(self, P):
        t0 = time.time()
        data, extras = build_data_from_precompute(
            self._prepare(P), self.data_dict, self.psd_dict, self.event_time,
            self.swh, self.iwh, self.l_max, self.fmax, verbose=False,
            analyticPSD_Q=self.analyticPSD_Q,
            **self.waveform_kwargs)
        self.t_precompute += time.time() - t0
        return data, extras

    def _shape_key(self, data):
        return tuple((det, tuple(map(tuple, data.detectors[det]["lms"])),
                      int(data.detectors[det]["npts_full"]),
                      float(data.detectors[det]["epoch"]))
                     for det in data.detector_names)

    def _build_kernel(self, data):
        template = data
        dets = list(data.detector_names)
        xg, lwg, pg, sg = self.x_grid, self.log_w_grid, self.phi_grid, self.psi_grid
        interp = self.interp

        def _kernel(arrays, ra, dec, incl):
            d2 = copy.copy(template)
            d2.detectors = {det: dict(template.detectors[det], Q=arrays[det][0],
                                      U=arrays[det][1], V=arrays[det][2])
                            for det in dets}
            return core.fused_log_likelihood_distphipsimarg(
                d2, ra, dec, incl, xg, lwg, pg, sg, interp=interp)
        self._kernel = jax.jit(_kernel)
        self._template = template
        self.n_compiles += 1

    def lnL_sky(self, P, ra=None, dec=None, incl=None, data=None):
        """Per-sky-point lnL (marginalized over t, d, phi, psi) and the data object."""
        if data is None:
            data, _ = self.precompute(P)
        key = self._shape_key(data)
        if key != self._key:
            if self._key is not None and self.verbose:
                print(" [intrinsic_objective] data shape changed; recompiling")
            self._build_kernel(data)
            self._key = key
        arrays = {det: (data.detectors[det]["Q"], data.detectors[det]["U"],
                        data.detectors[det]["V"]) for det in data.detector_names}
        if ra is None:
            ra, dec, incl = self.sky.ra, self.sky.dec, self.sky.incl
        t0 = time.time()
        out = np.asarray(self._kernel(arrays, jnp.asarray(ra), jnp.asarray(dec),
                                      jnp.asarray(incl)))
        self.t_kernel += time.time() - t0
        return out, data

    def __call__(self, P):
        self.n_calls += 1
        try:
            lnL, _ = self.lnL_sky(P)
        except Exception as exc:   # waveform generation failure, out-of-domain point
            if self.verbose:
                print(" [intrinsic_objective] evaluation failed:", exc)
            return -np.inf
        bad = ~np.isfinite(lnL)
        if np.any(bad):
            # intermittent non-finite rows seen on CPU (open issue, see
            # DESIGN_transverse_seed_climb.md); score them as zero likelihood
            self.n_nonfinite += int(bad.sum())
            lnL = np.where(bad, -np.inf, lnL)
        if self.reduce == "profile":
            return float(np.max(lnL))
        return float(logsumexp(lnL + self.sky.log_w) - np.log(len(lnL)))

    def adapt_sky(self, P, n_pilot_batches=4, n_rounds=6, neff_target=None,
                  n_prior_batches=16, **kw):
        """Refit the sky/inclination proposal at P.

        Round 0 pilots from the prior (n_prior_batches batches); later rounds
        pilot from the current proposal, with weights ln L + ln(p/q).  Stops early once the batch n_eff
        reaches neff_target (default: 5% of the batch).
        """
        data, _ = self.precompute(P)
        neff_target = neff_target or 0.05 * self.sky.n
        info = {}
        for rnd in range(n_rounds):
            ras, decs, incls, lws = [], [], [], []
            for _ in range(n_prior_batches if rnd == 0 else n_pilot_batches):
                if rnd == 0:
                    ra, dec, incl = _prior_draw(self.rng, self.sky.n)
                    lw0 = np.zeros(self.sky.n)
                else:
                    self.sky.redraw()
                    ra, dec, incl, lw0 = self.sky.ra, self.sky.dec, self.sky.incl, self.sky.log_w
                lnL, _ = self.lnL_sky(P, ra, dec, incl, data=data)
                ras.append(ra); decs.append(dec); incls.append(incl); lws.append(lnL + lw0)
            ra, dec, incl, lw = map(np.concatenate, (ras, decs, incls, lws))
            info = self.sky.adapt(ra, dec, incl, lw, **kw)
            lnL_new, _ = self.lnL_sky(P, data=data)
            w = lnL_new + self.sky.log_w
            w = np.exp(w - w.max())
            info.update(round=rnd, neff_batch=float(w.sum() ** 2 / np.sum(w ** 2)),
                        lnL_max=float(np.max(lnL_new)))
            if self.verbose:
                print(" [intrinsic_objective] sky adapt", info)
            if info["neff_batch"] >= neff_target:
                break
        return info

    def stats(self):
        return dict(n_calls=self.n_calls, n_compiles=self.n_compiles, n_nonfinite=self.n_nonfinite,
                    t_precompute=self.t_precompute, t_kernel=self.t_kernel)
