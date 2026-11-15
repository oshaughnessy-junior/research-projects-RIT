# mcsamplerVaraha2 -- "AV2"
#
# Algorithm: Varaha v2, Tiwari arXiv:2405.16568 (code: github.com/vaibhavtewari/Varaha, v2/ at 234c820),
# with three RIFT additions.  See DESIGN_varaha2.md beside this file for the algorithm, the
# measurements behind each default, and the failure modes.
#
#   adaptation (Varaha v2): every draw is kept; its density is the deterministic mixture of all
#       cycles' bin grids; the likelihood threshold rises by a fixed SAMPLING-volume fraction per
#       cycle, gated on the Kish size of the live set's inverse densities.
#   stall guard: part of each cycle's draws goes to bins in proportion to their posterior weight,
#       and every bin holding non-negligible weight gets at least one draw.  Without it the
#       sampler stalls on elongated high-D targets (ESS ~1, lnZ off by nats).
#   refresh: lnZ and the exported samples come from FRESH draws of the frozen final mixture plus a
#       defensive full-box component -- fixed-proposal importance sampling.  It removes the adaptive
#       estimator's bias, but a mode the adaptation never found is reached only through the defensive
#       draws, so in practice it is missed and lnZ reads low with healthy diagnostics.  The pooled
#       adaptive estimate is reported only as a diagnostic.
#   verdict: a pass whose refresh n_eff, Kish ESS or Pareto k-hat fails is reported as collapsed
#       (dict_return['live_volume_collapsed']), which the ILE driver already acts on.
#
# Default OFF: reached only through --sampler-method AV2 or a portfolio member named AV2.

import sys
import time

import numpy
np = numpy
from scipy.special import logsumexp

from RIFT.integrators.rvs_record import RvsRecord, SamplerOutputMixin
from RIFT.integrators.statutils import pareto_khat_from_log, ess_from_log_weights
import RIFT.integrators.mcsamplerAdaptiveVolume as _av

cupy_ok = _av.cupy_ok
identity_convert = _av.identity_convert
identity_convert_togpu = _av.identity_convert_togpu
xpy_default = _av.xpy_default

__author__ = "R. O'Shaughnessy, V. Tiwari"


class Varaha2Stall(Exception):
    pass


class _Grid(object):
    """One cycle's proposal: occupied bins of a regular grid on the unit cube, with draw counts.
    Bins are matched by a random 64-bit hash of the integer bin index (collision odds ~N^2/2^64)."""

    def __init__(self, nbins, bins, counts, coef):
        self.nbins = np.asarray(nbins, dtype=np.int64)
        self.coef = coef
        self.bins = np.asarray(bins, dtype=np.int64)
        self.counts = np.asarray(counts, dtype=np.int64)
        h = self._hash(self.bins)
        order = np.argsort(h)
        self.keys = h[order]
        self.log_n = np.log(self.counts[order].astype(float))
        self.log_vol = -np.sum(np.log(self.nbins.astype(float)))

    def _hash(self, idx):
        return (idx.astype(np.int64) * self.coef).sum(axis=1)   # wraps mod 2^64 by design

    def log_density(self, U):
        """ln(expected count per unit u-volume) of this grid at U; -inf off the grid."""
        idx = np.minimum((U * self.nbins).astype(np.int64), self.nbins - 1)
        h = self._hash(idx)
        pos = np.minimum(np.searchsorted(self.keys, h), len(self.keys) - 1)
        hit = self.keys[pos] == h
        out = np.full(len(U), -np.inf)
        out[hit] = self.log_n[pos[hit]] - self.log_vol
        return out

    def draw(self, n=None):
        """All counts (n None), or n points from this grid's normalized density."""
        if n is None:
            lo = np.repeat(self.bins, self.counts, axis=0)
        else:
            j = np.random.choice(len(self.counts), size=int(n), p=self.counts / self.counts.sum())
            lo = self.bins[j]
        return (lo + np.random.uniform(size=lo.shape)) / self.nbins


def _weighted_threshold(g, logw, frac):
    idx = np.argsort(g)[::-1]
    w = np.exp(logw[idx] - np.max(logw))
    cw = np.cumsum(w)
    cw /= cw[-1]
    return g[idx][min(np.searchsorted(cw, frac), len(g) - 1)]


def _get_bins(log_dV, adaptive_mask, cycle, ncand=20000):
    """Per-axis bin counts whose product is closest to 1/dV over the adaptive axes (upstream
    get_bins: alternate log-uniform and Gaussian scatter about the isotropic count)."""
    D = len(adaptive_mask)
    r = int(np.sum(adaptive_mask))
    nb = np.ones(D, dtype=np.int64)
    if r == 0 or not np.isfinite(log_dV):
        return nb
    mbin = np.exp(-log_dV / r)
    if cycle % 2 == 0:
        c = (mbin * 2 ** np.random.uniform(-1, 1, size=(ncand, r)) + 0.5).astype(np.int64)
    else:
        c = np.maximum(1, mbin + np.sqrt(mbin) * np.random.standard_normal((ncand, r))).astype(np.int64)
    c = np.maximum(1, c)
    k = np.argmin(np.abs(1 - np.exp(np.log(c).sum(axis=1) + log_dV)))
    nb[adaptive_mask] = c[k]
    return nb


class MCSampler(SamplerOutputMixin, object):
    """Varaha v2 with a stall guard and an unbiased refresh pass.  Same driver API as
    mcsamplerAdaptiveVolume.MCSampler (add_parameter / setup / integrate[_log]) and the same
    portfolio member contract (draw_simplified, sampling_density, update_sampling_prior_selfish)."""

    joint_p_s_is_normalized_density = True

    # parameters (see DESIGN_varaha2.md for the measurements)
    frac = 0.8                 # sampling volume kept per contraction (paper: 0.95)
    neff_gate = 2000           # Kish size of the live set's 1/q needed to contract (paper: 1e4)
    nsel = 500                 # sum(1/q)/max(1/q) needed to contract (paper: 2000)
    stop_pinside = 0.05        # adaptation stops when posterior mass above the threshold < this
    post_frac = 0.3            # stall guard: share of a cycle's draws allocated by posterior weight
    post_floor = 1e-3          # stall guard: bins with weight > floor*max get >= 1 draw
    defensive_frac = 0.05      # share of refresh draws from the full box (support guarantee)
    adapt_budget_frac = 0.7    # adaptation may use at most this share of nmax
    max_cycle_draws = 50       # per-cycle draws capped near max_cycle_draws * neff_gate
    verdict_khat = 0.7
    verdict_min_neff = 10.0

    @staticmethod
    def match_params_from_args(args, params):
        return _av.MCSampler.match_params_from_args(args, params)

    def __init__(self, n_chunk=400000, **kwargs):
        self.ntotal = 0
        self.params = set()
        self.params_ordered = []
        self.params_pinned_vals = {}
        self._rvs = {}
        self.llim, self.rlim = {}, {}
        self.n_chunk = n_chunk
        self.adaptive = []
        self.pdf = {}
        self.prior_pdf = {}
        self.xpy = numpy
        self.identity_convert = lambda x: x
        self._state = None
        self.is_varaha = True        # portfolio: train only on own draws (update_sampling_prior_selfish)
        for k, v in kwargs.items():
            if hasattr(type(self), k):
                setattr(self, k, v)

    # parameter bookkeeping is shared with AV verbatim
    add_parameter = _av.MCSampler.add_parameter
    prior_prod = _av.MCSampler.prior_prod
    clear = _av.MCSampler.clear

    @property
    def has_unbounded_support(self):
        # the defensive full-box component makes the mixture density nowhere zero in the box
        return self.defensive_frac > 0

    def setup(self, **kwargs):
        ndim = len(self.params_ordered)
        self.my_ranges = np.array([[self.llim[x], self.rlim[x]] for x in self.params_ordered], dtype=float)
        self.dx0 = np.diff(self.my_ranges, axis=1).flatten()
        self.log_box = float(np.sum(np.log(self.dx0)))
        self.V_s = float(np.prod(self.dx0))
        self.coef = (np.random.randint(1, 2 ** 62, size=ndim, dtype=np.int64) | 1)
        self._state = None
        self.is_varaha = True

    # ---------------------------------------------------------------- coordinates
    def _adaptive_mask(self):
        m = np.array([p in self.adaptive and p not in self.params_pinned_vals for p in self.params_ordered])
        return m

    def _to_x(self, U):
        X = self.my_ranges[:, 0] + U * self.dx0
        for p, val in self.params_pinned_vals.items():
            X[:, self.params_ordered.index(p)] = val
        return X

    def _to_u(self, X):
        return np.clip((np.asarray(X, float) - self.my_ranges[:, 0]) / self.dx0, 0.0, 1.0 - 1e-15)

    # ---------------------------------------------------------------- integrand
    def _eval(self, lnF, X, no_protect_names=True):
        """lnL at host points X (N, ndim), device-first with a remembered host fallback (the AV
        contract).  Returns host float64."""
        def call(samples):
            if no_protect_names:
                return lnF(*samples.T)
            return lnF(**dict(zip(self.params_ordered, samples.T)))
        use_dev = cupy_ok and (self.xpy is not numpy)
        step = max(int(self.n_chunk), 1)
        out = []
        for i0 in range(0, len(X), step):
            Xb = X[i0:i0 + step]
            if not use_dev or getattr(self, '_integrand_wants_host', False):
                lnL = call(Xb)
            else:
                try:
                    lnL = call(identity_convert_togpu(Xb))
                except (TypeError, ValueError):
                    self._integrand_wants_host = True
                    print("  [AV2] integrand refused a device array; evaluating on the host from here on")
                    lnL = call(Xb)
            out.append(np.asarray(identity_convert(lnL), dtype=np.float64).ravel())
        lnL = np.concatenate(out) if out else np.zeros(0)
        lnL[np.isnan(lnL)] = -np.inf
        return lnL

    def _log_prior(self, X):
        return np.log(np.asarray(identity_convert(self.prior_prod(X)), dtype=float))

    # ---------------------------------------------------------------- adaptation state
    def _init_state(self):
        D = len(self.params_ordered)
        nmc = int(self.neff_gate / self.frac) + 1
        g0 = _Grid(np.ones(D), np.zeros((1, D)), [nmc], self.coef)
        self._state = dict(
            grids={1: g0}, cycle=1, lkl_cycle=1, probmass_cycle=1,
            thr={1: -np.inf}, logV={1: 0.0}, logdV={1: -np.inf}, del_upto={},
            U=np.zeros((0, D)), G=np.zeros(0), LQ=np.zeros(0), LNL=np.zeros(0), LP=np.zeros(0),
            CYC=np.zeros(0, dtype=int), pinside=1.0, n_drawn=0, nfail_alloc=0,
            n_forced_floor=0, converged=False)

    def _cycle(self, lnF, no_protect_names=True, max_draws=None, draw_budget=None):
        """One Varaha-v2 cycle: draw the current grid, evaluate, update densities, threshold, and
        build the next grid.  Returns the number of likelihood evaluations made.  max_draws caps the
        NEXT grid's planned draws (portfolio selfish steps, whose calls fall outside the portfolio's
        nmax); draw_budget caps the total draws so far plus the next grid's (standalone adaptation).
        The density uses the planned counts, so either cap changes efficiency, not the estimate."""
        s = self._state
        f = self.frac
        nback = int(-np.log(2.0) / np.log(f))
        cyc = s['cycle']
        grids = s['grids']
        u = grids[cyc].draw()
        X = self._to_x(u)
        lnL = self._eval(lnF, X, no_protect_names)
        lp = self._log_prior(X)
        g = lnL + lp
        nd = len(u)
        s['n_drawn'] += nd
        thr_pm = s['thr'][s['probmass_cycle']]
        ok = np.isfinite(g) & (g >= thr_pm)
        u, g, lnL, lp = u[ok], g[ok], lnL[ok], lp[ok]
        keep = s['G'] >= thr_pm
        for k in ('U', 'G', 'LQ', 'LNL', 'LP', 'CYC'):
            s[k] = s[k][keep]
        if cyc > 1 and len(s['U']):
            s['LQ'] = np.logaddexp(s['LQ'], grids[cyc].log_density(s['U']))
        lq = np.full(len(u), -np.inf)
        for gr in grids.values():
            lq = np.logaddexp(lq, gr.log_density(u))
        s['U'] = np.vstack([s['U'], u]); s['G'] = np.append(s['G'], g); s['LQ'] = np.append(s['LQ'], lq)
        s['LNL'] = np.append(s['LNL'], lnL); s['LP'] = np.append(s['LP'], lp)
        s['CYC'] = np.append(s['CYC'], np.full(len(u), cyc))
        if len(s['G']) == 0:
            # nothing finite yet: keep drawing the same grid (cold high-SNR start)
            print("  [AV2] cycle {}: no finite draws yet ({} drawn)".format(cyc, nd))
            return nd
        thr_l = s['thr'][s['lkl_cycle']]
        eff = np.sum(g >= thr_l) / float(nd)
        live = s['G'] >= thr_l
        logiw = -s['LQ'][live]
        iw = np.exp(logiw - logiw.max())
        injneff = iw.sum() ** 2 / np.sum(iw ** 2)
        if iw.sum() > self.nsel and injneff > self.neff_gate:
            nxt = _weighted_threshold(s['G'][live], logiw, f)
            s['lkl_cycle'] += 1
            lc = s['lkl_cycle']
            s['thr'][lc] = nxt
            above = s['G'][live] > nxt
            s['logV'][lc] = s['logV'][lc - 1] + np.log(iw[above].sum() / iw.sum())
            s['logdV'][lc] = s['logV'][lc] - 0.5 * np.log(injneff)
        lc = s['lkl_cycle']
        s['del_upto'][cyc] = s['thr'][lc]
        if s['probmass_cycle'] > 5 * nback:
            cut = s['thr'][s['probmass_cycle'] - 5 * nback]
            for c in [c for c in list(grids) if c != cyc and s['del_upto'].get(c, np.inf) < cut]:
                ld = grids[c].log_density(s['U'])
                m = np.isfinite(ld)
                with np.errstate(invalid='ignore', divide='ignore'):
                    s['LQ'][m] = s['LQ'][m] + np.log1p(-np.exp(ld[m] - s['LQ'][m]))
                sel = s['CYC'] != c
                for k in ('U', 'G', 'LQ', 'LNL', 'LP', 'CYC'):
                    s[k] = s[k][sel]
                del grids[c]
        if not np.any(s['G'] >= s['thr'][lc]):
            # pruning removed every live draw: keep drawing the current grid again
            s['cycle'] = cyc + 1
            grids[cyc + 1] = _Grid(grids[cyc].nbins, grids[cyc].bins, grids[cyc].counts, self.coef)
            return nd
        lw = s['G'] - s['LQ']
        w = np.exp(lw - lw.max())
        s['pinside'] = float(w[s['G'] > s['thr'][lc]].sum() / w.sum())
        if s['pinside'] > 0.999:
            s['probmass_cycle'] = max(s['probmass_cycle'], lc - nback)
        if s['pinside'] <= self.stop_pinside:
            s['converged'] = True
        # next proposal
        live = s['G'] >= s['thr'][lc]
        logiw = -s['LQ'][live]
        iw = np.exp(logiw - logiw.max())
        injneff2 = iw.sum() ** 2 / np.sum(iw ** 2)
        nsamp = (1 - f) * live.sum() / max(eff, 1e-3) / 0.9 * (self.neff_gate / injneff2)
        nsamp = min(nsamp, self.max_cycle_draws * self.neff_gate)
        if draw_budget is not None:
            left = draw_budget - s['n_drawn']
            max_draws = left if max_draws is None else min(max_draws, left)
        if max_draws is not None:
            if max_draws < 1:
                s['cycle'] = cyc + 1      # budget spent: no next grid, the caller stops adapting
                return nd
            nsamp = min(nsamp, float(max_draws))
        s['cycle'] = cyc = cyc + 1
        nb = _get_bins(s['logdV'][lc], self._adaptive_mask(), cyc)
        binidx = np.minimum((s['U'][live] * nb).astype(np.int64), nb - 1)
        bu, inv = np.unique(binidx, axis=0, return_inverse=True)
        inv = np.asarray(inv).ravel()
        B = np.zeros(len(bu))
        np.add.at(B, inv, np.exp(s['LQ'][live] - s['LQ'][live].max()))
        n_j = B.max() - B
        if n_j.max() == 0:
            n_j = np.ones_like(n_j)
        p = n_j * nsamp / n_j.sum()
        if self.post_frac > 0:
            lwl = lw[live]
            W = np.zeros(len(bu))
            np.add.at(W, inv, np.exp(lwl - lwl.max()))
            p = (1 - self.post_frac) * p + self.post_frac * nsamp * W / W.sum()
            p = np.where(W > self.post_floor * W.max(), np.maximum(p, 1.0), p)
            if max_draws is not None and p.sum() > max_draws:
                p *= max_draws / p.sum()      # the >=1 floor must not break the cap
        cnt = p.astype(np.int64)
        cnt += (p - cnt) > np.random.random(len(p))
        sel = cnt > 0
        if not np.any(sel):
            cnt[np.argmax(p)] = 1
            sel = cnt > 0
        grids[cyc] = _Grid(nb, bu[sel], cnt[sel], self.coef)
        return nd

    # ---------------------------------------------------------------- mixture proposal
    def _mixture(self):
        """(grids drawn so far, their total counts) defining q_mix = sum_c n_c(bin)/|bin| / Ntot."""
        s = self._state
        if s is None:
            return [], 0.0
        gl = [g for c, g in s['grids'].items() if c != s['cycle']]   # current grid not drawn yet
        tot = float(sum(g.counts.sum() for g in gl))
        return gl, tot

    def _draw_mixture_u(self, n):
        gl, tot = self._mixture()
        D = len(self.params_ordered)
        n_def = np.random.binomial(int(n), self.defensive_frac) if gl else int(n)
        parts = [np.random.uniform(size=(n_def, D))]
        if gl and n - n_def > 0:
            pick = np.random.multinomial(int(n - n_def), np.array([g.counts.sum() for g in gl]) / tot)
            for g, m in zip(gl, pick):
                if m:
                    parts.append(g.draw(m))
        u = np.vstack(parts)
        return u[np.random.permutation(len(u))]

    def _log_q_mix_u(self, U):
        gl, tot = self._mixture()
        if not gl:
            return np.zeros(len(U))
        lq = np.full(len(U), -np.inf)
        for g in gl:
            lq = np.logaddexp(lq, g.log_density(U))
        lq = lq - np.log(tot)
        return np.logaddexp(np.log1p(-self.defensive_frac) + lq, np.log(self.defensive_frac))

    def _log_q_x(self, U):
        """normalized sampling density in x (pinned dims carry 1/width, the AV convention)"""
        return self._log_q_mix_u(U) - self.log_box

    # ---------------------------------------------------------------- portfolio member contract
    def sampling_density(self, X):
        if getattr(self, 'my_ranges', None) is None:
            return None
        X = np.atleast_2d(np.asarray(identity_convert(X), dtype=float))
        if X.shape[1] != len(self.params_ordered) and X.shape[0] == len(self.params_ordered):
            X = X.T
        inside = np.all((X >= self.my_ranges[:, 0]) & (X <= self.my_ranges[:, 1]), axis=1)
        q = np.exp(self._log_q_x(self._to_u(X)))
        q[~inside] = 0.0
        return q

    def draw_simplified(self, n_to_get, *args, **kwargs):
        if getattr(self, 'my_ranges', None) is None:
            self.setup()
        if self._state is None:
            self._init_state()
        U = self._draw_mixture_u(int(n_to_get))
        X = self._to_x(U)
        ps = np.exp(self._log_q_x(U))
        p = np.exp(self._log_prior(X))
        if cupy_ok and self.xpy is not numpy:
            ps, p, X = identity_convert_togpu(ps), identity_convert_togpu(p), identity_convert_togpu(X)
        return ps, p, X.T

    def reset_selfish_state(self):
        """New portfolio pass: the integrand may differ (calmarg burn-in), so restart adaptation."""
        self._state = None

    def update_sampling_prior_selfish(self, lnF, *args, xpy=None, no_protect_names=True, **kwargs):
        if getattr(self, 'my_ranges', None) is None:
            self.setup()
        if self._state is None:
            self._init_state()
            g0 = self._state['grids'][1]
            if g0.counts[0] > self.n_chunk:
                self._state['grids'][1] = _Grid(g0.nbins, g0.bins, [int(self.n_chunk)], self.coef)
        if self._state['converged']:
            return
        # bounded like AV's selfish step: at most n_chunk own draws per portfolio chunk
        self._cycle(lnF, no_protect_names=no_protect_names, max_draws=self.n_chunk)

    # ---------------------------------------------------------------- integration
    def integrate_log(self, lnF, *args, xpy=None, **kwargs):
        t0 = time.time()
        for p, val in list(kwargs.items()):
            if p in self.params_ordered:
                self.params_pinned_vals[p] = val
        self._rvs_is_fairdraw = False
        self._rvs_record = None
        nmax = kwargs.get("nmax", float("inf"))
        neff = kwargs.get("neff", float("inf"))
        if not np.isfinite(nmax):
            raise ValueError("mcsamplerVaraha2.integrate_log needs a finite nmax")
        if not (0.0 < self.frac < 1.0):
            raise ValueError("mcsamplerVaraha2: frac must lie in (0, 1), got {}".format(self.frac))
        n = int(kwargs.get("n", min(100000, nmax)))
        self.n_chunk = n
        no_protect = 'no_protect_names' in kwargs
        verbose = kwargs.get("verbose", False)
        bFairdraw = kwargs.get("igrand_fairdraw_samples", False)
        n_extr = kwargs.get("igrand_fairdraw_samples_max", None)
        if 'integrand' in self._rvs:
            del self._rvs['integrand']
        self.setup()
        self._init_state()
        s = self._state

        # --- adaptation
        # Every grid is sized before it is drawn, so adaptation never spends more than
        # n_adapt_max calls and the refresh keeps the rest of nmax.
        n_adapt_max = int(self.adapt_budget_frac * nmax)
        g0 = s['grids'][1]
        if g0.counts[0] > n_adapt_max:
            s['grids'][1] = _Grid(g0.nbins, g0.bins, [max(n_adapt_max, 1)], self.coef)
        while not s['converged'] and s['cycle'] < 5000:
            cur = s['grids'].get(s['cycle'])
            # a redraw path (no finite draws, or pruning emptied the live set) can leave a grid
            # larger than what is left; stop rather than overrun
            if cur is None or s['n_drawn'] + cur.counts.sum() > n_adapt_max:
                break
            self._cycle(lnF, no_protect_names=no_protect, draw_budget=n_adapt_max)
            if verbose:
                print("  [AV2] cycle {} N={} pinside={:.4f} logV={:.3f} kept={} grids={}".format(
                    s['cycle'], s['n_drawn'], s['pinside'], s['logV'][s['lkl_cycle']], len(s['G']), len(s['grids'])))
        n_adapt = s['n_drawn']
        lw_pool = s['LNL'] + s['LP'] + self.log_box - s['LQ']
        lnI_pool = float(logsumexp(lw_pool)) if len(lw_pool) else -np.inf   # sum L p |dx/du| / q_u
        ess_pool = ess_from_log_weights(lw_pool) if len(lw_pool) else 0.0
        if not s['converged']:
            print("  [AV2 STALL] adaptation did not converge in {} draws (pinside={:.3f}, pooled ESS={:.1f});"
                  " the refresh pass still gives an unbiased lnZ, judge it by its n_eff and k-hat.".format(
                      n_adapt, s['pinside'], ess_pool))

        # --- refresh: fixed-proposal IS from the frozen mixture
        XR, LNL, LP, LQX = [], [], [], []
        n_ref = 0
        eff_samp = 0.0
        while True:
            U = self._draw_mixture_u(max(1, min(n, int(nmax) - n_adapt - n_ref)))
            X = self._to_x(U)
            lnL = self._eval(lnF, X, no_protect)
            lp = self._log_prior(X)
            XR.append(X); LNL.append(lnL); LP.append(lp); LQX.append(self._log_q_x(U))
            n_ref += len(U)
            lw = np.concatenate(LNL) + np.concatenate(LP) - np.concatenate(LQX)
            fin = np.isfinite(lw)
            eff_samp = float(np.exp(logsumexp(lw[fin]) - lw[fin].max())) if fin.any() else 0.0
            if eff_samp >= neff or n_adapt + n_ref >= nmax:
                break
        X = np.vstack(XR); lnL = np.concatenate(LNL); lp = np.concatenate(LP); lqx = np.concatenate(LQX)
        lw = lnL + lp - lqx
        fin = np.isfinite(lw)
        log_int = float(logsumexp(lw[fin]) - np.log(n_ref)) if fin.any() else -np.inf
        self.ntotal = int(n_adapt + n_ref)

        # export finite rows; rescale the density so logsumexp(lw)-log(n_rows) is the estimate
        nf = int(fin.sum())
        for i, p in enumerate(self.params_ordered):
            self._rvs[p] = X[fin, i]
        self._rvs['log_integrand'] = lnL[fin]
        self._rvs['log_joint_prior'] = lp[fin]
        self._rvs['log_joint_s_prior'] = lqx[fin] + np.log(n_ref) - np.log(max(nf, 1))
        lw_f = lw[fin]
        w = np.exp(lw_f - lw_f.max()) if nf else np.zeros(0)
        rel_var = float(np.sum(w ** 2) / np.sum(w) ** 2 - 1.0 / n_ref) if nf else np.inf
        rel_var = max(rel_var, 0.0)
        self._rvs_record = RvsRecord.retained(self._rvs)
        if bFairdraw and n_extr is not None and nf:
            n_extr = int(np.min([n_extr, 1.5 * eff_samp, 1.5 * neff]))
            pw = w / w.sum()
            if n_extr < nf:
                ix = np.random.choice(nf, size=n_extr, replace=True, p=pw)
                for key in list(self._rvs.keys()):
                    self._rvs[key] = np.asarray(self._rvs[key])[ix]
                self._rvs_is_fairdraw = True
                self._rvs_record = RvsRecord.fair_draw(self._rvs, n_retained=self._rvs_record.n_retained())

        khat = pareto_khat_from_log(lw_f) if nf else None
        kish = ess_from_log_weights(lw_f) if nf else 0.0
        reasons = []
        if eff_samp < self.verdict_min_neff:
            reasons.append("refresh n_eff {:.1f} < {}".format(eff_samp, self.verdict_min_neff))
        if khat is not None and khat > self.verdict_khat and kish < 100:
            reasons.append("Pareto k-hat {:.2f} with Kish ESS {:.0f}".format(khat, kish))
        if not np.isfinite(log_int):
            reasons.append("no finite refresh draw")
        collapsed = len(reasons) > 0
        dict_return = dict(
            live_volume_collapsed=collapsed, av2_converged=bool(s['converged']),
            av2_n_adapt=int(n_adapt), av2_n_refresh=int(n_ref), av2_cycles=int(s['cycle']),
            av2_pinside=float(s['pinside']), av2_lnZ_pooled=lnI_pool, av2_ESS_pooled=float(ess_pool),
            n_ESS=float(kish), sigma_lnZ_mc=float(np.sqrt(rel_var)), wall=time.time() - t0)
        if khat is not None:
            dict_return['pareto_khat'] = float(khat)
        if collapsed:
            dict_return['collapse_reason'] = "; ".join(reasons)
            print(" [AV2 COLLAPSE] " + dict_return['collapse_reason'] + ": lnZ and samples are not usable.")
        print(" [AV2] lnZ={:.4f} (pooled {:.4f}) n_eff={:.1f} ESS={:.0f} khat={} draws adapt/refresh={}/{}"
              " converged={}".format(log_int, lnI_pool, eff_samp, kish,
                                    None if khat is None else round(khat, 3), n_adapt, n_ref, s['converged']))
        log_var = np.log(rel_var) + 2 * log_int if (rel_var > 0 and np.isfinite(rel_var) and np.isfinite(log_int)) else -np.inf
        return log_int, log_var, eff_samp, dict_return

    def integrate(self, func, *args, **kwargs):
        use_lnL = bool(kwargs.get('use_lnL', False))
        if use_lnL:
            out = self.integrate_log(func, *args, **kwargs)
            self._rvs['integrand'] = self._rvs['log_integrand']
            return out
        return self.integrate_log(lambda *a, **k: np.log(func(*a, **k)), *args, **kwargs)
