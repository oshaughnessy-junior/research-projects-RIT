"""Brute-force reference integral for one pinned-distance Omega slice.

WHY THIS EXISTS
---------------
The adaptive-volume (AV/VARAHA) integrator is *adaptive*: its proposal, its
likelihood threshold and its bookkept live volume ``V`` all evolve using the
very samples it later averages over.  That makes it impossible to certify a
small shift in ``lnI`` (e.g. the grid-freeze change) from AV replicates alone --
more replicates only shrink the scatter around whatever AV converges to, they
cannot detect a common offset.

This module computes the SAME integral,

    I = \\int_box L(Omega) p(Omega) dOmega        (p = product of prior_pdf)

with a plain importance-sampling estimator whose proposal ``q`` is FIXED BEFORE
any of the samples that enter the estimate are drawn.  For a fixed q > 0 on the
support, ``mean(L p / q)`` is EXACTLY unbiased for any q, good or bad -- the
proposal quality controls only the variance.  So this is a legitimate reference
even though q is *built* from a pilot AV run: the pilot samples are thrown away
and never enter the average.

THE PROPOSAL
------------
A three-component defensive mixture, all piecewise constant and therefore
exactly evaluable and exactly sampleable:

    q = w_f * hist_fine  +  w_c * hist_coarse  +  w_u * uniform(box)

``hist_fine`` / ``hist_coarse`` are regular histograms of the pilot cloud on the
Omega box at two resolutions; ``uniform`` guarantees q > 0 everywhere in the
box, so no part of the support can be missed however bad the pilot was.  That
last term is what makes the unbiasedness argument unconditional rather than a
hope about pilot coverage.

WHAT IS REPORTED
----------------
lnI, its Monte Carlo error, ESS, and the Pareto k-hat of the weight tail.
k-hat is the honest health check: a large k means the reported error understates
the truth, and the reference should not be trusted at face value.
"""
from __future__ import print_function

import numpy as np

__all__ = ["brute_reference_integral"]


def _pareto_khat(w):
    """Pareto-smoothing shape parameter of the top tail of the weights.
    Standard PSIS estimator (Vehtari et al.).  Returns nan if too few points."""
    w = np.sort(np.asarray(w, dtype=float))
    w = w[np.isfinite(w) & (w > 0)]
    n = len(w)
    if n < 50:
        return float("nan")
    m = int(min(n / 5.0, 3.0 * np.sqrt(n)))
    if m < 10:
        return float("nan")
    tail = w[-m:]
    mu = w[-m - 1]
    x = tail - mu
    prior = 3.0
    kn = 30 + int(np.sqrt(m))
    bs = 1.0 - np.sqrt(kn / (np.arange(1, kn + 1) - 0.5))
    bs = bs / (prior * x[int(m / 4 + 0.5) - 1]) + 1.0 / x[-1]
    ks = np.array([np.mean(np.log1p(-b * x)) for b in bs])
    ls = m * (np.log(-bs / ks) - ks - 1.0)
    ws = 1.0 / np.array([np.sum(np.exp(ls - l)) for l in ls])
    b = np.sum(bs * ws)
    k = np.mean(np.log1p(-b * x))
    return float((m * k + prior * 0.5) / (m + prior))


class _HistProposal(object):
    """Piecewise-constant proposal supported on the cells a point cloud occupies.

    Stores only OCCUPIED cells (flat cell indices), so memory is O(n_pilot),
    not O(nbins**ndim) -- 5 dims at 20 bins/axis is 3.2e6 cells but at most
    n_pilot of them are ever touched.

    ``uniform_cells`` (default True) puts EQUAL mass on every occupied cell
    instead of mass proportional to the pilot counts.  That matters more than
    it looks: with count weighting, a cell holding one pilot point gets a tiny
    q, so any decent likelihood there produces an enormous importance weight.
    Measured on the real slice integral, that tail made ESS go DOWN as samples
    were added (519 at 4e5 draws, 232 at 6e5) with Pareto k-hat ~1.3 -- an
    estimator whose error bar means nothing.  With equal mass, q is bounded
    below on the cover and the weights are bounded by max(L*p)/q_min, so the
    variance is finite by construction.

    ``dilate`` grows the cover by axis-neighbour layers, which both widens
    coverage and further raises q_min.
    """

    def __init__(self, pts, lo, hi, nbins, uniform_cells=True, dilate=0,
                 pt_weights=None):
        self.lo = np.asarray(lo, dtype=float)
        self.hi = np.asarray(hi, dtype=float)
        self.ndim = len(self.lo)
        self.nb = int(nbins)
        self.width = (self.hi - self.lo) / self.nb
        self.cell_vol = float(np.prod(self.width))
        idx = self._cell_index(np.asarray(pts, dtype=float))
        if pt_weights is None:
            cells, counts = np.unique(idx, return_counts=True)
        else:
            # Accumulate the pilot's IMPORTANCE WEIGHT per cell, not its
            # occupancy.  This matters more than any other choice here: AV draws
            # ~uniformly over its live volume, so an occupancy histogram
            # approximates the live region's INDICATOR, not the integrand -- and
            # L varies over many e-folds inside that region.  Proposing from the
            # indicator therefore leaves weights proportional to L, with exactly
            # the heavy tail we are trying to avoid (measured: ESS collapsing
            # from 3604 to 50 when one draw landed in a low-occupancy,
            # high-likelihood cell).  Weighting by exp(lnL + ln p - ln q_AV)
            # makes q track L*p instead.
            w = np.asarray(pt_weights, dtype=float)
            cells, inv = np.unique(idx, return_inverse=True)
            counts = np.bincount(inv, weights=w, minlength=len(cells))
            keep = counts > 0
            cells, counts = cells[keep], counts[keep]
        if dilate > 0:
            cells = self._dilate(cells, int(dilate))
            counts = np.ones(len(cells), dtype=float)
        self.cells = cells                      # flat indices, sorted
        if uniform_cells:
            self.frac = np.full(len(cells), 1.0 / len(cells))
        else:
            self.frac = counts / float(counts.sum())
        self.cdf = np.cumsum(self.frac)
        self.cdf[-1] = 1.0

    def _dilate(self, cells, layers):
        """Add the +-1 axis neighbours of every cell, `layers` times.  The
        axis cross (2*ndim+1 offsets), not the full 3**ndim block: in 5-D the
        block is 243x and buys little that two cross layers do not."""
        b = self._unflatten(cells)
        for _ in range(layers):
            outs = [b]
            for j in range(self.ndim):
                for s in (-1, 1):
                    nb_ = b.copy()
                    nb_[:, j] += s
                    outs.append(nb_)
            b = np.vstack(outs)
            np.clip(b, 0, self.nb - 1, out=b)
            flat = np.zeros(len(b), dtype=np.int64)
            for j in range(self.ndim):
                flat = flat * self.nb + b[:, j]
            flat = np.unique(flat)
            b = self._unflatten(flat)
        flat = np.zeros(len(b), dtype=np.int64)
        for j in range(self.ndim):
            flat = flat * self.nb + b[:, j]
        return np.unique(flat)

    def _cell_index(self, X):
        b = np.floor((X - self.lo) / self.width).astype(np.int64)
        np.clip(b, 0, self.nb - 1, out=b)
        flat = np.zeros(len(X), dtype=np.int64)
        for j in range(self.ndim):
            flat = flat * self.nb + b[:, j]
        return flat

    def _unflatten(self, flat):
        b = np.empty((len(flat), self.ndim), dtype=np.int64)
        rem = flat.copy()
        for j in range(self.ndim - 1, -1, -1):
            b[:, j] = rem % self.nb
            rem //= self.nb
        return b

    def density(self, X):
        """q_hist(x): mass of x's cell divided by the cell volume; 0 outside."""
        flat = self._cell_index(X)
        pos = np.searchsorted(self.cells, flat)
        np.clip(pos, 0, len(self.cells) - 1, out=pos)
        hit = self.cells[pos] == flat
        out = np.zeros(len(X), dtype=float)
        out[hit] = self.frac[pos[hit]] / self.cell_vol
        return out

    def sample(self, n, rng):
        pick = np.searchsorted(self.cdf, rng.random(n))
        np.clip(pick, 0, len(self.cells) - 1, out=pick)
        b = self._unflatten(self.cells[pick])
        return self.lo + (b + rng.random((n, self.ndim))) * self.width


class _Mixture(object):
    """sum_m w_m q_m, plus a final uniform-on-the-box term.  Exactly evaluable,
    exactly sampleable, and strictly positive inside the box.

    The components are deliberately of two kinds, and BOTH are needed:

      * count-weighted histograms give the EFFICIENCY -- they put mass where
        the pilot says the integrand lives;
      * a uniform-over-the-cover component (plus the uniform-over-box term)
        gives the FLOOR on q, which is what bounds the importance weights.

    With only the first kind, a cell holding one pilot point has q ~ 1/n_pilot
    and any real likelihood there produces a huge weight (measured: ESS falling
    as draws were added, k-hat 1.3).  With only the second, mass is spread
    evenly over a cover far larger than the support and the efficiency collapses
    (measured on the analytic check: ESS 326 -> 8).  Mixing them fixes both;
    that is the whole design.
    """

    def __init__(self, components, weights, lo, hi):
        self.components = list(components)
        self.lo = np.asarray(lo, dtype=float)
        self.hi = np.asarray(hi, dtype=float)
        self.ndim = len(self.lo)
        self.box_vol = float(np.prod(self.hi - self.lo))
        w = np.asarray(weights, dtype=float)
        assert len(w) == len(self.components) + 1, "one weight per component + box"
        self.w = w / w.sum()

    def density(self, X):
        out = np.full(len(X), self.w[-1] / self.box_vol)
        for wi, c in zip(self.w[:-1], self.components):
            if wi > 0:
                out += wi * c.density(X)
        return out

    def sample(self, n, rng):
        which = rng.choice(len(self.w), size=n, p=self.w)
        out = np.empty((n, self.ndim), dtype=float)
        for i, c in enumerate(self.components):
            k = int(np.sum(which == i))
            if k:
                out[which == i] = c.sample(k, rng)
        k = int(np.sum(which == len(self.w) - 1))
        if k:
            out[which == len(self.w) - 1] = (
                self.lo + rng.random((k, self.ndim)) * (self.hi - self.lo))
        return out


def brute_reference_integral(like_fn, params, lo, hi, prior_pdfs, pilot_pts,
                             n_total=2000000, block=10000, nb_fine=20,
                             nb_coarse=6, weights=(0.60, 0.30, 0.10),
                             seed=1234, tag="BRUTE", verbose=True,
                             uniform_cells=True, dilate_fine=1, dilate_coarse=0,
                             pilot_weights=None):
    """Fixed-proposal importance-sampling estimate of int L p dOmega.

    ``like_fn(**{param: array})`` must return lnL as a host array.
    ``pilot_pts`` is an (M, ndim) cloud in ``params`` order, used ONLY to build
    the proposal; it never enters the average.
    Returns a dict with lnI, sigma_lnI, ESS, khat and diagnostics.
    """
    lo = np.asarray(lo, dtype=float)
    hi = np.asarray(hi, dtype=float)
    ndim = len(params)
    rng = np.random.default_rng(seed)

    pilot_pts = np.atleast_2d(np.asarray(pilot_pts, dtype=float))
    # Four components: count-weighted fine histogram (efficiency), a DILATED
    # uniform cover at the same resolution (floors q near the support),
    # count-weighted coarse histogram (broad coverage), uniform box (floors q
    # everywhere, so the estimator is unbiased whatever the pilot missed).
    fine = _HistProposal(pilot_pts, lo, hi, nb_fine, uniform_cells=False, dilate=0,
                         pt_weights=pilot_weights)
    cover = _HistProposal(pilot_pts, lo, hi, nb_fine, uniform_cells=True,
                          dilate=dilate_fine)
    coarse = _HistProposal(pilot_pts, lo, hi, nb_coarse, uniform_cells=False,
                           dilate=dilate_coarse, pt_weights=pilot_weights)
    q = _Mixture([fine, cover, coarse], weights, lo, hi)
    if verbose:
        print("%s PROPOSAL pilot=%d fine_cells=%d cover_cells=%d (nb=%d)"
              " coarse_cells=%d (nb=%d) w=%s box_vol=%.6g"
              % (tag, len(pilot_pts), len(fine.cells), len(cover.cells), nb_fine,
                 len(coarse.cells), nb_coarse,
                 np.array2string(q.w, precision=3), q.box_vol), flush=True)

    ln_w_all = []
    n_done = 0
    n_nonfinite = 0
    while n_done < n_total:
        nb = int(min(block, n_total - n_done))
        X = q.sample(nb, rng)
        # keep strictly inside the box: the density is zero outside and the
        # likelihood's arccos()s dislike exact edges
        eps = 1e-12 * np.maximum(np.abs(hi - lo), 1.0)
        np.clip(X, lo + eps, hi - eps, out=X)
        lnL = np.asarray(like_fn(**{p: X[:, j] for j, p in enumerate(params)}),
                         dtype=float)
        ln_p = np.zeros(nb, dtype=float)
        for j, p in enumerate(params):
            ln_p += np.log(np.maximum(prior_pdfs[p](X[:, j]), 1e-300))
        ln_q = np.log(np.maximum(q.density(X), 1e-300))
        lw = lnL + ln_p - ln_q
        bad = ~np.isfinite(lw)
        n_nonfinite += int(np.sum(bad))
        lw[bad] = -np.inf          # a -inf likelihood contributes exactly 0
        ln_w_all.append(lw)
        n_done += nb
        if verbose and (n_done % (20 * block) == 0 or n_done >= n_total):
            cur = np.concatenate(ln_w_all)
            m = np.max(cur)
            lnI_now = m + np.log(np.sum(np.exp(cur - m))) - np.log(len(cur))
            wn = np.exp(cur - m)
            ess_now = float(np.sum(wn) ** 2 / np.sum(wn ** 2))
            print("%s PROGRESS n=%d lnI=%.4f ESS=%.1f" % (tag, n_done, lnI_now, ess_now),
                  flush=True)

    lw = np.concatenate(ln_w_all)
    m = np.max(lw)
    if not np.isfinite(m):
        return dict(lnI=float("-inf"), sigma_lnI=float("nan"), ess=0.0,
                    khat=float("nan"), n=len(lw), n_nonfinite=n_nonfinite)
    wn = np.exp(lw - m)                      # normalized-by-max weights
    N = len(lw)
    lnI = m + np.log(np.sum(wn)) - np.log(N)
    # MC error of the MEAN of w: sigma(I)/I = sqrt(var(w)/N)/mean(w)
    mean_w = np.mean(wn)
    rel_sig = float(np.sqrt(np.var(wn) / N) / mean_w) if mean_w > 0 else float("nan")
    ess = float(np.sum(wn) ** 2 / np.sum(wn ** 2))
    khat = _pareto_khat(wn)
    out = dict(lnI=float(lnI), sigma_lnI=rel_sig, ess=ess, khat=khat, n=N,
               n_nonfinite=n_nonfinite,
               frac_zero=float(np.mean(~np.isfinite(lw))),
               max_lnw=float(m), pilot=len(pilot_pts),
               nb_fine=nb_fine, nb_coarse=nb_coarse, seed=seed)
    if verbose:
        print("%s RESULT lnI=%.6f sigma_lnI=%.6f ESS=%.2f khat=%.3f N=%d"
              " nonfinite=%d fine_cells=%d coarse_cells=%d seed=%d"
              % (tag, out["lnI"], out["sigma_lnI"], out["ess"], out["khat"],
                 out["n"], out["n_nonfinite"], len(fine.cells), len(coarse.cells),
                 seed), flush=True)
    return out
