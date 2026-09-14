"""Per-intrinsic likelihood-vs-distance export for ILE.

The exported ``lnL`` is the *pure* extrinsic-marginalized likelihood as a
function of luminosity distance::

    L_pure(d) = integral L(d, Omega) pi_Omega(Omega) dOmega

i.e. the distance sampling prior has been divided out.  Downstream consumers
can re-marginalize over distance with any prior pi'(d)::

    L_marg' = sum_k exp(lnL[k]) * pi'(dist[k]) * dist_weight[k]

For convenience the grid also carries ``ln_prior_d_sampling``, the per-bin
log of the distance prior that ILE used while integrating, so the original
marginal likelihood can be reproduced exactly::

    lnL_marg = logsumexp(lnL + ln_prior_d_sampling + log(dist_weight))
"""
import numpy as np


DISTANCE_GRID_FIELDS = (
    "lnL",
    "sigmaL",
    "m1",
    "m2",
    "s1x",
    "s1y",
    "s1z",
    "s2x",
    "s2y",
    "s2z",
    "lambda1",
    "lambda2",
    "eccentricity",
    "meanPerAno",
    "eos_index",
    "dist",
    "dist_weight",
    "ln_prior_d_sampling",
)


def _logsumexp(vals):
    vals = np.asarray(vals, dtype=float)
    vmax = np.max(vals)
    if not np.isfinite(vmax):
        return vmax
    return vmax + np.log(np.sum(np.exp(vals - vmax)))


# A bin centre is the weighted mean distance of its block, so two blocks built
# from the same distance value share a centre and leave a zero-width bin between
# them.  One distinct distance therefore cannot be a grid at all.
MIN_UNIQUE_DISTANCES = 2


def _as_positive_integer(value, default):
    if value is None:
        return default
    value = int(value)
    if value < 1:
        raise ValueError("distance grid size must be positive")
    return value


def _weighted_blocks(distance, ln_prior_d, probability, n_grid):
    """Sort samples by distance, split into at most n_grid blocks that never cut a
    run of identical distances, and return per-block (center, mass, width, mean
    ln-prior).  n_grid is reduced to the number of DISTINCT distances when there are
    fewer of those than bins asked for."""
    order = np.argsort(distance)
    distance = np.asarray(distance, dtype=float)[order]
    probability = np.asarray(probability, dtype=float)[order]
    ln_prior_d = np.asarray(ln_prior_d, dtype=float)[order]

    finite = np.isfinite(distance) & np.isfinite(probability) & (probability > 0) & np.isfinite(ln_prior_d)
    distance = distance[finite]
    probability = probability[finite]
    ln_prior_d = ln_prior_d[finite]
    if len(distance) == 0:
        raise ValueError("no finite positive-weight distance samples to export")

    # Block boundaries must fall BETWEEN distinct distances, never inside a run
    # of identical ones.  ILE's fair draw resamples with replacement, so a
    # starved extrinsic pass hands us a few distinct distances each repeated
    # several times; an equal-count split of the raw samples then cuts such a
    # run, gives both halves the same weighted-mean centre, and leaves a
    # zero-width bin between them.  Splitting the DISTINCT distances instead is
    # the identical equal-count split whenever the samples are all distinct.
    starts = np.concatenate(([0], np.flatnonzero(np.diff(distance)) + 1))
    n_unique = len(starts)
    if n_unique < MIN_UNIQUE_DISTANCES:
        raise ValueError(
            "cannot resolve a distance grid: {} finite positive-weight sample(s) "
            "span only {} distinct distance(s)".format(len(distance), n_unique))

    n_grid = min(_as_positive_integer(n_grid, n_unique), n_unique)
    first_unique = [group[0] for group in np.array_split(np.arange(n_unique), n_grid)]
    bounds = np.append(starts[first_unique], len(distance))
    blocks = [np.arange(bounds[i], bounds[i + 1]) for i in range(n_grid)]
    grid_dist = np.empty(len(blocks))
    grid_mass = np.empty(len(blocks))
    grid_ln_prior = np.empty(len(blocks))
    for i, block in enumerate(blocks):
        w = probability[block]
        grid_mass[i] = np.sum(w)
        grid_dist[i] = np.sum(distance[block] * w) / grid_mass[i]
        # weighted average of ln_prior_d (in log space, by importance weights):
        # log E_w[pi_d] = logsumexp(ln_prior_d + log w) - log sum_w
        grid_ln_prior[i] = (
            _logsumexp(ln_prior_d[block] + np.log(w)) - np.log(grid_mass[i])
        )

    if len(grid_dist) == 1:
        width = np.array([np.ptp(distance)])
    else:
        edges = np.empty(len(grid_dist) + 1)
        edges[1:-1] = 0.5 * (grid_dist[1:] + grid_dist[:-1])
        edges[0] = min(distance[0], grid_dist[0] - (edges[1] - grid_dist[0]))
        edges[-1] = max(distance[-1], grid_dist[-1] + (grid_dist[-1] - edges[-2]))
        width = np.diff(edges)

    # lnL carries -log(width), so a floored zero width is not a small error: the
    # old np.maximum(width, eps) floor reported a bin as ~36 nats brighter than
    # its neighbours.  Distinct-distance blocking makes the centres strictly
    # increasing, so duplicate rows can no longer reach here -- but distances
    # separated by an ULP still can (5000.0 + arange(5)*1e-12 raises), because
    # their midpoints collapse onto the centres in float64.  Such a grid cannot be
    # resolved at all; refuse it rather than floor it into a bright bin.
    if not np.all(width > 0):
        raise ValueError(
            "cannot resolve a distance grid: {} of {} bin(s) have non-positive "
            "width over distances [{:g}, {:g}]".format(
                int(np.sum(width <= 0)), len(width), distance[0], distance[-1]))

    return grid_dist, grid_mass, width, grid_ln_prior


def reserve_distance_and_ln_weights(reserve, param="distance"):
    """(distance, ln importance weight) from a sampler's retained-set reserve, or None.

    The reserve (RIFT.integrators.mcsamplerAdaptiveVolume.make_warm_seed_reserve) is a
    bounded copy of the rows a pass actually RETAINED, taken before the fair draw
    rebinds ``_rvs`` to a few rows resampled with replacement.  It carries both prior
    components precisely so a consumer can rebuild the importance weight from it, which
    is what a distance grid needs: the retained rows are NOT equal-weight, so they must
    be weighted by w, unlike a fair-drawn record.

    Returns None whenever the reserve cannot answer -- absent, empty, missing a prior
    component, or built by a sampler whose parameter list has no such coordinate -- so
    the caller can fall back to ``_rvs`` without inspecting the dict itself.
    """
    if not reserve:
        return None
    names = list(reserve.get("params_ordered") or [])
    if param not in names:
        return None
    X = np.asarray(reserve.get("X"))
    if X.ndim != 2 or X.shape[0] == 0 or X.shape[1] != len(names):
        return None
    ln_prior = reserve.get("log_joint_prior")
    ln_s_prior = reserve.get("log_joint_s_prior")
    if ln_prior is None or ln_s_prior is None:
        return None
    lnL = np.asarray(reserve.get("lnL"), dtype=float).ravel()
    ln_prior = np.asarray(ln_prior, dtype=float).ravel()
    ln_s_prior = np.asarray(ln_s_prior, dtype=float).ravel()
    if not (len(lnL) == len(ln_prior) == len(ln_s_prior) == X.shape[0]):
        return None
    return X[:, names.index(param)].astype(float), lnL + ln_prior - ln_s_prior


def _ess(ln_weights):
    """Kish effective sample size of log weights; 0.0 if none are usable."""
    lw = np.asarray(ln_weights, dtype=float).ravel()
    lw = lw[np.isfinite(lw)]
    if lw.size == 0:
        return 0.0
    p = np.exp(lw - _logsumexp(lw))
    denom = float(np.sum(p ** 2))
    return 1.0 / denom if denom > 0 else 0.0


def distance_grid_resolution_warning(distance, n_grid=None, ln_weights=None,
                                     n_eff_population=None):
    """Describe a distance grid the samples cannot resolve, or return None.

    Pass ``n_eff_population`` when the rows handed in are a bounded subsample of a larger
    retained set (RvsRecord's reserve records it as ``ess_finite``): the subsample's own
    effective sample size describes the subsample, not the pass.

    Two separate ways the samples can fail to support the table that gets written, and
    which one bites depends on where the rows came from:

    * FROM THE FAIR DRAW.  ``_rvs`` after integrate_log holds a few rows resampled WITH
      REPLACEMENT, so the grid cannot carry more rows than there are DISTINCT distances
      in it, however many were asked for.
    * FROM THE RETAINED SET.  Thousands of distinct distances, so the row count looks
      healthy -- but on a starved pass nearly all the weight sits on a handful of them.
      Pass ``ln_weights`` and the effective sample size is checked against the bin
      count: below one effective sample per bin the per-bin lnL is not estimated at all.

    The second check is why the first is not enough on its own.  Reading the retained
    set instead of the export resample removes the duplicate-distance failure and would
    otherwise turn a visibly broken table into a smooth-looking one at the same n_eff.
    """
    d = np.asarray(distance, dtype=float).ravel()
    keep = np.isfinite(d)
    lw = None
    if ln_weights is not None:
        lw = np.asarray(ln_weights, dtype=float).ravel()
        if len(lw) != len(d):
            lw = None
    if lw is not None:
        # THE SAME ROWS THE BUILDER WILL BIN.  _weighted_blocks drops every row whose
        # normalized probability is not strictly positive, so counting distinct distances
        # over all FINITE rows describes a different sample set than the one that gets
        # binned -- and the divergence is not symmetric: a pass where one weight survives
        # and the rest underflow is the genuinely starved one, and it was the silent one.
        finite_w = np.isfinite(lw)
        if not np.any(finite_w):
            return "no finite importance weight survived: there is nothing to bin"
        # `p > 0` subsumes isfinite(lw): a -inf weight gives p == 0 and a NaN gives NaN,
        # and both fail this test.  Masking on isfinite(lw) as well was dead belt-and-braces
        # -- it could be deleted with every test still green, which is how it was found.
        p = np.exp(lw - _logsumexp(lw[finite_w]))
        keep = keep & np.isfinite(p) & (p > 0)
    d = d[keep]
    if len(d) == 0:
        return "no finite positive-weight distance samples to export"
    n_unique = len(np.unique(d))
    n_requested = min(_as_positive_integer(n_grid, len(d)), len(d))
    n_rows = min(n_requested, n_unique)
    problems = []
    if n_unique < n_requested:
        # A FACT ABOUT THE TABLE, not a verdict on it.  Four usable samples over three
        # distinct distances gives a perfectly sound three-row grid; whether three rows
        # is a curve is the effective-sample-size question below, and saying so here too
        # made one duplicate in the default five-row draw print the strongest wording
        # available.
        problems.append(
            "{} usable sample(s) span only {} distinct distance(s), so the grid carries "
            "{} row(s), not the {} requested.".format(
                len(d), n_unique, n_rows, n_requested))
    if lw is not None or n_eff_population is not None:
        # THE POPULATION'S ESS WHEN THE CALLER HAS IT.  When the rows are a bounded uniform
        # subsample of the retained set, the subsample's own ESS is not an estimate of the
        # population's: drop the one row that carries the weight and what is left looks
        # healthy (measured 505 against a population 2.1).  The reserve records the exact
        # pre-cap value for exactly this reason; use it when it is there.
        n_eff = float(n_eff_population) if n_eff_population is not None else _ess(lw[keep])
        if n_eff < n_rows:
            problems.append(
                "the weights carry n_eff={:.1f} across {} bin(s), fewer than one "
                "effective sample per bin: the exported curve is resampled points, not a "
                "resolved likelihood-vs-distance curve.".format(n_eff, n_rows))
    return " ".join(problems) if problems else None


def build_distance_grid(distance, ln_weights, lnL_marginal, sigmaL, params,
                        ln_prior_d_at_samples, n_grid=None):
    """Build a likelihood-vs-distance grid from weighted ILE samples.

    Parameters
    ----------
    distance : array
        Per-sample luminosity distances drawn by the ILE sampler.
    ln_weights : array
        Per-sample log importance weights, ``log L_i + log pi(theta_i) - log q(theta_i)``,
        with ``pi`` and ``q`` being the joint prior and proposal used by ILE.
        These weights include the distance prior.
    lnL_marginal : float
        The marginalized lnL the ILE batchmode would report (``log_res +
        manual_avoid_overflow_logarithm``).  Used as the absolute calibration.
    sigmaL : float
        ILE's reported lnL uncertainty.  Carried verbatim into the grid.
    params : dict
        Intrinsic parameters to broadcast across the grid rows (mass, spins,
        tides, ...).  Missing keys default to 0.
    ln_prior_d_at_samples : array
        Per-sample log of the *distance* prior pi_d(d_i) used by ILE.  This
        is divided out so the exported ``lnL`` is a pure likelihood, not a
        density-times-prior.
    n_grid : int, optional
        Number of grid bins.  Defaults to, and is capped at, the number of
        DISTINCT finite positive-weight distances -- two bins built from the same
        distance would share a centre and leave no width between them.  (Bin
        widths themselves are the midpoints between adjacent centres, with the two
        end bins extrapolated, so an individual width is not the local sample
        spacing.)
    """
    ln_weights = np.asarray(ln_weights, dtype=float)
    ln_norm = _logsumexp(ln_weights)
    probability = np.exp(ln_weights - ln_norm)
    grid_dist, grid_mass, grid_width, grid_ln_prior = _weighted_blocks(
        distance, ln_prior_d_at_samples, probability, n_grid)

    dtype = [(name, float) for name in DISTANCE_GRID_FIELDS]
    grid = np.zeros(len(grid_dist), dtype=dtype)
    # Pure likelihood density in d: subtract log mean prior_d in bin so
    # exp(lnL) = L_marg * p_post(d) / pi_d(d) = L(d) [extrinsic-marginalized].
    grid["lnL"] = (
        lnL_marginal + np.log(grid_mass) - np.log(grid_width) - grid_ln_prior
    )
    grid["sigmaL"] = sigmaL
    grid["dist"] = grid_dist
    grid["dist_weight"] = grid_width
    grid["ln_prior_d_sampling"] = grid_ln_prior

    for name in DISTANCE_GRID_FIELDS:
        if name in {"lnL", "sigmaL", "dist", "dist_weight", "ln_prior_d_sampling"}:
            continue
        grid[name] = float(params.get(name, 0.0))
    return grid


def save_distance_grid(fname, grid):
    header = " ".join(grid.dtype.names)
    np.savetxt(fname, np.column_stack([grid[name] for name in grid.dtype.names]), header=header)


def load_distance_grid(fname):
    return np.genfromtxt(fname, names=True)


def reconstruct_marginal_lnL(grid, ln_prior_d=None):
    """Reconstruct the marginal lnL by integrating exp(lnL)*prior(d) over the
    grid.  If ``ln_prior_d`` is None and the grid has the ``ln_prior_d_sampling``
    column, that column (the sampling prior) is used.  Otherwise integrates
    against a flat prior (treats lnL as already-pure).  Pass a callable
    ``ln_prior_d(d)`` to integrate against a custom distance prior.
    """
    names = grid.dtype.names
    if "dist_weight" not in names:
        # legacy grids without dist_weight: trapezoidal
        order = np.argsort(grid["dist"])
        trap = np.trapezoid if hasattr(np, "trapezoid") else np.trapz
        return np.log(trap(np.exp(grid["lnL"][order]), grid["dist"][order]))

    log_dw = np.log(grid["dist_weight"])
    if ln_prior_d is not None:
        ln_pi = np.asarray(ln_prior_d(grid["dist"]), dtype=float)
        return _logsumexp(grid["lnL"] + ln_pi + log_dw)
    if "ln_prior_d_sampling" in names:
        return _logsumexp(grid["lnL"] + grid["ln_prior_d_sampling"] + log_dw)
    # legacy grids with dist_weight but no separate prior column: treat lnL
    # as a pre-multiplied density (old format)
    return _logsumexp(grid["lnL"] + log_dw)
