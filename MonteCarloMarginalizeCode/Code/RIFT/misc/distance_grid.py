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
    """Sort samples by distance, split into n_grid blocks that never cut a run
    of identical distances, and return per-block (center, mass, width, mean
    ln-prior)."""
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
    # increasing, so this should be unreachable; refuse rather than floor.
    if not np.all(width > 0):
        raise ValueError(
            "cannot resolve a distance grid: {} of {} bin(s) have non-positive "
            "width over distances [{:g}, {:g}]".format(
                int(np.sum(width <= 0)), len(width), distance[0], distance[-1]))

    return grid_dist, grid_mass, width, grid_ln_prior


def distance_grid_resolution_warning(distance, n_grid=None):
    """Describe a distance grid the samples cannot resolve, or return None.

    The grid can carry at most one row per DISTINCT sampled distance.  ILE's
    fair draw resamples with replacement and is sized from the achieved n_eff,
    so a starved extrinsic pass (GMM at n_eff of a few) collapses onto fewer
    distinct distances than rows were requested.  What survives is a handful of
    repeated points, not a likelihood-vs-distance curve, and nothing in the
    written file says so.
    """
    d = np.asarray(distance, dtype=float)
    d = d[np.isfinite(d)]
    if len(d) == 0:
        return "no finite distance samples to export"
    n_unique = len(np.unique(d))
    n_requested = min(_as_positive_integer(n_grid, len(d)), len(d))
    if n_unique >= n_requested:
        return None
    return (
        "{} sample(s) span only {} distinct distance(s), {} row(s) requested.  The "
        "exported curve is repeated draws, not a resolved likelihood-vs-distance "
        "curve.".format(len(d), n_unique, n_requested))


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
        DISTINCT finite positive-weight distances: a bin cannot be narrower
        than the spacing of the samples that define it.
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
