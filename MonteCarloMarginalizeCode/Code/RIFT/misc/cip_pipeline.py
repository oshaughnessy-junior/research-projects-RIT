"""Helpers for the CIP posterior export draw and iteration-specific CIP arguments.

The CIP posterior export draws indices from the weighted sample cache.  A
weighted numpy draw without replacement is *successive sampling* (indices
returned in draw order, head enriched in high-weight points), which biased the
export at any output size once the head was truncated.  The replacement here is
systematic (stratified) resampling on the weight CDF: expected counts are
exactly N*p_i at any N, duplication is the minimum possible, and the draw is
duplicate-free whenever N <= sum(w)/max(w).  That sum/max bound (not the Kish
ESS) is the exact frontier of the fair-AND-unique region: no fair draw of size
N > sum(w)/max(w) can avoid duplicates.
"""

import numpy as np

POSTERIOR_UNIQUE_FLAG = "--posterior-unique-draw"


def worker_partition(n_points, group_size, clamp_last=False):
    """Split *n_points* indices into worker batches of at most *group_size*.

    Returns a list of ``(start, count)`` pairs.  Three places in RIFT decide
    how many workers an iteration gets and which slice each one takes, and
    they must agree: the ILE fan-out and the terminal extrinsic fan-out in
    ``create_event_parameter_pipeline_BasicIteration``, and the Hyperpipe
    terminal fan-out assembled in ``util_RIFT_pseudo_pipe.py``.

    **Correction (2026-08-25).** An earlier version of this docstring said the
    line replaced in ``create_event_parameter_pipeline_BasicIteration`` was
    ``int(n/g)`` and that it under-allocated workers in production.  It was
    not: that call site already read ``int(np.ceil(n/g))``, which is exactly
    equivalent to this function, verified over ``n in [0,200) x g in [1,40)``
    with zero mismatches.  Converting it changed nothing, which is the right
    outcome for a refactor but is not what the docstring claimed.

    The buggy form is real, and it is elsewhere: ``int(n/g)`` guarded by ``if
    indx_max*n < g: indx_max += 1`` still stands in
    ``create_event_parameter_pipeline_AlternateIteration``,
    ``create_event_parameter_pipeline_BasicMultiApproxIteration`` and
    ``create_event_nr_pipeline_with_cip``.  That guard fires only when ``n <
    g`` (where ``int(n/g)`` is 0).  For ``n > g`` with a remainder, ``indx_max
    >= 1`` and ``indx_max*n >= n >= g``, so it never fires and the request
    allocates too FEW workers, leaving the tail of the requested points
    unevaluated with no error and no log line.  **None of those three builders
    is converted here**, so the "places that must agree" still do not all
    agree -- converting them is a separate change with its own blast radius,
    and claiming otherwise would suggest a coverage this module does not have.

    ``clamp_last`` controls the tail:

    * ``False`` (the ILE fan-out): every batch is a full ``group_size``, so the
      last one can ask for indices past the end.  ILE tolerates that -- it
      stops at the end of the grid -- and the uniform ``macrongroup`` is what
      the historical DAG emitted.  Kept as-is deliberately: changing it would
      change the shape of every production DAG.
    * ``True`` (the extrinsic fan-outs): the last batch is truncated so the
      total is exactly ``n_points``.  Here the count IS the deliverable --
      it is how many posterior samples the run produces -- so over-requesting
      is not free.
    """
    n_points = int(n_points)
    group_size = int(group_size)
    if group_size <= 0:
        raise ValueError("group_size must be positive, got {}".format(group_size))
    if n_points <= 0:
        return []
    n_workers = -(-n_points // group_size)   # ceil, without float rounding
    batches = []
    for index in range(n_workers):
        start = index * group_size
        count = group_size
        if clamp_last:
            count = min(group_size, n_points - start)
        batches.append((start, count))
    return batches


def expand_argument_schedule(lines, n_iterations, allow_special=False,
                             include_prefix=False):
    """Expand a grouped CIP/posterior argument schedule by iteration.

    Each non-empty line begins with an integer repeat count.  The legacy event
    pipeline also accepts ``G<count>`` (alternate Gaussian-resampling
    executable) and ``Z`` (run-to-convergence subdag).  Callers that cannot
    reproduce those execution semantics must leave ``allow_special`` false;
    the parser then fails explicitly instead of silently changing an analysis.

    If the schedule is shorter than ``n_iterations``, its final ordinary group
    is extended to cover the requested iterations. Longer schedules are
    truncated to the requested iteration count.
    """
    n_iterations = int(n_iterations)
    if n_iterations <= 0:
        raise ValueError("n_iterations must be positive")
    groups = []
    for raw in lines:
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        words = raw.split()
        prefix = words[0]
        args = " ".join(words[1:]).strip()
        if prefix == "Z" or prefix.startswith("G"):
            if not allow_special:
                raise ValueError(
                    "special CIP schedule prefix {!r} is not supported by this pipeline writer".format(prefix))
            repeat = 1 if prefix == "Z" else int(prefix[1:])
        else:
            try:
                repeat = int(prefix)
            except ValueError:
                raise ValueError("invalid CIP schedule prefix {!r}".format(prefix))
        if repeat <= 0:
            raise ValueError("CIP schedule repeat must be positive: {!r}".format(prefix))
        groups.append((prefix, repeat, args))
    if not groups:
        raise ValueError("CIP argument schedule is empty")

    expanded = []
    for prefix, repeat, args in groups:
        expanded.extend([(prefix, args)] * repeat)
    if len(expanded) < n_iterations:
        prefix = groups[-1][0]
        if prefix == "Z" or prefix.startswith("G"):
            raise ValueError("cannot extend a special final CIP schedule group")
        expanded.extend(
            [(groups[-1][0], groups[-1][2])] *
            (n_iterations - len(expanded)))
    expanded = expanded[:n_iterations]
    if include_prefix:
        return expanded
    return [args for _prefix, args in expanded]


def _validated_scaled_weights(weights):
    """Validate weights and return them scaled by their maximum, in the input dtype.

    Scaling by the maximum first matters: RIFT builds export weights in
    extended precision (longdouble on x86_64), where finite values can exceed
    float64's range, so casting to float64 before normalizing would overflow
    them to inf.  After max-scaling, every value lies in [0, 1].
    """
    w = np.asarray(weights)
    if w.ndim != 1 or len(w) == 0:
        raise ValueError("weights must be a nonempty 1-d array")
    if not np.all(np.isfinite(w)) or np.any(w < 0):
        raise ValueError("weights must be finite and nonnegative")
    w_max = np.max(w)
    if not (w_max > 0):
        raise ValueError("weights must have a positive sum")
    return w / w_max


def _normalized_probabilities(weights):
    """Validate weights and return them as float64 probabilities."""
    w = _validated_scaled_weights(weights)
    return np.asarray(w / np.sum(w), dtype=float)


def unique_draw_bound(weights):
    """Largest fair draw size that can be duplicate-free: floor(sum(w)/max(w)).

    Computed as floor(sum(w/max(w))) directly in the input dtype: taking a
    float64 reciprocal of the normalized maximum instead would round the exact
    bound down by one whenever roundoff lands just below an integer (93 equal
    weights -> 92).
    """
    return int(np.floor(np.sum(_validated_scaled_weights(weights))))


def systematic_resample(weights, n_out, rng=None):
    """Systematic (stratified) resample: n_out indices drawn ~ weights.

    Expected counts are exactly n_out*w_i/sum(w) for every i, at any n_out
    (unlike weighted choice(replace=False)), and each count is at most
    ceil(n_out*w_i/sum(w)) so the draw has no duplicates when
    n_out <= sum(w)/max(w).  The returned order is shuffled, so any
    contiguous truncation of the result is itself a fair draw.

    rng defaults to the legacy global numpy generator, matching the rest of
    CIP (and old numpy on clusters without default_rng).
    """
    if rng is None:
        rng = np.random
    cdf = np.cumsum(_normalized_probabilities(weights))
    cdf[-1] = 1.0  # guard against roundoff excluding the final bin
    positions = (rng.uniform() + np.arange(n_out)) / n_out
    indx = np.searchsorted(cdf, positions, side='left')
    rng.shuffle(indx)
    return indx


def flag_final_group_unique(lines):
    """Add the unique-draw flag to the final CIP argument-group line.

    CIP argument files group repeated iterations by prefixing each line with a
    count, ``G<count>`` (Gaussian-resampling executable), or ``Z`` (terminal
    run-to-convergence subdag).  Internal iterations keep CIP's default draw
    (fair, duplicates possible); only the final group -- the product consumed
    downstream -- gets the unique-draw cap.  The flag goes on the whole final
    group, not just its last iteration, so a convergence-test abort partway
    through the group still publishes a unique fair draw.

    A final ``G`` line is left untouched: the Gaussian-resampling executable
    does not accept the flag (strict argparse), so flagging it would kill the
    job.  Callers should avoid ending the schedule with a G group if they need
    the uniqueness guarantee.
    """
    lines = [line.rstrip() for line in lines if line.strip()]
    if not lines:
        return []
    final_line = lines[-1]
    prefix = final_line.split()[0]
    if prefix.startswith("G") or POSTERIOR_UNIQUE_FLAG in final_line.split():
        return lines
    lines[-1] = "{} {}".format(final_line, POSTERIOR_UNIQUE_FLAG)
    return lines
