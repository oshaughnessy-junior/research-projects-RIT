"""Regression tests for the worker fan-out arithmetic (PR #181 items A1/A2).

Why these exist
---------------
PR #181 changed how many workers an iteration gets.  The historical form in
``create_event_parameter_pipeline_BasicIteration`` was::

    indx_max = int(n_points / group_size)
    if indx_max * n_points < group_size:   # essentially never true
        indx_max += 1

The guard fires only when ``n_points < group_size``.  For a request LARGER than
one worker's group that does not divide evenly, it never fires: too few workers
were allocated and the tail of the requested points went unevaluated --
silently, with a zero exit status.  The same shape governed the terminal extrinsic fan-out, where the
count is not an implementation detail but the number of posterior samples the
run produces.

These tests pin the corrected behaviour, and each one is written so that it
FAILS if the historical formula is restored.  ``test_legacy_formula_is_lethal``
makes that explicit rather than leaving it to be believed: it evaluates the old
expression directly and asserts it disagrees, so if someone ever "simplifies"
worker_partition back toward it, this file says why not.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))

from RIFT.misc.cip_pipeline import worker_partition


def _legacy_worker_count(n_points, group_size):
    """The pre-#181 expression, reproduced exactly."""
    indx_max = int((1.0 * n_points) / group_size)
    if indx_max * n_points < group_size:
        indx_max += 1
    return indx_max


# (n_points, group_size) pairs that do NOT divide evenly.  These are the only
# configurations where the defect was observable, which is why a production
# run at a divisible operating point could carry it for years.
INDIVISIBLE = [(9, 4), (300, 200), (500, 300), (17, 5), (999, 250)]
#: The one case the legacy guard DID rescue: fewer points than one worker
#: holds, where ``int(n/g)`` is 0 and the bump fires.  Kept separate because
#: asserting the legacy formula is lethal here would be false -- and a test
#: that asserts something false about the old code is worse than no test.
FEWER_POINTS_THAN_GROUP = [(1, 7), (3, 10)]
DIVISIBLE = [(500, 20), (9, 3), (400, 200), (1000, 250)]


@pytest.mark.parametrize("n_points,group_size",
                         INDIVISIBLE + DIVISIBLE + FEWER_POINTS_THAN_GROUP)
def test_every_requested_point_is_covered(n_points, group_size):
    batches = worker_partition(n_points, group_size)
    covered = set()
    for start, count in batches:
        covered.update(range(start, start + count))
    assert set(range(n_points)) <= covered, (
        "worker fan-out leaves points unevaluated: {} of {} covered"
        .format(len(covered & set(range(n_points))), n_points))


@pytest.mark.parametrize("n_points,group_size",
                         INDIVISIBLE + DIVISIBLE + FEWER_POINTS_THAN_GROUP)
def test_clamped_partition_totals_exactly(n_points, group_size):
    """For the extrinsic fan-out the total IS the deliverable."""
    batches = worker_partition(n_points, group_size, clamp_last=True)
    assert sum(count for _start, count in batches) == n_points
    assert all(count > 0 for _start, count in batches)
    starts = [start for start, _count in batches]
    assert starts == sorted(starts)
    for (start, count), (next_start, _) in zip(batches, batches[1:]):
        assert start + count == next_start, "batches must tile without a gap"


@pytest.mark.parametrize("n_points,group_size",
                         DIVISIBLE + FEWER_POINTS_THAN_GROUP)
def test_divisible_configurations_are_unchanged(n_points, group_size):
    """The fix must be a no-op wherever production already divided evenly.

    This is the test that says a production run at a divisible operating point
    keeps its historical DAG shape -- the claim Papers 2 and 3 depend on.
    """
    assert len(worker_partition(n_points, group_size)) == _legacy_worker_count(
        n_points, group_size)


@pytest.mark.parametrize("n_points,group_size", INDIVISIBLE)
def test_legacy_formula_is_lethal(n_points, group_size):
    """The historical expression really does under-allocate here.

    A regression test that also passes against the buggy code tests nothing.
    """
    legacy = _legacy_worker_count(n_points, group_size)
    fixed = len(worker_partition(n_points, group_size))
    assert fixed > legacy, (
        "expected the legacy formula to under-allocate for {}/{}, but it gave "
        "{} and the fix gives {}".format(n_points, group_size, legacy, fixed))
    legacy_covered = legacy * group_size
    assert legacy_covered < n_points, (
        "legacy allocation covered {} of {} requested points"
        .format(legacy_covered, n_points))


def test_unclamped_tail_may_overrun_and_that_is_deliberate():
    """The ILE fan-out keeps a uniform group size; document the consequence.

    ILE stops at the end of its grid, so the overrun is harmless -- but it is
    a real difference from the clamped extrinsic fan-out, and pinning it here
    stops someone "fixing" one to match the other and silently changing the
    shape of every production DAG.
    """
    batches = worker_partition(9, 4)
    assert batches[-1] == (8, 4)
    assert sum(count for _s, count in batches) == 12 > 9


@pytest.mark.parametrize("n_points,group_size", FEWER_POINTS_THAN_GROUP)
def test_legacy_guard_rescued_the_small_request(n_points, group_size):
    """Document the one case the old code got right, so nobody re-breaks it."""
    assert _legacy_worker_count(n_points, group_size) == 1
    assert len(worker_partition(n_points, group_size)) == 1


def test_rejects_a_nonsense_group_size():
    with pytest.raises(ValueError):
        worker_partition(10, 0)
    assert worker_partition(0, 5) == []
