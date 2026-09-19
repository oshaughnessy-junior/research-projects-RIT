#!/usr/bin/env python
"""
Regression tests for the adaptive-volume POST-FREEZE GRID RUNAWAY
(RIFT/integrators/mcsamplerAdaptiveVolume.py).

Background (the defect these tests lock down).  ``integrate_log`` contracts the
live volume by raising a likelihood threshold, spending a truncation budget
``trunc_p`` as it goes.  Once that budget is spent, ``at_final_threshold``
becomes True and STAYS True: the threshold never moves again, every drawn
sample is kept, so ``nrec == ninj`` and ``V *= nrec/ninj`` is a no-op -- the
fractional live volume ``V`` is constant from then on.

The bin grid, however, kept being recomputed from

    delta_V = V / sqrt(nrec)          ->   prod(nbins) = 1/delta_V

and ``nrec`` keeps growing simply because samples accumulate.  So the grid keeps
refining after the threshold has frozen, more distinct bins become occupied, and

    ninbin = n_chunk // n_occupied_bins + 1

floors to one draw per bin as soon as ``n_occupied_bins > n_chunk``.  From that
point the per-cycle block IS the occupied-bin count and grows without bound --
positive feedback (more samples -> finer grid -> bigger block -> more samples)
whose only ceiling is ``nmax``.  In production this reached a block of 149,543,
which the CPU ILE likelihood turns into ~67.6 GB of host allocation.

The in-source comment on that block claimed it "asymptotically becomes
stationary".  It does not, and these tests are the counter-example.

WHAT IS ASSERTED.  Not a wall-clock or a memory number -- those depend on the
machine.  The invariant: once the threshold has frozen, the sampler's grid, and
therefore the block it draws each cycle, must stop changing, even while the live
set keeps growing.

THE TARGET.  A thin curved shell in 5-D, not a compact blob.  That geometry
matters: the number of occupied bins grows with resolution like the surface
area, which is what drives ``n_occupied`` past ``n_chunk``.  A round Gaussian
saturates instead and does not reproduce the defect.
"""

import contextlib
import io

import numpy as np
import pytest

from RIFT.integrators import mcsamplerAdaptiveVolume as mav

NDIM = 5
LO, HI = -6.0, 6.0
R0, W_SHELL, W_BLOB = 3.0, 0.05, 0.4
PARAMS = ["x%d" % j for j in range(NDIM)]


def shell_lnL(*args):
    """Thin spherical shell in the first three axes x a narrow blob in the rest."""
    X = np.column_stack(args)
    r = np.sqrt(np.sum(X[:, :3] ** 2, axis=1))
    return (-0.5 * ((r - R0) / W_SHELL) ** 2
            - 0.5 * np.sum((X[:, 3:] / W_BLOB) ** 2, axis=1))


class _TracingSampler(mav.MCSampler):
    """Records, per cycle, the grid the sampler is about to draw from.

    ``_allocate_nbins`` is the natural probe: it is the ONLY place the bin
    counts are recomputed from ``delta_V``, so counting its calls separates
    "the grid was recomputed this cycle" from "the frozen grid was reused".
    """

    def _allocate_nbins(self, live_pts, delta_V, ndim):
        nb = mav.MCSampler._allocate_nbins(self, live_pts, delta_V, ndim)
        self.trace.append(dict(nrec=int(len(live_pts)), delta_V=float(delta_V),
                               recomputed=True))
        return nb

    def draw_simple(self):
        # called once per cycle, AFTER the grid for that cycle is settled
        self.cycles.append(dict(block=int(np.sum(self.ninbin)),
                                occ=int(self.binunique.shape[0]),
                                nbins=np.array(self.nbins, dtype=float).copy(),
                                n_recomputes=len(self.trace)))
        return mav.MCSampler.draw_simple(self)


def _run(seed=42, nmax=400000, neff=10 ** 6, nchunk=400):
    """Integrate the shell target.  neff is set unreachably high on purpose:
    we want the run to spend its whole budget cycling past the threshold
    freeze, which is the regime under test."""
    np.random.seed(seed)
    s = _TracingSampler()
    for p in PARAMS:
        s.add_parameter(
            p,
            pdf=lambda x: np.ones(len(np.atleast_1d(x))) / (HI - LO),
            prior_pdf=lambda x: np.ones(len(np.atleast_1d(x))) / (HI - LO),
            left_limit=LO, right_limit=HI, adaptive_sampling=True)
    s.trace = []
    s.cycles = []
    with contextlib.redirect_stdout(io.StringIO()):
        res = s.integrate_log(shell_lnL, *PARAMS, nmax=nmax, neff=neff,
                              n=nchunk, no_protect_names=True, verbose=False)
    return s, res


@pytest.fixture(scope="module")
def run_result():
    return _run()


def _frozen_tail(s):
    """Cycles from the first one that REUSED the grid (no _allocate_nbins call)
    to the end.  Empty if the grid was recomputed every cycle."""
    tail = []
    for i in range(1, len(s.cycles)):
        if s.cycles[i]["n_recomputes"] == s.cycles[i - 1]["n_recomputes"]:
            tail.append(i)
    if not tail:
        return []
    return list(range(tail[0], len(s.cycles)))


def test_reaches_the_regime_under_test(run_result):
    """Guard against a vacuous pass: the run must actually get past the
    threshold freeze and then keep cycling for a long time."""
    s, _ = run_result
    assert len(s.cycles) > 100, (
        "the target did not produce enough cycles to exercise the post-freeze "
        "regime (got {})".format(len(s.cycles)))
    tail = _frozen_tail(s)
    assert len(tail) >= 50, (
        "the grid was still being recomputed on essentially every cycle "
        "({} reused of {} cycles): either the threshold never froze (so this "
        "test proves nothing) or the grid freeze is not in effect".format(
            len(tail), len(s.cycles)))


def test_live_set_keeps_growing_after_the_freeze(run_result):
    """The freeze must not be an artefact of the run having gone quiet: the
    quantity that USED to drive the runaway (nrec) has to still be growing."""
    s, _ = run_result
    tail = _frozen_tail(s)
    assert tail, "no frozen tail; see test_reaches_the_regime_under_test"
    nrec = [t["nrec"] for t in s.trace]
    assert nrec[-1] > 5 * nrec[len(nrec) // 4], (
        "the live set stopped growing ({} -> {}), so a constant block would "
        "prove nothing".format(nrec[len(nrec) // 4], nrec[-1]))


def test_block_stops_growing_after_the_freeze(run_result):
    """THE defect.  Before the fix the per-cycle block climbs monotonically
    with nrec (measured on this target: 400 -> 1220 and still rising when the
    budget ran out, with no ceiling but nmax)."""
    s, _ = run_result
    tail = _frozen_tail(s)
    assert tail, "no frozen tail; see test_reaches_the_regime_under_test"
    blocks = np.array([s.cycles[i]["block"] for i in tail])
    assert blocks.max() == blocks.min(), (
        "the per-cycle block still moves after the threshold freeze: "
        "{} -> {} over {} cycles".format(blocks[0], blocks[-1], len(blocks)))


def test_grid_is_identical_across_the_frozen_tail(run_result):
    """Stronger than the block test: the bin counts themselves, and hence the
    occupied-bin set the estimator's volume bookkeeping refers to, must be
    unchanged."""
    s, _ = run_result
    tail = _frozen_tail(s)
    assert tail, "no frozen tail; see test_reaches_the_regime_under_test"
    first = s.cycles[tail[0]]["nbins"]
    for i in tail:
        assert np.array_equal(s.cycles[i]["nbins"], first), (
            "nbins changed after the freeze: {} -> {}".format(
                first, s.cycles[i]["nbins"]))
    occ = np.array([s.cycles[i]["occ"] for i in tail])
    assert occ.max() == occ.min(), (
        "the occupied-bin count moved after the freeze: {} -> {}".format(
            occ[0], occ[-1]))


def test_freeze_state_is_reset_between_integrations(run_result):
    """A frozen grid must not leak into the next integrate_log call: setup()
    is what clears it, and setup() runs at the top of every integration."""
    s, _ = run_result
    assert s._nbins_at_threshold_freeze is not None, (
        "this run never froze; the reset test below would be vacuous")
    s.setup()
    assert s._nbins_at_threshold_freeze is None
    assert s._dx_at_threshold_freeze is None


def test_integral_is_still_sane(run_result):
    """Not a precision claim -- a sanity floor.  The estimate must remain a
    finite number of the right order for this target."""
    s, res = run_result
    lnI = float(res[0])
    assert np.isfinite(lnI)
    # crude bracket: the shell's mass is O(1e-4) of the box, and the AV
    # estimate for this target sits near -9; anything outside this window means
    # the freeze broke the estimator, not merely the schedule.
    assert -20.0 < lnI < 0.0, "lnI = {}".format(lnI)
