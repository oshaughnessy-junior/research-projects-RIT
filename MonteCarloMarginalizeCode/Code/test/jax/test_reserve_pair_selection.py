"""The (local, reserve) pair is CHOSEN from analysis, before any row is evaluated.

RO, 2026-09-08: rely on analysis and the known physics to pick the pair, rather
than try-then-decline-then-refine.  Everything the selector uses is computable
from the precomputed inputs, so the choice and its reasons are printed in the
run's first lines instead of being discovered from a ledger at the end.

The tests below pin the RULE, not a run.  The most important one is
`test_the_predictor_reproduces_the_measured_refine4_failure`: an independent
session MEASURED the whole-window reserve failing its own convergence warrant at
the lowest rung, and the predictor says the same thing from the physics alone.
"""

import numpy as np
import pytest

from RIFT.likelihood.jax_ile import direct_marginalization_policy as DP
from RIFT.likelihood.jax_ile.anglemarg import ANGLE_MARG_CROSSOVER_AMPLITUDE

from test_angle_marg_exact import make_synth

# Ladder-2 network SNRs.
RHO_40, RHO_160, RHO_320, RHO_640 = 40.7691, 163.0766, 326.1531, 652.3062


@pytest.fixture(scope="module")
def data():
    return make_synth(scale=1.0, npts=614)


def _pick(data, rho, refine_max=32, **kw):
    return DP.predict_reserve_pair(
        data, rho, reserve_time_refine_max=refine_max,
        crossover_amplitude=ANGLE_MARG_CROSSOVER_AMPLITUDE, **kw)


def test_the_bandwidth_comes_from_the_complex_Q_spectrum(data):
    """Q (rholm) is complex.  A real-input transform rejects it outright, and
    taking only the real part would discard half the phase structure, so this
    pins that the second moment is finite, positive and Nyquist-bounded."""
    sigma_f = DP.q_effective_bandwidth_hz(data)
    assert np.isfinite(sigma_f) and sigma_f > 0.0
    f_nyq = 0.5 / (data.deltaT / data.q_time_pregrid_factor)
    assert sigma_f < f_nyq


def test_the_angle_scheme_follows_the_validated_crossover(data):
    """A = rho^2/2 against ANGLE_MARG_CROSSOVER_AMPLITUDE, which is a measured
    accuracy crossover in anglemarg, not a tuning constant introduced here."""
    lo, _ = _pick(data, 1.0)                      # A = 0.5, far below 450
    assert lo == "exact"
    hi, info = _pick(data, RHO_40)                # A = 831, above 450
    assert hi == "laplace"
    assert info["amplitude_A"] > info["crossover_amplitude"]


def test_a_peak_the_ceiling_cannot_resolve_is_refused_not_refined(data):
    """The whole point.  When the escalation ceiling cannot resolve the peak and
    no peak-local time reserve is available, the selector returns None so the
    caller REFUSES.  Silently falling back to whole-window refinement is the
    failure mode this exists to prevent."""
    scheme, info = _pick(data, RHO_640)
    assert scheme is None
    assert not info["time_peak_resolvable_whole_window"]
    assert info["whole_window_nodes_needed"] > info["whole_window_nodes_available"]
    assert "NOT IMPLEMENTED" in info["reason"]


def test_the_same_signal_selects_peaklocal_once_it_exists(data):
    """The refusal is about availability, not about the signal.  With a
    peak-local time reserve on the menu the identical inputs select it, so the
    day that kernel lands the selector starts choosing it with no rule change."""
    scheme, info = _pick(data, RHO_640, available=("exact", "laplace", "peaklocal"))
    assert scheme == "peaklocal"
    assert "peak-local" in info["reason"]


def test_the_predictor_reproduces_the_measured_refine4_failure(data):
    """Independent confirmation, and the reason to trust the rule at all.

    The reserve-scheme session MEASURED the whole-window refined reserve at
    refine=4 failing its OWN half-refined convergence warrant at the LOWEST
    ladder rung, rho 40.77: 0.0018, 0.0040 and 0.0114 nats on three rows
    against a 1e-3 target.  The predictor is told nothing about that.  From the
    physics alone it says the refine-4 rule affords 2453 nodes on this window
    while the peak needs more, i.e. under-resolved at the bottom of the ladder,
    which is what they saw.  At the ceiling of 32 the same rung is comfortable.
    """
    _, tight = _pick(data, RHO_40, refine_max=4)
    assert not tight["time_peak_resolvable_whole_window"]
    assert tight["whole_window_nodes_available"] == pytest.approx(
        (data.npts - 1) * 4 + 1)

    scheme, loose = _pick(data, RHO_40, refine_max=32)
    assert loose["time_peak_resolvable_whole_window"]
    assert scheme == "laplace"


def test_an_explicit_request_bypasses_the_analysis(data):
    """Overrides stay overrides: 'auto' analyses, anything else is obeyed and
    labelled as such so a run log cannot be misread as an analysed choice."""
    scheme, info = _pick(data, RHO_640, requested="exact")
    assert scheme == "exact"
    assert "explicit request" in info["reason"]


def test_the_pair_line_is_printable_and_names_the_refusal(data):
    scheme, info = _pick(data, RHO_640)
    line = DP.format_reserve_pair(scheme, info)
    assert line.startswith("RESERVE-PAIR local=four-axis reserve=REFUSED")
    for token in ("rho=", "sigma_f=", "A=", "peak=", "nodes_needed="):
        assert token in line
