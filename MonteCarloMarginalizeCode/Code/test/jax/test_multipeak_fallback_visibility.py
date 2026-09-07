"""A planner fault must not be readable as a conservative policy decline.

``multipeak_local_marginalize`` returns the caller's reserve for two unrelated
reasons.  One is a budget outcome: both tiers ran and a diagnostic failed.  The
other is a fault: something raised and the reserve is standing in for a step
that never ran.  Before the visibility change both looked the same in the log
(silence) and differed in the record only by a substring of ``provenance``, so
a ladder campaign in which every row declined by ``RuntimeError`` was read as a
conservative controller rather than as a defect.

These tests pin the separation itself, not the tier1 defect that exposed it:
the fault warns and the budget decline does not, ``fail_on_fallback`` is fatal
on the first and inert on the second, the record carries a machine-readable
``decline_kind``/``fault``, and the default path is byte-for-byte what it was.
"""

import warnings

import jax
import numpy as np
import pytest

jax.config.update("jax_enable_x64", True)

from RIFT.likelihood.jax_ile import multipeak_planner as planner  # noqa: E402


# The field list MultiPeakResult had before decline_kind/fault were appended.
# New fields are trailing and defaulted, so an existing caller's positional
# unpacking, indexing, and attribute access are all unaffected.  Frozen here so
# an insertion in the middle -- which would silently reorder a caller's tuple --
# fails instead of passing.
_LEGACY_FIELDS = (
    "value", "accepted", "used_reserve", "provenance", "delta_log_integral",
    "tier0", "tier1", "tier0_portfolio", "tier1_portfolio",
    "total_lattice_evaluations", "total_refinement_steps",
    "total_local_evaluations", "modeled_peak_bytes",
)

# The two provenance strings the pre-change module produced.  Downstream
# analysis that greps them must keep working; the new record fields are an
# addition, not a replacement.
_PROVENANCE_ACCEPTED = "uvq-multipeak-tier1"
_PROVENANCE_BUDGET = "dense-reserve:enrichment-or-local-diagnostic"
_PROVENANCE_FAULT_PREFIX = "dense-reserve:planner-exception:"


def _synthetic_tables(n_time=9):
    """Small reflected-polynomial problem with an interior four-axis peak."""
    time = np.arange(n_time, dtype=float)
    C_A = np.zeros((3, 3, n_time), dtype=np.complex128)
    C_B = np.zeros((5, 5), dtype=np.complex128)
    C_A[0, 1] = 20.0 - 2.0 * np.cos(2.0 * np.pi * time / (n_time - 1))
    C_A[2, 0] = 0.25
    C_A[2, 2] = 0.25
    C_B[0, 2] = 4.0
    C_B[2, 1] = 0.02
    C_B[2, 3] = 0.02
    return C_A, C_B


# Settings that accept the local branch, and settings whose only difference is
# an unreachable agreement budget, so the same tables decline by budget.  Both
# rows run both tiers to completion; neither raises.
_ACCEPT_KWARGS = dict(
    log_integral_tol=0.1, tier0=(2, 2, 24), tier1=(3, 3, 48),
    quadrature_order=7, cell_sigma=4.0, chunk_size=32)
_BUDGET_KWARGS = dict(
    log_integral_tol=1.0e-12, tier0=(2, 2, 24), tier1=(3, 3, 48),
    quadrature_order=5, cell_sigma=4.0, chunk_size=32)


# Captured once, before any test patches it.
_REAL_RUN_STRUCTURAL_TIER = planner._run_structural_tier


class _CountingReserve(object):
    """A finite reserve that records whether the planner actually paid for it."""

    def __init__(self, value=123.456):
        self.value = float(value)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.value


def _fault_at(monkeypatch, which, message="deliberate tier fault"):
    """Make the ``which``-th structural tier raise, and the others run for real.

    ``which=2`` reproduces the campaign's shape exactly: tier0 converges, tier1
    raises, and the row falls back.  The failure is injected at the tier seam
    rather than by feeding bad tables, so tier0's report is real and the stage
    the planner names can be checked against a known answer.
    """
    state = {"n": 0}

    def wrapper(*args, **kwargs):
        state["n"] += 1
        if state["n"] == int(which):
            raise RuntimeError(message)
        # Always the pristine function, so patching twice in one test does not
        # stack wrappers and fire on the wrong call.
        return _REAL_RUN_STRUCTURAL_TIER(*args, **kwargs)

    monkeypatch.setattr(planner, "_run_structural_tier", wrapper)
    return state


def test_fault_warns_once_and_budget_decline_stays_silent():
    C_A, C_B = _synthetic_tables()

    # A budget decline is a normal outcome.  It must not warn, or a campaign
    # that declines legitimately drowns the faults it is supposed to surface.
    reserve = _CountingReserve()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        declined = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, reserve, **_BUDGET_KWARGS)
    assert declined.used_reserve and not declined.accepted
    assert declined.decline_kind == planner.DECLINE_DIAGNOSTIC
    assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []

    # An accepted row must not warn either.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        accepted = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, _CountingReserve(), **_ACCEPT_KWARGS)
    assert accepted.accepted and accepted.decline_kind is None
    assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []


def test_fault_warns_with_stage_exception_and_label(monkeypatch):
    C_A, C_B = _synthetic_tables()
    _fault_at(monkeypatch, 2, message="tier1 refinement is degenerate")
    reserve = _CountingReserve()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, reserve, label="ladder-row-7",
            **_ACCEPT_KWARGS)
    runtime = [w for w in caught if issubclass(w.category, RuntimeWarning)]

    # Exactly one warning per CALL, not per Newton step: a 40-row campaign gets
    # 40 lines, which is readable; per-step would not be.
    assert len(runtime) == 1
    text = str(runtime[0].message)
    assert "tier1" in text
    assert "RuntimeError" in text
    assert "tier1 refinement is degenerate" in text
    assert "ladder-row-7" in text
    assert "fault" in text.lower()
    assert result.used_reserve and result.value == reserve.value


def test_record_separates_fault_from_budget_without_parsing_provenance(
        monkeypatch):
    C_A, C_B = _synthetic_tables()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        budget = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, _CountingReserve(), **_BUDGET_KWARGS)
        _fault_at(monkeypatch, 2, message="tier1 refinement is degenerate")
        fault = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, _CountingReserve(), **_ACCEPT_KWARGS)

    # Both used the reserve; only the second is a defect.  The distinction is a
    # field comparison, not a substring search on provenance.
    assert budget.used_reserve and fault.used_reserve
    assert budget.decline_kind == planner.DECLINE_DIAGNOSTIC
    assert fault.decline_kind == planner.DECLINE_FAULT
    assert planner.DECLINE_DIAGNOSTIC != planner.DECLINE_FAULT
    assert budget.fault is None
    assert isinstance(fault.fault, planner.FallbackFault)
    assert fault.fault.stage == "tier1"
    assert fault.fault.error_type == "RuntimeError"
    assert fault.fault.message == "tier1 refinement is degenerate"


def test_fault_stage_names_the_step_that_raised(monkeypatch):
    """The stage is measured, not assumed: tier0 and tier1 report differently."""
    C_A, C_B = _synthetic_tables()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        _fault_at(monkeypatch, 1)
        first = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, _CountingReserve(), **_ACCEPT_KWARGS)
        _fault_at(monkeypatch, 2)
        second = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, _CountingReserve(), **_ACCEPT_KWARGS)
    assert first.fault.stage == "tier0"
    assert second.fault.stage == "tier1"

    # A fault before either tier runs is attributed to its own stage, so a bad
    # table is never reported as a tier defect.
    repeated = np.repeat(C_B[..., None], 7, axis=-1)
    repeated[1, 1, 3] += 1.0e-3
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        early = planner.multipeak_local_marginalize(
            C_A, repeated, 1.0, 8.0, _CountingReserve(), **_ACCEPT_KWARGS)
    assert early.decline_kind == planner.DECLINE_FAULT
    assert early.fault.stage == "uv-summary"
    assert early.fault.error_type == "ValueError"


def test_fail_on_fallback_raises_on_fault_and_is_inert_on_budget_decline(
        monkeypatch):
    C_A, C_B = _synthetic_tables()

    # Inert on the budget decline: same value, same record, no exception.
    reserve = _CountingReserve()
    declined = planner.multipeak_local_marginalize(
        C_A, C_B, 1.0, 8.0, reserve, fail_on_fallback=True, **_BUDGET_KWARGS)
    assert declined.used_reserve and declined.value == reserve.value
    assert declined.decline_kind == planner.DECLINE_DIAGNOSTIC
    assert reserve.calls == 1

    # Inert on an accepted row.
    accepted = planner.multipeak_local_marginalize(
        C_A, C_B, 1.0, 8.0, _CountingReserve(), fail_on_fallback=True,
        **_ACCEPT_KWARGS)
    assert accepted.accepted and accepted.provenance == _PROVENANCE_ACCEPTED

    # Fatal on the fault, and it does not pay for the reserve first: the point
    # is to stop, not to produce a value nobody should trust.
    _fault_at(monkeypatch, 2, message="tier1 refinement is degenerate")
    fatal_reserve = _CountingReserve()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        with pytest.raises(planner.MultiPeakFallbackError) as excinfo:
            planner.multipeak_local_marginalize(
                C_A, C_B, 1.0, 8.0, fatal_reserve, fail_on_fallback=True,
                label="ladder-row-7", **_ACCEPT_KWARGS)
    assert fatal_reserve.calls == 0
    assert "tier1" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, RuntimeError)

    # MultiPeakFallbackError sits outside the set the planner catches, so a
    # nested or repeated call cannot swallow it back into a decline.
    assert not isinstance(
        excinfo.value, (RuntimeError, ValueError, np.linalg.LinAlgError))


def test_default_path_is_unchanged(monkeypatch):
    """Same values, same provenance, same legacy record shape, no exception."""
    C_A, C_B = _synthetic_tables()
    assert planner.MultiPeakResult._fields[:len(_LEGACY_FIELDS)] \
        == _LEGACY_FIELDS

    # A caller built on the pre-change arity still constructs the record.
    legacy = planner.MultiPeakResult(*range(len(_LEGACY_FIELDS)))
    assert len(legacy) == len(_LEGACY_FIELDS) + 2
    assert tuple(legacy)[:len(_LEGACY_FIELDS)] \
        == tuple(range(len(_LEGACY_FIELDS)))
    assert legacy.decline_kind is None and legacy.fault is None

    accepted = planner.multipeak_local_marginalize(
        C_A, C_B, 1.0, 8.0, _CountingReserve(), **_ACCEPT_KWARGS)
    assert accepted.accepted and not accepted.used_reserve
    assert accepted.provenance == _PROVENANCE_ACCEPTED
    assert np.isfinite(accepted.value)

    reserve = _CountingReserve()
    budget = planner.multipeak_local_marginalize(
        C_A, C_B, 1.0, 8.0, reserve, **_BUDGET_KWARGS)
    assert not budget.accepted and budget.used_reserve
    assert budget.value == reserve.value
    assert budget.provenance == _PROVENANCE_BUDGET
    assert reserve.calls == 1

    _fault_at(monkeypatch, 2)
    fault_reserve = _CountingReserve(77.25)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        fault = planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, fault_reserve, **_ACCEPT_KWARGS)
    # Default is off, so the fault still RETURNS the reserve exactly as before.
    assert not fault.accepted and fault.used_reserve
    assert fault.value == 77.25
    assert fault.provenance == _PROVENANCE_FAULT_PREFIX + "RuntimeError"
    assert not np.isfinite(fault.delta_log_integral)
    assert fault_reserve.calls == 1

    # A failing reserve still surfaces as _DenseReserveError, not as a planner
    # decline and not as the new fallback error.
    def failing_reserve():
        raise ValueError("deliberate reserve failure")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        with pytest.raises(planner._DenseReserveError):
            planner.multipeak_local_marginalize(
                C_A, C_B, 1.0, 8.0, failing_reserve, **_BUDGET_KWARGS)
