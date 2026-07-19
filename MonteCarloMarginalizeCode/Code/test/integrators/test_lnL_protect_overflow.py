#!/usr/bin/env python
"""
Regression test for the --lnL-protect-overflow fix in
bin/util_ConstructIntrinsicPosterior_GenericCoordinates.py.

Background
----------
CIP passes the *likelihood* exp(lnL) (not lnL) into the sampler integrand.  For a
loud/peaked event (e.g. GW250114, lnL ~ 3300) exp(lnL) overflows float64 to +inf,
so the GMM sampler (mcsamplerEnsemble) sees non-finite probabilities and dies with
"probabilities contain NaN"; CIP then aborts at
    lnLmax = np.max(dat_logL[np.isfinite(dat_logL)])
-> "zero-size array to reduction operation maximum" (all values non-finite).

--lnL-protect-overflow was documented ("Before fitting, subtract lnLmax - 100.  Add
this quantity back at the end.") but was a silent no-op -- never wired into the body.
The fix computes a dynamic offset lnL_shift = max(prev_shift, max(Y) - 100) and feeds
it through the SAME lnL_shift machinery already used by --lnL-shift-prevent-overflow:
Y is shifted down before every fit/exp(), and the offset is added back to the final
ln-evidence, so results are unchanged for well-behaved cases.

This test reproduces the overflow with a synthetic high-lnL (~3000) integrand:
  * unprotected  -> exp(lnL) overflows -> GMM raises / returns non-finite   (the crash)
  * protected    -> exp(lnL - lnL_shift) is finite -> GMM integrates          (the fix)
and checks the pure-numpy invariants of the shift transform (overflow removed +
evidence exactly restored + default path byte-identical).
"""
from __future__ import print_function
import numpy as np
from scipy.special import logsumexp

# lnL_protect_overflow shift, exactly as implemented in
# util_ConstructIntrinsicPosterior_GenericCoordinates.py:
#   if opts.lnL_protect_overflow:
#       lnL_shift = np.max([lnL_shift, max_lnL - 100])
def protect_shift(Y, prev_shift=0.0, target_peak=100.0):
    return float(np.max([prev_shift, np.max(Y) - target_peak]))


def _synthetic_lnL(seed=1234, n=400, lnLmax=3000.0):
    """A sharp 2D Gaussian log-likelihood peak with max lnL ~ lnLmax."""
    rng = np.random.RandomState(seed)
    # training points clustered near a peak at (0,0)
    X = rng.normal(0, 0.15, size=(n, 2))
    r2 = np.sum(X**2, axis=1)
    Y = lnLmax - 0.5 * r2 / (0.1**2)   # lnL, peaks at lnLmax
    return X, Y


def test_transform_removes_overflow_and_restores_evidence():
    """Core invariant of the fix, deterministic and dependency-light."""
    _, Y = _synthetic_lnL(lnLmax=3000.0)

    # 1) Unprotected: exp(lnL) overflows to +inf (this is what the sampler sees)
    with np.errstate(over="ignore"):
        L_unprot = np.exp(Y)
    assert not np.all(np.isfinite(L_unprot)), \
        "expected exp(lnL) to overflow for lnLmax~3000 (the failure this fix targets)"

    # 2) Protected: shift = max_lnL - 100, exp(lnL - shift) is finite
    shift = protect_shift(Y)
    assert abs(shift - (np.max(Y) - 100.0)) < 1e-9
    L_prot = np.exp(Y - shift)
    assert np.all(np.isfinite(L_prot)), "protected exp(lnL-shift) must be finite"
    assert np.max(L_prot) == np.exp(100.0)  # peak arg is exactly 100 by construction

    # 3) Evidence is restored exactly: the code returns ln_integrand_value + lnL_shift.
    #    A shift-invariant log-sum-exp over the training points stands in for that:
    ln_evidence_direct = logsumexp(Y)                     # would overflow in linear space
    ln_evidence_shifted = logsumexp(Y - shift) + shift    # "+ lnL_shift" restoration
    assert np.isclose(ln_evidence_direct, ln_evidence_shifted, rtol=0, atol=1e-9)


def test_default_path_is_byte_identical_for_well_behaved_data():
    """Flag-absent / low-lnL case: shift stays at its prior value (0) -> no change."""
    _, Y = _synthetic_lnL(lnLmax=8.0)     # well-behaved, max_lnL < 100
    assert np.all(np.isfinite(np.exp(Y)))
    # max(Y)-100 is negative, so the clamp keeps the prior shift (0): default unchanged
    assert protect_shift(Y, prev_shift=0.0) == 0.0
    # and an explicit static --lnL-shift-prevent-overflow is never reduced by protect
    assert protect_shift(Y, prev_shift=250.0) == 250.0


def test_protect_never_below_static_shift():
    """--lnL-protect-overflow and --lnL-shift-prevent-overflow coexist: take the max."""
    _, Y = _synthetic_lnL(lnLmax=3000.0)   # max_lnL - 100 ~ 2900
    dyn = np.max(Y) - 100.0
    assert protect_shift(Y, prev_shift=0.0) == dyn          # dynamic wins when larger
    assert protect_shift(Y, prev_shift=dyn + 500.0) == dyn + 500.0  # static wins when larger


def _run_gmm(Y_func):
    """Drive the actual GMM sampler (mcsamplerEnsemble) over a 2D box."""
    from RIFT.integrators import mcsamplerEnsemble
    np.random.seed(0)
    s = mcsamplerEnsemble.MCSampler()
    for p in ["x", "y"]:
        s.add_parameter(p, left_limit=-1.0, right_limit=1.0, adaptive_sampling=True)

    def f(x, y):
        r2 = x**2 + y**2
        lnL = 3000.0 - 0.5 * r2 / (0.1**2)
        return Y_func(lnL)

    return s.integrate(f, "x", "y", min_iter=5, max_iter=20, n_comp=1)


def test_gmm_overflow_then_protected_succeeds():
    """End-to-end via the real GMM sampler CIP uses (mcsamplerEnsemble)."""
    try:
        from RIFT.integrators import mcsamplerEnsemble  # noqa: F401
    except Exception as e:               # pragma: no cover - optional dependency
        import pytest
        pytest.skip("mcsamplerEnsemble unavailable: %s" % e)

    # Unprotected: integrand = exp(lnL) overflows -> GMM must NOT return a finite result
    crashed = False
    res_unprot = None
    try:
        with np.errstate(over="ignore", invalid="ignore"):
            res_unprot = _run_gmm(lambda lnL: np.exp(lnL))[0]
    except Exception:
        crashed = True
    assert crashed or not np.isfinite(res_unprot), \
        "unprotected GMM should overflow/fail on exp(lnL) with lnLmax~3000"

    # Protected: integrand = exp(lnL - shift), shift = lnLmax - 100 -> finite result
    shift = 3000.0 - 100.0
    res_prot = _run_gmm(lambda lnL: np.exp(lnL - shift))[0]
    assert np.isfinite(res_prot) and res_prot > 0, \
        "protected GMM (exp(lnL-shift)) must return a finite positive integral"


if __name__ == "__main__":
    test_transform_removes_overflow_and_restores_evidence()
    test_default_path_is_byte_identical_for_well_behaved_data()
    test_protect_never_below_static_shift()
    test_gmm_overflow_then_protected_succeeds()
    print("ALL REGRESSION TESTS PASSED")
