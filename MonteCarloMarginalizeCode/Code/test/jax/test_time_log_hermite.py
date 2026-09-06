"""Log-space cubic-Hermite time quadrature.

The terminal time integral is the binding error term on the differentiable arm,
and the band-limited rule cannot fix it there: exp(lnL_t) is not band-limited
after a nonlinear (phi_ref, psi) marginalization, so FFT reconstruction is the
wrong operation and `_time_marginalize_terminal` refuses it.

This rule claims no band-limitedness.  It interpolates lnL in LOG space, where
the function is smooth and nearly quadratic near its peak, and integrates exp of
that interpolant with per-interval Gauss-Legendre.  So it is valid exactly where
Simpson is valid, and the tests below pin that it is far more accurate in the
regime that actually bites: a peak narrower than the sample spacing.
"""
import numpy as np
import pytest

jax = pytest.importorskip("jax")
jax.config.update("jax_enable_x64", True)
import jax.numpy as jnp  # noqa: E402

from RIFT.likelihood.jax_ile.core import (  # noqa: E402
    _time_marginalize, _time_marginalize_log_hermite, _simpson_weights,
    _TIME_QUAD_CHOICES)


def _gaussian_case(h_over_sigma, npts=1025, srate=4096.0, offset_frac=0.3):
    """lnL(t) = -(t-t0)^2 / 2 sigma^2 with an OFF-GRID peak.

    Off-grid on purpose: a peak sitting exactly on a sample is the easy case and
    would flatter any rule.
    """
    deltaT = 1.0 / srate
    sigma = deltaT / h_over_sigma
    t = (np.arange(npts) - npts // 2) * deltaT
    t0 = offset_frac * deltaT
    lnL = -((t - t0) ** 2) / (2.0 * sigma ** 2)
    truth = 0.5 * np.log(2.0 * np.pi) + np.log(sigma)   # log integral, analytic
    return jnp.asarray(lnL[None, :]), deltaT, npts, truth


def test_registered_as_a_choice():
    assert "log-hermite" in _TIME_QUAD_CHOICES


# NOTE h/sigma <= 0.5 only.  h/sigma = 1.0 is NOT the resolved regime: Simpson is
# already 2.8e-3 nats off there, which this test caught when it was written with
# 1.0 included.  Asserting agreement with a rule that is itself wrong would have
# pinned the wrong tolerance and made the comparison meaningless.
@pytest.mark.parametrize("h_over_sigma", [0.25, 0.5])
def test_agrees_with_simpson_when_resolved(h_over_sigma):
    """Where Simpson is trustworthy the two must agree: same quantity, same norm."""
    y, deltaT, npts, truth = _gaussian_case(h_over_sigma)
    w = jnp.asarray(_simpson_weights(npts, deltaT))
    a = float(_time_marginalize(y, w)[0])
    b = float(_time_marginalize_log_hermite(y, deltaT)[0])
    assert abs(a - b) < 1e-6, (a, b)
    assert abs(b - truth) < 1e-6


@pytest.mark.parametrize("h_over_sigma,simpson_floor",
                         [(1.0, 1e-3), (2.0, 1e-2), (4.0, 1e-1)])
def test_beats_simpson_when_under_resolved(h_over_sigma, simpson_floor):
    """The regime that bites: peak narrower than the spacing.

    Asserts BOTH that Simpson is genuinely bad here (so the comparison is not
    against a straw man) and that log-hermite is orders better.
    """
    y, deltaT, npts, truth = _gaussian_case(h_over_sigma)
    w = jnp.asarray(_simpson_weights(npts, deltaT))
    e_s = abs(float(_time_marginalize(y, w)[0]) - truth)
    e_h = abs(float(_time_marginalize_log_hermite(y, deltaT)[0]) - truth)
    assert e_s > simpson_floor, "Simpson is not actually failing here; test is vacuous"
    assert e_h < e_s / 100.0, (e_s, e_h)


def test_jit_vmap_grad():
    """Must survive the three transforms the JAX likelihood composes."""
    y, deltaT, npts, truth = _gaussian_case(2.0)
    f = lambda z: _time_marginalize_log_hermite(z, deltaT)
    plain = float(f(y)[0])
    assert abs(float(jax.jit(f)(y)[0]) - plain) < 1e-12
    stacked = jnp.concatenate([y, y + 1.0, y - 2.0], axis=0)
    v = jax.jit(f)(stacked)
    assert v.shape == (3,)
    # the additive shifts must pass straight through a log integral
    assert abs(float(v[1] - v[0]) - 1.0) < 1e-10
    assert abs(float(v[2] - v[0]) + 2.0) < 1e-10
    g = jax.grad(lambda z: _time_marginalize_log_hermite(z, deltaT)[0])(y)
    assert np.all(np.isfinite(np.asarray(g)))
    # d/d(lnL) of a log integral is a normalized weight: it must sum to 1
    assert abs(float(jnp.sum(g)) - 1.0) < 1e-8


def test_shift_and_scale_invariances():
    """Two identities a log integral must satisfy exactly."""
    y, deltaT, npts, truth = _gaussian_case(1.5)
    base = float(_time_marginalize_log_hermite(y, deltaT)[0])
    shifted = float(_time_marginalize_log_hermite(y + 3.5, deltaT)[0])
    assert abs((shifted - base) - 3.5) < 1e-10
    doubled = float(_time_marginalize_log_hermite(y, 2.0 * deltaT)[0])
    assert abs((doubled - base) - np.log(2.0)) < 1e-10


# --------------------------------------------------------------------------
# Tests added by adversarial review of this file's own first version.  The
# original suite tested ONE Gaussian, which is the single easiest integrand for
# a cubic interpolant -- a fixture chosen, unintentionally, to flatter the
# method.  These pin the cases where the advantage is small or absent.
# --------------------------------------------------------------------------

# numpy 2 renamed trapz -> trapezoid and REMOVED the old name; CI installs numpy
# unpinned, and the two interpreters here disagree (IGWN 1.26.4 has only trapz,
# the dev env 2.4.6 has only trapezoid).  Written against one, this helper failed
# on the other -- caught only by running the suite under both.
_TRAPZ = getattr(np, "trapezoid", None) or np.trapz


def _log_integral_reference(fn, t, n=400001):
    """Reference by dense trapezoid.  Converged: identical to 12 digits from
    n=1e5 to n=8e6 on the skewed case, checked rather than assumed."""
    tt = np.linspace(t[0], t[-1], n)
    g = fn(tt)
    m = g.max()
    return m + np.log(_TRAPZ(np.exp(g - m), tt))


def test_advantage_is_smaller_on_a_skewed_peak():
    """A skewed lnL is physical and much harder than the suite's Gaussian.

    Pins the ADVANTAGE, not just the direction: on the Gaussian log-hermite is
    ~4 orders better; skewed it is ~5x.  Quoting the Gaussian figure as if it
    were general would overstate the method.
    """
    npts, deltaT = 513, 1.0 / 4096
    t = (np.arange(npts) - npts // 2) * deltaT
    fn = lambda tt: -(tt ** 2) / (2 * (deltaT / 2) ** 2) - 8.0 * np.tanh(tt / deltaT)
    truth = _log_integral_reference(fn, t)
    y = jnp.asarray(fn(t)[None, :])
    w = jnp.asarray(_simpson_weights(npts, deltaT))
    e_s = abs(float(_time_marginalize(y, w)[0]) - truth)
    e_h = abs(float(_time_marginalize_log_hermite(y, deltaT)[0]) - truth)
    assert e_h < e_s, (e_s, e_h)
    assert e_h > e_s / 100.0, (
        "skewed advantage is now >100x; if that is real, update the claim in the "
        "docstring, which says the large factors are the smooth-Gaussian case")


def test_error_changes_sign_so_convergence_must_not_be_read_at_one_anchor():
    """Undershoots while resolved, overshoots once not -- zero-crossing ~h/sigma 3.

    A convergence test anchored near the crossing would report near-perfect
    accuracy for the wrong reason.
    """
    npts, deltaT = 513, 1.0 / 4096
    t = (np.arange(npts) - npts // 2) * deltaT
    errs = {}
    for hs in (2.0, 8.0):
        sigma = deltaT / hs
        y = jnp.asarray((-((t - 0.3 * deltaT) ** 2) / (2 * sigma ** 2))[None, :])
        truth = 0.5 * np.log(2 * np.pi) + np.log(sigma)
        errs[hs] = float(_time_marginalize_log_hermite(y, deltaT)[0]) - truth
    assert errs[2.0] < 0.0 < errs[8.0], errs


def test_non_finite_rows_are_handled_or_refused_but_never_silently_wrong():
    """Four cases, and the middle one is a deliberate refusal.

    A mixed -inf row cannot be represented in log space: flooring -inf to
    row_max-700 makes Catmull-Rom overshoot to +51.85 and return +43.10 where the
    truth is just below -6.24, and no floor depth is safe (the overshoot scales
    with it, a shallow floor contributes spuriously).  Returning NaN is the honest
    answer -- callers needing those rows should use `simpson`, which is linear in
    exp(lnL) and drops a -inf sample cleanly.
    """
    deltaT = 1.0 / 4096
    finite = float(_time_marginalize_log_hermite(jnp.zeros((1, 9)), deltaT)[0])
    assert np.isfinite(finite)

    allneg = np.full((1, 9), -np.inf)
    assert float(_time_marginalize_log_hermite(jnp.asarray(allneg), deltaT)[0]) == -np.inf

    mixed = np.zeros((1, 9)); mixed[0, 3] = -np.inf
    assert np.isnan(float(_time_marginalize_log_hermite(jnp.asarray(mixed), deltaT)[0])), \
        "a mixed -inf row must be REFUSED, not given a plausible-looking value"

    nan = np.zeros((1, 9)); nan[0, 3] = np.nan
    assert np.isnan(float(_time_marginalize_log_hermite(jnp.asarray(nan), deltaT)[0])), \
        "NaN must propagate; hiding a numerical failure is worse than the failure"

    pos = np.zeros((1, 9)); pos[0, 3] = np.inf
    assert float(_time_marginalize_log_hermite(jnp.asarray(pos), deltaT)[0]) == np.inf


def test_quadratic_lnL_is_reproduced_far_better_than_a_cubic_one():
    """The corrected claim, pinned on a physical observable.

    Centered differences are exact for a quadratic and not for a cubic, so a
    cubic Hermite segment fed them reproduces quadratics and NOT cubics -- the
    original docstring said cubics.  A purely quadratic lnL is a Gaussian, which
    the rule integrates essentially exactly at h/sigma = 1; adding a cubic term at
    the same resolution must degrade it by orders of magnitude.

    Deliberately NOT a convergence-order test: the error crosses zero near
    h/sigma ~ 3 (see the docstring), so a slope fitted across that region would be
    meaningless.  The order O(h^3.01) quoted there was measured on the interpolant
    directly, not end-to-end.
    """
    npts, deltaT = 513, 1.0 / 4096
    t = (np.arange(npts) - npts // 2) * deltaT
    sigma = deltaT                      # h/sigma = 1, the resolved regime

    quad = lambda tt: -(tt ** 2) / (2 * sigma ** 2)
    e_quad = abs(float(_time_marginalize_log_hermite(jnp.asarray(quad(t)[None, :]), deltaT)[0])
                 - (0.5 * np.log(2 * np.pi) + np.log(sigma)))

    cubic = lambda tt: -(tt ** 2) / (2 * sigma ** 2) + 0.35 * (tt / sigma) ** 3
    ref = _log_integral_reference(cubic, t)
    e_cubic = abs(float(_time_marginalize_log_hermite(jnp.asarray(cubic(t)[None, :]), deltaT)[0]) - ref)

    assert e_quad < 1e-9, e_quad
    assert e_cubic > 1e3 * e_quad, (e_quad, e_cubic,
                                    "cubic is now as accurate as quadratic; the "
                                    "docstring's reproduction claim needs revisiting")


def test_real_jax_vmap_not_just_a_stacked_batch():
    """test_jit_vmap_grad stacks rows; that is not jax.vmap.  This is."""
    npts, deltaT = 257, 1.0 / 4096
    t = (np.arange(npts) - npts // 2) * deltaT
    y = jnp.asarray((-((t - 0.3 * deltaT) ** 2) / (2 * (deltaT / 2) ** 2))[None, :])
    stacked = jnp.concatenate([y, y + 1.0], axis=0)
    v = jax.vmap(lambda z: _time_marginalize_log_hermite(z[None, :], deltaT)[0])(stacked)
    assert v.shape == (2,)
    assert abs(float(v[1] - v[0]) - 1.0) < 1e-10
