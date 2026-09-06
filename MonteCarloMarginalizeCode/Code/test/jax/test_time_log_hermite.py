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
