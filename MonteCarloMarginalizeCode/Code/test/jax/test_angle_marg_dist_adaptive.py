"""
Gate for the ADAPTIVE DISTANCE QUADRATURE of the dense angle-marg schemes
(dist_quad='adaptive'): the campaign-cost contract change.

WHAT CHANGED (2026-08-28): with dist_quad='adaptive' the dense schemes stop
integrating distance on the caller's fixed (x_grid, log_w_grid) quadrature
and place per-point node sets instead -- core's single-Gaussian GH machinery
for the exact scheme (one Gaussian per dense (phi, u) point), and a new
PSI-MARGINAL placement for the laplace scheme (the psi axis is integrated
out, so the distance integrand at each (phi, S, t) point is a u-FAMILY of
Gaussians; nodes must cover the union of the relevant family peaks, not one
peak).  Values legitimately DIFFER from the fixed-grid path by the fixed
grid's own quadrature error, so accuracy is pinned against a CONVERGED
fixed grid, not bit-for-bit against the old path.

These tests pin (1) the dist_quad resolution contract (legacy env behavior
when None; explicit + env refuses), (2) the exact scheme's parameterized
path being THE SAME machinery as the env path, (3) the laplace placement's
coverage of the analytic peak family, (4) agreement with a converged
reference at a tolerance the coarse fixed grid measurably FAILS (so a
silent fall-back-to-grid mutation is caught), (5) tile/sort invariance of
the cost-only restructuring, and (6) the coverage fail-safe.

Each test fails under a deliberate mutation (verified in the gate's env;
mutations and observed failures recorded in the PR).
"""

import numpy as np
import pytest

import jax
jax.config.update("jax_enable_x64", True)
import jax.numpy as jnp

from RIFT.likelihood.jax_ile import anglemarg as AM
from RIFT.likelihood.jax_ile import core as core_mod
from RIFT.likelihood.jax_ile.core import make_distance_grid

from test_angle_marg_compile_cost import make_synth

INTERP = "sinc"
RA, DEC, INCL = (jnp.asarray(np.array([0.9, 2.1, 4.4])),
                 jnp.asarray(np.array([0.4, -0.7, 0.1])),
                 jnp.asarray(np.array([1.1, 0.3, 2.6])))
AMP_S = AM.ANGLE_MARG_CROSSOVER_AMPLITUDE


def _grids(data, n):
    return make_distance_grid(30.0, 3000.0, n, distMpcRef=data.distMpcRef)


# ---------------------------------------------------------------------------
# 1. resolution contract
# ---------------------------------------------------------------------------

def test_resolve_dist_quad_contract(monkeypatch):
    monkeypatch.setattr(core_mod, "_DISTMARG_GH_N", 0)
    assert AM._resolve_dist_quad(None, None) == ("grid", 0)
    assert AM._resolve_dist_quad("grid", None) == ("grid", 0)
    assert AM._resolve_dist_quad("adaptive", None) == (
        "adaptive", AM._DIST_ADAPT_N_ENV)
    assert AM._resolve_dist_quad("adaptive", 32) == ("adaptive", 32)
    with pytest.raises(ValueError):
        AM._resolve_dist_quad("bogus", None)
    with pytest.raises(ValueError):
        AM._resolve_dist_quad("adaptive", 1)
    # None keeps legacy env behavior; explicit + env refuses to pick a winner
    monkeypatch.setattr(core_mod, "_DISTMARG_GH_N", 17)
    assert AM._resolve_dist_quad(None, None) == ("gh-env", 17)
    for dq in ("grid", "adaptive"):
        with pytest.raises(ValueError, match="DISTMARG_GH"):
            AM._resolve_dist_quad(dq, None)


# ---------------------------------------------------------------------------
# 2. exact scheme: the parameterized path IS the env path
# ---------------------------------------------------------------------------

def test_exact_adaptive_beats_env_gh_at_prior_dominated_points(monkeypatch):
    """The composite rule must integrate the d^2-prior bulk that the env
    GH machinery misses at prior-dominated (weak-amplitude) points --
    measured 1.2-2.3 nats on the fused marginal.  Fails if 'adaptive' is
    silently rewired back to the +-7 sigma peak-only GH placement."""
    data = make_synth(scale=1.0)          # weak: prior-dominated regime
    xg, lwg = _grids(data, 48)
    ex = AM.fused_log_likelihood_distphipsimarg_exact
    ref = np.asarray(ex(data, RA, DEC, INCL, *_grids(data, 1024),
                        interp=INTERP, amp_sizing=AMP_S))
    monkeypatch.setattr(core_mod, "_DISTMARG_GH_N", 24)
    via_env = np.asarray(ex(data, RA, DEC, INCL, xg, lwg, interp=INTERP,
                            amp_sizing=AMP_S))
    monkeypatch.setattr(core_mod, "_DISTMARG_GH_N", 0)
    via_comp = np.asarray(ex(data, RA, DEC, INCL, xg, lwg, interp=INTERP,
                             amp_sizing=AMP_S, dist_quad="adaptive"))
    assert np.abs(via_comp - ref).max() < 1e-5
    assert np.abs(via_env - ref).max() > 1e-2   # the documented GH defect


# ---------------------------------------------------------------------------
# 3. laplace placement covers the analytic peak family
# ---------------------------------------------------------------------------

def test_laplace_placement_covers_relevant_family():
    """For random psi-coefficient fields, the node interval must contain
    every family peak within DROP nats of the local max (clipped to the
    support), and node spacing must resolve the narrowest relevant width
    once n_nodes exceeds the census requirement.  Fails if the relevance
    union, the +-7 sigma extension, or the width floor is dropped."""
    rng = np.random.default_rng(5)
    X = 64
    x_min, x_max = 0.3333, 33.333
    A0 = jnp.asarray(rng.uniform(-5, 60, X))
    A1 = jnp.asarray(rng.uniform(-30, 30, X) + 1j * rng.uniform(-30, 30, X))
    B0 = jnp.asarray(rng.uniform(0.5, 30, X))
    B1 = jnp.asarray(rng.uniform(-8, 8, X) + 1j * rng.uniform(-8, 8, X))
    B2 = jnp.asarray(rng.uniform(-6, 6, X) + 1j * rng.uniform(-6, 6, X))
    G = 48
    x_k, lw_k = AM._laplace_adaptive_dist_nodes(
        A0, A1, B0, B1, B2, x_min, x_max, G)
    n_out = AM._DIST_ADAPT_N_OUTER
    x_env = np.asarray(x_k)[n_out:n_out + G]    # envelope segment nodes
    x_k = np.asarray(x_k)
    lo, hi = x_env.min(axis=0), x_env.max(axis=0)
    assert (lo >= x_min - 1e-12).all() and (hi <= x_max + 1e-12).all()
    # analytic family on a fine u scan
    u = np.linspace(0, 2 * np.pi, 256, endpoint=False)[:, None]
    A_u = np.asarray(A0)[None] + (np.asarray(A1)[None] * np.exp(1j * u)).real
    B_u = np.maximum(np.asarray(B0)[None]
                     + (np.asarray(B1)[None] * np.exp(1j * u)).real
                     + (np.asarray(B2)[None] * np.exp(2j * u)).real, 0.0)
    Bs = np.maximum(B_u, 1e-300)
    xh = np.clip(A_u / Bs, x_min, x_max)
    amp_u = xh * A_u - 0.5 * xh ** 2 * B_u
    rel = amp_u >= amp_u.max(axis=0)[None] - AM._DIST_ADAPT_DROP
    sig = 1.0 / np.sqrt(Bs)
    # coverage: every relevant peak inside the envelope span (fine-u content
    # can peek marginally past the 32-sample placement; allow half a sigma;
    # GL endpoints sit slightly inside [lo, hi], allow the same slack)
    xh_rel = np.where(rel, xh, np.nan)
    assert np.nanmax((xh_rel - hi[None]) / sig, axis=0).max() < 0.75
    assert np.nanmax((lo[None] - xh_rel) / sig, axis=0).max() < 0.75
    # central GL spacing resolves the narrowest relevant width
    sig_min = np.where(rel, sig, np.inf).min(axis=0)
    dx = (hi - lo) * (np.pi / (2 * G))
    assert (dx <= sig_min * 1.1).all()


# ---------------------------------------------------------------------------
# 4. accuracy against a converged reference (and the coarse grid must FAIL
#    the same tolerance, so a silent fall-back-to-grid mutation is caught)
# ---------------------------------------------------------------------------

def _lap(data, xg, lwg, **kw):
    return np.asarray(AM.fused_log_likelihood_distphipsimarg_laplace(
        data, RA, DEC, INCL, xg, lwg, interp=INTERP, amp_sizing=AMP_S, **kw))

TOL_ADAPT = 1.0e-6      # measured: see PR (adaptive-vs-converged max |dlnL|)


def test_laplace_adaptive_matches_converged_grid():
    data = make_synth(scale=6.0)
    xg, lwg = _grids(data, 48)          # the coarse grid the caller passes
    ref = _lap(data, *_grids(data, 1024))
    ref2 = _lap(data, *_grids(data, 512))
    assert np.abs(ref - ref2).max() < 0.1 * TOL_ADAPT   # reference converged
    got = _lap(data, xg, lwg, dist_quad="adaptive", dist_adapt_n=48,
               s_block=2, t_block=16)
    assert np.abs(got - ref).max() < TOL_ADAPT
    # the 48-node FIXED grid measurably fails the same tolerance: a mutation
    # that silently ignores dist_quad cannot pass
    coarse = _lap(data, xg, lwg)
    assert np.abs(coarse - ref).max() > 10 * TOL_ADAPT


def test_exact_adaptive_matches_converged_grid():
    data = make_synth(scale=6.0)
    xg, lwg = _grids(data, 48)
    ex = AM.fused_log_likelihood_distphipsimarg_exact
    ref = np.asarray(ex(data, RA, DEC, INCL, *_grids(data, 1024),
                        interp=INTERP, amp_sizing=AMP_S))
    got = np.asarray(ex(data, RA, DEC, INCL, xg, lwg, interp=INTERP,
                        amp_sizing=AMP_S, dist_quad="adaptive",
                        dist_adapt_n=AM._DIST_ADAPT_N_EXACT))
    assert np.abs(got - ref).max() < TOL_ADAPT
    coarse = np.asarray(ex(data, RA, DEC, INCL, xg, lwg, interp=INTERP,
                           amp_sizing=AMP_S))
    assert np.abs(coarse - ref).max() > 10 * TOL_ADAPT


# ---------------------------------------------------------------------------
# 5. tile/sort invariance of the cost-only restructuring, incl. gradients
# ---------------------------------------------------------------------------

def test_laplace_adaptive_tile_and_sort_invariance():
    data = make_synth(scale=3.0)
    xg, lwg = _grids(data, 48)
    base = _lap(data, xg, lwg, dist_quad="adaptive", dist_adapt_n=32,
                s_block=len(RA), t_block=data.npts)
    for sb, tb in ((1, 4), (2, 16)):
        tiled = _lap(data, xg, lwg, dist_quad="adaptive", dist_adapt_n=32,
                     s_block=sb, t_block=tb)
        assert np.abs(tiled - base).max() < 1e-12

    def scalar(theta):
        v = AM.fused_log_likelihood_distphipsimarg_laplace(
            data, theta[0:1], theta[1:2], theta[2:3], xg, lwg,
            interp=INTERP, amp_sizing=AMP_S, dist_quad="adaptive",
            dist_adapt_n=32, s_block=1, t_block=8)
        return v[0]
    theta = jnp.asarray([0.9, 0.4, 1.1])
    v, g = jax.value_and_grad(scalar)(theta)
    assert np.isfinite(float(v)) and np.isfinite(np.asarray(g)).all()
    # AD against central finite differences
    for i in range(3):
        e = np.zeros(3); e[i] = 1e-5
        fd = (scalar(theta + jnp.asarray(e)) - scalar(theta - jnp.asarray(e))) / 2e-5
        assert abs(float(fd) - float(np.asarray(g)[i])) < 5e-5 * max(1.0, abs(float(np.asarray(g)[i])))


# ---------------------------------------------------------------------------
# 6. coverage fail-safe: fires on a deliberately starved node budget,
#    stays clean at the shipped budget
# ---------------------------------------------------------------------------

def test_dist_cover_failsafe():
    data = make_synth(scale=6.0, kappa_boost=4.0)
    xg, lwg = _grids(data, 48)
    AM.reset_dist_cover_failsafe()
    _lap(data, xg, lwg, dist_quad="adaptive", dist_adapt_n=48)
    st = AM.dist_cover_failsafe_state()
    assert not st["tripped"]
    _lap(data, xg, lwg, dist_quad="adaptive", dist_adapt_n=3)
    st = AM.dist_cover_failsafe_state()
    assert st["tripped"] and st["worst_ratio"] > 1.0
    AM.reset_dist_cover_failsafe()
    assert not AM.dist_cover_failsafe_state()["tripped"]


# ---------------------------------------------------------------------------
# 7. wrapper passthrough + refusal (silently-inert-flag discipline)
# ---------------------------------------------------------------------------

def test_wrapper_dist_quad_passthrough_and_refusal():
    from RIFT.likelihood.jax_ile.wrapper import JAXDistPhiPsiMargLikelihood
    data = make_synth(scale=3.0)
    with pytest.raises(ValueError, match="no effect"):
        JAXDistPhiPsiMargLikelihood(data, 30.0, 3000.0, n_grid=48,
                                    interp=INTERP,
                                    angle_marg_dist_quad="adaptive")
    like = JAXDistPhiPsiMargLikelihood(data, 30.0, 3000.0, n_grid=48,
                                       interp=INTERP, angle_marg="laplace",
                                       angle_marg_dist_quad="adaptive")
    assert like.angle_marg_info["dist_quad"] == "adaptive"
    got = np.asarray(like.log_likelihood(np.asarray(RA), np.asarray(DEC),
                                         np.asarray(INCL)))
    direct = np.asarray(AM.fused_log_likelihood_distphipsimarg_laplace(
        data, RA, DEC, INCL, like.x_grid, like.log_w_grid, interp=INTERP,
        amp_sizing=like.angle_marg_info["amp_sizing"], dist_quad="adaptive"))
    assert np.abs(got - direct).max() < 1e-12
