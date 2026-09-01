"""The reordered angle-first / distance-GH path for the 'laplace' scheme.

The laplace angle-marg scheme used to REFUSE the adaptive distance quadrature
(JAX_ILE_DISTMARG_GH): that quadrature places its nodes from a FIXED-PSI
exponent, and on this path psi has already been integrated out.  Since a
uniform distance grid must resolve a peak of width ~1/rho, the refusal left the
distance axis costing O(rho) on the one scheme whose angle axes do not --
blocking an O(rho) extrinsic stage.  anglemarg now supplies a node-placement
rule for the psi-MARGINAL (``_psimarg_gh_placement``), opted in with
JAX_ILE_LAPLACE_GH=1.

What this file gates, and why each test is here:

  * THE OPT-IN IS REALLY OPT-IN.  Two tests pin that with the env unset the
    refusal still fires and `auto` still resolves to `exact` under
    JAX_ILE_DISTMARG_GH, and one pins that setting the flag WITHOUT the GH
    quadrature changes the laplace result by exactly zero.  This project's
    standing rule is that no existing default moves; a silently-shifted default
    is the failure these three exist to catch.

  * THE PLACEMENT RULE REDUCES EXACTLY.  At A1 = B1 = B2 = 0 the psi-marginal
    IS the fixed-psi exponent, so the new rule must return core's own
    (clip(K/R), 1/sqrt(R)) and the whole reordered quadrature must return
    core._distmarg_gh_logL to roundoff.  This is the equivalence check: it is a
    comparison against the SHIPPED, independently-validated quadrature, not a
    self-comparison.

  * THE FEATURE IS ACTUALLY CONNECTED.  A test that only exercised
    _psimarg_gh_placement in isolation would stay green if the fused driver
    never called it -- this repo has a documented history of features that were
    inert while their tests passed.  So the last two tests go through
    fused_log_likelihood_distphipsimarg_laplace itself and assert a property
    that is FALSE for the code path it replaces: the adaptive result does not
    depend on the number of uniform grid points (it uses the grid only for the
    support bounds), while the uniform sum demonstrably does.

NOT gated here (deliberately, and stated so this file is not over-read):
coverage of the placement rule for a strongly MULTI-MODAL psi profile, and any
production-amplitude accuracy claim.  See the block comment in anglemarg.py.
"""
import numpy as np
import jax.numpy as jnp
import pytest

from RIFT.likelihood.jax_ile import anglemarg as AM
from RIFT.likelihood.jax_ile import build_likelihood_data
from RIFT.likelihood.jax_ile import core as C

INTERP = "sinc"
RA, DEC, INCL = np.array([0.9]), np.array([0.4]), np.array([1.1])
D_MIN, D_MAX = 30.0, 3000.0


@pytest.fixture
def gh_off():
    """Restore both switches whatever the test does to them."""
    gh0, lap0 = C._DISTMARG_GH_N, AM._LAPLACE_GH
    yield
    C._DISTMARG_GH_N, AM._LAPLACE_GH = gh0, lap0


def make_synth(scale=1.0, seed=3, modes=((2, 2), (2, -2)), npts=32,
               deltaT=1.0 / 1024, kappa_boost=1.0):
    """Structurally-faithful synthetic packed data (cf. test_angle_marg_smoke).

    ``kappa_boost`` scales the rholm timeseries only, so it raises the coherent
    (phi,psi) amplitude A -- ``scale`` does NOT (it multiplies rho and U/V
    together, leaving A = K^2/2R fixed).
    """
    rng = np.random.default_rng(seed)
    tw = npts * deltaT / 2.0
    tvals = np.linspace(-tw, tw, npts)
    tref = 1126259462.413
    K = len(modes)
    packed = {}
    for det in ("H1", "L1"):
        white = (rng.standard_normal((K, 4096))
                 + 1j * rng.standard_normal((K, 4096)))
        kx = np.arange(-40, 41)
        kern = np.exp(-0.5 * (kx / 12.0) ** 2)
        kern /= kern.sum()
        rho = np.stack([np.convolve(white[k].real, kern, "same")
                        + 1j * np.convolve(white[k].imag, kern, "same")
                        for k in range(K)]).astype(np.complex128)
        rho *= np.sqrt(len(kx)) * scale * kappa_boost
        M = rng.standard_normal((K, K)) + 1j * rng.standard_normal((K, K))
        U = (M @ M.conj().T + 3 * np.eye(K)) * scale ** 2
        B = rng.standard_normal((K, K)) + 1j * rng.standard_normal((K, K))
        V = (B @ B.T) * scale ** 2 * 0.3
        packed[det] = dict(lms=np.array(modes, dtype=int), rholmArray=rho,
                           U=U, V=V, epoch=tref - 0.5)
    return build_likelihood_data(packed, deltaT, tref, tvals)


def _laplace(data, x_grid, log_w, amp):
    return np.asarray(AM.fused_log_likelihood_distphipsimarg_laplace(
        data, jnp.asarray(RA), jnp.asarray(DEC), jnp.asarray(INCL),
        x_grid, log_w, interp=INTERP, amp_sizing=amp))


def _grid(data, n):
    return C.make_distance_grid(D_MIN, D_MAX, n, distMpcRef=data.distMpcRef)


# ---------------------------------------------------------------------------
# The opt-in is opt-in
# ---------------------------------------------------------------------------

def test_laplace_still_refuses_gh_without_the_optin(gh_off):
    """Default behaviour is UNCHANGED: laplace + JAX_ILE_DISTMARG_GH raises,
    and the message names the opt-in rather than leaving the reader stuck."""
    data = make_synth(scale=1.0)
    x_grid, log_w = _grid(data, 32)
    amp = AM.estimate_angle_amplitude(data, x_grid)
    C._DISTMARG_GH_N = 32
    AM._LAPLACE_GH = 0
    with pytest.raises(ValueError) as exc:
        _laplace(data, x_grid, log_w, amp)
    assert "JAX_ILE_LAPLACE_GH" in str(exc.value)


def test_selector_still_forces_exact_under_gh_without_the_optin(gh_off):
    """`auto` + JAX_ILE_DISTMARG_GH resolved to 'exact' before this change and
    must still, or every existing adaptive-distance run silently changes
    scheme."""
    amp = 10.0 * AM.ANGLE_MARG_CROSSOVER_AMPLITUDE
    assert AM.choose_angle_marg_scheme(
        amp, gh_enabled=True, laplace_gh_enabled=False)[0] == "exact"
    assert AM.choose_angle_marg_scheme(
        amp, gh_enabled=True, laplace_gh_enabled=True)[0] == "laplace"
    # and the env-driven default resolves to the untouched behaviour
    C._DISTMARG_GH_N, AM._LAPLACE_GH = 32, 0
    assert AM.choose_angle_marg_scheme(amp)[0] == "exact"


def test_the_optin_alone_changes_nothing(gh_off):
    """JAX_ILE_LAPLACE_GH must be inert while the adaptive quadrature is off:
    the reordered path is reachable ONLY through JAX_ILE_DISTMARG_GH."""
    data = make_synth(scale=1.0)
    x_grid, log_w = _grid(data, 48)
    amp = AM.estimate_angle_amplitude(data, x_grid)
    C._DISTMARG_GH_N = 0
    AM._LAPLACE_GH = 0
    a = _laplace(data, x_grid, log_w, amp)
    AM._LAPLACE_GH = 1
    b = _laplace(data, x_grid, log_w, amp)
    assert np.array_equal(a, b), "the opt-in perturbed the uniform-grid path"


# ---------------------------------------------------------------------------
# Equivalence with the shipped quadrature where both are valid
# ---------------------------------------------------------------------------

def test_placement_reduces_to_the_fixed_psi_rule():
    """With no psi harmonics the psi-marginal IS the fixed-psi exponent, so the
    moment-matched rule must return core's (clip(K/R), 1/sqrt(R)) exactly.  A
    mutation to either moment (dropping Var_w(m), or the sub-grid term, or the
    s^2 term) breaks this."""
    rng = np.random.default_rng(0)
    A0 = jnp.asarray(rng.uniform(0.5, 40.0, (9,)))
    B0 = jnp.asarray(rng.uniform(0.5, 40.0, (9,)))
    Z = jnp.zeros((9,), dtype=jnp.complex128)
    x_min, x_max = 0.05, 3.0
    ctr, sig = AM._psimarg_gh_placement(A0, Z, B0, Z, Z, x_min, x_max)
    assert np.allclose(np.asarray(ctr),
                       np.clip(np.asarray(A0) / np.asarray(B0), x_min, x_max),
                       rtol=0, atol=1e-12)
    assert np.allclose(np.asarray(sig), 1.0 / np.sqrt(np.asarray(B0)),
                       rtol=0, atol=1e-12)


@pytest.mark.parametrize("n_nodes", [32, 64])
def test_reordered_quadrature_reproduces_core_gh_in_the_fixed_psi_limit(n_nodes):
    """THE equivalence check.  The reordered construction -- psi-marginal
    placement, halo-padded blocked nodes, x^-4 measure, prior normalization --
    must reproduce the SHIPPED core._distmarg_gh_logL wherever both are valid.
    Measured 4.3e-14 nats; the bar below is loose enough for another backend and
    still far tighter than any quadrature difference."""
    rng = np.random.default_rng(1)
    A0 = jnp.asarray(rng.uniform(0.5, 60.0, (11,)))
    B0 = jnp.asarray(rng.uniform(0.5, 60.0, (11,)))
    Z = jnp.zeros((11,), dtype=jnp.complex128)
    x_min, x_max = 0.05, 3.0
    z, dz = C.make_distance_gh(n_nodes)
    got = AM._distmarg_gh_psimarg_lnI(A0, Z, B0, Z, Z, z, x_min, x_max,
                                      node_block=4)
    ref = C._distmarg_gh_logL(A0, B0, z, dz, x_min, x_max)
    d = float(np.max(np.abs(np.asarray(got) - np.asarray(ref))))
    assert d < 1e-10, ("reordered GH disagrees with core._distmarg_gh_logL by "
                       "%.3e nats in the fixed-psi limit" % d)


# ---------------------------------------------------------------------------
# The feature is CONNECTED: these go through the fused driver
# ---------------------------------------------------------------------------

def test_fused_laplace_gh_runs_and_ignores_the_uniform_grid_size(gh_off):
    """Wiring, asserted by a property that is FALSE of the code it replaces.

    On the reordered path the static x_grid supplies only the support bounds,
    so doubling its point count changes NOTHING; the uniform sum it replaces
    changes by orders of magnitude more.  If the fused driver stopped calling
    _distmarg_gh_psimarg_lnI -- or silently fell back to the uniform sum -- the
    first assertion fails.  Both legs run the real
    fused_log_likelihood_distphipsimarg_laplace.
    """
    data = make_synth(scale=1.0, kappa_boost=6.0)
    xg_a, lw_a = _grid(data, 32)
    xg_b, lw_b = _grid(data, 256)
    amp = AM.estimate_angle_amplitude(data, xg_b)

    C._DISTMARG_GH_N = 48
    AM._LAPLACE_GH = 1
    gh_a = _laplace(data, xg_a, lw_a, amp)
    gh_b = _laplace(data, xg_b, lw_b, amp)
    C._DISTMARG_GH_N = 0
    un_a = _laplace(data, xg_a, lw_a, amp)
    un_b = _laplace(data, xg_b, lw_b, amp)

    assert np.all(np.isfinite(gh_a))
    d_gh = float(np.max(np.abs(gh_a - gh_b)))
    d_un = float(np.max(np.abs(un_a - un_b)))
    assert d_gh < 1e-9, ("the adaptive path must not depend on the uniform "
                         "grid size, but moved %.3e nats" % d_gh)
    assert d_un > 100.0 * max(d_gh, 1e-12), (
        "the uniform sum did not move either (%.3e nats) -- this test no "
        "longer discriminates" % d_un)


def test_fused_laplace_gh_matches_a_dense_uniform_reference(gh_off):
    """Accuracy of the NEW combination against a densely-converged reference,
    at an amplitude where the laplace scheme is the one that would be selected
    (A ~ 200; the selector crossover is 450 on a bound that over-reads ~2x).

    The reference is the same fused laplace path on a 1024-point uniform
    distance grid -- so this isolates the DISTANCE quadrature, which is the
    only thing the reordering changes.  Measured 3.7e-4 nats at 64 nodes.
    """
    data = make_synth(scale=1.0, kappa_boost=6.0)
    xg_ref, lw_ref = _grid(data, 1024)
    amp = AM.estimate_angle_amplitude(data, xg_ref)
    C._DISTMARG_GH_N = 0
    ref = _laplace(data, xg_ref, lw_ref, amp)
    xg, lw = _grid(data, 32)
    C._DISTMARG_GH_N = 64
    AM._LAPLACE_GH = 1
    got = _laplace(data, xg, lw, amp)
    d = float(np.max(np.abs(got - ref)))
    assert d < 5e-3, ("laplace + adaptive distance quadrature is %.3e nats "
                      "from the dense uniform reference" % d)
