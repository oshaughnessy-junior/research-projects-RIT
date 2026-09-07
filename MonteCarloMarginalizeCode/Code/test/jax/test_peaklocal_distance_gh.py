"""The adaptive distance quadrature for the peak-local (phi,psi) kernel.

WHY THIS FILE EXISTS.  ``peak-local`` localizes both angle axes so that cost stops
growing with amplitude, and then summed the caller's UNIFORM distance grid -- which is a
third dense axis whose required size scales as rho, putting the factor straight back.
Everything here is about the distance axis: that the adaptive placement is right where a
uniform grid is still trustworthy, that it converges in its node count, that the
``(2 pi)^-2`` prior factor is applied exactly ONCE end to end, and that the window
certifies itself rather than being assumed.

The guards in this module have been the recurring defect class across the peak-local
review rounds -- things that look like coverage and are not -- so every assertion here
was checked by BREAKING the code it guards and confirming this file goes red.
"""
import numpy as np
import pytest

jax = pytest.importorskip("jax")
import jax.numpy as jnp

from RIFT.likelihood.jax_ile import anglemarg as AM
from RIFT.likelihood.jax_ile import core as CORE
from RIFT.likelihood.jax_ile import joint_anglemarg_peaklocal as JP
from RIFT.likelihood.jax_ile import samplers as SAMP


@pytest.fixture(autouse=True)
def _drop_jax_caches():
    """Same reason as test_joint_anglemarg_peaklocal.py: this file sweeps shapes
    (node counts, phi/u sizes) and JAX retains a compiled executable per shape."""
    yield
    if hasattr(jax, "clear_caches"):
        jax.clear_caches()


D_MIN, D_MAX = 1.0, 10000.0


def _tables(rho=None, seed=3, modes=((2, 2), (2, -2))):
    """One ``(sample, time)`` coefficient pair from the SHARED faithful synthetic data.

    Random tables are not usable here.  The physics guarantees ``B(phi,u) = <h|h> >= 0``,
    and a table that violates it makes the x-profile unbounded above, pins every peak at
    the support edge, and turns every claim below into a claim about the fixture: the
    first draft of this file used random tables and four assertions were vacuous or
    inverted because of it.  ``test_angle_marg_smoke.make_synth`` builds U Hermitian
    positive definite and V complex symmetric, as the real precompute does.

    ``rho`` rescales BOTH tables by the same factor, which is the physical way to make a
    signal louder at a FIXED distance: ``K -> b^2 K`` and ``R -> b^2 R`` holds
    ``x* = K/R`` fixed while ``rho_mf = K/sqrt(R)`` grows as ``b``.  Scaling A alone
    instead marches the distance peak across the support and makes every rung a different
    problem.
    """
    import test_angle_marg_smoke as SM
    data = SM.make_synth(scale=1.0, seed=seed, modes=modes)
    C_A, C_B, _ = AM.angle_coefficient_tables(data, SM.RA, SM.DEC, SM.INCL, "sinc")
    ca, cb = np.asarray(C_A), np.asarray(C_B)
    t = int(np.argmax(np.abs(ca).sum(axis=(0, 1))[0]))
    A = jnp.asarray(ca[:, :, 0, t])
    B = jnp.asarray(cb[:, :, 0, t])
    dref = float(data.distMpcRef)
    if rho is not None:
        xlo, xhi = dref / D_MAX, dref / D_MIN
        _, _, F0, _ = JP.x_profile_peak(A, B, xlo, xhi)
        b2 = (float(rho) / max(np.sqrt(2 * float(F0)), 1e-12)) ** 2
        A, B = A * b2, B * b2
    return A, B, dref


def _grid(dref, n=256, **kw):
    return CORE.make_distance_grid(D_MIN, D_MAX, n, distMpcRef=dref, **kw)


def _support(dref):
    return dref / D_MAX, dref / D_MIN


# ---------------------------------------------------------------- normalization

def test_the_2pi_squared_prior_factor_is_applied_exactly_once_on_both_paths():
    """THE trap a stacked quadrature falls into, and it is worth 2 ln(2 pi) = 3.676 nats
    -- large enough to matter, small enough to look plausible.

    With ZERO coefficient tables the torus integral is exactly ``4 pi^2`` at every
    distance, so the whole value collapses to ``logsumexp(log_w)``: the ``(2 pi)^-2``
    factor cancels the torus area and nothing else is left.  Applying it twice, or not at
    all, moves this by exactly -+3.676.  The uniform weights sum to one by construction,
    so the uniform arm must return exactly ZERO -- and the adaptive arm must return
    exactly the log of ITS OWN total weight, which makes the assertion about the
    normalization SPLIT rather than about how good the distance rule is.
    """
    dref = 1000.0
    Z_A = jnp.zeros((3, 5), dtype=jnp.complex128)
    Z_B = jnp.zeros((5, 5), dtype=jnp.complex128)
    xg, lw = _grid(dref, 256)
    v = float(JP.joint_lnL_phi_dense(Z_A, Z_B, xg, lw, n_phi=32, n_nodes=48))
    assert abs(v) < 1e-12, ("the uniform path must return the log of the total prior "
                            "weight, which is exactly zero here; got %r" % v)
    xlo, xhi = _support(dref)
    x_k, lw_k = JP.distance_gh_nodes(0.3, 4.0, xlo, xhi, 32, log_w_grid=lw)
    got = float(JP.joint_lnL_phi_dense(Z_A, Z_B, x_k, lw_k, n_phi=32, n_nodes=48))
    want = float(jax.scipy.special.logsumexp(lw_k))
    assert abs(got - want) < 1e-12, (got, want)
    # and the value must track log_w ONE FOR ONE, so the constant is pinned as a
    # constant rather than as one number that happened to come out right.
    shifted = float(JP.joint_lnL_phi_dense(Z_A, Z_B, xg, lw + 5.0, n_phi=32, n_nodes=48))
    assert abs(shifted - 5.0) < 1e-12, shifted


def test_the_gh_weight_density_is_the_volumetric_prior_pushed_onto_x():
    """``exp(C0) x^-4 dx`` IS ``p(d)|dd/dx|dx`` for the volumetric prior normalized over
    the support -- the continuum limit of what ``make_distance_grid`` builds discretely.

    Checked two independent ways, because the failure mode is a plausible-looking
    constant: the per-node density is compared against
    ``core._distance_prior_density`` pushed through the Jacobian (a function this module
    does not call), and the density is integrated over the support by ``scipy.quad`` (a
    rule that is not the one the nodes use).  A ``x^-3`` exponent, or a dropped 3, fails
    both.
    """
    from scipy.integrate import quad
    dref = 1000.0
    xlo, xhi = _support(dref)
    x_k, lw_k = JP.distance_gh_nodes(0.3, 4.0, xlo, xhi, 64)
    dx = np.diff(np.asarray(x_k))
    w = np.concatenate([0.5 * dx[:1], 0.5 * (dx[1:] + dx[:-1]), 0.5 * dx[-1:]])
    live = w > 0
    dens = np.exp(np.asarray(lw_k)[live]) / w[live]          # weight per unit x
    d = dref / np.asarray(x_k)[live]
    p_d = np.asarray(CORE._distance_prior_density(d, "euclidean"))
    p_d = p_d / ((D_MAX ** 3 - D_MIN ** 3) / 3.0)            # normalize over the support
    want = p_d * dref / np.asarray(x_k)[live] ** 2           # |dd/dx|
    assert np.allclose(dens, want, rtol=1e-12), (dens[:3], want[:3])
    C0 = 3.0 / (xlo ** -3.0 - xhi ** -3.0)
    total, _ = quad(lambda x: C0 * x ** -4.0, xlo, xhi, limit=200)
    assert abs(total - 1.0) < 1e-9, total


def test_a_narrowed_prior_range_is_carried_by_the_callers_weights():
    """``--limit-distance`` narrows the INTEGRATION range while leaving the prior
    normalized over the wider PHYSICAL range, and ``make_distance_grid`` records that as
    a total weight below one.  The GH constant is reconstructed from the support alone
    and cannot see it, so the caller's weights are folded in.

    Both halves are asserted, and the second is what makes this safe to ship: with no
    narrowing the correction must be identically zero to floating point, so the ordinary
    path is unchanged rather than approximately unchanged.
    """
    dref = 1000.0
    xlo, xhi = _support(dref)
    _, lw_plain = _grid(dref, 256)
    _, lw_box = _grid(dref, 256, d_prior_range=(D_MIN, 4.0 * D_MAX))
    off = float(jax.scipy.special.logsumexp(lw_box))
    assert off < -1.0, ("the fixture must actually narrow the prior, else this test is "
                        "vacuous; got %r" % off)
    _, a = JP.distance_gh_nodes(0.3, 4.0, xlo, xhi, 32)
    _, b = JP.distance_gh_nodes(0.3, 4.0, xlo, xhi, 32, log_w_grid=lw_box)
    _, c = JP.distance_gh_nodes(0.3, 4.0, xlo, xhi, 32, log_w_grid=lw_plain)
    fin = np.isfinite(np.asarray(a))
    assert np.allclose(np.asarray(b)[fin] - np.asarray(a)[fin], off, atol=1e-12)
    assert np.max(np.abs(np.asarray(c)[fin] - np.asarray(a)[fin])) < 1e-14, (
        "an unnarrowed grid must leave the constant untouched")


# ---------------------------------------------------------------- the placement

def test_the_profile_maximum_is_the_CONSTRAINED_one():
    """The unconstrained profile ``A^2/(2B)`` DIVERGES wherever the response vanishes, so
    ranking angles by it selects directions with no detector response and nothing else.
    Clipping x into the physical support FIRST is what makes the objective bounded; this
    asserts the returned maximum is that bounded one, against a brute-force torus scan,
    and that the fixture actually exhibits the divergence being guarded against.
    """
    A, B, dref = _tables()
    xlo, xhi = _support(dref)
    # A GENUINE RESPONSE NULL, constructed rather than hoped for: subtracting its own
    # minimum takes B(phi,u) down to zero somewhere on the torus, which is exactly the
    # sky/polarization direction with no detector response.  A generic faithful table has
    # B bounded away from zero and does NOT exhibit the divergence, so asserting on one
    # would have been a test that could not fail.
    g0 = jnp.linspace(0.0, 2 * jnp.pi, 512, endpoint=False)
    P0, U0 = jnp.meshgrid(g0, g0, indexing="ij")
    KS = (B.shape[1] - 1) // 2
    B = B.at[0, KS].add(-jnp.min(JP._AB_at(B, P0, U0)) + 0j)
    x_c, B_c, F_c, _ = JP.x_profile_peak(A, B, xlo, xhi)
    g = jnp.linspace(0.0, 2 * jnp.pi, 720, endpoint=False)
    PH, UU = jnp.meshgrid(g, g, indexing="ij")
    a = JP._AB_at(A, PH, UU)
    b = JP._AB_at(B, PH, UU)
    xh = jnp.clip(jnp.where(b > 0, a / jnp.where(b > 0, b, 1.0), jnp.inf), xlo, xhi)
    F_brute = float(jnp.max(xh * a - 0.5 * xh * xh * b))
    unconstrained = float(jnp.max(jnp.where(b > 1e-12, 0.5 * a * a
                                            / jnp.where(b > 1e-12, b, 1.0), -jnp.inf)))
    assert unconstrained > 10.0 * abs(F_brute), (
        "this fixture must actually exhibit the divergence, else the test is vacuous: "
        "%r vs %r" % (unconstrained, F_brute))
    # TWO-SIDED, and the upper side needs a REFINED reference: Newton beats a 720x720
    # scan (measured 865.81 against 862.85), so asserting F_c <= F_brute would fail for
    # the right reason.  The bound is a local scan at 1/500 of the coarse cell around the
    # coarse argmax, which resolves the maximum far better than the locator needs to be.
    i = int(jnp.argmax(xh * a - 0.5 * xh * xh * b))
    p0, u0 = float(PH.ravel()[i]), float(UU.ravel()[i])
    cell = 2 * np.pi / 720
    gp = jnp.linspace(p0 - cell, p0 + cell, 401)
    gu = jnp.linspace(u0 - cell, u0 + cell, 401)
    P2, U2 = jnp.meshgrid(gp, gu, indexing="ij")
    a2, b2 = JP._AB_at(A, P2, U2), JP._AB_at(B, P2, U2)
    x2 = jnp.clip(jnp.where(b2 > 0, a2 / jnp.where(b2 > 0, b2, 1.0), jnp.inf), xlo, xhi)
    F_ref = float(jnp.max(x2 * a2 - 0.5 * x2 * x2 * b2))
    assert F_ref > F_brute, "the refinement must actually improve on the coarse scan"
    assert float(F_c) >= F_brute, (float(F_c), F_brute)
    assert float(F_c) <= F_ref * (1.0 + 1e-9) + 1e-9, (float(F_c), F_ref)
    assert xlo <= float(x_c) <= xhi


def test_the_seed_lattice_is_offset_off_the_symmetry_points():
    """A seed ON a symmetry point of the torus sits at a stationary point: its gradient
    vanishes, it never moves, and the locator reports whatever that seed started near.
    For a 2-mode table those points are the multiples of pi/2 an unshifted lattice lands
    on.  Asserted structurally AND arithmetically, because the failure is silent: an
    unshifted lattice still returns a plausible number.
    """
    import inspect
    src = inspect.getsource(JP.x_profile_peak)
    assert "0.5 * cell" in src and "0.3183098861837907 * cell" in src, (
        "the seed lattice must be offset on BOTH axes")
    cell = 2.0 * np.pi / JP.GH_SEED_LATTICE
    base = np.arange(JP.GH_SEED_LATTICE) * cell
    sym = np.arange(9) * np.pi / 2.0
    for off in (0.5 * cell, 0.3183098861837907 * cell):
        d = np.min(np.abs((base + off)[:, None] - sym[None, :]))
        assert d > 1e-3, ("a seed lands on a symmetry point at offset %r" % off)


def test_the_newton_step_backtracks_rather_than_freezing():
    """Without a line search a rejected step leaves the seed where it is FOREVER -- there
    is no shorter step to fall back to -- and the locator settles for whatever that seed
    happened to reach.  Measured on a faithful table, freeze-on-rejection missed the
    profile maximum by 3.9e-4 of the amplitude at EVERY amplitude, which is 0.2 sigma of
    distance offset at rho 40 growing to 3.1 sigma at rho 640, because sigma shrinks as
    1/rho while the (phi, u) error does not.

    ASSERTED AGAINST A DENSE SCAN, and that is the second version of this test: the first
    asked only that the iteration beat its own seed lattice, which a frozen iteration
    still does easily -- it SURVIVED the mutation that removes the backtracking.  A
    relative 4e-4 shortfall is only visible against a reference that is itself close to
    the maximum.
    """
    A, B, dref = _tables(rho=40.0)
    xlo, xhi = _support(dref)
    _, _, F_c, _ = JP.x_profile_peak(A, B, xlo, xhi)
    g = jnp.linspace(0.0, 2 * jnp.pi, 720, endpoint=False)
    PH, UU = jnp.meshgrid(g, g, indexing="ij")
    a, b = JP._AB_at(A, PH, UU), JP._AB_at(B, PH, UU)
    xh = jnp.clip(jnp.where(b > 0, a / jnp.where(b > 0, b, 1.0), jnp.inf), xlo, xhi)
    F_scan = float(jnp.max(xh * a - 0.5 * xh * xh * b))
    assert float(F_c) >= F_scan, (
        "a 720x720 scan must not beat the locator; a frozen iteration lets it "
        "(%r vs %r)" % (float(F_c), F_scan))
    # and the seed lattice alone must NOT already clear that bar, or the assertion above
    # is about the seeds rather than about the iteration.
    n = JP.GH_SEED_LATTICE
    cell = 2.0 * np.pi / n
    base = jnp.arange(n) * cell
    P2, U2 = jnp.meshgrid(base + 0.5 * cell, base + 0.3183098861837907 * cell,
                          indexing="ij")
    a2, b2 = JP._AB_at(A, P2, U2), JP._AB_at(B, P2, U2)
    x2 = jnp.clip(jnp.where(b2 > 0, a2 / jnp.where(b2 > 0, b2, 1.0), jnp.inf), xlo, xhi)
    assert float(jnp.max(x2 * a2 - 0.5 * x2 * x2 * b2)) < F_scan


def test_the_window_is_scaled_by_the_local_distance_width():
    """The whole point: node spacing must follow 1/sqrt(B), so it shrinks with amplitude
    while the COUNT does not.  A placement that ignored B would be a fixed grid wearing
    an adaptive name."""
    xlo, xhi = _support(1000.0)
    # centred well inside the support, so the SUPPORT is not what sets the width
    widths = [float(JP.distance_gh_nodes(100.0, Bc, xlo, xhi, 32)[0][-1]
                    - JP.distance_gh_nodes(100.0, Bc, xlo, xhi, 32)[0][0])
              for Bc in (1.0, 100.0, 10000.0)]
    assert 9.5 < widths[0] / widths[1] < 10.5, widths
    assert 9.5 < widths[1] / widths[2] < 10.5, widths


def test_the_placement_carries_no_gradient():
    """Node positions come from an argmax over a seed lattice and from a clip, neither of
    which has a useful derivative; freezing them is what keeps the AD graph finite and
    makes a displaced node harmless (it adds to one trapezoid panel exactly what it
    removes from its neighbour).

    THE CENTRE AND THE WIDTH ARE PROBED SEPARATELY.  A single probe that scales both at
    once is satisfied by either freeze alone, and a mutation sweep found exactly that: it
    could not tell a load-bearing stop_gradient from a redundant one.
    """
    xlo, xhi = _support(1000.0)
    centre = lambda c: jnp.sum(JP.distance_gh_nodes(0.3 * c, 4.0, xlo, xhi, 16)[0])
    width = lambda w: jnp.sum(JP.distance_gh_nodes(0.3, 4.0 * w, xlo, xhi, 16)[0])
    assert float(jax.grad(centre)(1.0)) == 0.0, "the node CENTRE must be frozen"
    assert float(jax.grad(width)(1.0)) == 0.0, "the node WIDTH must be frozen"
    # and both probes must be non-vacuous: without the freeze the node positions really
    # do move with these arguments.
    n0 = np.asarray(JP.distance_gh_nodes(0.3, 4.0, xlo, xhi, 16)[0])
    n1 = np.asarray(JP.distance_gh_nodes(0.33, 4.0, xlo, xhi, 16)[0])
    n2 = np.asarray(JP.distance_gh_nodes(0.3, 4.4, xlo, xhi, 16)[0])
    assert not np.allclose(n0, n1) and not np.allclose(n0, n2)


# ---------------------------------------------------------------- the certificate

def _displace(n_sigma_off):
    """A stand-in for :func:`x_profile_peak` whose centre is deliberately wrong by a
    stated number of sigma.  Used to prove the bracket earns its cost, and to make a row
    actually decline."""
    real = JP.x_profile_peak

    def displaced(C_A, C_B, x_min, x_max, *a, **kw):
        x_c, B_c, F_c, ang = real(C_A, C_B, x_min, x_max, *a, **kw)
        moved = jnp.clip(x_c + float(n_sigma_off) / jnp.sqrt(B_c), x_min, x_max)
        return moved, B_c, F_c, ang

    return displaced


def _gauss_window(n, sigma_nodes, centre=None):
    k = jnp.arange(n) - (0.5 * (n - 1) if centre is None else centre)
    return -0.5 * (k / sigma_nodes) ** 2


def test_the_certificate_declines_a_window_that_misses_the_peak():
    """The certificate must be able to say NO, and on the failure it exists for.  Both
    arms use the SHAPE a correctly-sized window has -- 16 nodes over +-7 sigma is
    sigma = 1.07 nodes -- so the only difference is WHERE the peak sits."""
    xlo, xhi = _support(1000.0)
    n = 16
    x_k = jnp.linspace(0.2, 0.4, n)
    lw = jnp.zeros(n)
    ok_good, f_good = JP.gh_window_ok(_gauss_window(n, 1.07), lw, x_k, xlo, xhi)
    ok_bad, f_bad = JP.gh_window_ok(_gauss_window(n, 1.07, centre=n - 1.0),
                                    lw, x_k, xlo, xhi)
    assert bool(ok_good) and not bool(ok_bad), (bool(ok_good), bool(ok_bad))
    assert float(f_good) < np.log(JP.GH_OMITTED_MASS_TOL) < float(f_bad)


def test_no_fixed_nats_clearance_could_replace_the_tail_measurement():
    """A fixed clearance between the window maximum and its end nodes was the first
    draft, and it is wrong in both directions -- which is why the criterion is a
    fractional OMITTED MASS instead.

    Both halves are exhibited on the shape a correct window actually has (16 nodes over
    +-7 sigma is sigma = 1.07 nodes):

    * a PERFECT window's edge is only 0.5*7^2 = 24.5 nats down, so any clearance at or
      above that declines every correct row -- measured, that is what a 25-nat rule did;
    * marginalizing the angles can only BROADEN the distance marginal relative to the
      1/sqrt(B_c) the window is scaled by, and at a plausible 1.7x the same window's edge
      sits only 8.5 nats down while still omitting ~1e-7 of the mass.  Any clearance
      above that declines a row that is right to seven figures.

    The two therefore bracket the fixed threshold out of existence: it would have to be
    below 8.5 and at least 24.5 at once.  A missed peak is still refused.
    """
    xlo, xhi = _support(1000.0)
    n = 16
    x_k = jnp.linspace(0.2, 0.4, n)
    lw = jnp.zeros(n)
    perfect = _gauss_window(n, (n - 1) / (2.0 * JP.GH_N_SIGMA))
    broad = _gauss_window(n, 1.4 * (n - 1) / (2.0 * JP.GH_N_SIGMA))
    edge_perfect = float(jnp.max(perfect) - perfect[0])
    edge_broad = float(jnp.max(broad) - broad[0])
    assert 24.0 < edge_perfect < 25.0, edge_perfect
    assert 12.0 < edge_broad < 13.0, edge_broad
    ok_p, f_p = JP.gh_window_ok(perfect, lw, x_k, xlo, xhi)
    ok_b, f_b = JP.gh_window_ok(broad, lw, x_k, xlo, xhi)
    assert bool(ok_p) and bool(ok_b), (bool(ok_p), bool(ok_b))
    assert float(f_b) < np.log(JP.GH_OMITTED_MASS_TOL), (
        "the broadened window must still omit a negligible fraction, or the argument "
        "above is about a window that genuinely is bad; got %r" % float(f_b))
    missed = _gauss_window(n, (n - 1) / (2.0 * JP.GH_N_SIGMA), centre=n - 1.0)
    assert not bool(JP.gh_window_ok(missed, lw, x_k, xlo, xhi)[0])


def test_a_node_pinned_at_the_distance_support_is_exempt():
    """At the edge of the prior's support the integral stops because the PRIOR does; there
    is no omitted tail to bound.  Without the exemption every low-amplitude row -- where
    the window is wider than the support -- would decline, which is the honest answer to
    nothing.  The same profile is judged twice, differing only in whether the low end is
    pinned."""
    xlo, xhi = _support(1000.0)
    n = 16
    # peak AT the low end, decaying fast toward the high end: the high end certifies on
    # its own, so the verdict turns entirely on whether the low end is pinned.
    prof = -3.0 * jnp.arange(n)
    lw = jnp.zeros(n)
    free = jnp.linspace(0.2, 0.4, n)
    pinned = jnp.concatenate([jnp.array([xlo]), free[1:]])
    ok_free, _ = JP.gh_window_ok(prof, lw, free, xlo, xhi)
    ok_pin, _ = JP.gh_window_ok(prof, lw, pinned, xlo, xhi)
    assert not bool(ok_free), "an unpinned peak ON the low end must decline"
    assert bool(ok_pin), "a pinned low end has no omitted tail and must be exempt"


def test_the_bracket_pass_rescues_a_misplaced_analytic_centre(monkeypatch):
    """THE ABLATION, and the reason the scheme does not rest on its own locator.

    :func:`x_profile_peak` is an estimator, not a proven bound.  Displacing its answer by
    many sigma must be repaired by the bracketing pass and must NOT be repaired without
    it -- otherwise the first pass is cost with no purchase and should be deleted.
    """
    A, B, dref = _tables(rho=40.0)
    xlo, xhi = _support(dref)
    kw = dict(n_phi=160, n_nodes=64)
    true, ok_true, _ = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 32, **kw)
    assert bool(ok_true)

    monkeypatch.setattr(JP, "x_profile_peak", _displace(40.0))
    with_br, ok_br, i_br = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 32, **kw)
    no_br, ok_no, _ = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 32, bracket=False, **kw)
    assert abs(float(no_br) - float(true)) > 1.0, (
        "a 40-sigma displaced centre must actually break the single-pass placement, or "
        "this ablation proves nothing: %r vs %r" % (float(no_br), float(true)))
    assert abs(float(with_br) - float(true)) < 1e-3, (float(with_br), float(true))
    assert bool(ok_br) and not bool(ok_no)


def test_a_bracket_that_fails_to_enclose_the_peak_is_reported(monkeypatch):
    """The bracket is wide, not infinite.  When its own argmax lands on an END node the
    peak was not enclosed, and that has to reach ``ok`` -- the resolving window would
    otherwise certify a perfectly tidy window around the wrong place.

    Merely narrowing the bracket does NOT test this (the first draft did): a hair-wide
    window centred on the true peak still finds an interior maximum, correctly.  The
    condition is a centre outside the bracket's reach."""
    A, B, dref = _tables(rho=40.0)
    xlo, xhi = _support(dref)
    monkeypatch.setattr(JP, "x_profile_peak", _displace(1000.0))
    v, ok, info = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 16, n_phi=160, n_nodes=64)
    assert not bool(info["bracket_ok"]), (
        "a centre 1000 sigma off must not be rescued by a 26-sigma bracket")
    assert not bool(ok), "and that failure must reach ok"


def test_a_peak_at_the_distance_support_still_certifies():
    """A distance posterior that piles up at d_min or d_max is the CORRECT answer, not an
    unenclosed peak: there the integral stops because the prior's support does.  Both
    certificates exempt a pinned end for that reason.

    Without the exemption this fires on most noise-dominated time bins of a real run, and
    a label that fires everywhere means nothing -- so the exemption is what keeps the
    label informative, not what weakens it.  The fixture is asserted to actually put the
    peak outside the support, else it certifies for the ordinary reason.
    """
    A, B, dref = _tables(rho=40.0)
    xlo_full, xhi_full = _support(dref)
    x_c, B_c, _, _ = JP.x_profile_peak(A, B, xlo_full, xhi_full)
    # move the support so the peak sits beyond its upper edge
    xhi = float(x_c) * 0.5
    xlo = xhi / 1000.0
    x_c2, _, _, _ = JP.x_profile_peak(A, B, xlo, xhi)
    assert abs(float(x_c2) - xhi) < 1e-9 * xhi, (
        "the fixture must clip the peak to the support edge", float(x_c2), xhi)
    v, ok, info = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 16, n_phi=160, n_nodes=64)
    assert np.isfinite(float(v))
    assert bool(info["bracket_ok"]), "a peak pinned at the support is not an escape"
    assert bool(ok)


def test_the_bracket_verdict_reaches_ok_independently_of_the_window(monkeypatch):
    """The two certificates answer DIFFERENT questions -- did the bracket enclose the
    peak, and is the resolving window's omitted tail negligible -- and ``ok`` is their
    conjunction.

    Asserting that on an end-to-end failure does NOT test it: a centre displaced far
    enough to escape the bracket also produces a resolving window that misses the peak,
    so the window declines too and dropping the bracket term entirely goes unnoticed --
    a mutation that removed it SURVIVED the end-to-end test.  So the window certificate
    is neutralized here and only the bracket can speak.
    """
    A, B, dref = _tables(rho=40.0)
    xlo, xhi = _support(dref)
    monkeypatch.setattr(JP, "x_profile_peak", _displace(1000.0))
    monkeypatch.setattr(JP, "gh_window_ok",
                        lambda *a, **k: (jnp.asarray(True), jnp.asarray(-jnp.inf)))
    v, ok, info = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 16, n_phi=160, n_nodes=64)
    assert not bool(info["bracket_ok"])
    assert not bool(ok), "the bracket's verdict must reach ok on its own"


# ---------------------------------------------------------------- the value

def test_gh_agrees_with_a_refined_uniform_grid():
    """The only regime where a trustworthy cross-check exists.  Above it the uniform grid
    is the wrong reference, which is the entire point of this change -- so the reference
    is REFINED here rather than taken at the production 256 nodes, and the refinement is
    asserted to matter, else the test compares two converged answers and cannot fail."""
    A, B, dref = _tables(rho=40.0)
    xlo, xhi = _support(dref)
    kw = dict(n_phi=160, n_nodes=64)
    uni = {}
    for n in (256, 1024, 4096):
        xg, lw = _grid(dref, n)
        uni[n] = float(JP.joint_lnL_phi_dense(A, B, xg, lw, **kw))
    # RICHARDSON, because the uniform grid's own error is still 3e-4 at 4096 nodes and
    # comparing against it would charge GH for the reference's error.  A grid uniform in
    # d converges as 1/n here (measured: -5.85e-3, -1.44e-3, -3.43e-4, -6.87e-5 at
    # 256/1024/4096/16384 against a 65536-node reference), so u_inf = u_4096 +
    # (u_4096 - u_1024)/3.  The extrapolation is asserted to actually move the reference,
    # else it is a decoration.
    ref = uni[4096] + (uni[4096] - uni[1024]) / 3.0
    spread = abs(uni[4096] - uni[256])
    assert spread > 1e-3, (
        "the production 256-node grid must actually be inadequate here, else this "
        "compares two converged answers and cannot fail", uni)
    assert abs(ref - uni[4096]) > 1e-5, ("the extrapolation must matter", ref, uni)
    v, ok, _ = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 32, log_w_grid=_grid(dref)[1],
                                         **kw)
    assert bool(ok)
    assert abs(float(v) - ref) < 0.02 * spread, (float(v), ref, uni)


def test_the_node_count_is_a_knob_and_not_an_accuracy_dial():
    """16 nodes over +-7 sigma is a spacing of 0.93 sigma, where the trapezoid's
    Euler-Maclaurin error on a Gaussian is ~1e-8.  Doubling and quadrupling must
    therefore move nothing that matters -- and, unlike the uniform grid, the requirement
    must not grow with amplitude, so this is asserted at two rungs a factor of four apart
    in amplitude."""
    for rho in (20.0, 40.0):
        A, B, dref = _tables(rho=rho)
        xlo, xhi = _support(dref)
        kw = dict(n_phi=160, n_nodes=64)
        v = {n: float(JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, n, **kw)[0])
             for n in (16, 32)}
        assert abs(v[16] - v[32]) < 1e-5, (rho, v)


def test_the_field_evaluator_agrees_with_the_kernels_own_coefficients():
    """:func:`_AB_at` is a SECOND reading of the same tables the kernel reduces through
    ``_a_c1_c2``, and the two must agree or the placement is optimizing a different
    function from the one being integrated.  The convention that would silently differ is
    the conjugate half: only the ``k > 0`` harmonics are stored, so a missing factor of
    two, or dropping ``conj(D_{-q})``, changes the field without changing its shape."""
    A, B, dref = _tables()
    x = 0.7
    T = JP._joint_table(A, B, x)
    for phi in (0.0, 0.9, 2.3, 5.1):
        a, c1, c2 = JP._a_c1_c2(T, jnp.atleast_1d(phi))
        for u in (0.0, 1.3, 4.4):
            ref = float(JP._g_u(a[0], c1[0], c2[0], u, 0))
            got = float(x * JP._AB_at(A, jnp.atleast_1d(phi), jnp.atleast_1d(u))[0]
                        - 0.5 * x * x
                        * JP._AB_at(B, jnp.atleast_1d(phi), jnp.atleast_1d(u))[0])
            assert abs(got - ref) < 1e-9 * max(1.0, abs(ref)), (phi, u, got, ref)


# ---------------------------------------------------------------- the wiring

def test_gh_is_no_longer_refused_and_the_uniform_branch_still_runs(monkeypatch):
    """The refusal this branch removes, and its other half: with the variable unset the
    kernel must still take the uniform grid, and must take it through the same expression
    it always did -- so the two arms differ, but only through the distance nodes."""
    from RIFT.likelihood.jax_ile import core as _core
    import test_angle_marg_smoke as SM

    data = SM.make_synth(scale=1.0, seed=3)
    xg, lw = CORE.make_distance_grid(30.0, 3000.0, 64, distMpcRef=data.distMpcRef)
    kw = dict(interp="sinc", amp_sizing=64.0, return_lnLt=True)

    monkeypatch.setattr(_core, "_DISTMARG_GH_N", 0)
    plain = np.asarray(AM.fused_log_likelihood_distphipsimarg_peaklocal(
        data, SM.RA, SM.DEC, SM.INCL, xg, lw, **kw))
    direct = np.asarray(jax.vmap(jax.vmap(lambda a, b: JP.joint_lnL_phi_dense(
        a, b, xg, lw, n_phi=JP.required_n_phi(64.0),
        n_nodes=JP.u_nodes_in_use(64.0))))(
            *[jnp.moveaxis(jnp.asarray(t), (2, 3), (0, 1))
              for t in AM.angle_coefficient_tables(
                  data, SM.RA, SM.DEC, SM.INCL, "sinc")[:2]]))
    assert np.array_equal(plain, direct), (
        "with GH unset the entry must be BITWISE the plain kernel over the caller's grid")

    monkeypatch.setattr(_core, "_DISTMARG_GH_N", 16)
    with_gh = np.asarray(AM.fused_log_likelihood_distphipsimarg_peaklocal(
        data, SM.RA, SM.DEC, SM.INCL, xg, lw, **kw))
    assert np.all(np.isfinite(with_gh)) and with_gh.shape == plain.shape
    assert not np.array_equal(with_gh, plain), (
        "and with it set the distance rule must actually change, or the flag is inert")


def test_return_per_x_does_not_move_the_value():
    """The seam is exposed by ADDING a return, never by recomputing.  If these differ in
    any bit, the uniform path was restructured and its bit-identity claim is false."""
    A, B, dref = _tables()
    xg, lw = _grid(dref, 64)
    a = JP.joint_lnL_phi_dense(A, B, xg, lw, n_phi=96, n_nodes=48)
    b, per_x = JP.joint_lnL_phi_dense(A, B, xg, lw, n_phi=96, n_nodes=48,
                                      return_per_x=True)
    assert float(a) == float(b)
    assert per_x.shape == (64,)


def test_the_gh_window_record_reaches_the_host():
    """A warning printed from inside jit does not stop anything, so the condition is
    recorded on the HOST for the driver to label the artifact.  Same contract, and the
    same caveats, as the amplitude failsafe -- including that the callback must sit
    INSIDE lax.cond, because an unconditional callback fires once per likelihood
    evaluation and destroys accelerator throughput even when nothing ever declines."""
    import inspect
    AM.reset_gh_window_record()
    st = AM.gh_window_state()
    assert st["declined"] is False and st["n_declined"] == 0
    for fn in (AM.gh_window_state, AM.reset_gh_window_record):
        assert "effects_barrier" in inspect.getsource(fn)
    src = inspect.getsource(AM._gh_window_failsafe)
    # THE CALLBACK ITSELF, not "some lax.cond precedes some callback".  The looser
    # ordering check was satisfied by the lax.cond around the debug.print, so a mutation
    # that pulled the callback out of its own cond SURVIVED it -- and an unconditional
    # host callback fires once per likelihood evaluation, per proposal, per chain.
    assert "lambda n_: jax.debug.callback(_record_gh_window" in src, (
        "the host callback must be the body of a lax.cond branch")
    assert "lambda n_: jax.debug.print(" in src
    AM._record_gh_window(3, 10, "peak-local")
    st = AM.gh_window_state(barrier=False)
    assert st["declined"] is True and st["n_declined"] == 3
    AM.reset_gh_window_record()
    assert AM.gh_window_state()["declined"] is False


def test_the_declining_row_actually_reaches_the_host_record(monkeypatch):
    """Not just that the record EXISTS, but that a declining call fills it.  A callback
    wired to a condition that never fires is the inert-guard shape this module keeps
    being bitten by."""
    from RIFT.likelihood.jax_ile import core as _core
    import test_angle_marg_smoke as SM

    data = SM.make_synth(scale=1.0, seed=3)
    xg, lw = CORE.make_distance_grid(30.0, 3000.0, 64, distMpcRef=data.distMpcRef)
    monkeypatch.setattr(_core, "_DISTMARG_GH_N", 16)
    monkeypatch.setattr(JP, "x_profile_peak", _displace(1000.0))
    AM.reset_gh_window_record()
    AM.fused_log_likelihood_distphipsimarg_peaklocal(
        data, SM.RA, SM.DEC, SM.INCL, xg, lw, interp="sinc", amp_sizing=64.0,
        return_lnLt=True)
    st = AM.gh_window_state()
    AM.reset_gh_window_record()
    assert st["declined"] is True and st["n_declined"] > 0, st


def test_the_batch_memory_model_follows_the_gh_node_count(monkeypatch):
    """THE TRAP THIS BRANCH IS MOST LIKELY TO SPRING.  Under GH the kernel does not
    evaluate ``x_grid`` at all -- it places its own nodes and reads only the support --
    so a guard still modelling the 256-node grid over-reads by 8x and REFUSES the very
    calls this change exists to make possible.  It is the ``u_nodes_in_use`` trap in the
    other direction: there the kernel grew past the guard, here it shrank below it."""
    class _D:
        npts = 64
        lms = np.array([[2, 2], [2, -2]])

    class _L:
        data = _D()
        x_grid = np.linspace(0.1, 100.0, 256)
        angle_marg_scheme = "peak-local"
        angle_marg_info = {"amp_sizing": 450.0}

    like = _L()
    monkeypatch.setattr(SAMP, "_GH_NODES", 0)
    off = SAMP._peaklocal_bytes_per_sample_pt(like)
    monkeypatch.setattr(SAMP, "_GH_NODES", 16)
    on = SAMP._peaklocal_bytes_per_sample_pt(like)
    assert on < off / 4.0, (
        "the model must shrink with the GH node count, or the guard keeps refusing the "
        "calls this change enables: %d vs %d" % (on, off))
    monkeypatch.setattr(SAMP, "_GH_NODES", 64)
    assert SAMP._peaklocal_bytes_per_sample_pt(like) > on, (
        "and it must still respond to the node count")


def test_too_few_nodes_is_refused_rather_than_silently_wrong():
    """Below four nodes the composite trapezoid has no interior panel and the end-node
    certificate has no second node to read a decay from.  Returning a number from a rule
    that is not the rule described is the failure this refuses."""
    A, B, dref = _tables()
    xlo, xhi = _support(dref)
    for n in (1, 2, 3):
        with pytest.raises(ValueError, match="too few nodes"):
            JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, n, n_phi=64, n_nodes=48)
    JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 4, n_phi=64, n_nodes=48)


def test_the_driver_resets_and_labels_on_the_distance_window():
    """A host record with no consumer is an inert guard.  The driver must (a) clear the
    record per EVENT -- a batch run analyzes several, and a decline on event 0 must not
    label event 1 -- and (b) turn a decline into an artifact label, kept SEPARATE from
    the angle-grid label because the angle grids can be perfect while the distance
    marginal is truncated."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[2].joinpath(
        "bin", "integrate_likelihood_extrinsic_jax").read_text()
    assert "reset_gh_window_record()" in src
    assert "SUSPECT-DISTANCE-WINDOW" in src
    assert "gh_window_state()" in src
    i_reset = src.index("reset_amp_failsafe()")
    assert abs(src.index("reset_gh_window_record()") - i_reset) < 200, (
        "the two per-event resets must sit together, or one will be moved without the "
        "other")


def test_no_mode_content_restriction_is_imposed_on_this_branch():
    """The ``m_max <= 2`` precondition belongs to the LAPLACE scheme's psi-MARGINAL node
    placement, which is DERIVED from the A0 == 0 / B1 == 0 identity.  peak-local does not
    marginalize psi analytically and places its nodes on the joint (x,phi,u) profile
    maximum, so that precondition does not transfer -- and quietly inheriting it would
    refuse valid higher-mode data.  Asserted on an m_max = 4 dataset."""
    A, B, dref = _tables(rho=30.0, seed=11,
                         modes=((2, 2), (2, -2), (4, 4), (4, -4)))
    xlo, xhi = _support(dref)
    v, ok, _ = JP.joint_lnL_phi_dense_gh(A, B, xlo, xhi, 16, n_phi=192, n_nodes=48)
    assert np.isfinite(float(v)) and bool(ok)
