"""Tests for fixed-shape all-variable multi-peak marginalization."""

import types

import numpy as np
import pytest
from scipy import integrate, special

jax = pytest.importorskip("jax")
import jax.numpy as jnp

jax.config.update("jax_enable_x64", True)

from RIFT.likelihood.jax_ile import all_axis_peaklocal as AAP
from RIFT.likelihood.jax_ile import anglemarg as AM


def _problem(n=129):
    span = n - 1.0
    t = np.arange(n, dtype=float)
    k0, kt, kp, ku, B = 5.0, 15.0, 8.0, 6.0, 10.0
    C_A = np.zeros((3, 3, n), dtype=np.complex128)
    C_A[0, 1] = k0 - kt * np.cos(2.0 * np.pi * t / span)
    C_A[2, 1] = 0.5 * kp
    C_A[0, 0] = 0.5 * ku
    C_A[0, 2] = 0.5 * ku
    C_B = np.zeros((5, 5), dtype=np.complex128)
    C_B[0, 2] = B
    constants = dict(span=span, k0=k0, kt=kt, kp=kp, ku=ku, B=B)
    return C_A, C_B, constants


def _joint_peak(constants):
    A = sum(constants[k] for k in ("k0", "kt", "kp", "ku"))
    B = constants["B"]
    x = (A + np.sqrt(A * A - 16.0 * B)) / (2.0 * B)
    centers = np.asarray([
        [constants["span"] / 2.0, 0.0, 0.0, x],
        [constants["span"] / 2.0, np.pi, 0.0, x],
    ])
    hessian = np.zeros((2, 4, 4))
    diagonal = np.asarray([
        -x * constants["kt"] * (2.0 * np.pi / constants["span"]) ** 2,
        -4.0 * x * constants["kp"],
        -x * constants["ku"],
        -B + 4.0 / (x * x),
    ])
    hessian[:, np.arange(4), np.arange(4)] = diagonal
    return centers, hessian


def _analytic_log_integral(constants, x_min, x_max):
    c = constants

    def log_i0(z):
        return np.log(special.i0e(z)) + abs(z)

    def log_integrand(x):
        return (-4.0 * np.log(x) - 0.5 * c["B"] * x * x + c["k0"] * x
                + log_i0(c["kt"] * x) + log_i0(c["kp"] * x)
                + log_i0(c["ku"] * x))

    probe = np.linspace(x_min, x_max, 2001)
    shift = max(log_integrand(x) for x in probe)
    value = integrate.quad(
        lambda x: np.exp(log_integrand(x) - shift), x_min, x_max,
        epsabs=1.0e-13, epsrel=1.0e-13, limit=500)[0]
    return (shift + np.log(value) + np.log(c["span"])
            + 2.0 * np.log(2.0 * np.pi))


def test_angle_tables_forward_primitive_guard_and_report_support(monkeypatch):
    data = types.SimpleNamespace(lms=np.asarray([[2, 2]]), npts=5)
    seen = []

    def fake_accumulate(data, ra, dec, psi, incl, phi, interp,
                        phase_marginalization, guard=0):
        seen.append(int(guard))
        shape = (ra.shape[0], data.npts + 2 * int(guard))
        return jnp.ones(shape, dtype=jnp.complex128), jnp.ones(shape)

    monkeypatch.setattr(AM, "_accumulate_unit", fake_accumulate)
    C_A, C_B, meta = AM.angle_coefficient_tables(
        data, jnp.asarray([0.1]), jnp.asarray([0.2]), jnp.asarray([0.3]),
        guard=3)
    assert C_A.shape == (3, 3, 1, 11)
    assert C_B.shape == (5, 5, 1, 11)
    assert meta["guard"] == 3 and meta["ntime"] == 11
    assert seen and set(seen) == {3}


def test_uv_summary_bounds_the_exact_norm_and_collapses_time():
    rng = np.random.default_rng(813)
    C_B = np.zeros((5, 5), dtype=np.complex128)
    C_B[0, 2] = 20.0
    perturbation = (rng.normal(size=(5, 5))
                    + 1j * rng.normal(size=(5, 5))) * 0.02
    perturbation[0, 2] = 0.0
    C_B += perturbation
    repeated = np.repeat(C_B[..., None], 17, axis=-1)
    summary = AAP.summarize_uv_norm_table(repeated)

    phi = rng.uniform(0.0, 2.0 * np.pi, 1000)
    u = rng.uniform(0.0, 2.0 * np.pi, 1000)
    values = np.asarray(jax.vmap(
        lambda p, q: AAP._angular_field(jnp.asarray(C_B), p, q))(
            jnp.asarray(phi), jnp.asarray(u)))
    assert summary.time_invariant
    assert values.min() >= summary.b_lower - 1.0e-12
    assert values.max() <= summary.b_upper + 1.0e-12
    assert summary.summary_build_count == 1
    assert summary.input_harmonic_coefficients == repeated.size


def test_uv_envelope_ranks_the_interior_time_mode_without_dense_starts():
    C_A, C_B, constants = _problem()
    summary = AAP.summarize_uv_norm_table(C_B)
    starts, envelope = AAP.rank_time_starts_from_uv(
        C_A, summary, 0.2, 7.0, max_starts=3, min_separation=4)
    assert starts[0] == int(constants["span"] // 2)
    assert len(starts) <= 3
    assert envelope[starts[0]] == pytest.approx(envelope.max())


def test_uv_ranked_time_start_feeds_algebraic_angles_and_analytic_distance():
    C_A, C_B, constants = _problem(65)
    summary = AAP.summarize_uv_norm_table(C_B)
    time_starts, _ = AAP.rank_time_starts_from_uv(
        C_A, summary, 0.2, 7.0, max_starts=1, min_separation=4)
    # This deliberately sparse symmetric polynomial includes zero/infinite
    # generalized roots; the authoritative report, not NumPy's intermediate
    # negative-power warning, carries their classification.
    with np.errstate(invalid="ignore", divide="ignore"):
        starts, algebraic_ok, reports = AAP.algebraic_angle_starts_from_uv(
            C_A, summary, time_starts, 0.2, 7.0)

    # The exact enumerator supplies the two maxima without a dense seed lattice.
    # Its independent completeness result is carried separately; definite
    # maxima remain useful targeting data if a numerically marginal projection
    # makes that result false on another LAPACK implementation.
    assert starts.shape == (2, 4)
    assert algebraic_ok == bool(reports[0]["ok"])
    assert len(reports) == 1 and reports[0]["n_maxima"] == 2
    assert np.allclose(starts[:, 0], constants["span"] / 2.0)
    assert np.all((starts[:, 3] > 0.2) & (starts[:, 3] < 7.0))


def test_joint_start_lattice_is_harmonic_order_sized_not_snr_sized():
    C_A, C_B, constants = _problem(65)
    summary = AAP.summarize_uv_norm_table(C_B)
    low = AAP.rank_joint_starts_from_uvq(
        C_A, summary, 0.2, 7.0, max_time_starts=1, max_starts=8)
    high = AAP.rank_joint_starts_from_uvq(
        32.0 * C_A, summary, 0.2, 7.0,
        max_time_starts=1, max_starts=8)

    assert low.starts.shape[1] == 4
    assert low.time_starts[0] == int(constants["span"] // 2)
    assert low.n_phi_lattice == high.n_phi_lattice == 17
    assert low.n_u_lattice == high.n_u_lattice == 9
    assert low.n_lattice_evaluations == high.n_lattice_evaluations == 17 * 9 * 65
    assert low.n_exact_symmetry_shifts == high.n_exact_symmetry_shifts == 2
    assert low.capacity_ok and high.capacity_ok
    assert np.all((low.starts[:, 3] >= 0.2) & (low.starts[:, 3] <= 7.0))


def test_joint_start_guard_discards_support_before_ranking():
    C_A, C_B, constants = _problem(65)
    guard = 8
    guarded = np.full(C_A.shape[:-1] + (65 + 2 * guard,),
                      1.0e8 + 2.0e8j, dtype=np.complex128)
    guarded[..., guard:-guard] = C_A
    summary = AAP.summarize_uv_norm_table(C_B)
    plan = AAP.rank_joint_starts_from_uvq(
        guarded, summary, 0.2, 7.0, time_guard=guard,
        max_time_starts=1, max_starts=8)

    assert plan.time_starts.tolist() == [int(constants["span"] // 2)]
    assert plan.n_lattice_evaluations == 17 * 9 * 65


def test_joint_start_capacity_declines_instead_of_silent_truncation():
    C_A, C_B, _ = _problem(65)
    summary = AAP.summarize_uv_norm_table(C_B)
    plan = AAP.rank_joint_starts_from_uvq(
        C_A, summary, 0.2, 7.0, max_time_starts=1, max_starts=1)
    assert plan.starts.shape == (1, 4)
    assert plan.n_candidates_before_cap > 1
    assert not plan.capacity_ok


def test_exact_coefficient_symmetry_completes_quadrupole_orbit():
    C_A = np.zeros((3, 3, 9), dtype=np.complex128)
    C_A[2, 0] = 1.0
    C_A[2, 2] = 0.7
    C_B = np.zeros((5, 5), dtype=np.complex128)
    C_B[0, 2] = 2.0
    shifts = AAP._exact_angular_translation_symmetries(C_A, C_B)
    want = np.asarray([
        [0.0, 0.0], [np.pi, 0.0],
        [0.5 * np.pi, np.pi], [1.5 * np.pi, np.pi]])
    assert shifts.shape == (4, 2)
    for shift in want:
        assert np.min(np.linalg.norm(shifts - shift, axis=1)) < 1.0e-12


def test_mode_stationarity_uses_curvature_scaled_displacement_at_high_snr():
    point = np.asarray([[10.0, 1.0, 2.0, 1.5]])
    value = np.asarray([10000.0])
    gradient = np.asarray([[2.0e-4, 0.0, 0.0, 0.0]])
    curvature = np.asarray([[35.0, 200.0, 1000.0, 100000.0]])
    selected, stationary = AAP.select_refined_modes(
        point, value, gradient, curvature, max_modes=1,
        gradient_tol=2.0e-6)
    assert stationary.tolist() == [True]
    assert selected.tolist() == [0]

    curvature[0, 0] = 1.0
    selected, stationary = AAP.select_refined_modes(
        point, value, gradient, curvature, max_modes=1,
        gradient_tol=2.0e-6)
    assert stationary.tolist() == [False]
    assert selected.size == 0


def test_jax_gradient_hessian_refinement_finds_both_angular_modes():
    C_A, C_B, constants = _problem(65)
    centers, _ = _joint_peak(constants)
    starts = centers + np.asarray([
        [1.3, 0.17, -0.11, -0.13],
        [-1.1, -0.15, 0.09, 0.16],
    ])
    refined, value, gradient, hessian, curvature = AAP.refine_all_axis_starts(
        C_A, C_B, starts, 0.2, 7.0, iterations=14)
    refined = np.asarray(refined)
    angular_error = np.abs(
        (refined[:, 1:3] - centers[:, 1:3] + np.pi) % (2.0 * np.pi) - np.pi)
    assert np.max(np.abs(refined[:, [0, 3]] - centers[:, [0, 3]])) < 2.0e-7
    assert np.max(angular_error) < 2.0e-7
    assert np.max(np.linalg.norm(np.asarray(gradient), axis=1)) < 2.0e-7
    assert np.all(np.asarray(curvature) > 0.0)
    assert np.all(np.isfinite(np.asarray(value)))
    assert np.all(np.isfinite(np.asarray(hessian)))
    selected, stationary = AAP.select_refined_modes(
        refined, value, gradient, curvature, max_modes=4)
    assert np.all(stationary)
    assert selected.shape == (2,)
    with pytest.raises(ValueError, match="exceeds fixed plan capacity"):
        AAP.select_refined_modes(
            refined, value, gradient, curvature, max_modes=1)
    with pytest.raises(ValueError, match="must be positive"):
        AAP.select_refined_modes(
            refined, value, gradient, curvature, max_modes=0)


def test_multimode_local_primitive_matches_oracle_but_stays_uncertified():
    C_A, C_B, constants = _problem()
    centers, hessian = _joint_peak(constants)
    transforms, half_widths = AAP.mode_local_geometry(hessian, w_sigma=5.0)
    x_min, x_max = 0.2, 7.0
    truth = _analytic_log_integral(constants, x_min, x_max)
    plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=4,
        local_transforms=transforms, local_radius=5.0,
        outside_log_bound=np.inf,
        enumeration_complete=True, outside_bound_certified=False)
    value, ok, ledger = AAP.all_axis_peak_local_marginalize(
        C_A, C_B, plan, x_min, x_max, local_order=13, check_order=19,
        quadrature_tol_nats=1.0e-4)

    assert not bool(ok)
    assert bool(ledger["decline_incomplete"])
    assert abs(float(value) - truth) < 2.0e-4
    assert int(ledger["n_modes"]) == 2
    assert int(ledger["n_local_evaluations_hi"]) == 2 * 19 ** 4
    assert int(ledger["n_selected_time_points_hi"]) == 2 * 19
    assert int(ledger["n_time_frequency_terms_hi"]) == (
        2 * 19 * (2 * C_A.shape[-1] - 2) * np.prod(C_A.shape[:-1]))
    assert int(ledger["n_angle_harmonic_terms_hi"]) == (
        2 * 19 ** 3 * (np.prod(C_A.shape[:-1]) + C_B.size))
    assert int(ledger["workspace_bytes_hi"]) < 8_000_000
    assert bool(ledger["reconciles"])


def test_empirical_enrichment_accepts_without_claiming_global_proof():
    C_A, C_B, constants = _problem(33)
    C_A *= 0.1
    x_min, x_max = 0.5, 2.0
    centers = np.asarray([[
        constants["span"] / 2.0, np.pi, np.pi,
        0.5 * (x_min + x_max)]])
    transforms = np.asarray([np.diag([
        constants["span"] / 2.0, np.pi, np.pi,
        0.5 * (x_max - x_min)])])
    plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=2, local_transforms=transforms,
        local_radius=1.0, outside_bound_certified=False,
        time_reconstruction_certified=True)
    value, accepted, ledger = AAP.empirical_enrichment_marginalize(
        C_A, C_B, plan, plan, x_min, x_max,
        convergence_tol_nats=1.0e-3)

    assert np.isfinite(float(value))
    assert bool(accepted)
    assert bool(ledger["acceptance_is_empirical_enrichment"])
    assert not bool(ledger["global_completeness_certified"])
    assert not bool(ledger["empirical_value_error_certified"])
    assert float(ledger["convergence_error"]) <= 1.0e-3
    assert bool(ledger["mode_nesting_ok"])
    assert not bool(ledger["fallback_required"])
    assert bool(ledger["reconciles"])

    shifted = centers.copy()
    shifted[:, 1] += 0.1
    shifted_plan = AAP.make_all_axis_mode_plan(
        shifted, max_modes=2, local_transforms=transforms,
        local_radius=1.0, outside_bound_certified=False,
        time_reconstruction_certified=True)
    _, accepted, nesting_ledger = AAP.empirical_enrichment_marginalize(
        C_A, C_B, plan, shifted_plan, x_min, x_max,
        convergence_tol_nats=1.0e-3)
    assert not bool(accepted)
    assert bool(nesting_ledger["decline_mode_nesting"])
    assert bool(nesting_ledger["reconciles"])

    truncated_plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=2, local_transforms=transforms,
        local_radius=1.0, outside_bound_certified=False,
        time_reconstruction_certified=True, discovery_capacity_ok=False)
    _, accepted, capacity_ledger = AAP.empirical_enrichment_marginalize(
        C_A, C_B, truncated_plan, plan, x_min, x_max,
        convergence_tol_nats=1.0e-3)
    assert not bool(accepted)
    assert bool(capacity_ledger["decline_capacity"])
    assert bool(capacity_ledger["fallback_required"])
    assert not bool(capacity_ledger["decline_is_waveform_failure"])
    assert bool(capacity_ledger["reconciles"])


def test_missing_completeness_declines_to_reserve_not_waveform_failure():
    C_A, C_B, constants = _problem(33)
    centers, hessian = _joint_peak(constants)
    transforms, half_widths = AAP.mode_local_geometry(hessian, w_sigma=3.0)
    plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=4, local_transforms=transforms,
        local_radius=3.0, enumeration_complete=False,
        outside_bound_certified=False)
    value, ok, ledger = AAP.all_axis_peak_local_marginalize(
        C_A, C_B, plan, 0.2, 7.0, local_order=3, check_order=5)

    assert np.isfinite(float(value))
    assert not bool(ok)
    assert bool(ledger["fallback_required"])
    assert bool(ledger["decline_incomplete"])
    assert not bool(ledger["decline_is_waveform_failure"])
    assert bool(ledger["reconciles"])


def test_two_guard_local_integral_validates_same_target_window():
    C_A, C_B, constants = _problem(33)
    guard = 32
    support_time = np.arange(-guard, C_A.shape[-1] + guard, dtype=float)
    guarded = np.zeros(C_A.shape[:-1] + (support_time.size,), dtype=np.complex128)
    guarded[0, 1] = (constants["k0"] - constants["kt"]
                     * np.cos(2.0 * np.pi * support_time / constants["span"]))
    guarded[2, 1] = 0.5 * constants["kp"]
    guarded[0, 0] = 0.5 * constants["ku"]
    guarded[0, 2] = 0.5 * constants["ku"]
    np.testing.assert_allclose(guarded[..., guard:-guard], C_A, atol=1e-14)

    centers, hessian = _joint_peak(constants)
    transforms, _ = AAP.mode_local_geometry(hessian, w_sigma=3.0)
    plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=4, local_transforms=transforms,
        local_radius=3.0, outside_bound_certified=False,
        time_reconstruction_certified=True)
    value, ok, ledger = AAP.all_axis_peak_local_marginalize(
        guarded, C_B, plan, 0.2, 7.0,
        local_order=7, check_order=11, time_guard=guard,
        time_guard_tol_nats=1.0e-3)

    assert np.isfinite(float(value))
    assert not bool(ok)  # outside mass is deliberately still unwarranted
    assert bool(ledger["time_guard_validated"])
    assert bool(ledger["time_reconstruction_warranted"])
    assert int(ledger["time_guard"]) == 32
    assert int(ledger["time_guard_inner"]) == 16
    assert float(ledger["time_guard_error"]) <= 1.0e-3
    assert int(ledger["n_guard_local_evaluations_hi"]) == 2 * 11 ** 4
    assert int(ledger["n_total_local_evaluations_hi"]) == 4 * 11 ** 4
    assert int(ledger["n_total_time_frequency_terms_hi"]) == (
        int(ledger["n_time_frequency_terms_hi"])
        + int(ledger["n_guard_time_frequency_terms_hi"]))
    assert int(ledger["workspace_bytes_peak_bound_hi"]) == max(
        int(ledger["workspace_bytes_hi"]),
        int(ledger["workspace_bytes_guard_hi"]))
    assert bool(ledger["decline_incomplete"])
    assert bool(ledger["reconciles"])

    # Corrupt only support discarded by the inner guard.  Integer target
    # samples remain unchanged, but the outer Fourier seam rings into the local
    # nodes; the two-guard comparison must see it rather than blessing exact
    # retained-sample parity or trusting the plan's stale external warrant.
    bad = guarded.copy()
    bad[0, 1, :guard // 2] += 1.0e4
    _, _, bad_ledger = AAP.all_axis_peak_local_marginalize(
        bad, C_B, plan, 0.2, 7.0,
        local_order=7, check_order=11, time_guard=guard,
        time_guard_tol_nats=1.0e-3)
    assert not bool(bad_ledger["time_guard_validated"])
    assert not bool(bad_ledger["time_reconstruction_warranted"])
    assert float(bad_ledger["time_guard_error"]) > 1.0e-3


def test_certified_omitted_mass_can_cover_an_incomplete_root_report():
    C_A, C_B, constants = _problem(33)
    scale = 0.1
    C_A *= scale
    constants = dict(constants)
    for name in ("k0", "kt", "kp", "ku"):
        constants[name] *= scale
    x_min, x_max = 0.5, 2.0
    truth = _analytic_log_integral(constants, x_min, x_max)
    # One diagonal affine region is exactly the complete t x phi x u x x
    # support.  Its complement has zero measure, so -inf is an actual outside
    # integral bound rather than a fabricated tail assertion.
    centers = np.asarray([[
        constants["span"] / 2.0, np.pi, np.pi,
        0.5 * (x_min + x_max)]])
    transforms = np.asarray([np.diag([
        constants["span"] / 2.0, np.pi, np.pi,
        0.5 * (x_max - x_min)])])
    plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=2,
        local_transforms=transforms, local_radius=1.0,
        outside_log_bound=-np.inf,
        # This models an incomplete/degenerate root report.  The exact full-
        # support cover, not the root count, owns the science error budget.
        enumeration_complete=False, outside_bound_certified=True,
        # This fixture is itself an exact finite reflected cosine series.  Real
        # packets need the independent two-guard convergence warrant.
        time_reconstruction_certified=True)
    value, ok, ledger = AAP.all_axis_peak_local_marginalize(
        C_A, C_B, plan, x_min, x_max, local_order=13, check_order=19,
        quadrature_tol_nats=2.0e-5)

    assert bool(ok)
    assert float(value) == pytest.approx(truth, abs=2.0e-5)
    assert not bool(ledger["enumeration_complete"])
    assert bool(ledger["outside_bound_certified"])
    assert not bool(ledger["fallback_required"])
    assert bool(ledger["reconciles"])


@pytest.mark.parametrize("mutation", ("nan", "upper", "negative_diagonal"))
def test_plan_rejects_geometry_that_does_not_match_the_integrated_region(mutation):
    _, _, constants = _problem(33)
    centers, hessian = _joint_peak(constants)
    transforms, _ = AAP.mode_local_geometry(hessian, w_sigma=3.0)
    if mutation == "nan":
        transforms[0, 0, 0] = np.nan
    elif mutation == "upper":
        transforms[0, 0, 1] = 1.0e-4
    else:
        transforms[0, 0, 0] *= -1.0
    with pytest.raises(ValueError):
        AAP.make_all_axis_mode_plan(
            centers, max_modes=4, local_transforms=transforms,
            local_radius=3.0)


def test_fixed_plan_is_transform_compatible_without_claiming_derivative_accuracy():
    C_A, C_B, constants = _problem(33)
    centers, hessian = _joint_peak(constants)
    transforms, half_widths = AAP.mode_local_geometry(hessian, w_sigma=3.0)
    plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=4, local_transforms=transforms,
        local_radius=3.0)
    C_A = jnp.asarray(C_A)
    C_B = jnp.asarray(C_B)

    @jax.jit
    def value(scale):
        answer, _, _ = AAP.all_axis_peak_local_marginalize(
            scale * C_A, C_B, plan, 0.2, 7.0,
            local_order=3, check_order=5)
        return answer

    got = value(1.0)
    gradient = jax.grad(value)(1.0)
    hessian_value = jax.hessian(value)(1.0)
    assert np.all(np.isfinite(np.asarray([got, gradient, hessian_value])))
    _, _, ledger = AAP.all_axis_peak_local_marginalize(
        C_A, C_B, plan, 0.2, 7.0, local_order=3, check_order=5)
    assert bool(ledger["fixed_plan_autodiff_only"])
    assert not bool(ledger["derivative_warrant_certified"])


def test_padded_fixed_plan_survives_outer_vmap():
    C_A, C_B, constants = _problem(33)
    centers, hessian = _joint_peak(constants)
    transforms, _ = AAP.mode_local_geometry(hessian, w_sigma=3.0)
    plan = AAP.make_all_axis_mode_plan(
        centers, max_modes=4, local_transforms=transforms,
        local_radius=3.0)

    def value(table):
        return AAP.all_axis_peak_local_marginalize(
            table, C_B, plan, 0.2, 7.0,
            local_order=3, check_order=5)[0]

    batch = jnp.asarray(np.stack((C_A, 1.01 * C_A)))
    result = jax.jit(jax.vmap(value))(batch)
    assert result.shape == (2,)
    assert np.all(np.isfinite(np.asarray(result)))
