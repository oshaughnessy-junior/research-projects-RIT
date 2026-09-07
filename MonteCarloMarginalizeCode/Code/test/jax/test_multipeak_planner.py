"""Load-bearing tests for the U,V/Q multi-peak diagnostic planner."""

import os

import jax
import numpy as np
import pytest

jax.config.update("jax_enable_x64", True)

from RIFT.likelihood.jax_ile import multipeak_planner as planner  # noqa: E402


def _synthetic_tables(n_time=9):
    """Small reflected-polynomial problem with an interior four-axis peak."""
    time = np.arange(n_time, dtype=float)
    C_A = np.zeros((3, 3, n_time), dtype=np.complex128)
    C_B = np.zeros((5, 5), dtype=np.complex128)
    # DCT-compatible time dependence, with its interior maximum at t=4.
    C_A[0, 1] = 20.0 - 2.0 * np.cos(2.0 * np.pi * time / (n_time - 1))
    C_A[2, 0] = 0.25
    C_A[2, 2] = 0.25
    C_B[0, 2] = 4.0
    C_B[2, 1] = 0.02
    C_B[2, 3] = 0.02
    return C_A, C_B


def test_uv_summary_rejects_time_dependent_norm():
    _, C_B = _synthetic_tables()
    repeated = np.repeat(C_B[..., None], 7, axis=-1)
    summary = planner.summarize_uv_norm_table(repeated)
    assert summary.time_invariant
    repeated[1, 1, 3] += 1.0e-3
    changed = planner.summarize_uv_norm_table(repeated)
    assert not changed.time_invariant
    C_A, _ = _synthetic_tables()
    with pytest.raises(ValueError, match="arrival-time-dependent"):
        planner.rank_joint_starts_from_uvq(C_A, changed, 1.0, 8.0)


def test_exact_symmetry_expansion_is_scale_invariant():
    C_A, C_B = _synthetic_tables()
    summary = planner.summarize_uv_norm_table(C_B)
    first = planner.rank_joint_starts_from_uvq(
        C_A, summary, 1.0, 8.0, max_time_starts=2, max_starts=24)

    assert first.symmetry.certified
    assert first.symmetry.group_order == 4
    np.testing.assert_allclose(
        first.symmetry.shifts,
        [[0.0, 0.0], [0.5 * np.pi, np.pi],
         [np.pi, 0.0], [1.5 * np.pi, np.pi]], atol=1.0e-13)
    assert len(first.starts) == first.symmetry.group_order * len(
        first.raw_starts)
    for raw_index in range(len(first.raw_starts)):
        actions = first.group_action[
            raw_index * first.symmetry.group_order:
            (raw_index + 1) * first.symmetry.group_order]
        np.testing.assert_array_equal(actions, np.arange(4))

    # A -> s A, B -> s^2 B, x -> x/s preserves every extrema location in
    # (time, phi, u) and changes the exponent only by the constant 4 log(s).
    scale = 4.0
    scaled = planner.rank_joint_starts_from_uvq(
        scale * C_A, planner.summarize_uv_norm_table(scale * scale * C_B),
        1.0 / scale, 8.0 / scale, max_time_starts=2, max_starts=24)
    assert len(scaled.raw_starts) == len(first.raw_starts)
    assert len(scaled.starts) == len(first.starts)
    np.testing.assert_allclose(scaled.starts[:, :3], first.starts[:, :3],
                               atol=1.0e-13)
    np.testing.assert_allclose(scaled.starts[:, 3], first.starts[:, 3] / scale,
                               rtol=1.0e-13, atol=1.0e-13)
    np.testing.assert_allclose(
        scaled.scores - first.scores, 4.0 * np.log(scale), atol=1.0e-12)


def test_symmetry_orbits_are_reduced_before_representative_capacity():
    """A louder orbit's four copies must not evict a lower distinct orbit."""
    n_time = 9
    time = np.arange(n_time, dtype=float)
    C_A = np.zeros((5, 3, n_time), dtype=np.complex128)
    C_B = np.zeros((5, 5), dtype=np.complex128)
    C_A[0, 1] = 20.0 - 2.0 * np.cos(
        2.0 * np.pi * time / (n_time - 1))
    C_A[2, 0] = 0.25
    C_A[2, 2] = 0.25
    C_A[4, 1] = -0.25 + 0.05j
    C_B[0, 2] = 4.0
    C_B[2, 1] = 0.02
    C_B[2, 3] = 0.02
    portfolio = planner.rank_joint_starts_from_uvq(
        C_A, planner.summarize_uv_norm_table(C_B), 1.0, 8.0,
        max_time_starts=1, max_starts=8)
    assert portfolio.symmetry.group_order == 4
    assert len(portfolio.raw_starts) == 2
    assert len(portfolio.starts) == 8
    assert not portfolio.capacity_truncated
    assert portfolio.raw_scores[1] < portfolio.raw_scores[0]
    np.testing.assert_array_equal(
        portfolio.group_action, np.tile(np.arange(4), 2))


def test_ranked_starts_optimize_distance_at_each_angular_candidate():
    C_A, C_B = _synthetic_tables()
    # A strong norm harmonic makes the analytic distance optimum follow angle.
    C_B[2, 1] = 0.35
    C_B[2, 3] = 0.35
    portfolio = planner.rank_joint_starts_from_uvq(
        C_A, planner.summarize_uv_norm_table(C_B), 1.0, 8.0,
        max_time_starts=2, max_starts=24)
    phi, u, A = planner._harmonic_lattice(
        C_A, portfolio.n_phi_lattice, portfolio.n_u_lattice)
    _, _, B = planner._harmonic_lattice(
        C_B, portfolio.n_phi_lattice, portfolio.n_u_lattice)
    for start in portfolio.raw_starts:
        it = int(start[0])
        iphi = int(np.argmin(np.abs(phi - start[1])))
        iu = int(np.argmin(np.abs(u - start[2])))
        _, expected_x = planner._distance_profile(
            np.asarray(A[iphi, iu, it]), np.asarray(B[iphi, iu]), 1.0, 8.0)
        assert start[3] == pytest.approx(float(expected_x), abs=1.0e-13)


def test_time_endpoints_require_one_sided_maximum():
    C_A, C_B = _synthetic_tables()
    C_A[0, 1] = np.linspace(12.0, 20.0, C_A.shape[-1])
    portfolio = planner.rank_joint_starts_from_uvq(
        C_A, planner.summarize_uv_norm_table(C_B), 1.0, 8.0,
        max_time_starts=3, max_starts=24)
    assert 0 not in portfolio.time_starts
    assert portfolio.time_starts.tolist() == [C_A.shape[-1] - 1]


def test_jax_refiner_reaches_strict_stationary_maximum():
    C_A, C_B = _synthetic_tables()
    starts = np.asarray([[3.3, 0.2, 0.2, 5.0],
                         [4.7, 3.0, 0.1, 5.0]])
    result = tuple(np.asarray(item) for item in planner.refine_joint_starts_jax(
        C_A, C_B, starts, 1.0, 8.0, iterations=18))
    points, values, gradients, hessians, curvatures = result
    selected, stationary = planner.select_refined_modes(
        points, values, gradients, curvatures, max_modes=2)
    assert stationary.any()
    assert len(selected) >= 1
    assert np.max(np.linalg.norm(gradients[selected], axis=1)) < 2.0e-6
    assert np.all(curvatures[selected] > 0.0)
    assert np.all(np.isfinite(hessians[selected]))


def test_two_tier_local_integral_accepts_or_returns_finite_reserve():
    C_A, C_B = _synthetic_tables()
    calls = []

    def reserve():
        calls.append("called")
        return 123.456

    accepted = planner.multipeak_local_marginalize(
        C_A, C_B, 1.0, 8.0, reserve, log_integral_tol=0.1,
        tier0=(2, 2, 24), tier1=(3, 3, 48), quadrature_order=7,
        cell_sigma=4.0, chunk_size=32)
    assert accepted.accepted
    assert not accepted.used_reserve
    assert accepted.provenance == "uvq-multipeak-tier1"
    assert np.isfinite(accepted.value)
    assert accepted.delta_log_integral < 0.1
    assert calls == []

    declined = planner.multipeak_local_marginalize(
        C_A, C_B, 1.0, 8.0, reserve, log_integral_tol=1.0e-12,
        tier0=(2, 2, 24), tier1=(3, 3, 48), quadrature_order=5,
        cell_sigma=4.0, chunk_size=32)
    assert not declined.accepted
    assert declined.used_reserve
    assert declined.value == 123.456
    assert declined.provenance.startswith("dense-reserve:")
    assert calls == ["called"]

    bad_calls = []

    def failing_reserve():
        bad_calls.append("called")
        raise ValueError("deliberate reserve failure")

    with pytest.raises(planner._DenseReserveError):
        planner.multipeak_local_marginalize(
            C_A, C_B, 1.0, 8.0, failing_reserve,
            log_integral_tol=1.0e-12, tier0=(2, 2, 24),
            tier1=(3, 3, 48), quadrature_order=5,
            cell_sigma=4.0, chunk_size=32)
    assert bad_calls == ["called"]


def test_affine_cell_overlap_is_partitioned_not_rejected():
    C_A, C_B = _synthetic_tables()
    refined = tuple(np.asarray(item) for item in
                    planner.refine_joint_starts_jax(
                        C_A, C_B, np.asarray([[4.0, 0.0, 0.0, 5.0]]),
                        1.0, 8.0, iterations=18))
    points, values, _, hessians, _ = refined
    one = planner.integrate_refined_modes_tensor(
        C_A, C_B, points, values, hessians, 1.0, 8.0,
        log_integral_tol=0.1, cell_sigma=4.0, quadrature_order=7,
        chunk_size=32)
    duplicate = planner.integrate_refined_modes_tensor(
        C_A, C_B, np.repeat(points, 2, axis=0),
        np.repeat(values, 2), np.repeat(hessians, 2, axis=0), 1.0, 8.0,
        log_integral_tol=0.1, cell_sigma=4.0, quadrature_order=7,
        chunk_size=32)
    assert duplicate.min_core_separation == pytest.approx(0.0)
    assert duplicate.overlap_ok
    assert duplicate.ok == one.ok
    assert duplicate.value == pytest.approx(one.value, abs=2.0e-12)


def _log_density_dense(C_A, C_B, theta):
    enclosure = planner._time_fourier_enclosure(C_A)
    C_t = planner._evaluate_spectrum_numpy(
        enclosure[0], enclosure[1], theta[0])
    A, _ = planner._field_variation(C_t, theta[1], theta[2], 0.0, 0.0)
    B, _ = planner._field_variation(C_B, theta[1], theta[2], 0.0, 0.0)
    x = theta[3]
    return x * A - 0.5 * x * x * B - 4.0 * np.log(x)


def test_local_fourier_box_bound_dominates_dense_points():
    C_A, C_B = _synthetic_tables()
    enclosure = planner._time_fourier_enclosure(C_A)
    lo = np.asarray([3.25, 0.0, 0.0, 3.0])
    hi = np.asarray([4.75, 0.6, 0.7, 6.0])
    log_integral_upper, _, point_upper = planner._box_log_upper(
        C_A, C_B, enclosure, lo, hi)
    rng = np.random.default_rng(1729)
    points = rng.uniform(lo, hi, size=(20000, 4))
    dense = np.asarray([_log_density_dense(C_A, C_B, p) for p in points])
    assert np.max(dense) <= point_upper + 2.0e-11
    assert log_integral_upper == pytest.approx(
        point_upper + np.log(np.prod(hi - lo)), abs=1.0e-13)


def test_overlap_is_owned_once_in_exact_axis_box_geometry():
    C_A, C_B = _synthetic_tables()
    summary = planner.summarize_uv_norm_table(C_B)
    center = np.asarray([[4.0, np.pi, np.pi, 4.5],
                         [4.0, np.pi, np.pi, 4.5]])
    half = np.asarray([[4.0, np.pi, np.pi, 3.5],
                       [4.0, np.pi, np.pi, 3.5]])
    report = planner.hierarchical_union_cover(
        C_A, summary, center, half, 1.0, 8.0,
        target_log_value=0.0, max_boxes=10)
    assert report.bound_certified
    assert report.budget_met
    assert report.n_owned_leaves == 1
    assert report.n_overlap_owned == 1
    assert report.n_outside_leaves == 0
    assert report.owned_mode.tolist() == [0]
    np.testing.assert_allclose(report.owned_centers, center[:1])
    np.testing.assert_allclose(report.owned_half_widths, half[:1])


def test_cover_cap_is_a_decline_not_a_failure_or_false_certificate():
    C_A, C_B = _synthetic_tables()
    summary = planner.summarize_uv_norm_table(C_B)
    report = planner.hierarchical_union_cover(
        C_A, summary, np.asarray([[4.0, 0.0, 0.0, 5.0]]),
        np.asarray([[0.1, 0.1, 0.1, 0.1]]), 1.0, 8.0,
        target_log_value=1.0e6, outside_tol_nats=-23.0, max_boxes=1)
    assert report.bound_certified
    assert report.budget_met  # A huge supplied target makes the comparison pass.
    assert not report.cap_reached  # No refinement was needed.

    declined = planner.hierarchical_union_cover(
        C_A, summary, np.asarray([[4.0, 0.0, 0.0, 5.0]]),
        np.asarray([[0.1, 0.1, 0.1, 0.1]]), 1.0, 8.0,
        target_log_value=-1.0e6, outside_tol_nats=-23.0, max_boxes=1)
    assert declined.bound_certified
    assert not declined.budget_met
    assert declined.cap_reached
    assert declined.n_outside_leaves == 1


_HM_PACKET = "/tmp/hm51_Ctables_incl0.6.npz"
_SNR40_PACKET = ("/tmp/rift-paper-av-ladder/analyses/va_sequence_20260902/"
                 "records/angle_coeffs_rung40_n256.npz")
_SNR160_PACKET = ("/tmp/rift-paper-av-ladder/analyses/va_sequence_20260902/"
                  "records/angle_coeffs_rung160_n256.npz")


@pytest.mark.skipif(not os.path.exists(_HM_PACKET),
                    reason="external real-table validation packet is absent")
def test_hm_second_mode_survives_unsafe_proxy_gap():
    """Regression for the real Lmax=4 mode proxy that defeated PR267's line."""
    packet = np.load(_HM_PACKET)
    C_A = packet["C_A"]
    summary = planner.summarize_uv_norm_table(packet["C_B"])
    portfolio = planner.rank_joint_starts_from_uvq(
        C_A, summary, 1000.0 / 720.0, 1000.0 / 240.0,
        max_time_starts=3, max_starts=24)
    # This is load-bearing: proxy pruning at the nominal -23 nat error budget,
    # or even at -32, discards a mode whose refined contribution is relevant.
    assert len(portfolio.raw_scores) >= 15
    assert portfolio.raw_scores[14] - portfolio.raw_scores[0] == pytest.approx(
        -37.13988596, abs=2.0e-6)

    result = tuple(np.asarray(item) for item in planner.refine_joint_starts_jax(
        C_A, summary.C_B, portfolio.starts, 1000.0 / 720.0,
        1000.0 / 240.0, iterations=18))
    points, values, gradients, _, curvatures = result
    selected, _ = planner.select_refined_modes(
        points, values, gradients, curvatures, max_modes=24)
    assert len(selected) == 2
    delta = np.sort(values[selected] - np.max(values[selected]))
    np.testing.assert_allclose(delta, [-11.671141934982415, 0.0],
                               rtol=0.0, atol=2.0e-8)
    assert np.max(np.linalg.norm(gradients[selected], axis=1)) < 2.0e-6


@pytest.mark.skipif(not os.path.exists(_HM_PACKET),
                    reason="external real-table validation packet is absent")
def test_hm_two_tier_integral_matches_overcomplete_oracle():
    packet = np.load(_HM_PACKET)
    oracle = 1305.8219235157544
    result = planner.multipeak_local_marginalize(
        packet["C_A"], packet["C_B"], 1000.0 / 720.0, 1000.0 / 240.0,
        oracle, log_integral_tol=1.0e-3, quadrature_order=7,
        cell_sigma=5.0, chunk_size=64)
    assert result.accepted
    assert not result.used_reserve
    assert result.tier0.n_retained_modes == 2
    assert result.tier1.n_retained_modes == 2
    assert abs(result.value - oracle) < 1.0e-3
    assert result.modeled_peak_bytes < 32 * 1024 ** 2


@pytest.mark.skipif(not os.path.exists(_SNR40_PACKET),
                    reason="external real-table validation packet is absent")
def test_real_low_snr_declines_to_finite_reserve():
    packet = np.load(_SNR40_PACKET)
    C_A = packet["C_A"][:, :, 148, :]
    C_B = packet["C_B"][:, :, 148, :]
    oracle = 814.7510954543737
    result = planner.multipeak_local_marginalize(
        C_A, C_B, 0.2, 7.0, oracle, log_integral_tol=1.0e-3,
        quadrature_order=7, cell_sigma=5.0, chunk_size=64)
    assert not result.accepted
    assert result.used_reserve
    assert result.value == oracle
    assert result.provenance.startswith("dense-reserve:")


@pytest.mark.skipif(not os.path.exists(_SNR160_PACKET),
                    reason="external real-table validation packet is absent")
def test_real_high_snr_two_tier_path_matches_overcomplete_oracle():
    packet = np.load(_SNR160_PACKET)
    C_A = packet["C_A"][:, :, 148, :]
    C_B = packet["C_B"][:, :, 148, :]
    oracle = 13255.018541583624
    result = planner.multipeak_local_marginalize(
        C_A, C_B, 0.2, 7.0, oracle, log_integral_tol=1.0e-3,
        quadrature_order=7, cell_sigma=5.0, chunk_size=64)
    assert result.accepted
    assert not result.used_reserve
    assert result.tier0.n_retained_modes == 4
    assert result.tier1.n_retained_modes == 4
    assert result.delta_log_integral < 1.0e-3
    assert abs(result.value - oracle) < 1.0e-3
