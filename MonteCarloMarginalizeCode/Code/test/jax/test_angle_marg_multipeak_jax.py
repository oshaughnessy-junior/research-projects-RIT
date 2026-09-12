"""Static-cost, JIT/AD-compatible multipeak wiring and contract."""

import numpy as np
import pytest

jax = pytest.importorskip("jax")
import jax.numpy as jnp

from RIFT.likelihood.jax_ile import anglemarg as AM
from RIFT.likelihood.jax_ile import direct_marginalization_policy as DP
from RIFT.likelihood.jax_ile.wrapper import JAXDistPhiPsiMargLikelihood
from test_angle_marg_exact import make_synth, RA, DEC, INCL, INTERP


def test_multipeak_jax_is_explicit_only_and_records_bounded_contract():
    assert "multipeak-jax" in AM.ANGLE_MARG_CHOICES
    for amp in (1.0, 500.0, 5.0e6):
        assert AM.choose_angle_marg_scheme(amp)[0] != "multipeak-jax"

    data = make_synth(scale=2.0)
    like = JAXDistPhiPsiMargLikelihood(
        data, 30.0, 3000.0, nphi=32, npsi=8, interp=INTERP,
        angle_marg="multipeak-jax")
    assert like.angle_marg_info["bounded_cost"] is True
    assert like.angle_marg_info["dense_reserve"] is False
    assert like.angle_marg_info["fixed_plan_autodiff_only"] is True
    assert like.angle_marg_info["derivative_warrant_certified"] is False


def test_multipeak_jax_wrapper_uses_jit_and_ad(monkeypatch):
    def differentiable_stub(data, ra, dec, incl, *args, **kwargs):
        return ra + 2.0 * dec + 3.0 * incl

    monkeypatch.setattr(
        DP, "fused_log_likelihood_four_axis_bounded", differentiable_stub)
    data = make_synth(scale=2.0)
    like = JAXDistPhiPsiMargLikelihood(
        data, 30.0, 3000.0, nphi=32, npsi=8, interp=INTERP,
        angle_marg="multipeak-jax")
    value = np.asarray(like._batched(
        jnp.asarray(RA), jnp.asarray(DEC), jnp.asarray(INCL)))
    assert value.shape == np.shape(RA)
    val, grad = like._value_and_grad(
        jnp.asarray([RA[0], DEC[0], INCL[0]]))
    assert np.isfinite(np.asarray(val))
    np.testing.assert_allclose(np.asarray(grad), [1.0, 2.0, 3.0])


def test_multipeak_jax_refuses_time_series_and_amp_grid_outputs(monkeypatch):
    monkeypatch.setattr(
        DP, "fused_log_likelihood_four_axis_bounded",
        lambda data, ra, dec, incl, *args, **kwargs: ra * 0.0)
    data = make_synth(scale=2.0)
    like = JAXDistPhiPsiMargLikelihood(
        data, 30.0, 3000.0, nphi=32, npsi=8, interp=INTERP,
        angle_marg="multipeak-jax")
    args = (data, jnp.asarray(RA), jnp.asarray(DEC), jnp.asarray(INCL))
    with pytest.raises(ValueError, match="no lnL"):
        like._fused(*args, return_lnLt=True)
    with pytest.raises(ValueError, match="static cost envelope"):
        like._fused(*args, return_amp=True)


def test_bounded_config_refuses_nonstatic_or_invalid_envelopes():
    with pytest.raises(ValueError, match="time_guard"):
        DP.validate_bounded_multipeak_config(
            DP.BoundedMultipeakConfig(time_guard=1))
    with pytest.raises(ValueError, match="enriched_oversample"):
        DP.validate_bounded_multipeak_config(
            DP.BoundedMultipeakConfig(base_oversample=2,
                                      enriched_oversample=2))
    with pytest.raises(ValueError, match="mode caps"):
        DP.validate_bounded_multipeak_config(
            DP.BoundedMultipeakConfig(base_max_starts=1,
                                      enriched_max_modes=3))


def test_device_function_is_jittable_differentiable_and_forwards_caps(
        monkeypatch):
    seen = []

    def fake_tables(data, ra, dec, incl, interp, guard):
        del data, interp
        ntime = 2 * int(guard) + 3
        signal = ra + 2.0 * dec + 3.0 * incl
        ca = jnp.broadcast_to(signal[None, None, :, None],
                              (1, 1, signal.size, ntime)).astype(jnp.complex128)
        cb = jnp.ones_like(ca)
        return ca, cb, {"m_max": 0}

    def fake_rank(table, norm, x_min, x_max, **kwargs):
        del table, norm, x_min, x_max
        seen.append((kwargs["max_starts"], kwargs["max_time_nodes"],
                     kwargs["angular_oversample"]))
        return jnp.asarray(0.0)

    def fake_plan(table, norm, base, extra, x_min, x_max, **kwargs):
        del norm, base, extra, x_min, x_max
        token = {"token": jnp.real(jnp.sum(table))}
        one = jnp.asarray(1, dtype=jnp.int32)
        ok = jnp.asarray(True)
        plan = dict(n_selected_modes=one, n_optimizer_starts=one,
                    n_lattice_evaluations=one, n_candidates_before_cap=one,
                    start_capacity_ok=ok, time_capacity_ok=ok)
        shared = {"n_optimizer_starts_executed": one}
        assert kwargs["max_modes"] == 1
        assert kwargs["enriched_max_modes"] == 2
        return token, token, plan, plan, shared

    def fake_integral(table, norm, base_plan, enriched_plan,
                      x_min, x_max, **kwargs):
        del norm, base_plan, enriched_plan, x_min, x_max, kwargs
        value = jnp.real(jnp.sum(table))
        return value, jnp.asarray(True), {"accepted_local": jnp.asarray(True)}

    monkeypatch.setattr(DP._anglemarg, "angle_coefficient_tables", fake_tables)
    monkeypatch.setattr(DP._anglemarg, "_runtime_amp_failsafe",
                        lambda *args, **kwargs: jnp.asarray(0.0))
    monkeypatch.setattr(DP._aap, "rank_joint_starts_from_uvq_device", fake_rank)
    monkeypatch.setattr(DP._aap, "make_all_axis_mode_plan_pair_device", fake_plan)
    monkeypatch.setattr(DP._aap, "empirical_enrichment_marginalize", fake_integral)

    cfg = DP.BoundedMultipeakConfig(
        time_guard=2, base_max_starts=2, max_time_nodes=3,
        base_oversample=1, enriched_oversample=2,
        max_modes=1, enriched_max_modes=2, refine_iterations=1,
        base_order=2, base_check_order=3,
        enriched_order=3, enriched_check_order=4)
    xg = jnp.asarray([0.5, 1.0])
    lwg = jnp.asarray([-np.log(2.0), -np.log(2.0)])

    def scalar(theta):
        value, ledger = DP.fused_log_likelihood_four_axis_bounded(
            object(), theta[0:1], theta[1:2], theta[2:3], xg, lwg,
            interp=None, amp_sizing=10.0, config=cfg,
            local_log_normalization=0.0, x_bounds=(0.5, 1.0),
            return_ledger=True)
        return value[0], ledger

    (value, ledger), grad = jax.jit(jax.value_and_grad(
        scalar, has_aux=True))(
            jnp.asarray([0.1, 0.2, 0.3]))
    assert np.isfinite(np.asarray(value))
    np.testing.assert_allclose(np.asarray(grad), 7.0 * np.asarray([1., 2., 3.]))
    assert bool(np.asarray(ledger["bounded_cost"][0]))
    assert not bool(np.asarray(ledger["derivative_warrant_certified"][0]))
    assert (2, 3, 1) in seen and (2, 3, 2) in seen
