"""Structural and downstream parity tests for the direct GPU-to-JAX adapter."""
import os

os.environ.setdefault("JAX_ENABLE_X64", "1")

import jax
import jax.numpy as jnp
import numpy as np
import pytest

from RIFT.likelihood import factored_likelihood_rotating_freqresponse as fr
from RIFT.likelihood import slowrot_freqresponse as sfr
from RIFT.likelihood.gpu_jax_handoff import (
    build_jax_rotating_freqresponse_data_from_device,
)
from RIFT.likelihood.gpu_precompute import pack_device_precompute
from RIFT.likelihood.jax_ile.banded import build_rotating_freqresponse_data
from RIFT.likelihood.jax_ile.core import fused_log_likelihood

jax.config.update("jax_enable_x64", True)


def _fixture(seed=71):
    rng = np.random.default_rng(seed)
    modes = [(2, 2), (2, -2)]
    a_list = fr.compound_index_set(0, 0)
    A, K, N = len(a_list), len(modes), 128
    q = rng.normal(size=(A, K, N)) + 1j*rng.normal(size=(A, K, N))
    u = rng.normal(size=(A, A, K, K)) + 1j*rng.normal(size=(A, A, K, K))
    v = rng.normal(size=(A, A, K, K)) + 1j*rng.normal(size=(A, A, K, K))
    # The two builders need only identical arrays for this routing test; the
    # likelihood need not represent a physical positive norm.
    packed = dict(q={"H1": jnp.asarray(q)}, U={"H1": jnp.asarray(u)},
                  V={"H1": jnp.asarray(v)}, epoch={"H1": 1000.-64/1024.},
                  delta_t=1/1024., modes=modes, a_list=a_list)
    meta = dict(feature="rotation_freqresponse", gpu_precompute=True,
                device_resident=True, post_phase_required=True,
                event_time_geo=1000., modes=modes, a_list=a_list,
                Qmax=0, p_max=0, f_sidereal=1.160576e-5)
    geom = {"H1": sfr.detector_geometry("H1", L_arm=4000.)}
    tvals = (np.arange(9)-4)/1024.
    return packed, meta, geom, tvals, q, u, v


def test_device_builder_matches_existing_banded_builder_downstream():
    packed, meta, geom, tvals, q, u, v = _fixture()
    direct = build_jax_rotating_freqresponse_data_from_device(
        packed, meta, tvals, geom, require_gpu=False)

    a_list = meta["a_list"]
    lookup = {"H1": np.asarray(meta["modes"], dtype=int)}
    rho = {"H1": {a: q[i] for i, a in enumerate(a_list)}}
    U = {"H1": {(a, ap): u[i, j] for i, a in enumerate(a_list)
                for j, ap in enumerate(a_list)}}
    V = {"H1": {(a, ap): v[i, j] for i, a in enumerate(a_list)
                for j, ap in enumerate(a_list)}}
    conventional = build_rotating_freqresponse_data(
        meta, lookup, rho, U, V, packed["epoch"], packed["delta_t"],
        tvals, geom)

    args = [jnp.asarray(x) for x in (
        [1.2, 1.21], [0.3, 0.31], [0.2, 0.4],
        [0.7, 0.8], [0.1, 0.5], [100., 120.])]
    got = fused_log_likelihood(direct, *args, interp="nearest")
    expected = fused_log_likelihood(conventional, *args, interp="nearest")
    np.testing.assert_allclose(np.asarray(got), np.asarray(expected),
                               rtol=2e-13, atol=2e-13)
    assert direct.detectors["H1"]["Q_bank"].shape == (len(a_list), 128, 2)
    q_devices = direct.detectors["H1"]["Q_bank"].devices()
    for key in ("location", "response", "x_arm", "y_arm"):
        assert direct.detectors["H1"][key].devices() == q_devices
    assert direct.gpu_handoff["contract_Q_U_V_host_copies"] == 0
    zero_packed = dict(packed)
    zero_packed["q"] = {"H1": jnp.zeros_like(packed["q"]["H1"])}
    zero_data = build_jax_rotating_freqresponse_data_from_device(
        zero_packed, meta, tvals, geom, require_gpu=False)
    zero_lnL = fused_log_likelihood(zero_data, *args, interp="nearest")
    assert np.max(np.abs(np.asarray(got-zero_lnL))) > 1e-6, \
        "fixture did not exercise the sampled Q data term"


def test_handoff_fails_closed_for_host_arrays_cpu_jax_and_pregrid():
    packed, meta, geom, tvals, q, u, v = _fixture()
    if all(d.platform != "gpu" for d in jax.devices()):
        with pytest.raises(RuntimeError, match="non-GPU"):
            build_jax_rotating_freqresponse_data_from_device(packed, meta, tvals, geom)
    host = dict(packed)
    host["q"] = {"H1": q}
    host["U"] = {"H1": u}
    host["V"] = {"H1": v}
    with pytest.raises(TypeError, match="device-resident"):
        build_jax_rotating_freqresponse_data_from_device(
            host, meta, tvals, geom, require_gpu=False)
    for bad in (1.5, 2):
        with pytest.raises((ValueError, NotImplementedError)):
            build_jax_rotating_freqresponse_data_from_device(
                packed, meta, tvals, geom, q_time_pregrid_factor=bad,
                require_gpu=False)


def test_classic_ile_device_pack_is_view_only_and_dense():
    packed, meta, _geom, _tvals, _q, _u, _v = _fixture()
    lookup, rho, U, V, epoch = pack_device_precompute(
        packed, meta, require_gpu=False)
    assert lookup["H1"].tolist() == [[2, 2], [2, -2]]
    for i, a in enumerate(meta["a_list"]):
        np.testing.assert_array_equal(np.asarray(rho["H1"][a]),
                                      np.asarray(packed["q"]["H1"][i]))
    assert U["H1"] is packed["U"]["H1"]
    assert V["H1"] is packed["V"]["H1"]
    assert epoch == packed["epoch"] and epoch is not packed["epoch"]

    bad = dict(packed)
    bad["U"] = {"H1": packed["U"]["H1"][1:]}
    with pytest.raises(ValueError, match="U/V"):
        pack_device_precompute(bad, meta, require_gpu=False)
    bad_dt = dict(packed, delta_t=0.0)
    with pytest.raises(ValueError, match="delta_t"):
        pack_device_precompute(bad_dt, meta, require_gpu=False)
    if all(d.platform != "gpu" for d in jax.devices()):
        with pytest.raises(RuntimeError, match="not on a GPU"):
            pack_device_precompute(packed, meta)
