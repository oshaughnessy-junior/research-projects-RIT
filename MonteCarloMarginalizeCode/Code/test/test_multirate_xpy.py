"""Multirate per-point preparation on an array backend (TwoRateDevice, ComputeModeCrossTermsPiecesDevice)
against the CPU reference path (Q_two_rate, ComputeModeCrossTermsPieces), and cupy against numpy.
The cupy test needs a working GPU; it is skipped without one, so run this file on a GPU host."""
import numpy as np
import pytest
import lal

import RIFT.lalsimutils as lsu
from RIFT.likelihood import factored_likelihood_multirate as flm
from test_multirate_precompute import _P, _schedule, inputs, T_GEO, LMAX, FMAX   # noqa: F401 (fixture)


def _run(tmp_path, inputs, **kw):
    flm._JOB_CACHE.clear()
    return flm.PrecomputeLikelihoodTermsMultirate(lal.LIGOTimeGPS(T_GEO), 0.05, _P(), inputs["data"], inputs["psd"],
                                                  LMAX, FMAX, verbose=False, quiet=True,
                                                  schedule=_schedule(tmp_path, "two_rate"), skip_interpolation=True, **kw)


def _assert_close(a, b, tol):
    _, Ua, Va, Ra, _, _ = a
    _, Ub, Vb, Rb, _, _ = b
    sU = max(abs(v) for v in Ua["H1"].values())
    for key in Ua["H1"]:
        assert abs(Ua["H1"][key] - Ub["H1"][key]) <= tol * sU, ("U", key)
        assert abs(Va["H1"][key] - Vb["H1"][key]) <= tol * sU, ("V", key)
    sQ = max(np.max(np.abs(v.data.data)) for v in Ra["H1"].values())
    for k in Ra["H1"]:
        assert abs(float(Ra["H1"][k].epoch) - float(Rb["H1"][k].epoch)) < 1e-9
        assert np.max(np.abs(Ra["H1"][k].data.data - Rb["H1"][k].data.data)) <= tol * sQ, ("Q", k)


def test_ifft_centred_matches_lal():
    rng = np.random.default_rng(1)
    n, df = 4096, 0.25
    hf = lal.CreateCOMPLEX16FrequencySeries("h", lal.LIGOTimeGPS(0), -n * df / 2, df, lsu.lsu_HertzUnit, n)
    hf.data.data = rng.standard_normal(n) + 1j * rng.standard_normal(n)
    ref = lsu.DataInverseFourier(hf).data.data
    new = flm.ifft_centred(np, hf.data.data, df)
    assert np.max(np.abs(new - ref)) <= 1e-12 * np.max(np.abs(ref))


def test_backend_numpy_matches_reference(tmp_path, inputs):
    ref = _run(tmp_path, inputs, multirate_reference=True)
    new = _run(tmp_path, inputs, multirate_xpy="numpy")
    _assert_close(ref, new, 1e-10)


def test_backend_cupy_matches_numpy(tmp_path, inputs):
    if flm._select_xpy() is np:
        pytest.skip("no usable GPU")
    a = _run(tmp_path, inputs, multirate_xpy="numpy")
    b = _run(tmp_path, inputs, multirate_xpy="cupy")
    _assert_close(a, b, 1e-10)
