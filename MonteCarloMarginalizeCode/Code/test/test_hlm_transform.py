"""Extra-parameter waveform-transform seam (RIFT/likelihood/hlm_transform.py, ILE
--supplementary-waveform-transform-*).

Precompute level (synthetic IMRPhenomD injection, analytic PSD, no files):
  * F == 1 reproduces U, V and Q bitwise against no transform;
  * real constant F = A scales U by A^2, V by A^2, Q by A;
  * a pure delay F = exp(-2 pi i f Delta) gives Q'(t) = Q(t + Delta) (template later), and
    the evaluate_fvals-sign version does not -- the packed-FFT phase-sign trap.
Time domain: the delayed bank, inverse-FFT'd, equals a numpy-FFT time-domain reference and
its peak moves LATER by Delta.
Loader, Hermitian construction and ILE refusals; one ILE end-to-end identity run on a
zero-data frame with the four REDLINE flags, which also checks the lambda echo.
"""
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import numpy as np
import pytest

import lal
import lalsimulation as lalsim
import RIFT.lalsimutils as lalsimutils
import RIFT.likelihood.factored_likelihood as FL
from RIFT.likelihood import hlm_transform as HT

CODE = Path(__file__).resolve().parents[1]
ILE = CODE / "bin" / "integrate_likelihood_extrinsic_batchmode"
DEMO = CODE / "demo" / "waveform_transform"
REDLINE = ["--time-marginalization", "--vectorized", "--gpu", "--force-xpy"]


def _write_plugin(tmp_path, name, body):
    p = tmp_path / (name + ".py")
    p.write_text(textwrap.dedent(body))
    return str(p)


def _fn_transform(fn, names=("lam",)):
    return HT.WaveformTransform(fn, names)


# ---------------------------------------------------------------------------- loader

def test_loader_file_path_ini_prepare_and_parameter_union(tmp_path):
    ini = tmp_path / "p.ini"
    ini.write_text("[lens]\nScale = 2.5\n")
    code = _write_plugin(tmp_path, "plug_a", """
        import numpy
        NAME = "plug_a"
        EXTRA_PARAMETERS = ["lens_a"]
        CALLS = []
        def prepare(config=None, extra_parameters=None, opts=None, **kw):
            CALLS.append((config.get("lens", "Scale"), list(extra_parameters)))
        def my_tf(f, extra_params, P=None, xpy=numpy):
            return numpy.ones_like(f, dtype=complex)
        """)
    t = HT.load_waveform_transform(code, function="my_tf", ini=str(ini),
                                   parameters=["lens_b", "lens_a"])
    assert t.extra_parameters == ("lens_a", "lens_b")
    assert t.name == "plug_a"
    assert t.module.CALLS == [("2.5", ["lens_a", "lens_b"])]   # once, key case preserved


def test_loader_dotted_name_and_errors(tmp_path, monkeypatch):
    _write_plugin(tmp_path, "plug_dotted", """
        import numpy
        def transfer_function(f, extra_params, P=None):
            return numpy.ones_like(f, dtype=complex)
        """)
    monkeypatch.syspath_prepend(str(tmp_path))
    t = HT.load_waveform_transform("plug_dotted", parameters=["x"])
    assert t.extra_parameters == ("x",)
    # a plugin without an xpy keyword is still callable
    assert np.all(HT.two_sided_response(t, 8, 1.0, {"x": 0.}) == 1)
    with pytest.raises(FileNotFoundError, match="--supplementary-waveform-transform-code"):
        HT.load_waveform_transform(str(tmp_path / "absent.py"))
    with pytest.raises(AttributeError, match="no function"):
        HT.load_waveform_transform("plug_dotted", function="nope")
    with pytest.raises(FileNotFoundError, match="--supplementary-waveform-transform-ini"):
        HT.load_waveform_transform("plug_dotted", ini=str(tmp_path / "absent.ini"))


def test_coordinate_plugin_loader_unchanged(tmp_path):
    from RIFT.misc import coordinate_plugin
    with pytest.raises(FileNotFoundError, match="--supplementary-coordinate-code: file not found"):
        coordinate_plugin._load_module_from_spec(str(tmp_path / "absent.py"))
    with pytest.raises(FileNotFoundError, match="--supplementary-coordinate-ini: file not found"):
        coordinate_plugin._parse_ini(str(tmp_path / "absent.ini"))
    assert coordinate_plugin._parse_ini(None) is None


# ------------------------------------------------------------- Hermitian construction

@pytest.mark.parametrize("n", [16, 17])
def test_two_sided_response_is_hermitian_on_the_lal_grid(n):
    df = 0.25
    t = _fn_transform(lambda f, lam, P=None, xpy=np: (1 + f) * np.exp(1j * (0.3 + f)))
    F = HT.two_sided_response(t, n, df, {"lam": 0.})
    f = df * (np.arange(n) - n // 2)
    m = n // 2
    assert F[m].imag == 0 and F[m].real == np.cos(0.3)              # DC = Re F(0)
    for k in range(n):
        if n % 2 == 0 and k == 0:
            assert F[0] == 1.0                                        # Nyquist identity
            continue
        kk = 2 * m - k                                                # bin of -f
        assert np.isclose(f[kk], -f[k])
        assert F[kk] == np.conj(F[k])
    pos = f > 0
    np.testing.assert_allclose(F[pos], (1 + f[pos]) * np.exp(1j * (0.3 + f[pos])), rtol=0, atol=0)


def test_response_validation_and_origin_assert():
    bad_shape = _fn_transform(lambda f, lam, P=None, xpy=np: np.ones(3, dtype=complex))
    with pytest.raises(ValueError, match="shape"):
        HT.two_sided_response(bad_shape, 16, 1.0, {})
    bad_val = _fn_transform(lambda f, lam, P=None, xpy=np: np.full(f.shape, np.nan + 0j))
    with pytest.raises(ValueError, match="non-finite"):
        HT.two_sided_response(bad_val, 16, 1.0, {})
    s = lal.CreateCOMPLEX16FrequencySeries("x", lal.LIGOTimeGPS(0), 0.0, 1.0, lal.HertzUnit, 16)
    with pytest.raises(ValueError, match="FFT-shift origin"):
        HT.packed_frequencies(s)


def test_demo_plugins_load_and_identity_shortcut():
    t = HT.load_waveform_transform(str(DEMO / "constant_amplitude_phase.py"))
    assert t.extra_parameters == ("lens_amplitude", "lens_phase")
    assert t.bind({"lens_amplitude": 1.0, "lens_phase": 0.0}) is None
    assert t.bind({"lens_amplitude": 2.0, "lens_phase": 0.0}) is not None
    pl = HT.load_waveform_transform(str(DEMO / "point_lens_geometric_optics.py"))
    F = HT.two_sided_response(pl, 4096, 0.25, {"lens_mass_z": 1e4, "lens_y": 0.7})
    assert np.all(np.isfinite(F))
    # |F|^2 averaged over frequency -> mu_+ + |mu_-| (incoherent sum of the two images)
    import importlib.util
    spec = importlib.util.spec_from_file_location("_pl", str(DEMO / "point_lens_geometric_optics.py"))
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    mu_p, mu_m, dt = mod.image_parameters(1e4, 0.7)
    assert dt > 0
    np.testing.assert_allclose(np.mean(np.abs(F[2049:]) ** 2), mu_p + abs(mu_m), rtol=2e-2)


# --------------------------------------------------- time domain: the phase-sign trap

def _pulse_bank(n=4096, dt=1.0 / 1024, t0=1.0):
    t = np.arange(n) * dt
    h = np.exp(-0.5 * ((t - t0) / 0.01) ** 2) * np.exp(2j * np.pi * 80 * t)
    ts = lal.CreateCOMPLEX16TimeSeries("h", lal.LIGOTimeGPS(0), 0.0, dt, lal.DimensionlessUnit, n)
    ts.data.data = h
    return h, lalsimutils.DataFourier(ts)


def test_pure_delay_moves_the_time_series_later_against_numpy_reference():
    n, dt, t0 = 4096, 1.0 / 1024, 1.0
    shift = 37                                                       # samples
    Delta = shift * dt
    h, hf = _pulse_bank(n, dt, t0)
    hf_conj = lalsimutils.DataFourier(_conj_ts(h, dt))
    delay = _fn_transform(lambda f, lam, P=None, xpy=np: np.exp(-2j * np.pi * f * lam["Delta"]), ("Delta",))
    banks, banks_conj = HT.apply_hlm_transform({(2, 2): hf}, {(2, 2): hf_conj}, transform=delay,
                                               extra_params={"Delta": Delta})
    # LAL's forward/reverse FFTs carry deltaT and deltaF, so the round trip is unnormalized
    out = lalsimutils.DataInverseFourier(banks[(2, 2)]).data.data
    out_conj = lalsimutils.DataInverseFourier(banks_conj[(2, 2)]).data.data
    # numpy time-domain reference: the same kernel (pure delay) built with numpy's FFT
    fnp = np.fft.fftfreq(n, dt)
    ref = np.fft.ifft(np.fft.fft(h) * np.exp(-2j * np.pi * fnp * Delta))
    scale = np.max(np.abs(h))
    # The seam forces the Nyquist bin to identity; the pulse has no power there.
    np.testing.assert_allclose(out, ref, atol=1e-10 * scale)
    np.testing.assert_allclose(out, np.roll(h, shift), atol=1e-10 * scale)
    assert np.argmax(np.abs(out)) == np.argmax(np.abs(h)) + shift     # LATER by Delta
    # conjugate bank: the same real kernel applied to conj(h)
    np.testing.assert_allclose(out_conj, np.roll(np.conj(h), shift), atol=1e-10 * scale)
    # negative control: labelling bins with evaluate_fvals' default sign moves it EARLIER
    fr = lalsimutils.evaluate_fvals(_pulse_bank(n, dt, t0)[1])
    wrong = np.fft.ifft(np.fft.ifftshift(_pulse_bank(n, dt, t0)[1].data.data * np.exp(-2j * np.pi * fr * Delta)))
    assert np.argmax(np.abs(wrong)) == np.argmax(np.abs(h)) - shift


def _conj_ts(h, dt):
    ts = lal.CreateCOMPLEX16TimeSeries("hc", lal.LIGOTimeGPS(0), 0.0, dt, lal.DimensionlessUnit, len(h))
    ts.data.data = np.conj(h)
    return ts


# ----------------------------------------------------------------- precompute level

FIDUCIAL = 1126259462.0
DETS = ["H1", "L1"]
LMAX, FMAX, TWIN = 2, 1000.0, 0.15


@pytest.fixture(scope="module")
def injection():
    P = lalsimutils.ChooseWaveformParams()
    P.m1, P.m2 = 35.0 * lal.MSUN_SI, 30.0 * lal.MSUN_SI
    P.s1z, P.s2z = 0.1, -0.2
    P.fmin = P.fref = 30.0
    P.deltaT, P.deltaF = 1.0 / 4096, 1.0 / 4
    P.dist = 800e6 * lal.PC_SI
    P.approx = lalsim.IMRPhenomD
    P.radec = True
    P.tref = FIDUCIAL
    P.phi, P.theta, P.psi, P.incl, P.phiref = 1.2, -0.4, 0.7, 0.9, 2.1
    data = {}
    for det in DETS:
        Pd = P.copy(); Pd.detector = det
        data[det] = lalsimutils.non_herm_hoff(Pd)
    psd = {det: lalsim.SimNoisePSDaLIGOZeroDetHighPower for det in DETS}
    return P, data, psd


def _precompute(injection, hlm_transform=None):
    P, data, psd = injection
    out = FL.PrecomputeLikelihoodTerms(FIDUCIAL, TWIN, P.copy(), data, psd, LMAX, FMAX,
                                       analyticPSD_Q=True, verbose=False, quiet=True,
                                       ignore_threshold=None, skip_interpolation=True,
                                       hlm_transform=hlm_transform)
    _, U, V, Q = out[:4]
    Q = {det: {mode: np.array(Q[det][mode].data.data) for mode in Q[det]} for det in Q}
    return U, V, Q


def _bound(fn, lam):
    return HT.WaveformTransform(fn, tuple(lam)).bind(lam)


def test_identity_transform_is_bitwise(injection):
    U0, V0, Q0 = _precompute(injection)
    U1, V1, Q1 = _precompute(injection, _bound(lambda f, lam, P=None, xpy=np: np.ones(f.shape, complex), {"a": 1.}))
    for det in DETS:
        assert U0[det].keys() == U1[det].keys()
        for k in U0[det]:
            assert U0[det][k] == U1[det][k] and V0[det][k] == V1[det][k], (det, k)
        for mode in Q0[det]:
            assert np.array_equal(Q0[det][mode], Q1[det][mode]), (det, mode)
    # and the precompute does carry a signal, so the comparison is not of zeros
    assert max(abs(U0["H1"][k]) for k in U0["H1"]) > 0
    assert max(np.max(np.abs(Q0["H1"][m])) for m in Q0["H1"]) > 0


def test_constant_real_amplitude_scales_U_V_by_A2_and_Q_by_A(injection):
    A = 0.37
    U0, V0, Q0 = _precompute(injection)
    UA, VA, QA = _precompute(injection, _bound(lambda f, lam, P=None, xpy=np: lam["A"] * np.ones(f.shape, complex), {"A": A}))
    for det in DETS:
        for k in U0[det]:
            np.testing.assert_allclose(UA[det][k], A * A * U0[det][k], rtol=1e-12, atol=1e-12 * abs(U0[det][(( 2, 2), (2, 2))]))
            np.testing.assert_allclose(VA[det][k], A * A * V0[det][k], rtol=1e-12, atol=1e-12 * abs(U0[det][((2, 2), (2, 2))]))
        for mode in Q0[det]:
            sc = np.max(np.abs(Q0[det][mode]))
            np.testing.assert_allclose(QA[det][mode], A * Q0[det][mode], rtol=1e-12, atol=1e-12 * sc)


def test_pure_delay_shifts_Q_in_the_template_later_direction(injection):
    """F = exp(-2 pi i f Delta) delays the template by Delta, so its best match against
    unchanged data is Delta EARLIER: Q'(t) = Q(t + Delta).  U and V are unchanged."""
    P = injection[0]
    shift = 25
    Delta = shift * P.deltaT
    U0, V0, Q0 = _precompute(injection)
    delay = lambda f, lam, P=None, xpy=np: np.exp(-2j * np.pi * f * lam["D"])
    Ud, Vd, Qd = _precompute(injection, _bound(delay, {"D": Delta}))
    wrong = lambda f, lam, P=None, xpy=np: np.exp(+2j * np.pi * f * lam["D"])   # evaluate_fvals sign
    _, _, Qw = _precompute(injection, _bound(wrong, {"D": Delta}))
    for det in DETS:
        for k in U0[det]:
            np.testing.assert_allclose(Ud[det][k], U0[det][k], rtol=1e-10)
            np.testing.assert_allclose(Vd[det][k], V0[det][k], rtol=1e-10, atol=1e-10 * abs(U0[det][((2, 2), (2, 2))]))
        for mode in Q0[det]:
            q0, qd, qw = Q0[det][mode], Qd[det][mode], Qw[det][mode]
            sc = np.max(np.abs(q0))
            np.testing.assert_allclose(qd[:-shift], q0[shift:], atol=1e-9 * sc)      # Q'(t) = Q(t+Delta)
            assert not np.allclose(qw[:-shift], q0[shift:], atol=1e-3 * sc)        # wrong sign fails
            np.testing.assert_allclose(qw[shift:], q0[:-shift], atol=1e-9 * sc)     # ... it is Q(t-Delta)


def test_precompute_refuses_unsupported_routes(injection):
    P, data, psd = injection
    with pytest.raises(ValueError, match="hlm_transform"):
        FL.PrecomputeLikelihoodTerms(FIDUCIAL, TWIN, P.copy(), data, psd, LMAX, FMAX,
                                     analyticPSD_Q=True, ROM_use_basis=True,
                                     hlm_transform=lambda a, b, P: (a, b))


# --------------------------------------------------------------------- ILE driver

def _env(**extra):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(CODE) + os.pathsep + env.get("PYTHONPATH", "")
    env["CUDA_VISIBLE_DEVICES"] = ""
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("GW_SURROGATE", "")
    env.update(extra)
    return env


def _hp_grid(path, extra_cols=("lens_amp",), extra_vals=(1.0,)):
    from RIFT.misc import hyperpipeline_io as hpio
    P = lalsimutils.ChooseWaveformParams()
    P.m1, P.m2 = 35.0 * lal.MSUN_SI, 30.0 * lal.MSUN_SI
    P.fmin = P.fref = 40.0
    P.ampO = -1
    cols = tuple(hpio.DEFAULT_BASE_COLUMNS) + tuple(extra_cols)
    row = [0, 0, 35.0, 30.0, 0, 0, 0, 0, 0, 0] + list(extra_vals)
    hpio.write_table_with_metadata(str(path), cols, [row], P=P)
    return str(path)


def _identity_plugin(tmp_path):
    return _write_plugin(tmp_path, "ident_plug", """
        import numpy
        EXTRA_PARAMETERS = ["lens_amp"]
        def transfer_function(f, extra_params, P=None, xpy=numpy):
            return extra_params["lens_amp"] * numpy.ones(numpy.shape(f), dtype=complex)
        """)


def _ile_refusal(tmp_path, extra, hyperpipe=True):
    # Data and PSD files do not exist: the refusal must come before anything reads them.
    argv = [sys.executable, str(ILE), "--event-time", "1000000000",
            "--data-start-time", "999999994", "--data-end-time", "1000000002",
            "--cache-file", str(tmp_path / "absent.cache"), "--channel-name", "H1=FAKE-STRAIN",
            "--psd-file", "H1=" + str(tmp_path / "absent_psd.xml.gz"),
            "--fmin-template", "40", "--reference-freq", "40", "--srate", "2048",
            "--l-max", "2", "--n-max", "100", "--output-file", str(tmp_path / "out")] + extra
    env = _env(RIFT_HYPERPIPELINE_FORMAT="1") if hyperpipe else _env()
    if not hyperpipe:
        env.pop("RIFT_HYPERPIPELINE_FORMAT", None)
    return subprocess.run(argv, capture_output=True, text=True, env=env, timeout=600, cwd=str(tmp_path))


@pytest.mark.parametrize("label,extra,hyperpipe,reason", [
    ("no-force-xpy", ["--time-marginalization", "--vectorized", "--gpu"], True, "not on the NoLoop likelihood"),
    ("no-vectorized", ["--time-marginalization", "--gpu", "--force-xpy"], True, "not on the NoLoop likelihood"),
    ("legacy-output", REDLINE, False, "legacy positional .dat output"),
    ("rotation-slow", REDLINE + ["--rotation-slow"], True, "--rotation-slow"),
    ("zero-likelihood", REDLINE + ["--zero-likelihood"], True, "--zero-likelihood"),
    ("peak-local", REDLINE + ["--time-marginalization-quadrature", "peak-local"], True, "peak-local"),
    ("save-samples", REDLINE + ["--save-samples"], True, "--save-samples"),
    ("calmarg", REDLINE + ["--calibration-envelope-directory", "/nonexistent"], True, "calibration marginalization"),
])
def test_ile_refuses_unsupported_configurations(tmp_path, label, extra, hyperpipe, reason):
    grid = _hp_grid(tmp_path / "grid.dat")
    r = _ile_refusal(tmp_path, extra + ["--sim-grid", grid,
                                        "--supplementary-waveform-transform-code", _identity_plugin(tmp_path)],
                     hyperpipe=hyperpipe)
    assert r.returncode != 0, r.stdout[-2000:]
    assert "refusing this job before analysis" in r.stderr, r.stderr[-3000:]
    assert reason in r.stderr, r.stderr[-3000:]


def test_ile_refuses_sim_xml_and_legacy_grid_and_orphan_flags(tmp_path):
    plug = _identity_plugin(tmp_path)
    r = _ile_refusal(tmp_path, REDLINE + ["--sim-xml", str(tmp_path / "x.xml.gz"),
                                         "--supplementary-waveform-transform-code", plug])
    assert r.returncode != 0 and "--sim-xml" in r.stderr, r.stderr[-2000:]
    legacy = tmp_path / "legacy.dat"
    legacy.write_text("# m1 m2 lens_amp\n35 30 1\n")
    r = _ile_refusal(tmp_path, REDLINE + ["--sim-grid", str(legacy),
                                         "--supplementary-waveform-transform-code", plug])
    assert r.returncode != 0 and "not a self-describing Hyperpipe grid" in r.stderr, \
        (r.stdout + r.stderr)[-3000:]
    r = _ile_refusal(tmp_path, REDLINE + ["--supplementary-waveform-transform-parameter", "x"])
    assert r.returncode != 0 and "require --supplementary-waveform-transform-code" in r.stderr


def test_ile_grid_column_checks(tmp_path):
    plug = _identity_plugin(tmp_path)
    for cols, vals, extra, msg in [
            (("lens_amp", "stray"), (1.0, 2.0), [], "neither intrinsic parameters nor declared"),
            ((), (), [], "are not columns of --sim-grid"),
            (("lens_amp",), (1.0,), ["--supplementary-waveform-transform-parameter", "m1"], "collide")]:
        grid = _hp_grid(tmp_path / "g.dat", cols, vals)
        r = _ile_refusal(tmp_path, REDLINE + ["--sim-grid", grid,
                                             "--supplementary-waveform-transform-code", plug] + extra)
        assert r.returncode != 0 and msg in r.stderr, (msg, (r.stdout + r.stderr)[-3000:])


@pytest.fixture(scope="module")
def zero_frame(tmp_path_factory):
    import lal.series
    from igwn_ligolw import utils as ligolw_utils
    outdir = tmp_path_factory.mktemp("seam_frame")
    event_time, srate, seg_start, seg_end = 1000000000.0, 2048.0, 1000000000.0 - 6.0, 1000000002.0
    duration = seg_end - seg_start
    npts = int(round(duration * srate))
    ht = lal.CreateREAL8TimeSeries("Zero strain", lal.LIGOTimeGPS(seg_start), 0.0, 1.0 / srate,
                                   lalsimutils.lsu_DimensionlessUnit, npts)
    ht.data.data = np.zeros(npts)
    fname = outdir / ("H-fake_strain-%d-%d.gwf" % (int(seg_start), int(duration)))
    lalsimutils.hoft_to_frame_data(str(fname), "H1:FAKE-STRAIN", ht)
    cache = outdir / "test.cache"
    os.system("echo %s | lal_path2cache > %s" % (fname, cache))
    psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0), 0, 1.0 / duration, lal.SecondUnit, npts // 2 + 1)
    farr = np.arange(psd.data.length) * psd.deltaF
    psd.data.data = np.where(farr > 1.0, [lalsim.SimNoisePSDaLIGOZeroDetHighPower(f) for f in farr], 1.0)
    psd_path = outdir / "H1_psd.xml.gz"
    ligolw_utils.write_filename(lal.series.make_psd_xmldoc({"H1": psd}), str(psd_path))
    return dict(cache=cache, psd=psd_path, event_time=event_time, seg_start=seg_start, seg_end=seg_end)


def _ile_run(tmp_path, zf, grid, out, extra):
    argv = [sys.executable, str(ILE), "--cache-file", str(zf["cache"]), "--channel-name", "H1=FAKE-STRAIN",
            "--psd-file", "H1=%s" % zf["psd"], "--event-time", str(zf["event_time"]),
            "--data-start-time", str(zf["seg_start"]), "--data-end-time", str(zf["seg_end"]),
            "--sim-grid", grid, "--approximant", "IMRPhenomD", "--l-max", "2",
            "--reference-freq", "40", "--fmin-template", "40", "--srate", "2048",
            "--inv-spec-trunc-time", "0", "--n-max", "4000", "--n-eff", "10", "--n-chunk", "1000",
            "--d-max", "1000", "--seed", "11", "--output-file", str(out)] + REDLINE + extra
    r = subprocess.run(argv, capture_output=True, text=True, env=_env(RIFT_HYPERPIPELINE_FORMAT="1"),
                       timeout=900, cwd=str(tmp_path))
    assert r.returncode == 0, (r.stdout + r.stderr)[-4000:]
    assert "FAILED ANALYSIS" not in r.stdout, r.stdout[-4000:]
    assert "NOT APPLIED" not in r.stdout or "calibration-fused-kernel" in r.stdout
    return r


def test_ile_identity_plugin_is_byte_identical_and_echoes_lambda(tmp_path, zero_frame):
    from RIFT.misc import hyperpipeline_io as hpio
    grid = _hp_grid(tmp_path / "grid.dat", ("lens_amp",), (1.0,))
    _ile_run(tmp_path, zero_frame, grid, tmp_path / "base", [])
    r = _ile_run(tmp_path, zero_frame, grid, tmp_path / "seam",
                 ["--supplementary-waveform-transform-code", _identity_plugin(tmp_path)])
    assert "EXTERNAL WAVEFORM TRANSFORM" in r.stdout and "'lens_amp': 1.0" in r.stdout
    base, bcols = hpio.read_table(str(tmp_path / "base_0_.dat"))
    seam, scols = hpio.read_table(str(tmp_path / "seam_0_.dat"))
    assert tuple(scols) == tuple(bcols) + ("lens_amp",)
    assert float(seam["lens_amp"][0]) == 1.0
    for c in bcols:
        assert float(seam[c][0]) == float(base[c][0]), c
    assert np.isfinite(float(base["lnL"][0]))
