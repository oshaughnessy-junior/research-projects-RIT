"""End-to-end recovery of an injected LENSED signal through ILE's extra-parameter seam.

A lensed IMRPhenomD signal is injected into zero-noise H1 and L1 frames.  The lens is applied
INDEPENDENTLY of RIFT's packed arrays: each detector's strain h(t) is filtered with numpy's
real FFT (standard sign, rfft ~ exp(-2 pi i f t)) by the demo plugin's F(f >= 0).  ILE then
evaluates a Hyperpipe grid at the TRUE intrinsic point with several lambda values, through
the seam, with all four REDLINE flags and the sky, distance and inclination pinned to the
injection (orbital phase, polarization and time are marginalized).

Checks, for the constant-amplitude demo (lambda = lens_amplitude, true A_t):
  * lnL is maximal at A_t on the grid;
  * lnL(A) - lnL(A_t) matches the analytic -(A - A_t)^2 rho_0^2 / 2 (rho_0 the unlensed
    optimal SNR at the pinned distance, computed with numpy from the injected strain), to
    within the time/phase Occam term ln(A_t / A) and MC error;
  * lnL(A_t) is rho_L^2/2 (rho_L the lensed optimal SNR, numpy) less the time/phase Occam
    factor (measured 11.7 nats, bound OCCAM_MAX), and never above it;
and for the geometric-optics point lens (lambda = lens_mass_z, lens_y; y scanned):
  * lnL is maximal at the true y and lnL(true) is again rho_L^2/2 less the same Occam factor.
A conjugated (wrong-sign) or absent transform fails these checks: the PR #518 review measured
lnL differences of hundreds of nats for both.

Runtime ~30 s on one CPU core (two ILE runs, 8 grid points, SNR ~25).
"""
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

import lal
import lalsimulation as lalsim
import RIFT.lalsimutils as lalsimutils

CODE = Path(__file__).resolve().parents[1]
ILE = CODE / "bin" / "integrate_likelihood_extrinsic_batchmode"
DEMO = CODE / "demo" / "waveform_transform"
REDLINE = ["--time-marginalization", "--vectorized", "--gpu", "--force-xpy"]

SRATE = 2048.0
EVENT_TIME = 1000000000.0
SEG_START, SEG_END = EVENT_TIME - 6.0, EVENT_TIME + 2.0
FMIN = 30.0
IFOS = ("H1", "L1")
INJ = dict(m1=35.0, m2=30.0, dist=2000.0, ra=1.2, dec=-0.4, incl=0.5, psi=0.3, phiref=0.7)
A_TRUE = 1.3
PL_TRUE = dict(lens_mass_z=3000.0, lens_y=0.8)
# lnL at the truth is the time- and (phi_orb, psi)-marginalized likelihood, so it sits below
# rho^2/2 by an Occam factor.  Measured 11.7 nats for both lenses here (rho_L^2/2 = 342 and
# 314; venv python 3.13, lalsuite 8, 2026-11-02); the bound leaves room for MC error.
OCCAM_MAX = 15.0


def _env():
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(CODE)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))
    env["PATH"] = os.pathsep.join([str(CODE / "bin"), env.get("PATH", "")])
    env["GW_SURROGATE"] = ""
    env["OMP_NUM_THREADS"] = "1"
    env["RIFT_HYPERPIPELINE_FORMAT"] = "1"
    return env


def _P(**over):
    P = lalsimutils.ChooseWaveformParams()
    P.m1, P.m2 = INJ["m1"] * lal.MSUN_SI, INJ["m2"] * lal.MSUN_SI
    P.dist = INJ["dist"] * 1e6 * lal.PC_SI
    P.theta, P.phi, P.incl, P.psi, P.phiref = INJ["dec"], INJ["ra"], INJ["incl"], INJ["psi"], INJ["phiref"]
    P.radec = True
    P.tref = EVENT_TIME
    P.fmin = P.fref = FMIN
    P.deltaT = 1.0 / SRATE
    P.approx = lalsim.IMRPhenomD
    P.ampO = -1
    for k, v in over.items():
        setattr(P, k, v)
    return P


def _psd(f):
    f = np.asarray(f, dtype=float)
    out = np.ones_like(f)
    m = f > 1.0
    out[m] = [lalsim.SimNoisePSDaLIGOZeroDetHighPower(x) for x in f[m]]
    return out


def _strain(det):
    """Unlensed detector strain on [SEG_START, SEG_END) (numpy array)."""
    P = _P(detector=det)
    ht = lalsimutils.hoft(P)
    n = int(round((SEG_END - SEG_START) * SRATE))
    out = np.zeros(n)
    i0 = int(round((float(ht.epoch) - SEG_START) * SRATE))
    src = ht.data.data
    lo, hi = max(i0, 0), min(i0 + len(src), n)
    out[lo:hi] = src[lo - i0:hi - i0]
    return out


def _lens(x, F_of_f):
    """Apply a real kernel with frequency response F(f >= 0) (standard sign) via numpy rfft."""
    X = np.fft.rfft(x)
    f = np.fft.rfftfreq(len(x), 1.0 / SRATE)
    F = np.asarray(F_of_f(f), dtype=complex)
    F[0] = F[0].real
    if len(x) % 2 == 0:
        F[-1] = F[-1].real
    return np.fft.irfft(X * F, n=len(x))


def _rho2(x):
    """Optimal SNR^2 4 df sum |X|^2/S over [FMIN, Nyquist), numpy only."""
    X = np.fft.rfft(x) / SRATE
    f = np.fft.rfftfreq(len(x), 1.0 / SRATE)
    df = f[1] - f[0]
    m = (f >= FMIN) & (f < SRATE / 2)
    return 4.0 * df * float(np.sum(np.abs(X[m]) ** 2 / _psd(f[m])))


def _load_demo(name):
    import importlib.util
    spec = importlib.util.spec_from_file_location("_inj_demo_" + name, str(DEMO / (name + ".py")))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write_frames(outdir, label, strains):
    from igwn_ligolw import utils as ligolw_utils
    import lal.series
    paths = []
    for det, x in strains.items():
        ht = lal.CreateREAL8TimeSeries("strain", lal.LIGOTimeGPS(SEG_START), 0.0, 1.0 / SRATE,
                                       lalsimutils.lsu_DimensionlessUnit, len(x))
        ht.data.data = np.asarray(x, dtype=float)
        fname = outdir / ("%s-%s_%s-%d-%d.gwf" % (det[0], label, det, int(SEG_START), int(SEG_END - SEG_START)))
        lalsimutils.hoft_to_frame_data(str(fname), "%s:FAKE-STRAIN" % det, ht)
        paths.append(str(fname))
    cache = outdir / ("%s.cache" % label)
    subprocess.run("ls %s | lal_path2cache > %s" % (" ".join(paths), cache), shell=True, check=True)
    npts = int(round((SEG_END - SEG_START) * SRATE))
    for det in strains:
        psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0), 0, 1.0 / (SEG_END - SEG_START),
                                             lal.SecondUnit, npts // 2 + 1)
        psd.data.data = _psd(np.arange(psd.data.length) * psd.deltaF)
        p = outdir / ("%s_psd.xml.gz" % det)
        if not p.exists():
            ligolw_utils.write_filename(lal.series.make_psd_xmldoc({det: psd}), str(p))
    return cache


def _grid(path, cols, rows):
    from RIFT.misc import hyperpipeline_io as hpio
    P = _P()
    allcols = tuple(hpio.DEFAULT_BASE_COLUMNS) + tuple(cols)
    hpio.write_table_with_metadata(str(path), allcols,
                                   [[0, 0, INJ["m1"], INJ["m2"], 0, 0, 0, 0, 0, 0] + list(r) for r in rows], P=P)
    return str(path)


def _run_ile(tmp, cache, grid, n_rows, plugin, out):
    argv = [sys.executable, str(ILE), "--cache-file", str(cache), "--event-time", str(EVENT_TIME),
            "--data-start-time", str(SEG_START), "--data-end-time", str(SEG_END),
            "--sim-grid", grid, "--event", "0", "--n-events-to-analyze", str(n_rows),
            "--approximant", "IMRPhenomD", "--l-max", "2",
            "--reference-freq", str(FMIN), "--fmin-template", str(FMIN), "--srate", str(int(SRATE)),
            "--inv-spec-trunc-time", "0", "--n-max", "20000", "--n-eff", "50", "--n-chunk", "4000",
            "--d-max", "5000", "--seed", "5",
            "--right-ascension", repr(INJ["ra"]), "--declination", repr(INJ["dec"]),
            "--distance", repr(INJ["dist"]), "--inclination", repr(INJ["incl"]),
            "--supplementary-waveform-transform-code", str(plugin),
            "--output-file", str(out)] + REDLINE
    for det in IFOS:
        argv += ["--channel-name", "%s=FAKE-STRAIN" % det, "--psd-file", "%s=%s" % (det, tmp / ("%s_psd.xml.gz" % det))]
    r = subprocess.run(argv, capture_output=True, text=True, env=_env(), timeout=1200, cwd=str(tmp))
    assert r.returncode == 0, (r.stdout + r.stderr)[-4000:]
    assert "FAILED ANALYSIS" not in r.stdout, r.stdout[-4000:]
    assert "NOT APPLIED" not in r.stdout, r.stdout[-4000:]
    from RIFT.misc import hyperpipeline_io as hpio
    rows = []
    for i in range(n_rows):
        a, _ = hpio.read_table(str(tmp / ("%s_%d_.dat" % (out.name, i))))
        rows.append(a)
    return r, rows


@pytest.fixture(scope="module")
def unlensed(tmp_path_factory):
    return {det: _strain(det) for det in IFOS}


def test_constant_amplitude_lens_is_recovered(tmp_path, unlensed):
    demo = _load_demo("constant_amplitude_phase")
    F = lambda f: demo.transfer_function(f, {"lens_amplitude": A_TRUE, "lens_phase": 0.0})
    lensed = {d: _lens(x, F) for d, x in unlensed.items()}
    rho0_2 = sum(_rho2(x) for x in unlensed.values())
    rhoL_2 = sum(_rho2(x) for x in lensed.values())
    np.testing.assert_allclose(rhoL_2, A_TRUE ** 2 * rho0_2, rtol=1e-6)   # sanity of the numpy lens
    cache = _write_frames(tmp_path, "const", lensed)
    amps = [A_TRUE * s for s in (0.8, 0.9, 1.0, 1.1, 1.2)]
    grid = _grid(tmp_path / "grid.dat", ("lens_amplitude", "lens_phase"), [(a, 0.0) for a in amps])
    r, rows = _run_ile(tmp_path, cache, grid, len(amps), DEMO / "constant_amplitude_phase.py", tmp_path / "const")
    lnL = np.array([float(a["lnL"][0]) for a in rows])
    sig = np.array([float(a["sigma_lnL"][0]) for a in rows])
    got = [float(a["lens_amplitude"][0]) for a in rows]
    np.testing.assert_allclose(got, amps)                                   # lambda echoed per row
    print("rho0^2", rho0_2, "rhoL^2/2", rhoL_2 / 2, "lnL", lnL, "sigma", sig)
    i_t = 2
    assert int(np.argmax(lnL)) == i_t, lnL
    # Peak value: rho_L^2/2 minus the time/phase Occam factor (pinned sky/distance/inclination).
    assert rhoL_2 / 2 - OCCAM_MAX < lnL[i_t] < rhoL_2 / 2, (lnL[i_t], rhoL_2 / 2)
    # Shape: lnL(A) - lnL(A_t) = -(A - A_t)^2 rho_0^2 / 2 + ln(A_t/A) (two constrained
    # directions whose width scales as 1/sqrt(A A_t)), within MC error.
    for i, A in enumerate(amps):
        expect = -0.5 * (A - A_TRUE) ** 2 * rho0_2 + np.log(A_TRUE / A)
        tol = 0.6 + 3 * np.hypot(sig[i], sig[i_t])
        assert abs((lnL[i] - lnL[i_t]) - expect) < tol, (A, lnL[i] - lnL[i_t], expect, tol)


def test_point_lens_impact_parameter_is_recovered(tmp_path, unlensed):
    demo = _load_demo("point_lens_geometric_optics")
    F = lambda f: demo.transfer_function(f, PL_TRUE)
    lensed = {d: _lens(x, F) for d, x in unlensed.items()}
    rhoL_2 = sum(_rho2(x) for x in lensed.values())
    cache = _write_frames(tmp_path, "pointlens", lensed)
    ys = [0.5, PL_TRUE["lens_y"], 1.2]
    grid = _grid(tmp_path / "grid.dat", ("lens_mass_z", "lens_y"), [(PL_TRUE["lens_mass_z"], y) for y in ys])
    r, rows = _run_ile(tmp_path, cache, grid, len(ys), DEMO / "point_lens_geometric_optics.py", tmp_path / "pl")
    lnL = np.array([float(a["lnL"][0]) for a in rows])
    print("rhoL^2/2", rhoL_2 / 2, "lnL", lnL)
    assert int(np.argmax(lnL)) == 1, lnL
    assert lnL[1] > max(lnL[0], lnL[2]) + 5, lnL
    assert rhoL_2 / 2 - OCCAM_MAX < lnL[1] < rhoL_2 / 2, (lnL[1], rhoL_2 / 2)
