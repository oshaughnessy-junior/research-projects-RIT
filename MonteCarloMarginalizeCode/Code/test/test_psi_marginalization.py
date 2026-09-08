"""
Tests for --psi-marginalization: analytic polarization-angle marginalization,
made reachable on the legacy scalar (non-vectorized, non-GPU, non-time-marginalized)
likelihood path via factored_likelihood.NetworkLogLikelihoodPolarizationMarginalized.

Before this change the function was dead code: its only tests are LEGACY (cannot
import: test_like_and_samp_margPsi.py, test_like_and_samp_noisydata_margPsi.py) and
no production driver called it.  It also had a live bug -- crossTermsV was indexed as
crossTermsV[(pair1,pair2)] instead of crossTermsV[det][(pair1,pair2)], which KeyErrors
immediately against the real precompute structure (crossTermsV is a dict keyed by
detector, then by mode pair) -- fixed alongside this PR.

1. test_matches_bruteforce_quadrature: the analytic marginal against a brute-force
   trapezoid over psi of exp(lnL(psi)) from the un-marginalized scalar likelihood, at
   two amplitudes, with a convergence sequence (the memory-known invariant: lnL(psi)
   is exactly two harmonics, so a coarse trapezoid on the LOG-likelihood would be
   exact, but exp(lnL) is not band-limited, so exp(lnL(psi)) needs many nodes).
2. test_flag_reaches_help: the option is registered.
3. test_refuses_incompatible_combinations: every documented refusal actually fires,
   with no data files needed (the refusal runs before any data is read).
4. test_driver_runs_end_to_end: the driver actually completes a tiny run on
   synthetic zero-signal data and writes a result row.
"""

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

import lal
import lal.series
import lalsimulation as lalsim
from igwn_ligolw import utils as ligolw_utils
import RIFT.lalsimutils as lalsimutils
import RIFT.likelihood.factored_likelihood as FL

MSUN = lal.MSUN_SI
PC = lal.PC_SI

BIN = Path(__file__).resolve().parents[1] / "bin" / "integrate_likelihood_extrinsic_batchmode"


###
### 1. Correctness: analytic marginal vs brute-force quadrature
###

def _make_injection(detectors, dist_mpc):
    fiducial_epoch = 1126259462.0
    P = lalsimutils.ChooseWaveformParams()
    P.m1 = 35.0 * MSUN
    P.m2 = 30.0 * MSUN
    P.s1z = 0.1
    P.s2z = -0.2
    P.fmin = 30.0
    P.fref = 30.0
    P.deltaT = 1.0 / 4096
    P.deltaF = 1.0 / 4
    P.dist = dist_mpc * 1e6 * PC
    P.fmax = 0.0
    P.approx = lalsim.IMRPhenomD
    P.radec = True
    P.tref = fiducial_epoch
    P.phi = 1.2       # RA
    P.theta = -0.4    # DEC
    P.psi = 0.7
    P.incl = 0.9
    P.phiref = 2.1
    data_dict, psd_dict = {}, {}
    for det in detectors:
        Pdet = P.copy()
        Pdet.detector = det
        data_dict[det] = lalsimutils.non_herm_hoff(Pdet)
        psd_dict[det] = lalsim.SimNoisePSDaLIGOZeroDetHighPower
    return P, data_dict, psd_dict, fiducial_epoch


def _bruteforce_psi_marginal(rholms_intp, cross_terms, cross_terms_V, P, Lmax, nnodes):
    """(1/pi) int_0^pi exp(lnL(psi)) dpsi, by trapezoid, from the UN-marginalized
    scalar likelihood -- an independent route from NetworkLogLikelihoodPolarizationMarginalized:
    a full network sum of complex antenna-pattern products vs. per-psi calls into the
    ordinary FactoredLogLikelihood used by the scalar sampled-psi path.
    """
    psis = np.linspace(0.0, np.pi, nnodes)
    extr = lalsimutils.ChooseWaveformParams()
    extr.phi, extr.theta = P.phi, P.theta
    extr.incl, extr.phiref, extr.dist, extr.tref = P.incl, P.phiref, P.dist, P.tref
    lnLs = np.empty(nnodes)
    for i, psi in enumerate(psis):
        extr.psi = psi
        lnLs[i] = FL.FactoredLogLikelihood(
            extr, None, rholms_intp, cross_terms, cross_terms_V, Lmax, interpolate=True)
    integral = np.trapz(np.exp(lnLs), psis) / np.pi
    return np.log(integral)


@pytest.mark.parametrize("dist_mpc,label", [(1200.0, "low_amplitude"), (600.0, "moderate_amplitude")])
def test_matches_bruteforce_quadrature(dist_mpc, label):
    detectors = ["H1", "L1"]
    P, data_dict, psd_dict, fiducial_epoch = _make_injection(detectors, dist_mpc)
    Lmax, fMax, t_window = 2, 1000.0, 0.15

    rholms_intp, cross_terms, cross_terms_V, rholms, guess_snr, _rest = FL.PrecomputeLikelihoodTerms(
        fiducial_epoch, t_window, P, data_dict, psd_dict, Lmax, fMax,
        analyticPSD_Q=True, verbose=False, quiet=True, ignore_threshold=None)
    assert guess_snr > 0.5, "fixture should carry a real, detectable signal (%s)" % label

    lnL_analytic = FL.NetworkLogLikelihoodPolarizationMarginalized(
        fiducial_epoch, rholms_intp, cross_terms, cross_terms_V,
        P.tref, P.phi, P.theta, P.incl, P.phiref, P.psi, P.dist, Lmax, detectors)
    assert np.isfinite(lnL_analytic)

    # Convergence sequence.  A coarse (16-node) trapezoid must NOT already be at the
    # 1e-3 nat tolerance -- exp(lnL(psi)) is not the two-harmonic function itself, so a
    # rule that is exact for lnL(psi) is not exact here.  A dense (1024-node) trapezoid
    # must be.
    bf_coarse = _bruteforce_psi_marginal(rholms_intp, cross_terms, cross_terms_V, P, Lmax, 16)
    bf_dense = _bruteforce_psi_marginal(rholms_intp, cross_terms, cross_terms_V, P, Lmax, 1024)

    assert abs(lnL_analytic - bf_dense) < 1e-3, (
        "%s: analytic %.10f vs dense brute force %.10f (diff %.3e)"
        % (label, lnL_analytic, bf_dense, lnL_analytic - bf_dense))
    assert abs(lnL_analytic - bf_coarse) > 1e-3, (
        "%s: a 16-node trapezoid over exp(lnL(psi)) should NOT already be converged "
        "to 1e-3 nat -- if it is, this test's coarse/dense contrast proves nothing"
        % label)


###
### 2. The flag is registered
###

def test_flag_reaches_help():
    env = dict(os.environ)
    out = subprocess.run([sys.executable, str(BIN), "--help"], env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          universal_newlines=True, timeout=60)
    assert "--psi-marginalization" in out.stdout


###
### 3. Refusals fire (no data files needed -- the refusal runs before any data load)
###

# A minimal distance-marginalization lookup table, just complete enough (a
# "phase_marginalization" key) for the driver to get past its OWN np.load() and
# earliest phase-marginalization read -- both of which run before our refusal check
# -- so the --distance-marginalization case below actually reaches this option's
# refusal instead of failing on an unrelated missing-file error first.
import tempfile as _tempfile
_LOOKUP_TABLE_PATH = os.path.join(
    _tempfile.mkdtemp(prefix="psi_marg_dummy_lookup_"), "lookup.npz")
np.savez(_LOOKUP_TABLE_PATH, phase_marginalization=np.array(False))

_REFUSAL_CASES = [
    (["--time-marginalization"], "time-marginalization"),
    (["--vectorized"], "vectorized"),
    (["--distance-marginalization", "--distance-marginalization-lookup-table", _LOOKUP_TABLE_PATH],
     "distance-marginalization"),
    (["--rotation-slow", "--vectorized"], "rotation-slow"),
    (["--freqresponse", "--vectorized"], "freqresponse"),
    (["--calibration-envelope-directory", "/nonexistent"], "calibration"),
    (["--interpolate-time", "nearest"], "interpolate-time"),
    (["--sampler-method", "GMM"], "sampler-method"),
    (["--internal-rotate-phase"], "internal-rotate-phase"),
    (["--limit-psi", "0,1"], "limit-psi"),
    (["--zero-likelihood"], "zero-likelihood"),
]


@pytest.mark.parametrize("extra_args,label", _REFUSAL_CASES, ids=[c[1] for c in _REFUSAL_CASES])
def test_refuses_incompatible_combinations(extra_args, label):
    env = dict(os.environ)
    cmd = [sys.executable, str(BIN), "--event-time", "1000000000.0",
           "--psi-marginalization"] + extra_args
    proc = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           universal_newlines=True, timeout=60)
    assert proc.returncode != 0, (
        "--psi-marginalization + %s should be REFUSED, not silently accepted" % label)
    assert "--psi-marginalization was requested, but this configuration cannot honour it" \
        in proc.stdout, proc.stdout[-2000:]


def test_gpu_prereq_present_in_source():
    """--gpu cannot be exercised as a subprocess refusal on a host without cupy: the
    driver's own (unrelated, pre-existing) '--gpu (not available)' downgrade clears
    opts.gpu back to False before this option's check ever runs, so the combination
    silently stops being a --gpu run at all rather than exercising the refusal.  Same
    story for a portfolio member carrying a GMM group, which is config-file-dependent
    and not reachable from a bare CLI probe.  Check the source directly instead, the
    same way test_ile_scalar_edge_cases.py checks call-site wiring it cannot run live.
    """
    text = BIN.read_text()
    start = text.index("_psi_marg_prereqs = (")
    end = text.index("_psi_marg_missing = [", start)
    block = text[start:end]
    assert "not bool(opts.gpu)" in block
    assert "'GMM', 'portfolio'" in block


###
### 4. End-to-end: the driver actually runs and writes a result row
###

@pytest.fixture(scope="module")
def synthetic_fixture(tmp_path_factory):
    """A tiny zero-signal H1 frame + PSD xml + LAL cache, so the driver can be run as
    a real subprocess without a network fetch or a real event.  Zero data (rather than
    an injected waveform) keeps the fixture simple and avoids sky/antenna-response
    bookkeeping that is irrelevant to what this test checks: that the WIRING for
    --psi-marginalization completes a run and writes output, not that the recovered
    parameters are accurate.
    """
    outdir = tmp_path_factory.mktemp("psi_marg_fixture")
    event_time = 1000000000.0
    srate = 2048.0
    deltaT = 1.0 / srate
    seg_start = event_time - 6.0
    seg_end = event_time + 2.0
    duration = seg_end - seg_start
    npts = int(round(duration / deltaT))

    channel = "H1:FAKE-STRAIN"
    ht = lal.CreateREAL8TimeSeries(
        "Zero strain", lal.LIGOTimeGPS(seg_start), 0.0, deltaT,
        lalsimutils.lsu_DimensionlessUnit, npts)
    ht.data.data = np.zeros(npts)

    fname = outdir / ("H-fake_strain-%d-%d.gwf" % (int(seg_start), int(duration)))
    lalsimutils.hoft_to_frame_data(str(fname), channel, ht)

    cache_path = outdir / "test.cache"
    os.system("echo %s | lal_path2cache > %s" % (fname, cache_path))

    psd_series = lal.CreateREAL8FrequencySeries(
        "psd", lal.LIGOTimeGPS(0), 0, 1.0 / duration, lal.SecondUnit, npts // 2 + 1)
    farr = psd_series.f0 + np.arange(psd_series.data.length) * psd_series.deltaF
    psd_vals = np.where(farr > 1.0, [lalsim.SimNoisePSDaLIGOZeroDetHighPower(f) for f in farr], 1.0)
    psd_series.data.data = psd_vals

    xmldoc = lal.series.make_psd_xmldoc({"H1": psd_series})
    psd_path = outdir / "H1_psd.xml.gz"
    ligolw_utils.write_filename(xmldoc, str(psd_path))

    return dict(outdir=outdir, cache=cache_path, psd=psd_path,
                event_time=event_time, seg_start=seg_start, seg_end=seg_end)


def test_driver_runs_end_to_end(synthetic_fixture):
    outdir = synthetic_fixture["outdir"]
    env = dict(os.environ)
    cmd = [
        sys.executable, str(BIN),
        "--cache-file", str(synthetic_fixture["cache"]),
        "--channel-name", "H1=FAKE-STRAIN",
        "--psd-file", "H1=%s" % synthetic_fixture["psd"],
        "--event-time", str(synthetic_fixture["event_time"]),
        "--data-start-time", str(synthetic_fixture["seg_start"]),
        "--data-end-time", str(synthetic_fixture["seg_end"]),
        "--mass1", "35.0", "--mass2", "30.0", "--approximant", "TaylorT4",
        "--l-max", "2", "--reference-freq", "40", "--fmin-template", "40",
        "--srate", "2048", "--inv-spec-trunc-time", "0",
        "--sampler-method", "AV",
        "--psi-marginalization", "--n-max", "8", "--n-eff", "2",
        "--output-file", "out.xml.gz", "--save-samples",
    ]
    proc = subprocess.run(cmd, cwd=str(outdir), env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           universal_newlines=True, timeout=180)
    assert proc.returncode == 0, proc.stdout[-4000:]
    assert "ANALYTICALLY MARGINALIZED (--psi-marginalization)" in proc.stdout

    result_dat = outdir / "out.xml.gz_0_.dat"
    assert result_dat.exists(), "driver did not write a result row\n" + proc.stdout[-4000:]
    row = result_dat.read_text().split()
    # event_id, m1, m2, 6 spin components, lnL, sigma_lnL, ntotal, neff (13 columns);
    # verified against this run's own out.xml.gz_0_integrator_status.json (column 9 ==
    # "lnL" there) rather than assumed from another test's layout.
    assert len(row) == 13, row
    assert float(row[1]) == pytest.approx(35.0)
    assert float(row[2]) == pytest.approx(30.0)
    lnL_row = float(row[9])
    assert np.isfinite(lnL_row)

    status_path = outdir / "out.xml.gz_0_integrator_status.json"
    if status_path.exists():
        import json
        status = json.loads(status_path.read_text())
        assert status["lnL"] == pytest.approx(lnL_row)
        assert np.isfinite(status["lnL"])
