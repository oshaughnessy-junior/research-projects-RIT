"""End-to-end RIFT pipeline on a case whose answer is known in CLOSED FORM.

WHY THIS EXISTS.  The sampler unit tests check samplers; the pipeline gates (pseudo_pipe,
asimov) check that the pipeline RUNS.  Nothing checked that the pipeline runs and is CORRECT,
and that gap hid two defects that four rounds of adversarial review of the drivers did not
find:

  * --zero-likelihood replaces the whole likelihood function with a stand-in, so a
    --supplementary-likelihood-factor-* passed alongside it was never applied.  The startup
    banner still printed "EXTERNAL SUPPLEMENTARY LIKELIHOOD FACTOR", and two runs differing
    only by that option returned ln Z bit-identical to every digit.
  * ILE --sampler-method GMM returns an evidence wrong by 29 nats on a flat target and 136
    nats on a peaked one, with a reported sigma of 0.178.  Pre-existing; see
    test_gmm_lane_is_known_wrong below.

THE TECHNIQUE.  --zero-likelihood makes the signal term exactly 0, so ln Z is the log of the
prior mass, which is 0 for normalized priors.  Adding an analytic factor A*cos(phi_orb) moves
it to ln I0(A) exactly, because phi_orb's prior is uniform on [0, 2pi) and the factor depends
on nothing else.  That is the constant-integrand trick of the sampler unit tests lifted to the
pipeline: no fit error, no MC scatter in the TARGET, and an answer you can write down.

A third expectation falls out free and is checked by the full-stage test: the likelihood is
CONSTANT across the intrinsic grid, so CIP's recovered intrinsic posterior must reproduce its
own prior, with no reference run needed.

COST.  About 7 s per ILE arm on one core; the whole module is a couple of minutes.  It needs
no network, no real event and no GPU, so it is cheap enough to run per configuration.
"""
import json
import os
import shutil
import subprocess
import sys

import numpy as np
import pytest

lal = pytest.importorskip("lal")
lalsim = pytest.importorskip("lalsimulation")
_lal_series = pytest.importorskip("lal.series")
_ligolw_utils = pytest.importorskip("igwn_ligolw.utils")
import RIFT.lalsimutils as lalsimutils

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.abspath(os.path.join(HERE, "..", "bin"))
ILE = os.path.join(BIN, "integrate_likelihood_extrinsic_batchmode")
CIP = os.path.join(BIN, "util_ConstructIntrinsicPosterior_GenericCoordinates.py")
SUPPLEMENT_MODULE = "analytic_supplement_for_e2e"

# TOLERANCE, MEASURED not guessed.  Each ILE run reports its own sigma_lnL, so the gate is a
# z-test and re-calibrates itself if n-max/n-eff change.  Over 8 seeds x 2 samplers on
# ldas-grid the observed |z| was at most 2.11 (AV) and 1.64 (portfolio), mean ~1.0.  Five
# sigma sits 2.4x above the worst observed draw and ~150x below the defect it guards (the GMM
# lane is 766 sigma out), so the two cannot be confused.
Z_TOLERANCE = 5.0
# ...and a floor on informativeness: a z-test passes vacuously if sigma is huge.  Observed
# sigma was 0.0125-0.0157, so 0.05 is 3x the worst and still makes 5 sigma a 0.25-nat
# constraint.  A run that cannot meet this has not converged and the gate says so.
MAX_SIGMA = 0.05


# ---------------------------------------------------------------------------------------
# fixture: a zero-strain event.  No network, no real data.

@pytest.fixture(scope="module")
def event(tmp_path_factory):
    """Zero-strain H1 frame + PSD + LAL cache.  Copied in shape from the fixture in
    test_psi_marginalization.py, which is already exercised in CI."""
    if shutil.which("lal_path2cache") is None:
        pytest.skip("lal_path2cache not on PATH")
    out = tmp_path_factory.mktemp("e2e_analytic")
    t0, srate = 1000000000.0, 2048.0
    dt = 1.0 / srate
    seg_start, seg_end = t0 - 6.0, t0 + 2.0
    dur = seg_end - seg_start
    npts = int(round(dur / dt))
    ht = lal.CreateREAL8TimeSeries("Zero strain", lal.LIGOTimeGPS(seg_start), 0.0, dt,
                                   lalsimutils.lsu_DimensionlessUnit, npts)
    ht.data.data = np.zeros(npts)
    frame = out / ("H-fake_strain-%d-%d.gwf" % (int(seg_start), int(dur)))
    lalsimutils.hoft_to_frame_data(str(frame), "H1:FAKE-STRAIN", ht)
    cache = out / "test.cache"
    if os.system("echo %s | lal_path2cache > %s" % (frame, cache)) != 0:
        pytest.skip("lal_path2cache failed")
    psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0), 0, 1.0 / dur,
                                         lal.SecondUnit, npts // 2 + 1)
    f = psd.f0 + np.arange(psd.data.length) * psd.deltaF
    psd.data.data = np.where(f > 1.0,
                             [lalsim.SimNoisePSDaLIGOZeroDetHighPower(x) for x in f], 1.0)
    _ligolw_utils.write_filename(_lal_series.make_psd_xmldoc({"H1": psd}),
                                 str(out / "H1_psd.xml.gz"))
    return dict(dir=out, cache=cache, psd=out / "H1_psd.xml.gz",
                t0=t0, seg_start=seg_start, seg_end=seg_end)


SAMPLER_ARGS = {
    "AV": ["--sampler-method", "AV"],
    "portfolio": ["--sampler-method", "portfolio",
                  "--sampler-portfolio", "AV", "--sampler-portfolio", "AC"],
    "GMM": ["--sampler-method", "GMM"],
}


def _run_ile(event, tag, sampler, a_coeff=None, mass=(35.0, 30.0), n_max=20000, n_eff=250,
             seed=1000, extra=()):
    """One ILE job as a subprocess.  Returns (lnL, sigma_lnL) from the output row."""
    d = event["dir"] / tag
    d.mkdir(exist_ok=True)
    env = dict(os.environ)
    # CUDA_VISIBLE_DEVICES="" is required, and is not this test's problem: on a host where
    # cupy sees a GPU the scalar AV path raises "Unsupported dtype float128" (see the same
    # note in test_psi_marginalization.py).
    env["CUDA_VISIBLE_DEVICES"] = ""
    env["OMP_NUM_THREADS"] = "1"
    env["MPLBACKEND"] = "Agg"
    env["PYTHONPATH"] = HERE + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, ILE,
           "--cache-file", str(event["cache"]), "--channel-name", "H1=FAKE-STRAIN",
           "--psd-file", "H1=%s" % event["psd"],
           "--event-time", str(event["t0"]),
           "--data-start-time", str(event["seg_start"]),
           "--data-end-time", str(event["seg_end"]),
           "--mass1", str(mass[0]), "--mass2", str(mass[1]),
           "--approximant", "TaylorT4", "--l-max", "2",
           "--reference-freq", "40", "--fmin-template", "40",
           "--srate", "2048", "--inv-spec-trunc-time", "0",
           "--zero-likelihood", "--n-max", str(n_max), "--n-eff", str(n_eff),
           "--seed", str(seed), "--output-file", "out"] + SAMPLER_ARGS[sampler] + list(extra)
    if a_coeff is not None:
        env["E2E_A_COEFF"] = str(a_coeff)
        cmd += ["--supplementary-likelihood-factor-code", SUPPLEMENT_MODULE,
                "--supplementary-likelihood-factor-function", "ln_analytic_phi_factor"]
    proc = subprocess.run(cmd, cwd=str(d), env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=1200)
    row = d / "out_0_.dat"
    if proc.returncode != 0 or not row.exists():
        pytest.fail("ILE (%s) exited %d; tail:\n%s"
                    % (tag, proc.returncode, proc.stdout.decode()[-2500:]))
    vals = np.atleast_2d(np.loadtxt(str(row)))[0]
    # lnL / sigma_lnL are the last four columns' first two (lnL, sigma, ntotal, neff); read
    # them from the END so an added intrinsic column cannot silently shift the meaning.
    return float(vals[-4]), float(vals[-3])


def _exact(a):
    from scipy.special import i0
    return float(np.log(i0(a)))


# ---------------------------------------------------------------------------------------
# 1. the prior-only answer

@pytest.mark.parametrize("sampler", ["AV", "portfolio"])
def test_zero_likelihood_alone_gives_ln_Z_zero(event, sampler):
    """--zero-likelihood makes the signal term exactly 0, so ln Z is the log prior mass, which
    is 0 for normalized extrinsic priors.  This is the whole extrinsic integrator, the driver
    and the export path checked against an absolute answer, not a difference."""
    lnL, sigma = _run_ile(event, "zero_%s" % sampler, sampler, a_coeff=None)
    assert sigma < MAX_SIGMA, "run not converged: sigma_lnL=%r" % sigma
    assert abs(lnL) < Z_TOLERANCE * sigma, \
        "%s: ln Z = %r +- %r with a zero likelihood; the exact answer is 0 (z=%.1f)" % (
            sampler, lnL, sigma, lnL / sigma)


# ---------------------------------------------------------------------------------------
# 2. the analytic factor, and the regression for it being dropped

@pytest.mark.parametrize("sampler", ["AV", "portfolio"])
@pytest.mark.parametrize("a_coeff", [0.75, 8.0])
def test_analytic_factor_marginal_is_recovered(event, sampler, a_coeff):
    """ln Z must equal ln I0(A).  A = 0.75 is a nearly flat target, A = 8 is sharply peaked in
    phi_orb; a sampler can be right on one and wrong on the other, so both are checked."""
    exact = _exact(a_coeff)
    lnL, sigma = _run_ile(event, "supp_%s_%s" % (sampler, a_coeff), sampler, a_coeff=a_coeff)
    assert sigma < MAX_SIGMA, "run not converged: sigma_lnL=%r" % sigma
    z = (lnL - exact) / sigma
    assert abs(z) < Z_TOLERANCE, \
        "%s at A=%r: ln Z = %r +- %r, exact ln I0(A) = %r (z=%.1f)" % (
            sampler, a_coeff, lnL, sigma, exact, z)


def test_supplementary_factor_survives_zero_likelihood(event):
    """THE REGRESSION.  --zero-likelihood swaps the likelihood function for a stand-in; the
    supplementary factor has to survive that, or the option pair is silently inert while the
    startup banner reports the factor as active.  Before the fix these two runs returned ln Z
    bit-identical (0.00652494967775219 both).

    Asserted as a DIFFERENCE as well as an absolute value: the difference cancels every prior
    normalization constant, so it isolates the factor itself."""
    a = 8.0
    with_factor, sig_w = _run_ile(event, "surv_with", "AV", a_coeff=a)
    without, sig_wo = _run_ile(event, "surv_without", "AV", a_coeff=None)
    assert with_factor != without, \
        "the supplementary factor changed nothing: ln Z = %r in both arms, so --zero-" \
        "likelihood is discarding it" % with_factor
    delta = with_factor - without
    sigma = float(np.hypot(sig_w, sig_wo))
    assert abs(delta - _exact(a)) < Z_TOLERANCE * sigma, \
        "factor contributed %r +- %r; exact ln I0(%r) = %r" % (delta, sigma, a, _exact(a))


# ---------------------------------------------------------------------------------------
# 3. the lane that is known to be wrong

def test_gmm_lane_is_known_wrong(event):
    """ILE --sampler-method GMM returns a badly wrong evidence, and says so with a small error
    bar.  Measured on rift_O4d at 94f352ad8 and unchanged since: -28.69 against an exact 0 on
    the flat target, -130.39 +- 0.178 against an exact 6.058 on the peaked one, with n_eff
    10.6/40000.  It finds the PEAK correctly (lnLmax = 8), so the maximum is fine and only the
    normalization is wrong.

    Recorded as a test rather than left out, so the defect cannot be forgotten and so fixing it
    FAILS here and forces this file to promote GMM into the parametrized lanes above.  Do not
    'fix' this by deleting it."""
    exact = _exact(8.0)
    lnL, sigma = _run_ile(event, "gmm_known_wrong", "GMM", a_coeff=8.0, n_max=20000, n_eff=250)
    if abs(lnL - exact) < Z_TOLERANCE * max(sigma, 1e-6):
        pytest.fail(
            "ILE --sampler-method GMM now agrees with the analytic answer (%r vs %r): the "
            "defect this test records appears to be FIXED.  Move GMM into the parametrized "
            "lanes above and delete this test." % (lnL, exact))
    assert lnL < exact - 5.0, \
        "GMM is wrong in an unexpected direction/size (%r vs exact %r); re-characterise it " \
        "rather than widening this assertion" % (lnL, exact)
