"""End-to-end ILE on a case whose answer is known in CLOSED FORM.

WHY THIS EXISTS.  The sampler unit tests check samplers; the pipeline gates (pseudo_pipe,
asimov) check that the pipeline RUNS.  Nothing checked that it runs and is CORRECT, and that
gap hid the defects listed below, none of which four rounds of adversarial review of the
drivers found.

THE TECHNIQUE.  --zero-likelihood makes the signal term exactly 0, so ln Z is the log of the
prior mass, which is 0 for normalized priors.  Adding an analytic --supplementary-likelihood-
factor-* whose marginal can be written down moves ln Z to a known value.  See
analytic_supplement_for_e2e for the factor and its closed form.  That is the constant-integrand
trick of the sampler unit tests lifted to the pipeline: no fit error, no MC scatter in the
TARGET, and an answer you can write down.

ITS FAST COMPANION.  test_zero_likelihood_standin.py checks the same stand-in as WIRING --
argument order against the driver's real call sites, generated signature, array module -- in
about 4 s.  It exists because an end-to-end marginal is structurally unable to distinguish a
permutation among right_ascension, phi_orb and psi: the three are independent and identically
distributed, so any factor's marginal is the same under the swap.  Read both files together.

SAMPLERS.  AV, portfolio (AV + AC) and GMM run every factor lane.  GMM was a recorded
known-wrong lane here until #359 ("GMM sampler: dim-group keys were in the wrong frame"), which
moved it from -23.1 to -0.26 sigma on the sharply peaked target; it is a first-class lane now,
and this file is what keeps it one.

WHAT IS DELIBERATELY ABSENT.  The high-SNR recipe in
~/rift-integrator-lore/coordinates-and-degeneracies.md (--force-adapt-all plus
--internal-rotate-phase and --internal-sky-network-coordinates) is the documented fix for a
collapsing n_eff, and it is NOT used here.  --internal-sky-network-coordinates needs two
detectors and this fixture is H1-only.  --internal-rotate-phase changes which coordinates the
supplementary factor is handed on the raw path, so the closed form would no longer describe the
integral being measured.  A lane that passes because the geometry was changed underneath it is
measuring something else.  If a lane will not converge, scope it or leave it out; do not add
these.

WHAT EACH ARM COSTS.  About 8-12 s of one core per ILE arm, plus ~15 s once for the
distance-marginalization lookup table.  No network, no real event, no GPU.
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
CODE = os.path.abspath(os.path.join(HERE, ".."))
BIN = os.path.join(CODE, "bin")
ILE = os.path.join(BIN, "integrate_likelihood_extrinsic_batchmode")
MARG_TABLE_TOOL = os.path.join(BIN, "util_InitMargTable")
SUPPLEMENT_MODULE = "analytic_supplement_for_e2e"

# TOLERANCES, MEASURED not guessed.  See CALIBRATION at the end of this file for the sweep
# these come from, what was varied, and how much headroom each one has.
Z_TOLERANCE = 5.0
# The informativeness floor, and the one that does the work: a z-test passes vacuously if sigma
# is large, so cap it.  5 * MAX_SIGMA is a 0.30-nat band.
MAX_SIGMA = 0.06
# A DEGENERACY TRIPWIRE, deliberately loose -- not a convergence criterion, which is MAX_SIGMA.
# n_eff is a noisy statistic at fixed accuracy: adaptive_cartesian reported 91 to 220 over eight
# seeds whose lnZ all landed inside 1.2 sigma, so a tight floor here buys a flake, not a check.
MIN_NEFF = 30.0

_AV = ["--sampler-method", "AV"]
_PORTFOLIO = ["--sampler-method", "portfolio",
              "--sampler-portfolio", "AV", "--sampler-portfolio", "AC"]
_GMM = ["--sampler-method", "GMM"]
SAMPLER_ARGS = {"AV": _AV, "portfolio": _PORTFOLIO, "GMM": _GMM}


# ---------------------------------------------------------------------------------------
# fixtures

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


# The distance-marginalized likelihood reads bmax/bref/s_array/t_array/lnI_array out of this
# file at SETUP, before any --zero-likelihood substitution, so the lane needs a real table and
# not a stub.  Its CONTENTS never reach the answer here (the likelihood body never runs), so
# the cheapest table the tool will build is the right one: a narrow distance range and low
# quadrature degrees take ~15 s instead of ~47 s for the defaults.
DMARG_TABLE_ARGS = ["--d-min", "100", "--d-max", "1000", "--max-snr", "100",
                    "--hermgauss-degree", "20", "--laggauss-degree", "20"]


@pytest.fixture(scope="module")
def dmarg_table(event):
    out = event["dir"] / "marg_lookup.npz"
    proc = subprocess.run([sys.executable, MARG_TABLE_TOOL] + DMARG_TABLE_ARGS
                          + ["--out", str(out)],
                          cwd=str(event["dir"]), env=_child_env(), stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=1200)
    if proc.returncode != 0 or not out.exists():
        pytest.skip("util_InitMargTable failed:\n%s" % proc.stdout.decode()[-1500:])
    return out


# ---------------------------------------------------------------------------------------
# running one ILE arm

def _child_env(**extra):
    # Every RIFT_* variable is stripped, not just the one that bit.  RIFT_HYPERPIPELINE_FORMAT
    # moves lnL/sigma to columns 0/1 and adds a header, under which the tail-column read below
    # returns a spin component (0.0) as sigma and the z-test passes VACUOUSLY.  Others
    # (RIFT_LOWLATENCY, RIFT_NO_GWSIGNAL, RIFT_GPU_*) change the code path.  A gate whose
    # answer depends on the invoking shell's environment is not a gate.
    env = {k: v for k, v in os.environ.items() if not k.startswith("RIFT_")}
    # CUDA_VISIBLE_DEVICES="" is required, and is not this test's problem: on a host where
    # cupy sees a GPU the scalar AV path raises "Unsupported dtype float128" (see the same
    # note in test_psi_marginalization.py).
    env["CUDA_VISIBLE_DEVICES"] = ""
    env["OMP_NUM_THREADS"] = "1"
    env["MPLBACKEND"] = "Agg"
    # THIS tree, not whatever RIFT is installed: the driver is run by absolute path out of the
    # checkout, but `import RIFT.integrators...` inside it would otherwise resolve to the
    # installed package, and half of what this gate covers lives there.  HERE is also on the
    # path so the child can import the analytic factor module.
    env["PYTHONPATH"] = os.pathsep.join(
        [HERE, CODE] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))
    env.update(extra)
    return env


def _read_result(d, tag):
    """(lnL, sigma, n_eff) from the ILE output row.

    The row is read by POSITION, which is what every downstream consumer does, and then
    CROSS-CHECKED against out_*_integrator_status.json, which carries the same four numbers
    keyed BY NAME.  That is the point of reading both: the positional contract is pinned by a
    self-describing file rather than by a comment that can go stale."""
    row = d / ("%s_0_.dat" % tag)
    status = d / ("%s_0_integrator_status.json" % tag)
    first = row.read_text().lstrip()[:1]
    assert first != "#", (
        "%s has a header line, so it is not the legacy column layout this reads by position; "
        "some RIFT_* output-format variable survived into the child environment" % row)
    vals = np.atleast_2d(np.loadtxt(str(row)))[0]
    lnL, sigma, ntotal, neff = (float(vals[-4]), float(vals[-3]),
                                float(vals[-2]), float(vals[-1]))
    st = json.loads(status.read_text())
    for name, got in (("lnL", lnL), ("sigma_lnL", sigma), ("ntotal", ntotal), ("neff", neff)):
        want = float(st[name])
        assert got == pytest.approx(want, rel=1e-12, nan_ok=True), (
            "column contract broken: the row's tail columns give %s=%r, the status JSON says "
            "%r.  The .dat layout changed and this file is reading the wrong columns."
            % (name, got, want))
    return lnL, sigma, neff


def _run_ile(event, tag, sampler_args, a_coeff=None, b_coeff=0.0, incl_is_cosine=False,
             n_max=20000, n_eff=250, seed=1000, extra=()):
    """One ILE job as a subprocess.  Returns (lnL, sigma_lnL, n_eff)."""
    d = event["dir"] / tag
    d.mkdir(exist_ok=True)
    env = _child_env()
    cmd = [sys.executable, ILE,
           "--cache-file", str(event["cache"]), "--channel-name", "H1=FAKE-STRAIN",
           "--psd-file", "H1=%s" % event["psd"],
           "--event-time", str(event["t0"]),
           "--data-start-time", str(event["seg_start"]),
           "--data-end-time", str(event["seg_end"]),
           "--mass1", "35.0", "--mass2", "30.0",
           "--approximant", "TaylorT4", "--l-max", "2",
           "--reference-freq", "40", "--fmin-template", "40",
           "--srate", "2048", "--inv-spec-trunc-time", "0",
           "--zero-likelihood", "--n-max", str(n_max), "--n-eff", str(n_eff),
           "--seed", str(seed), "--output-file", tag] + list(sampler_args) + list(extra)
    if a_coeff is not None:
        env["E2E_A_COEFF"] = str(a_coeff)
        env["E2E_B_COEFF"] = str(b_coeff)
        env["E2E_INCL_IS_COSINE"] = "1" if incl_is_cosine else "0"
        cmd += ["--supplementary-likelihood-factor-code", SUPPLEMENT_MODULE,
                "--supplementary-likelihood-factor-function", "ln_analytic_factor"]
    proc = subprocess.run(cmd, cwd=str(d), env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=1800)
    # The driver CATCHES an exception from analyze_event, prints "FAILED ANALYSIS", skips the
    # point and EXITS 0 -- so in a DAG a crashed configuration is silent.  Absence of the
    # output row, not the exit code, is what says the run failed.
    row = d / ("%s_0_.dat" % tag)
    if proc.returncode != 0 or not row.exists():
        pytest.fail("ILE (%s) exited %d and wrote no result row; tail:\n%s"
                    % (tag, proc.returncode, proc.stdout.decode()[-3000:]))
    return _read_result(d, tag)


if HERE not in sys.path:
    sys.path.insert(0, HERE)
import analytic_supplement_for_e2e as _supplement


def _exact(a, b=0.0):
    return _supplement.exact_ln_Z(a, b)


def _assert_converged(tag, lnL, sigma, neff):
    assert np.isfinite(lnL) and np.isfinite(sigma), \
        "%s: the run reported lnL=%r sigma=%r -- no usable estimate" % (tag, lnL, sigma)
    assert neff >= MIN_NEFF, "%s: n_eff=%r did not reach the floor %r" % (tag, neff, MIN_NEFF)
    assert sigma < MAX_SIGMA, \
        "%s: sigma_lnL=%r exceeds %r, so a %r-sigma band is too wide to say anything" % (
            tag, sigma, MAX_SIGMA, Z_TOLERANCE)


def _assert_lnZ(tag, lnL, sigma, neff, exact):
    _assert_converged(tag, lnL, sigma, neff)
    z = (lnL - exact) / sigma
    assert abs(z) < Z_TOLERANCE, \
        "%s: ln Z = %r +- %r, exact = %r (z = %.1f)" % (tag, lnL, sigma, exact, z)


# ---------------------------------------------------------------------------------------
# 1. the prior-only answer

@pytest.mark.parametrize("sampler", ["AV", "portfolio", "GMM"])
def test_zero_likelihood_alone_gives_ln_Z_zero(event, sampler):
    """--zero-likelihood makes the signal term exactly 0, so ln Z is the log prior mass, which
    is 0 for normalized extrinsic priors.  This is the whole extrinsic integrator, the driver
    and the export path checked against an absolute answer, not a difference."""
    tag = "zero_%s" % sampler
    lnL, sigma, neff = _run_ile(event, tag, SAMPLER_ARGS[sampler], a_coeff=None)
    _assert_lnZ(tag, lnL, sigma, neff, 0.0)


# ---------------------------------------------------------------------------------------
# 2. the analytic factor

# (A, B): A alone is a nearly flat / sharply peaked target in phi_orb.  B turns on the
# inclination term, which is what makes the factor able to detect a MIS-WIRING; see the module
# docstring of analytic_supplement_for_e2e.
FACTOR_CASES = [(0.75, 0.0), (8.0, 0.0), (0.75, 3.0)]
# (8.0, 2.0), peaked AND asymmetric, is the case every lane in section 3 runs, so it is not
# repeated here.


@pytest.mark.parametrize("sampler", ["AV", "portfolio", "GMM"])
@pytest.mark.parametrize("a_coeff,b_coeff", FACTOR_CASES)
def test_analytic_factor_marginal_is_recovered(event, sampler, a_coeff, b_coeff):
    """ln Z must equal ln I0(A) + ln(sinh(B)/B).  A sampler can be right on a flat target and
    wrong on a peaked one, so both are checked, with and without the inclination term."""
    tag = "supp_%s_%s_%s" % (sampler, a_coeff, b_coeff)
    lnL, sigma, neff = _run_ile(event, tag, SAMPLER_ARGS[sampler],
                                a_coeff=a_coeff, b_coeff=b_coeff)
    _assert_lnZ(tag, lnL, sigma, neff, _exact(a_coeff, b_coeff))


def test_supplementary_factor_survives_zero_likelihood(event):
    """THE REGRESSION.  --zero-likelihood swaps the likelihood function for a stand-in; the
    supplementary factor has to survive that, or the option pair is silently inert while the
    startup banner reports the factor as active.  Measured on rift_O4d at d1d7c7e84, these two
    runs returned ln Z bit-identical: -0.027110200401507356 with the factor and without it,
    where the exact answer with it is 6.653324.

    Asserted as a DIFFERENCE as well as an absolute value: the difference cancels every prior
    normalization constant, so it isolates the factor itself."""
    a, b = 8.0, 2.0
    with_factor, sig_w, neff_w = _run_ile(event, "surv_with", _AV, a_coeff=a, b_coeff=b)
    without, sig_wo, neff_wo = _run_ile(event, "surv_without", _AV, a_coeff=None)
    _assert_converged("surv_with", with_factor, sig_w, neff_w)
    _assert_converged("surv_without", without, sig_wo, neff_wo)
    assert with_factor != without, \
        "the supplementary factor changed nothing: ln Z = %r in both arms, so --zero-" \
        "likelihood is discarding it" % with_factor
    delta = with_factor - without
    sigma = float(np.hypot(sig_w, sig_wo))
    exact = _exact(a, b)
    assert abs(delta - exact) < Z_TOLERANCE * sigma, \
        "factor contributed %r +- %r; exact is %r" % (delta, sigma, exact)


def test_the_factor_receives_the_raw_sampled_inclination(event):
    """THE COORDINATE CONTRACT.  ILE hands a supplementary factor the RAW sampled inputs, so
    under --inclination-cosine-sampler the 'inclination' argument IS cos(iota), not iota.  The
    factor is told which convention to expect (E2E_INCL_IS_COSINE) and its closed form is the
    same either way, so this arm answers a question the other arms cannot: a stand-in that
    helpfully applied the arccos itself would move ln Z well off sinh(B)/B -- by 3.4 nats at
    B = 2, against a sigma of 0.034."""
    a, b = 8.0, 2.0
    tag = "raw_incl_cosine"
    lnL, sigma, neff = _run_ile(event, tag, _AV, a_coeff=a, b_coeff=b, incl_is_cosine=True,
                                extra=("--inclination-cosine-sampler",))
    _assert_lnZ(tag, lnL, sigma, neff, _exact(a, b))


# ---------------------------------------------------------------------------------------
# 3. the configurations that CHANGE likelihood_function's signature

def test_time_marginalized_portfolio(event):
    """--time-marginalization drops t_ref from likelihood_function's signature.  With an AV
    member in the portfolio the integrand is called through
    mcsamplerPortfolio.integrate_log -> AV.update_sampling_prior_selfish -> lnF(*samples.T),
    i.e. POSITIONALLY: a stand-in that unpacked a fixed 7-tuple died here with 'not enough
    values to unpack (expected 7, got 6)', and the driver swallowed it, printed FAILED ANALYSIS
    and exited 0.  This is a production setting."""
    a, b = 8.0, 2.0
    tag = "tmarg_portfolio"
    lnL, sigma, neff = _run_ile(event, tag, _PORTFOLIO, a_coeff=a, b_coeff=b,
                                extra=("--time-marginalization",))
    _assert_lnZ(tag, lnL, sigma, neff, _exact(a, b))


def test_distance_marginalized(event, dmarg_table):
    """--distance-marginalization removes 'distance' from the sampled parameters and from
    likelihood_function's signature, which becomes (right_ascension, declination, phi_orb,
    inclination, psi).  The factor still takes six arguments, so the stand-in has to supply a
    value for that one; it uses 0.0, which is what the two distance-marginalized real call
    sites already pass.  The factor here does not use distance, which is the advice --help
    gives for a portable factor.  Reading it out of the sampled values instead raised KeyError
    on this configuration."""
    a, b = 8.0, 2.0
    tag = "dmarg"
    lnL, sigma, neff = _run_ile(
        event, tag, _AV, a_coeff=a, b_coeff=b,
        extra=("--distance-marginalization",
               "--distance-marginalization-lookup-table", str(dmarg_table),
               "--d-min", "100", "--d-max", "1000",
               "--time-marginalization", "--vectorized", "--gpu", "--force-xpy"))
    _assert_lnZ(tag, lnL, sigma, neff, _exact(a, b))


def test_adaptive_cartesian(event):
    """mcsampler decides which of its parameters to pass by reading
    func.__code__.co_varnames[:co_argcount].  A `def zero_like(*args, **kwargs)` stand-in
    reports ZERO arguments there, so --zero-likelihood --sampler-method adaptive_cartesian died
    in mcsampler with 'cannot reshape array of size 70000 into shape (0,newaxis)' -- on the
    driver's own default sampler, and again behind FAILED ANALYSIS and exit 0."""
    a, b = 8.0, 2.0
    tag = "adaptive_cartesian"
    # A LARGER --n-max than the other lanes, because at 20000 this sampler stops on the budget
    # rather than on --n-eff: over eight seeds it reported n_eff 91 to 220 against a target of
    # 250, with sigma up to 0.0430 against a 0.06 cap.  At 60000 it stops on n_eff instead, at
    # ntotal 30000, with n_eff ~300 and sigma ~0.03.  One-and-a-half times the arm, and the
    # lane stops sitting one bad draw away from its own informativeness floor.
    lnL, sigma, neff = _run_ile(event, tag, ["--sampler-method", "adaptive_cartesian"],
                                a_coeff=a, b_coeff=b, n_max=60000)
    _assert_lnZ(tag, lnL, sigma, neff, _exact(a, b))


# ---------------------------------------------------------------------------------------
# CALIBRATION
#
# Where Z_TOLERANCE, MAX_SIGMA and MIN_NEFF come from.  Eight seeds (1000-1007) per lane on
# ldas-grid, IGWN CVMFS python 3.11, numpy 1.26.4, cupy absent, CUDA_VISIBLE_DEVICES="".
# Non-GMM lanes were measured at d1d7c7e84 and re-run after the rebase onto 0a5fdb3be: the
# prior-only AV lane came back BIT-IDENTICAL on all eight seeds, so those numbers carry over.
# GMM lanes were measured at 0a5fdb3be, i.e. after #359.
#
#   lane                                      max |z|   max sigma   min n_eff
#   prior-only,    AV                            2.03      0.0134        1349
#   prior-only,    portfolio                     1.68      0.0104        2316
#   prior-only,    GMM                           1.88      0.0134        1335
#   A=0.75 B=0,    AV                            1.37      0.0159         745
#   A=0.75 B=0,    portfolio                     1.14      0.0127        1287
#   A=0.75 B=0,    GMM                           1.66      0.0160         750
#   A=8    B=0,    AV                            1.73      0.0272         416
#   A=8    B=0,    portfolio                     1.31      0.0305         346
#   A=8    B=0,    GMM                           1.52      0.0349         267
#   A=0.75 B=3,    AV                            1.25      0.0234         363
#   A=0.75 B=3,    portfolio                     1.54      0.0227         392
#   A=0.75 B=3,    GMM                           1.34      0.0235         381
#   A=8    B=2,    AV (the survives-swap lanes)  1.06      0.0332         247
#   raw inclination contract (cosine sampler)    1.47      0.0353         163
#   time-marginalized portfolio                  1.65      0.0298         237
#   distance-marginalized                        2.15      0.0326         336
#   adaptive_cartesian, --n-max 60000            1.35      0.0305         250
#
# Z_TOLERANCE = 5     is 2.3x the worst |z| seen (2.15, distance-marginalized).
# MAX_SIGMA   = 0.06  is 1.7x the worst sigma seen (0.0353).  5 * MAX_SIGMA is a 0.30-nat band.
# MIN_NEFF    = 30    is 5.4x below the worst n_eff seen (163).  See its comment for why it is
#                     this loose.
#
# WHAT THE GATE HAS TO SEPARATE A CORRECT RUN FROM.  Each row was run on this fixture, not
# argued.  The smallest is 6.3 sigma, against a tolerance of 5 and a worst observed draw of 2.15:
#
#   defect                                                        lane              z
#   portfolio member p_s, rift_O4d @ 9e55f12b7 (without #356)     prior-only    -43.6
#   portfolio member p_s, same tree                               A=0.75 B=3    -25.9
#   ILE --sampler-method GMM before #359                          A=8 B=2      -684
#   phi_orb and inclination swapped in _SUPPLEMENT_ARG_ORDER      A=8 B=2       -19.5
#   inclination and psi swapped                                   A=8 B=2        +6.3
#   inclination and psi swapped, B term off                       A=8 B=0        -0.1
#   phi_orb and psi swapped                                       all            ~0
#
# The last two rows are why the B term and test_zero_likelihood_standin.py both exist.  With
# the B term off, mis-routing inclination is invisible.  And phi_orb, psi and right_ascension
# are independent and identically distributed, so NO closed-form lane can see a permutation of
# those three.  Run as a mutation, EVERY z-test in this file passes with phi_orb and psi
# swapped in the stand-in.  The module does go red, but on the adaptive_cartesian lane's sigma
# budget, because the mutated integrand happens to be harder for that one sampler -- an
# efficiency artifact, not a detection, and not something to rely on: a swap between phi_orb
# and right_ascension need not perturb any sampler at all.  The case is caught by reading the
# wiring, which is the companion file's job; it fails there immediately and for the right
# reason.
#
# ONE THING THE GMM LANES DO NOT COVER, measured while calibrating them.  GMM's EVIDENCE is
# correct on all three cases above, but at A=8 with the inclination term ON (B=2, which no lane
# here runs) its n_eff is a bimodal lottery: over eight seeds, six landed at n_eff 266-324 with
# sigma ~0.025 and two collapsed to 73.5 and 14.5 with sigma 0.028 and 0.073, while ln Z stayed
# right on every one (max |z| 1.49).  A=8 with B=0 does NOT collapse, so the inclination term is
# what drives it, not the phi_orb peak.  This is the shape recorded in
# ~/rift-integrator-lore/coordinates-and-degeneracies.md, section "What the n_eff lottery
# actually needs", whose diagnosis is extrinsic mode collapse rather than sample starvation --
# raising --n-max re-rolls the dice instead of fixing it.  Narrower than the production case it
# resembles: this fixture is H1-only, so neither the sky ring nor the two-detector
# phase-polarization degeneracy exists here.  Contrast adaptive_cartesian above, which DID get
# a bigger --n-max: there the answer was right on every seed and only the error bar was short,
# which is sample starvation.  Do not carry one precedent to the other case.
