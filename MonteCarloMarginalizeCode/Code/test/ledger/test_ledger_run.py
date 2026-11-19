"""Ledger runs (RIFT/ledger), driven through bin/rift-run with a known-answer engine.

fake_engine.py evaluates ln L = ln N(A; MU, COV) - ln p(A): the posterior of block A is
that Gaussian and the evidence is 1.  No real likelihood is involved.
"""
import json
import os
import signal
import subprocess
import sys
import time

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.dirname(os.path.dirname(HERE))
RIFT_RUN = os.path.join(CODE, "bin", "rift-run")
sys.path.insert(0, HERE)
sys.path.insert(0, CODE)

import fake_engine  # noqa: E402
from RIFT.ledger import controller, estimator, proposal, rundir  # noqa: E402
from RIFT.ledger.space import Box  # noqa: E402

SIZES = ["--set", "seed=7", "--set", "learn_per_stream=250", "--set", "frozen_per_stream=250", "--set", "target_n_eff=500"]
FINAL_CIP = ("1 --lnL-protect-overflow --no-plots --fit-method rf --parameter mc --parameter delta_mc "
             "--mc-range [21.418182160215295,41.97447913941358] --eta-range [0.045351473922902494,0.25] --chi-max 0.99 "
             "--chi-small-max 0.99 --parameter-implied xi --parameter-implied chiMinus --use-precessing "
             "--parameter-nofit chi1 --parameter-nofit chi2 --parameter-nofit cos_theta1 --parameter-nofit cos_theta2 "
             "--parameter-nofit phi1 --parameter-nofit phi2 --parameter-implied s1x --parameter-implied s1y "
             "--parameter-implied s2x --parameter-implied s2y --n-output-samples 2500 --n-eff 2500 --n-max 100000000 "
             "--fail-unless-n-eff 27 --downselect-parameter m2 --downselect-parameter-range [1,1000] "
             "--sampler-method AV --eccentricity-prior uniform --posterior-unique-draw")
ILE = ("--manual-logarithm-offset 224.5 --time-marginalization --vectorized --gpu --force-xpy --sampler-method AV "
       "--n-chunk 10000 --distance-marginalization")


def _command(run_dir, extra=()):
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([CODE, os.environ.get("PYTHONPATH", "")]))
    engine = "%s -B %s {lease} {keys} {shard}" % (sys.executable, os.path.join(HERE, "fake_engine.py"))
    box = fake_engine.BOX
    cmd = [sys.executable, "-B", RIFT_RUN, "--run-dir", run_dir, "--engine-command", engine,
           "--mc-range", json.dumps(box["mc_range"]), "--eta-range", json.dumps(box["eta_range"]),
           "--chi-max", str(box["chi_max"])] + SIZES + list(extra)
    return cmd, env


@pytest.fixture(scope="module")
def clean_run(tmp_path_factory):
    run_dir = str(tmp_path_factory.mktemp("ledger") / "clean")
    cmd, env = _command(run_dir)
    assert subprocess.run(cmd, env=env).returncode == 0
    return run_dir


def test_certified_estimate_matches_the_known_posterior(clean_run):
    """Protects: a certified run reports ln Z and a posterior mean within their stated errors.
    Oracle: analytic.  The fake likelihood has ln Z = 0 and a block-A posterior N(MU, COV)."""
    cert = json.load(open(os.path.join(clean_run, "certificate.json")))
    est = cert["estimate"]
    assert cert["certified"] and cert["open_indices"] == 0
    assert abs(est["lnZ"]) < 4 * est["lnZ_err"]
    mc = est["params"]["mc"]
    assert abs(mc["mean"] - fake_engine.MU[0]) < 4 * mc["se_mean"]
    assert abs(mc["sd"] / fake_engine.SD[0] - 1) < 4 / np.sqrt(2 * est["n_eff"])
    draws = np.loadtxt(os.path.join(clean_run, "posterior_samples.dat"), ndmin=2)
    m1, m2 = draws[:, 2], draws[:, 3]
    mc_draws = (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2
    assert abs(mc_draws.mean() - fake_engine.MU[0]) < 4 * np.hypot(mc["se_mean"], fake_engine.SD[0] / np.sqrt(len(draws)))


def test_kills_and_restarts_leave_the_certificate_unchanged(clean_run, tmp_path):
    """Protects: engine kills inside a lease, a killed supervisor and a different number of
    engines change no proposal and no estimate.  Oracle: metamorphic, against the uninterrupted
    one-engine run with the same seed."""
    run_dir = str(tmp_path / "killed")
    cmd, env = _command(run_dir, ["--chaos", "engine=2,seed=3,lo=0.8,hi=1.6", "--engines", "2"])
    env["LEDGER_FAKE_SLEEP"] = "0.0015"
    first = subprocess.Popen(cmd, env=env)
    time.sleep(4.0)
    first.send_signal(signal.SIGKILL)            # the supervisor dies; its engine is orphaned
    first.wait()
    for _ in range(20):
        if subprocess.run(cmd, env=env).returncode == 0:
            break
    else:
        pytest.fail("the killed run did not finish")
    kills = [json.loads(x) for x in open(os.path.join(run_dir, "ctl", "chaos.jsonl"))]
    assert sum(k["rows_kept"] > 0 and k["in_flight"] is not None for k in kills) >= 1, "no kill landed inside a lease"
    a, b = (json.load(open(os.path.join(d, "certificate.json"))) for d in (clean_run, run_dir))
    assert a["proposals"] == b["proposals"]
    assert a["estimate"] == b["estimate"]


def test_the_recorded_plan_is_the_record(clean_run, monkeypatch):
    """Protects: an estimate is computed from the points that were evaluated, when a rebuilt
    plan would differ (another LAPACK build, another numpy).  Oracle: metamorphic.  The draw
    routine is perturbed after the run; the recomputed certificate must not move."""
    run = controller.Run(clean_run)
    arts = run.artifacts()
    recorded = run.plan(len(arts))
    monkeypatch.setattr(proposal, "_affine", lambda mean, chol, z: [float(m) * (1 + 1e-6) for m in mean])
    prop = proposal.proposal_from_json(arts[-1]["proposal"], run.box)
    redrawn = proposal.draw_plan(prop, run.box, run.cfg["n_streams"], 0, 8)
    assert not np.allclose(redrawn["a"], recorded["a"][recorded["index"] < 8], rtol=0, atol=1e-9)   # the mutant bites
    check = controller.verify(run)["certificate"]
    assert check["lnZ_difference"] == 0.0 and check["n_eff_difference"] == 0.0
    assert all(check["plans_match"].values())


def _correlated_proposal():
    sd = np.array([1.5, 0.08, 0.10, 0.12])
    corr = np.array([[1.0, 0.3, 0.8, -0.2], [0.3, 1.0, 0.1, -0.7], [0.8, 0.1, 1.0, 0.0], [-0.2, -0.7, 0.0, 1.0]])
    cov = corr * np.outer(sd, sd)
    mu = np.array([30.0, 0.25, 0.30, -0.30])
    near_equal_mass = mu + [1.0, -0.22, 0.05, 0.1]          # delta 0.03: half of this component folds
    return mu, sd, proposal.BlockProposal.from_covariances(
        [0.6, 0.4], [mu, near_equal_mass], [cov, 0.5 * cov], mu, 2.25 * cov, a=0.2, seed=5)


@pytest.mark.parametrize("kind", ["prior", "block shape", "block tail", "block fold"])
def test_a_proposal_density_is_the_density_of_its_draws(kind):
    """Protects: importance weights p L / q are valid, since q is the density the points were
    drawn from.  Oracle: calibration.  E_q[g / q] = 1 for a normalised test density g inside
    the support; the four g probe the prior's normalisation, the correlations of a component,
    the Student-t tail and the fold in delta."""
    box = Box.from_json(fake_engine.BOX)
    mu, sd, block = _correlated_proposal()
    cov = block.chols[0] @ block.chols[0].T
    if kind == "prior":
        prop, n = proposal.PriorProposal(box, 5), 120000
        lo, hi = np.array([22.0, 0.02, 0.02, -0.60]), np.array([41.0, 0.60, 0.60, -0.02])

        def ln_g(a):
            return np.where(np.all((a > lo) & (a < hi), axis=1), -np.log(np.prod(hi - lo)), -np.inf)
    else:
        prop, n = block, 40000
        centre, scale = {"block shape": (mu + 0.3 * sd * [1, -1, -1, 1], 0.5),
                         "block tail": (mu, 9.0),         # wider than the Student-t's own shape
                         "block fold": (block.means[1], 0.3)}[kind]
        if kind == "block tail":
            prop.a = 0.5
        chol = np.linalg.cholesky(scale * cov)

        def ln_g(a):       # a Gaussian folded in delta, as the proposal is
            return np.logaddexp(proposal.ln_normal(a, centre, chol), proposal.ln_normal(a * [1, -1, 1, 1], centre, chol))
    a = np.array([prop.draw(0, i)[0] for i in range(n)])
    val = np.exp(ln_g(a) - prop.ln_q(a))
    err = val.std(ddof=1) / np.sqrt(len(val))
    assert err < 0.02, "this test density is too rarely hit for the check to mean anything"
    assert abs(val.mean() - 1.0) < 4 * err


def test_one_extension_is_limited():
    """Protects: one heavy draw cannot make the frozen round ask for its whole draw limit in
    one step (gate G7, run A).  Oracle: analytic."""
    cfg = dict(controller.DEFAULTS)
    assert estimator.extend(cfg, 400, 1e9) == int(np.ceil(cfg["extension_factor"] * 400))
    assert estimator.extend(cfg, 400, 0.0) == 400 + cfg["min_extension"]
    assert estimator.extend(cfg, cfg["max_frozen_per_stream"] - 1, 1e9) == cfg["max_frozen_per_stream"]


@pytest.mark.parametrize("change, named", [
    (("--posterior-unique-draw", "--aligned-prior alignedspin-zprior"), "--aligned-prior"),
    (("--parameter-nofit chi1", "--parameter-nofit s1x"), "polar spin"),
    (("[1,1000]", "[10,1000]"), "--downselect-parameter"),
    (("--chi-small-max 0.99", "--chi-small-max 0.5"), "--chi-small-max"),
])
def test_a_prior_that_is_not_implemented_is_refused(change, named):
    """Protects: the stage never samples a prior other than the one the run asked for.
    Oracle: analytic.  The unchanged line gives the campaign's box; each change is refused by name."""
    box = rundir.box_from_cip_tokens(FINAL_CIP.split()[1:])
    assert box.mc_range == (21.418182160215295, 41.97447913941358) and box.chi_max == 0.99
    assert abs(box.delta_max - np.sqrt(1 - 4 * 0.045351473922902494)) < 1e-15
    with pytest.raises(SystemExit) as err:
        rundir.box_from_cip_tokens(FINAL_CIP.replace(*change).split()[1:])
    assert named in str(err.value)


@pytest.mark.parametrize("change, named", [
    (("--force-xpy ", ""), "--force-xpy"),
    (("--sampler-method AV", "--sampler-method GMM"), "--sampler-method AV"),
    (("--distance-marginalization", "--auto-logarithm-offset"), "--auto-logarithm-offset"),
])
def test_ile_arguments_off_the_supported_path_are_refused(change, named):
    """Protects: every engine command reaches the NoLoop likelihood under AV with a fixed
    ln L offset.  Oracle: analytic (the REDLINE of RIFT AGENTS.md)."""
    assert rundir.check_ile_tokens(ILE.split()) == 224.5
    with pytest.raises(SystemExit) as err:
        rundir.check_ile_tokens(ILE.replace(*change).split())
    assert named in str(err.value)
