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
import stub_ile  # noqa: E402
from RIFT.ledger import controller, engine, estimator, proposal, rundir  # noqa: E402
from RIFT.ledger.space import Box, summaries, to_grid  # noqa: E402

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
ILE_EXE = os.path.join(CODE, "bin", "integrate_likelihood_extrinsic_batchmode")


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
    cmd, env = _command(run_dir, ["--chaos", "engine=2,rows=150", "--engines", "2"])
    env["LEDGER_FAKE_SLEEP"] = "0.001"
    first = subprocess.Popen(cmd, env=env)
    time.sleep(3.0)
    first.send_signal(signal.SIGKILL)            # the supervisor dies; its engine is orphaned
    first.wait()
    for _ in range(20):
        if subprocess.run(cmd, env=env).returncode == 0:
            break
    else:
        pytest.fail("the killed run did not finish")
    kills = [json.loads(x) for x in open(os.path.join(run_dir, "ctl", "chaos.jsonl"))]
    assert len(kills) == 2 and all(k["rows_kept"] >= 150 and k["in_flight"] is not None for k in kills)
    a, b = (json.load(open(os.path.join(d, "certificate.json"))) for d in (clean_run, run_dir))
    assert a["proposals"] == b["proposals"]
    assert a["estimate"] == b["estimate"]


def test_the_recorded_plan_is_the_record(clean_run, monkeypatch):
    """Protects: an estimate is computed from the points that were evaluated, when a rebuilt
    plan would differ (another LAPACK build, another numpy).  Oracle: metamorphic.  The draw
    routine is perturbed after the run; the recomputed certificate must not move."""
    run = controller.Run(clean_run)
    arts = run.artifacts()
    assert len(set(a["proposal"]["seed"] for a in arts)) == len(arts)      # each version draws from its own seed
    recorded = run.plan(len(arts))
    monkeypatch.setattr(proposal, "_affine", lambda mean, chol, z: [float(m) * (1 + 1e-6) for m in mean])
    prop = proposal.proposal_from_json(arts[-1]["proposal"], run.box)
    redrawn = proposal.draw_plan(prop, run.box, run.cfg["n_streams"], 0, 8)
    assert not np.allclose(redrawn["a"], recorded["a"][recorded["index"] < 8], rtol=0, atol=1e-9)   # the mutant bites
    check = controller.verify(run)["certificate"]
    assert check["same_result"] and check["lnZ_difference"] == 0.0 and check["n_eff_difference"] == 0.0
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
    (("[1,1000]", "[1,60]"), "--downselect-parameter"),
    (("--downselect-parameter m2", "--downselect-parameter q"), "--downselect-parameter"),
    (("--use-precessing ", ""), "--use-precessing"),
    (("--posterior-unique-draw", "--assume-volumetric-spin"), "--assume-volumetric-spin"),
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
    (("--gpu ", ""), "--gpu"),
    (("--vectorized ", ""), "--vectorized"),
    (("--time-marginalization ", ""), "--time-marginalization"),
    (("--sampler-method AV", "--sampler-method GMM"), "--sampler-method AV"),
    (("--distance-marginalization", "--auto-logarithm-offset"), "--auto-logarithm-offset"),
    (("--distance-marginalization", "--auto-logarithm-off"), "--auto-logarithm-offset"),       # optparse abbreviation
    (("--distance-marginalization", "--calibration-burn-in-neff 20"), "--calibration-burn-in-neff"),
    (("--distance-marginalization", "--mc-error-replicas 3"), "--mc-error-replicas"),
    (("--distance-marginalization", "--sampler-sequential-warmstart"), "--sampler-sequential-warmstart"),
    (("--distance-marginalization", "--no-adapt-after-first"), "--no-adapt-after-first"),
    (("--distance-marginalization", "--internal-soft-fail-on-cuda-error"), "--internal-soft-fail-on-cuda-error"),
])
def test_ile_arguments_off_the_supported_path_are_refused(change, named):
    """Protects: every engine command reaches the NoLoop likelihood under AV with a fixed
    ln L offset, one sampler call per point and no state carried between points.
    Oracle: analytic (the REDLINE of RIFT AGENTS.md; ILE's own option list)."""
    assert rundir.check_ile_tokens(ILE.split(), ILE_EXE) == 224.5
    assert rundir.check_ile_tokens((ILE + " --no-adapt").split(), ILE_EXE) == 224.5      # an option, not a prefix
    with pytest.raises(SystemExit) as err:
        rundir.check_ile_tokens(ILE.replace(*change).split(), ILE_EXE)
    assert named in str(err.value)


def test_an_engine_that_cannot_start_stops_the_run(tmp_path):
    """Protects: a broken engine is reported as such, and does not turn the points of a version
    into failed rows with weight zero one after another.  Oracle: analytic (exit status 6 and
    at most one key marked failed)."""
    run_dir = str(tmp_path / "broken")
    cmd, env = _command(run_dir)
    cmd[cmd.index("--engine-command") + 1] = "exit 7"
    assert subprocess.run(cmd, env=env).returncode == 6
    assert controller.Run(run_dir).ledger()[0] == {}
    for _ in range(2):                       # nor do repeated starts
        assert subprocess.run(cmd, env=env).returncode == 6
    assert controller.Run(run_dir).ledger()[0] == {}


def test_draws_differ_between_streams_and_between_versions():
    """Protects: the four streams are independent draws, and a frozen plan is not the plan its
    proposal was fitted to; identical streams would inflate n_eff fourfold and pass the stream
    check trivially.  Oracle: metamorphic."""
    box = Box.from_json(fake_engine.BOX)
    _, _, block = _correlated_proposal()
    for prop in (proposal.PriorProposal(box, 5), block):
        pl = proposal.draw_plan(prop, box, 4, 0, 50)
        a = pl["a"].reshape(4, 50, 4)
        for s1 in range(4):
            for s2 in range(s1):
                assert np.abs(a[s1] - a[s2]).min() > 0
        assert np.abs(a[0][1:] - a[0][:-1]).min() > 0
        assert np.abs(prop.draw(1, 2)[0] - prop.draw(2, 1)[0]).min() > 0
        assert np.array_equal(proposal.draw_plan(prop, box, 4, 20, 30)["a"].reshape(4, 10, 4), a[:, 20:30])
    other = proposal.BlockProposal.from_json(dict(block.to_json(), seed=block.seed + 1000))
    assert np.abs(proposal.draw_plan(other, box, 4, 0, 50)["a"] - proposal.draw_plan(block, box, 4, 0, 50)["a"]).min() > 0


def _synthetic_round(n_per=500, outside=0.0, failed=0.0, tail=None, shift_stream=None, seed=3):
    rng = np.random.default_rng(seed)
    n = 4 * n_per
    ok = rng.random(n) >= outside
    lnl = np.zeros(n)
    if tail is not None:
        lnl = np.log((1 - tail) * (1 - rng.random(n)) ** (-tail))     # mean-one weights, tail index `tail`
    lnl[rng.random(n) < failed] = -np.inf
    stream = np.repeat(np.arange(4), n_per)
    if shift_stream is not None:
        lnl[stream == shift_stream] += 1.0
    grid = np.column_stack([rng.uniform(25, 35, n), rng.uniform(15, 25, n), rng.uniform(-0.5, 0.5, (n, 6))])
    return dict(stream=stream, grid=grid, ok=ok, ln_pq=np.zeros(n)), lnl


def test_zero_weight_draws_count_and_too_many_failures_block_the_certificate():
    """Protects: a draw outside the prior support or with a failed evaluation counts with weight
    zero, and a round with more failed rows than `max_failed_fraction` is not certified.
    Oracle: analytic.  With unit weights, ln Z = ln(1 - f) for a fraction f of zero-weight draws."""
    cfg = dict(controller.DEFAULTS, target_n_eff=100.0)
    pl, lnl = _synthetic_round(outside=0.25)
    res = estimator.frozen_estimate(cfg, pl, lnl)
    assert res["estimate"]["lnZ"] == pytest.approx(np.log(pl["ok"].mean()), abs=1e-12)
    assert res["outside_support"] == int((~pl["ok"]).sum())
    assert not res["checks"]["tail"] and not res["stop"]       # equal weights have no tail to fit: the check cannot pass
    pl, lnl = _synthetic_round(failed=0.1)
    res = estimator.frozen_estimate(cfg, pl, lnl)
    assert res["estimate"]["lnZ"] == pytest.approx(np.log(np.isfinite(lnl).mean()), abs=1e-12)
    assert res["failed_rows"] == int((~np.isfinite(lnl)).sum())
    assert not res["checks"]["failed"] and not res["stop"]
    pl, lnl = _synthetic_round(failed=0.001, n_per=2000)
    assert estimator.frozen_estimate(cfg, pl, lnl)["checks"]["failed"]
    shifted = estimator.frozen_estimate(dict(cfg, lnl_offset=5.0), pl, lnl)
    assert shifted["estimate"]["lnZ"] == pytest.approx(5.0 + np.log(np.isfinite(lnl).mean()), abs=1e-12)
    assert all(g["lnZ"] > 4.9 for g in shifted["groups"])
    lnl[pl["stream"] == 1] = -np.inf                  # one stream with no weighted row: no estimate, no crash
    res = estimator.frozen_estimate(cfg, pl, lnl)
    assert not res["checks"]["groups"] and not res["stop"] and np.isnan(res["group_chi2"]["lnZ"])


@pytest.mark.parametrize("k", [0.15, 0.45, 0.85])
def test_tail_index_and_checks_against_a_known_tail(k):
    """Protects: the tail index is the weights' tail index, the tail check refuses a heavy tail,
    and the stream check refuses streams that disagree.  Oracle: analytic (generalized-Pareto
    weights of index k; a stream whose weights are all e times larger)."""
    cfg = dict(controller.DEFAULTS, target_n_eff=10.0, lnz_tol=10.0)
    pl, lnl = _synthetic_round(n_per=5000, tail=k)
    res = estimator.frozen_estimate(cfg, pl, lnl)
    assert abs(res["estimate"]["k_hat"] - k) < 0.2          # 424 tail weights give a standard error near 0.07
    assert res["checks"]["tail"] == (k < cfg["k_max"])
    if k < 0.5:
        # exact quantiles of the same tail: the evidence beyond the largest weight is (0.5 / n)^(1 - k)
        n = 8000
        w = (1 - k) * (1 - (np.arange(n) + 0.5) / n) ** (-k)
        k_fit, unseen = estimator.tail_fit(w)
        assert abs(k_fit - k) < 0.1
        assert 0.5 < unseen / (0.5 / n) ** (1 - k) < 2.0
        assert estimator.estimate(np.log(w), {})["lnZ_tail"] == pytest.approx(np.log1p(unseen * n / w.sum()), rel=1e-12)
    if k < 0.5:
        assert res["checks"]["groups"]
        pl, lnl = _synthetic_round(n_per=5000, tail=k, shift_stream=2)
        assert not estimator.frozen_estimate(cfg, pl, lnl)["checks"]["groups"]


def test_the_prior_is_the_stated_prior():
    """Protects: block A and block B together are uniform in (m1, m2) inside the box with spins
    uniform in magnitude and isotropic, including when the eta range ends below 0.25.
    Oracle: an independent rejection sample of that prior (the fake likelihood divides by the
    code's own prior density, so the end-to-end tests cannot see an error in it)."""
    from scipy import stats
    box = Box([25.0, 35.0], [0.12, 0.24], 0.8)
    prop = proposal.PriorProposal(box, 3)
    a, u = zip(*(prop.draw(0, i) for i in range(40000)))
    mine = summaries(to_grid(np.array(a), box.draw_B(np.array(a), np.array(u))))
    rng = np.random.default_rng(8)
    m1, m2 = rng.uniform(10, 120, 4000000), rng.uniform(10, 120, 4000000)
    mc, eta = (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2, m1 * m2 / (m1 + m2) ** 2
    keep = (m1 >= m2) & (mc > 25) & (mc < 35) & (eta > 0.12) & (eta < 0.24)
    m1, m2 = m1[keep], m2[keep]
    n = len(m1)

    def spins():
        chi, cth, phi = rng.uniform(0, 0.8, n), rng.uniform(-1, 1, n), rng.uniform(0, 2 * np.pi, n)
        return chi * np.sqrt(1 - cth ** 2) * np.cos(phi), chi * np.sqrt(1 - cth ** 2) * np.sin(phi), chi * cth

    s1, s2 = spins(), spins()
    ref = summaries(np.column_stack([m1, m2, s1[0], s1[1], s1[2], s2[0], s2[1], s2[2]]))
    assert n > 20000
    for name in ("mc", "q", "chi_eff", "chi_p"):
        assert stats.ks_2samp(mine[name], ref[name]).pvalue > 1e-3, name


def test_the_first_row_of_a_key_wins(tmp_path):
    """Protects: one canonical row per key, chosen by shard name and not by value, and a count
    of repeats that disagree.  Oracle: analytic."""
    run_dir = str(tmp_path / "r")
    controller.init(run_dir, Box.from_json(fake_engine.BOX))
    os.makedirs(os.path.join(run_dir, "ledger"))
    for name, val in (("shard-v01-n0001.jsonl", 2.0), ("shard-v01-n0000.jsonl", 1.0), ("shard-v01-n0002.jsonl", 3.0)):
        with open(os.path.join(run_dir, "ledger", name), "w") as f:
            f.write(json.dumps(dict(v=1, s=0, i=0, status="OK", lnL=val)) + "\n")
    rows, stat = controller.Run(run_dir).ledger()
    assert rows[(1, 0, 0)]["lnL"] == 1.0 and stat["repeats"] == 2 and stat["repeat_mismatch"] == 2


def test_a_row_must_carry_its_keys_point(clean_run):
    """Protects: a row evaluated at another point than its key's cannot enter an estimate, and a
    row whose ln L is not finite counts as failed.  Oracle: metamorphic.  A change of any one
    column, both masses scaled together, and no point at all are each refused."""
    run = controller.Run(clean_run)
    rows, _ = run.ledger()
    pl = run.plan(1)
    j = int(np.flatnonzero(pl["ok"])[0])
    key = (1, int(pl["stream"][j]), int(pl["index"][j]))
    good = dict(rows[key])
    assert np.isfinite(run.lnl(rows, 1, pl)[j])
    changes = [lambda p, c=c: p[:c] + [p[c] * (1 + 1e-6) + 1e-6] + p[c + 1:] for c in range(8)]
    changes += [lambda p: [3 * p[0], 3 * p[1]] + p[2:], lambda p: None]
    for change in changes:
        row = dict(good, point=change(list(good["point"])))
        if row["point"] is None:
            del row["point"]
        with pytest.raises(SystemExit) as err:
            run.lnl(dict(rows, **{}) | {key: row}, 1, pl)
        assert "does not carry its key's point" in str(err.value)
    assert run.lnl({**rows, key: dict(good, lnL=float("nan"))}, 1, pl)[j] == -np.inf


def test_exit_statuses(tmp_path):
    """Protects: the supervisor's exit status says what happened: 3 budget spent, 4 stopped
    without a certificate, 5 an engine off the NoLoop path (and the run then stays stopped).
    Oracle: analytic."""
    run_dir = str(tmp_path / "budget")
    cmd, env = _command(run_dir, ["--budget", "1"])
    assert subprocess.run(cmd, env=env).returncode == 3
    assert controller.Run(run_dir).ledger()[0] == {}
    run_dir = str(tmp_path / "nofreeze")
    cmd, env = _command(run_dir, ["--set", "max_learn_versions=1"])
    assert subprocess.run(cmd, env=env).returncode == 4
    assert not os.path.exists(os.path.join(run_dir, "certificate.json"))
    run_dir = str(tmp_path / "offpath")
    cmd, env = _command(run_dir)
    env["LEDGER_FAKE_OFF_NOLOOP"] = "1"
    assert subprocess.run(cmd, env=env).returncode == 5
    del env["LEDGER_FAKE_OFF_NOLOOP"]
    assert subprocess.run(cmd, env=env).returncode == 4
    assert controller.Run(run_dir).head()["state"] == "FAILED"


def test_failed_evaluations_end_a_run_and_never_a_certificate(clean_run, tmp_path):
    """Protects: a learning version with too many failed evaluations ends the run; a frozen round
    whose failures cannot be diluted is closed uncertified at once, without drawing to the limit;
    and `rift-run verify` exits 1 when a certificate does not follow from its ledger.
    Oracle: analytic."""
    run_dir = str(tmp_path / "many")
    cmd, env = _command(run_dir)
    env["LEDGER_FAKE_FAIL"] = "0.3"
    assert subprocess.run(cmd, env=env).returncode == 4
    head = controller.Run(run_dir).head()
    assert head["version"] == 1 and "evaluations failed" in head["reason"]
    run_dir = str(tmp_path / "few")
    cmd, env = _command(run_dir)
    env["LEDGER_FAKE_FAIL"] = "0.02"
    assert subprocess.run(cmd, env=env).returncode == 4
    cert = json.load(open(os.path.join(run_dir, "certificate.json")))
    assert not cert["certified"] and not cert["checks"]["failed"] and cert["failed_rows"] > 6
    assert cert["draws"] == 1000, "the round was extended although more draws fail at the same rate"
    verify = [sys.executable, "-B", RIFT_RUN, "verify", "--run-dir"]
    assert subprocess.run(verify + [run_dir], env=env, stdout=subprocess.DEVNULL).returncode == 0
    for field, value in (("certified", True), ("failed_rows", 0)):
        path = os.path.join(run_dir, "certificate.json")
        edited = dict(cert, **{field: value})
        open(path, "w").write(json.dumps(edited))
        assert subprocess.run(verify + [run_dir], env=env, stdout=subprocess.DEVNULL).returncode == 1, field
        open(path, "w").write(json.dumps(cert))


@pytest.fixture(scope="module")
def stub_run(tmp_path_factory):
    """One learning version through rift-run, the real engine and the real AV sampler, with
    stub_ile.py in ILE's place.  The run stops by design after version 1."""
    work = tmp_path_factory.mktemp("stub")
    (work / "args_ile.txt").write_text("X --manual-logarithm-offset 0 --time-marginalization --vectorized --gpu "
                                       "--force-xpy --sampler-method AV --n-chunk 2000\n")
    (work / "args_cip_list.txt").write_text(FINAL_CIP + "\n")
    (work / "grid.dat").write_text("\n".join(controller.GRID_HEADER) + "\n")
    run_dir = str(work / "ledger")
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([CODE, os.environ.get("PYTHONPATH", "")]))
    cmd = [sys.executable, "-B", RIFT_RUN, "--run-dir", run_dir, "--ile-args-file", str(work / "args_ile.txt"),
           "--prior-args-file", str(work / "args_cip_list.txt"), "--grid-template", str(work / "grid.dat"),
           "--ile-exe", os.path.join(HERE, "stub_ile.py"), "--set", "seed=9", "--set", "learn_per_stream=20",
           "--set", "max_learn_versions=1", "--set", "n_max_samples=20000"]
    assert subprocess.run(cmd, env=env, cwd=str(work)).returncode == 4
    return run_dir, env, str(work)


def test_the_engine_writes_keyed_rows_at_the_lease_sample_count(stub_run):
    """Protects: through the real engine, each row is the sampler's integral at the key's point,
    run to the lease's sample count, and a fresh process returns the same rows bit for bit.
    Oracle: analytic (ln L of stub_ile.py) and metamorphic (replay)."""
    run_dir, env, work = stub_run
    run = controller.Run(run_dir)
    rows, stat = run.ledger()
    pl = run.plan(1)
    assert stat["rows"] == int(pl["ok"].sum()) == 80
    lnl = run.lnl(rows, 1, pl)                       # also checks every row's point against its key
    z = []
    for j in np.flatnonzero(pl["ok"]):
        r = rows[(1, int(pl["stream"][j]), int(pl["index"][j]))]
        assert 20000 <= r["ntotal"] < 20000 + 2 * stub_ile.CHUNK
        z.append((lnl[j] - stub_ile.analytic(pl["grid"][j])) / r["sigma"])
    assert abs(np.mean(z)) < 4 / np.sqrt(len(z)) + 0.5        # AV reads slightly low at this count (gate G3)
    assert np.max(np.abs(z)) < 6
    assert len(set(r["key_seed"] for r in rows.values())) == 80
    out = subprocess.run([sys.executable, "-B", RIFT_RUN, "replay", "--run-dir", run_dir, "--replay-keys", "8"],
                         env=env, cwd=work, stdout=subprocess.PIPE, text=True)
    assert out.returncode == 0, out.stdout
    assert json.loads(out.stdout)["identical"] == 8


@pytest.mark.parametrize("flags, status", [(["--stub-skip", "0"], 0), (["--stub-skip", "1"], 0), (["--stub-skip", "2"], 0),
                                           (["--stub-exit", "1"], 3),
                                           (["--stub-double"], engine.EXIT_DOUBLE_CALL)])
def test_the_engine_reports_what_ile_did_to_a_point(stub_run, tmp_path, flags, status):
    """Protects: a point ILE skips gets a FAILED line of its own, wherever it is in the lease; a
    point ILE never starts gets no line, and ILE's exit status is the engine's; a second
    sampler call for one point voids the key and stops the engine, since ILE would swallow an
    exception and leave a row that holds only the first call.  Oracle: analytic."""
    run_dir, env, work = stub_run
    meta = json.load(open(os.path.join(run_dir, "leases", "lease-v01-n0000.json")))
    text = open(os.path.join(run_dir, "leases", "lease-v01-n0000.dat")).read().split("\n")
    head = [x for x in text if x.startswith("#")]
    (tmp_path / "lease.dat").write_text("\n".join(head + [x for x in text if x and not x.startswith("#")][:3]) + "\n")
    (tmp_path / "lease.json").write_text(json.dumps(dict(meta, keys=meta["keys"][:3])))
    shard = str(tmp_path / "shard.jsonl")
    cmd = [sys.executable, "-B", "-m", "RIFT.ledger.engine", "--lease-keys", str(tmp_path / "lease.json"), "--shard", shard,
           "--", os.path.join(HERE, "stub_ile.py"), "--time-marginalization", "--vectorized", "--gpu", "--force-xpy",
           "--sampler-method", "AV", "--sim-grid", str(tmp_path / "lease.dat")] + flags
    assert subprocess.run(cmd, env=env, cwd=work).returncode == status
    lines = [json.loads(x) for x in open(shard)]
    assert not os.path.exists(shard + ".inflight") or flags[0] == "--stub-double"
    if flags[0] == "--stub-skip":
        want = ["OK", "OK", "OK"]
        want[int(flags[1])] = "FAILED"
        assert [x["status"] for x in lines] == want and [x["lease_row"] for x in lines] == [0, 1, 2]
    elif flags[0] == "--stub-exit":
        assert [x["status"] for x in lines] == ["OK"]
    else:
        assert [x["status"] for x in lines] == ["OK", "INVALID"]
        os.makedirs(tmp_path / "run" / "ledger")
        controller.init(str(tmp_path / "run2"), Box.from_json(fake_engine.BOX))
        os.makedirs(tmp_path / "run2" / "ledger")
        os.replace(shard, str(tmp_path / "run2" / "ledger" / "shard-v01-n0000.jsonl"))
        with pytest.raises(SystemExit) as err:
            controller.Run(str(tmp_path / "run2")).ledger()
        assert "voided" in str(err.value)


def test_pseudo_pipe_builds_the_stage_and_refuses_what_it_cannot_run(tmp_path):
    """Protects: `--ledger-run` builds a workflow of one node whose files rift-run accepts, and
    is refused where the stage could not run.  Oracle: analytic.  (The pseudo-build gate holds
    the same checks, but CI does not run that gate.)"""
    import shutil
    import lal
    from RIFT import lalsimutils
    from RIFT.misc import hyperpipeline_io
    pts = []
    for m1, m2 in ((36.0, 29.0), (34.0, 30.0)):
        p = lalsimutils.ChooseWaveformParams()
        p.m1, p.m2, p.s1z, p.fref = m1 * lal.MSUN_SI, m2 * lal.MSUN_SI, 0.1, 20.0
        pts.append(p)
    grid = str(tmp_path / "seed.dat")
    hyperpipeline_io.write_grid_from_P_list(grid, pts, hyperpipeline_io.DEFAULT_BASE_COLUMNS, lal_module=lal,
                                            lalsimutils_module=lalsimutils)
    (tmp_path / "empty.cache").write_text("")
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([CODE, os.environ.get("PYTHONPATH", "")]),
               PATH=os.pathsep.join([os.path.join(CODE, "bin"), os.path.dirname(sys.executable), os.environ["PATH"]]),
               RIFT_DAG_BACKEND="local", GW_SURROGATE="")
    env.pop("RIFT_HYPERPIPELINE_FORMAT", None)
    base = [sys.executable, os.path.join(CODE, "bin", "util_RIFT_pseudo_pipe.py"), "--manual-initial-grid", grid,
            "--fake-data-cache", str(tmp_path / "empty.cache"), "--event-time", "1126259462.391",
            "--manual-ifo-list", "H1,L1", "--approx", "IMRPhenomXPHM", "--assume-precessing", "--fmin-template", "20",
            "--internal-force-iterations", "2", "--internal-n-evaluations-per-iteration", "4", "--n-output-samples", "7",
            "--n-output-samples-last", "7", "--ile-jobs-per-worker", "3", "--skip-reproducibility", "--ledger-run"]

    def build(name, extra):
        return subprocess.run(base + ["--use-rundir", str(tmp_path / name)] + extra, cwd=str(tmp_path), env=env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    out = build("ok", ["--pipeline-builder", "Hyperpipe", "--ile-sampler-method", "AV", "--ledger-run-args", "--budget 600"])
    assert out.returncode == 0, out.stdout[-3000:]
    run = tmp_path / "ok"
    assert [x["name"] for x in json.load(open(run / "terminal_stage_specs.json"))["stages"]] == ["ledger_run"]
    nodes = [x for x in open(run / "marginalize_hyperparameters_local.sh").read().split("\n")
             if x.startswith(("run_unit ", "run_node "))]
    assert len(nodes) == 1 and "TERMINAL_ledger_run" in nodes[0]
    assert "--budget" in open(run / "TERMINAL_ledger_run.sub").read()
    rundir.check_ile_tokens(rundir.read_args_line(str(run / "args_ile.txt")), ILE_EXE)
    rundir.box_from_cip_tokens(rundir.read_args_line(str(run / "args_cip_list.txt")))
    rundir.grid_header(str(run / "proposed-grid.dat"))
    for name, extra, named in (
            ("r1", ["--pipeline-builder", "Hyperpipe"], "--ledger-run without --ile-sampler-method AV"),
            ("r2", ["--pipeline-builder", "Hyperpipe", "--ile-sampler-method", "AV", "--add-extrinsic",
                    "--add-extrinsic-time-resampling"], "--ledger-run with --add-extrinsic"),
            ("r3", ["--pipeline-builder", "Hyperpipe", "--ile-sampler-method", "AV", "--use-osg"], "--ledger-run with --use-osg"),
            ("r4", ["--pipeline-builder", "BasicIteration", "--ile-sampler-method", "AV"],
             "--ledger-run requires --pipeline-builder Hyperpipe")):
        out = build(name, extra)
        assert out.returncode != 0 and named in out.stdout, (name, out.stdout[-1500:])
    shutil.rmtree(str(tmp_path / "ok"))


def _stub_command(work, extra=()):
    work.mkdir(exist_ok=True)
    (work / "args_ile.txt").write_text("X --manual-logarithm-offset 0 --time-marginalization --vectorized --gpu "
                                       "--force-xpy --sampler-method AV --n-chunk 2000\n")
    (work / "args_cip_list.txt").write_text(FINAL_CIP + "\n")
    (work / "grid.dat").write_text("\n".join(controller.GRID_HEADER) + "\n")
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([CODE, os.environ.get("PYTHONPATH", "")]))
    cmd = [sys.executable, "-B", RIFT_RUN, "--run-dir", str(work / "ledger"), "--ile-args-file", str(work / "args_ile.txt"),
           "--prior-args-file", str(work / "args_cip_list.txt"), "--grid-template", str(work / "grid.dat"),
           "--ile-exe", os.path.join(HERE, "stub_ile.py"), "--set", "seed=9", "--set", "learn_per_stream=5",
           "--set", "max_learn_versions=1", "--set", "n_max_samples=4000"] + list(extra)
    return cmd, env


@pytest.mark.parametrize("fault, row", [("STUB_ABORT_PRE", 7), ("STUB_ABORT_SAMPLER", 7), ("STUB_ABORT_PRE", 0),
                                        ("STUB_RAISE_SAMPLER", 7)])
def test_a_point_that_breaks_the_engine_is_failed_and_the_run_goes_on(tmp_path, fault, row):
    """Protects: one point that kills the engine, before or inside the sampler, is marked failed
    after two deaths on it and the other points are all evaluated; a point whose integrand
    raises is failed by the engine itself.  Oracle: analytic (19 rows OK, that one FAILED)."""
    cmd, env = _stub_command(tmp_path / "w")
    assert subprocess.run(cmd + ["--budget", "1"], env=env, cwd=str(tmp_path / "w")).returncode == 3   # plan only
    run = controller.Run(str(tmp_path / "w" / "ledger"))
    pl = run.plan(1)
    env[fault] = repr(float(pl["grid"][row, 0]))
    for _ in range(6):
        status = subprocess.run(cmd, env=env, cwd=str(tmp_path / "w")).returncode
        if status != 6:
            break
    assert status == 4                                   # the run got past the point; it stops by design after version 1
    rows, _ = run.ledger()
    bad = (1, int(pl["stream"][row]), int(pl["index"][row]))
    assert len(rows) == 20 and rows[bad]["status"] == "FAILED"
    assert sum(r["status"] == "OK" for r in rows.values()) == 19
    leases = [json.loads(x) for x in open(run.p("ctl", "leases.jsonl"))]
    assert sum("strike" in x for x in leases) == (0 if fault == "STUB_RAISE_SAMPLER" else 2)


def test_an_engine_that_dies_on_every_point_marks_none(tmp_path):
    """Protects: an engine that dies inside the sampler on whatever point it is given (a lost GPU
    context) is reported as not working, and no point is marked failed.  Oracle: analytic."""
    cmd, env = _stub_command(tmp_path / "w")
    env["STUB_ABORT_SAMPLER"] = "all"
    for _ in range(2):
        assert subprocess.run(cmd, env=env, cwd=str(tmp_path / "w")).returncode == 6
    run = controller.Run(str(tmp_path / "w" / "ledger"))
    assert run.ledger()[0] == {}
    leases = [json.loads(x) for x in open(run.p("ctl", "leases.jsonl"))]
    assert len(leases) == 6 and all(x.get("not_working") for x in leases)


def test_reported_parameters_and_the_prior_density():
    """Protects: chi_eff, chi_p, q and mc are the standard definitions, and the prior density is
    normalised on its stated support, including an eta range that ends below 0.25.
    Oracle: analytic (one hand-computed binary; the density integrates to 1, estimated with draws
    from a wide proposal that does not come from the prior code)."""
    g = np.array([[30.0, 20.0, 0.3, 0.4, 0.1, 0.0, -0.6, -0.2]])
    out = summaries(g)
    assert out["q"][0] == pytest.approx(2 / 3) and out["mc"][0] == pytest.approx(600 ** 0.6 / 50 ** 0.2)
    assert out["chi_eff"][0] == pytest.approx((30 * 0.1 - 20 * 0.2) / 50)
    assert out["chi_p"][0] == pytest.approx(max(0.5, (4 * 2 / 3 + 3) / (4 + 3 * 2 / 3) * (2 / 3) * 0.6))
    box = Box([25.0, 35.0], [0.12, 0.24], 0.8)
    sd = np.array([4.0, 0.25, 0.5, 0.5])
    wide = proposal.BlockProposal.from_covariances([1.0], [[30.0, 0.4, 0.0, 0.0]], [np.diag(sd ** 2)],
                                                   [30.0, 0.4, 0.0, 0.0], np.diag((2 * sd) ** 2), a=0.3, seed=2)
    a = np.array([wide.draw(0, i)[0] for i in range(60000)])
    val = np.exp(box.ln_prior_A(a) - wide.ln_q(a))
    err = val.std(ddof=1) / np.sqrt(len(val))
    assert err < 0.03 and abs(val.mean() - 1.0) < 4 * err
    edge = np.array([[30.0, box.delta_min - 1e-3, 0.3, 0.3], [30.0, box.delta_max + 1e-3, 0.3, 0.3], [30.0, 0.5, 0.81, 0.3],
                     [24.9, 0.5, 0.3, 0.3], [35.1, 0.5, 0.3, 0.3], [30.0, 0.5, 0.3, 0.0]])
    assert np.all(np.isneginf(box.ln_prior_A(edge))) and np.isfinite(box.ln_prior_A([[30.0, 0.5, 0.3, 0.3]])[0])


@pytest.mark.parametrize("name", sorted(engine.ILE_REFUSED))
def test_every_refused_ile_option_is_refused(name):
    """Protects: each option in engine.ILE_REFUSED is an option ILE declares, and is refused by
    the engine and by rift-run under its full name.  Oracle: ILE's own option list."""
    assert name in engine.option_names(ILE_EXE)
    for check in (lambda t: engine.check_ile_arguments(t, ILE_EXE), lambda t: rundir.check_ile_tokens(t, ILE_EXE)):
        with pytest.raises(SystemExit) as err:
            check(ILE.split() + [name, "1"])
        assert name in str(err.value)


def test_the_builder_accepts_zero_iterations_only_for_terminal_stages(tmp_path):
    """Protects: `--n-iterations 0` builds terminal stages alone and is refused without them,
    with terminal evidence, or below zero.  Oracle: analytic."""
    builder = os.path.join(CODE, "bin", "create_eos_posterior_pipeline")
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([CODE, os.environ.get("PYTHONPATH", "")]), RIFT_DAG_BACKEND="local")
    (tmp_path / "grid.dat").write_text("\n".join(controller.GRID_HEADER) + "\n0 0 30 20 0 0 0 0 0 0\n")
    (tmp_path / "stages.json").write_text(json.dumps(dict(version=1, stages=[dict(
        name="only", kind="command-v1", exe="/bin/true", args="", initial_dir=str(tmp_path), log_dir=str(tmp_path))])))
    base = [sys.executable, builder, "--input-grid", str(tmp_path / "grid.dat"), "--working-directory", str(tmp_path)]
    for extra, named in ((["--n-iterations", "0"], "builds terminal stages only"),
                         (["--n-iterations", "0", "--terminal-stage-spec-file", str(tmp_path / "stages.json"),
                           "--terminal-evidence"], "--terminal-evidence needs a posterior iteration"),
                         (["--n-iterations", "-1"], "must not be negative")):
        out = subprocess.run(base + extra, cwd=str(tmp_path), env=env, text=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        assert out.returncode != 0 and named in out.stdout, out.stdout[-800:]
