#!/usr/bin/env python3
"""Close the Hyperpipe loop for extra parameters lambda, locally, without HTCondor.

util_RIFT_pseudo_pipe.py --pipeline-builder Hyperpipe builds a two-iteration run with ILE's
extra-parameter seam (--internal-ile-supplementary-waveform-transform-*): lens_amplitude is
fit on [1, 2] (--internal-supplementary-waveform-transform-parameter-range) and puffed with
reflection, lens_extra is pinned at pi/2 (-pin; not exact at consolidation's 5 decimals).  The generated DAG then runs with the
constant-likelihood gate's HTCondor-file executor (one
distinct $(cluster) per node, as HTCondor assigns), the REAL CIP, PUFF, consolidation, convergence
and evidence stages, and lambda_marg_stub.py as the MARG worker.  The stub checks it got the
NoLoop seam line, and returns an analytic lnL(lens_amplitude) peaked at A_PEAK; it needs no
data, because ILE's seam refuses --zero-likelihood.

Checks: lambda survives ILE -> CIP -> PUFF -> ILE.  The iteration-1 MARG rows carry exactly
the lambda values of grid-1.dat (CIP) and grid_puff-1.dat (PUFF); every stage's grid has both
columns, lens_amplitude in range and lens_extra == pi/2; PUFF loses no row to the lambda range
(reflection keeps rows pushed past the lower edge); and the final CIP grid keeps the stub's
strong mc-lambda correlation (lambda stays paired with its own intrinsic point) and
concentrates lens_amplitude near the peak (so lambda is fit, not just copied).
"""
import argparse
import importlib.util
import math
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np

from RIFT.misc import hyperpipeline_io as hpio

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "_const_gate", str(HERE / "run_constant_likelihood_pipeline_gate.py"))
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)
STUB = HERE / "lambda_marg_stub.py"
A_PEAK, SIGMA = 1.2, 0.05            # lambda_marg_stub.py's lnL parameters
SLOPE, MC_REF = 0.2, 23.0
LO, HI = 1.0, 2.0
PIN = math.pi / 2

PLUGIN = '''
import numpy
EXTRA_PARAMETERS = ["lens_amplitude"]
def transfer_function(f, extra_params, P=None, xpy=numpy, **kw):
    return extra_params["lens_amplitude"] * xpy.ones(xpy.shape(f), dtype=complex)
'''


def _seed(path):
    rows = []
    amps = np.linspace(1.05, 1.9, 6)
    for i, (m1, m2) in enumerate([(30., 20.), (32., 22.), (34., 24.), (31., 21.)]):
        for a in amps:
            rows.append([0., 0., m1, m2, 0., 0., 0., 0., 0., 0., a, PIN])
    rows.append([0., 0., 33., 23., 0., 0., 0., 0., 0., 0., 1.2, 2.0])  # off the pin: CIP drops it
    hpio.write_table(str(path), tuple(hpio.DEFAULT_BASE_COLUMNS) + ("lens_amplitude", "lens_extra"), rows)
    return len(rows)


def _build(base):
    seed, cache, rundir = base / "seed.dat", base / "empty.cache", base / "run"
    n_seed = _seed(seed)
    cache.touch()
    plugin = base / "lens_plug.py"
    plugin.write_text(PLUGIN)
    cmd = [sys.executable, str(gate.PSEUDO_PIPE), "--use-rundir", str(rundir),
           "--pipeline-builder", "Hyperpipe", "--manual-initial-grid", str(seed),
           "--fake-data-cache", str(cache), "--event-time", "1126259462.391",
           "--manual-ifo-list", "H1", "--approx", "IMRPhenomD", "--assume-nospin",
           "--fmin-template", "20", "--force-mc-range", "[18,30]",
           "--force-eta-range", "[0.14,0.249999]",
           "--internal-force-iterations", "2", "--internal-force-puff-iterations", "2",
           "--internal-n-evaluations-per-iteration", "40",
           "--n-output-samples", "40", "--n-output-samples-last", "40",
           "--ile-jobs-per-worker", "20", "--cip-explode-jobs", "2", "--cip-explode-jobs-last", "2",
           "--ile-no-gpu", "--ile-exe", str(STUB),
           "--manual-extra-test-args=--always-succeed", "--skip-reproducibility",
           "--internal-ile-supplementary-waveform-transform-code", str(plugin),
           "--internal-ile-supplementary-waveform-transform-parameter", "lens_extra",
           "--internal-supplementary-waveform-transform-parameter-range", "lens_amplitude:[{},{}]".format(LO, HI),
           "--internal-supplementary-waveform-transform-parameter-reflect", "lens_amplitude",
           "--internal-supplementary-waveform-transform-parameter-pin", "lens_extra:{!r}".format(PIN)]
    env = gate._environment(base)
    r = subprocess.run(cmd, cwd=str(base), env=env, text=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (base / "pipeline-build.log").write_text(r.stdout)
    if r.returncode:
        raise RuntimeError("lambda pseudo build failed ({}):\n{}".format(r.returncode, r.stdout[-5000:]))
    return rundir, env, n_seed


def _execute(rundir, env):
    """gate._execute_without_condor, but with a distinct $(cluster) per node.

    MARG and MARG_PUFF nodes of one iteration write MARG-<event>-$(cluster)-$(process).dat
    into the same directory, so a constant $(cluster) -- the const-lnL executor's "0" --
    overwrites one with the other.  (The `local` backend draws a fresh id per node since
    RIFT #519.)"""
    counter = [0]
    original = gate._substitute

    def substitute(text, macros):
        return original(text, dict({"cluster": str(macros["_cluster"])}, **macros))
    jobs, macros, _parents = gate._parse_dag(rundir / "marginalize_hyperparameters.dag")
    for node in jobs:
        counter[0] += 1
        macros.setdefault(node, {})["_cluster"] = str(counter[0])
    gate._parse_dag = lambda path, _r=(jobs, macros, _parents): _r
    gate._substitute = substitute
    try:
        return gate._execute_without_condor(rundir, env)
    finally:
        gate._substitute = original


def _lam(path):
    t, cols = hpio.read_table(str(path))
    t = np.atleast_1d(t)
    assert "lens_amplitude" in cols and "lens_extra" in cols, (path, cols)
    return np.column_stack([t["lens_amplitude"], t["lens_extra"]]).astype(float), t


def _marg_rows(rundir, it):
    files = sorted((rundir / "iteration_{}_marg".format(it)).rglob("MARG*.dat"))
    assert files, "no MARG output in iteration {}".format(it)
    return np.vstack([_lam(f)[0] for f in files])


def _key(rows):
    return sorted(map(tuple, np.round(rows, 9)))


def _check(rundir, n_seed):
    m0 = _marg_rows(rundir, 0)
    assert len(m0) == n_seed, (len(m0), n_seed)
    seed_lam, _ = _lam(rundir / "proposed-grid.dat")
    assert _key(m0) == _key(seed_lam), "iteration-0 ILE did not echo the seed lambda"
    for name in ("grid-1.dat", "grid_puff-1.dat", "grid-2.dat"):
        lam, _ = _lam(rundir / name)
        assert len(lam), name
        assert np.all((lam[:, 0] >= LO) & (lam[:, 0] <= HI)), (name, lam[:, 0].min(), lam[:, 0].max())
        assert np.allclose(lam[:, 1], PIN, rtol=0, atol=1e-12), (name, np.unique(lam[:, 1]))
    puffed, _ = _lam(rundir / "grid_puff-1.dat")
    cip1, _ = _lam(rundir / "grid-1.dat")
    assert len(np.unique(np.round(puffed[:, 0], 9))) > 1
    m1 = _marg_rows(rundir, 1)
    assert _key(m1) == _key(np.vstack([cip1, puffed])), \
        "iteration-1 ILE rows do not carry the lambda of grid-1 (CIP) + grid_puff-1 (PUFF)"
    final, t2 = _lam(rundir / "grid-2.dat")
    m1c, m2c = np.asarray(t2["m1"], float), np.asarray(t2["m2"], float)
    mc = (m1c * m2c) ** 0.6 / (m1c + m2c) ** 0.2
    # the stub's conditional peak is A_PEAK + SLOPE (mc - MC_REF): test lambda about it
    resid = final[:, 0] - SLOPE * (mc - MC_REF)
    mean, std = resid.mean(), resid.std()
    assert abs(mean - A_PEAK) < 0.1 and std < 0.2, (mean, std)
    corr = float(np.corrcoef(mc, final[:, 0])[0, 1])
    assert corr > 0.5, ("lambda is not paired with its intrinsic point", corr)
    return dict(n_seed=n_seed, n_marg1=len(m1), n_cip1=len(cip1), n_puff1=len(puffed),
                final_resid_mean=mean, final_resid_std=std, final_corr_mc_lambda=corr)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-output", action="store_true")
    args = ap.parse_args(argv)
    tmp = None
    if args.keep_output:
        base = Path(tempfile.mkdtemp(prefix="rift-lambda-loop-gate-"))
    else:
        tmp = tempfile.TemporaryDirectory(prefix="rift-lambda-loop-gate-")
        base = Path(tmp.name)
    try:
        rundir, env, n_seed = _build(base)
        _execute(rundir, env)
        summary = _check(rundir, n_seed)
        print("lambda loop gate: PASS", summary)
        if args.keep_output:
            print("outputs retained at {}".format(base))
    finally:
        if tmp is not None:
            tmp.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
