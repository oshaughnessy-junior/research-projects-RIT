#!/usr/bin/env python3
"""Run a tiny real Hyperpipe pipeline locally, without HTCondor."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile

import numpy as np

from RIFT.misc import hyperpipeline_io


HERE = Path(__file__).resolve()
RIFT_ROOT = HERE.parents[5]
CODE = RIFT_ROOT / "MonteCarloMarginalizeCode" / "Code"
BIN = CODE / "bin"
PSEUDO_PIPE = BIN / "util_RIFT_pseudo_pipe.py"


def _environment(run_base: Path):
    env = os.environ.copy()
    env.pop("RIFT_HYPERPIPELINE_FORMAT", None)
    env["RIFT_DAG_BACKEND"] = "htcondor"
    env["RIFT_ROOT"] = str(RIFT_ROOT)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(CODE), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env["PATH"] = os.pathsep.join(
        [str(BIN), str(Path(sys.executable).parent),
         env.get("PATH", "")]).rstrip(os.pathsep)
    env["GW_SURROGATE"] = ""
    for name in ("XDG_CACHE_HOME", "MPLCONFIGDIR", "PYTHONPYCACHEPREFIX"):
        directory = run_base / name.lower()
        directory.mkdir(parents=True, exist_ok=True)
        env[name] = str(directory)
    return env


def _write_zero_spin_grid(path: Path):
    rows = []
    for m1 in (30.0, 32.0, 34.0):
        for m2 in (20.0, 22.0, 24.0):
            rows.append([0.0, 0.0, m1, m2, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0])
    hyperpipeline_io.write_table(
        str(path), hyperpipeline_io.DEFAULT_BASE_COLUMNS, rows)


def _build(run_base: Path, osg_contract=False):
    seed = run_base / "zero-spin-grid.dat"
    cache = run_base / "empty.cache"
    rundir = run_base / "run"
    _write_zero_spin_grid(seed)
    cache.touch()
    command = [
        sys.executable, str(PSEUDO_PIPE),
        "--use-rundir", str(rundir),
        "--pipeline-builder", "Hyperpipe",
        "--manual-initial-grid", str(seed),
        "--fake-data-cache", str(cache),
        "--event-time", "1126259462.391",
        "--manual-ifo-list", "H1",
        "--approx", "IMRPhenomD",
        "--assume-nospin",
        "--fmin-template", "20",
        "--force-mc-range", "[18,30]",
        "--force-eta-range", "[0.14,0.249999]",
        "--internal-force-iterations", "1",
        "--internal-force-puff-iterations", "-1",
        "--internal-n-evaluations-per-iteration", "9",
        "--n-output-samples", "9",
        "--n-output-samples-last", "9",
        "--ile-jobs-per-worker", "3",
        "--cip-explode-jobs", "1",
        "--cip-explode-jobs-last", "1",
        "--ile-no-gpu",
        "--ile-zero-likelihood-data-free",
        "--manual-extra-test-args=--always-succeed",
        "--skip-reproducibility",
    ]
    env = _environment(run_base)
    if osg_contract:
        command.extend([
            "--use-osg",
            "--use-osg-file-transfer",
            "--internal-use-oauth-files", "scitokens",
        ])
        env["SINGULARITY_RIFT_IMAGE"] = "osdf://example.invalid/rift-test.sif"
        env["SINGULARITY_BASE_EXE_DIR"] = str(BIN)
    result = subprocess.run(
        command, cwd=str(run_base), env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (run_base / "pipeline-build.log").write_text(result.stdout)
    if result.returncode:
        raise RuntimeError(
            "constant-likelihood pseudo build failed ({}):\n{}".format(
                result.returncode, result.stdout))
    return rundir


def _assert_osg_data_free_contract(rundir: Path):
    marg_submit = (rundir / "MARG_0.sub").read_text()
    commands = _read_submit(rundir / "MARG_0.sub")
    transferred = commands.get("transfer_input_files", "")
    for token in ("H1-psd.xml.gz", "frames_dir", "ile_pre.sh"):
        assert token not in transferred, (token, transferred)
    assert "precmd" not in commands
    assert "+precmd" not in commands
    assert "hyperpipeline_io.py" in transferred
    assert commands.get("transfer_executable", "true").lower() != "false"
    assert commands["executable"] == str(
        BIN / "integrate_likelihood_extrinsic_batchmode")
    assert "--zero-likelihood-data-free" in marg_submit
    assert "use_oauth_services = scitokens" in marg_submit
    assert "rift-test.sif" in marg_submit


def _parse_dag(path: Path):
    jobs, macros, parents = {}, {}, {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if fields and fields[0] == "JOB":
            jobs[fields[1]] = Path(fields[2])
        elif fields and fields[0] == "VARS":
            macros[fields[1]] = dict(re.findall(r'(\w+)="([^"]*)"', line))
        elif fields and fields[0] == "PARENT":
            split_at = fields.index("CHILD")
            for child in fields[split_at + 1:]:
                parents.setdefault(child, set()).update(fields[1:split_at])
    return jobs, macros, parents


def _read_submit(path: Path):
    values = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key.strip().lower()] = value.strip().strip('"')
    return values


def _substitute(text: str, macros):
    values = {"cluster": "0", "process": "0", "macromassid": "0"}
    values.update(macros)
    for key, value in values.items():
        text = text.replace("$({})".format(key), str(value))
    unresolved = re.findall(r"\$\([^)]+\)", text)
    if unresolved:
        raise RuntimeError(
            "unresolved submit macros {} in {!r}".format(unresolved, text))
    return text


def _execute_without_condor(rundir: Path, env):
    jobs, macros, parents = _parse_dag(
        rundir / "marginalize_hyperparameters.dag")
    pending, completed = set(jobs), set()
    logs = rundir / "local-execution-logs"
    logs.mkdir()
    while pending:
        ready = sorted(
            node for node in pending
            if parents.get(node, set()) <= completed)
        if not ready:
            blocked = {node: sorted(parents.get(node, set()) - completed)
                       for node in pending}
            raise RuntimeError("local DAG executor is blocked: {}".format(blocked))
        for node in ready:
            submit = _read_submit(jobs[node])
            node_macros = macros.get(node, {})
            executable = _substitute(submit["executable"], node_macros)
            arguments = _substitute(submit.get("arguments", ""), node_macros)
            initialdir = Path(_substitute(
                submit.get("initialdir", str(rundir)), node_macros))
            initialdir.mkdir(parents=True, exist_ok=True)
            log_path = logs / (re.sub(r"[^A-Za-z0-9_.-]", "_", node) + ".log")
            command = [executable] + shlex.split(arguments)
            with log_path.open("w") as stream:
                result = subprocess.run(
                    command, cwd=str(initialdir), env=env, text=True,
                    stdout=stream, stderr=subprocess.STDOUT, timeout=180)
            if result.returncode:
                tail = "\n".join(log_path.read_text().splitlines()[-80:])
                raise RuntimeError(
                    "local DAG node {} failed ({}): {}\n{}".format(
                        node, result.returncode, shlex.join(command), tail))
            pending.remove(node)
            completed.add(node)
    return logs, completed


def _assert_outputs(rundir: Path, logs: Path, completed):
    marg_files = sorted((rundir / "iteration_0_marg" / "event_0").glob(
        "MARG*.dat"))
    # Three ILE nodes each analyze three indexed points; ILE emits one standard
    # shard per event rather than one multi-row shard per node.
    assert len(marg_files) == 9, marg_files
    total_rows = 0
    for path in marg_files:
        table, columns = hyperpipeline_io.read_table(str(path))
        table = np.atleast_1d(table)
        total_rows += len(table)
        assert np.allclose(table["lnL"], 0.0)
        assert np.allclose(table["sigma_lnL"], 0.0)
        for spin in ("a1x", "a1y", "a1z", "a2x", "a2y", "a2z"):
            assert spin in columns and np.allclose(table[spin], 0.0)
    assert total_rows == 9

    posterior = rundir / "grid-1.dat"
    assert posterior.is_file() and posterior.stat().st_size > 0
    posterior_table, posterior_columns = hyperpipeline_io.read_table(
        str(posterior))
    posterior_table = np.atleast_1d(posterior_table)
    assert len(posterior_table) == 9
    assert np.allclose(posterior_table["lnL"], 0.0)
    assert np.allclose(posterior_table["sigma_lnL"], 0.0)
    data_lines = [line for line in posterior.read_text().splitlines()
                  if line.strip() and not line.startswith("#")]
    assert all(len(line.split()) == len(posterior_columns)
               for line in data_lines)
    for spin in ("a1x", "a1y", "a1z", "a2x", "a2y", "a2z"):
        if spin in posterior_columns:
            assert np.allclose(posterior_table[spin], 0.0)

    assert (rundir / "all.marg_net").stat().st_size > 0
    normalized_evidence = np.atleast_1d(np.loadtxt(
        rundir / "evidence_0_normalized"))
    assert len(normalized_evidence) >= 6
    assert np.all(np.isfinite(normalized_evidence[:6]))
    # A constant likelihood has a unit Bayes factor after dividing by the
    # independent L=1 prior integral, modulo the two Monte Carlo estimates.
    assert abs(normalized_evidence[4]) < max(
        0.1, 5.0 * normalized_evidence[5])
    combined_logs = "\n".join(
        path.read_text(errors="replace") for path in logs.glob("*.log"))
    assert "Data-free zero likelihood output:" in combined_logs
    assert "Reading channel" not in combined_logs
    assert len(completed) >= 8


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keep-output", action="store_true",
        help="Keep the generated pipeline, local execution logs, and outputs")
    parser.add_argument(
        "--osg-build-contract-only", action="store_true",
        help="Build and inspect the OSG data-free transfer contract without running jobs")
    args = parser.parse_args(argv)

    temporary = None
    if args.keep_output:
        run_base = Path(tempfile.mkdtemp(
            prefix="rift-constant-likelihood-gate-"))
    else:
        temporary = tempfile.TemporaryDirectory(
            prefix="rift-constant-likelihood-gate-")
        run_base = Path(temporary.name)
    try:
        env = _environment(run_base)
        rundir = _build(run_base, osg_contract=args.osg_build_contract_only)
        if args.osg_build_contract_only:
            _assert_osg_data_free_contract(rundir)
            print("constant-likelihood OSG build contract gate: PASS")
            if args.keep_output:
                print("outputs retained at {}".format(run_base))
            return 0
        logs, completed = _execute_without_condor(rundir, env)
        _assert_outputs(rundir, logs, completed)
        print("constant-likelihood local pipeline gate: PASS")
        if args.keep_output:
            print("outputs retained at {}".format(run_base))
    finally:
        if temporary is not None:
            temporary.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
