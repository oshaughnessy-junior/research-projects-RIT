"""Posterior/prior transport invariants; fixture execution is not a science test."""
from __future__ import annotations

import json
import os
from pathlib import Path
import runpy
import shlex
import shutil
import subprocess
import sys

import pytest


@pytest.mark.parametrize("key", [
    "universe", "INITIALDIR", "transfer_output_files", "arguments", "executable",
    "MY.i_w_d", "+tRaNsFeR_oUtPuT_rEmApS", "MY.Out", "+Cmd", "MY.Args",
    "+JobUniverse", "+TransferInputFiles", "MY.UserLog", "+Requirements",
    "MY.SingularityImage", "+ContainerImage", "docker_image",
])
def test_posterior_policy_reserves_transport(hp_modules, key):
    with pytest.raises(ValueError, match="reserved"):
        hp_modules.execution_contract.normalize_posterior_commands({key: "x"})


@pytest.mark.parametrize("value", [[], None, {"environment": {}},
                                  {"environment": "a\nb"}, {"a=b": "x"}])
def test_posterior_policy_rejects_invalid_commands(hp_modules, value):
    with pytest.raises(ValueError):
        hp_modules.execution_contract.normalize_posterior_commands(value)


def test_posterior_policy_preserves_explicit_runtime(hp_modules):
    policy = {"environment": '"PATH=/pinned/bin PYTHONPATH=."',
              "request_memory": "2G", "+AccountingGroup": '"test"',
              "+UNDESIRED_Sites": '"bad"', "periodic_remove": "False"}
    assert hp_modules.execution_contract.normalize_posterior_commands(policy) == policy


@pytest.mark.parametrize("outputs, expected", [
    (["a.dat", "b.dat", "a.dat"], "a.dat,b.dat"),
    ("a.dat,b.dat", "a.dat,b.dat"), ([], ""),
])
def test_hyperpost_output_transfer_is_emitted(hp_modules, outputs, expected):
    job, _ = hp_modules.dag_utils_generic.write_hyperpost_sub(
        exe="/bin/true", arg_str="--no-plots", out_dir=".",
        transfer_output_files=outputs)
    commands = dict(job.condor_cmds)
    assert commands["transfer_output_files"] == expected
    assert commands["should_transfer_files"] == "YES"


def _ads(path):
    return dict((key.strip().lower(), value.strip()) for key, value in (
        line.split("=", 1) for line in path.read_text().splitlines()
        if "=" in line))


def _build(hp_modules, rift_bin, tmp_path, monkeypatch, mode, policy=True):
    run = tmp_path / "run"
    run.mkdir()
    (run / "seed.dat").write_text(
        "# RIFT_HYPERPIPELINE_V1\n# lnL sigma_lnL m1 m2\n0 0 36 29\n")
    (run / "jobs.json").write_text(json.dumps([{
        "protocol": "indexed-grid-v1", "name": "fixture",
        "exe": "/bin/true", "args": "", "n_chunk": 1,
        "execution": {"requires_data_inputs": False}}]))
    (run / "post.args").write_text("1 --parameter m1 --parameter m2\n")
    (run / "runtime.json").write_text('{"marker":"staged"}\n')
    (run / "candidate_module.py").write_text('MARKER = "candidate"\n')
    worker = run / "candidate-post"
    worker.write_text("""#!/usr/bin/env python3
import argparse, json
from pathlib import Path
import candidate_module
p=argparse.ArgumentParser()
p.add_argument("--fname")
p.add_argument("--fname-output-integral")
p.add_argument("--fname-output-samples")
a, rest=p.parse_known_args()
assert "--integrate-prior" in rest
assert candidate_module.MARKER == "candidate"
assert json.loads(Path("runtime.json").read_text())["marker"] == "staged"
for name in (a.fname,a.fname_output_integral,a.fname_output_samples):
    assert not Path(name).is_absolute(), name
assert Path(a.fname).is_file()
for suffix in (".dat","+annotation.dat","_withpriorchange.dat","_withpriorchange+annotation.dat"):
    Path(a.fname_output_integral+suffix).write_text("# lnL sigmaL neff\\n0 0.1 100\\n")
""")
    worker.chmod(0o755)
    # These sources are relative to ordinary posterior iteration_N_post.
    (run / "transfers.txt").write_text("../runtime.json\n")
    commands = {"environment": '"PATH=/pinned/bin PYTHONPATH=."',
                "periodic_remove": "JobStatus == 2 && CurrentTime-JobStartDate > 60"}
    (run / "commands.json").write_text(json.dumps(commands))
    argv = [
        str(rift_bin / "create_eos_posterior_pipeline"),
        "--working-directory", str(run), "--input-grid", str(run / "seed.dat"),
        "--marg-job-spec-file", str(run / "jobs.json"),
        "--eos-post-args-list", str(run / "post.args"),
        "--eos-post-exe", str(worker), "--n-iterations", "2",
        "--n-samples-per-job", "1", "--eos-post-explode-jobs", "1",
        "--eos-post-explode-jobs-last", "1", "--terminal-evidence",
        "--condor-local-nonworker", "--use-full-submit-paths"]
    if mode != "legacy":
        argv += ["--use-singularity", "--transfer-file-list",
                 str(run / "transfers.txt"), "--eos-post-transfer-executable",
                 "--eos-post-transfer-file", "../candidate_module.py",
                 "--eos-post-request-disk", "4G"]
        monkeypatch.setenv("SINGULARITY_RIFT_IMAGE", "/cvmfs/test/image.sif")
    if mode == "osg":
        argv += ["--use-osg"]
    if policy:
        argv += ["--eos-post-condor-commands", str(run / "commands.json")]
    monkeypatch.setenv("RIFT_REQUIRE_NONWORKER", "HAS_CVMFS_software_igwn_org")
    monkeypatch.setenv("RIFT_DAG_BACKEND", "htcondor")
    monkeypatch.setenv("RIFT_HYPERPIPELINE_FORMAT", "1")
    monkeypatch.chdir(run)
    monkeypatch.setattr(sys, "argv", argv)
    runpy.run_path(str(rift_bin / "create_eos_posterior_pipeline"), run_name="__main__")
    return run, commands


@pytest.mark.parametrize("mode", ["osg", "container"])
def test_generated_prior_inherits_remote_policy(
        hp_modules, rift_bin, tmp_path, monkeypatch, mode):
    run, commands = _build(hp_modules, rift_bin, tmp_path, monkeypatch, mode)
    prior = _ads(run / "EOS_POST_prior.sub")
    normal = _ads(run / "EOS_POST_worker_0.sub")
    for key in ("environment", "periodic_remove", "my.singularityimage",
                "transfer_executable", "request_disk"):
        assert prior[key] == normal[key], (key, prior, normal)
    assert prior["request_disk"] == "4G"
    assert "HAS_CVMFS_software_igwn_org" in prior["requirements"]
    assert "HAS_CVMFS_software_igwn_org" in normal["requirements"]
    assert prior["universe"].lower() == "vanilla"
    # Scientific prior remains remote when AP bookkeeping is explicitly local.
    assert _ads(run / "evidence_final.sub")["universe"].lower() == "local"
    assert prior["initialdir"] == str(run)
    inputs = prior["transfer_input_files"].split(",")
    for name in ("all.marg_net", "grid-$(macroiteration).dat",
                 "runtime.json", "candidate_module.py", "candidate-post"):
        assert str(run / name) in inputs, (name, inputs)
    assert "../all.marg_net" not in prior["transfer_input_files"]
    assert "--fname=./all.marg_net" in shlex.split(prior["arguments"].strip('"'))
    assert str(run) not in prior["arguments"]
    assert "prior-integral-$(macroiteration)_withpriorchange+annotation.dat" in (
        prior["transfer_output_files"].split(","))
    if mode == "osg":
        assert "getenv" not in prior and "getenv" not in normal


def test_legacy_prior_defaults_unchanged(
        hp_modules, rift_bin, tmp_path, monkeypatch):
    run, _ = _build(hp_modules, rift_bin, tmp_path, monkeypatch, "legacy", False)
    prior = _ads(run / "EOS_POST_prior.sub")
    assert prior["universe"].lower() == "local"
    assert str(run / "all.marg_net") in prior["arguments"]
    assert "transfer_output_files" not in prior
    assert "transfer_input_files" not in prior
    assert "getenv" in prior
    assert "environment" not in prior


@pytest.mark.parametrize("mutant", [None, "wrong-input", "missing-return"])
def test_prior_nonshared_fixture_return_consumer(
        hp_modules, rift_bin, tmp_path, monkeypatch, mutant):
    run, _ = _build(hp_modules, rift_bin, tmp_path, monkeypatch, "osg")
    prior = _ads(run / "EOS_POST_prior.sub")
    (run / "all.marg_net").write_text("physical-input-placeholder\n")
    (run / "grid-0.dat").write_text("grid-placeholder\n")
    sandbox = tmp_path / "nonshared"
    sandbox.mkdir()
    inputs = prior["transfer_input_files"].replace("$(macroiteration)", "0").split(",")
    if mutant == "wrong-input":
        inputs = [str(run.parent / "all.marg_net") if Path(item).name == "all.marg_net"
                  else item for item in inputs]
    def stage_execute_return():
        for item in inputs:
            shutil.copyfile(item, sandbox / Path(item).name)
        args = prior["arguments"].replace("$(macroiteration)", "0")
        subprocess.run([sys.executable, str(sandbox / "candidate-post")]
                       + shlex.split(args.strip('"')), cwd=sandbox, check=True)
        outputs = prior["transfer_output_files"].replace("$(macroiteration)", "0").split(",")
        if mutant == "missing-return":
            outputs = [x for x in outputs if "_withpriorchange+annotation" not in x]
        for name in outputs:
            shutil.copyfile(sandbox / name, run / name)
        returned = run / "prior-integral-0_withpriorchange+annotation.dat"
        worker = run / "worker"
        for suffix in ("+annotation.dat", "_withpriorchange+annotation.dat"):
            Path(str(worker) + suffix).write_text("# lnL sigmaL neff\n1 0.1 100\n")
        consumer = runpy.run_path(str(rift_bin / "util_CIPDirSummarizeEvidence.py"))
        result = consumer["main"]([
            "--cip-dir", str(run), "--cip-prefix", str(worker), "--strict",
            "--output", str(run / "evidence.dat"),
            "--prior-integral", str(returned),
            "--normalized-output", str(run / "normalized.dat")])
        assert result == 0, "strict evidence consumer rejected returned annotation"
        assert returned.read_text().splitlines()[1] == "0 0.1 100"
        assert (run / "normalized.dat").is_file()
    if mutant:
        with pytest.raises((FileNotFoundError, AssertionError)):
            stage_execute_return()
    else:
        stage_execute_return()
