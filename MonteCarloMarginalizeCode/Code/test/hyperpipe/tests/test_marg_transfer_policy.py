"""MARG transfer policy survives normalization and actual pipeline emission."""
from __future__ import annotations

import json
import runpy
import shutil
import subprocess
import sys

import pytest


URIS = ["osdf:///ospool/test/frames", "https://example.invalid/psd.dat",
        "custom+stage://host/path"]


@pytest.mark.parametrize("outputs", ["omitted", None, [], ["MARG.dat", "result.xml.gz"]])
def test_transfer_normalization(hp_modules, tmp_path, outputs):
    execution = {"transfer_files": URIS + ["local.cache", "/absolute/psd.dat"]}
    if outputs != "omitted":
        execution["transfer_output_files"] = outputs
    spec = hp_modules.marg_contract.MargJobSpec(
        {"exe": "/bin/true", "protocol": "indexed-grid-v1",
         "execution": execution}, str(tmp_path))
    assert spec.execution["transfer_files"] == URIS + [
        str(tmp_path / "local.cache"), "/absolute/psd.dat"]
    assert spec.execution["transfer_output_files"] == (
        None if outputs == "omitted" else outputs)


def _ads(path):
    return dict((key.strip().lower(), value.strip()) for key, value in (
        line.split("=", 1) for line in path.read_text().splitlines() if "=" in line))


@pytest.mark.parametrize("outputs", ["omitted", None, [], ["MARG.dat"]])
def test_generated_marg_and_indexed_terminal_transfer_policy(
        hp_modules, rift_bin, tmp_path, monkeypatch, outputs):
    run = tmp_path / "run"
    run.mkdir()
    seed = run / "seed.dat"
    seed.write_text("# RIFT_HYPERPIPELINE_V1\n# lnL sigma_lnL m1 m2\n0 0 36 29\n")
    execution = {
        "requires_data_inputs": False, "use_osg": True,
        "transfer_files": URIS + ["local.cache"]}
    if outputs != "omitted":
        execution["transfer_output_files"] = outputs
    job = {"name": "ile", "exe": "/bin/true", "protocol": "indexed-grid-v1",
           "execution": execution}
    (run / "local.cache").write_text("fixture\n")
    (run / "jobs.json").write_text(json.dumps([job]))
    (run / "terminal.json").write_text(json.dumps({"version": 1, "stages": [{
        "name": "extrinsic", "kind": "indexed-grid-fanout-v1",
        "job": job, "grid": str(seed), "output_file": "samples.dat"}]}))
    (run / "post.args").write_text("1 --parameter m1 --parameter m2\n")
    monkeypatch.chdir(run)
    monkeypatch.setenv("RIFT_DAG_BACKEND", "htcondor")
    monkeypatch.setenv("RIFT_HYPERPIPELINE_FORMAT", "1")
    monkeypatch.setenv("RIFT_REQUIRE_NONWORKER", "HAS_CVMFS_software_igwn_org")
    argv = [str(rift_bin / "create_eos_posterior_pipeline"),
            "--working-directory", str(run), "--input-grid", str(seed),
            "--marg-job-spec-file", str(run / "jobs.json"),
            "--terminal-stage-spec-file", str(run / "terminal.json"),
            "--eos-post-args-list", str(run / "post.args"),
            "--eos-post-exe", "/bin/true", "--n-iterations", "1",
            "--n-samples-per-job", "1", "--eos-post-explode-jobs", "1",
            "--eos-post-explode-jobs-last", "1", "--use-full-submit-paths"]
    monkeypatch.setattr(sys, "argv", argv)
    runpy.run_path(str(rift_bin / "create_eos_posterior_pipeline"), run_name="__main__")
    for name in ("MARG_0.sub", "TERMINAL_extrinsic.sub"):
        submit = run / name
        ads = _ads(submit)
        inputs = ads["transfer_input_files"].split(",")
        assert all(uri in inputs for uri in URIS), inputs
        assert str(run / "local.cache") in inputs
        if outputs in ("omitted", None):
            assert "transfer_output_files" not in ads
        else:
            assert ads["transfer_output_files"] == ",".join(outputs)
        condor_submit = shutil.which("condor_submit")
        if condor_submit:
            out = run / (name + ".classad")
            result = subprocess.run(
                [condor_submit, "-dry-run", str(out), "-append", "macroiteration = 0",
                 "-append", "macroevent = 0", "-append", "macroid = 0",
                 "-append", "macrongroup = 1",
                 str(submit)], capture_output=True, text=True, timeout=20)
            assert result.returncode == 0, result.stdout + result.stderr
            text = out.read_text()
            assert all(uri in text for uri in URIS)
            # Empty RHS and omission both leave TransferOutput absent on this
            # Condor parser. Preserve explicit [] at the submit-text boundary
            # without claiming it suppresses output discovery.
            parsed = _ads(out)
            if outputs in ("omitted", None) or outputs == []:
                assert "transferoutput" not in parsed
            else:
                assert parsed["transferoutput"].strip('"') == ",".join(outputs)
