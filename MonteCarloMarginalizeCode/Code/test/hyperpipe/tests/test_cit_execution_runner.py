from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


HERE = Path(__file__).resolve()
RUNNER = HERE.parents[1] / "production_build" / "run_cit_execution_gate.py"


def _runner_module():
    spec = importlib.util.spec_from_file_location("cit_execution_gate", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_output_guard_rejects_production_tree():
    runner = _runner_module()
    with pytest.raises(ValueError, match="output beneath /home/pe.o4"):
        runner._validate_location(Path("/home/pe.o4/GWTC5-HLV/project"))


def test_unique_output_never_reuses_directory(tmp_path):
    runner = _runner_module()
    first = runner._unique_run_dir(tmp_path, "profile", "a" * 40)
    second = runner._unique_run_dir(tmp_path, "profile", "a" * 40)
    assert first != second
    assert first.is_dir() and second.is_dir()


def test_scheduler_guards_fail_closed(tmp_path, monkeypatch):
    runner = _runner_module()
    monkeypatch.setenv("PATH", "/usr/bin")
    log = runner._install_scheduler_guards(tmp_path)
    result = runner.subprocess.run(
        ["condor_submit_dag", "workflow.dag"], text=True,
        stdout=runner.subprocess.PIPE, stderr=runner.subprocess.STDOUT)
    assert result.returncode == 97
    assert "blocked scheduler command" in result.stdout
    assert "condor_submit_dag workflow.dag" in log.read_text()
