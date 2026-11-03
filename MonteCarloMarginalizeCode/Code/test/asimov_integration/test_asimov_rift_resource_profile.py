"""scheduler.resource profile: build_dag plans ILE matching and hands it to the builder."""

import configparser
import json
import os
import pathlib
import shutil

import pytest

from test_asimov_rift_build_contract import (
    TRAVIS_INPUTS, _Event, _Logger, _Production, _Repository, _require_htcondor,
    _require_supported_asimov)

yaml = pytest.importorskip("yaml")

WORKLOAD = {"label": "bbh", "n chunk": 40000, "n eff": 20, "sample rate": 4096,
            "q pregrid factor": 2, "ifos": 2, "approximant": "SEOBNRv5PHM", "lmax": 4,
            "time resampling": 32768}
PROFILE = {
    "version": 1,
    "classes": [
        {"label": "fast", "capability": [9.0, None], "memory mb": 24000, "slots": 10},
        {"label": "slow", "capability": [6.0, 9.0], "memory mb": 8000, "slots": 1000},
    ],
    "observations": [
        {"class": "fast", "outcome": "success", "workload": WORKLOAD,
         "draws per point": [60000], "draws per second": [1000], "peak vram mb": 2000, "host memory mb": 6000},
        {"class": "slow", "outcome": "success", "workload": WORKLOAD, "draws per second": [100]},
    ],
}


def _pipe(monkeypatch, tmp_path, resource_profile, manual_args="--n-chunk 40000"):
    _require_supported_asimov()
    _require_htcondor()
    __import__("asimov")
    from RIFT.asimov import rift as rift_module
    from RIFT.asimov.rift import Rift

    repo_category = tmp_path / "repo" / "C01_offline"
    repo_category.mkdir(parents=True)
    shutil.copyfile(TRAVIS_INPUTS / "GW150914.ini", repo_category / "rift-ci.ini")
    coinc_file = tmp_path / "coinc.xml"
    shutil.copyfile(TRAVIS_INPUTS / "coinc.xml", coinc_file)
    event = _Event("GW150914", tmp_path, _Repository(tmp_path / "repo"))
    production = _Production(event=event, name="rift-plan", rundir=tmp_path / "rift-run",
                             ini_relpath="rift-ci.ini", coinc_file=coinc_file)
    production.meta.update({
        "interferometers": ["H1", "L1"],
        "likelihood": {"assume": {}, "sample rate": 4096},
        "sampler": {"ile": {"n eff": 20, "q time pregrid factor": 2, "manual extra args": manual_args}},
    })
    production.meta["scheduler"]["pipeline"]["internal-ile-srate-time-resampling"] = 32768
    production.meta["scheduler"]["resource profile"] = resource_profile
    event.productions.append(production)

    pipe = Rift.__new__(Rift)
    pipe.production, pipe.category, pipe.bootstrap = production, production.category, False
    pipe.logger = _Logger()
    monkeypatch.setattr(Rift, "before_build", lambda self: None)
    values = {("condor", "user"): "rift-ci", ("general", "calibration"): "C01",
              ("general", "calibration_directory"): "C01_offline",
              ("pipelines", "environment"): str(tmp_path / "env"),
              ("rift", "environment"): str(tmp_path / "env")}

    def fake_config_get(section, option):
        if section == "authentication":
            raise configparser.NoSectionError(section)
        return values[(section, option)]

    monkeypatch.setattr(rift_module.config, "get", fake_config_get)
    calls = []
    real_popen = rift_module.subprocess.Popen

    class FakePopen:
        returncode = 0

        def __new__(cls, command, *args, **kwargs):
            if not str(command[0]).endswith("util_RIFT_pseudo_pipe.py"):
                return real_popen(command, *args, **kwargs)
            return super().__new__(cls)

        def __init__(self, command, stdout=None, stderr=None, env=None):
            self.command = command
            calls.append((command, env))

        def communicate(self):
            rundir = pathlib.Path(self.command[self.command.index("--use-rundir") + 1])
            rundir.mkdir(parents=True)
            (rundir / "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag").write_text("# test\n")
            return b"fake builder", None

    monkeypatch.setattr(rift_module.subprocess, "Popen", FakePopen)
    return pipe, production, calls, repo_category


def test_build_dag_applies_resource_plan(monkeypatch, tmp_path):
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    user = '(DeviceName =!= "Tesla K10.G1.8GB")'
    monkeypatch.setenv("RIFT_REQUIRE_GPUS", user)
    pipe, production, calls, repo_category = _pipe(
        monkeypatch, tmp_path, {"path": str(profile), "workload label": "bbh"})
    pipe.build_dag()

    (command, env), = calls
    planned_ini = pathlib.Path(command[command.index("--use-ini") + 1])
    assert planned_ini.parent.parent == repo_category.resolve()
    assert planned_ini.parent.name.startswith(".rift-resource-plan-")
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.optionxform = str
    cfg.read(planned_ini)
    assert cfg["rift-pseudo-pipe"]["ile-jobs-per-worker"] == "11"      # was 30 in the source ini
    assert cfg["rift-pseudo-pipe"]["internal-ile-request-memory"] == "7500"
    plan = json.loads((pathlib.Path(production.rundir) / "resource-plan.json").read_text())
    assert plan["admitted"] == ["fast", "slow"]
    assert env["RIFT_REQUIRE_GPUS"] == "({}) && {}".format(user, plan["require gpus"])
    assert os.environ["RIFT_REQUIRE_GPUS"] == user          # the asimov process is untouched


def test_build_dag_without_profile_keeps_builder_call(monkeypatch, tmp_path):
    pipe, _, calls, repo_category = _pipe(monkeypatch, tmp_path, None)
    pipe.build_dag()
    (command, env), = calls
    assert env is None
    assert command[command.index("--use-ini") + 1] == str((repo_category / "rift-ci.ini").resolve())


@pytest.mark.parametrize("manual_args, settings, match", [
    ("--n-max 100", {}, "found none"),
    ("--n-chunk 40000", {"n chunk": 20000}, r"found \[20000, 40000\]"),
    ("--n-chunk 40000", {"wall": 3}, "unknown keys"),
])
def test_build_dag_rejects_unplannable_ledger(monkeypatch, tmp_path, manual_args, settings, match):
    from asimov.pipeline import PipelineException
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    pipe, _, calls, _ = _pipe(monkeypatch, tmp_path, dict({"path": str(profile)}, **settings),
                              manual_args=manual_args)
    with pytest.raises(PipelineException, match=match):
        pipe.build_dag()
    assert calls == []
