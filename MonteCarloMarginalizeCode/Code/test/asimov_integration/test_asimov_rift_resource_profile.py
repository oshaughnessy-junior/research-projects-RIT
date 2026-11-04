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
FLAT = [[1e6 * (i + 1), 1000.0, None] for i in range(8)]
PROFILE = {
    "version": 1,
    "classes": [
        {"label": "fast", "capability": [9.0, None], "memory mb": 24000, "slots": 10},
        {"label": "slow", "capability": [6.0, 9.0], "memory mb": 8000, "slots": 1000},
    ],
    "observations": [
        {"class": "fast", "outcome": "success", "workload": WORKLOAD,
         "draws per point": [60000], "draws per second": [1000], "host memory mb": 6000, "memory samples": FLAT},
        {"class": "slow", "outcome": "success", "workload": WORKLOAD, "draws per second": [100], "memory samples": FLAT},
    ],
}


ILE_SUB = """arguments = "--n-chunk {n_chunk} --n-eff 20 --srate 4096 --q-time-pregrid-factor {q} --channel-name H1=a --channel-name L1=b --approx SEOBNRv5PHM --l-max 4 --srate-resample-time-marginalization 32768"
request_memory = {memory}M
require_gpus = {require}
"""
built_q = {"value": 2}


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
            self.command, self.env = command, env
            calls.append((command, env))

        def communicate(self):
            rundir = pathlib.Path(self.command[self.command.index("--use-rundir") + 1])
            rundir.mkdir(parents=True)
            (rundir / "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag").write_text("# test\n")
            cfg = configparser.ConfigParser(interpolation=None)
            cfg.optionxform = str
            cfg.read(self.command[self.command.index("--use-ini") + 1])
            pipe_ini = cfg["rift-pseudo-pipe"] if cfg.has_section("rift-pseudo-pipe") else {}
            (rundir / "ILE.sub").write_text(ILE_SUB.format(
                n_chunk=pipe_ini.get("internal-ile-n-chunk", 40000), q=built_q["value"],
                memory=pipe_ini.get("internal-ile-request-memory", 1000),
                require=(self.env or {}).get("RIFT_REQUIRE_GPUS", "")))
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
    assert cfg["rift-pseudo-pipe"]["ile-jobs-per-worker-first"] == "11"
    assert cfg["rift-pseudo-pipe"]["internal-ile-n-chunk"] == "40000"
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
    ("--n-chunk 4e4", {}, "must be an integer"),
])
def test_build_dag_rejects_unplannable_ledger(monkeypatch, tmp_path, manual_args, settings, match):
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    pipe, _, calls, _ = _pipe(monkeypatch, tmp_path, dict({"path": str(profile)}, **settings),
                              manual_args=manual_args)
    from asimov.pipeline import PipelineException     # after _pipe's skip guards
    with pytest.raises(PipelineException, match=match):
        pipe.build_dag()
    assert calls == []


def test_profile_n_chunk_reaches_the_ini(monkeypatch, tmp_path):
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    pipe, _, calls, _ = _pipe(monkeypatch, tmp_path, {"path": str(profile), "workload label": "bbh",
                                                      "n chunk": 40000}, manual_args="")
    pipe.build_dag()
    (command, _), = calls
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.optionxform = str
    cfg.read(command[command.index("--use-ini") + 1])
    assert cfg["rift-pseudo-pipe"]["internal-ile-n-chunk"] == "40000"


@pytest.mark.parametrize("key", ["request_memory", "require_gpus"])
def test_build_dag_refuses_condor_overrides(monkeypatch, tmp_path, key):
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    pipe, production, calls, _ = _pipe(monkeypatch, tmp_path, {"path": str(profile), "workload label": "bbh"})
    from asimov.pipeline import PipelineException     # after _pipe's skip guards
    production.meta["sampler"]["ile"]["condor commands"] = {key: "1"}
    with pytest.raises(PipelineException, match="override the plan"):
        pipe.build_dag()
    assert calls == []


@pytest.mark.parametrize("key", ["MY.RequestMemory", "+RequireGPUs", "request_memory"])
def test_build_dag_refuses_each_spelling_of_an_override(monkeypatch, tmp_path, key):
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    pipe, production, calls, _ = _pipe(monkeypatch, tmp_path, {"path": str(profile), "workload label": "bbh"})
    from asimov.pipeline import PipelineException     # after _pipe's skip guards
    production.meta["sampler"]["ile"]["condor commands"] = {key: "1"}
    with pytest.raises(PipelineException, match="override the plan"):
        pipe.build_dag()
    assert calls == []


def test_build_dag_refuses_a_run_that_does_not_match_the_plan(monkeypatch, tmp_path):
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    pipe, _, _, _ = _pipe(monkeypatch, tmp_path, {"path": str(profile), "workload label": "bbh"})
    from asimov.pipeline import PipelineException     # after _pipe's skip guards
    monkeypatch.setitem(built_q, "value", 4)
    with pytest.raises(PipelineException, match="q pregrid factor"):
        pipe.build_dag()


def test_harvest_needs_a_directory_profile_before_the_build(monkeypatch, tmp_path):
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(PROFILE))
    pipe, _, calls, _ = _pipe(monkeypatch, tmp_path, {"path": str(profile), "workload label": "bbh", "harvest": True})
    from asimov.pipeline import PipelineException     # after _pipe's skip guards
    with pytest.raises(PipelineException, match="directory profile"):
        pipe.build_dag()
    assert calls == []


def test_completion_harvest_writes_one_whole_file_and_skips_an_empty_one(monkeypatch, tmp_path):
    root = tmp_path / "profile"
    root.mkdir()
    (root / "resources.yaml").write_text(yaml.safe_dump(PROFILE))
    pipe, production, _, _ = _pipe(monkeypatch, tmp_path, {"path": str(root), "workload label": "bbh", "harvest": True})
    pipe.build_dag()
    from RIFT.misc import resource_profile
    observation = {"class": "fast", "outcome": "success", "workload": WORKLOAD}
    monkeypatch.setattr(resource_profile, "harvest_rundir", lambda *a: ([], {}))
    pipe._harvest_resource_observations()
    assert not (root / "observations").exists() or not list((root / "observations").iterdir())
    monkeypatch.setattr(resource_profile, "harvest_rundir", lambda *a: ([observation], {}))
    pipe._harvest_resource_observations()
    (written,) = (root / "observations").iterdir()
    assert written.suffix == ".yaml"
    assert len(resource_profile.load_resource_profile(str(root))["observations"]) == 3
