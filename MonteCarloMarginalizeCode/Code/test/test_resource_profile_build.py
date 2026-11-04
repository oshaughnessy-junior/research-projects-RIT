#!/usr/bin/env python3
"""RIFT.misc.resource_profile_build: survey, planless workloads, probe emit and collect."""

import importlib.util
import json
import os
import re
import subprocess
import sys

import pytest

yaml = pytest.importorskip("yaml")
from RIFT.misc import resource_profile as rp
from RIFT.misc import resource_profile_build as build

CODE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIELDS = len(build.SURVEY_FIELDS)
ROWS = [
    "slot1@a\tGeForce GTX 1050 Ti\t6.1\t4040\t11.0\tGPU-1, GPU-2",
    "slot1_1@a\tGeForce GTX 1050 Ti\t6.1\t4040\t11.0\tGPU-1, GPU-2",
    "slot1@b\tNVIDIA A100-PCIE-40GB\t8.0\t40441\t13.0\tGPU-3",
    "slot1@c\tNVIDIA A10\t8.6\t22590\t13.2\tGPU-4",
    "slot1@d\tundefined\tundefined\tundefined\tundefined\tundefined",
]
ARGUMENTS = ["--n-chunk", "40000", "--n-chunk", "10000", "--n-eff", "10", "--srate", "4096",
             "--q-time-pregrid-factor", "2", "--channel-name", "H1=X", "--channel-name", "L1=Y",
             "--approx", "IMRPhenomXPHM", "--l-max", "4", "--srate-resample-time-marginalization", "32768",
             "--time-marginalization", "--vectorized", "--gpu", "--force-xpy"]
MANIFEST = {"fallback": "legacy", "containers": [
    {"label": "modern", "image": "osdf:///x/modern.sif", "cuda_capability_min": 9.0},
    {"label": "legacy", "image": "/cvmfs/x/legacy.sif", "cuda_capability_min": 6.1}]}


def _devices():
    return build.summarize_inventory(build.parse_inventory("\n".join(ROWS)))


def _profile(tmp_path, data):
    (tmp_path / "resources.yaml").write_text(yaml.safe_dump(data))
    return rp.load_resource_profile(str(tmp_path))


def test_survey_counts_gpus_not_slots():
    devices = {d["name"]: d for d in _devices()}
    assert sorted(devices) == ["GeForce GTX 1050 Ti", "NVIDIA A10", "NVIDIA A100-PCIE-40GB"]
    assert devices["GeForce GTX 1050 Ti"]["gpus"] == 2
    assert devices["NVIDIA A10"]["drivers"] == [13.2]


def test_new_classes_match_only_their_own_card(tmp_path):
    data, report = build.update_resources(None, _devices(), MANIFEST)
    assert len(report["added"]) == 3 and report["absent"] == []
    profile = _profile(tmp_path, data)
    assert rp.classify_device(profile, "NVIDIA A10", 8.6) != rp.classify_device(profile, "NVIDIA A100-PCIE-40GB", 8.0)
    assert rp.classify_device(profile, "NVIDIA A10G", 8.6) is None
    a10 = next(c for c in data["classes"] if "A10$" in c["device names"][0])
    assert a10["memory family"] == "legacy" and a10["slots"] == 1
    assert 'regexp("^NVIDIA A10$", DeviceName)' in rp._class_selector(rp._validate_class(a10, "x"))


def test_device_name_metacharacters_are_literal():
    pattern = build._exact_name("Card (x) 1.5")
    assert re.search(pattern, "Card (x) 1.5") and not re.search(pattern, "Card (x) 125")


def test_survey_keeps_absent_classes_and_their_slots():
    kept = {"label": "kept", "capability": [7.0, 7.1], "memory mb": 16000, "slots": 44, "memory family": "old",
            "relative speed": 1.2, "device names": ["V100"]}
    data, report = build.update_resources({"version": 1, "policy": {"timing quantile": 0.8}, "classes": [kept]},
                                          _devices(), MANIFEST)
    entry = data["classes"][0]
    assert report["absent"] == ["kept"] and entry["slots"] == 44 and entry["relative speed"] == 1.2
    assert entry["memory family"] == "legacy"              # families must not split on absence
    assert report["image moved"] == ["kept: old to legacy"]
    assert data["policy"] == {"timing quantile": 0.8}


def test_survey_updates_a_described_class_unless_told_to_keep():
    entry = {"label": "pascal", "capability": [6.1, 7.0], "memory mb": 3900, "slots": 194, "device names": ["1050"]}
    data, report = build.update_resources({"version": 1, "classes": [entry]}, _devices())
    assert data["classes"][0]["slots"] == 2 and "pascal" not in report["added"]
    data, _ = build.update_resources({"version": 1, "classes": [entry]}, _devices(), keep_slots=True)
    assert data["classes"][0]["slots"] == 194


def test_label_collision_gets_a_new_label():
    entry = {"label": "a10", "capability": [8.6, 8.7], "memory mb": 99999}
    data, report = build.update_resources({"version": 1, "classes": [entry]}, _devices())
    assert "a10-2" in report["added"]


def test_relative_speed_keeps_the_existing_scale(tmp_path):
    workload = build.workload_from_arguments(ARGUMENTS, "x")
    classes = [{"label": "a", "capability": [6.0, 7.0], "memory mb": 1000, "relative speed": 2.0},
               {"label": "b", "capability": [7.0, 8.0], "memory mb": 1000},
               {"label": "c", "capability": [8.0, 9.0], "memory mb": 1000, "relative speed": 9.0}]
    observations = [{"class": "a", "outcome": "success", "workload": workload, "draws per second": [100, 100, 100]},
                    {"class": "b", "outcome": "success", "workload": workload, "draws per second": [50]}]
    data = {"version": 1, "classes": classes, "observations": observations}
    assert build.refresh_relative_speeds(data, _profile(tmp_path, data)) == ["a", "b"]
    assert [c["relative speed"] for c in classes if "relative speed" in c] == [2.0, 1.0, 9.0]


def test_workload_from_arguments_last_value_wins():
    workload = rp.normalize_workload(build.workload_from_arguments(ARGUMENTS, "bbh"))
    assert workload["n chunk"] == 10000 and workload["ifos"] == 2 and workload["q pregrid factor"] == 2
    assert workload["time resampling"] == 32768


def test_workload_defaults_and_missing_options():
    short = ["--n-chunk", "4e4", "--srate", "4096", "--approx", "X"]
    workload = rp.normalize_workload(build.workload_from_arguments(short, "bbh"))
    assert workload["n chunk"] == 40000 and workload["q pregrid factor"] == 1 and workload["time resampling"] is None
    with pytest.raises(rp.ResourceProfileError, match="lack"):
        build.workload_from_arguments(["--srate", "4096"], "bbh")


def test_workload_from_rundir_prefers_the_plan(tmp_path):
    (tmp_path / "ILE.sub").write_text('arguments = "{} --internal-waveform-extra-lalsuite-args \'\'{{1: 2}}\'\'"\n'.format(
        " ".join(ARGUMENTS)))
    workload = build.workload_from_rundir(str(tmp_path), "bbh")
    assert workload["label"] == "bbh" and workload["n chunk"] == 10000
    (tmp_path / "resource-plan.json").write_text(json.dumps({"workload": {"label": "planned"}}))
    assert build.workload_from_rundir(str(tmp_path))["label"] == "planned"


def test_workload_from_rundir_without_either_file(tmp_path):
    with pytest.raises(rp.ResourceProfileError, match="no resource-plan.json"):
        build.workload_from_rundir(str(tmp_path))


def _emit(tmp_path, **kwargs):
    case = tmp_path / "input"
    case.mkdir()
    (case / "local.cache").write_text("x")
    data = {"version": 1, "classes": [
        {"label": "old", "capability": [6.1, 6.2], "memory mb": 4000, "device names": ["^Old$"]},
        {"label": "new", "capability": [12.0, 12.1], "memory mb": 24000}]}
    profile_dir = tmp_path / "profile"
    profile_dir.mkdir()
    profile = _profile(profile_dir, data)
    return profile, build.emit_probes(profile, str(case), ARGUMENTS, str(tmp_path / "jobs"), "bbh", **kwargs)


def test_emit_writes_one_job_per_class_on_its_own_image(tmp_path):
    _, submits = _emit(tmp_path, manifest=MANIFEST, points=3, n_chunk=10000, pools="IGWN,CIT")
    text = {os.path.basename(os.path.dirname(s)): open(s).read() for s in submits}
    assert "container_image = /cvmfs/x/legacy.sif" in text["old"] and "use_oauth_services" not in text["old"]
    assert "container_image = osdf:///x/modern.sif" in text["new"] and "use_oauth_services = scitokens" in text["new"]
    assert 'regexp("^Old$", DeviceName)' in text["old"]
    assert 'MY.POOLS = "IGWN,CIT"' in text["new"] and "getenv = False" in text["new"]
    assert not re.search("x2200", "".join(text.values()), re.I)
    probe = json.load(open(os.path.join(os.path.dirname(submits[0]), "probe.json")))
    assert probe["points"] == 3 and probe["n chunk"] == 10000
    assert os.path.exists(os.path.join(os.path.dirname(submits[0]), "case", "local.cache"))


@pytest.mark.parametrize("kwargs, match", [({}, "image or a manifest"),
                                           ({"image": "x.sif", "classes": ["gone"]}, "no such classes")])
def test_emit_rejects_bad_requests(tmp_path, kwargs, match):
    with pytest.raises(rp.ResourceProfileError, match=match):
        _emit(tmp_path, **kwargs)


@pytest.mark.parametrize("flag", build.NOLOOP_FLAGS)
def test_emit_refuses_a_command_line_off_noloop(tmp_path, flag):
    profile = {"classes": []}
    with pytest.raises(rp.ResourceProfileError, match="NoLoop"):
        build.emit_probes(profile, str(tmp_path), [a for a in ARGUMENTS if a != flag], str(tmp_path / "j"), "x")


def _receipt(tmp_path, name, code, device="Old", draws=(1000.0, 2000.0), oom=False, wall=1000.0):
    directory = tmp_path / "jobs" / name
    (directory / "results").mkdir(parents=True)
    (directory / "probe.json").write_text(json.dumps({"label": "bbh"}))
    (directory / "results" / "receipt.json").write_text(json.dumps({
        "gpu": {"name": device, "sm": 61}, "run": {
            "arguments": ARGUMENTS, "returncode": code, "wall_seconds": wall, "gpu_oom": oom,
            "points": [{"draws": d} for d in draws],
            "sampled_memory": {"gpu_peak_used_mb": 2000.0, "host_peak_rss_kb": 2048000}}}))


def test_collect_maps_outcomes_and_skips_what_it_cannot_place(tmp_path):
    data = {"version": 1, "classes": [{"label": "old", "capability": [6.1, 6.2], "memory mb": 4000,
                                       "device names": ["^Old$"]}]}
    (tmp_path / "profile").mkdir()
    profile = _profile(tmp_path / "profile", data)
    _receipt(tmp_path, "a-good", 0)
    _receipt(tmp_path, "b-timeout", 124)
    _receipt(tmp_path, "c-crash", -11)
    _receipt(tmp_path, "d-oom", 1, oom=True)
    _receipt(tmp_path, "e-fail", 1)
    _receipt(tmp_path, "f-stranger", 0, device="Other")
    (tmp_path / "jobs" / "g-empty").mkdir()
    observations, skipped = build.collect_probes(str(tmp_path / "jobs"), profile)
    assert [o["outcome"] for o in observations] == ["success", "success", "crash", "memory-failure", "failure"]
    good, timeout, crash = observations[:3]
    assert good["memory samples"] == [[3000.0, 2000.0, 2000.0, 1]]
    assert good["draws per second"] == [3000.0 / 700.0]        # wall less the 300 s policy setup
    assert "draws per second" not in timeout and timeout["draws per point"] == [1000.0, 2000.0]
    assert crash["failure draws"] == [3000.0]
    assert len(skipped) == 2
    (tmp_path / "profile" / "observations").mkdir()
    (tmp_path / "profile" / "observations" / "probes.yaml").write_text(yaml.safe_dump({"observations": observations}))
    assert len(rp.load_resource_profile(str(tmp_path / "profile"))["observations"]) == 5


def test_worker_helpers(tmp_path):
    spec = importlib.util.spec_from_file_location("worker", build.WORKER)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    assert worker.set_option(["--a", "1", "--b", "2", "--a", "3"], "--a", 4) == ["--b", "2", "--a", "4"]
    (tmp_path / "CME.xml_0_.dat").write_text(" ".join(["1.0"] * 11) + " 500.0 2.0\n")
    (tmp_path / "CME.xml_1_.dat").write_text(" ".join(["nan"] * 11) + " 700.0 2.0\n# comment\n")
    points, finite = worker.read_points(tmp_path)
    assert sorted(p["draws"] for p in points) == [500.0, 700.0] and not finite


def test_cli_survey_from_a_saved_inventory(tmp_path):
    (tmp_path / "inventory.txt").write_text("\n".join(ROWS))
    env = dict(os.environ, PYTHONPATH=CODE + os.pathsep + os.environ.get("PYTHONPATH", ""))
    command = [sys.executable, os.path.join(CODE, "bin", "util_RIFT_resource_profile.py"), "survey",
               str(tmp_path / "profile"), "--inventory", str(tmp_path / "inventory.txt")]
    done = subprocess.run(command, env=env, stdout=subprocess.PIPE, universal_newlines=True, check=True)
    assert len(json.loads(done.stdout)["added"]) == 3
    assert len(rp.load_resource_profile(str(tmp_path / "profile"))["classes"]) == 3
    subprocess.run(command, env=env, stdout=subprocess.PIPE, check=True)      # a second survey is stable
    assert len(rp.load_resource_profile(str(tmp_path / "profile"))["classes"]) == 3
    assert (tmp_path / "profile" / "resources.yaml.previous").exists()
