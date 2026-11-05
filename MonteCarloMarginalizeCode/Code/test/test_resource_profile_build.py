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
    "slot1_2@a\tGeForce GTX 1050 Ti\t6.1\t4040\t11.0\tGPU-1, GPU-2",
    "slot1@b\tNVIDIA A100-PCIE-40GB\t8.0\t40441\t13.0\tGPU-3",
    "slot1@c\tNVIDIA A10\t8.6\t22590\t13.2\tGPU-4",
    "slot1@d\tundefined\tundefined\tundefined\tundefined\tundefined",
]
ARGUMENTS = ["--n-chunk", "40000", "--n-chunk", "10000", "--n-eff", "10", "--srate", "4096",
             "--q-time-pregrid-factor", "2", "--channel-name", "H1=X", "--channel-name", "L1=Y",
             "--approx", "IMRPhenomXPHM", "--l-max", "4", "--srate-resample-time-marginalization", "32768",
             "--time-marginalization", "--vectorized", "--gpu", "--force-xpy"]
MANIFEST = {"fallback": "legacy", "containers": [
    {"label": "modern", "image": "osdf:///x/modern.sif", "cuda_capability_min": 9.0, "select_requirements": None},
    {"label": "legacy", "image": "/cvmfs/x/legacy.sif", "cuda_capability_min": 6.1, "select_requirements": None}]}


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


def test_inventory_keeps_memory_range_and_shares_ids_across_pools():
    rows = build.parse_inventory("\n".join(ROWS + ["slot9@z\tNVIDIA A10\t8.6\t22000\t13.2\tGPU-4"]))
    a10 = next(d for d in build.summarize_inventory(rows) if d["name"] == "NVIDIA A10")
    assert (a10["memory mb"], a10["memory mb max"], a10["gpus"]) == (22000, 22590, 1)


def test_default_survey_leaves_out_opt_in_slots():
    assert 'unparse(Start)' in build.DEFAULT_CONSTRAINT and build.DEFAULT_CONSTRAINT.startswith(build.ANY_GPU)
    assert "\\" not in build.OPT_IN_START
    for start in ("TARGET.Online_CBC_IIR_GPU_x2200 is true", "TARGET.Fat_PE =?= true", "false",
                  'TARGET.SGNL_GPU is true && (TARGET.Owner is "sgnl.online")'):
        assert re.search(build.OPT_IN_START, start), start
    for start in ("true", "TARGET.requestgpus >= 1", "TARGET.Benchmarking_Mix isnt true && (x > 0)",
                  "((true && (SiteWMS_WN_Draining is false)) && (true))"):
        assert not re.search(build.OPT_IN_START, start), start


def test_new_classes_match_only_their_own_card(tmp_path):
    data, report = build.update_resources(None, _devices(), MANIFEST)
    assert len(report["added"]) == 3 and report["absent"] == []
    profile = _profile(tmp_path, data)
    assert rp.classify_device(profile, "NVIDIA A10", 8.6) != rp.classify_device(profile, "NVIDIA A100-PCIE-40GB", 8.0)
    assert rp.classify_device(profile, "NVIDIA A10G", 8.6) is None
    a10 = next(c for c in data["classes"] if "A10$" in c["device names"][0])
    assert a10["memory family"] == "legacy" and a10["slots"] == 1
    assert a10["capability"] == [8.6, 8.7] and a10["memory mb"] < 22590 < a10["memory mb max"]
    assert 'regexp("^NVIDIA A10$", DeviceName)' in rp._class_selector(rp._validate_class(a10, "x"))


def test_small_memory_drift_keeps_the_class():
    data, _ = build.update_resources(None, _devices())
    drifted = [dict(d, **{"memory mb": d["memory mb"] + 1, "memory mb max": d["memory mb max"] + 1}) for d in _devices()]
    again, report = build.update_resources(data, drifted)
    assert report["added"] == [] and len(again["classes"]) == 3


def test_a_broad_class_sums_its_devices():
    entry = {"label": "ampere", "capability": [8.0, 9.0], "memory mb": 20000}
    data, _ = build.update_resources({"version": 1, "classes": [entry]}, _devices())
    assert data["classes"][0]["slots"] == 2


# the fallback's own min is not tested: it is only the final else
@pytest.mark.parametrize("capability, expected", [(6.1, "low"), (7.5, "mid"), (8.0, "tie"), (12.0, "tie")])
def test_image_label_follows_manifest_selection(capability, expected):
    manifest = {"fallback": "low", "containers": [
        {"label": "low", "cuda_capability_min": 12.0, "select_requirements": None},
        {"label": "tie", "cuda_capability_min": 8.0, "select_requirements": None},
        {"label": "mid", "cuda_capability_min": 7.0, "select_requirements": None}]}
    assert build._image_label(manifest, capability) == expected


def test_image_label_will_not_guess_a_slot_test():
    manifest = {"fallback": "low", "containers": [
        {"label": "big", "cuda_capability_min": 8.0, "select_requirements": "TARGET.GPUs_GlobalMemoryMb >= 40000"},
        {"label": "low", "cuda_capability_min": 6.0, "select_requirements": None}]}
    assert build._image_label(manifest, 8.0) is None
    assert build._image_label(manifest, 7.0) == "low"


def test_device_name_metacharacters_are_literal():
    pattern = build._exact_name("Card (x) 1.5")
    assert re.search(pattern, "Card (x) 1.5") and not re.search(pattern, "Card (x) 125")
    assert "\\" not in pattern                    # condor would keep both backslashes and match nothing


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


def test_survey_records_the_image_as_the_class_build():
    entry = {"label": "a10", "capability": [8.6, 8.7], "memory mb": 20000, "device names": ["A10"], "slots": 1,
             "memory family": "legacy", "build": "old.sif"}
    data, report = build.update_resources({"version": 1, "classes": [entry]}, _devices(), MANIFEST)
    assert next(c for c in data["classes"] if c["label"] == "a10")["build"] == "/cvmfs/x/legacy.sif"
    assert report["build changed"][0] == "a10: old.sif to /cvmfs/x/legacy.sif" and report["image moved"] == []
    _, report = build.update_resources(data, _devices(), MANIFEST)
    assert report["build changed"] == []


def test_survey_leaves_the_build_when_it_cannot_tell_the_image():
    gated = dict(MANIFEST["containers"][0], cuda_capability_min=6.1, select_requirements="x")
    manifest = {"fallback": "legacy", "containers": [gated, MANIFEST["containers"][1]]}
    entry = {"label": "a10", "capability": [8.6, 8.7], "memory mb": 20000, "device names": ["A10"], "slots": 1,
             "memory family": "legacy", "build": "old.sif"}
    data, report = build.update_resources({"version": 1, "classes": [entry]}, _devices(), manifest)
    assert data["classes"][0]["build"] == "old.sif" and report["build changed"] == []
    assert "a10" in report["image unknown"]


def test_same_file_name_in_a_new_directory_is_a_new_build():
    """build_family.sh gives every build the same file name."""
    entry = {"label": "a10", "capability": [8.6, 8.7], "memory mb": 20000, "device names": ["A10"], "slots": 1,
             "memory family": "legacy", "build": "/cvmfs/rc13/legacy.sif"}
    moved = {"fallback": "legacy", "containers": [dict(MANIFEST["containers"][1], image="/cvmfs/rc14/legacy.sif")]}
    data, report = build.update_resources({"version": 1, "classes": [entry]}, _devices(), moved)
    assert data["classes"][0]["build"] == "/cvmfs/rc14/legacy.sif" and len(report["build changed"]) >= 1


@pytest.mark.parametrize("capability, expected", [
    ([6.1, 7.0], "legacy"),            # below every threshold
    ([9.0, 9.1], "modern"),
    ([7.5, 9.0], "legacy"),            # ends at the threshold
    ([7.5, 10.0], None),               # spans the threshold
    ([7.5, None], None)])
def test_class_container_needs_one_image_for_the_whole_class(capability, expected):
    entry = build.class_container(MANIFEST, {"capability": capability})
    assert (entry and entry["label"]) == expected


def test_class_container_will_not_guess_past_a_capability_max():
    top = dict(MANIFEST["containers"][0], cuda_capability_max=9.5)
    manifest = {"fallback": "legacy", "containers": [top, MANIFEST["containers"][1]]}
    assert build.class_container(manifest, {"capability": [9.0, 9.1]})["label"] == "modern"
    assert build.class_container(manifest, {"capability": [12.0, 12.1]}) is None


def test_rebuild_under_one_family_label_loses_the_old_memory_fit(tmp_path):
    """A resurvey after a rebuild drops the old fit, though the family label never changed."""
    entry = {"label": "a10", "capability": [8.6, 8.7], "memory mb": 20000, "device names": ["NVIDIA A10$"],
             "slots": 1, "memory family": "legacy", "build": "rc13.sif"}
    workload = {"label": "w", "n chunk": 10000, "n eff": 10, "sample rate": 4096, "q pregrid factor": 2,
                "ifos": 2, "approximant": "X", "lmax": 4, "time resampling": 32768}
    grown = [[1e6 * (i + 1), 500.0 + 300 * (i + 1), None] for i in range(8)]
    observation = {"class": "a10", "outcome": "success", "workload": workload, "build": "rc13.sif",
                   "memory samples": grown, "draws per point": [1e6], "draws per second": [1e4]}
    data = {"version": 1, "classes": [entry], "observations": [observation]}
    before = rp.plan_resources(_profile(tmp_path, data), workload)["classes"][0]
    assert before["evidence"]["used"] == 1 and "300" in before["memory"]
    moved, _ = build.update_resources(data, _devices(), {"fallback": "legacy", "containers": [
        {"label": "legacy", "image": "rc14.sif", "cuda_capability_min": 6.1, "select_requirements": None}]})
    moved["classes"] = [c for c in moved["classes"] if c["label"] == "a10"]
    with pytest.raises(rp.ResourceProfileError, match="1 observations of other builds set aside"):
        rp.plan_resources(_profile(tmp_path, moved), workload)


def test_cli_survey_stamps_the_class_and_keeps_observations(tmp_path):
    profile = tmp_path / "profile"
    (profile / "observations").mkdir(parents=True)
    entry = {"label": "a10", "capability": [8.6, 8.7], "memory mb": 20000, "device names": ["A10"], "slots": 1,
             "memory family": "old"}
    (profile / "resources.yaml").write_text(yaml.safe_dump({"version": 1, "classes": [entry]}))
    observation = {"class": "a10", "outcome": "success", "workload": {"n chunk": 1}}
    (profile / "observations" / "a.yaml").write_text(yaml.safe_dump({"observations": [observation]}))
    (tmp_path / "inventory.txt").write_text("\n".join(ROWS))
    (tmp_path / "manifest.yaml").write_text(yaml.safe_dump(MANIFEST))
    env = dict(os.environ, PYTHONPATH=CODE + os.pathsep + os.environ.get("PYTHONPATH", ""))
    done = subprocess.run([sys.executable, os.path.join(CODE, "bin", "util_RIFT_resource_profile.py"), "survey",
                           str(profile), "--inventory", str(tmp_path / "inventory.txt"),
                           "--manifest", str(tmp_path / "manifest.yaml")],
                          env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
    assert done.returncode == 0, done.stderr
    entry = next(c for c in yaml.safe_load((profile / "resources.yaml").read_text())["classes"]
                 if c["label"] == "a10")
    assert entry["memory family"] == "legacy" and entry["build"] == "/cvmfs/x/legacy.sif"


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


def test_relative_speed_will_not_mix_scales(tmp_path):
    workload = build.workload_from_arguments(ARGUMENTS, "x")
    classes = [{"label": "a", "capability": [6.0, 7.0], "memory mb": 1000},
               {"label": "b", "capability": [7.0, 8.0], "memory mb": 1000},
               {"label": "c", "capability": [8.0, 9.0], "memory mb": 1000, "relative speed": 4.0}]
    observations = [{"class": label, "outcome": "success", "workload": workload, "draws per second": [100]}
                    for label in "ab"]
    data = {"version": 1, "classes": classes, "observations": observations}
    assert build.refresh_relative_speeds(data, _profile(tmp_path, data)) == []
    assert "relative speed" not in classes[0]


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


def test_workload_refuses_joined_options():
    with pytest.raises(rp.ResourceProfileError, match="separate words"):
        build.workload_from_arguments(ARGUMENTS + ["--channel-name=V1=Z"], "bbh")


def test_workload_from_rundir_prefers_the_plan(tmp_path):
    # condor quoting: a quoted word sits inside doubled quotes
    (tmp_path / "ILE.sub").write_text('arguments = "{} --approx \'\'Spaced Name\'\'"\n'.format(" ".join(ARGUMENTS)))
    workload = build.workload_from_rundir(str(tmp_path), "bbh")
    assert workload["label"] == "bbh" and workload["n chunk"] == 10000
    assert workload["approximant"] == "Spaced Name"
    (tmp_path / "resource-plan.json").write_text(json.dumps({"workload": {"label": "planned"}}))
    assert build.workload_from_rundir(str(tmp_path))["label"] == "planned"


def test_workload_from_rundir_without_either_file(tmp_path):
    with pytest.raises(rp.ResourceProfileError, match="no ILE.sub"):
        build.workload_from_rundir(str(tmp_path))


def _built(tmp_path, *extra, memory=7500, require="(x)"):
    (tmp_path / "ILE.sub").write_text('arguments = "{}"\nrequest_memory = {}M\nrequire_gpus = ({}) && y\n'.format(
        " ".join(list(ARGUMENTS) + list(extra)), memory, require))
    return {"workload": build.workload_from_arguments(ARGUMENTS, "bbh"), "request memory mb": 7500,
            "require gpus": "(x)"}


def test_check_built_run_accepts_what_was_planned(tmp_path):
    assert build.check_built_run(str(tmp_path), _built(tmp_path)) == []


@pytest.mark.parametrize("extra, kwargs, word", [
    (("--q-time-pregrid-factor", "4"), {}, "q pregrid factor"), (("--n-chunk", "20000"), {}, "n chunk"),
    (("--n-eff", "1000"), {}, "n eff"),
    ((), {"memory": 1000}, "request_memory"), ((), {"require": "other"}, "require_gpus")])
def test_check_built_run_finds_what_another_key_changed(tmp_path, extra, kwargs, word):
    plan = _built(tmp_path, *extra, **kwargs)
    (difference,) = build.check_built_run(str(tmp_path), plan)
    assert word in difference


def test_check_built_run_refuses_jax_ile(tmp_path):
    plan = _built(tmp_path)
    with open(str(tmp_path / "ILE.sub"), "a") as f:
        f.write("executable = /x/integrate_likelihood_extrinsic_jax\n")
    assert "JAX" in build.check_built_run(str(tmp_path), plan)[0]


def test_check_built_run_reads_the_ile_wrapper(tmp_path):
    plan = _built(tmp_path)
    (tmp_path / "ile_pre.sh").write_text('exec "$PY" - "/x/integrate_likelihood_extrinsic_jax" "$@"\n')
    with open(str(tmp_path / "ILE.sub"), "a") as f:
        f.write("executable = ile_pre.sh\n")
    assert "JAX" in build.check_built_run(str(tmp_path), plan)[0]


def test_check_built_run_refuses_a_build_off_noloop(tmp_path):
    plan = _built(tmp_path)
    text = (tmp_path / "ILE.sub").read_text()
    (tmp_path / "ILE.sub").write_text(text.replace(" --force-xpy", ""))
    (difference,) = build.check_built_run(str(tmp_path), plan)
    assert "--force-xpy" in difference


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
    assert "EnteredCurrentStatus > 12600" in text["old"]        # the ILE timeout plus 30 minutes
    assert os.path.exists(os.path.join(os.path.dirname(submits[0]), "case", "local.cache"))


def test_emit_image_overrides_the_manifest(tmp_path):
    _, submits = _emit(tmp_path, manifest=MANIFEST, image="/one.sif")
    assert all("container_image = /one.sif" in open(s).read() for s in submits)


def test_emit_refuses_arguments_collect_could_not_read(tmp_path):
    with pytest.raises(rp.ResourceProfileError, match="lack"):
        build.emit_probes({"classes": []}, str(tmp_path), [a for a in ARGUMENTS if a != "--srate" and a != "4096"],
                          str(tmp_path / "j"), "x")


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


def _receipt(tmp_path, name, code, device="Old", draws=(1000.0, 2000.0, 3000.0), oom=False, finite=True, points=3, noloop_lost=False, image=None):
    directory = tmp_path / "jobs" / name
    (directory / "results").mkdir(parents=True)
    (directory / "probe.json").write_text(json.dumps({"label": "bbh", "points": points, "image": image}))
    (directory / "results" / "receipt.json").write_text(json.dumps({
        "gpu": {"name": device, "sm": 61, "vram_bytes": 4236312576}, "run": {
            "arguments": ARGUMENTS, "returncode": code, "wall_seconds": 1000.0, "gpu_oom": oom, "all_finite": finite,
            "noloop_lost": noloop_lost,
            "points": [{"draws": d, "mtime": 100.0 * (i + 1)} for i, d in enumerate(draws)],
            "sampled_memory": {"gpu_peak_used_mb": 2000.0, "host_peak_rss_kb": 2048000}}}))


def _collect(tmp_path):
    data = {"version": 1, "classes": [
        {"label": "small", "capability": [6.1, 6.2], "memory mb": 1000, "memory mb max": 2000},
        {"label": "old", "capability": [6.1, 6.2], "memory mb": 4000, "device names": ["^Old$"]}]}
    (tmp_path / "profile").mkdir()
    profile = _profile(tmp_path / "profile", data)
    return build.collect_probes(str(tmp_path / "jobs"), profile)


def test_collect_maps_outcomes_and_skips_what_it_cannot_place(tmp_path):
    _receipt(tmp_path, "a-good", 0)
    _receipt(tmp_path, "b-timeout", 124, draws=(1000.0,))
    _receipt(tmp_path, "c-crash", -11)
    _receipt(tmp_path, "d-oom", 1, oom=True)
    _receipt(tmp_path, "e-fail", 1)
    _receipt(tmp_path, "f-stranger", 0, device="Other")
    (tmp_path / "jobs" / "g-empty").mkdir()
    observations, skipped = _collect(tmp_path)
    assert [o["outcome"] for o in observations] == ["success", "success", "crash", "memory-failure", "failure"]
    assert {o["class"] for o in observations} == {"old"}
    good, timeout, crash = observations[:3]
    assert good["memory samples"] == [[6000.0, 2000.0, 2000.0, 1]]
    assert good["draws per second"] == [5000.0 / 200.0]        # first point, and its compilation, left out
    assert "draws per second" not in timeout and timeout["draws per point"] == [1000.0]
    assert crash["failure draws"] == [6000.0] and crash["host memory mb"] == 2000.0
    assert "failure draws" not in observations[4]
    assert len(skipped) == 2
    (tmp_path / "profile" / "observations").mkdir()
    (tmp_path / "profile" / "observations" / "probes.yaml").write_text(yaml.safe_dump({"observations": observations}))
    assert len(rp.load_resource_profile(str(tmp_path / "profile"))["observations"]) == 5


@pytest.mark.parametrize("kwargs", [{"finite": False}, {"points": 4}])
def test_collect_does_not_call_a_bad_run_a_success(tmp_path, kwargs):
    _receipt(tmp_path, "a", 0, **kwargs)
    (observation,), _ = _collect(tmp_path)
    assert observation["outcome"] == "failure" and "draws per second" not in observation


def test_collect_a_gpu_oom_before_any_point_is_a_memory_failure(tmp_path):
    _receipt(tmp_path, "a", 1, draws=(), oom=True)
    (observation,), _ = _collect(tmp_path)
    assert observation["outcome"] == "memory-failure" and "failure draws" not in observation


@pytest.mark.parametrize("kwargs", [{"draws": ()}, {"oom": True}, {"noloop_lost": True}])
def test_collect_a_timeout_needs_clean_points(tmp_path, kwargs):
    _receipt(tmp_path, "a", build.TIMEOUT_RETURNCODE, **kwargs)
    (observation,), _ = _collect(tmp_path)
    assert observation["outcome"] != "success"


@pytest.mark.parametrize("code, kwargs", [(0, {}), (1, {"oom": True}), (-11, {})])
def test_collect_a_run_off_noloop_is_no_evidence(tmp_path, code, kwargs):
    _receipt(tmp_path, "a", code, noloop_lost=True, **kwargs)
    (observation,), _ = _collect(tmp_path)
    assert observation["outcome"] == "failure" and "draws per second" not in observation


def test_collect_oom_text_in_a_complete_run_is_a_success(tmp_path):
    _receipt(tmp_path, "a", 0, oom=True)
    (observation,), _ = _collect(tmp_path)
    assert observation["outcome"] == "success"


def test_collect_a_crash_before_any_point_caps_nothing(tmp_path):
    _receipt(tmp_path, "a", -11, draws=())
    (observation,), _ = _collect(tmp_path)
    assert observation["outcome"] == "failure" and "failure draws" not in observation


def test_collect_drops_a_zero_draw_row_that_would_break_the_profile(tmp_path):
    _receipt(tmp_path, "a", 0, draws=(0.0, 1000.0, 2000.0), points=2)
    (observation,), _ = _collect(tmp_path)
    assert observation["draws per point"] == [1000.0, 2000.0]


def test_collect_needs_the_probe_directory(tmp_path):
    with pytest.raises(rp.ResourceProfileError, match="not found"):
        build.collect_probes(str(tmp_path / "gone"), {"policy": {}})


def _worker():
    spec = importlib.util.spec_from_file_location("worker", build.WORKER)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    return worker


def test_worker_helpers(tmp_path):
    worker = _worker()
    assert worker.set_option(["--a", "1", "--b", "2", "--a", "3"], "--a", 4) == ["--b", "2", "--a", "4"]
    (tmp_path / "CME.xml_0_.dat").write_text(" ".join(["1.0"] * 11) + " 500.0 2.0\n")
    (tmp_path / "CME.xml_1_.dat").write_text(" ".join(["nan"] * 11) + " 700.0 2.0\n# comment\n")
    (tmp_path / "CME.xml_0_cal.dat").write_text(" ".join(["1.0"] * 11) + " 900.0 2.0\n")
    (tmp_path / "CME.xml_2_.dat").write_text(" ".join(["1.0"] * 12) + "\n")          # 12 columns: not a point
    points, finite = worker.read_points(tmp_path)
    assert sorted(p["draws"] for p in points) == [500.0, 700.0] and not finite


@pytest.mark.parametrize("text", ["CUDA_ERROR_OUT_OF_MEMORY", "cupy.cuda.memory.OutOfMemoryError: x", "CUDA out of memory",
                                  "CUFFT_ALLOC_FAILED"])
def test_worker_recognizes_what_the_harvest_recognizes(text):
    assert _worker().OOM.search(text)


STUB = """import os, signal, sys, time
a = sys.argv[1:]
get = lambda k: a[len(a) - 1 - a[::-1].index(k) + 1]
out = get('--output-file')
open(os.path.join(os.path.dirname(out), 'argv.txt'), 'w').write(' '.join(a))
for i in range(int(get('--n-events-to-analyze'))):
    open('{}_{}_.dat'.format(out, i), 'w').write(' '.join(['1.0'] * 11) + ' 500.0 2.0\\n')
    if MODE == 'crash':
        os.kill(os.getpid(), signal.SIGSEGV)
if MODE == 'hang':
    time.sleep(60)
"""


def _run_worker(tmp_path, monkeypatch, mode, timeout=60):
    worker = _worker()
    monkeypatch.chdir(tmp_path)
    (tmp_path / "case").mkdir()
    stub = tmp_path / "ile_stub.py"
    stub.write_text("MODE = {!r}\n".format(mode) + STUB)
    (tmp_path / "probe.json").write_text(json.dumps({
        "label": "bbh", "class": "old", "image": "x", "ile": str(stub), "arguments": ARGUMENTS + ["--event", "7"],
        "points": 2, "event": 40, "n chunk": 5000, "n eff": 12, "n max": None, "timeout": timeout}))
    monkeypatch.setattr(worker, "find_gpu", lambda: ({"pci_bus_id": "x", "name": "Old", "sm": 61, "vram_bytes": 1}, None))
    code = worker.main()
    return code, json.load(open("results/receipt.json"))


def test_worker_main_applies_the_probe_and_records_points(tmp_path, monkeypatch):
    code, receipt = _run_worker(tmp_path, monkeypatch, "ok")
    assert code == 0 and receipt["run"]["returncode"] == 0 and len(receipt["run"]["points"]) == 2
    argv = (tmp_path / "results" / "ile" / "argv.txt").read_text().split()
    assert " ".join(argv).count("--event ") == 1 and argv[argv.index("--event") + 1] == "40"
    assert argv[argv.index("--n-events-to-analyze") + 1] == "2"
    assert argv[argv.index("--n-chunk") + 1] == "5000" and argv[argv.index("--n-eff") + 1] == "12"
    assert all(flag in argv for flag in build.NOLOOP_FLAGS)
    assert (tmp_path / "results" / "ile" / "memory.jsonl").exists()


def test_worker_main_reports_a_crash_with_its_finished_points(tmp_path, monkeypatch):
    code, receipt = _run_worker(tmp_path, monkeypatch, "crash")
    assert code == 4 and receipt["run"]["returncode"] == -11 and len(receipt["run"]["points"]) == 1


def test_worker_main_timeout_uses_the_code_collect_expects(tmp_path, monkeypatch):
    code, receipt = _run_worker(tmp_path, monkeypatch, "hang", timeout=3)
    assert code == 4 and receipt["run"]["returncode"] == build.TIMEOUT_RETURNCODE


def test_worker_main_records_a_missing_gpu(tmp_path, monkeypatch):
    worker = _worker()
    monkeypatch.chdir(tmp_path)
    (tmp_path / "probe.json").write_text("{}")
    monkeypatch.setattr(worker, "find_gpu", lambda: (None, "no device"))
    assert worker.main() == 3
    assert json.load(open("results/receipt.json"))["gpu_probe_error"] == "no device"


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
    previous = tmp_path / "profile" / "resources.yaml.previous"
    assert previous.read_text() == (tmp_path / "profile" / "resources.yaml").read_text()


def test_cli_emit_passes_an_integer_n_eff_to_ile(tmp_path):
    (tmp_path / "inventory.txt").write_text("\n".join(ROWS))
    (tmp_path / "case").mkdir()
    (tmp_path / "ile.args").write_text(" ".join(ARGUMENTS))
    env = dict(os.environ, PYTHONPATH=CODE + os.pathsep + os.environ.get("PYTHONPATH", ""))
    cli = [sys.executable, os.path.join(CODE, "bin", "util_RIFT_resource_profile.py")]
    subprocess.run(cli + ["survey", str(tmp_path / "p"), "--inventory", str(tmp_path / "inventory.txt")],
                   env=env, stdout=subprocess.PIPE, check=True)
    subprocess.run(cli + ["emit-probes", str(tmp_path / "p"), "--case", str(tmp_path / "case"), "--arguments",
                          str(tmp_path / "ile.args"), "--label", "x", "--out", str(tmp_path / "j"), "--image", "i.sif",
                          "--n-eff", "10"], env=env, stdout=subprocess.PIPE, check=True)
    probe = json.load(open(str(next((tmp_path / "j").glob("*/probe.json")))))
    assert str(probe["n eff"]) == "10"
    output = tmp_path / "p" / "observations" / "probes.yaml"  # a new profile has no such directory
    subprocess.run(cli + ["collect-probes", str(tmp_path / "p"), "--probes", str(tmp_path / "j"), "--output",
                          str(output)], env=env, stdout=subprocess.PIPE, check=True)
    assert output.exists()


def test_collect_stamps_the_image_or_the_given_build(tmp_path):
    _receipt(tmp_path, "a", 0, image="osdf:///x/rc14.sif")
    _receipt(tmp_path, "b", 0)
    profile = {"policy": {}, "classes": [{"label": "old", "capability": [6.1, 6.2], "memory mb": 4000,
                                          "device names": ["^Old$"]}]}
    observations, _ = build.collect_probes(str(tmp_path / "jobs"), profile)
    assert [o.get("build") for o in observations] == ["osdf:///x/rc14.sif", None]
    observations, _ = build.collect_probes(str(tmp_path / "jobs"), profile, stamp="rc14+fix")
    assert {o["build"] for o in observations} == {"rc14+fix"}


@pytest.mark.parametrize("lines, expected", [
    (['container_image = /x/rc14.sif'], "/x/rc14.sif"),
    (['MY.SingularityImage = "osdf:///x/rc14.sif"'], "osdf:///x/rc14.sif"),
    (['+SingularityImage = "/x/rc14.sif"'], "/x/rc14.sif"),
    (['#MY.SingularityImage = "/x/old.sif"', 'container_image = /x/rc14.sif'], "/x/rc14.sif"),
    (['container_image = /x/a.sif', 'MY.SingularityImage = "/x/b.sif"'], None),
    (['container_image = $$([ifThenElse(x, "a.sif", "b.sif")])'], None),
    (['MY.SingularityImage = ifThenElse(x, "a.sif", "b.sif")'], None),
    (['universe = vanilla'], None)])
def test_run_builds_reads_one_literal_image(tmp_path, lines, expected):
    (tmp_path / "ILE.sub").write_text('arguments = "x"\n' + "\n".join(lines) + "\n")
    assert build.run_builds(str(tmp_path)) == expected


def test_run_builds_prefers_the_override_then_the_image_then_the_plan(tmp_path):
    sub = tmp_path / "ILE.sub"
    sub.write_text('arguments = "x"\ncontainer_image = /x/sub.sif\n')
    plan = tmp_path / "resource-plan.json"
    plan.write_text(json.dumps({"classes": [{"label": "a", "build": "plan.sif"}]}))
    assert build.run_builds(str(tmp_path), "mine") == "mine"
    assert build.run_builds(str(tmp_path)) == "/x/sub.sif"
    sub.write_text('arguments = "x"\ncontainer_image = $$([x ? "a" : "b"])\n')
    assert build.run_builds(str(tmp_path)) == {"a": "plan.sif"}
    sub.write_text('arguments = "x"\n')                     # a bare checkout ran no image
    assert build.run_builds(str(tmp_path)) is None
    assert build.run_builds(str(tmp_path / "none")) is None


def _plan_with_builds(plan, *rows):
    plan.update({"admitted": ["a", "b"], "classes": list(rows) + [{"label": "out", "build": "gone.sif"}]})
    return plan


@pytest.mark.parametrize("built, refused", [("/x/rc14.sif", False), ("/y/rc14.sif", True)])
def test_literal_image_must_be_the_planned_build(tmp_path, built, refused):
    plan = _plan_with_builds(_built(tmp_path), {"label": "a", "build": "/x/rc14.sif"}, {"label": "b", "build": None})
    with open(str(tmp_path / "ILE.sub"), "a") as f:
        f.write("container_image = {}\n".format(built))
    assert bool(build.check_built_run(str(tmp_path), plan)) is refused


def test_family_run_checks_each_class_against_its_own_branch(tmp_path):
    plan = _built(tmp_path)
    with open(str(tmp_path / "ILE.sub"), "a") as f:
        f.write('container_image = $$([x ? "modern.sif" : "legacy.sif"])\n')
    right = {"label": "a", "capability": [9.0, 9.1], "build": "osdf:///x/modern.sif"}
    wrong = {"label": "b", "capability": [8.6, 8.7], "build": "osdf:///x/modern.sif"}
    assert build.check_built_run(str(tmp_path), _plan_with_builds(plan, right), MANIFEST) == []
    found = build.check_built_run(str(tmp_path), _plan_with_builds(plan, right, wrong), MANIFEST)
    assert len(found) == 1 and "built /cvmfs/x/legacy.sif" in found[0]
    assert "cannot be told" in build.check_built_run(str(tmp_path), _plan_with_builds(plan, right))[0]


def test_family_run_from_another_manifest_is_refused(tmp_path):
    plan = _plan_with_builds(_built(tmp_path), {"label": "a", "capability": [9.0, 9.1], "build": "osdf:///x/modern.sif"})
    with open(str(tmp_path / "ILE.sub"), "a") as f:
        f.write('container_image = $$([x ? "other.sif" : "legacy.sif"])\n')
    assert "cannot be told" in build.check_built_run(str(tmp_path), plan, MANIFEST)[0]


@pytest.mark.parametrize("url", ["osdf:///x/r.sif", "igwn+osdf:///x/r.sif"])
def test_transferred_image_is_known_by_its_source(tmp_path, url):
    plan = _plan_with_builds(_built(tmp_path), {"label": "a", "build": url})
    with open(str(tmp_path / "ILE.sub"), "a") as f:
        f.write('MY.SingularityImage = "./r.sif"\ntransfer_input_files = a.txt,{},b.txt\n'.format(url))
    assert build.check_built_run(str(tmp_path), plan) == []
    assert build.run_builds(str(tmp_path)) == url
    plan["classes"][0]["build"] = "osdf:///old/r.sif"
    assert build.check_built_run(str(tmp_path), plan)


def test_class_container_splits_a_class_at_a_capability_max():
    top = {"label": "B", "image": "/b.sif", "cuda_capability_min": 7.0, "cuda_capability_max": 8.0,
           "select_requirements": None}
    fallback = {"label": "A", "image": "/a.sif", "cuda_capability_min": 6.0, "select_requirements": None}
    manifest = {"fallback": "A", "containers": [top, fallback]}
    assert build.class_container(manifest, {"capability": [7.0, 7.5]})["label"] == "B"
    assert build.class_container(manifest, {"capability": [7.0, 9.0]}) is None
    assert build.class_container(manifest, {"capability": [7.0, None]}) is None


def test_runtime_wrapper_counts_as_a_container(tmp_path):
    plan = _plan_with_builds(_built(tmp_path), {"label": "a", "capability": [9.0, 9.1], "build": "osdf:///x/modern.sif"})
    (tmp_path / "rift_container_select.sh").write_text("#!/bin/sh\nIMAGE=modern.sif\n")
    with open(str(tmp_path / "ILE.sub"), "a") as f:
        f.write("executable = rift_container_select.sh\n")
    assert build.check_built_run(str(tmp_path), plan, MANIFEST) == []


def test_bare_checkout_is_not_the_planned_image(tmp_path):
    plan = _plan_with_builds(_built(tmp_path), {"label": "a", "build": "/x/rc14.sif"})
    assert "no container" in build.check_built_run(str(tmp_path), plan)[0]


def test_cli_build_option_reaches_harvest_and_collect(tmp_path):
    (tmp_path / "profile.yaml").write_text(yaml.safe_dump({"version": 1, "classes": [
        {"label": "old", "capability": [6.1, 6.2], "memory mb": 4000, "device names": ["^Old$"]}]}))
    _receipt(tmp_path, "a", 0)
    cli = [sys.executable, os.path.join(CODE, "bin", "util_RIFT_resource_profile.py")]
    env = dict(os.environ, PYTHONPATH=CODE + os.pathsep + os.environ.get("PYTHONPATH", ""))
    done = subprocess.run(cli + ["collect-probes", str(tmp_path / "profile.yaml"), "--probes", str(tmp_path / "jobs"),
                                 "--build", "mine"],
                          env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert done.returncode == 0, done.stderr
    assert yaml.safe_load(done.stdout)["observations"][0]["build"] == "mine"
    seen = {}
    spec = importlib.util.spec_from_file_location("cli", cli[1])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.resource_profile.harvest_rundir, keep = lambda *a, **k: (seen.update(k) or ([], {})), module.resource_profile.harvest_rundir
    try:
        (tmp_path / "w.yaml").write_text(yaml.safe_dump({"n chunk": 1}))
        assert module.main(["harvest", str(tmp_path / "profile.yaml"), str(tmp_path), "--workload",
                            str(tmp_path / "w.yaml"), "--build", "mine"]) == 0
    finally:
        module.resource_profile.harvest_rundir = keep
    assert seen["builds"] == "mine"
