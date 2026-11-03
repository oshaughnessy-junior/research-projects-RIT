#!/usr/bin/env python3
"""RIFT.misc.resource_profile: profile loading, ILE resource planning, ini rewrite, harvest."""

import configparser
import os

import pytest

yaml = pytest.importorskip("yaml")
from RIFT.misc import resource_profile as rp

WORKLOAD = {"label": "bbh", "n chunk": 40000, "n eff": 20, "sample rate": 4096,
            "q pregrid factor": 2, "ifos": 2, "approximant": "IMRPhenomXPHM", "lmax": 4,
            "time resampling": 32768}

CLASSES = [
    {"label": "pascal-4gb", "capability": [6.1, 7.0], "memory mb": 4000, "memory mb max": 8191,
     "slots": 400, "relative speed": 0.3},
    {"label": "legacy-16gb", "capability": [7.0, 9.0], "memory mb": 16000, "slots": 40,
     "relative speed": 1.0, "device names": ["V100", "T4", "A100"]},
    {"label": "modern-24gb", "capability": [9.0, 12.1], "memory mb": 24000, "slots": 10,
     "relative speed": 2.0},
]


def _write_profile(tmp_path, observations=(), policy=None, classes=CLASSES, directory=False):
    data = {"version": 1, "classes": list(classes), "observations": list(observations)}
    if policy:
        data["policy"] = policy
    if directory:
        root = tmp_path / "profile"
        root.mkdir()
        (root / "resources.yaml").write_text(yaml.safe_dump(data))
        return str(root)
    path = tmp_path / "profile.yaml"
    path.write_text(yaml.safe_dump(data))
    return str(path)


DPP = 60000.0


def _t(seconds, draws=DPP):
    """Fields for a success that took ``seconds`` per point."""
    return {"draws per point": [draws], "draws per second": [draws / seconds]}


def _obs(cls, outcome, n_chunk=40000, **extra):
    return dict({"class": cls, "outcome": outcome, "workload": dict(WORKLOAD, **{"n chunk": n_chunk})}, **extra)


def _plan(tmp_path, observations, workload=None, policy=None, **kwargs):
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations, policy))
    return rp.plan_resources(profile, dict(WORKLOAD, **(workload or {})), **kwargs)


def _row(plan, label):
    return next(r for r in plan["classes"] if r["label"] == label)


# -- loading ---------------------------------------------------------------

@pytest.mark.parametrize("mutate, match", [
    (lambda d: d.update(version=2), "version"),
    (lambda d: d["classes"].append(dict(CLASSES[0])), "unique"),
    (lambda d: d.update(policy={"wall": 3}), "unknown policy"),
    (lambda d: d["classes"].__setitem__(0, dict(CLASSES[0], capability=[7, 6])), "empty"),
    (lambda d: d["classes"].__setitem__(0, dict(CLASSES[0], **{"memory mb": 0})), "memory mb"),
    (lambda d: d["observations"].append(_obs("nope", "success")), "not a profile class"),
    (lambda d: d["observations"].append(_obs("pascal-4gb", "oom")), "outcome"),
    (lambda d: d["observations"].append(_obs("pascal-4gb", "success", n_chunk=0)), "n chunk"),
])
def test_load_rejects_malformed(tmp_path, mutate, match):
    data = {"version": 1, "classes": [dict(c) for c in CLASSES], "observations": []}
    mutate(data)
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(rp.ResourceProfileError, match=match):
        rp.load_resource_profile(str(path))


def test_directory_profile_reads_observation_files_and_hashes(tmp_path):
    root = _write_profile(tmp_path, directory=True)
    os.mkdir(os.path.join(root, "observations"))
    with open(os.path.join(root, "observations", "run1.yaml"), "w") as f:
        yaml.safe_dump({"observations": [_obs("legacy-16gb", "success", **_t(60))]}, f)
    profile = rp.load_resource_profile(root, policy_overrides={"worker wall target hours": 3})
    assert len(profile["observations"]) == 1
    assert len(profile["files"]) == 2
    assert profile["policy"]["worker wall target hours"] == 3


def test_numbers_compare_numerically():
    a = rp.normalize_workload(dict(WORKLOAD, **{"sample rate": "4096", "n chunk": 40000.0}))
    b = rp.normalize_workload(WORKLOAD)
    assert a == b and a["n chunk"] == 40000


# -- device memory ---------------------------------------------------------

def test_memory_failure_bars_class_at_same_and_larger_chunk_only(tmp_path):
    obs = [_obs("pascal-4gb", "memory-failure"),
           _obs("legacy-16gb", "success", **_t(600))]
    plan = _plan(tmp_path, obs, policy={"admit unprofiled": True})
    assert "memory-failure on pascal-4gb" in _row(plan, "pascal-4gb")["excluded"]
    # At a smaller chunk the 40k failure is not evidence; unprofiled policy admits it.
    obs.append(_obs("legacy-16gb", "success", n_chunk=10000, **{"draws per second": [100]}))
    plan = _plan(tmp_path, obs, workload={"n chunk": 20000}, policy={"admit unprofiled": True},
                 draws_per_point=DPP)
    assert _row(plan, "pascal-4gb")["memory"] == "no comparable memory evidence"


def test_failure_on_larger_card_bars_smaller_card(tmp_path):
    obs = [_obs("legacy-16gb", "crash", n_chunk=20000),
           _obs("modern-24gb", "success", **_t(30))]
    plan = _plan(tmp_path, obs, policy={"admit unprofiled": True})
    assert plan["admitted"] == ["modern-24gb"]
    assert "crash on legacy-16gb" in _row(plan, "pascal-4gb")["excluded"]


def test_unprofiled_class_excluded_by_default(tmp_path):
    obs = [_obs("legacy-16gb", "success", **_t(600))]
    plan = _plan(tmp_path, obs)
    # Success on a 16 GB card says nothing about a 4 GB card.
    assert _row(plan, "pascal-4gb")["excluded"] == "memory: no comparable memory evidence"
    # ...but does admit the larger card, whose timing transfers by relative speed.
    assert sorted(plan["admitted"]) == ["legacy-16gb", "modern-24gb"]
    assert _row(plan, "modern-24gb")["seconds per point"] == pytest.approx(300)


def test_vram_estimate_scales_with_chunk_and_applies_margin(tmp_path):
    obs = [_obs("legacy-16gb", "success", n_chunk=20000, **{"peak vram mb": 3000}),
           _obs("legacy-16gb", "success", n_chunk=5000, **{"draws per second": [1000]})]
    plan = _plan(tmp_path, obs, draws_per_point=DPP)
    assert plan["vram need mb"] == pytest.approx(6000)
    assert "only 3400 MB usable" in _row(plan, "pascal-4gb")["excluded"]
    plan = _plan(tmp_path, obs, workload={"n chunk": 10000}, draws_per_point=DPP)
    assert plan["vram need mb"] == pytest.approx(1500)
    assert "pascal-4gb" in plan["admitted"]
    plan = _plan(tmp_path, obs, workload={"n chunk": 10000}, draws_per_point=DPP,
                 policy={"vram fixed mb": 1000})
    assert plan["vram need mb"] == pytest.approx(2000)


def test_signature_mismatch_does_not_transfer(tmp_path):
    obs = [_obs("legacy-16gb", "success", n_chunk=20000, **{"peak vram mb": 3000}),
           _obs("legacy-16gb", "success", n_chunk=5000, **dict(_t(60), workload=dict(WORKLOAD, **{
               "n chunk": 5000, "q pregrid factor": 1})))]
    plan = _plan(tmp_path, obs, workload={"q pregrid factor": 1}, draws_per_point=DPP,
                 policy={"admit unprofiled": True})
    assert plan["vram need mb"] is None


# -- timing and points per worker ------------------------------------------

def _two_class_profile(tmp_path, slow_slots):
    classes = [{"label": "fast", "capability": [9.0, None], "memory mb": 24000, "slots": 10},
               {"label": "slow", "capability": [6.0, 9.0], "memory mb": 8000, "slots": slow_slots}]
    obs = [_obs("fast", "success", **_t(60), **{"peak vram mb": 2000}),
           _obs("slow", "success", **_t(600))]
    return rp.load_resource_profile(_write_profile(tmp_path, obs, classes=classes))


def test_many_slow_cards_join_pool_with_fewer_points_per_worker(tmp_path):
    plan = rp.plan_resources(_two_class_profile(tmp_path, 1000), WORKLOAD)
    # wall 7200 s, setup 300 s: slowest admitted 600 s/point -> 11 points.
    assert plan["admitted"] == ["fast", "slow"]
    assert plan["jobs per worker"] == 11
    rate = 10 * 11 / (11 * 60 + 300) + 1000 * 11 / (11 * 600 + 300)
    assert plan["points per hour"] == pytest.approx(rate * 3600)
    assert plan["slowest worker seconds"] == 11 * 600 + 300


def test_few_slow_cards_are_not_worth_the_shorter_workers(tmp_path):
    plan = rp.plan_resources(_two_class_profile(tmp_path, 1), WORKLOAD)
    assert plan["admitted"] == ["fast"]
    assert plan["jobs per worker"] == (7200 - 300) // 60
    assert _row(plan, "slow")["excluded"] == "slower than the throughput-optimal pool"


def test_explicit_jobs_per_worker_caps_plan(tmp_path):
    plan = rp.plan_resources(_two_class_profile(tmp_path, 1), WORKLOAD, jobs_per_worker_cap=20)
    assert plan["jobs per worker"] == 20


def test_timing_quantile_and_per_point_cap(tmp_path):
    obs = [_obs("legacy-16gb", "success", **{"draws per point": [10e3, 20e3, 30e3, 40e3, 2000e3],
                                             "draws per second": [1000]})]
    plan = _plan(tmp_path, obs, policy={"timing quantile": 0.8})
    assert _row(plan, "legacy-16gb")["seconds per point"] == 40
    plan = _plan(tmp_path, obs, policy={"timing quantile": 1.0})
    assert _row(plan, "legacy-16gb")["excluded"] == "2000 s per point exceeds max seconds per point"
    assert plan["admitted"] == ["modern-24gb"]   # 1000 s by relative speed


def test_rate_transfers_up_in_chunk_only(tmp_path):
    obs = [_obs("legacy-16gb", "success", n_chunk=10000, **_t(60))]
    plan = _plan(tmp_path, obs, workload={"n chunk": 10000})
    assert _row(plan, "legacy-16gb")["speed"] == "measured, 1 workers"
    plan = _plan(tmp_path, obs, workload={"n chunk": 20000}, draws_per_point=DPP,
                 policy={"admit unprofiled": True})
    assert _row(plan, "legacy-16gb")["speed"].startswith("measured at smaller n chunk")
    with pytest.raises(rp.ResourceProfileError, match="at or below this n chunk"):
        _plan(tmp_path, obs, workload={"n chunk": 5000}, draws_per_point=DPP)


def test_worker_wall_uses_sum_of_heavy_tailed_points(tmp_path):
    # 90% of points are cheap, 10% cost 100x: the p90 point is cheap, the p90 worker is not.
    draws = [1e3] * 90 + [1e5] * 10
    obs = [_obs("legacy-16gb", "success", **{"draws per point": draws, "draws per second": [100]})]
    plan = _plan(tmp_path, obs, policy={"jobs per worker max": 1000, "max seconds per point": 1e4})
    n = plan["jobs per worker"]
    assert plan["slowest worker seconds"] <= 7200
    naive = (7200 - 300) // (rp._quantile(draws, 0.9) / 100)
    assert n < naive / 5


def test_timing_needs_matching_label(tmp_path):
    obs = [_obs("legacy-16gb", "success", **_t(60))]
    with pytest.raises(rp.ResourceProfileError, match="no draws per point"):
        _plan(tmp_path, obs, workload={"label": "bns"})


# -- require_gpus ----------------------------------------------------------

def test_require_gpus_styles(tmp_path):
    obs = [_obs("pascal-4gb", "memory-failure"),
           _obs("legacy-16gb", "success", **_t(600))]
    exclude = _plan(tmp_path, obs)["require gpus"]
    assert exclude.startswith("((GlobalMemoryMb >= 16000) && (")
    assert exclude.endswith("|| (!((Capability >= 6.1) && (Capability < 7.0) && (GlobalMemoryMb >= 4000) "
                            "&& (GlobalMemoryMb <= 8191)))))")
    include = _plan(tmp_path, obs, policy={"requirement style": "include"})["require gpus"]
    assert 'regexp("V100", DeviceName)' in include and "6.1" not in include


def test_require_gpus_evaluates_in_classad(tmp_path):
    classad = pytest.importorskip("classad")
    obs = [_obs("pascal-4gb", "memory-failure"),
           _obs("legacy-16gb", "success", **_t(600))]
    for style in ("exclude", "include"):
        expr = _plan(tmp_path, obs, policy={"requirement style": style})["require gpus"]
        verdicts = []
        for cap, mem, name in [(6.1, 4096, "GeForce GTX 1050 Ti"), (7.0, 32000, "Tesla V100-PCIE-32GB"),
                               (12.0, 32000, "RTX 5090")]:
            ad = classad.ClassAd({"Capability": cap, "GlobalMemoryMb": mem, "DeviceName": name})
            verdicts.append(bool(classad.ExprTree(expr).eval(ad)))
        assert verdicts == [False, True, True], style


# -- ini rewrite -----------------------------------------------------------

def test_apply_plan_to_ini(tmp_path):
    src = tmp_path / "in.ini"
    src.write_text("[analysis]\nifos=['H1','L1']\n[rift-pseudo-pipe]\n"
                   "ile-jobs-per-worker= 100\nile_jobs_per_worker=7\nsrate = 4096\n"
                   "manual-extra-ile-args= --n-chunk 40000 --pct 50%\n")
    out = tmp_path / "out.ini"
    rp.apply_plan_to_ini(str(src), str(out), {"jobs per worker": 11, "request memory mb": 7900,
                                             "workload": {"n chunk": 20000}})
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.optionxform = str
    cfg.read(str(out))
    section = cfg["rift-pseudo-pipe"]
    assert section["ile-jobs-per-worker"] == "11" and "ile_jobs_per_worker" not in section
    assert section["internal-ile-request-memory"] == "7900"
    assert section["ile-jobs-per-worker-first"] == "11" and section["internal-ile-n-chunk"] == "20000"
    assert section["manual-extra-ile-args"] == "--n-chunk 40000 --pct 50%"
    assert cfg["analysis"]["ifos"] == "['H1','L1']"


# -- harvest ---------------------------------------------------------------

BANNER = "Device 0 Name                : {}\nDevice 0 Compute Capability  : {}\n"
AD_LOG = """MyType = "SubmitEvent"
EventTime = "2026-11-01T00:00:00.000"
...
MyType = "ExecuteEvent"
EventTime = "2026-11-01T01:00:00.000"
...
MyType = "JobTerminatedEvent"
EventTime = "2026-11-01T01:{minutes:02d}:00.000"
MemoryUsage = {memory}
ReturnValue = {rc}
TerminatedNormally = true
...
"""
CLASSIC_LOG = """000 (7.000.000) 2026-11-01 00:00:00 Job submitted from host: <x>
...
001 (7.000.000) 2026-11-01 01:00:00 Job executing on host: <y>
...
005 (7.000.000) 2026-11-01 01:50:00 Job terminated.
\t(0) Abnormal termination (signal 11)
\tPartitionable Resources :    Usage  Request Allocated
\t   Memory (MB)          :     6000    12000     12032
...
"""


def _ile(rundir, stem, device, cap, log, err="", outputs=()):
    logs = rundir / "iteration_1_ile" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    (logs / (stem + ".out")).write_text(BANNER.format(device, cap) if device else "no gpu\n")
    (logs / (stem + ".log")).write_text(log)
    (logs / (stem + ".err")).write_text(err)
    for name in outputs:   # event m1 m2 ... lnL sigma_lnL ntotal neff
        (rundir / "iteration_1_ile" / name).write_text("0 30 25 0 0 0 0 0 0 40.0 0.1 600000 20\n")


def test_harvest_rundir(tmp_path):
    profile = rp.load_resource_profile(_write_profile(tmp_path, policy={"worker setup seconds": 0}))
    run = tmp_path / "run"
    # V100 worker over two GPUs: 4 points x 6e5 draws in 40 min -> 500 draws/s per GPU.
    _ile(run, "ILE-0-5-0", "Tesla V100-PCIE-32GB", 70, AD_LOG.format(minutes=40, memory=1977, rc=0),
         outputs=["CME_out-0-5-0.xml.gpu0_0_.dat", "CME_out-0-5-0.xml.gpu0_1_.dat",
                  "CME_out-0-5-0.xml.gpu1_0_.dat", "CME_out-0-5-0.xml.gpu1_1_.dat"])
    _ile(run, "ILE-10-5-0", "NVIDIA GeForce GTX 1050 Ti", 61, AD_LOG.format(minutes=5, memory=900, rc=1),
         err="cupy.cuda.memory.OutOfMemoryError: out of memory allocating")
    _ile(run, "ILE-20-6-0", "NVIDIA GeForce GTX 1080", 61, CLASSIC_LOG)
    _ile(run, "ILE-30-6-0", "Quadro K2200", 50, AD_LOG.format(minutes=1, memory=1, rc=0))
    _ile(run, "ILE-40-6-0", None, None, AD_LOG.format(minutes=1, memory=1, rc=0))
    # A worker whose execute and terminate events share a timestamp yields no timing.
    _ile(run, "ILE-50-6-0", "Tesla T4", 75, AD_LOG.replace("01:{minutes:02d}", "01:00").format(memory=1, rc=0),
         outputs=["CME_out-50-6-0.xml_0_.dat"])
    observations, skipped = rp.harvest_rundir(str(run), profile, WORKLOAD, source="test")
    by = {(o["class"], o["outcome"]): o for o in observations}
    ok = by[("legacy-16gb", "success")]
    assert ok["draws per second"] == [500.0] and ok["host memory mb"] == 1977
    assert ok["draws per point"] == [600000.0] * 5 and ok["workers"] == 2
    assert ("pascal-4gb", "memory-failure") in by
    crash = by[("pascal-4gb", "crash")]
    assert crash["host memory mb"] == 6000 and crash["draws per point"] == []
    assert skipped == {"not a grid worker": 0, "no gpu banner": 1, "no termination": 0,
                       "unclassified device": 1}
    # The harvest round-trips through the loader and feeds the planner.
    root = _write_profile(tmp_path, directory=True)
    os.mkdir(os.path.join(root, "observations"))
    with open(os.path.join(root, "observations", "run.yaml"), "w") as f:
        yaml.safe_dump({"observations": observations}, f)
    plan = rp.plan_resources(rp.load_resource_profile(root), WORKLOAD)
    assert "pascal-4gb" not in plan["admitted"]
    assert plan["request memory mb"] == int(-(-6000 * 1.25 // 100) * 100)


# -- review regressions ----------------------------------------------------

def test_exclude_style_keeps_admitted_class_inside_broader_excluded_class(tmp_path):
    classad = pytest.importorskip("classad")
    classes = [{"label": "a100", "capability": [8.0, 9.0], "memory mb": 40000, "slots": 5,
                "device names": ["A100"]},
               {"label": "ampere-other", "capability": [8.0, 9.0], "memory mb": 8000, "slots": 50}]
    obs = [_obs("a100", "success", **_t(60))]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=classes))
    plan = rp.plan_resources(profile, WORKLOAD)
    assert plan["admitted"] == ["a100"]
    expr = classad.ExprTree(plan["require gpus"])
    a100 = classad.ClassAd({"Capability": 8.0, "GlobalMemoryMb": 40536, "DeviceName": "NVIDIA A100-SXM4-40GB"})
    a10 = classad.ClassAd({"Capability": 8.6, "GlobalMemoryMb": 22000, "DeviceName": "NVIDIA A10"})
    other = classad.ClassAd({"Capability": 12.0, "GlobalMemoryMb": 48000, "DeviceName": "RTX 6000 Ada"})
    assert [bool(expr.eval(ad)) for ad in (a100, a10, other)] == [True, False, True]


def test_success_outweighs_stray_crash(tmp_path):
    obs = [_obs("legacy-16gb", "success", **_t(60)), _obs("legacy-16gb", "crash"),
           _obs("modern-24gb", "success", **_t(30))]
    plan = _plan(tmp_path, obs)
    assert sorted(plan["admitted"]) == ["legacy-16gb", "modern-24gb"]
    assert "crash" in _row(plan, "pascal-4gb")["excluded"]


def test_device_regex_agrees_between_python_and_classad(tmp_path):
    classad = pytest.importorskip("classad")
    classes = [{"label": "rtx", "capability": [7.0, 9.0], "memory mb": 20000, "slots": 5,
                "device names": [r"RTX\s6000"]}]
    profile = rp.load_resource_profile(_write_profile(
        tmp_path, [_obs("rtx", "success", **_t(60))], classes=classes, policy={"requirement style": "include"}))
    assert rp.classify_device(profile, "Quadro RTX 6000", 7.5) == "rtx"
    ad = classad.ClassAd({"Capability": 7.5, "GlobalMemoryMb": 24000, "DeviceName": "Quadro RTX 6000"})
    assert classad.ExprTree(rp.plan_resources(profile, WORKLOAD)["require gpus"]).eval(ad) is True


def test_vram_estimate_limited_to_chunk_scaling(tmp_path):
    obs = [_obs("legacy-16gb", "success", n_chunk=160000, **{"peak vram mb": 12000}),
           _obs("legacy-16gb", "success", n_chunk=5000, **{"draws per second": [1000]})]
    plan = _plan(tmp_path, obs, workload={"n chunk": 20000}, draws_per_point=DPP)
    assert plan["vram need mb"] is None                      # 8x is beyond the default 2x
    plan = _plan(tmp_path, obs, workload={"n chunk": 20000}, draws_per_point=DPP,
                 policy={"max chunk scaling": 8})
    assert plan["vram need mb"] == pytest.approx(1500)


@pytest.mark.parametrize("mutate, match", [
    (lambda d: d["classes"].__setitem__(0, dict(CLASSES[0], capability=[None, 9])), "capability"),
    (lambda d: d["classes"].__setitem__(0, "pascal"), "mapping"),
    (lambda d: d["observations"].append(_obs("pascal-4gb", "success", **{"peak vram mb": "4 GB"})), "peak vram"),
    (lambda d: d.update(policy={"timing quantile": None}), "must be a number"),
    (lambda d: d.update(policy={"worker wall target hours": "2h"}), "must be a number"),
    (lambda d: d.update(policy={"admit unprofiled": "yes"}), "boolean"),
])
def test_load_rejects_values_that_would_crash_planner(tmp_path, mutate, match):
    data = {"version": 1, "classes": [dict(c) for c in CLASSES], "observations": []}
    mutate(data)
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(rp.ResourceProfileError, match=match):
        rp.load_resource_profile(str(path))


def test_harvest_review_cases(tmp_path):
    classes = [{"label": "pascal-4gb", "capability": [6.1, 7.0], "memory mb": 3900, "memory mb max": 6000,
                "slots": 10},
               {"label": "pascal-8gb", "capability": [6.1, 7.0], "memory mb": 7900, "slots": 10}]
    profile = rp.load_resource_profile(_write_profile(tmp_path, classes=classes, policy={"worker setup seconds": 0}))
    run = tmp_path / "run"
    gpu_ad = 'ExecuteProps = [ GlobalMemoryMb = 8192 ]\nGpusMemoryUsage = 3100.0\n'
    ok_log = AD_LOG.format(minutes=10, memory=1000, rc=0).replace('MemoryUsage', gpu_ad + 'MemoryUsage', 1)
    # exit 0 with a harmless allocator warning: still a success, classified by card memory
    _ile(run, "ILE-0-5-0", "GeForce GTX 1080", 61, ok_log,
         err="Allocator (GPU_0_bfc) ran out of memory trying to allocate; not a failure",
         outputs=["CME_out-0-5-0.xml_0_.dat"])
    # hyperpipeline row layout: no ntotal column, so no draws
    _ile(run, "ILE-1-5-0", "GeForce GTX 1080", 61, ok_log)
    (run / "iteration_1_ile" / "CME_out-1-5-0.xml_0_.dat").write_text("-3.2 0.1 30 25 0.1 0.2 0.35 0.9\n")
    # extrinsic job: not a grid worker
    _ile(run, "ILEextr-0-7-0", "GeForce GTX 1080", 61, ok_log)
    observations, skipped = rp.harvest_rundir(str(run), profile, WORKLOAD)
    by = {(o["class"], o["outcome"]): o for o in observations}
    assert set(by) == {("pascal-8gb", "success"), ("pascal-8gb", "failure")}
    ok = by[("pascal-8gb", "success")]
    assert ok["peak vram mb"] == 3100.0 and ok["draws per point"] == [600000.0]
    assert ok["draws per second"] == [pytest.approx(1000.0)]
    assert skipped["not a grid worker"] == 1
    root = _write_profile(tmp_path, classes=classes, directory=True)
    os.mkdir(os.path.join(root, "observations"))
    with open(os.path.join(root, "observations", "run.yaml"), "w") as f:
        yaml.safe_dump({"observations": observations}, f)
    rp.load_resource_profile(root)
