#!/usr/bin/env python3
"""RIFT.misc.resource_profile: profile loading, ILE resource planning, ini rewrite, harvest."""

import configparser
import math
import os

import pytest

yaml = pytest.importorskip("yaml")
from RIFT.misc import resource_profile as rp

WORKLOAD = {"label": "bbh", "n chunk": 40000, "n eff": 20, "sample rate": 4096,
            "q pregrid factor": 2, "ifos": 2, "approximant": "IMRPhenomXPHM", "lmax": 4,
            "time resampling": 32768}

CLASSES = [
    {"label": "pascal-4gb", "capability": [6.1, 7.0], "memory mb": 4000, "memory mb max": 8191,
     "slots": 400, "relative speed": 0.3, "memory family": "f"},
    {"label": "legacy-16gb", "capability": [7.0, 9.0], "memory mb": 16000, "slots": 40,
     "relative speed": 1.0, "device names": ["V100", "T4", "A100"], "memory family": "f"},
    {"label": "modern-24gb", "capability": [9.0, 12.1], "memory mb": 24000, "slots": 10,
     "relative speed": 2.0, "memory family": "f"},
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
# Workers whose device memory stays flat, whatever they draw.
FLAT = [[1e6 * (i + 1), 1000.0, None] for i in range(10)]


def _t(seconds, draws=DPP):
    """Fields for a success that took ``seconds`` per point, with flat memory."""
    return {"draws per point": [draws], "draws per second": [draws / seconds], "memory samples": FLAT}


def _growing(base, per_mdraw, column=1):
    """Memory samples from workers whose memory grows with draws."""
    samples = []
    for i in range(8):
        sample = [1e6 * (i + 1), None, None]
        sample[column] = base + per_mdraw * (i + 1)
        samples.append(sample)
    return samples


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

def test_memory_failure_bars_class_at_any_chunk(tmp_path):
    # Measured memory follows draws per worker, so a failure at 40k also speaks for 20k.
    obs = [_obs("pascal-4gb", "memory-failure"), _obs("legacy-16gb", "success", **_t(60)),
           _obs("legacy-16gb", "success", n_chunk=10000, **_t(60))]
    for chunk in (40000, 20000):
        plan = _plan(tmp_path, obs, workload={"n chunk": chunk}, draws_per_point=DPP)
        assert "memory-failure on pascal-4gb" in _row(plan, "pascal-4gb")["excluded"]


def test_failure_on_larger_card_bars_smaller_card(tmp_path):
    obs = [_obs("legacy-16gb", "crash", n_chunk=20000),
           _obs("modern-24gb", "success", **_t(30))]
    plan = _plan(tmp_path, obs, policy={"admit unprofiled": True})
    assert plan["admitted"] == ["modern-24gb"]
    assert "crash on legacy-16gb" in _row(plan, "pascal-4gb")["excluded"]


def test_unprofiled_class_excluded_by_default(tmp_path):
    obs = [_obs("legacy-16gb", "success", **_t(600))]
    classes = [dict(CLASSES[0], **{"memory family": "other"})] + CLASSES[1:]
    plan = rp.plan_resources(rp.load_resource_profile(_write_profile(tmp_path, obs, classes=classes)), WORKLOAD)
    # Success on a 16 GB card of another family says nothing about a 4 GB card.
    assert _row(plan, "pascal-4gb")["excluded"] == "memory: no comparable memory evidence"
    # ...but does admit the larger card, whose timing transfers by relative speed.
    assert sorted(plan["admitted"]) == ["legacy-16gb", "modern-24gb"]
    assert _row(plan, "modern-24gb")["seconds per point"] == pytest.approx(300)


def _family_classes():
    return [{"label": "small", "capability": [6.0, 7.0], "memory mb": 4000, "slots": 400,
             "relative speed": 1.0, "memory family": "old"},
            {"label": "big", "capability": [7.0, 9.0], "memory mb": 20000, "slots": 10,
             "relative speed": 1.0, "memory family": "old"},
            {"label": "new", "capability": [9.0, None], "memory mb": 20000, "slots": 10,
             "relative speed": 1.0, "memory family": "new"}]


def test_memory_growth_limits_points_per_worker(tmp_path):
    # 500 MB + 300 MB/Mdraw, 1 Mdraw per point: 4000 MB x 0.85 = 3400 MB holds 9 points.
    obs = [_obs("big", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                     "memory samples": _growing(500, 300)})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:2]))
    plan = rp.plan_resources(profile, WORKLOAD)
    assert _row(plan, "small")["points limit"] == 9
    assert _row(plan, "small")["max worker draws"] == pytest.approx(2900 / 300 * 1e6)
    assert "family old" in _row(plan, "small")["memory"]


def test_memory_fit_does_not_cross_families(tmp_path):
    obs = [_obs("big", "success", **{"draws per point": [1e6], "draws per second": [1000],
                                     "memory samples": _growing(500, 300)}),
           _obs("new", "success", **{"draws per second": [1000], "memory samples": _growing(1000, 0)})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()))
    plan = rp.plan_resources(profile, WORKLOAD, jobs_per_worker_cap=100)
    assert _row(plan, "new")["max worker draws"] == 16e6      # flat, so only extrapolation limits it
    assert _row(plan, "big")["max worker draws"] == pytest.approx(16e6)
    assert "new" in plan["admitted"]


def test_failure_caps_draws_at_what_the_worker_completed(tmp_path):
    obs = [_obs("big", "success", **{"draws per point": [1e6], "draws per second": [1000],
                                     "memory samples": _growing(500, 1)}),
           _obs("small", "crash", **{"failure draws": [3e6]})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:2]))
    plan = rp.plan_resources(profile, WORKLOAD)
    assert _row(plan, "small")["max worker draws"] == 3e6
    assert _row(plan, "small")["points limit"] == 3


def test_host_memory_limits_points_and_sets_request(tmp_path):
    # Host 1000 MB + 500 MB/Mdraw: 16000 MB / 1.25 allows 23.6 Mdraw, so 23 points.
    obs = [_obs("legacy-16gb", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                             "memory samples": _growing(1000, 500, column=2)}),
           _obs("legacy-16gb", "success", **{"memory samples": [[1e7 * (i + 1), 1000.0, None] for i in range(8)]})]
    plan = _plan(tmp_path, obs)
    assert plan["jobs per worker"] == 23
    assert plan["request memory mb"] == 15700     # (1000 + 500 x 23) x 1.25, rounded up


def test_no_fit_trusts_only_the_draws_a_worker_completed(tmp_path):
    obs = [_obs("legacy-16gb", "success", **{"draws per point": [1e6, 1e6], "draws per second": [1000],
                                             "memory samples": [[2e6, 1000.0, None]]})]
    plan = _plan(tmp_path, obs)
    assert plan["jobs per worker"] == 2


def test_flat_fit_above_the_card_excludes_it(tmp_path):
    obs = [_obs("big", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                     "memory samples": _growing(8000, 0)})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:2]))
    plan = rp.plan_resources(profile, WORKLOAD)
    assert plan["admitted"] == ["big"] and _row(plan, "small")["max worker draws"] == 0


def test_host_fit_alone_is_no_device_evidence(tmp_path):
    obs = [_obs("big", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                     "memory samples": _growing(1000, 10, column=2)})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:2]))
    assert "no comparable" in _row(rp.plan_resources(profile, WORKLOAD), "small")["excluded"]


def test_fit_envelope_covers_scatter(tmp_path):
    # Half the workers sit 1000 MB above the line, so the base is 1500 MB, not the fit's 1000:
    # (3400 - 1500) / 300 MB per Mdraw.
    samples = _growing(500, 300) + [[s[0], s[1] + 1000, None] for s in _growing(500, 300)]
    obs = [_obs("big", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                     "memory samples": samples})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:2]))
    assert _row(rp.plan_resources(profile, WORKLOAD), "small")["max worker draws"] == pytest.approx(1900 / 300 * 1e6)


def test_family_fit_tempers_a_flat_own_fit(tmp_path):
    obs = [_obs("big", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                     "memory samples": _growing(500, 300)}),
           _obs("small", "success", **{"memory samples": _growing(500, 0)})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:2]))
    row = _row(rp.plan_resources(profile, WORKLOAD), "small")
    # Pooled slope 150, base 1700: (3400 - 1700) / 150; the own flat fit alone would allow 16 Mdraw.
    assert "family old" in row["memory"] and row["max worker draws"] == pytest.approx(1700 / 150 * 1e6)


def test_one_success_does_not_void_several_crashes(tmp_path):
    crash = [_obs("small", "crash", **{"failure draws": [d]}) for d in (2e6, 3e6, 4e6)]
    obs = crash + [_obs("small", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                               "memory samples": [[7e6, 3900.0, None, 1]]})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:1]))
    assert _row(rp.plan_resources(profile, WORKLOAD, jobs_per_worker_cap=1), "small")["max worker draws"] == 2e6


def test_fan_out_counts_draws_per_gpu(tmp_path):
    obs = [_obs("small", "crash", **{"failure draws": [15e6]}),
           _obs("small", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                       "memory samples": [[20e6, None, None, 2]] * 20})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[:1]))
    # 20 Mdraw over 2 GPUs is 10 Mdraw per GPU: it neither passes the crash nor counts as 20.
    assert _row(rp.plan_resources(profile, WORKLOAD), "small")["max worker draws"] == 10e6


def test_host_request_covers_each_admitted_class(tmp_path):
    obs = [_obs("big", "success", **dict(_t(1), **{"memory samples": _growing(1000, 0) + [[1e6, None, 2000.0]] * 8,
                                                   "host memory mb": 1})),
           _obs("new", "success", **dict(_t(1), **{"host memory mb": 14000}))]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=_family_classes()[1:]))
    plan = rp.plan_resources(profile, WORKLOAD)
    assert sorted(plan["admitted"]) == ["big", "new"] and plan["request memory mb"] == 17500


def test_limited_class_joins_when_setup_is_free(tmp_path):
    # With no setup cost, a smaller N loses nothing and adds a class.
    classes = [{"label": "a", "capability": [9.0, None], "memory mb": 20000, "slots": 10},
               {"label": "b", "capability": [6.0, 9.0], "memory mb": 20000, "slots": 10}]
    obs = [_obs("a", "success", **_t(60)), _obs("b", "success", **_t(60))]
    obs[1] = dict(obs[1], **{"memory samples": [[DPP * 3, 1000.0, None]]})
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=classes, policy={"worker setup seconds": 0}))
    plan = rp.plan_resources(profile, WORKLOAD)
    assert plan["jobs per worker"] == 3 and sorted(plan["admitted"]) == ["a", "b"]


def test_failure_does_not_cross_families(tmp_path):
    obs = [_obs("new", "crash", **{"failure draws": [3e6]}),
           _obs("small", "success", **{"draws per point": [1e6], "draws per second": [1e5],
                                       "memory samples": _growing(500, 100)})]
    classes = [_family_classes()[0], _family_classes()[2]]
    profile = rp.load_resource_profile(_write_profile(tmp_path, obs, classes=classes))
    assert _row(rp.plan_resources(profile, WORKLOAD), "small")["max worker draws"] == 16e6


def test_wall_quantile_is_of_the_sum_not_its_mean():
    draws = [1e3] * 90 + [1e5] * 10
    walls = rp._wall_quantiles(draws, 50, 0.9)
    assert walls[4] > 1.5 * 5 * sum(draws) / len(draws)
    assert all(b >= a for a, b in zip(walls, walls[1:]))


def test_zero_cap_is_rejected(tmp_path):
    with pytest.raises(rp.ResourceProfileError, match="cap"):
        _plan(tmp_path, [_obs("legacy-16gb", "success", **_t(60))], jobs_per_worker_cap=0)


def test_signature_mismatch_does_not_transfer(tmp_path):
    obs = [_obs("legacy-16gb", "success", **_t(600))]
    with pytest.raises(rp.ResourceProfileError, match="no comparable memory evidence"):
        _plan(tmp_path, obs, workload={"q pregrid factor": 1}, draws_per_point=DPP)


# -- timing and points per worker ------------------------------------------

def _two_class_profile(tmp_path, slow_slots):
    classes = [{"label": "fast", "capability": [9.0, None], "memory mb": 24000, "slots": 10},
               {"label": "slow", "capability": [6.0, 9.0], "memory mb": 8000, "slots": slow_slots}]
    obs = [_obs("fast", "success", **_t(60)),
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
    assert _row(plan, "slow")["excluded"] == "its limit of 11 points is below the throughput-optimal 115"


def test_explicit_jobs_per_worker_caps_plan(tmp_path):
    plan = rp.plan_resources(_two_class_profile(tmp_path, 1), WORKLOAD, jobs_per_worker_cap=20)
    assert plan["jobs per worker"] == 20


def test_timing_quantile_and_per_point_cap(tmp_path):
    obs = [_obs("legacy-16gb", "success", **{"draws per point": [10e3, 20e3, 30e3, 40e3, 2000e3],
                                             "draws per second": [1000], "memory samples": FLAT})]
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
    obs = [_obs("legacy-16gb", "success", **{"draws per point": draws, "draws per second": [100],
                                             "memory samples": FLAT})]
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
    assert skipped == {"not a grid worker": 0, "no gpu banner": 1, "no termination": 0, "host memory hold": 0,
                       "unclassified device": 1}
    # The harvest round-trips through the loader and feeds the planner.
    root = _write_profile(tmp_path, directory=True)
    os.mkdir(os.path.join(root, "observations"))
    with open(os.path.join(root, "observations", "run.yaml"), "w") as f:
        yaml.safe_dump({"observations": observations}, f)
    plan = rp.plan_resources(rp.load_resource_profile(root), WORKLOAD)
    assert "pascal-4gb" not in plan["admitted"]
    assert plan["request memory mb"] == 2500      # 1977 MB on the admitted class; pascal's crash does not count


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
                "device names": ["RTX[ ]6000"]}]
    profile = rp.load_resource_profile(_write_profile(
        tmp_path, [_obs("rtx", "success", **_t(60))], classes=classes, policy={"requirement style": "include"}))
    assert rp.classify_device(profile, "Quadro RTX 6000", 7.5) == "rtx"
    ad = classad.ClassAd({"Capability": 7.5, "GlobalMemoryMb": 24000, "DeviceName": "Quadro RTX 6000"})
    assert classad.ExprTree(rp.plan_resources(profile, WORKLOAD)["require gpus"]).eval(ad) is True


@pytest.mark.parametrize("mutate, match", [
    (lambda d: d["classes"].__setitem__(0, dict(CLASSES[0], capability=[None, 9])), "capability"),
    (lambda d: d["classes"].__setitem__(0, "pascal"), "mapping"),
    (lambda d: d["observations"].append(_obs("pascal-4gb", "success", **{"host memory mb": "4 GB"})), "host memory"),
    (lambda d: d["observations"].append(_obs("pascal-4gb", "success", **{"memory samples": [[1e6, "4 GB", 1]]})),
     "memory samples"),
    (lambda d: d["observations"].append(_obs("pascal-4gb", "crash", **{"failure draws": ["many"]})), "failure draws"),
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
    assert ok["memory samples"] == [[600000.0, 3100.0, 1000.0, 1]] and ok["draws per point"] == [600000.0]
    assert ok["draws per second"] == [pytest.approx(1000.0)]
    assert skipped["not a grid worker"] == 1
    root = _write_profile(tmp_path, classes=classes, directory=True)
    os.mkdir(os.path.join(root, "observations"))
    with open(os.path.join(root, "observations", "run.yaml"), "w") as f:
        yaml.safe_dump({"observations": observations}, f)
    rp.load_resource_profile(root)


def test_harvest_fan_out_failure_and_zero_usage(tmp_path):
    profile = rp.load_resource_profile(_write_profile(tmp_path, policy={"worker setup seconds": 0}))
    run = tmp_path / "run"
    # Two shards, one wrote nothing: still two GPUs, so 2 x 6e5 draws are 6e5 per GPU.
    _ile(run, "ILE-0-5-0", "Tesla V100-PCIE-32GB", 70, AD_LOG.format(minutes=9, memory=1000, rc=139),
         err="[rift_ile_multigpu] shard 0 GPU 0\n[rift_ile_multigpu] shard 1 GPU 1\n",
         outputs=["CME_out-0-5-0.xml.gpu0_0_.dat", "CME_out-0-5-0.xml.gpu0_1_.dat"])
    _ile(run, "ILE-1-5-0", "Tesla V100-PCIE-32GB", 70, AD_LOG.format(minutes=10, memory=0, rc=0),
         outputs=["CME_out-1-5-0.xml_0_.dat"])
    # A fanned-out worker's device peak spans GPUs, so it is not kept.
    log = AD_LOG.format(minutes=10, memory=900, rc=0).replace("MemoryUsage", "GpusMemoryUsage = 3100.0\nMemoryUsage", 1)
    _ile(run, "ILE-2-5-0", "Tesla V100-PCIE-32GB", 70, log,
         outputs=["CME_out-2-5-0.xml.gpu0_0_.dat", "CME_out-2-5-0.xml.gpu1_0_.dat"])
    observations, _ = rp.harvest_rundir(str(run), profile, WORKLOAD)
    by = {o["outcome"]: o for o in observations}
    assert by["crash"]["failure draws"] == [600000.0]
    assert by["success"]["memory samples"] == [[600000.0, None, None, 1], [1200000.0, None, 900.0, 2]]
    root = _write_profile(tmp_path, directory=True)
    os.mkdir(os.path.join(root, "observations"))
    with open(os.path.join(root, "observations", "run.yaml"), "w") as f:
        yaml.safe_dump({"observations": observations}, f)
    rp.load_resource_profile(root)


# -- adversarial review, 2026-11-04 -------------------------------------------

def _two_pascal(first_excluded=True):
    specific = {"label": "gtx1050", "capability": [6.0, 7.0], "memory mb": 3900, "memory mb max": 5000,
                "device names": ["1050"], "slots": 100, "memory family": "f"}
    broad = {"label": "pascal", "capability": [6.0, 7.0], "memory mb": 3900, "slots": 100, "memory family": "f"}
    return [specific, broad] if first_excluded else [broad, specific]


@pytest.mark.parametrize("style", ["exclude", "include"])
def test_admitted_selector_leaves_out_an_earlier_excluded_class(tmp_path, style):
    observations = [_obs("pascal", "success", **_t(10))]
    path = _write_profile(tmp_path, observations, {"requirement style": style}, _two_pascal())
    plan = rp.plan_resources(rp.load_resource_profile(path), WORKLOAD)
    assert plan["admitted"] == ["pascal"]
    assert 'regexp("1050", DeviceName)' in plan["require gpus"]
    assert '&& !((Capability >= 6.0)' in plan["require gpus"]


def test_an_excluded_class_listed_later_does_not_touch_the_admitted_one():
    profile = {"classes": [dict(c, **{"device names": c.get("device names", [])}) for c in _two_pascal(False)]}
    selector = rp._admitted_selector(profile, profile["classes"][0], {"pascal"})
    assert selector == rp._class_selector(profile["classes"][0])


@pytest.mark.parametrize("code", [139, 245, 134, 250])
def test_every_form_of_a_signalled_exit_is_a_crash(tmp_path, code):
    profile = rp.load_resource_profile(_write_profile(tmp_path, policy={"worker setup seconds": 0}))
    run = tmp_path / "run"
    _ile(run, "ILE-0-5-0", "Tesla V100-PCIE-32GB", 70, AD_LOG.format(minutes=9, memory=1000, rc=code),
         outputs=["CME_out-0-5-0.xml_0_.dat"])
    observations, _ = rp.harvest_rundir(str(run), profile, WORKLOAD)
    assert [o["outcome"] for o in observations] == ["crash"]


def test_family_fit_is_raised_over_the_class_own_few_samples(tmp_path):
    classes = _family_classes()
    thin = [[1e6 * (i + 1), 3800.0, None] for i in range(7)]
    # enough workers that the 99th percentile of the family passes under the seven
    wide = [[1e6 * (1 + i % 40), 500.0 + 10.0 * (1 + i % 40), None] for i in range(1000)]
    label = classes[0]["label"]
    other = classes[1]["label"]
    observations = [_obs(label, "success", **dict(_t(10), **{"memory samples": thin})),
                    _obs(other, "success", **dict(_t(10), **{"memory samples": wide}))]
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations, classes=classes))
    fits = rp._memory_models(profile, profile["classes"][0], rp.normalize_workload(WORKLOAD), 1)
    assert fits and fits[-1][0][0] >= 3800.0 - 10.0


def test_host_request_without_a_fit_follows_the_draws(tmp_path):
    samples = [[1e6 * (i + 1), 1000.0, 1000.0 + 500.0 * (i + 1)] for i in range(7)]
    workload = rp.normalize_workload(WORKLOAD)
    profile = rp.load_resource_profile(_write_profile(
        tmp_path, [_obs("modern-24gb", "success", **dict(_t(10), **{"memory samples": samples}))]))
    by_label = {c["label"]: c for c in profile["classes"]}
    request = rp._host_request(profile, workload, [{"label": "modern-24gb"}], by_label, 14e6)
    assert request == 1000 * math.ceil((1000.0 + 500.0 * 14) * 1.25 / 1000)


def _thin_host_request(tmp_path, samples, draws, **observed):
    profile = rp.load_resource_profile(_write_profile(
        tmp_path, [_obs("modern-24gb", "success", **dict(_t(10), **dict(observed, **{"memory samples": samples})))]))
    by_label = {c["label"]: c for c in profile["classes"]}
    return rp._host_request(profile, rp.normalize_workload(WORKLOAD), [{"label": "modern-24gb"}], by_label, draws)


def test_host_request_sets_no_slope_from_workers_with_similar_draws(tmp_path):
    samples = [[3.00e6, 1000.0, 5000.0], [3.05e6, 1000.0, 5400.0]]
    assert _thin_host_request(tmp_path, samples, 14e6) == 6800         # 5400 x 1.25, rounded up


def test_host_request_line_stops_at_the_largest_request(tmp_path):
    samples = [[1e6, 1000.0, 2000.0], [3e6, 1000.0, 6000.0]]
    assert _thin_host_request(tmp_path, samples, 100e6) == 16000


def test_host_request_keeps_a_class_value_beside_a_family_fit(tmp_path):
    classes = _family_classes()
    wide = [[1e6 * (i + 1), None, 500.0 + 10.0 * (i + 1)] for i in range(40)]
    observations = [_obs("small", "success", **dict(_t(10), **{"host memory mb": 12000.0})),
                    _obs("big", "success", **dict(_t(10), **{"memory samples": wide}))]
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations, classes=classes))
    by_label = {c["label"]: c for c in profile["classes"]}
    request = rp._host_request(profile, rp.normalize_workload(WORKLOAD), [{"label": "small"}], by_label, 1e6)
    assert request == 15000


def test_memory_ceiling_below_the_floor_is_refused(tmp_path):
    classes = [dict(CLASSES[0], **{"memory mb max": 0})]
    with pytest.raises(rp.ResourceProfileError, match="memory mb max"):
        rp.load_resource_profile(_write_profile(tmp_path, [], classes=classes))


def test_admit_unprofiled_keeps_the_host_ceiling(tmp_path):
    # "small" has no evidence of its own, and "big" gives the family a host line.
    observations = [_obs("big", "success", **dict(_t(10), **{"memory samples": _growing(500, 1000, column=2)}))]
    limits = {}
    for host_max in (16000, 1000000):
        path = tmp_path / str(host_max)
        path.mkdir()
        profile = rp.load_resource_profile(_write_profile(
            path, observations, classes=_family_classes(),
            policy={"admit unprofiled": True, "host memory max mb": host_max, "jobs per worker max": 1000,
                    "worker wall target hours": 1000}))
        limits[host_max] = _row(rp.plan_resources(profile, WORKLOAD), "small")["points limit"]
    assert limits[16000] < limits[1000000]


def test_device_fit_is_per_gpu_and_host_fit_is_total(tmp_path):
    samples = [[2e6 * (i + 1), 500.0 + 100.0 * (i + 1), 500.0 + 100.0 * (i + 1), 2] for i in range(8)]
    profile = rp.load_resource_profile(_write_profile(
        tmp_path, [_obs("modern-24gb", "success", **dict(_t(10), **{"memory samples": samples}))]))
    c, workload = profile["classes"][2], rp.normalize_workload(WORKLOAD)
    device, host = (rp._memory_models(profile, c, workload, column)[0][0][1] for column in (1, 2))
    assert device == pytest.approx(2 * host)


def test_null_memory_ceiling_means_none():
    profile = {"classes": [{"label": "x", "capability": [6.0, 7.0], "memory mb": 1000, "memory mb max": None,
                            "device names": []}]}
    assert rp.classify_device(profile, "card", 6.5, 5000) == "x"


def test_exactly_the_stated_survivors_set_a_failure_aside(tmp_path):
    samples = [[5e6, None, None]] * 8 + [[5e6 + 1, None, None]]
    observations = [_obs("pascal-4gb", "crash", **{"failure draws": [4e6]}),
                    _obs("pascal-4gb", "success", **{"memory samples": samples})]
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations))
    ceiling, reason = rp._draw_ceiling(profile, profile["classes"][0], rp.normalize_workload(WORKLOAD))
    assert ceiling > 4e6, reason
    observations[1]["memory samples"] = samples[:8]
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations))
    ceiling, reason = rp._draw_ceiling(profile, profile["classes"][0], rp.normalize_workload(WORKLOAD))
    assert ceiling == 4e6, reason


def test_draw_rate_takes_the_low_quantile(tmp_path):
    rates = [100.0 * (i + 1) for i in range(10)]
    profile = rp.load_resource_profile(_write_profile(
        tmp_path, [_obs("modern-24gb", "success", **{"draws per second": rates})]))
    rate, _ = rp._draw_rate(profile, profile["classes"][2], rp.normalize_workload(WORKLOAD))
    assert rate == 100.0


def test_failed_workers_do_not_enter_a_memory_fit(tmp_path):
    observations = [_obs("modern-24gb", "failure", **{"memory samples": _growing(500, 1000)}),
                    _obs("modern-24gb", "success", **_t(10))]
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations))
    fits = rp._memory_models(profile, profile["classes"][2], rp.normalize_workload(WORKLOAD), 1)
    assert fits and all(fit[1] == pytest.approx(0, abs=1e-9) for fit, _ in fits)


def test_harvest_counts_host_memory_holds_apart(tmp_path):
    logs = tmp_path / "iteration_0_ile" / "logs"
    logs.mkdir(parents=True)
    (logs / "ILE-1-2-0.out").write_text("Device 0 Name                : Card\nDevice 0 Compute Capability  : 120\n")
    (logs / "ILE-1-2-0.log").write_text("012 (2.0.0) 2026-10-30 10:00:00 Job was held.\n"
                                        "\tJob memory usage exceeded request_memory\n")
    profile = rp.load_resource_profile(_write_profile(tmp_path, []))
    _, skipped = rp.harvest_rundir(str(tmp_path), profile, WORKLOAD)
    assert skipped["host memory hold"] == 1 and skipped["no termination"] == 0
    (logs / "ILE-1-2-0.out").write_text("")                     # held before ILE printed its banner
    (logs / "ILE-1-2-0.log").write_text("012 (2.0.0) 2026-10-30 10:00:00 Job was held.\n"
                                        "\tJob has gone over cgroup memory limit of 4000 megabytes.\n")
    _, skipped = rp.harvest_rundir(str(tmp_path), profile, WORKLOAD)
    assert skipped["host memory hold"] == 1 and skipped["no gpu banner"] == 0


def test_device_name_with_a_backslash_is_refused(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump({"version": 1, "classes": [
        {"label": "a", "capability": [8, 9], "memory mb": 1000, "device names": ["^A1\\.5$"]}], "observations": []}))
    with pytest.raises(rp.ResourceProfileError, match="backslash"):
        rp.load_resource_profile(str(path))


def test_thin_host_line_limits_the_worker(tmp_path):
    samples = [[1e6, 1000.0, 2000.0], [3e6, 1000.0, 6000.0]]
    observation = _obs("modern-24gb", "success", **dict(_t(10), **{"memory samples": samples}))
    profile = rp.load_resource_profile(_write_profile(tmp_path, [observation]))
    (ceiling, reason), = rp._host_ceilings(profile, profile["classes"][2], rp.normalize_workload(WORKLOAD))
    assert ceiling == pytest.approx(6.4e6) and "extreme workers" in reason   # 12800 MB at 2000 MB per Mdraw


def _success(label="legacy-16gb", samples=None, **extra):
    fields = dict(_t(60), **({"memory samples": samples} if samples else {}))
    return dict({"class": label, "outcome": "success", "workload": WORKLOAD}, **fields, **extra)


def _built_classes(**extra):
    return [dict(CLASSES[1], **extra)]


def _plan_row(tmp_path, observations, classes):
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations, classes=classes))
    return rp.plan_resources(profile, WORKLOAD)["classes"][0]


def test_class_with_no_build_plans_as_before_and_reports_its_evidence(tmp_path):
    row = _plan_row(tmp_path, [_success(), _success(build="a.sif")], [CLASSES[1]])
    assert row["build"] is None
    assert row["evidence"] == {"used": 2, "set aside": 0, "unstamped used": 1, "builds used": ["a.sif"]}


def test_memory_fit_needs_the_class_build(tmp_path):
    old = _success(samples=_growing(500.0, 300.0), build="rc13.sif")
    new = _success(build="rc14.sif")
    row = _plan_row(tmp_path, [old, new], _built_classes(build="rc14.sif"))
    assert row["evidence"]["set aside"] == 1 and row["evidence"]["builds used"] == ["rc14.sif"]
    assert "300" not in row["memory"]
    assert "300" in _plan_row(tmp_path, [old, new], _built_classes(build="rc13.sif"))["memory"]


def test_family_fit_needs_the_class_build(tmp_path):
    other = dict(CLASSES[2], build="rc13.sif")
    old = _success("modern-24gb", samples=_growing(500.0, 300.0), build="rc13.sif")
    row = _plan_row(tmp_path, [old, _success(build="rc14.sif")], _built_classes(build="rc14.sif") + [other])
    assert "family" not in row["memory"] and row["evidence"]["set aside"] == 1


def test_unstamped_observations_are_set_aside_unless_the_class_names_their_build(tmp_path):
    with pytest.raises(rp.ResourceProfileError, match="1 observations of other builds set aside"):
        _plan_row(tmp_path, [_success()], _built_classes(build="rc14.sif"))
    row = _plan_row(tmp_path, [_success()], _built_classes(**{"build": "rc13.sif", "unstamped build": "rc13.sif"}))
    assert row["evidence"]["used"] == 1 and row["evidence"]["unstamped used"] == 1
    # the next rebuild leaves them behind with no further edit
    with pytest.raises(rp.ResourceProfileError, match="set aside"):
        _plan_row(tmp_path, [_success()], _built_classes(**{"build": "rc14.sif", "unstamped build": "rc13.sif"}))


@pytest.mark.parametrize("failed_build, capped", [("rc13.sif", False), ("rc14.sif", True)])
def test_failure_caps_only_its_own_build(tmp_path, failed_build, capped):
    crash = {"class": "legacy-16gb", "outcome": "crash", "workload": WORKLOAD, "failure draws": [9.5e6],
             "build": failed_build}
    row = _plan_row(tmp_path, [_success(build="rc14.sif"), crash], _built_classes(build="rc14.sif"))
    assert ("crash on" in row["memory"]) is capped


def test_host_request_ignores_other_builds(tmp_path):
    host = [[1e6 * (i + 1), 1000.0, 1000.0 + 900.0 * (i + 1)] for i in range(8)]
    flat = [[1e6 * (i + 1), 1000.0, 1000.0] for i in range(8)]
    observations = [_success(samples=host, build="rc13.sif", **{"host memory mb": 8000}),
                    _success(samples=flat, build="rc14.sif")]
    profile = rp.load_resource_profile(_write_profile(tmp_path, observations, classes=_built_classes(build="rc14.sif")))
    assert rp.plan_resources(profile, WORKLOAD)["request memory mb"] == 2000


def test_timing_transfers_across_builds(tmp_path):
    old = _success(build="rc13.sif")
    new = dict(_success(build="rc14.sif"), **{"draws per point": [], "draws per second": []})
    row = _plan_row(tmp_path, [old, new], _built_classes(build="rc14.sif"))
    assert row["draws per second"] == DPP / 60 and "excluded" not in row


@pytest.mark.parametrize("field", ["build", "unstamped build"])
def test_class_build_must_be_text(tmp_path, field):
    with pytest.raises(rp.ResourceProfileError, match=field):
        rp.load_resource_profile(_write_profile(tmp_path, classes=_built_classes(**{field: 3})))


def test_observation_build_must_be_text(tmp_path):
    with pytest.raises(rp.ResourceProfileError, match="observation build"):
        rp.load_resource_profile(_write_profile(tmp_path, [_success(build=3)]))


@pytest.mark.parametrize("builds, expected", [(None, None), ("one.sif", "one.sif"),
                                              ({"legacy-16gb": "two.sif"}, "two.sif"), ({"other": "x"}, None)])
def test_harvest_stamps_the_build(tmp_path, builds, expected):
    profile = rp.load_resource_profile(_write_profile(tmp_path))
    run = tmp_path / "run"
    _ile(run, "ILE-50-6-0", "Tesla T4", 75, AD_LOG.format(minutes=5, memory=1, rc=0),
         outputs=["CME_out-50-6-0.xml_0_.dat"])
    observations, _ = rp.harvest_rundir(str(run), profile, WORKLOAD, builds=builds)
    assert observations[0].get("build") == expected
