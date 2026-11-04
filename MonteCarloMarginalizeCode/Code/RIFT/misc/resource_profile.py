"""
resource_profile
================

Choose the ILE GPU requirement, host memory request and points per worker for
one run from a *resource profile*: a site-maintained YAML record of the GPU
classes a pool offers and of what earlier runs observed on them.  The design,
the decision rule and the schema rationale are in
``DESIGN_resource_profile.md`` beside this file.

A profile is a YAML file, or a directory holding ``resources.yaml`` plus any
number of ``observations/*.yaml`` files (one per harvested run, so concurrent
monitors never edit the same file).

    version: 1
    policy:                       # every key optional; defaults in POLICY_DEFAULTS
      worker wall target hours: 2
    classes:                      # most specific first: harvest maps a device to the
      - label: pascal-4gb         # first class it matches
        capability: [6.1, 7.0]    # [min, max) compute capability
        memory mb: 4096           # GlobalMemoryMb floor of the class
        memory mb max: 8191       # optional ceiling
        device names: ['1050']    # optional regexes on the device name
        slots: 400                # GPUs a run can typically hold at once
        relative speed: 0.3       # optional; transfers draws per second between classes
        memory family: cuda118    # optional; classes sharing an image share a memory fit
    observations:
      - class: pascal-4gb
        outcome: memory-failure   # success | memory-failure | crash | failure
        workload: {label: bbh-loud, n chunk: 40000, n eff: 20, sample rate: 4096,
                   q pregrid factor: 2, ifos: 2, approximant: IMRPhenomXPHM,
                   lmax: 4, time resampling: 32768}
        memory samples: [[12.1e6, 3649, 5400], ...]  # per worker: total draws, device MB, host MB
        failure draws: [6.1e6]    # per failed worker: draws in its completed points
        host memory mb: 6286      # optional
        draws per point: [...]    # optional: ILE ntotal of each point
        draws per second: [...]   # optional: one per worker, per GPU
        source: <path or job id>
"""

import glob
import hashlib
import math
import os
import re

__all__ = [
    "ResourceProfileError",
    "POLICY_DEFAULTS",
    "load_resource_profile",
    "normalize_workload",
    "classify_device",
    "plan_resources",
    "apply_plan_to_ini",
    "harvest_rundir",
]

POLICY_DEFAULTS = {
    "worker wall target hours": 2.0,
    "worker setup seconds": 300.0,
    "max seconds per point": 1800.0,
    "jobs per worker min": 1,
    "jobs per worker max": 200,
    "vram margin": 0.15,
    "host memory margin": 0.25,
    "host memory min mb": 2000,
    "host memory max mb": 16000,
    "timing quantile": 0.9,
    "requirement style": "exclude",
    "admit unprofiled": False,
}

# Fields that change the memory footprint.  Observations transfer between
# workloads only when all of these agree.  n chunk is not one of them: measured
# memory follows draws per worker, not chunk size (see DESIGN).
SIGNATURE_KEYS = ("sample rate", "q pregrid factor", "ifos", "approximant",
                  "lmax", "time resampling")
# Cost per point is draws per point / draws per second.  Draws per second is a
# property of the GPU class (it transfers when the signature and n chunk
# agree); draws per point is a property of the workload (signature, n chunk,
# n eff and label must agree) and is pooled over classes.
SPEED_KEYS = ("n chunk",)
TIMING_KEYS = ("n chunk", "n eff", "label")
BOOTSTRAP_SAMPLES = 1000
OUTCOMES = ("success", "memory-failure", "crash", "failure")
# Outcomes that cap the draws a worker may run on a class.
BLOCKING_OUTCOMES = ("memory-failure", "crash")
# A memory fit needs this many workers, spanning this factor in draws.
MIN_FIT_SAMPLES = 8
MIN_FIT_SPAN = 2.0
# The fit is shifted up to this quantile of its residuals, so it bounds workers.
FIT_ENVELOPE_QUANTILE = 0.99


class ResourceProfileError(Exception):
    """Raised for a malformed profile or a workload the profile cannot plan."""


def _load_yaml(path):
    try:
        import yaml
    except ImportError as exc:
        raise ResourceProfileError("resource profile {} needs pyyaml".format(path)) from exc
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except (OSError, yaml.YAMLError) as exc:
        raise ResourceProfileError("cannot read resource profile {}: {}".format(path, exc)) from exc


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _norm_value(value):
    """Compare numbers numerically (4096 == 4096.0) and everything else as text."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def normalize_workload(workload):
    """Return a workload dict with every key present and values comparable."""
    if not isinstance(workload, dict):
        raise ResourceProfileError("workload must be a mapping, got {!r}".format(workload))
    unknown = set(workload) - set(SIGNATURE_KEYS) - set(TIMING_KEYS)
    if unknown:
        raise ResourceProfileError("unknown workload keys: {}".format(sorted(unknown)))
    out = {key: _norm_value(workload.get(key)) for key in SIGNATURE_KEYS + TIMING_KEYS}
    if out["label"] is not None:
        out["label"] = str(workload["label"])
    n_chunk = out["n chunk"]
    if not isinstance(n_chunk, float) or n_chunk <= 0 or n_chunk != int(n_chunk):
        raise ResourceProfileError("workload n chunk must be a positive integer, got {!r}".format(
            workload.get("n chunk")))
    out["n chunk"] = int(n_chunk)
    return out


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_class(entry, path):
    if not isinstance(entry, dict):
        raise ResourceProfileError("{}: every class must be a mapping".format(path))
    label = entry.get("label")
    if not label or not isinstance(label, str):
        raise ResourceProfileError("{}: every class needs a string label".format(path))
    cap = entry.get("capability")
    if not (isinstance(cap, (list, tuple)) and len(cap) == 2 and _is_number(cap[0])
            and (cap[1] is None or _is_number(cap[1]))):
        raise ResourceProfileError("{}: class {} capability must be [min, max) numbers (max may be null)".format(
            path, label))
    if cap[1] is not None and float(cap[1]) <= float(cap[0]):
        raise ResourceProfileError("{}: class {} capability range is empty".format(path, label))
    mem = entry.get("memory mb")
    if not isinstance(mem, (int, float)) or isinstance(mem, bool) or mem <= 0:
        raise ResourceProfileError("{}: class {} needs a positive memory mb".format(path, label))
    for key in ("slots", "relative speed", "memory mb max"):
        value = entry.get(key)
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0):
            raise ResourceProfileError("{}: class {} {} must be a non-negative number".format(path, label, key))
    if entry.get("relative speed") == 0:
        raise ResourceProfileError("{}: class {} relative speed must be positive".format(path, label))
    if entry.get("memory family") is not None and not isinstance(entry["memory family"], str):
        raise ResourceProfileError("{}: class {} memory family must be a string".format(path, label))
    names = entry.get("device names") or []
    if isinstance(names, str) or not all(isinstance(n, str) for n in names):
        raise ResourceProfileError("{}: class {} device names must be a list of regexes".format(path, label))
    for name in names:
        if '"' in name:
            raise ResourceProfileError("{}: class {} device name {!r} may not contain a double quote".format(
                path, label, name))
        re.compile(name)
    return dict(entry, **{"device names": list(names)})


def _validate_observation(entry, labels, path):
    if not isinstance(entry, dict):
        raise ResourceProfileError("{}: observation must be a mapping".format(path))
    if entry.get("class") not in labels:
        raise ResourceProfileError("{}: observation class {!r} is not a profile class".format(
            path, entry.get("class")))
    if entry.get("outcome") not in OUTCOMES:
        raise ResourceProfileError("{}: observation outcome must be one of {}".format(path, OUTCOMES))
    obs = dict(entry)
    obs["workload"] = normalize_workload(entry.get("workload") or {})
    host = entry.get("host memory mb")
    if host is not None and not (_is_number(host) and host > 0):
        raise ResourceProfileError("{}: host memory mb must be a positive number, got {!r}".format(path, host))
    samples = entry.get("memory samples") or []
    for sample in samples:
        if not (isinstance(sample, (list, tuple)) and len(sample) == 3 and _is_number(sample[0]) and sample[0] > 0
                and all(v is None or (_is_number(v) and v > 0) for v in sample[1:])):
            raise ResourceProfileError("{}: memory samples must be [draws, device MB or null, host MB or null]".format(path))
    obs["memory samples"] = [list(sample) for sample in samples]
    failures = entry.get("failure draws") or []
    if not all(_is_number(v) and v >= 0 for v in failures):
        raise ResourceProfileError("{}: failure draws must be non-negative numbers".format(path))
    obs["failure draws"] = [float(v) for v in failures]
    for key in ("draws per point", "draws per second"):
        values = entry.get(key) or []
        if isinstance(values, (int, float)):
            values = [values]
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0 for v in values):
            raise ResourceProfileError("{}: {} must be positive numbers".format(path, key))
        obs[key] = [float(v) for v in values]
    return obs


def load_resource_profile(path, policy_overrides=None):
    """Load and validate a profile file or directory.

    ``policy_overrides`` (from the blueprint) replace profile policy keys.
    The returned dict records every file read and its sha256.
    """
    path = os.path.abspath(os.path.expanduser(path))
    if os.path.isdir(path):
        main = os.path.join(path, "resources.yaml")
        extra = sorted(glob.glob(os.path.join(path, "observations", "*.yaml")))
    else:
        main, extra = path, []
    data = _load_yaml(main)
    if not isinstance(data, dict) or data.get("version") != 1:
        raise ResourceProfileError("{}: resource profile must be a mapping with version: 1".format(main))
    policy = dict(POLICY_DEFAULTS)
    for source in (data.get("policy") or {}), (policy_overrides or {}):
        unknown = set(source) - set(POLICY_DEFAULTS)
        if unknown:
            raise ResourceProfileError("{}: unknown policy keys {}".format(main, sorted(unknown)))
        policy.update(source)
    numeric = [k for k, v in POLICY_DEFAULTS.items()
               if isinstance(v, (int, float)) and not isinstance(v, bool)]
    for key in numeric:
        if isinstance(policy[key], bool) or not isinstance(policy[key], (int, float)):
            raise ResourceProfileError("policy {} must be a number, got {!r}".format(key, policy[key]))
    if not isinstance(policy["admit unprofiled"], bool):
        raise ResourceProfileError("policy admit unprofiled must be a YAML boolean")
    if policy["requirement style"] not in ("exclude", "include"):
        raise ResourceProfileError("requirement style must be exclude or include")
    if not 0 <= policy["vram margin"] < 1:
        raise ResourceProfileError("vram margin must be in [0, 1)")
    if not 0 < policy["timing quantile"] <= 1:
        raise ResourceProfileError("timing quantile must be in (0, 1]")
    if not 1 <= int(policy["jobs per worker min"]) <= int(policy["jobs per worker max"]):
        raise ResourceProfileError("need 1 <= jobs per worker min <= jobs per worker max")
    classes = [_validate_class(c, main) for c in (data.get("classes") or [])]
    labels = [c["label"] for c in classes]
    if not classes or len(set(labels)) != len(labels):
        raise ResourceProfileError("{}: classes must be a nonempty list with unique labels".format(main))
    observations = [_validate_observation(o, labels, main) for o in (data.get("observations") or [])]
    for name in extra:
        chunk = _load_yaml(name)
        if isinstance(chunk, dict):
            chunk = chunk.get("observations")
        if not isinstance(chunk, list):
            raise ResourceProfileError("{}: expected a list of observations".format(name))
        observations += [_validate_observation(o, labels, name) for o in chunk]
    return {
        "source": path,
        "files": {name: _sha256(name) for name in [main] + extra},
        "policy": policy,
        "classes": classes,
        "observations": observations,
    }


def classify_device(profile, device_name, capability, memory_mb=None):
    """Return the label of the first class a device matches, or None."""
    for c in profile["classes"]:
        lo, hi = c["capability"]
        if capability < float(lo) or (hi is not None and capability >= float(hi)):
            continue
        if memory_mb is not None and (memory_mb < c["memory mb"]
                                      or memory_mb > c.get("memory mb max", float("inf"))):
            continue
        if c["device names"] and not any(re.search(p, device_name or "") for p in c["device names"]):
            continue
        return c["label"]
    return None


def _class_selector(c):
    """ClassAd selector for one class, in require_gpus attribute names."""
    lo, hi = c["capability"]
    terms = ["(Capability >= {})".format(float(lo))]
    if hi is not None:
        terms.append("(Capability < {})".format(float(hi)))
    terms.append("(GlobalMemoryMb >= {})".format(int(c["memory mb"])))
    if c.get("memory mb max") is not None:
        terms.append("(GlobalMemoryMb <= {})".format(int(c["memory mb max"])))
    if c["device names"]:
        terms.append("(" + " || ".join('regexp("{}", DeviceName)'.format(p.replace("\\", "\\\\"))
                                       for p in c["device names"]) + ")")
    return "(" + " && ".join(terms) + ")"


def _same_signature(a, b):
    return all(a[k] == b[k] for k in SIGNATURE_KEYS)


def _quantile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int(math.ceil(q * len(ordered))) - 1))]


def _same_family(a, b):
    return a.get("memory family") == b.get("memory family")


def _fit_memory(pairs):
    """(base MB, MB per draw) bounding (draws, MB) pairs, or None if they cannot fix a line."""
    import numpy as np
    if len(pairs) < MIN_FIT_SAMPLES:
        return None
    x, y = np.array(pairs, dtype=float).T
    if x.max() < MIN_FIT_SPAN * x.min():
        return None
    slope, base = np.polyfit(x, y, 1)
    slope = max(float(slope), 0.0)
    residual = y - slope * x
    return _quantile(residual.tolist(), FIT_ENVELOPE_QUANTILE), slope


def _memory_model(profile, c, workload, column):
    """Fit of memory against draws per worker for a class, else for its family.

    ``column`` is 1 for device and 2 for host memory.  Returns (fit, source);
    fit is None when neither has enough samples.
    """
    by_label = {k["label"]: k for k in profile["classes"]}

    def pairs(keep):
        return [(sample[0], sample[column]) for o in profile["observations"]
                if o["outcome"] == "success" and keep(by_label[o["class"]])
                and _same_signature(o["workload"], workload)
                for sample in o["memory samples"] if sample[column] is not None]

    own = pairs(lambda k: k["label"] == c["label"])
    fit = _fit_memory(own)
    if fit is not None:
        return fit, "{} workers on {}".format(len(own), c["label"])
    if c.get("memory family") is not None:
        family = pairs(lambda k: _same_family(k, c))
        fit = _fit_memory(family)
        if fit is not None:
            return fit, "{} workers in family {}".format(len(family), c["memory family"])
    return None, "too few memory samples"


def _success_reach(o):
    """Most draws one worker of this observation is known to have completed."""
    reach = [sample[0] for sample in o["memory samples"]] + o["draws per point"]
    return max(reach) if reach else 0.0


def _draw_ceiling(profile, c, workload):
    """(most draws a worker may run on this class, reason); (None, reason) if unknown.

    The ceiling is the least of what the memory fit allows and what failures
    show.  A failure on a card with at least this much memory, in this family,
    caps the class at the draws that worker completed, unless a worker on a
    card with no more memory completed as many.  Without a fit, a class is
    trusted only as far as such a worker has gone.
    """
    by_label = {k["label"]: k for k in profile["classes"]}
    mem = c["memory mb"]
    comparable = [o for o in profile["observations"] if _same_signature(o["workload"], workload)
                  and _same_family(by_label[o["class"]], c)]
    reach = max([_success_reach(o) for o in comparable
                 if o["outcome"] == "success" and by_label[o["class"]]["memory mb"] <= mem] or [0.0])
    failures = [(draws, o) for o in comparable if o["outcome"] in BLOCKING_OUTCOMES
                and by_label[o["class"]]["memory mb"] >= mem
                for draws in (o["failure draws"] or [0.0]) if draws >= reach]
    ceilings = []
    if failures:
        draws, o = min(failures, key=lambda item: item[0])
        ceilings.append((draws, "{} on {} after {:.3g} draws".format(o["outcome"], o["class"], draws)))
    fit, source = _memory_model(profile, c, workload, 1)
    if fit is not None:
        base, slope = fit
        usable = mem * (1 - profile["policy"]["vram margin"])
        allowed = float("inf") if slope == 0 else max(0.0, (usable - base) / slope)
        ceilings.append((allowed, "{:.0f} MB + {:.3g} MB per Mdraw ({}), {:.0f} MB usable".format(
            base, slope * 1e6, source, usable)))
    elif reach:
        ceilings.append((reach, "completed {:.3g} draws, no memory fit".format(reach)))
    # Host memory grows the same way, and a larger request matches fewer slots.
    host_fit, source = _memory_model(profile, c, workload, 2)
    if host_fit is not None and host_fit[1] > 0:
        policy = profile["policy"]
        allowed = float(policy["host memory max mb"]) / (1 + float(policy["host memory margin"]))
        ceilings.append((max(0.0, (allowed - host_fit[0]) / host_fit[1]),
                         "host {:.0f} MB + {:.3g} MB per Mdraw ({}), {:.0f} MB allowed".format(
                             host_fit[0], host_fit[1] * 1e6, source, allowed)))
    if not ceilings:
        return None, "no comparable memory evidence"
    return min(ceilings, key=lambda item: item[0])


def _samples(profile, workload, key, keys, label=None):
    return [v for o in profile["observations"]
            if o["outcome"] == "success" and (label is None or o["class"] == label)
            and _same_signature(o["workload"], workload)
            and all(o["workload"][k] == workload[k] for k in keys)
            for v in o[key]]


def _rate_samples(profile, workload, label):
    """Draws per second for a class at this n chunk, else at smaller ones.

    A smaller chunk keeps the GPU less busy, so its rate is a conservative
    stand-in for a larger chunk; the converse does not hold.
    """
    exact = _samples(profile, workload, "draws per second", SPEED_KEYS, label)
    if exact:
        return exact, "measured"
    smaller = [v for o in profile["observations"]
               if o["outcome"] == "success" and o["class"] == label
               and _same_signature(o["workload"], workload)
               and o["workload"]["n chunk"] < workload["n chunk"]
               for v in o["draws per second"]]
    return smaller, "measured at smaller n chunk"


def _draw_rate(profile, c, workload):
    """(draws per second, provenance) for a class, low quantile, or (None, reason)."""
    q = 1 - profile["policy"]["timing quantile"]
    own, how = _rate_samples(profile, workload, c["label"])
    if own:
        return _quantile(own, q), "{}, {} workers".format(how, len(own))
    if c.get("relative speed"):
        refs = [(_rate_samples(profile, workload, r["label"]), r)
                for r in profile["classes"] if r.get("relative speed")]
        (samples, how), ref = max(refs, key=lambda item: len(item[0][0])) if refs else (([], ""), None)
        if samples:
            return (_quantile(samples, q) * c["relative speed"] / ref["relative speed"],
                    "{} on {} ({} workers), scaled by relative speed".format(how, ref["label"], len(samples)))
    return None, "no draws per second for this signature at or below this n chunk"


def _wall_quantiles(draws, n_max, q):
    """Quantile q of the draws needed for 1..n_max points, by bootstrap.

    Each bootstrap row is one sequence of points, and the sum for n points is
    its first n entries, so the quantiles are non-decreasing in n.
    """
    import numpy as np
    if len(draws) == 1:
        return draws[0] * np.arange(1, n_max + 1)
    rng = np.random.default_rng(0)
    sums = np.cumsum(rng.choice(np.asarray(draws), size=(BOOTSTRAP_SAMPLES, n_max)), axis=1)
    return np.quantile(sums, q, axis=0)


def _points_per_worker(walls, rate, n_min, n_max, budget):
    """Largest n in [n_min, n_max] whose quantile runtime fits the budget, or None."""
    fits = [n for n in range(n_min, n_max + 1) if walls[n - 1] / rate <= budget]
    return fits[-1] if fits else None


def _host_request(profile, workload, admitted, by_label, draws):
    """Host MB request covering a worker of ``draws`` draws on any admitted class."""
    policy = profile["policy"]
    needs = []
    for r in admitted:
        fit, _ = _memory_model(profile, by_label[r["label"]], workload, 2)
        if fit is not None:
            needs.append(fit[0] + fit[1] * draws)
    if not needs:
        needs = [float(o["host memory mb"]) for o in profile["observations"]
                 if o.get("host memory mb") is not None and _same_signature(o["workload"], workload)]
    if not needs:
        return None
    request = max(float(policy["host memory min mb"]), max(needs) * (1 + float(policy["host memory margin"])))
    return int(math.ceil(request / 100.0) * 100)


def plan_resources(profile, workload, draws_per_point=None, jobs_per_worker_cap=None):
    """Choose admitted GPU classes, points per worker, host memory and require_gpus.

    ``draws_per_point`` is a blueprint estimate of ILE draws per point, used
    only when the profile has no comparable observation.
    ``jobs_per_worker_cap`` is an explicit ledger value; the plan never exceeds it.
    """
    workload = normalize_workload(workload)
    policy = profile["policy"]
    q = float(policy["timing quantile"])
    budget = float(policy["worker wall target hours"]) * 3600.0 - float(policy["worker setup seconds"])
    setup = float(policy["worker setup seconds"])
    n_min, n_max = int(policy["jobs per worker min"]), int(policy["jobs per worker max"])
    if jobs_per_worker_cap is not None:
        n_max = min(n_max, int(jobs_per_worker_cap))
        n_min = min(n_min, n_max)
    draws = _samples(profile, workload, "draws per point", TIMING_KEYS)
    draws_source = "measured, {} points".format(len(draws))
    if not draws and draws_per_point:
        draws, draws_source = [float(draws_per_point)], "blueprint draws per point"
    mean_draws = sum(draws) / len(draws) if draws else None
    walls = _wall_quantiles(draws, n_max, q) if draws else None

    rows, candidates = [], []
    for c in profile["classes"]:
        row = {"label": c["label"], "memory mb": c["memory mb"], "slots": c.get("slots", 0)}
        ceiling, row["memory"] = _draw_ceiling(profile, c, workload)
        rate, row["speed"] = _draw_rate(profile, c, workload)
        row["draws per second"] = rate
        row["max worker draws"] = ceiling
        row["seconds per point"] = _quantile(draws, q) / rate if (rate and draws) else None
        if ceiling is None and policy["admit unprofiled"]:
            ceiling = float("inf")
        limit = None
        if ceiling is None:
            row["excluded"] = "memory: " + row["memory"]
        elif not draws:
            row["excluded"] = "timing: no draws per point for this workload"
        elif rate is None:
            row["excluded"] = "timing: " + row["speed"]
        elif row["seconds per point"] > float(policy["max seconds per point"]):
            row["excluded"] = "{:.0f} s per point exceeds max seconds per point".format(row["seconds per point"])
        elif not row["slots"]:
            row["excluded"] = "no slots"
        else:
            fits = [n for n in range(n_min, n_max + 1) if walls[n - 1] <= ceiling]
            timed = _points_per_worker(walls, rate, n_min, n_max, budget)
            if not fits:
                row["excluded"] = "memory: {} points exceed it: {}".format(n_min, row["memory"])
            elif timed is None:
                row["excluded"] = "cannot finish {} points within the wall target".format(n_min)
            else:
                limit = min(fits[-1], timed)
        row["points limit"] = limit
        if limit is not None:
            candidates.append(row)
        rows.append(row)
    if not candidates:
        raise ResourceProfileError("no GPU class can run this workload: " + "; ".join(
            "{}: {}".format(r["label"], r["excluded"]) for r in rows))

    # One points-per-worker value serves the whole DAG.  At a given N every class
    # that can run N points is worth admitting, so only each class's own limit
    # needs trying.  Keep the N with the highest pool throughput; ties go to
    # the smaller N, which has the larger, more robust pool.
    best = None
    for n in sorted({r["points limit"] for r in candidates}, reverse=True):
        admitted = [r for r in candidates if r["points limit"] >= n]
        rate = sum(r["slots"] * n / (n * mean_draws / r["draws per second"] + setup) for r in admitted)
        if best is None or rate >= best[2]:
            best = (admitted, n, rate)
    admitted, n, rate = best
    admitted_labels = {r["label"] for r in admitted}
    for r in rows:
        if r["label"] not in admitted_labels and "excluded" not in r:
            r["excluded"] = "its limit of {} points is below the throughput-optimal {}".format(r["points limit"], n)
    by_label = {c["label"]: c for c in profile["classes"]}
    if policy["requirement style"] == "include":
        require = " || ".join(_class_selector(by_label[r["label"]]) for r in admitted)
    else:
        # A card no excluded class describes may match, so unlisted cards are not
        # locked out; a card an admitted class describes always may, even when a
        # broader excluded class also describes it.
        floor = min(r["memory mb"] for r in admitted)
        excluded = ["!" + _class_selector(by_label[r["label"]]) for r in rows if r["label"] not in admitted_labels]
        allowed = [_class_selector(by_label[r["label"]]) for r in admitted]
        if excluded:
            allowed.append("(" + " && ".join(excluded) + ")")
            require = "(GlobalMemoryMb >= {}) && ({})".format(int(floor), " || ".join(allowed))
        else:
            require = "(GlobalMemoryMb >= {})".format(int(floor))
    worker_draws = float(walls[n - 1])
    return {
        "profile": profile["source"],
        "profile files": profile["files"],
        "policy": policy,
        "workload": workload,
        "draws per point": {"source": draws_source, "mean": mean_draws, "quantile": _quantile(draws, q)},
        "worker draws": worker_draws,
        "jobs per worker": n,
        "admitted": [r["label"] for r in admitted],
        "require gpus": "(" + require + ")",
        "request memory mb": _host_request(profile, workload, admitted, by_label, worker_draws),
        "points per hour": rate * 3600.0,
        "slowest worker seconds": setup + worker_draws / min(r["draws per second"] for r in admitted),
        "classes": rows,
    }


def apply_plan_to_ini(ini_in, ini_out, plan):
    """Write a copy of the pseudo_pipe ini with the planned values set.

    pseudo_pipe lets the ini override its command line, so the plan has to
    live in the ini rather than on the command line.  Iteration 0 gets the
    same points per worker.
    """
    import configparser
    settings = configparser.ConfigParser(interpolation=None)
    settings.optionxform = str
    if not settings.read(ini_in):
        raise ResourceProfileError("cannot read ini {}".format(ini_in))
    if not settings.has_section("rift-pseudo-pipe"):
        raise ResourceProfileError("ini {} has no [rift-pseudo-pipe] section".format(ini_in))
    section = settings["rift-pseudo-pipe"]
    # n chunk is pinned too: the helper otherwise scales it with SNR, and the
    # plan's memory and timing evidence holds only at the planned value.
    values = {"ile-jobs-per-worker": int(plan["jobs per worker"]),
              "ile-jobs-per-worker-first": int(plan["jobs per worker"]),
              "internal-ile-n-chunk": int(plan["workload"]["n chunk"])}
    if plan.get("request memory mb"):
        values["internal-ile-request-memory"] = int(plan["request memory mb"])
    for key, value in values.items():
        section.pop(key.replace("-", "_"), None)   # pseudo_pipe reads either spelling
        section[key] = str(value)
    with open(ini_out, "w") as f:
        settings.write(f)


# ---------------------------------------------------------------------------
# Harvest: turn a finished run's ILE logs into observations.

_DEV = re.compile(r"Device 0 Name\s*:\s*(.+)")
_CAP = re.compile(r"Device 0 Compute Capability\s*:\s*(\d+)")
_OOM = re.compile(r"OutOfMemoryError|cudaErrorMemoryAllocation|CUDA_ERROR_OUT_OF_MEMORY|out of memory",
                  re.IGNORECASE)
_AD_LINE = re.compile(r'^(\w+)\s*=\s*"?(.*?)"?\s*$')
_CLASSIC = re.compile(r"^(\d{3}) \(\d+\.\d+\.\d+\) (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)")


def _parse_condor_log(path):
    """Wall time, memory use, exit status and GPU memory of the last execution.

    GPU fields come from the ClassAd-format events (GlobalMemoryMb of the
    assigned GPU, GpusMemoryUsage at termination) and are None when absent.
    """
    import datetime
    try:
        lines = open(path, errors="ignore").read().splitlines()
    except OSError:
        return None
    text = "\n".join(lines)
    gpu_mem = re.findall(r"\bGlobalMemoryMb = (\d+)", text)
    gpu_peak = re.findall(r"\bGpusMemoryUsage = ([0-9.]+)", text)
    out = {"wall": None, "memory_mb": None, "return_value": None, "signal": None,
           "gpu_memory_mb": int(gpu_mem[0]) if gpu_mem else None,
           "gpu_peak_mb": float(gpu_peak[-1]) if gpu_peak else None}
    start = None
    if any(line.startswith("MyType = ") for line in lines):
        event = {}
        for line in lines + ["..."]:
            if line.startswith("..."):
                kind, when = event.get("MyType"), event.get("EventTime")
                stamp = datetime.datetime.fromisoformat(when[:19]) if when else None
                if kind == "ExecuteEvent":
                    start = stamp
                elif kind == "JobTerminatedEvent" and start and stamp:
                    out["wall"] = (stamp - start).total_seconds()
                    if event.get("MemoryUsage"):
                        out["memory_mb"] = float(event["MemoryUsage"])
                    if event.get("ReturnValue") not in (None, ""):
                        out["return_value"] = int(event["ReturnValue"])
                    if event.get("TerminatedBySignal") not in (None, ""):
                        out["signal"] = int(event["TerminatedBySignal"])
                event = {}
                continue
            m = _AD_LINE.match(line.strip())
            if m:
                event[m.group(1)] = m.group(2)
        return out
    for line in lines:
        m = _CLASSIC.match(line)
        if m:
            stamp = datetime.datetime.strptime(m.group(2), "%Y-%m-%d %H:%M:%S")
            if m.group(1) == "001":
                start = stamp
            elif m.group(1) == "005" and start:
                out["wall"] = (stamp - start).total_seconds()
        m = re.search(r"Normal termination \(return value (\d+)\)", line)
        if m:
            out["return_value"] = int(m.group(1))
        m = re.search(r"Abnormal termination \(signal (\d+)\)", line)
        if m:
            out["signal"] = int(m.group(1))
        m = re.match(r"\s*Memory \(MB\)\s*:\s*(\d+)", line)
        if m:
            out["memory_mb"] = float(m.group(1))
    return out


def harvest_rundir(rundir, profile, workload, source=None):
    """Observations for every ILE worker in ``rundir`` whose GPU matches a class.

    Returns (observations, skipped) where skipped counts workers with no GPU
    banner, no termination record, or a device no class matches.
    Draws per point are the ILE ``ntotal`` column of each output row.  Draws
    per second are per GPU, over the wall time less the policy worker setup
    time (at most half the wall): a worker that fanned out over K GPUs
    (``.gpuK`` output shards) ran K points at a time.  Memory samples pair a
    worker's total draws with condor's GpusMemoryUsage (single-GPU workers
    only) and MemoryUsage.
    Held-then-removed workers have no termination and are skipped.
    """
    workload = normalize_workload(workload)
    setup = float(profile["policy"]["worker setup seconds"])
    grouped, skipped = {}, {"not a grid worker": 0, "no gpu banner": 0, "no termination": 0,
                            "unclassified device": 0}
    for out_path in sorted(glob.glob(os.path.join(rundir, "iteration_*_ile", "logs", "ILE*-*.out"))):
        stem = os.path.basename(out_path)[:-4]
        try:
            text = open(out_path, errors="ignore").read()
        except OSError:
            continue
        kind, _, suffix = stem.partition("-")          # ILE-<event>-<cluster>-<proc>
        prefix = {"ILE": "CME_out", "ILE_puff": "CME_puff_out"}.get(kind)
        if prefix is None:                             # e.g. ILEextr: not a grid worker
            skipped["not a grid worker"] += 1
            continue
        dev, cap = _DEV.search(text), _CAP.search(text)
        if not (dev and cap):
            skipped["no gpu banner"] += 1
            continue
        log = _parse_condor_log(out_path[:-4] + ".log")
        if not log or log["wall"] is None:
            skipped["no termination"] += 1
            continue
        label = classify_device(profile, dev.group(1).strip(), int(cap.group(1)) / 10.0,
                                log["gpu_memory_mb"])
        if label is None:
            skipped["unclassified device"] += 1
            continue
        try:
            err = open(out_path[:-4] + ".err", errors="ignore").read()
        except OSError:
            err = ""
        shards, draws = set(), []
        if prefix:
            pattern = os.path.join(os.path.dirname(os.path.dirname(out_path)),
                                   "{}-{}.xml*_*_.dat".format(prefix, suffix))
            for name in glob.glob(pattern):
                try:
                    row = open(name).read().split()
                    ntotal = float(row[-2])           # ... lnL sigma_lnL ntotal neff
                except (OSError, IndexError, ValueError):
                    continue
                # ntotal is a whole number of draws; anything else is another row
                # layout (e.g. RIFT_HYPERPIPELINE_FORMAT has no ntotal column).
                if ntotal < 1 or ntotal != int(ntotal):
                    continue
                draws.append(ntotal)
                m = re.search(r"\.xml\.gpu([^_]+)_", name)
                shards.add(m.group(1) if m else "")
        points = len(draws)
        failed = bool(log["signal"]) or log["return_value"] != 0
        if failed and _OOM.search(err):
            outcome = "memory-failure"
        elif log["signal"] or log["return_value"] in (134, 139):
            outcome = "crash"
        elif log["return_value"] == 0 and points:
            outcome = "success"
        else:
            outcome = "failure"
        obs = grouped.setdefault((label, outcome), {
            "class": label, "outcome": outcome, "workload": dict(workload),
            "draws per point": [], "draws per second": [], "memory samples": [], "failure draws": [],
            "workers": 0,
            "source": source or os.path.abspath(rundir)})
        obs["workers"] += 1
        if log["memory_mb"]:
            obs["host memory mb"] = max(obs.get("host memory mb", 0.0), log["memory_mb"])
        if outcome in BLOCKING_OUTCOMES:
            obs["failure draws"].append(sum(draws))
        if outcome == "success":
            obs["draws per point"] += draws
            if draws:   # a fanned-out worker's device peak spans GPUs, so only host counts
                obs["memory samples"].append([sum(draws), log["gpu_peak_mb"] if len(shards) <= 1 else None,
                                              log["memory_mb"]])
            if log["wall"] > 0 and draws:
                busy = max(log["wall"] - setup, 0.5 * log["wall"])
                obs["draws per second"].append(sum(draws) / (busy * max(1, len(shards))))
    return list(grouped.values()), skipped
