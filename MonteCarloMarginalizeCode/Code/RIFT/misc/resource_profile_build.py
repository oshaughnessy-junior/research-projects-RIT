"""
resource_profile_build
======================

Build the site record that ``RIFT.misc.resource_profile`` plans from, the way
``containers/survey_scan`` builds a warmup record for a container family:

* ``survey_pool`` / ``update_resources``: condor_status inventory into the
  classes of ``resources.yaml``.  A class absent from a survey keeps its entry:
  glidein cards come and go, and one snapshot does not retire a class.
* ``workload_from_rundir``: the workload of any built run, planned or not, so
  old runs can be harvested.
* ``emit_probes`` / ``collect_probes``: one bounded ILE group per class, then
  its receipts as observations.

The commands and workflow are in ``containers/README.md``.  The worker that
runs inside the container is ``resource_probe_worker.py`` beside this file.
"""

import datetime
import glob
import json
import os
import re
import shlex
import shutil
import statistics
import subprocess

from RIFT.misc import resource_profile as rp
from RIFT.misc.container_manifest import _image_basename, has_ile_profiles, selection_order

__all__ = [
    "SURVEY_FIELDS",
    "parse_inventory",
    "survey_pool",
    "summarize_inventory",
    "update_resources",
    "refresh_relative_speeds",
    "DEFAULT_CONSTRAINT",
    "workload_from_arguments",
    "workload_from_rundir",
    "run_builds",
    "class_container",
    "check_built_run",
    "emit_probes",
    "collect_probes",
]

SURVEY_FIELDS = ("Name", "GPUs_DeviceName", "GPUs_Capability", "GPUs_GlobalMemoryMb",
                 "GPUs_DriverVersion", "DetectedGPUs")
# Slots a run cannot reach: START opens with a job-side opt-in, or is plain false.
# Evaluating Start here cannot tell them apart, since glidein START is false without
# a job.  condor_status prints =?= as "is".  Nothing here ever sets such an attribute.
# Character classes, because condor keeps both backslashes of an escaped dot.
OPT_IN_START = "^(false$|TARGET[.][A-Za-z0-9_]+ (is|=[?]=) true)"
ANY_GPU = "GPUs_DeviceName =!= undefined"
DEFAULT_CONSTRAINT = '{} && !regexp("{}", unparse(Start))'.format(ANY_GPU, OPT_IN_START)
# ILE options the workload signature reads; optparse also accepts --key=value.
WORKLOAD_OPTIONS = ("--n-chunk", "--n-eff", "--srate", "--q-time-pregrid-factor", "--channel-name",
                    "--approx", "--l-max", "--srate-resample-time-marginalization")
# A new class's memory bounds allow for a driver reporting slightly different memory.
MEMORY_TOLERANCE = 0.02
# Every ILE command line must reach the NoLoop likelihood (RIFT AGENTS.md REDLINE).
NOLOOP_FLAGS = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")
WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resource_probe_worker.py")
# Our own timeout in the worker: a worker that reaches it has good finished points.
TIMEOUT_RETURNCODE = 124
CONTAINER = re.compile(r"container_image|SingularityImage|rift_container_select", re.I)
HEADER = "# Written by util_RIFT_resource_profile.py survey; see containers/README.md.\n"


def parse_inventory(text):
    """Rows of tab-separated ``condor_status -af:t`` output, one dict per slot."""
    rows = []
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) == len(SURVEY_FIELDS):
            rows.append(dict(zip(SURVEY_FIELDS, (p.strip() for p in parts))))
    return rows


def survey_pool(constraint=DEFAULT_CONSTRAINT, pool=None):
    """Query condor_status.  Device names hold spaces and DetectedGPUs holds commas."""
    if shutil.which("condor_status") is None:
        raise rp.ResourceProfileError("condor_status not found on PATH")
    command = ["condor_status", "-constraint", constraint, "-af:t"] + list(SURVEY_FIELDS)
    if pool:
        command[1:1] = ["-pool", pool]
    done = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True, check=False)
    if done.returncode:
        raise rp.ResourceProfileError(done.stderr.strip() or "condor_status failed")
    return parse_inventory(done.stdout)


def _number(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def summarize_inventory(rows):
    """Devices in an inventory, counting physical GPUs.

    Slots of one machine repeat its GPUs, so GPUs are counted by id.  A slot
    advertises one device name, so a machine with mixed cards counts as its
    advertised card.
    """
    devices = {}
    for row in rows:
        name, cap = row["GPUs_DeviceName"], _number(row["GPUs_Capability"])
        memory = _number(row["GPUs_GlobalMemoryMb"])
        if name in ("", "undefined") or cap is None or memory is None:
            continue
        d = devices.setdefault((name, cap), {"name": name, "capability": cap, "memory": [],
                                             "drivers": set(), "ids": set()})
        d["memory"].append(memory)
        driver = _number(row["GPUs_DriverVersion"])
        if driver is not None:
            d["drivers"].add(driver)
        ids = [i.strip() for i in row["DetectedGPUs"].split(",")
               if i.strip() and i.strip() != "undefined"]
        d["ids"].update(ids or [row["Name"]])
    return [{"name": d["name"], "capability": d["capability"], "memory mb": int(min(d["memory"])),
             "memory mb max": int(max(d["memory"])), "gpus": len(d["ids"]),
             "drivers": sorted(d["drivers"])}
            for d in sorted(devices.values(), key=lambda d: (d["capability"], d["name"]))]


def _exact_name(name):
    """A regex matching this device name only: 'A10' must not match 'A100'."""
    return "^" + re.sub(r"[.$*+?()\[\]{}|]", r"[\g<0>]", name) + "$"


def _slug(name):
    return re.sub(r"[^a-z0-9]+", "-", re.sub(r"^(nvidia|geforce|tesla|quadro)\s+", "", name.lower())).strip("-")


def _image_label(manifest, capability):
    """The manifest container selection gives a card, or None if that cannot be told here.

    A branch with select_requirements is a ClassAd test on the slot, which this
    function cannot evaluate.  Past a cuda_capability_max, the submit selector
    and the runtime wrapper choose differently.
    """
    chosen = manifest["fallback"]
    for c in selection_order(manifest):
        low = c["cuda_capability_min"]
        if low is not None and capability < float(low):
            continue
        if c.get("select_requirements"):
            return None
        chosen = c["label"]
        break
    entry = next(c for c in manifest["containers"] if c["label"] == chosen)
    top = entry.get("cuda_capability_max")
    return None if top is not None and capability >= float(top) else chosen


def class_container(manifest, c):
    """The manifest entry selection gives every card of class ``c``, or None if that cannot be told."""
    low, high = (None if v is None else float(v) for v in c["capability"])
    edges = {float(m[key]) for m in manifest["containers"] for key in ("cuda_capability_min", "cuda_capability_max")
             if m.get(key) is not None}
    points = [low] + [v for v in edges if v > low and (high is None or v < high)]
    labels = {_image_label(manifest, point) for point in points}
    if None in labels or len(labels) != 1:
        return None
    return next(m for m in manifest["containers"] if m["label"] in labels)


def update_resources(resources, devices, manifest=None, keep_slots=False):
    """Merge a survey into a ``resources.yaml`` mapping.  Returns (resources, report).

    A device an existing class describes updates that class's slots, unless
    ``keep_slots``.  Any other device gets a new class.  Classes the survey did
    not see keep their entry and slots, and are reported: only the ratio of
    slots matters, so their slots may then be on another scale.  With a
    manifest, every class's memory family and build are the image selection
    gives it, and plans set aside memory evidence from any other build.  A
    class whose image cannot be told keeps both, and is reported.
    """
    data = dict(resources or {"version": 1})
    classes = [dict(c) for c in (data.get("classes") or [])]
    lookup = {"classes": [dict(c, **{"device names": c.get("device names") or []}) for c in classes]}
    seen, added, moved, rebuilt, unknown, drivers = {}, [], [], [], [], {}
    for device in devices:
        label = rp.classify_device(lookup, device["name"], device["capability"], device["memory mb"])
        if label is None:
            base = label = _slug(device["name"])
            count = 1
            while any(c["label"] == label for c in classes):
                count += 1
                label = "{}-{}".format(base, count)
            entry = {"label": label, "capability": [device["capability"], round(device["capability"] + 0.1, 1)],
                     "memory mb": int(device["memory mb"] * (1 - MEMORY_TOLERANCE)),
                     "memory mb max": int(device["memory mb max"] * (1 + MEMORY_TOLERANCE)) + 1,
                     "device names": [_exact_name(device["name"])], "slots": 0}
            classes.append(entry)
            lookup["classes"].append(entry)
            added.append(label)
        seen[label] = seen.get(label, 0) + device["gpus"]
        drivers.setdefault(label, set()).update(device["drivers"])
    for c in classes:
        if c["label"] in seen and not (keep_slots and c.get("slots")):
            c["slots"] = seen[c["label"]]
        if manifest is None:
            continue
        entry = class_container(manifest, c)
        if entry is None:
            unknown.append(c["label"])
            continue
        if c.get("memory family") not in (None, entry["label"]):
            moved.append("{}: {} to {}".format(c["label"], c["memory family"], entry["label"]))
        c["memory family"] = entry["label"]
        if c.get("build") != entry["image"]:
            rebuilt.append("{}: {} to {}".format(c["label"], c.get("build"), entry["image"]))
            c["build"] = entry["image"]
    data["classes"] = classes
    return data, {"added": added, "image moved": moved, "build changed": rebuilt, "image unknown": unknown,
                  "seen": seen,
                  "absent": [c["label"] for c in classes if c["label"] not in seen],
                  "drivers": {k: sorted(v) for k, v in drivers.items()}}


def refresh_relative_speeds(resources, profile):
    """Set ``relative speed`` from observed draws per second.  Returns the labels set.

    Rates are comparable only at one signature and n chunk, so this uses the
    group of those shared by the most classes.  The class in the group with the
    most rates and a speed keeps it, which keeps other classes on the same
    scale.  With no speed in the group and one elsewhere, nothing is set.
    """
    groups = {}
    for o in profile["observations"]:
        rates = o.get("draws per second")
        if rates:
            w = o["workload"]
            key = tuple(w[k] for k in rp.SIGNATURE_KEYS) + (w["n chunk"],)
            groups.setdefault(key, {}).setdefault(o["class"], []).extend(rates)
    if not groups:
        return []
    group = max(groups.values(), key=lambda g: (len(g), sum(len(r) for r in g.values())))
    if len(group) < 2:
        return []
    by_label = {c["label"]: c for c in resources["classes"]}
    anchored = [label for label in group if by_label[label].get("relative speed")]
    if not anchored and any(c.get("relative speed") for c in resources["classes"]):
        return []
    reference = max(anchored or group, key=lambda label: len(group[label]))
    scale = (by_label[reference].get("relative speed") or 1.0) / statistics.median(group[reference])
    for label, rates in group.items():
        by_label[label]["relative speed"] = round(statistics.median(rates) * scale, 3)
    return sorted(group)


def workload_from_arguments(arguments, label):
    """Workload keys from an ILE argument list.  The last value of a repeated option wins."""
    joined = [a for a in arguments if a.split("=", 1)[0] in WORKLOAD_OPTIONS and "=" in a]
    if joined:
        raise rp.ResourceProfileError("write ILE options as separate words, not {}".format(joined))

    def last(name):
        values = [arguments[i + 1] for i, a in enumerate(arguments[:-1]) if a == name]
        return values[-1] if values else None

    missing = [k for k in ("--n-chunk", "--srate", "--approx") if last(k) is None]
    if missing:
        raise rp.ResourceProfileError("ILE arguments lack {}".format(missing))
    return {"label": label, "n chunk": int(float(last("--n-chunk"))), "n eff": last("--n-eff"),
            "sample rate": last("--srate"), "q pregrid factor": last("--q-time-pregrid-factor") or 1,
            "ifos": sum(a == "--channel-name" for a in arguments), "approximant": last("--approx"),
            "lmax": last("--l-max"), "time resampling": last("--srate-resample-time-marginalization")}


def _read_ile_sub(rundir):
    """(submit file text, ILE arguments) of a built run."""
    try:
        with open(os.path.join(rundir, "ILE.sub")) as f:
            text = f.read()
    except OSError as exc:
        raise rp.ResourceProfileError("{}: no ILE.sub: {}".format(rundir, exc)) from exc
    found = re.search(r'^arguments\s*=\s*"(.*)"\s*$', text, re.M)
    if not found:
        raise rp.ResourceProfileError("{}: ILE.sub has no arguments line".format(rundir))
    # condor's new argument syntax doubles a quote inside a quoted word
    return text, shlex.split(found.group(1).replace("''", "'"))


def workload_from_rundir(rundir, label=None):
    """A run's workload: its ``resource-plan.json``, else its ``ILE.sub`` arguments."""
    plan = os.path.join(rundir, "resource-plan.json")
    if os.path.exists(plan):
        with open(plan) as f:
            return json.load(f)["workload"]
    _, arguments = _read_ile_sub(rundir)
    return workload_from_arguments(arguments, label or os.path.basename(os.path.normpath(rundir)))


def _literal_image(text):
    """The one image a submit file names outright, or None.

    A transferred image runs as ``./name``, so its source is in the transfer list.
    """
    images = re.findall(r'^\s*(?:container_image|[^\s#]*SingularityImage)\s*=\s*"?([^"\s$()]+)"?\s*$',
                        text, re.M | re.I)
    if len(images) != 1:
        return None
    if not images[0].startswith("./"):
        return images[0]
    lists = re.findall(r'^\s*(?:transfer_input_files|\S*TransferInput)\s*=\s*(.*)$', text, re.M | re.I)
    sources = {name.strip(' "\\') for line in lists for name in line.split(",")}
    sources = [name for name in sources if "/" in name and name.split("/")[-1] == images[0][2:]]
    return sources[0] if len(sources) == 1 else images[0]


def _names_image(launched, image):
    """True if the text names ``image``, and no other image of that file name.

    A selector can carry the bare name, which build_family.sh reuses across builds.
    """
    name = _image_basename(image)
    text = re.sub(r"^\s*#.*$", "", launched, flags=re.M)
    found = {t for t in re.findall(r"[^\s,\"'()\[\]=]+", text) if t.split("/")[-1] == name}
    return bool(found) and found <= {image, name, "./" + name}


def run_builds(rundir, override=None):
    """What to stamp on a run's observations: a build, a class-to-build mapping, or None.

    ``override``, else the run's one literal image, else the builds its plan
    used.  check_built_run ties those to the image each class ran.  A family
    run with no plan matched an image per slot, and cannot be told here.
    """
    if override:
        return override
    try:
        text = _read_ile_sub(rundir)[0]
    except rp.ResourceProfileError:
        return None
    image = _literal_image(text)
    if image or not CONTAINER.search(text):
        return image
    try:
        with open(os.path.join(rundir, "resource-plan.json")) as f:
            builds = {r["label"]: r.get("build") for r in json.load(f)["classes"]}
    except (OSError, ValueError, KeyError):
        return None
    return builds if any(builds.values()) else None


def _build_differences(text, launched, plan, manifest):
    """Admitted classes whose build is not the image the built run gives them."""
    image = _literal_image(text)
    contained = image is not None or CONTAINER.search(launched)
    differences = []
    for r in plan.get("classes", ()):
        if r["label"] not in plan["admitted"] or not r.get("build"):
            continue
        entry = class_container(manifest, r) if contained and image is None and manifest else None
        if entry and not _names_image(launched, entry["image"]):
            entry = None                               # the run did not come from this manifest
        built = image or (entry and entry["image"])
        if built != r["build"]:
            differences.append("image: planned {} for {}, built {}".format(
                r["build"], r["label"], built or ("one that cannot be told" if contained else "no container")))
    return differences


def check_built_run(rundir, plan, manifest=None):
    """Differences between a plan and the ``ILE.sub`` the builder wrote, as text.

    The plan is made from ledger keys, and other keys can reach ILE too.  Memory
    and speed evidence holds only if the job runs what was planned.  ``manifest``
    is the container family of a run that selects its image per slot.
    """
    text, arguments = _read_ile_sub(rundir)
    built = rp.normalize_workload(workload_from_arguments(arguments, None))
    planned = rp.normalize_workload(plan["workload"])
    differences = ["{}: planned {}, built {}".format(key, planned[key], built[key])
                   for key in rp.SIGNATURE_KEYS + ("n chunk", "n eff") if planned[key] != built[key]]
    missing = [flag for flag in NOLOOP_FLAGS if flag not in arguments]
    if missing:
        differences.append("ILE misses {}: profile evidence is for NoLoop".format(" ".join(missing)))
    executable = re.search(r"^executable\s*=\s*(\S+)", text, re.M | re.I)
    wrapper = os.path.join(rundir, executable.group(1)) if executable else ""
    launched = text
    if wrapper.endswith(".sh") and os.path.isfile(wrapper):  # pseudo_pipe names ILE inside ile_pre.sh
        with open(wrapper, errors="replace") as f:
            launched += f.read()
    if "integrate_likelihood_extrinsic_jax" in launched:
        differences.append("JAX ILE: profile evidence is for the batchmode executable")
    differences += _build_differences(text, launched, plan, manifest)
    memory = re.search(r"^request_memory\s*=\s*(\d+)", text, re.M | re.I)
    wanted = plan.get("request memory mb")
    if wanted and (memory is None or int(memory.group(1)) != int(wanted)):
        differences.append("request_memory: planned {}, built {}".format(wanted, memory and memory.group(1)))
    require = re.search(r"^require_gpus\s*=\s*(.*)$", text, re.M | re.I)
    if require is None or plan["require gpus"] not in require.group(1):
        differences.append("require_gpus does not carry the plan")
    return differences


SUBMIT = """universe = container
container_image = {image}
executable = {python}
arguments = resource_probe_worker.py
transfer_executable = False
getenv = False
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_input_files = resource_probe_worker.py,probe.json,{case}
transfer_output_files = results
output = worker.out
error = worker.err
log = worker.events
request_cpus = 1
request_memory = {memory}M
request_disk = {disk}M
request_gpus = 1
require_gpus = {require_gpus}
requirements = (HAS_SINGULARITY =?= TRUE) && (TARGET.GPUs_Capability =!= undefined)
{extra}periodic_remove = ((JobStatus == 2) && (time() - EnteredCurrentStatus > {run_limit})) || ((JobStatus == 5) && (time() - EnteredCurrentStatus > 3600)) || ((JobStatus == 1) && (time() - QDate > {idle_limit}))
queue 1
"""


def emit_probes(profile, case, arguments, out, label, image=None, manifest=None, classes=None,
                points=10, event=0, n_chunk=None, n_eff=None, n_max=None, python="python3",
                ile="integrate_likelihood_extrinsic_batchmode", memory=12000, disk=24000,
                timeout=10800, idle_limit=86400, pools=None, accounting_group=None,
                accounting_user=None):
    """One job directory per class under ``out``.  Returns the submit files.

    ``case`` is a directory of ILE inputs, and ``arguments`` an ILE argument
    list whose paths are relative to it.  Each job runs ``points`` points on one
    GPU of its class, so the memory it records matches a worker of that size.
    Jobs never set a job-side opt-in attribute.
    """
    missing = [flag for flag in NOLOOP_FLAGS if flag not in arguments]
    if missing:
        raise rp.ResourceProfileError("ILE arguments must reach NoLoop; missing {}".format(missing))
    if manifest is not None and has_ile_profiles(manifest):
        raise rp.ResourceProfileError("this manifest sets per-image ILE profiles; pass image and ile instead")
    # collect_probes needs this workload: fail here, before any GPU time is spent
    workload_from_arguments(list(arguments) + (["--n-chunk", str(n_chunk)] if n_chunk else []), label)
    if not os.path.isdir(case):
        raise rp.ResourceProfileError("case directory not found: {}".format(case))
    wanted = [c for c in profile["classes"] if classes is None or c["label"] in classes]
    unknown = set(classes or ()) - {c["label"] for c in wanted}
    if unknown or not wanted:
        raise rp.ResourceProfileError("no such classes: {}".format(sorted(unknown) or classes))
    extra = ""
    if pools:
        extra += 'MY.POOLS = "{}"\n'.format(pools)
    if accounting_group:
        extra += "accounting_group = {}\n".format(accounting_group)
    if accounting_user:
        extra += "accounting_group_user = {}\n".format(accounting_user)
    submits = []
    for c in wanted:
        chosen = image
        if manifest is not None and image is None:
            entry = class_container(manifest, c)
            if entry is None:
                raise rp.ResourceProfileError("cannot tell the image of class {}; pass image".format(c["label"]))
            chosen = entry["image"]
        if chosen is None:
            raise rp.ResourceProfileError("emit_probes needs an image or a manifest")
        directory = os.path.join(out, c["label"])
        os.makedirs(directory)
        shutil.copy(WORKER, os.path.join(directory, "resource_probe_worker.py"))
        shutil.copytree(case, os.path.join(directory, "case"))
        with open(os.path.join(directory, "probe.json"), "w") as f:
            json.dump({"label": label, "class": c["label"], "image": chosen, "ile": ile,
                       "arguments": list(arguments), "points": points, "event": event,
                       "n chunk": n_chunk, "n eff": n_eff, "n max": n_max, "timeout": timeout},
                      f, indent=1)
        auth = "use_oauth_services = scitokens\n" if chosen.startswith("osdf:") else ""
        path = os.path.join(directory, "probe.sub")
        with open(path, "w") as f:
            f.write(SUBMIT.format(image=chosen, python=python, case="case", memory=int(memory), disk=int(disk),
                                  require_gpus=rp._class_selector(c), extra=auth + extra,
                                  run_limit=int(timeout) + 1800, idle_limit=int(idle_limit)))
        submits.append(path)
    return submits


def collect_probes(out, profile, stamp=None):
    """Observations from the receipts under ``out``.  Returns (observations, skipped).

    The class comes from the device the job reports, not the job it was
    emitted for, and a job with no receipt or no known device is skipped.
    Draws per second run from the end of the first point to the end of the last,
    so the rate leaves out startup and compilation.  A job that stopped before
    any point carries no failure draws, and blocks its class only on a GPU OOM.
    Observations carry ``stamp`` as their build, else the job's image.
    """
    if not os.path.isdir(out):
        raise rp.ResourceProfileError("probe directory not found: {}".format(out))
    observations, skipped = [], []
    for directory in sorted(glob.glob(os.path.join(out, "*", ""))):
        path = os.path.join(directory, "results", "receipt.json")
        try:
            with open(path) as f:
                receipt = json.load(f)
            with open(os.path.join(directory, "probe.json")) as f:
                probe = json.load(f)
        except (OSError, ValueError):
            skipped.append(directory)
            continue
        gpu, run = receipt.get("gpu"), receipt.get("run")
        label = gpu and rp.classify_device(profile, gpu["name"], gpu["sm"] / 10.0, gpu["vram_bytes"] // 1048576)
        if not label or not run or not run.get("arguments"):
            skipped.append(directory)
            continue
        points = sorted((p for p in run["points"] if p["draws"] >= 1), key=lambda p: p["mtime"])
        draws = [p["draws"] for p in points]
        memory = run["sampled_memory"]
        device = memory.get("gpu_peak_used_mb") or None
        host = (memory.get("host_peak_rss_kb") or 0) / 1024.0 or None
        code = run["returncode"]
        noloop = not run.get("noloop_lost")
        complete = code == 0 and run["all_finite"] and noloop and len(draws) == probe["points"]
        o = {"class": label, "workload": workload_from_arguments(run["arguments"], probe["label"]),
             "source": path}
        if stamp or probe.get("image"):
            o["build"] = stamp or probe["image"]
        if complete or (code == TIMEOUT_RETURNCODE and run["all_finite"] and noloop
                        and not run.get("gpu_oom") and draws):
            o["outcome"] = "success"
            if draws:
                o["draws per point"] = draws
                o["memory samples"] = [[sum(draws), device, host, 1]]
            span = points[-1]["mtime"] - points[0]["mtime"] if points else 0
            if len(points) > 1 and span > 0:
                o["draws per second"] = [sum(draws[1:]) / span]
        elif noloop and (run.get("gpu_oom") or (draws and code < 0)):
            o["outcome"] = "memory-failure" if run.get("gpu_oom") else "crash"
            if draws:
                o["failure draws"] = [sum(draws)]
            if host:
                o["host memory mb"] = host
        else:
            o["outcome"] = "failure"
        observations.append(o)
    return observations, skipped


def write_resources(path, data):
    """Replace ``path`` atomically, keeping the file it replaces as ``path.previous``."""
    import yaml
    temporary = path + ".new"
    with open(temporary, "w") as f:
        f.write(HEADER)
        yaml.safe_dump(data, f, sort_keys=False)
    if os.path.exists(path):
        shutil.copy2(path, path + ".previous")
    os.replace(temporary, path)


def save_inventory(directory, rows, devices):
    """Keep this survey beside the profile: a later reader can check its slots."""
    os.makedirs(directory, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(directory, "inventory-{}.json".format(stamp))
    with open(path, "w") as f:
        json.dump({"utc": stamp, "devices": devices, "slots": len(rows)}, f, indent=1)
    return path
