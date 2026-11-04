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

__all__ = [
    "SURVEY_FIELDS",
    "parse_inventory",
    "survey_pool",
    "summarize_inventory",
    "update_resources",
    "refresh_relative_speeds",
    "workload_from_arguments",
    "workload_from_rundir",
    "emit_probes",
    "collect_probes",
]

SURVEY_FIELDS = ("Name", "GPUs_DeviceName", "GPUs_Capability", "GPUs_GlobalMemoryMb",
                 "GPUs_DriverVersion", "DetectedGPUs")
# Every ILE command line must reach the NoLoop likelihood (RIFT AGENTS.md REDLINE).
NOLOOP_FLAGS = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")
WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resource_probe_worker.py")
# Our own timeout in the worker: a worker that reaches it has good finished points.
TIMEOUT_RETURNCODE = 124
HEADER = "# Written by util_RIFT_resource_profile.py survey; see containers/README.md.\n"


def parse_inventory(text):
    """Rows of tab-separated ``condor_status -af:t`` output, one dict per slot."""
    rows = []
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) == len(SURVEY_FIELDS):
            rows.append(dict(zip(SURVEY_FIELDS, (p.strip() for p in parts))))
    return rows


def survey_pool(constraint="GPUs_DeviceName =!= undefined", pool=None):
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
    return "^" + re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", name) + "$"


def _slug(name):
    return re.sub(r"[^a-z0-9]+", "-", re.sub(r"^(nvidia|geforce|tesla|quadro)\s+", "", name.lower())).strip("-")


def _image_label(manifest, capability):
    """The manifest container that selection gives a card, as a memory family."""
    if manifest is None:
        return None
    for c in manifest["containers"]:     # sorted by cuda_capability_min, descending
        low = c.get("cuda_capability_min")
        if low is not None and capability >= float(low):
            return c["label"]
    return manifest.get("fallback")


def update_resources(resources, devices, manifest=None, keep_slots=False):
    """Merge a survey into a ``resources.yaml`` mapping.  Returns (resources, report).

    A device an existing class describes updates that class's slots, unless
    ``keep_slots``.  Any other device gets a new class.  Classes the survey did
    not see keep their entry and slots, and are reported: only the ratio of
    slots matters, so their slots may then be on another scale.  With a
    manifest, every class's memory family is the image selection gives it.
    """
    data = dict(resources or {"version": 1})
    classes = [dict(c) for c in (data.get("classes") or [])]
    lookup = {"classes": [dict(c, **{"device names": c.get("device names") or []}) for c in classes]}
    seen, added, moved, drivers = {}, [], [], {}
    for device in devices:
        label = rp.classify_device(lookup, device["name"], device["capability"], device["memory mb"])
        if label is None:
            base = label = _slug(device["name"])
            count = 1
            while any(c["label"] == label for c in classes):
                count += 1
                label = "{}-{}".format(base, count)
            entry = {"label": label, "capability": [device["capability"], round(device["capability"] + 0.1, 1)],
                     "memory mb": device["memory mb"], "memory mb max": device["memory mb max"],
                     "device names": [_exact_name(device["name"])], "slots": 0}
            classes.append(entry)
            lookup["classes"].append(entry)
            added.append(label)
        seen[label] = seen.get(label, 0) + device["gpus"]
        drivers.setdefault(label, set()).update(device["drivers"])
    for c in classes:
        if c["label"] in seen and not (keep_slots and c.get("slots")):
            c["slots"] = seen[c["label"]]
        image = _image_label(manifest, float(c["capability"][0]))
        if image is not None and c.get("memory family") != image:
            if c.get("memory family") is not None:
                moved.append("{}: {} to {}".format(c["label"], c["memory family"], image))
            c["memory family"] = image
    data["classes"] = classes
    return data, {"added": added, "image moved": moved, "seen": seen,
                  "absent": [c["label"] for c in classes if c["label"] not in seen],
                  "drivers": {k: sorted(v) for k, v in drivers.items()}}


def refresh_relative_speeds(resources, profile):
    """Set ``relative speed`` from observed draws per second.  Returns the labels set.

    Rates are comparable only at one signature and n chunk, so this uses the
    group of those shared by the most classes.  The class with the most rates
    keeps its speed (or takes 1), which keeps other classes on the same scale.
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
    reference = max(group, key=lambda label: len(group[label]))
    scale = (by_label[reference].get("relative speed") or 1.0) / statistics.median(group[reference])
    for label, rates in group.items():
        by_label[label]["relative speed"] = round(statistics.median(rates) * scale, 3)
    return sorted(group)


def workload_from_arguments(arguments, label):
    """Workload keys from an ILE argument list.  The last value of a repeated option wins."""
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


def workload_from_rundir(rundir, label=None):
    """A run's workload: its ``resource-plan.json``, else its ``ILE.sub`` arguments."""
    plan = os.path.join(rundir, "resource-plan.json")
    if os.path.exists(plan):
        with open(plan) as f:
            return json.load(f)["workload"]
    try:
        with open(os.path.join(rundir, "ILE.sub")) as f:
            found = re.search(r'^arguments\s*=\s*"(.*)"\s*$', f.read(), re.M)
    except OSError as exc:
        raise rp.ResourceProfileError("{}: no resource-plan.json or ILE.sub: {}".format(rundir, exc)) from exc
    if not found:
        raise rp.ResourceProfileError("{}: ILE.sub has no arguments line".format(rundir))
    # condor's new argument syntax doubles a quote inside a quoted word
    arguments = shlex.split(found.group(1).replace("''", "'"))
    return workload_from_arguments(arguments, label or os.path.basename(os.path.normpath(rundir)))


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
    A class needing a separate opt-in attribute gets none here: add it by hand.
    """
    missing = [flag for flag in NOLOOP_FLAGS if flag not in arguments]
    if missing:
        raise rp.ResourceProfileError("ILE arguments must reach NoLoop; missing {}".format(missing))
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
            family = _image_label(manifest, float(c["capability"][0]))
            chosen = next(m["image"] for m in manifest["containers"] if m["label"] == family)
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


def collect_probes(out, profile):
    """Observations from the receipts under ``out``.  Returns (observations, skipped).

    The class comes from the device the job reports, not the job it was
    emitted for, and a job with no receipt or no known device is skipped.
    """
    setup = float(profile["policy"]["worker setup seconds"])
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
        label = gpu and rp.classify_device(profile, gpu["name"], gpu["sm"] / 10.0)
        if not label or not run or not run.get("arguments"):
            skipped.append(directory)
            continue
        draws = [p["draws"] for p in run["points"]]
        memory = run["sampled_memory"]
        device = memory.get("gpu_peak_used_mb") or None
        host = (memory.get("host_peak_rss_kb") or 0) / 1024.0 or None
        wall, code = run["wall_seconds"], run["returncode"]
        o = {"class": label, "workload": workload_from_arguments(run["arguments"], probe["label"]),
             "source": path}
        if code in (0, TIMEOUT_RETURNCODE):
            o["outcome"] = "success"
            if draws:
                o["draws per point"] = draws
                o["memory samples"] = [[sum(draws), device, host, 1]]
                if code == 0:
                    o["draws per second"] = [sum(draws) / (wall - min(setup, wall / 2.0))]
        else:
            o["outcome"] = "memory-failure" if run.get("gpu_oom") else "crash" if code < 0 else "failure"
            o["failure draws"] = [sum(draws)]
            if host:
                o["host memory mb"] = host
        observations.append(o)
    return observations, skipped


def write_resources(path, data):
    """Replace ``path`` atomically, keeping the previous file as ``path.previous``."""
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
