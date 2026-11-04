#!/usr/bin/env python3
"""Plan ILE GPU matching from a resource profile, or build and extend one.

    util_RIFT_resource_profile.py plan PROFILE --workload workload.yaml [--draws-per-point D]
    util_RIFT_resource_profile.py harvest PROFILE RUNDIR [--workload workload.yaml] [--output FILE]
    util_RIFT_resource_profile.py survey PROFILE_DIR [--manifest family.yaml] [--pool HOST]
    util_RIFT_resource_profile.py emit-probes PROFILE_DIR --case DIR --arguments ile.args --label L --out DIR
    util_RIFT_resource_profile.py collect-probes PROFILE_DIR --probes DIR --output FILE

``plan`` prints the plan as JSON.  ``harvest`` reads the workload from
RUNDIR/resource-plan.json, else RUNDIR/ILE.sub, unless --workload is given, and
writes the observations as YAML to --output (default: stdout).  With a directory
profile, put that file under PROFILE/observations/.  ``survey`` creates or
updates PROFILE_DIR/resources.yaml from condor_status.  ``emit-probes`` writes
one bounded ILE job per class, and ``collect-probes`` turns their receipts into
observations.  See containers/README.md and RIFT/misc/DESIGN_resource_profile.md.
"""

import argparse
import json
import os
import shlex
import sys

import yaml

from RIFT.misc import resource_profile
from RIFT.misc import resource_profile_build as build
from RIFT.misc.container_manifest import ContainerManifestError, load_container_manifest


def _workload(path, rundir=None):
    if path:
        with open(path) as f:
            return yaml.safe_load(f)
    return build.workload_from_rundir(rundir)


def _write_observations(observations, output):
    """Write YAML to a new file, or stdout.  Returns the exit code."""
    text = yaml.safe_dump({"observations": observations}, sort_keys=False)
    if output:
        if os.path.lexists(output):
            print("refusing to replace {}".format(output), file=sys.stderr)
            return 2
        with open(output, "w") as f:
            f.write(text)
    else:
        sys.stdout.write(text)
    return 0


def _survey(opts):
    main = os.path.join(opts.profile, "resources.yaml")
    existing = None
    if os.path.exists(main):
        with open(main) as f:
            existing = yaml.safe_load(f)
    if opts.inventory:
        with open(opts.inventory) as f:
            rows = build.parse_inventory(f.read())
    else:
        rows = [row for pool in opts.pool or [None] for row in build.survey_pool(opts.constraint, pool)]
    devices = build.summarize_inventory(rows)
    if not devices and not existing:
        raise resource_profile.ResourceProfileError("survey found no GPUs and there is no profile to keep")
    manifest = load_container_manifest(opts.manifest) if opts.manifest else None
    data, report = build.update_resources(existing, devices, manifest, keep_slots=opts.keep_slots)
    os.makedirs(opts.profile, exist_ok=True)
    report["inventory"] = build.save_inventory(os.path.join(opts.profile, "surveys"), rows, devices)
    build.write_resources(main, data)
    profile = resource_profile.load_resource_profile(opts.profile)
    report["relative speed set"] = build.refresh_relative_speeds(data, profile)
    if report["relative speed set"]:
        build.write_resources(main, data)
        resource_profile.load_resource_profile(opts.profile)
    print(json.dumps(report, indent=1))
    return 0


def _emit(opts, profile):
    with open(opts.arguments) as f:
        arguments = shlex.split(f.read())
    manifest = load_container_manifest(opts.manifest) if opts.manifest else None
    submits = build.emit_probes(
        profile, opts.case, arguments, opts.out, opts.label, image=opts.image, manifest=manifest,
        classes=opts.classes.split(",") if opts.classes else None, points=opts.points, event=opts.event,
        n_chunk=opts.n_chunk, n_eff=opts.n_eff, n_max=opts.n_max, python=opts.python, ile=opts.ile,
        memory=opts.request_memory, disk=opts.request_disk, timeout=opts.timeout, pools=opts.pools,
        accounting_group=opts.accounting_group, accounting_user=opts.accounting_user)
    print("\n".join(submits))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan")
    plan.add_argument("profile")
    plan.add_argument("--workload", required=True, help="YAML mapping of workload keys")
    plan.add_argument("--draws-per-point", type=float, default=None)
    plan.add_argument("--jobs-per-worker-cap", type=int, default=None)
    plan.add_argument("--policy", default=None, help="YAML mapping of policy overrides, as in the ledger")
    harvest = sub.add_parser("harvest")
    harvest.add_argument("profile")
    harvest.add_argument("rundir")
    harvest.add_argument("--workload", default=None)
    harvest.add_argument("--output", default=None)
    harvest.add_argument("--policy", default=None, help="YAML mapping of policy overrides, as in the ledger")
    survey = sub.add_parser("survey")
    survey.add_argument("profile", help="profile directory; resources.yaml is created or updated")
    survey.add_argument("--manifest", default=None, help="container family manifest: sets each memory family")
    survey.add_argument("--constraint", default="GPUs_DeviceName =!= undefined")
    survey.add_argument("--pool", action="append", help="collector to query; repeat to merge several")
    survey.add_argument("--keep-slots", action="store_true", help="keep the slots of existing classes")
    survey.add_argument("--inventory", default=None, help="saved condor_status -af:t output, instead of a query")
    emit = sub.add_parser("emit-probes")
    emit.add_argument("profile")
    emit.add_argument("--case", required=True, help="directory of ILE inputs; transferred, and the working directory")
    emit.add_argument("--arguments", required=True, help="file holding the ILE arguments, shell quoted")
    emit.add_argument("--label", required=True, help="workload label for the observations")
    emit.add_argument("--out", required=True)
    emit.add_argument("--image", default=None, help="one image for every class, instead of --manifest")
    emit.add_argument("--manifest", default=None)
    emit.add_argument("--classes", default=None, help="comma-separated labels; default every class")
    emit.add_argument("--points", type=int, default=10)
    emit.add_argument("--event", type=int, default=0)
    emit.add_argument("--n-chunk", type=int, default=None)
    emit.add_argument("--n-eff", type=float, default=None)
    emit.add_argument("--n-max", type=int, default=None)
    emit.add_argument("--python", default="python3", help="interpreter inside the image, as an absolute path")
    emit.add_argument("--ile", default="integrate_likelihood_extrinsic_batchmode")
    emit.add_argument("--request-memory", type=int, default=12000)
    emit.add_argument("--request-disk", type=int, default=24000)
    emit.add_argument("--timeout", type=int, default=10800, help="seconds of ILE time")
    emit.add_argument("--pools", default=None, help='for example "IGWN,CIT"')
    emit.add_argument("--accounting-group", default=os.environ.get("LIGO_ACCOUNTING"))
    emit.add_argument("--accounting-user", default=os.environ.get("LIGO_USER_NAME"))
    collect = sub.add_parser("collect-probes")
    collect.add_argument("profile")
    collect.add_argument("--probes", required=True, help="the --out directory of emit-probes")
    collect.add_argument("--output", default=None)
    opts = parser.parse_args(argv)

    try:
        if opts.command == "survey":
            return _survey(opts)
        policy = yaml.safe_load(opts.policy) if getattr(opts, "policy", None) else None
        profile = resource_profile.load_resource_profile(opts.profile, policy)
        if opts.command == "emit-probes":
            return _emit(opts, profile)
        if opts.command == "collect-probes":
            observations, skipped = build.collect_probes(opts.probes, profile)
            code = _write_observations(observations, opts.output)
            print("skipped jobs: {}".format(skipped), file=sys.stderr)
            return code
        if opts.command == "plan":
            result = resource_profile.plan_resources(
                profile, _workload(opts.workload), draws_per_point=opts.draws_per_point,
                jobs_per_worker_cap=opts.jobs_per_worker_cap)
            print(json.dumps(result, indent=1, default=str))
            return 0
        observations, skipped = resource_profile.harvest_rundir(
            opts.rundir, profile, _workload(opts.workload, opts.rundir))
    except (resource_profile.ResourceProfileError, ContainerManifestError, OSError, yaml.YAMLError,
            KeyError) as error:
        print("resource profile: {}".format(error), file=sys.stderr)
        return 2
    code = _write_observations(observations, opts.output)
    print("skipped workers: {}".format(skipped), file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
