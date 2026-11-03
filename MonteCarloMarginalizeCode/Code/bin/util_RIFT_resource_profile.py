#!/usr/bin/env python3
"""Plan ILE GPU matching from a resource profile, or harvest a run into one.

    util_RIFT_resource_profile.py plan PROFILE --workload workload.yaml [--draws-per-point D]
    util_RIFT_resource_profile.py harvest PROFILE RUNDIR [--workload workload.yaml] [--output FILE]

``plan`` prints the plan as JSON.  ``harvest`` reads the workload from
RUNDIR/resource-plan.json unless --workload is given, and writes the
observations as YAML to --output (default: stdout).  With a directory
profile, put that file under PROFILE/observations/.
See RIFT/misc/DESIGN_resource_profile.md.
"""

import argparse
import json
import os
import sys

import yaml

from RIFT.misc import resource_profile


def _workload(path, rundir=None):
    if path:
        with open(path) as f:
            return yaml.safe_load(f)
    with open(os.path.join(rundir, "resource-plan.json")) as f:
        return json.load(f)["workload"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan")
    plan.add_argument("profile")
    plan.add_argument("--workload", required=True, help="YAML mapping of workload keys")
    plan.add_argument("--draws-per-point", type=float, default=None)
    plan.add_argument("--jobs-per-worker-cap", type=int, default=None)
    harvest = sub.add_parser("harvest")
    harvest.add_argument("profile")
    harvest.add_argument("rundir")
    harvest.add_argument("--workload", default=None)
    harvest.add_argument("--output", default=None)
    opts = parser.parse_args(argv)

    try:
        profile = resource_profile.load_resource_profile(opts.profile)
        if opts.command == "plan":
            result = resource_profile.plan_resources(
                profile, _workload(opts.workload), draws_per_point=opts.draws_per_point,
                jobs_per_worker_cap=opts.jobs_per_worker_cap)
            print(json.dumps(result, indent=1, default=str))
            return 0
        observations, skipped = resource_profile.harvest_rundir(
            opts.rundir, profile, _workload(opts.workload, opts.rundir))
    except resource_profile.ResourceProfileError as error:
        print("resource profile: {}".format(error), file=sys.stderr)
        return 2
    text = yaml.safe_dump({"observations": observations}, sort_keys=False)
    if opts.output:
        if os.path.lexists(opts.output):
            print("refusing to replace {}".format(opts.output), file=sys.stderr)
            return 2
        with open(opts.output, "w") as f:
            f.write(text)
    else:
        sys.stdout.write(text)
    print("skipped workers: {}".format(skipped), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
