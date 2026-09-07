#!/usr/bin/env python3
"""Render a sanitized CIT/OSG Hyperpipe execution profile without submitting.

This is the deliberately small first production-build runner.  It validates
ILE/CIP submit semantics and the open execution contract.  It does not read a
production event, invoke Asimov, or submit a DAG.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile
import types


HERE = Path(__file__).resolve()
RIFT_ROOT = HERE.parents[5]
CODE = RIFT_ROOT / "MonteCarloMarginalizeCode" / "Code"
DEFAULT_PROFILE = HERE.parent / "profiles" / "gwtc5_hlv_osg_execution.json"
FORBIDDEN_OUTPUT_ROOT = Path("/home/pe.o4")

if str(CODE) not in sys.path:
    sys.path.insert(0, str(CODE))
# This gate needs only the lightweight workflow modules.  Avoid importing the
# full RIFT package (and therefore LAL) so it can run in a login-node Python.
if "RIFT" not in sys.modules:
    rift_package = types.ModuleType("RIFT")
    rift_package.__path__ = [str(CODE / "RIFT")]
    sys.modules["RIFT"] = rift_package

from RIFT.hyperpipe.execution_contract import (  # noqa: E402
    apply_backend_commands, apply_portable_resources, commands_for_backend,
    normalize_execution)
from RIFT.hyperpipe.marg_contract import (  # noqa: E402
    MARG_EXECUTION_KEYS, MargJobSpec)
from RIFT.misc import dag_utils_generic as dag_utils  # noqa: E402


def _is_beneath(path, parent):
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _validate_location(output_root):
    if _is_beneath(RIFT_ROOT, FORBIDDEN_OUTPUT_ROOT):
        raise ValueError("refusing to run from a checkout beneath /home/pe.o4")
    if _is_beneath(output_root, FORBIDDEN_OUTPUT_ROOT):
        raise ValueError("refusing output beneath /home/pe.o4")


def _unique_run_dir(output_root, profile_name, commit):
    output_root = output_root.expanduser().resolve()
    _validate_location(output_root)
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = "{}-{}-{}-{}".format(
        stamp, commit[:12], profile_name, secrets.token_hex(3))
    run_dir = output_root / name
    run_dir.mkdir(mode=0o700)
    return run_dir


def _replace_run_dir(value, run_dir):
    if isinstance(value, str):
        return value.replace("<RUN_DIR>", str(run_dir))
    if isinstance(value, list):
        return [_replace_run_dir(item, run_dir) for item in value]
    if isinstance(value, dict):
        return {key: _replace_run_dir(item, run_dir)
                for key, item in value.items()}
    return value


def _install_scheduler_guards(run_dir):
    guard_dir = run_dir / "scheduler-guards"
    guard_dir.mkdir()
    log_path = run_dir / "blocked-scheduler-calls.log"
    for name in ("condor_submit", "condor_submit_dag", "condor_submit_bid",
                 "condor_dagman"):
        path = guard_dir / name
        path.write_text(
            "#!/bin/sh\n"
            "echo \"{} $*\" >> \"{}\"\n"
            "echo \"blocked scheduler command: {}\" >&2\n"
            "exit 97\n".format(name, log_path, name))
        path.chmod(0o700)
    os.environ["PATH"] = str(guard_dir) + os.pathsep + os.environ.get("PATH", "")
    return log_path


def _write_assets(run_dir):
    (run_dir / "frames_dir").mkdir()
    (run_dir / "iteration_0_ile").mkdir()
    (run_dir / "frames_dir" / "synthetic.gwf").write_bytes(b"")
    for name in ("H1-psd.xml.gz", "L1-psd.xml.gz", "all.net"):
        (run_dir / name).write_bytes(b"")
    (run_dir / "grid.dat").write_text(
        "# RIFT_HYPERPIPELINE_V1\n"
        "# columns: lnL sigma_lnL m1 m2\n0 0 30 20\n")


def _render(profile, run_dir):
    old_cwd = Path.cwd()
    old_backend = dag_utils.current_backend_name()
    try:
        os.chdir(run_dir)
        dag_utils.set_backend("htcondor")
        ile = MargJobSpec(profile["ile"], run_dir)
        execution = ile.execution
        ile_job, _ = dag_utils.write_ILE_sub_simple(
            tag="ILE", log_dir=str(run_dir / "logs") + "/",
            exe=ile.exe, arg_str=ile.args + " --sim-grid grid.dat",
            output_file="MARG.dat", ncopies=1,
            request_memory=int(execution["request_memory"]),
            request_cpus=int(execution["request_cpus"]),
            request_disk=execution["request_disk"],
            request_gpu=bool(execution["request_gpu"]),
            use_osg=True, use_singularity=True,
            singularity_image=execution["singularity_image"],
            use_oauth_files=execution["use_oauth_files"],
            frames_dir=execution["frames_dir"],
            transfer_files=list(execution["transfer_files"]),
            max_runtime_minutes=execution["max_runtime_minutes"],
            condor_commands=commands_for_backend(execution, "htcondor"))
        apply_backend_commands(ile_job, execution)
        ile_job.add_condor_cmd(
            "initialdir", str(run_dir / "iteration_0_ile"))
        ile_job.set_sub_file(str(run_dir / "ILE.sub"))
        ile_job.write_sub_file()

        cip_execution = normalize_execution(
            profile["cip"]["execution"], MARG_EXECUTION_KEYS,
            "CIT profile CIP")
        cip_job, _ = dag_utils.write_CIP_sub(
            tag="CIP", log_dir=str(run_dir / "logs") + "/",
            exe="/bin/true", arg_str="--parameter m1 --parameter m2",
            input_net="all.net", output="posterior",
            out_dir=str(run_dir), request_memory=8192,
            request_cpus=int(cip_execution.get("request_cpus", 1)),
            request_disk=cip_execution["request_disk"],
            use_osg=True, use_singularity=True,
            singularity_image=cip_execution["singularity_image"],
            use_oauth_files=cip_execution["use_oauth_files"],
            transfer_files=list(cip_execution["transfer_files"]),
            condor_commands=commands_for_backend(
                cip_execution, "htcondor"))
        apply_backend_commands(cip_job, cip_execution)
        cip_job.set_sub_file(str(run_dir / "CIP.sub"))
        cip_job.write_sub_file()
    finally:
        dag_utils.set_backend(old_backend)
        os.chdir(old_cwd)


def _assert_profile(profile, run_dir):
    report = {"profile": profile["name"], "submits": {}}
    for label in ("ile", "cip"):
        path = run_dir / (label.upper() + ".sub")
        text = path.read_text()
        missing = [token for token in profile["expected"][label]
                   if token not in text]
        if missing:
            raise AssertionError(
                "{} is missing production contract tokens {}".format(
                    path.name, missing))
        report["submits"][label] = {
            "path": path.name,
            "expected_tokens": len(profile["expected"][label]),
        }
    ile_text = (run_dir / "ILE.sub").read_text()
    if "executable = /usr/bin/true" not in ile_text:
        raise AssertionError(
            "ILE PreCmd profile must retain the science executable")
    if "executable = ile_pre.sh" in ile_text:
        raise AssertionError("ILE unexpectedly replaced the executable with PreCmd")
    pre_text = (run_dir / "ile_pre.sh").read_text()
    if "/bin/true" in pre_text or "exec " in pre_text:
        raise AssertionError("PreCmd must prepare inputs, not invoke ILE")
    return report


def _condor_dry_run(run_dir, submit_names, real_condor_submit):
    if not real_condor_submit:
        raise ValueError("--condor-dry-run requires --real-condor-submit")
    real = Path(real_condor_submit).resolve()
    if not real.is_file():
        raise ValueError("real condor_submit does not exist: {}".format(real))
    for name in submit_names:
        result = subprocess.run(
            [str(real), "-dry-run", str(run_dir / (name + ".dryrun")),
             str(run_dir / name)],
            cwd=str(run_dir), text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=60)
        if result.returncode:
            raise RuntimeError(
                "condor_submit -dry-run failed for {}:\n{}".format(
                    name, result.stdout))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--expected-commit")
    parser.add_argument("--condor-dry-run", action="store_true")
    parser.add_argument("--real-condor-submit")
    parser.add_argument("--keep-output", action="store_true")
    args = parser.parse_args(argv)

    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=str(RIFT_ROOT), text=True).strip()
    if args.expected_commit and commit != args.expected_commit:
        raise SystemExit(
            "checkout mismatch: expected {}, found {}".format(
                args.expected_commit, commit))
    profile = json.loads(args.profile.read_text())
    if profile.get("version") != 1:
        raise SystemExit("production execution profile must have version 1")

    temporary = None
    if args.output_root:
        run_dir = _unique_run_dir(args.output_root, profile["name"], commit)
    elif args.keep_output:
        run_dir = _unique_run_dir(
            Path.home() / "LVK" / "hyperpipe-build-gate" / "runs",
            profile["name"], commit)
    else:
        temporary = tempfile.TemporaryDirectory(
            prefix="rift-cit-execution-gate-")
        run_dir = Path(temporary.name)
        _validate_location(run_dir)

    os.umask(0o077)
    profile = _replace_run_dir(profile, run_dir)
    blocked_log = _install_scheduler_guards(run_dir)
    _write_assets(run_dir)
    (run_dir / "logs").mkdir()
    _render(profile, run_dir)
    report = _assert_profile(profile, run_dir)
    if args.condor_dry_run:
        _condor_dry_run(
            run_dir, ["ILE.sub", "CIP.sub"], args.real_condor_submit)
    if blocked_log.exists() and blocked_log.read_text().strip():
        raise AssertionError("a blocked scheduler command was attempted")
    report.update({"commit": commit, "submitted": False})
    (run_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n")
    print("CIT OSG execution profile gate: PASS")
    if temporary is None:
        print("output: {}".format(run_dir))


if __name__ == "__main__":
    main()
