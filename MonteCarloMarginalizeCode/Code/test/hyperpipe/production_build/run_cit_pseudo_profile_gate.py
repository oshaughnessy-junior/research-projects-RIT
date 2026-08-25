#!/usr/bin/env python3
"""Build a sanitized production-shaped Hyperpipe DAG on CIT, without submit."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve()
RIFT_ROOT = HERE.parents[5]
CODE = RIFT_ROOT / "MonteCarloMarginalizeCode" / "Code"
PSEUDO_PIPE = CODE / "bin" / "util_RIFT_pseudo_pipe.py"
DEFAULT_PROFILE = HERE.parent / "profiles" / "gwtc5_hlv_osg_topology.json"
FORBIDDEN_OUTPUT_ROOT = Path("/home/pe.o4")

if str(CODE) not in sys.path:
    sys.path.insert(0, str(CODE))

import lal  # noqa: E402

from RIFT import lalsimutils  # noqa: E402
from RIFT.misc import hyperpipeline_io  # noqa: E402


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
    run_dir = output_root / "{}-{}-{}-{}".format(
        stamp, commit[:12], profile_name, secrets.token_hex(3))
    run_dir.mkdir(mode=0o700)
    return run_dir


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
    return guard_dir, log_path


def _write_inputs(run_dir):
    point_rows = [(36.0, 29.0, 0.0), (34.0, 30.0, 0.0)]
    points = []
    for m1, m2, s1z in point_rows:
        point = lalsimutils.ChooseWaveformParams()
        point.m1 = m1 * lal.MSUN_SI
        point.m2 = m2 * lal.MSUN_SI
        point.s1z = s1z
        point.s2z = 0.0
        point.fref = 20.0
        points.append(point)
    grid = run_dir / "seed-grid.dat"
    hyperpipeline_io.write_grid_from_P_list(
        str(grid), points, hyperpipeline_io.DEFAULT_BASE_COLUMNS,
        lal_module=lal, lalsimutils_module=lalsimutils)
    cache = run_dir / "empty.cache"
    cache.touch()
    bilby_ini = run_dir / "event.ini"
    bilby_ini.write_text(
        "channel-dict = {H1:FAKE,L1:FAKE}\n"
        "data-dict = {H1:/dev/null,L1:/dev/null}\n"
        "waveform-approximant = IMRPhenomXPHM\n")
    return {"GRID": grid, "CACHE": cache, "BILBY_INI": bilby_ini}


def _expand(value, values):
    for name, path in values.items():
        value = value.replace("<{}>".format(name), str(path))
    return value


def _build(profile, run_dir, values, guard_dir):
    pipeline_dir = run_dir / "pipeline"
    args = [_expand(item, values) for item in profile["pseudo_pipe_args"]]
    if any(item in args for item in ("--submit", "--submit-dag")):
        raise ValueError("production build profiles may not request submission")
    command = [sys.executable, str(PSEUDO_PIPE), "--pipeline-builder",
               "Hyperpipe", "--use-rundir", str(pipeline_dir)] + args
    env = os.environ.copy()
    env.update({key: _expand(value, values)
                for key, value in profile.get("environment", {}).items()})
    env["PATH"] = os.pathsep.join(
        [str(guard_dir), str(CODE / "bin"), env.get("PATH", "")])
    env["PYTHONPATH"] = os.pathsep.join(
        [str(CODE), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env["RIFT_ROOT"] = str(RIFT_ROOT)
    env["RIFT_DAG_BACKEND"] = "htcondor"
    env["GW_SURROGATE"] = ""
    for name in ("XDG_CACHE_HOME", "MPLCONFIGDIR", "PYTHONPYCACHEPREFIX"):
        path = run_dir / name.lower()
        path.mkdir()
        env[name] = str(path)
    result = subprocess.run(
        command, cwd=str(run_dir), env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        timeout=int(profile.get("build_timeout_seconds", 300)))
    (run_dir / "pipeline-build.log").write_text(result.stdout)
    if result.returncode:
        raise RuntimeError(
            "production pseudo-pipe build failed ({}):\n{}".format(
                result.returncode, result.stdout))
    return pipeline_dir, command


def _dag_graph(path):
    jobs = {}
    edges = set()
    subdags = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if not fields:
            continue
        if fields[0] == "JOB":
            jobs[fields[1]] = Path(fields[2]).name
        elif fields[:2] == ["SUBDAG", "EXTERNAL"]:
            subdags[fields[2]] = Path(fields[3])
        elif fields[0] == "PARENT":
            split_at = fields.index("CHILD")
            edges.update((parent, child)
                         for parent in fields[1:split_at]
                         for child in fields[split_at + 1:])
    return jobs, subdags, edges


def _reachable(edges, source, target):
    pending = [source]
    visited = set()
    while pending:
        node = pending.pop()
        if node == target:
            return True
        if node in visited:
            continue
        visited.add(node)
        pending.extend(child for parent, child in edges if parent == node)
    return False


def _assert_container_executable_contract(pipeline_dir):
    """Every containerised job must name an executable that exists where it runs.

    Two ways to satisfy that, and the submit file has to pick one consistently:

    * **not** transferring the executable -- `executable` is the in-container
      path (`$SINGULARITY_BASE_EXE_DIR/<basename>`, `/usr/bin/...` here), which
      requires the image to already carry a compatible RIFT;
    * transferring it -- `executable` is the submit host's path so HTCondor
      ships the file, and the RIFT **package** must ship with it, because a
      script and the library it imports are one unit: dropping a newer script
      into an older image's RIFT is how you get a failure with no traceback.

    The illegal combination is the third one: a host path with no RIFT
    alongside. On a worker that path does not exist, or exists as a different
    version, and the job dies for a reason the submit file does not explain.

    This replaces two literal `executable = /usr/bin/...` tokens in the
    profile. Those were correct when written and became wrong 2.5 hours later,
    when the transfer-executable branch landed and made every `--use-osg` run
    take the other arm -- and nothing noticed, because this gate was not in the
    sweep anyone was running. A literal cannot express "one of two, matched to
    the flag"; this can, and it holds for either arm.
    """
    checked = 0
    for submit in sorted(pipeline_dir.glob("*.sub")):
        text = submit.read_text()
        exe_lines = [line for line in text.splitlines()
                     if line.startswith("executable = ")]
        if not exe_lines or "SingularityImage" not in text:
            continue
        exe = exe_lines[0].split(" = ", 1)[1].strip()
        checked += 1
        if not os.path.isabs(exe):
            # A wrapper resolved relative to the sandbox; it is transferred by
            # construction, so there is nothing to check here.
            continue
        transfers = ""
        for line in text.splitlines():
            if line.startswith("transfer_input_files"):
                transfers = line.split(" = ", 1)[-1]
        ships_rift = any(
            item.rstrip("/").endswith("/RIFT")
            for item in transfers.split(","))
        inside_container = exe.startswith("/usr/") or exe.startswith("/opt/")
        if inside_container:
            continue
        if not ships_rift:
            raise AssertionError(
                "{}: executable is the submit-host path {!r}, which does not "
                "exist on a worker, and the RIFT package is not in "
                "transfer_input_files. Either name the in-container path or "
                "ship the script and its library together.".format(
                    submit.name, exe))
    if not checked:
        raise AssertionError(
            "no containerised submit files found in {}; this check silently "
            "passed on nothing".format(pipeline_dir))
    return checked


def _assert_topology(profile, pipeline_dir):
    dag = pipeline_dir / "marginalize_hyperparameters.dag"
    jobs, subdags, edges = _dag_graph(dag)
    expected = profile["expected"]
    if len(subdags) != expected["subdag_count"]:
        raise AssertionError("unexpected recursive subdag count")
    for child in subdags.values():
        text = child.read_text()
        missing = [token for token in expected["child_dag_tokens"]
                   if token not in text]
        if missing:
            raise AssertionError("child DAG missing {}".format(missing))

    manifest = json.loads(
        (pipeline_dir / "terminal_stage_specs.json").read_text())
    stage_names = {stage["name"] for stage in manifest["stages"]}
    missing_stages = set(expected["terminal_stages"]) - stage_names
    if missing_stages:
        raise AssertionError(
            "terminal manifest missing {}".format(sorted(missing_stages)))

    for submit_name, tokens in expected["submit_tokens"].items():
        submit = pipeline_dir / submit_name
        text = submit.read_text()
        missing = [token for token in tokens if token not in text]
        if missing:
            raise AssertionError(
                "{} missing {}".format(submit_name, missing))

    fetch = [node for node, submit in jobs.items()
             if submit.startswith("FETCH_Z_")]
    terminal = [node for node, submit in jobs.items()
                if submit == "TERMINAL_extrinsic_samples.sub"]
    if len(fetch) != 1 or not terminal:
        raise AssertionError("missing Z fetch or terminal extrinsic nodes")
    if not all(_reachable(edges, fetch[0], node) for node in terminal):
        raise AssertionError("terminal exports are not downstream of Z fetch")
    containerised = _assert_container_executable_contract(pipeline_dir)
    return {
        "dag": dag.name,
        "jobs": len(jobs),
        "subdags": len(subdags),
        "terminal_stages": sorted(stage_names),
        "containerised_submits_checked": containerised,
    }


def _condor_dry_run(profile, pipeline_dir, real_condor_submit):
    if not real_condor_submit:
        raise ValueError("--condor-dry-run requires --real-condor-submit")
    real = Path(real_condor_submit).resolve()
    if not real.is_file():
        raise ValueError("real condor_submit does not exist: {}".format(real))
    checked = []
    macro_assignments = [
        "{}={}".format(name, value)
        for name, value in profile["expected"].get(
            "dry_run_macros", {}).items()
    ]
    for pattern in profile["expected"].get("dry_run_submits", []):
        matches = sorted(pipeline_dir.glob(pattern))
        if not matches:
            raise AssertionError(
                "no submit files match dry-run pattern {!r}".format(pattern))
        submit = matches[0]
        output = pipeline_dir / (submit.name + ".dryrun")
        result = subprocess.run(
            [str(real), "-dry-run", str(output), str(submit)] +
            macro_assignments,
            cwd=str(pipeline_dir), text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=60)
        if result.returncode:
            raise RuntimeError(
                "condor_submit -dry-run failed for {}:\n{}".format(
                    submit.name, result.stdout))
        checked.append(submit.name)
    return checked


def _assert_no_scheduler_artifacts(run_dir):
    forbidden = []
    for pattern in ("*.dagman.*", "*.nodes.log", "*.metrics"):
        forbidden.extend(run_dir.rglob(pattern))
    if forbidden:
        raise AssertionError(
            "scheduler runtime artifacts were created: {}".format(
                [str(path.relative_to(run_dir)) for path in forbidden]))


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
        raise SystemExit("checkout mismatch: expected {}, found {}".format(
            args.expected_commit, commit))
    profile = json.loads(args.profile.read_text())
    if profile.get("schema") != "rift-hyperpipe-production-build/v1":
        raise SystemExit("unsupported production build profile schema")

    temporary = None
    if args.output_root:
        run_dir = _unique_run_dir(args.output_root, profile["name"], commit)
    elif args.keep_output:
        run_dir = _unique_run_dir(
            Path.home() / "LVK" / "hyperpipe-build-gate" / "runs",
            profile["name"], commit)
    else:
        temporary = tempfile.TemporaryDirectory(
            prefix="rift-cit-pseudo-profile-gate-")
        run_dir = Path(temporary.name)
        _validate_location(run_dir)

    os.umask(0o077)
    guard_dir, blocked_log = _install_scheduler_guards(run_dir)
    values = _write_inputs(run_dir)
    pipeline_dir, command = _build(profile, run_dir, values, guard_dir)
    topology = _assert_topology(profile, pipeline_dir)
    dry_run_submits = []
    if args.condor_dry_run:
        dry_run_submits = _condor_dry_run(
            profile, pipeline_dir, args.real_condor_submit)
    if blocked_log.exists() and blocked_log.read_text().strip():
        raise AssertionError("a scheduler command was attempted")
    _assert_no_scheduler_artifacts(run_dir)
    report = {
        "schema": "rift-hyperpipe-production-build-report/v1",
        "profile": profile["name"],
        "commit": commit,
        "submitted": False,
        "command": command,
        "condor_dry_run_submits": dry_run_submits,
        "topology": topology,
    }
    (run_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n")
    print("CIT Hyperpipe production topology gate: PASS")
    if temporary is None:
        print("output: {}".format(run_dir))


if __name__ == "__main__":
    main()
