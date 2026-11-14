#!/usr/bin/env python3
"""Render and locally execute a generic terminal command-stage DAG fragment.

The host need not provide HTCondor.  A small executor reads the rendered DAG
and submit files, substitutes DAG macros, honors terminal-stage dependencies,
and runs only the stub terminal commands.  This catches build-only mistakes in
argument rendering, fan-out barriers, initial directories, and output paths.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile


HERE = Path(__file__).resolve()
RIFT_ROOT = HERE.parents[5]
CODE = RIFT_ROOT / "MonteCarloMarginalizeCode" / "Code"
BIN = CODE / "bin"
PIPELINE = BIN / "create_eos_posterior_pipeline"


STUB_SOURCE = r'''#!/usr/bin/env python3
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("operation", choices=("batch", "merge", "convert"))
parser.add_argument("input", nargs="?")
parser.add_argument("--start", type=int)
parser.add_argument("--end", type=int)
parser.add_argument("--directory")
parser.add_argument("--output")
args = parser.parse_args()

if args.operation == "batch":
    directory = Path(args.directory)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "weight-{}-{}.dat".format(args.start, args.end)).write_text(
        "{} {}\n".format(args.start, args.end))
elif args.operation == "merge":
    files = sorted(Path(args.directory).glob("weight-*.dat"))
    if len(files) != 3:
        raise SystemExit("merge saw {} batches, expected 3".format(len(files)))
    Path(args.output).write_text("".join(path.read_text() for path in files))
else:
    source = Path(args.input)
    if not source.is_file():
        raise SystemExit("missing positional input {}".format(source))
    Path(args.output).write_text("converted\n" + source.read_text())
'''


def _write_fixture(run: Path):
    stub = run / "terminal_stub.py"
    stub.write_text(STUB_SOURCE)
    stub.chmod(0o755)
    grid = run / "grid.dat"
    grid.write_text(
        "# RIFT_HYPERPIPELINE_V1\n"
        "# lnL sigma_lnL m1 m2 a1x a1y a1z a2x a2y a2z\n"
        "0 0 36 29 0 0 0.1 0 0 0.05\n")
    (run / "post.args").write_text("1 --parameter m1 --parameter m2\n")
    jobs = [{
        "name": "stub-marg",
        "protocol": "indexed-grid-v1",
        "exe": "/bin/true",
        "args": "",
        "n_chunk": 1,
    }]
    (run / "jobs.json").write_text(json.dumps(jobs))

    weights = run / "weights"
    merged = run / "merged.dat"
    converted = run / "converted.dat"
    stages = {
        "version": 1,
        "stages": [
            {
                "name": "batches",
                "kind": "command-v1",
                "exe": str(stub),
                "args": "batch --start $(macrostart) --end $(macroend) "
                        "--directory {}".format(weights),
                "instances": [
                    {"start": 0, "end": 2},
                    {"start": 2, "end": 4},
                    {"start": 4, "end": 6},
                ],
                "initial_dir": str(run),
                "log_dir": str(run / "logs"),
            },
            {
                "name": "merge",
                "kind": "command-v1",
                "depends_on": ["batches"],
                "exe": str(stub),
                "args": "merge --directory {} --output {}".format(
                    weights, merged),
                "initial_dir": str(run),
                "log_dir": str(run / "logs"),
            },
            {
                "name": "convert",
                "kind": "command-v1",
                "depends_on": ["merge"],
                "exe": str(stub),
                "args": "convert {} --output {}".format(merged, converted),
                "initial_dir": str(run),
                "log_dir": str(run / "logs"),
            },
        ],
    }
    (run / "terminal.json").write_text(json.dumps(stages, indent=2))
    return converted


def _render(run: Path):
    env = os.environ.copy()
    env["RIFT_HYPERPIPELINE_FORMAT"] = "1"
    env["RIFT_DAG_BACKEND"] = "htcondor"
    env["PYTHONPATH"] = os.pathsep.join(
        [str(CODE), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    subprocess.run([
        sys.executable, str(PIPELINE),
        "--working-directory", str(run),
        "--input-grid", str(run / "grid.dat"),
        "--marg-job-spec-file", str(run / "jobs.json"),
        "--eos-post-args-list", str(run / "post.args"),
        "--eos-post-exe", "/bin/true",
        "--n-iterations", "1",
        "--n-samples-per-job", "1",
        "--eos-post-explode-jobs", "1",
        "--eos-post-explode-jobs-last", "1",
        "--terminal-stage-spec-file", str(run / "terminal.json"),
        "--use-full-submit-paths",
    ], cwd=str(run), env=env, check=True, stdout=subprocess.PIPE,
       stderr=subprocess.STDOUT, text=True)


def _parse_dag(path: Path):
    jobs = {}
    macros = {}
    parents = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if fields and fields[0] == "JOB":
            jobs[fields[1]] = Path(fields[2])
        elif fields and fields[0] == "VARS":
            macros[fields[1]] = dict(re.findall(r'(\w+)="([^"]*)"', line))
        elif fields and fields[0] == "PARENT":
            split_at = fields.index("CHILD")
            for child in fields[split_at + 1:]:
                parents.setdefault(child, set()).update(fields[1:split_at])
    return jobs, macros, parents


def _read_submit(path: Path):
    values = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key.strip().lower()] = value.strip().strip('"')
    return values


def _execute_terminal_nodes(run: Path):
    jobs, macros, parents = _parse_dag(run / "marginalize_hyperparameters.dag")
    pending = {
        node for node, submit in jobs.items()
        if submit.name.startswith("TERMINAL_")
    }
    completed = set()
    while pending:
        ready = sorted(
            node for node in pending
            if not (parents.get(node, set()) & pending))
        if not ready:
            raise RuntimeError("terminal DAG has a dependency cycle")
        for node in ready:
            submit = _read_submit(jobs[node])
            arguments = submit.get("arguments", "")
            for key, value in macros.get(node, {}).items():
                arguments = arguments.replace("$({})".format(key), value)
            subprocess.run(
                [submit["executable"]] + shlex.split(arguments),
                cwd=submit.get("initialdir", str(run)), check=True)
            pending.remove(node)
            completed.add(node)
    assert len(completed) == 5


def main():
    with tempfile.TemporaryDirectory(prefix="rift-terminal-execution-") as tmp:
        run = Path(tmp)
        converted = _write_fixture(run)
        _render(run)
        _execute_terminal_nodes(run)
        assert converted.read_text() == "converted\n0 2\n2 4\n4 6\n"
    print("terminal execution gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
