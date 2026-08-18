#!/usr/bin/env python3
"""Reproduce the bounded HyperPipe proposal-boundary observation."""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[3]
PROOF = Path(__file__).resolve().parent
RIFT_PY = ROOT / "MonteCarloMarginalizeCode" / "Code"
SCRIPT = RIFT_PY / "bin" / "util_HyperparameterTracerUpdate.py"
FIXTURE = PROOF / "fixtures" / "evaluated-grid.dat"
REFERENCE = PROOF / "fixtures" / "proposed-grid.reference.dat"
MANIFEST = PROOF / "trace-manifest.json"
TIMEOUT_SECONDS = 30

ARGUMENT_TEMPLATE = [
    "--inj-file", "{input}",
    "--inj-file-out", "{output}",
    "--parameter", "x",
    "--parameter", "y",
    "--update-method", "smc-mala-bd",
    "--tracer-fit-method", "quadratic",
    "--n-mala-steps", "2",
    "--target-ess-frac", "0.5",
    "--birth-death-rate", "1.0",
    "--force-away", "0",
    "--rng-seed", "1729",
]


class TraceError(RuntimeError):
    """The bounded proposal trace could not be reproduced safely."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git_blob_sha1(data: bytes) -> str:
    prefix = f"blob {len(data)}\0".encode("ascii")
    return hashlib.sha1(prefix + data).hexdigest()


def _environment() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(RIFT_PY)
    for name in (
        "OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS",
    ):
        env[name] = "1"
    env["PYTHONHASHSEED"] = "0"
    return env


def preflight() -> None:
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from RIFT.misc.tracer_placement import fits, samplers; "
                "assert callable(fits.build); assert callable(samplers.smc_mala_bd)"
            ),
        ],
        cwd=ROOT,
        env=_environment(),
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )
    if probe.returncode != 0:
        raise TraceError("canonical tracer import preflight failed: " + probe.stderr.strip())


def run_once(directory: Path) -> bytes:
    input_path = directory / "evaluated-grid.dat"
    output_path = directory / "proposed-grid.dat"
    shutil.copyfile(FIXTURE, input_path)
    original = input_path.read_bytes()
    arguments = [
        str(input_path) if value == "{input}" else
        str(output_path) if value == "{output}" else value
        for value in ARGUMENT_TEMPLATE
    ]
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        cwd=directory,
        env=_environment(),
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )
    if completed.returncode != 0:
        raise TraceError("proposal command failed: " + completed.stderr.strip())
    if completed.stderr.strip():
        raise TraceError("proposal command emitted unexpected stderr: " + completed.stderr.strip())
    if "falling back to puffball" in completed.stdout.lower():
        raise TraceError("proposal command used the fallback path")
    if input_path.read_bytes() != original:
        raise TraceError("proposal command modified its input")
    if not output_path.is_file():
        raise TraceError("proposal command did not create its declared output")
    return output_path.read_bytes()


def reproduce_twice() -> bytes:
    preflight()
    fixture_before = FIXTURE.read_bytes()
    with tempfile.TemporaryDirectory(prefix="rift-proposal-trace-a-") as first_dir:
        first = run_once(Path(first_dir))
    with tempfile.TemporaryDirectory(prefix="rift-proposal-trace-b-") as second_dir:
        second = run_once(Path(second_dir))
    if first != second:
        raise TraceError("fixed-seed outputs differ within one test environment")
    if FIXTURE.read_bytes() != fixture_before:
        raise TraceError("bundled input fixture changed during reproduction")
    return first


def parse_table(data: bytes) -> tuple[str, list[list[float]]]:
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError as exc:
        raise TraceError("trace table is not ASCII") from exc
    lines = text.splitlines()
    if not lines or not lines[0].startswith("#"):
        raise TraceError("trace table is missing its header")
    rows: list[list[float]] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        values = [float(value) for value in line.split()]
        if not all(math.isfinite(value) for value in values):
            raise TraceError("trace table contains a non-finite value")
        rows.append(values)
    return lines[0], rows


def validate_output_structure(data: bytes) -> None:
    header, rows = parse_table(data)
    if header != "# lnL sigma_lnL x y":
        raise TraceError("proposal output header differs")
    if len(rows) != 25 or any(len(row) != 4 for row in rows):
        raise TraceError("proposal output must contain 25 four-column rows")
    if any(row[0] != 0.0 or row[1] != 0.0 for row in rows):
        raise TraceError("proposal output did not zero lnL and sigma_lnL")
    if any(not -2.4 <= value <= 2.4 for row in rows for value in row[2:]):
        raise TraceError("proposal coordinate escaped the inferred padded input box")


def _git(*arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *arguments],
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )
    if completed.returncode != 0:
        raise TraceError("git provenance check failed: " + completed.stderr.strip())
    return completed.stdout.strip()


def verify_manifest() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if manifest["command"]["arguments"] != ARGUMENT_TEMPLATE:
        raise TraceError("manifest argument template differs from the reproducer")
    if manifest["input"]["sha256"] != sha256_bytes(FIXTURE.read_bytes()):
        raise TraceError("input hash differs from the manifest")
    if manifest["reference_output"]["sha256"] != sha256_bytes(REFERENCE.read_bytes()):
        raise TraceError("reference-output hash differs from the manifest")
    revision = manifest["revision"]["rift"]
    for source in manifest["source_identity"].values():
        identity = _git("rev-parse", f"{revision}:{source['path']}")
        expected = source.get("git_blob", source.get("git_tree"))
        if identity != expected:
            raise TraceError("pinned source identity differs: " + source["path"])
        current_path = ROOT / source["path"]
        if source.get("git_blob") and (
            not current_path.is_file()
            or git_blob_sha1(current_path.read_bytes()) != source["git_blob"]
        ):
            raise TraceError("working source blob differs: " + source["path"])
        changed = subprocess.run(
            ["git", "-C", str(ROOT), "diff", "--quiet", revision, "--", source["path"]],
            timeout=TIMEOUT_SECONDS,
            check=False,
        )
        if changed.returncode != 0:
            raise TraceError("working source tree differs: " + source["path"])
        untracked = _git("ls-files", "--others", "--exclude-standard", "--", source["path"])
        if untracked:
            raise TraceError("untracked source can shadow pinned code: " + source["path"])


def main() -> int:
    observed = reproduce_twice()
    validate_output_structure(observed)
    verify_manifest()
    print("proposal-boundary trace reproduced: " + sha256_bytes(observed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
