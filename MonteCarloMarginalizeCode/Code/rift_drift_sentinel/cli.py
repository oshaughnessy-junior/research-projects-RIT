"""Command line boundary for resolved, local-only sentinel inputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .engine import evaluate
from .model import SentinelInputError, load_registry, load_resolved_inputs
from .report import render_human, render_machine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rift-drift-sentinel")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate a registry and print its canonical fingerprint")
    validate.add_argument("--registry", required=True, type=Path)
    check = subparsers.add_parser("check", help="compare registered local contract fixtures")
    check.add_argument("--registry", required=True, type=Path)
    check.add_argument("--resolved-inputs", required=True, type=Path)
    check.add_argument("--run-id", required=True, help="runner-assigned stable run identifier")
    check.add_argument("--as-of", required=True, help="runner-assigned YYYY-MM-DD for exception evaluation")
    check.add_argument("--machine-output", type=Path, help="write deterministic JSON report")
    check.add_argument(
        "--fail-on-incompatible",
        action="store_true",
        help="opt-in exit 1 for incompatible findings; observation-only is the default",
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        registry = load_registry(args.registry)
        if args.command == "validate":
            print(f"{registry.registry_id} {registry.fingerprint}")
            return 0
        resolved = load_resolved_inputs(args.resolved_inputs, registry)
        report = evaluate(registry, resolved, args.run_id, args.as_of)
    except (OSError, SentinelInputError, ValueError) as exc:
        print(f"invalid sentinel input: {exc}", file=sys.stderr)
        return 2
    machine = render_machine(report)
    if args.machine_output:
        args.machine_output.write_text(machine, encoding="utf-8")
    print(render_human(report), end="")
    return 1 if args.fail_on_incompatible and report["summary"]["blocking_incompatible"] else 0
