"""Deterministic machine and human report rendering."""

from __future__ import annotations

import json
from typing import Any, Mapping


def render_machine(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True, ensure_ascii=True) + "\n"


def render_human(report: Mapping[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        f"Drift sentinel run {report['run_id']}: {report['status']}",
        (
            f"Registry {report['registry']['id']} ({report['registry']['fingerprint']}); "
            f"compatible={summary['compatible']} incompatible={summary['incompatible']} "
            f"indeterminate={summary['indeterminate']} excepted={summary['intentionally_divergent']}"
        ),
    ]
    for check in report["checks"]:
        lines.append(
            f"- [{check['outcome']}/{check['observation_status']}] {check['group']}/{check['edge']} "
            f"({check['producer']} -> {check['consumer']}), owner={check['owner']}, severity={check['severity']}"
        )
        if check.get("reason"):
            lines.append(f"  reason: {check['reason']}")
        if check.get("exception"):
            item = check["exception"]
            lines.append(f"  exception: {item['id']} owned by {item['owner']}, expires {item['expires']}")
        for mismatch in check.get("mismatches", ()):  # bounded to declared fixture size
            lines.append(
                f"  {mismatch['path']}: expected {json.dumps(mismatch['expected'], sort_keys=True)}, "
                f"observed {json.dumps(mismatch['observed'], sort_keys=True)}"
            )
    return "\n".join(lines) + "\n"
