#!/usr/bin/env python3
"""Regenerate the sanitized archive-root golden from a supplied checkout.

This is a bounded provenance check, not part of the sentinel core. It imports
and executes the caller-selected local implementation, so callers must supply a
trusted checkout. It performs no network access and uses only a temporary output
directory.
"""

import argparse
import hashlib
import json
import sys
import tempfile
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[2]
GOLDEN = (
    CODE_ROOT / "rift_drift_sentinel" / "examples" / "nodes" /
    "supernu-manager" / "golden" / "archive.json"
)


def normalized_bytes(payload):
    created_utc = payload.get("created_utc")
    if isinstance(created_utc, bool) or not isinstance(created_utc, (int, float)):
        raise ValueError("created_utc must be a numeric producer timestamp")
    normalized = dict(payload)
    normalized["created_utc"] = 0.0
    return json.dumps(normalized, indent=2, sort_keys=True).encode("utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout", required=True, type=Path)
    args = parser.parse_args(argv)
    checkout = args.checkout.resolve()
    if not (checkout / "simulation_manager" / "database.py").is_file():
        parser.error("checkout does not contain simulation_manager/database.py")

    sys.path.insert(0, str(checkout))
    from simulation_manager.database import make_archive

    with tempfile.TemporaryDirectory(prefix="drift-sentinel-golden-") as temp:
        root = Path(temp) / "archive"
        make_archive(
            root,
            scheduler="condor",
            description="sanitized drift-sentinel fixture",
            archive_id="00000000-0000-0000-0000-000000000000",
        )
        actual = normalized_bytes(json.loads((root / "archive.json").read_text(encoding="utf-8")))

    expected = GOLDEN.read_bytes()
    if actual != expected:
        print("sanitized golden differs from supplied implementation", file=sys.stderr)
        return 1
    digest = hashlib.sha256(actual).hexdigest()
    print(f"exact sanitized golden match: sha256:{digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
