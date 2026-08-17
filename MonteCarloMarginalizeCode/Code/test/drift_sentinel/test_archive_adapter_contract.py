#!/usr/bin/env python3
"""Focused tests for the synthetic backend-neutral adapter contract."""

import copy
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "rift_drift_sentinel" / "examples" / "archive-adapter" / "v1"


def load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def semantic_projection(snapshot):
    """Remove backend-native identity and transport from a read envelope."""
    projected = copy.deepcopy(snapshot)
    projected.pop("adapter")
    for simulation in projected["simulations"]:
        simulation.pop("native_id", None)
        simulation["state"] = {"normalized": simulation["state"].get("normalized")}
        for level in simulation["levels"]:
            level["state"] = {"normalized": level["state"].get("normalized")}
            for artifact in level["artifacts"]:
                artifact.pop("locator")
    return projected


class ArchiveAdapterContractTests(unittest.TestCase):
    def test_schema_requires_read_capability(self):
        schema = load("envelope.schema.json")
        capabilities = schema["properties"]["adapter"]["properties"]["capabilities"]
        self.assertEqual(capabilities["contains"], {"const": "archive.read/v1"})

    def test_backends_emit_same_semantics(self):
        file_backed = load("file-backed.snapshot.json")
        indexed = load("indexed.snapshot.json")
        self.assertEqual(semantic_projection(file_backed), semantic_projection(indexed))

    def test_lightweight_backend_needs_only_read_capability(self):
        payload = load("file-backed.snapshot.json")
        self.assertEqual(payload["adapter"]["capabilities"], ["archive.read/v1"])
        self.assertEqual(payload["adapter"]["backend"]["kind"], "plain-files")

    def test_optional_query_capability_does_not_change_read_semantics(self):
        payload = load("indexed.snapshot.json")
        self.assertIn("archive.query/v1", payload["adapter"]["capabilities"])
        self.assertNotIn("archive.register/v1", payload["adapter"]["capabilities"])

    def test_deterministic_simulation_order(self):
        for name in ("file-backed.snapshot.json", "indexed.snapshot.json"):
            ids = [item["id"] for item in load(name)["simulations"]]
            self.assertEqual(ids, sorted(ids))


if __name__ == "__main__":
    unittest.main()
