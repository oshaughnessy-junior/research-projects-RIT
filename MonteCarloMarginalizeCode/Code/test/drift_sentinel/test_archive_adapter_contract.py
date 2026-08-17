#!/usr/bin/env python3
"""Focused tests for the synthetic backend-neutral adapter contract."""

import copy
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = (
    ROOT / "rift_drift_sentinel" / "examples" / "archive-adapter" /
    "snapshot-draft-v0"
)


def load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def selected_field_projection(snapshot):
    """Compare only deliberately selected fields in hand-authored fixtures."""
    projected = copy.deepcopy(snapshot)
    projected.pop("producer")
    for simulation in projected["simulations"]:
        simulation.pop("native_id", None)
        simulation["state"] = {"normalized": simulation["state"].get("normalized")}
        for level in simulation["levels"]:
            level["state"] = {"normalized": level["state"].get("normalized")}
            for artifact in level["artifacts"]:
                artifact.pop("handle")
    return projected


class ArchiveAdapterContractTests(unittest.TestCase):
    def test_snapshots_validate_when_jsonschema_is_available(self):
        try:
            import jsonschema
        except ImportError:
            self.skipTest("jsonschema is test-only and is not installed")
        schema = load("envelope.schema.json")
        for name in ("file-backed.snapshot.json", "indexed.snapshot.json"):
            jsonschema.validate(load(name), schema)

    def test_unsafe_handle_is_rejected_when_jsonschema_is_available(self):
        try:
            import jsonschema
        except ImportError:
            self.skipTest("jsonschema is test-only and is not installed")
        payload = load("file-backed.snapshot.json")
        payload["simulations"][0]["levels"][0]["artifacts"][0]["handle"]["value"] = (
            "../../private/key"
        )
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(payload, load("envelope.schema.json"))

    def test_schema_is_explicitly_nonconformant_draft(self):
        schema = load("envelope.schema.json")
        self.assertEqual(
            schema["properties"]["vocabulary_version"]["const"],
            "archive.snapshot-draft/v0",
        )
        self.assertNotIn("capabilities", schema["properties"]["producer"]["properties"])

    def test_hand_authored_selected_fields_match(self):
        file_backed = load("file-backed.snapshot.json")
        indexed = load("indexed.snapshot.json")
        self.assertEqual(
            selected_field_projection(file_backed),
            selected_field_projection(indexed),
        )

    def test_lightweight_backend_description_is_non_normative(self):
        payload = load("file-backed.snapshot.json")
        self.assertEqual(payload["producer"]["backend"]["kind"], "plain-files")

    def test_indexed_backend_does_not_claim_operational_capabilities(self):
        payload = load("indexed.snapshot.json")
        self.assertEqual(payload["producer"]["backend"]["kind"], "indexed-database")
        self.assertNotIn("capabilities", payload["producer"])

    def test_artifact_handles_are_opaque_and_bounded(self):
        for name in ("file-backed.snapshot.json", "indexed.snapshot.json"):
            artifact = load(name)["simulations"][0]["levels"][0]["artifacts"][0]
            self.assertEqual(artifact["handle"]["kind"], "opaque")
            self.assertNotIn("/", artifact["handle"]["value"])


if __name__ == "__main__":
    unittest.main()
