#!/usr/bin/env python3
"""Focused stdlib tests for the offline drift-sentinel MVP."""

import copy
import contextlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE_ROOT))

from rift_drift_sentinel.cli import main as cli_main  # noqa: E402
from rift_drift_sentinel.engine import evaluate  # noqa: E402
from rift_drift_sentinel.model import SentinelInputError, load_registry, load_resolved_inputs  # noqa: E402
from rift_drift_sentinel.report import render_human, render_machine  # noqa: E402


EXAMPLES = CODE_ROOT / "rift_drift_sentinel" / "examples"
REVISION_A = "a" * 40
REVISION_B = "b" * 40


class DriftSentinelTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        shutil.copytree(EXAMPLES, self.root / "examples")
        self.registry_path = self.root / "examples" / "pilot-registry.json"
        self.registry = load_registry(self.registry_path)
        self.resolved_path = self.root / "resolved.json"
        self._write_resolved()

    def tearDown(self):
        self.tempdir.cleanup()

    def _write_resolved(self):
        nodes = self.root / "examples" / "nodes"
        payload = {
            "resolved_inputs_version": "rift-drift-resolved-inputs/v1",
            "registry_fingerprint": self.registry.fingerprint,
            "groups": {
                "rift-supernu": {
                    "supernu-manager": {
                        "root": str(nodes / "supernu-manager"),
                        "revision": REVISION_A,
                        "source_id": "oshaughnessy-junior/sim_manager_supernu"
                    },
                    "rift-hyperpipe": {
                        "root": str(nodes / "rift-hyperpipe"),
                        "revision": REVISION_B,
                        "source_id": "oshaughnessy-junior/research-projects-RIT"
                    }
                },
                "synthetic-protocol-runner": {
                    "protocol": {
                        "root": str(nodes / "protocol"),
                        "revision": "sha256:" + "c" * 64,
                        "source_id": "local/synthetic-protocol"
                    },
                    "runner": {
                        "root": str(nodes / "runner"),
                        "revision": "sha256:" + "d" * 64,
                        "source_id": "local/synthetic-runner"
                    }
                }
            }
        }
        self.resolved_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_disconnected_groups_and_inventory_uncertainty(self):
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        report = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        self.assertEqual(report["summary"]["compatible"], 1)
        self.assertEqual(report["summary"]["incompatible"], 0)
        self.assertEqual(report["summary"]["indeterminate"], 1)
        pilot = next(item for item in report["checks"] if item["group"] == "rift-supernu")
        self.assertEqual(pilot["outcome"], "indeterminate")
        self.assertIn("not been owner-verified", pilot["reason"])

    def test_incompatible_fixture_is_actionable_and_default_is_observation_only(self):
        producer = self.root / "examples" / "nodes" / "protocol" / "contracts" / "report.schema.json"
        schema = json.loads(producer.read_text(encoding="utf-8"))
        schema["properties"]["status"]["enum"] = ["compatible"]
        producer.write_text(json.dumps(schema), encoding="utf-8")
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        report = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        finding = next(item for item in report["checks"] if item["outcome"] == "incompatible")
        self.assertEqual(finding["edge"], "report-consumption")
        self.assertEqual(finding["owner"], "drift-sentinel-maintainers")
        self.assertTrue(finding["mismatches"][0]["path"].startswith("$.properties[sha256:"))
        self.assertTrue(finding["mismatches"][0]["path"].endswith("].enum"))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli_main([
                "check", "--registry", str(self.registry_path), "--resolved-inputs", str(self.resolved_path),
                "--run-id", "test-run", "--as-of", "2026-08-17"
            ]), 0)
            self.assertEqual(cli_main([
                "check", "--registry", str(self.registry_path), "--resolved-inputs", str(self.resolved_path),
                "--run-id", "test-run", "--as-of", "2026-08-17", "--fail-on-incompatible"
            ]), 1)

    def test_machine_and_human_reports_are_deterministic_and_do_not_leak_roots(self):
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        first = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        second = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        self.assertEqual(render_machine(first), render_machine(second))
        self.assertEqual(render_human(first), render_human(second))
        self.assertNotIn(str(self.root), render_machine(first))

    def test_registry_rejects_cycles(self):
        raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
        group = raw["groups"][1]
        reverse = copy.deepcopy(group["edges"][0])
        reverse.update({"id": "reverse-cycle", "producer": "runner", "consumer": "protocol"})
        group["edges"].append(reverse)
        path = self.root / "cycle.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(SentinelInputError, "contains a cycle"):
            load_registry(path)

    def test_resolved_inputs_require_immutable_revision_and_matching_fingerprint(self):
        raw = json.loads(self.resolved_path.read_text(encoding="utf-8"))
        raw["groups"]["rift-supernu"]["rift-hyperpipe"]["revision"] = "main"
        raw["registry_fingerprint"] = "sha256:" + "0" * 64
        self.resolved_path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaises(SentinelInputError) as context:
            load_resolved_inputs(self.resolved_path, self.registry)
        self.assertIn("does not match", str(context.exception))
        self.assertIn("immutable", str(context.exception))

    def test_path_escape_becomes_indeterminate_not_pass(self):
        # A symlink can change after registry validation; the engine rechecks containment.
        contract_dir = self.root / "examples" / "nodes" / "protocol" / "contracts"
        source = contract_dir / "report.schema.json"
        outside = self.root / "outside.json"
        outside.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        source.unlink()
        source.symlink_to(outside)
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        report = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        check = next(item for item in report["checks"] if item["edge"] == "report-consumption")
        self.assertEqual(check["outcome"], "indeterminate")
        self.assertIn("escapes", check["reason"])

    def test_published_wire_schemas_are_versioned_json(self):
        schemas = CODE_ROOT / "rift_drift_sentinel" / "schemas"
        registry_schema = json.loads((schemas / "registry.schema.json").read_text(encoding="utf-8"))
        resolved_schema = json.loads((schemas / "resolved-inputs.schema.json").read_text(encoding="utf-8"))
        report_schema = json.loads((schemas / "report.schema.json").read_text(encoding="utf-8"))
        self.assertEqual(registry_schema["properties"]["registry_version"]["const"], "rift-drift-registry/v1")
        self.assertEqual(
            resolved_schema["properties"]["resolved_inputs_version"]["const"],
            "rift-drift-resolved-inputs/v1",
        )
        self.assertEqual(report_schema["properties"]["report_version"]["const"], "rift-drift-report/v1")

    def test_active_exception_is_visible_and_suppresses_opt_in_gate_only_until_expiry(self):
        raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
        edge = raw["groups"][1]["edges"][0]
        edge["exceptions"] = [{
            "id": "seeded-test-divergence",
            "owner": "drift-sentinel-maintainers",
            "rationale": "exercise explicit exception handling",
            "expires": "2026-08-18",
            "approvers": ["drift-sentinel-maintainers", "coding-sysadmin"]
        }]
        self.registry_path.write_text(json.dumps(raw), encoding="utf-8")
        self.registry = load_registry(self.registry_path)
        self._write_resolved()
        producer = self.root / "examples" / "nodes" / "protocol" / "contracts" / "report.schema.json"
        schema = json.loads(producer.read_text(encoding="utf-8"))
        schema["properties"]["status"]["enum"] = ["compatible"]
        producer.write_text(json.dumps(schema), encoding="utf-8")
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        active = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        finding = next(item for item in active["checks"] if item["edge"] == "report-consumption")
        self.assertEqual(finding["observation_status"], "intentionally_divergent")
        self.assertEqual(active["summary"]["blocking_incompatible"], 0)
        expired = evaluate(self.registry, resolved, "test-run", "2026-08-19")
        finding = next(item for item in expired["checks"] if item["edge"] == "report-consumption")
        self.assertEqual(finding["observation_status"], "observed")
        self.assertEqual(expired["summary"]["blocking_incompatible"], 1)

    def test_exception_requires_both_edge_owners(self):
        raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
        raw["groups"][0]["edges"][0]["exceptions"] = [{
            "id": "one-sided-waiver",
            "owner": "coding-rift",
            "rationale": "must not be sufficient",
            "expires": "2026-08-18",
            "approvers": ["coding-rift"]
        }]
        path = self.root / "one-sided.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(SentinelInputError, "missing affected node owners"):
            load_registry(path)

    def test_untrusted_mismatch_values_are_fingerprinted_not_copied(self):
        producer = self.root / "examples" / "nodes" / "protocol" / "contracts" / "report.schema.json"
        schema = json.loads(producer.read_text(encoding="utf-8"))
        marker = "TOKEN_MUST_NOT_APPEAR_IN_REPORT"
        schema["properties"]["status"]["enum"] = [marker]
        producer.write_text(json.dumps(schema), encoding="utf-8")
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        report_text = render_machine(evaluate(self.registry, resolved, "test-run", "2026-08-17"))
        self.assertNotIn(marker, report_text)
        self.assertIn("sha256", report_text)


if __name__ == "__main__":
    unittest.main()
