#!/usr/bin/env python3
"""Focused stdlib tests for the offline drift-sentinel MVP."""

import copy
import contextlib
import hashlib
import io
import json
import os
import shutil
import subprocess
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
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "archive-root"
PRODUCER_CONTRACT_REVISION = "sha256:6161347d63360a35251f623c4d1d9584bb9d19cea8d5f7550a81f254198dc14f"
CONSUMER_CONTRACT_REVISION = "sha256:820d0b894fd9d7e75f27c990dd47840ab70bd86cfa148a32e347666d70cfa2b1"
GOLDEN_SHA256 = "2ebdf7b01e9035f99befb84b41352b955cbb1b81ff4db59f20354b8c3281d62f"


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
                        "revision": PRODUCER_CONTRACT_REVISION,
                        "source_id": "private/source-a"
                    },
                    "rift-simulation-manager": {
                        "root": str(nodes / "rift-simulation-manager"),
                        "revision": CONSUMER_CONTRACT_REVISION,
                        "source_id": "public/rift-simulation-manager"
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
        self.assertGreater(len(pilot["mismatches"]), 0)
        self.assertEqual(pilot["revisions"]["supernu-manager"], PRODUCER_CONTRACT_REVISION)
        self.assertEqual(pilot["revisions"]["rift-simulation-manager"], CONSUMER_CONTRACT_REVISION)
        producer_contract = (
            self.root / "examples" / "nodes" / "supernu-manager" /
            "contracts" / "archive-root.producer.schema.json"
        ).read_bytes()
        consumer_contract = (
            self.root / "examples" / "nodes" / "rift-simulation-manager" /
            "contracts" / "archive-root.required.schema.json"
        ).read_bytes()
        self.assertEqual(
            pilot["evidence"]["producer"]["sha256"],
            PRODUCER_CONTRACT_REVISION,
        )
        self.assertEqual(
            pilot["evidence"]["consumer"]["sha256"],
            CONSUMER_CONTRACT_REVISION,
        )

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

    def test_unsupported_property_type_is_indeterminate_not_compatible(self):
        producer = self.root / "examples" / "nodes" / "protocol" / "contracts" / "report.schema.json"
        consumer = self.root / "examples" / "nodes" / "runner" / "contracts" / "report-required.schema.json"
        for path in (producer, consumer):
            schema = json.loads(path.read_text(encoding="utf-8"))
            schema["properties"]["status"]["type"] = "not-a-json-schema-type"
            path.write_text(json.dumps(schema), encoding="utf-8")
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        report = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        finding = next(item for item in report["checks"] if item["edge"] == "report-consumption")
        self.assertEqual(finding["outcome"], "indeterminate")
        self.assertIn("not supported", finding["reason"])

    def test_enum_order_is_not_a_contract_mismatch(self):
        producer = self.root / "examples" / "nodes" / "protocol" / "contracts" / "report.schema.json"
        schema = json.loads(producer.read_text(encoding="utf-8"))
        schema["properties"]["status"]["enum"].reverse()
        producer.write_text(json.dumps(schema), encoding="utf-8")
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        report = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        finding = next(item for item in report["checks"] if item["edge"] == "report-consumption")
        self.assertEqual(finding["outcome"], "compatible")

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
        raw["groups"]["rift-supernu"]["rift-simulation-manager"]["revision"] = "main"
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
        producer = self.root / "examples" / "nodes" / "protocol" / "contracts" / "report.schema.json"
        schema = json.loads(producer.read_text(encoding="utf-8"))
        schema["properties"]["status"]["enum"] = ["compatible"]
        producer.write_text(json.dumps(schema), encoding="utf-8")
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        initial = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        initial_finding = next(item for item in initial["checks"] if item["edge"] == "report-consumption")
        raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
        edge = raw["groups"][1]["edges"][0]
        edge["exceptions"] = [{
            "id": "seeded-test-divergence",
            "owner": "drift-sentinel-maintainers",
            "rationale": "exercise explicit exception handling",
            "expires": "2026-08-18",
            "approvers": ["drift-sentinel-maintainers", "coding-sysadmin"],
            "mismatch_fingerprint": initial_finding["mismatch_fingerprint"]
        }]
        self.registry_path.write_text(json.dumps(raw), encoding="utf-8")
        self.registry = load_registry(self.registry_path)
        self._write_resolved()
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        active = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        finding = next(item for item in active["checks"] if item["edge"] == "report-consumption")
        self.assertEqual(finding["observation_status"], "intentionally_divergent")
        self.assertEqual(active["summary"]["blocking_incompatible"], 0)
        schema["required"].remove("status")
        producer.write_text(json.dumps(schema), encoding="utf-8")
        expanded = evaluate(self.registry, resolved, "test-run", "2026-08-17")
        finding = next(item for item in expanded["checks"] if item["edge"] == "report-consumption")
        self.assertEqual(finding["observation_status"], "observed")
        self.assertEqual(expanded["summary"]["blocking_incompatible"], 1)
        schema["required"].append("status")
        producer.write_text(json.dumps(schema), encoding="utf-8")
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
            "approvers": ["coding-rift"],
            "mismatch_fingerprint": "sha256:" + "0" * 64
        }]
        path = self.root / "one-sided.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(SentinelInputError, "missing affected node owners"):
            load_registry(path)

    def test_exception_requires_sha256_mismatch_scope(self):
        raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
        edge = raw["groups"][1]["edges"][0]
        edge["exceptions"] = [{
            "id": "bad-scope",
            "owner": "drift-sentinel-maintainers",
            "rationale": "a Git commit is not a mismatch-set identity",
            "expires": "2026-08-18",
            "approvers": ["drift-sentinel-maintainers", "coding-sysadmin"],
            "mismatch_fingerprint": "0" * 40,
        }]
        path = self.root / "bad-scope.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(SentinelInputError, "expected sha256 content identity"):
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

    def _evaluate_seeded_pilot_schema(self, fixture_name):
        raw = json.loads((EXAMPLES / "pilot-registry.json").read_text(encoding="utf-8"))
        raw["registry_id"] = "synthetic-seeded-registry"
        group = raw["groups"][0]
        group["id"] = "synthetic-archive-root-fixture"
        group["owner"] = "synthetic-fixture-owner"
        producer_node, consumer_node = group["nodes"]
        producer_node.update({
            "id": "synthetic-archive-producer",
            "role": "synthetic-producer",
            "owner": "synthetic-fixture-owner",
            "source": {
                "id": "local/synthetic-archive-producer",
                "default_ref": "sha256:" + "e" * 64,
                "visibility": "local",
            },
        })
        consumer_node.update({
            "id": "synthetic-archive-consumer",
            "role": "synthetic-consumer",
            "owner": "synthetic-fixture-owner",
            "source": {
                "id": "local/synthetic-archive-consumer",
                "default_ref": "sha256:" + "f" * 64,
                "visibility": "local",
            },
        })
        edge = group["edges"][0]
        edge.update({
            "id": "synthetic-archive-root-check",
            "producer": "synthetic-archive-producer",
            "consumer": "synthetic-archive-consumer",
            "owner": "synthetic-fixture-owner",
        })
        edge["contract"].update({
            "id": "synthetic-archive-root-contract",
            "version": "synthetic-v1",
            "verification": "verified",
            "semantics": {"content_identity": "synthetic fixture bytes only"},
        })
        disconnected = raw["groups"][1]
        disconnected["id"] = "synthetic-disconnected-fixture"
        disconnected["owner"] = "synthetic-fixture-owner"
        protocol_node, runner_node = disconnected["nodes"]
        protocol_node.update({
            "id": "synthetic-protocol-node",
            "owner": "synthetic-fixture-owner",
            "source": {
                "id": "local/synthetic-protocol-node",
                "default_ref": "sha256:" + "c" * 64,
                "visibility": "local",
            },
        })
        runner_node.update({
            "id": "synthetic-runner-node",
            "owner": "synthetic-fixture-owner",
            "source": {
                "id": "local/synthetic-runner-node",
                "default_ref": "sha256:" + "d" * 64,
                "visibility": "local",
            },
        })
        disconnected_edge = disconnected["edges"][0]
        disconnected_edge.update({
            "id": "synthetic-report-consumption",
            "producer": "synthetic-protocol-node",
            "consumer": "synthetic-runner-node",
            "owner": "synthetic-fixture-owner",
        })
        disconnected_edge["contract"].update({
            "id": "synthetic-report-envelope",
            "version": "synthetic-v1",
        })
        self.registry_path.write_text(json.dumps(raw), encoding="utf-8")
        self.registry = load_registry(self.registry_path)
        producer = (
            self.root / "examples" / "nodes" / "supernu-manager" /
            "contracts" / "archive-root.producer.schema.json"
        )
        shutil.copyfile(FIXTURES / fixture_name, producer)
        consumer = (
            self.root / "examples" / "nodes" / "rift-simulation-manager" /
            "contracts" / "archive-root.required.schema.json"
        )
        consumer_schema = json.loads(consumer.read_text(encoding="utf-8"))
        consumer_schema["x-provenance"] = {
            "source_id": "local/synthetic-archive-consumer",
            "evidence_scope": "synthetic root manifest filename fixture",
        }
        consumer.write_text(
            json.dumps(consumer_schema, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        nodes = self.root / "examples" / "nodes"
        payload = {
            "resolved_inputs_version": "rift-drift-resolved-inputs/v1",
            "registry_fingerprint": self.registry.fingerprint,
            "groups": {
                "synthetic-archive-root-fixture": {
                    "synthetic-archive-producer": {
                        "root": str(nodes / "supernu-manager"),
                        "revision": "sha256:" + "e" * 64,
                        "source_id": "local/synthetic-archive-producer",
                    },
                    "synthetic-archive-consumer": {
                        "root": str(nodes / "rift-simulation-manager"),
                        "revision": "sha256:" + "f" * 64,
                        "source_id": "local/synthetic-archive-consumer",
                    },
                },
                "synthetic-disconnected-fixture": {
                    "synthetic-protocol-node": {
                        "root": str(nodes / "protocol"),
                        "revision": "sha256:" + "c" * 64,
                        "source_id": "local/synthetic-protocol-node",
                    },
                    "synthetic-runner-node": {
                        "root": str(nodes / "runner"),
                        "revision": "sha256:" + "d" * 64,
                        "source_id": "local/synthetic-runner-node",
                    },
                },
            },
        }
        self.resolved_path.write_text(json.dumps(payload), encoding="utf-8")
        resolved = load_resolved_inputs(self.resolved_path, self.registry)
        report = evaluate(self.registry, resolved, "seeded-pilot", "2026-08-17")
        machine = render_machine(report)
        self.assertNotIn("supernu-manager", machine)
        self.assertNotIn("rift-simulation-manager", machine)
        self.assertNotIn(PRODUCER_CONTRACT_REVISION, machine)
        self.assertNotIn(CONSUMER_CONTRACT_REVISION, machine)
        return next(item for item in report["checks"] if item["edge"] == "synthetic-archive-root-check")

    def test_seeded_archive_root_compatible_fixture(self):
        check = self._evaluate_seeded_pilot_schema("producer.compatible-synthetic.schema.json")
        self.assertEqual(check["outcome"], "compatible")
        self.assertEqual(check["mismatches"], [])

    def test_seeded_archive_root_incompatible_variants(self):
        variants = (
            "producer.incompatible-root-filename.schema.json",
            "producer.incompatible-missing-root.schema.json",
        )
        for fixture_name in variants:
            with self.subTest(fixture=fixture_name):
                check = self._evaluate_seeded_pilot_schema(fixture_name)
                self.assertEqual(check["outcome"], "incompatible")
                self.assertGreater(len(check["mismatches"]), 0)

    def test_supernu_golden_artifact_and_provenance_are_self_consistent(self):
        golden_dir = EXAMPLES / "nodes" / "supernu-manager" / "golden"
        golden_bytes = (golden_dir / "archive.json").read_bytes()
        golden = json.loads(golden_bytes)
        provenance = json.loads((golden_dir / "PROVENANCE.json").read_text(encoding="utf-8"))
        producer_schema = json.loads(
            (EXAMPLES / "nodes" / "supernu-manager" / "contracts" /
             "archive-root.producer.schema.json").read_text(encoding="utf-8")
        )
        self.assertEqual(set(golden), set(producer_schema["required"]))
        self.assertEqual(golden["schema"], "supernu.archive/1")
        self.assertEqual(golden["created_utc"], 0.0)
        self.assertEqual(provenance["producer"]["source_id"], "private/source-a")
        self.assertEqual(provenance["producer"]["contract_revision"], PRODUCER_CONTRACT_REVISION)
        self.assertEqual(provenance["consumer"]["source_id"], "public/rift-simulation-manager")
        self.assertEqual(hashlib.sha256(golden_bytes).hexdigest(), provenance["golden_sha256"])
        self.assertFalse(golden_bytes.endswith(b"\n"))

    @unittest.skipUnless(
        os.environ.get("RIFT_DRIFT_SENTINEL_SUPERNU_CHECKOUT"),
        "set RIFT_DRIFT_SENTINEL_SUPERNU_CHECKOUT for implementation regeneration",
    )
    def test_supplied_implementation_regenerates_exact_golden(self):
        script = Path(__file__).resolve().parent / "reproduce_supernu_archive_golden.py"
        subprocess.run(
            [
                sys.executable,
                str(script),
                "--checkout",
                os.environ["RIFT_DRIFT_SENTINEL_SUPERNU_CHECKOUT"],
            ],
            check=True,
            capture_output=True,
            text=True,
        )


if __name__ == "__main__":
    unittest.main()
