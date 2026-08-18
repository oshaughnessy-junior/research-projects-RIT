#!/usr/bin/env python3
"""Validate the sanitized historical local-DAG observation."""

import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]
PROOF = ROOT / "vision" / "proofs" / "rift-local-dag-observation-v0"
RECORD_PATH = PROOF / "sanitized-observation.json"


def reject_non_json_number(value):
    raise ValueError(f"non-JSON numeric constant: {value}")


def load_record():
    with RECORD_PATH.open("r", encoding="utf-8") as stream:
        return json.load(stream, parse_constant=reject_non_json_number)


class RIFTLocalDAGObservationTests(unittest.TestCase):
    def setUp(self):
        self.record = load_record()

    def test_record_is_closed_at_reviewed_boundaries(self):
        self.assertEqual(
            set(self.record),
            {
                "record_type", "status", "source_revision", "environment",
                "budget", "dag", "events", "artifacts", "resources",
                "failure", "claims",
            },
        )
        self.assertEqual(
            set(self.record["environment"]),
            {"platform_family", "python", "numpy", "scipy", "scikit_learn", "hydra", "htcondor"},
        )
        self.assertEqual(
            set(self.record["budget"]),
            {"maximum_nodes", "configured_retries", "wall_time_limit_seconds", "local_scheduler_only", "external_network_allowed"},
        )
        self.assertEqual(set(self.record["dag"]), {"node_roles", "edges"})
        self.assertEqual(
            set(self.record["resources"]),
            {"same_host_sandbox_transfer_observed", "external_transfer_observed", "posterior_worker"},
        )
        self.assertEqual(
            set(self.record["failure"]),
            {"node_role", "exit_code", "category", "tool", "repair_attempted", "retry_attempted"},
        )
        self.assertEqual(
            self.record["claims"],
            {
                "dag_success": False,
                "proposal_artifact_before_dag_failure": True,
                "artifact_existence_implies_success": False,
                "controller_assimilation_observed": False,
                "controller_contract_established": False,
                "portable_behavior_established": False,
                "scientific_validity_established": False,
                "retry_semantics_established": False,
                "checkpoint_restart_semantics_established": False,
                "memory_request_approved": False,
            },
        )

    def test_dag_is_exactly_seven_roles_zero_retry_and_acyclic(self):
        roles = self.record["dag"]["node_roles"]
        edges = [tuple(edge) for edge in self.record["dag"]["edges"]]
        self.assertEqual(len(roles), 7)
        self.assertEqual(len(set(roles)), 7)
        self.assertEqual(self.record["budget"]["maximum_nodes"], 7)
        self.assertEqual(self.record["budget"]["configured_retries"], 0)
        self.assertEqual(
            set(edges),
            {
                ("marginalization", "event_consolidation"),
                ("event_consolidation", "population_consolidation"),
                ("population_consolidation", "unify"),
                ("unify", "posterior_worker"),
                ("posterior_worker", "posterior_join"),
                ("unify", "proposal"),
            },
        )
        self.assertTrue(all(parent in roles and child in roles for parent, child in edges))

        children = {role: [] for role in roles}
        indegree = {role: 0 for role in roles}
        for parent, child in edges:
            children[parent].append(child)
            indegree[child] += 1
        ready = [role for role, degree in indegree.items() if degree == 0]
        visited = []
        while ready:
            role = ready.pop()
            visited.append(role)
            for child in children[role]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
        self.assertEqual(set(visited), set(roles))

    def test_event_sequence_records_six_successes_then_terminal_failure(self):
        events = self.record["events"]
        self.assertEqual([event["sequence"] for event in events], list(range(1, 29)))
        allowed_keys = {
            "dag_submitted": {"sequence", "event_type", "source"},
            "node_submitted": {"sequence", "event_type", "node_role", "source"},
            "node_started": {"sequence", "event_type", "node_role", "source"},
            "node_terminated": {"sequence", "event_type", "node_role", "exit_code", "source"},
            "artifact_observed": {"sequence", "event_type", "artifact_role", "source"},
            "dag_terminated": {"sequence", "event_type", "dag_status", "source"},
        }
        for event in events:
            self.assertIn(event["event_type"], allowed_keys)
            self.assertEqual(set(event), allowed_keys[event["event_type"]])
            self.assertIn(event["source"], {"scheduler_log", "artifact_metadata"})
        successes = {
            event["node_role"]
            for event in events
            if event["event_type"] == "node_terminated" and event.get("exit_code") == 0
        }
        self.assertEqual(
            successes,
            {
                "marginalization", "event_consolidation",
                "population_consolidation", "unify", "proposal",
                "posterior_worker",
            },
        )
        failure = next(
            event for event in events
            if event["event_type"] == "node_terminated" and event.get("exit_code") == 127
        )
        self.assertEqual(failure["node_role"], "posterior_join")

        proposal_success = next(
            event["sequence"] for event in events
            if event["event_type"] == "node_terminated"
            and event.get("node_role") == "proposal"
        )
        proposal_artifact = next(
            event["sequence"] for event in events
            if event.get("artifact_role") == "proposal_population"
        )
        dag_failure = next(
            event["sequence"] for event in events
            if event["event_type"] == "dag_terminated"
        )
        self.assertLess(proposal_success, proposal_artifact)
        self.assertLess(proposal_artifact, failure["sequence"])
        self.assertLess(failure["sequence"], dag_failure)
        self.assertEqual(events[-1]["dag_status"], "node_failed")

    def test_artifact_existence_is_not_success(self):
        artifacts = self.record["artifacts"]
        self.assertTrue(artifacts["proposal_population"]["present"])
        self.assertEqual(artifacts["proposal_population"]["row_count"], 27)
        self.assertTrue(artifacts["proposal_population"]["finite"])
        self.assertTrue(artifacts["proposal_population"]["score_columns_zero"])
        self.assertTrue(artifacts["aggregate_posterior"]["present"])
        self.assertEqual(artifacts["aggregate_posterior"]["row_count"], 0)
        self.assertTrue(artifacts["aggregate_posterior"]["header_only"])
        self.assertFalse(self.record["claims"]["dag_success"])
        self.assertFalse(self.record["claims"]["artifact_existence_implies_success"])

    def test_resource_observations_have_explicit_units_and_no_approval(self):
        resources = self.record["resources"]
        posterior = resources["posterior_worker"]
        self.assertEqual(posterior["request_memory"], {"value": 8192, "unit": "MiB"})
        self.assertEqual(posterior["observed_memory_usage"], {"value": 2, "unit": "MiB"})
        self.assertEqual(posterior["observed_resident_set_size"]["unit"], "KiB")
        self.assertEqual(posterior["sandbox_bytes_received"]["unit"], "byte")
        self.assertTrue(resources["same_host_sandbox_transfer_observed"])
        self.assertFalse(resources["external_transfer_observed"])
        self.assertFalse(self.record["claims"]["memory_request_approved"])

    def test_failure_and_nonclaims_are_explicit(self):
        self.assertEqual(
            self.record["failure"],
            {
                "node_role": "posterior_join",
                "exit_code": 127,
                "category": "unavailable_local_tool",
                "tool": "shuf",
                "repair_attempted": False,
                "retry_attempted": False,
            },
        )
        for claim in (
            "controller_assimilation_observed",
            "controller_contract_established",
            "portable_behavior_established",
            "scientific_validity_established",
            "retry_semantics_established",
            "checkpoint_restart_semantics_established",
        ):
            self.assertFalse(self.record["claims"][claim])

    def test_public_evidence_is_bounded_and_sanitized(self):
        paths = [RECORD_PATH, PROOF / "README.md"]
        forbidden = (
            "/users/", "/private/", "127.0.0.1", "localhost:",
            "endpoint.rit.edu", "rossma", "clusterid", "procid",
            "dagmanjobid", "18641", "18642", "18643", "18644", "18645",
            "18646", "18647", "18648", "mmapipe", "supernu", "github.com",
            "--inj-file", "--using-eos", "grid-1.dat", "all.marg_net",
        )
        for path in paths:
            raw = path.read_text(encoding="utf-8")
            self.assertLess(len(raw.encode("utf-8")), 64 * 1024)
            lowered = raw.lower()
            for marker in forbidden:
                self.assertNotIn(marker, lowered, f"{marker!r} leaked in {path}")

        readme = (PROOF / "README.md").read_text(encoding="utf-8")
        for phrase in (
            "historical, observation-only evidence",
            "artifact existence did not establish overall DAG success",
            "No repair, retry, rescue, or second submission was attempted",
            "establishes no:",
            "one observed environmental boundary",
        ):
            self.assertIn(phrase, readme)


if __name__ == "__main__":
    unittest.main()
