#!/usr/bin/env python3
"""Validate the sanitized two-iteration native handoff observation."""

import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]
PROOF = ROOT / "vision" / "proofs" / "rift-two-iteration-handoff-v0"
RECORD_PATH = PROOF / "sanitized-observation.json"


def reject_non_json_number(value):
    raise ValueError(f"non-JSON numeric constant: {value}")


def load_record():
    with RECORD_PATH.open("r", encoding="utf-8") as stream:
        return json.load(stream, parse_constant=reject_non_json_number)


class RIFTTwoIterationHandoffTests(unittest.TestCase):
    def setUp(self):
        self.record = load_record()

    def test_record_and_nested_shapes_are_closed(self):
        self.assertEqual(
            set(self.record),
            {"record_type", "status", "source_revision", "run_bounds", "dag",
             "observations", "populations", "path_outcomes", "claims"},
        )
        self.assertEqual(
            set(self.record["run_bounds"]),
            {"iteration_count", "node_count", "configured_retries",
             "dag_unchanged_for_observation"},
        )
        self.assertEqual(set(self.record["dag"]), {"node_roles", "edges"})
        self.assertEqual(
            set(self.record["populations"]),
            {"proposal_0", "marginalization_1_output", "relations"},
        )
        for role in ("proposal_0", "marginalization_1_output"):
            self.assertEqual(
                set(self.record["populations"][role]),
                {"row_count", "finite", "unique_proposal_coordinates"},
            )
        self.assertEqual(
            set(self.record["path_outcomes"]),
            {"completed", "posterior_joins", "convergence_node", "dag_status"},
        )
        for failure in self.record["path_outcomes"]["posterior_joins"]:
            self.assertEqual(
                set(failure), {"iteration", "exit_code", "category", "tool"}
            )

    def test_observation_variants_are_exactly_closed(self):
        allowed = {
            "native_direct_handoff": {
                "observation_type", "producer_role", "consumer_role",
                "direct_edge_observed",
            },
            "absent_direct_edge": {
                "observation_type", "producer_role", "consumer_role",
                "direct_edge_observed",
            },
            "native_iteration_input_wiring": {
                "observation_type", "producer_role",
                "producer_output_iteration_index", "consumer_role",
                "consumer_input_iteration_index", "wiring_observed",
            },
            "node_interval_overlap": {
                "observation_type", "running_role", "completed_role",
                "overlap_observed",
            },
        }
        observations = self.record["observations"]
        self.assertEqual(
            [item["observation_type"] for item in observations], list(allowed)
        )
        for item in observations:
            self.assertEqual(set(item), allowed[item["observation_type"]])
        self.assertEqual(
            observations,
            [{"observation_type": "native_direct_handoff",
              "producer_role": "proposal_0",
              "consumer_role": "marginalization_1",
              "direct_edge_observed": True},
             {"observation_type": "absent_direct_edge",
              "producer_role": "posterior_join_0",
              "consumer_role": "marginalization_1",
              "direct_edge_observed": False},
             {"observation_type": "native_iteration_input_wiring",
              "producer_role": "proposal_0",
              "producer_output_iteration_index": 1,
              "consumer_role": "marginalization_1",
              "consumer_input_iteration_index": 1,
              "wiring_observed": True},
             {"observation_type": "node_interval_overlap",
              "running_role": "posterior_worker_0",
              "completed_role": "marginalization_1",
              "overlap_observed": True}],
        )

    def test_exact_two_iteration_dag_and_handoff(self):
        roles = self.record["dag"]["node_roles"]
        edges = {tuple(edge) for edge in self.record["dag"]["edges"]}
        expected_edges = {
            ("marginalization_0", "event_consolidation_0"),
            ("event_consolidation_0", "population_consolidation_0"),
            ("population_consolidation_0", "unify_0"),
            ("unify_0", "posterior_worker_0"),
            ("posterior_worker_0", "posterior_join_0"),
            ("unify_0", "proposal_0"),
            ("proposal_0", "marginalization_1"),
            ("marginalization_1", "event_consolidation_1"),
            ("event_consolidation_1", "population_consolidation_1"),
            ("population_consolidation_1", "unify_1"),
            ("unify_1", "posterior_worker_1"),
            ("posterior_worker_1", "posterior_join_1"),
            ("unify_1", "proposal_1"),
            ("posterior_join_1", "convergence"),
        }
        self.assertEqual(len(roles), 15)
        self.assertEqual(len(set(roles)), 15)
        self.assertEqual(len(self.record["dag"]["edges"]), len(expected_edges))
        self.assertEqual(edges, expected_edges)
        self.assertIn(("proposal_0", "marginalization_1"), edges)
        self.assertNotIn(("posterior_join_0", "marginalization_1"), edges)
        self.assertEqual(
            self.record["run_bounds"],
            {"iteration_count": 2, "node_count": 15, "configured_retries": 0,
             "dag_unchanged_for_observation": True},
        )

    def test_native_population_relationship_is_exact(self):
        populations = self.record["populations"]
        for role in ("proposal_0", "marginalization_1_output"):
            self.assertEqual(populations[role]["row_count"], 27)
            self.assertTrue(populations[role]["finite"])
            self.assertTrue(populations[role]["unique_proposal_coordinates"])
        self.assertEqual(
            populations["relations"],
            {"proposal_0_to_marginalization_1_ordered_coordinate_equality": True,
             "proposal_0_differs_from_initial_population": True},
        )

    def test_overlap_failures_and_terminal_state_are_bounded(self):
        overlap = self.record["observations"][3]
        self.assertEqual(overlap["running_role"], "posterior_worker_0")
        self.assertEqual(overlap["completed_role"], "marginalization_1")
        self.assertTrue(overlap["overlap_observed"])
        outcomes = self.record["path_outcomes"]
        self.assertEqual(
            outcomes["completed"],
            ["iteration_0_evaluated_data", "iteration_0_proposal",
             "iteration_1_evaluated_data", "iteration_1_proposal"],
        )
        self.assertEqual(
            outcomes["posterior_joins"],
            [{"iteration": 0, "exit_code": 127,
              "category": "unavailable_local_tool", "tool": "shuf"},
             {"iteration": 1, "exit_code": 127,
              "category": "unavailable_local_tool", "tool": "shuf"}],
        )
        self.assertEqual(outcomes["convergence_node"], "futile")
        self.assertEqual(outcomes["dag_status"], "node_failed")

    def test_claim_map_limits_evidence_to_native_handoff(self):
        self.assertEqual(
            self.record["claims"],
            {"native_handoff_observed": True,
             "direct_proposal_to_next_marginalization_edge_observed": True,
             "marginalization_1_completed_before_posterior_join_0_terminated": True,
             "controller_assimilation_observed": False,
             "controller_state_observed": False,
             "backend_neutral_contract_established": False,
             "portable_ordering_established": False,
             "success_policy_established": False,
             "scientific_validity_established": False,
             "cross_domain_behavior_established": False,
             "retry_or_recovery_semantics_established": False},
        )

    def test_public_evidence_is_bounded_and_sanitized(self):
        paths = [RECORD_PATH, PROOF / "README.md"]
        forbidden = (
            "/users/", "/private/", "127.0.0.1", "localhost:", "clusterid",
            "procid", "dagmanjobid", "globaljobid", "machine =", "owner =",
            "github.com", "all.marg_net", "grid-", ".dag", ".sub", ".log",
        )
        for path in paths:
            raw = path.read_text(encoding="utf-8")
            self.assertLess(len(raw.encode("utf-8")), 64 * 1024)
            lowered = raw.lower()
            for marker in forbidden:
                self.assertNotIn(marker, lowered, f"{marker!r} leaked in {path}")

        readme = (PROOF / "README.md").read_text(encoding="utf-8")
        for phrase in (
            "native handoff evidence",
            "They do not show controller",
            "controller-owned state",
            "It is not a portable row-order contract",
            "establishes no:",
        ):
            self.assertIn(phrase, readme)


if __name__ == "__main__":
    unittest.main()
