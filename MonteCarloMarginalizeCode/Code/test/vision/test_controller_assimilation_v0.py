#!/usr/bin/env python3
"""Focused tests for the standalone controller assimilation reducer."""

import copy
import importlib.util
import json
from pathlib import Path
import threading
import unittest


ROOT = Path(__file__).resolve().parents[4]
CONTRACT = ROOT / "vision" / "contracts" / "controller-assimilation-v0"
SPEC = importlib.util.spec_from_file_location("assimilation_v0", CONTRACT / "reference.py")
assimilation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(assimilation)

try:
    from jsonschema import Draft202012Validator
except ImportError:  # pragma: no cover - optional test-only dependency
    Draft202012Validator = None


def request(request_id, logical_id, attempt, domain_id="opaque.domain"):
    return {
        "vocabulary_version": "evaluation.record-draft/v0",
        "record_type": "request",
        "request_id": request_id,
        "logical_evaluation_id": logical_id,
        "attempt_number": attempt,
        "domain_contract": {"id": domain_id, "version": "v0"},
        "payload": {"opaque": True},
        "producer": {"id": "synthetic", "version": "v0"},
    }


def result(
    result_id,
    request_id,
    logical_id,
    attempt,
    outcome,
    payload=None,
    domain_id="opaque.domain",
):
    record = {
        "vocabulary_version": "evaluation.record-draft/v0",
        "record_type": "result",
        "result_id": result_id,
        "request_id": request_id,
        "logical_evaluation_id": logical_id,
        "attempt_number": attempt,
        "domain_contract": {"id": domain_id, "version": "v0"},
        "outcome": outcome,
        "producer": {"id": "synthetic", "version": "v0"},
        "uncertainty": {"status": "not_reported"},
    }
    if payload is not None:
        record["payload"] = payload
    return record


def transition(transition_id, expected, decisions):
    return {
        "contract_version": "campaign.assimilation/v0",
        "record_type": "assimilation_transition",
        "transition_id": transition_id,
        "campaign_id": "campaign:synthetic",
        "iteration_id": "iteration:7",
        "expected_campaign_revision": expected,
        "committed_campaign_revision": expected + 1,
        "controller_policy": {"id": "policy:synthetic", "version": "v0"},
        "decisions": decisions,
    }


def decision(request_id, logical_id, result_id, attempt, disposition):
    return {
        "request_id": request_id,
        "logical_evaluation_id": logical_id,
        "result_id": result_id,
        "attempt_number": attempt,
        "disposition": disposition,
    }


def exact_batches(candidate, request_records, result_records):
    request_ids = {item["request_id"] for item in candidate["decisions"]}
    result_ids = {item["result_id"] for item in candidate["decisions"]}
    requests = [item for item in request_records if item["request_id"] in request_ids]
    results = [item for item in result_records if item["result_id"] in result_ids]
    return requests, results


def apply_exact(ledger, candidate, request_records, result_records):
    requests, results = exact_batches(candidate, request_records, result_records)
    return ledger.apply(candidate, requests, results)


class AssimilationContractTests(unittest.TestCase):
    def setUp(self):
        # Domain-shaped payloads remain opaque to the reducer.
        self.requests = [
            request("request:rift", "logical:gw", 1, "gw.domain"),
            request("request:rift-2", "logical:gw", 2, "gw.domain"),
            request("request:r3", "logical:transport", 1, "transport.domain"),
            request("request:failed", "logical:retry", 1),
            request("request:retry", "logical:retry", 2),
        ]
        self.results = [
            result(
                "result:rift",
                "request:rift",
                "logical:gw",
                1,
                "complete",
                {"lnL": -4.5},
                "gw.domain",
            ),
            result(
                "result:rift-2",
                "request:rift-2",
                "logical:gw",
                2,
                "complete",
                {"lnL": -4.0},
                "gw.domain",
            ),
            result(
                "result:r3",
                "request:r3",
                "logical:transport",
                1,
                "partial",
                {"band_luminosity": [1.0, 2.0], "fidelity": "coarse"},
                "transport.domain",
            ),
            result("result:failed", "request:failed", "logical:retry", 1, "failed"),
            result(
                "result:retry",
                "request:retry",
                "logical:retry",
                2,
                "complete",
                {"objective": 3.0},
            ),
        ]

    def test_closed_schema_declares_bounded_orderless_transition(self):
        schema = json.loads((CONTRACT / "transition.schema.json").read_text())
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["properties"]["decisions"]["minItems"], 1)
        self.assertEqual(schema["properties"]["decisions"]["maxItems"], 10000)
        self.assertFalse(schema["$defs"]["decision"]["additionalProperties"])
        self.assertEqual(
            set(schema["required"]),
            {
                "contract_version",
                "record_type",
                "transition_id",
                "campaign_id",
                "iteration_id",
                "expected_campaign_revision",
                "committed_campaign_revision",
                "controller_policy",
                "decisions",
            },
        )
        self.assertIn("request_id", schema["$defs"]["decision"]["required"])
        self.assertFalse(schema["$defs"]["controller_policy"]["additionalProperties"])
        readme = (CONTRACT / "README.md").read_text()
        self.assertIn("Decision array order has no", readme)
        self.assertIn("semantic meaning", readme)
        self.assertIn("Receipts do not", readme)
        self.assertIn("archive evaluation payloads", readme)

    @unittest.skipIf(Draft202012Validator is None, "jsonschema is not installed")
    def test_real_draft202012_instances_match_schema(self):
        transition_schema = json.loads(
            (CONTRACT / "transition.schema.json").read_text(encoding="utf-8")
        )
        evaluation_schema = json.loads(
            (
                ROOT
                / "vision"
                / "contracts"
                / "evaluation-record-draft-v0"
                / "envelope.schema.json"
            ).read_text(encoding="utf-8")
        )
        Draft202012Validator.check_schema(transition_schema)
        Draft202012Validator.check_schema(evaluation_schema)
        transition_validator = Draft202012Validator(transition_schema)
        evaluation_validator = Draft202012Validator(evaluation_schema)
        candidate = transition("transition:schema", 0, [
            decision("request:rift", "logical:gw", "result:rift", 1, "assimilated")
        ])
        request_records, result_records = exact_batches(
            candidate, self.requests, self.results
        )
        self.assertEqual(list(transition_validator.iter_errors(candidate)), [])
        for record in request_records + result_records:
            self.assertEqual(list(evaluation_validator.iter_errors(record)), [])

        missing_payload = copy.deepcopy(result_records[0])
        del missing_payload["payload"]
        self.assertTrue(list(evaluation_validator.iter_errors(missing_payload)))
        failed_payload = copy.deepcopy(result_records[0])
        failed_payload["outcome"] = "failed"
        self.assertTrue(list(evaluation_validator.iter_errors(failed_payload)))

    def test_atomic_cas_accepts_opaque_rift_and_r3_results(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        choices = [
            decision("request:r3", "logical:transport", "result:r3", 1, "assimilated"),
            decision("request:rift", "logical:gw", "result:rift", 1, "assimilated"),
        ]
        receipt, changed = apply_exact(
            ledger,
            transition("transition:1", 0, choices), self.requests, self.results
        )
        self.assertTrue(changed)
        self.assertEqual(ledger.revision, 1)
        self.assertEqual(set(ledger.assimilated), {"logical:gw", "logical:transport"})
        self.assertEqual(receipt["transition"]["decisions"], sorted(
            choices, key=lambda item: (
                item["logical_evaluation_id"], item["request_id"], item["result_id"],
                item["attempt_number"], item["disposition"]
            )
        ))

    def test_semantic_replay_is_noop_and_id_reuse_conflicts(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        choices = [
            decision("request:rift", "logical:gw", "result:rift", 1, "assimilated"),
            decision("request:r3", "logical:transport", "result:r3", 1, "deferred"),
        ]
        original, changed = apply_exact(
            ledger,
            transition("transition:1", 0, choices), self.requests, self.results
        )
        self.assertTrue(changed)
        replay = transition("transition:1", 0, list(reversed(choices)))
        repeated, changed = apply_exact(ledger, replay, self.requests, self.results)
        self.assertFalse(changed)
        self.assertEqual(repeated, original)
        self.assertEqual(ledger.revision, 1)

        # IDs are external: replay validates shape and correlation, not payload
        # equivalence. The receipt remains the originally committed receipt.
        replay_results = copy.deepcopy(self.results)
        replay_results[0]["payload"] = {"lnL": 999.0}
        content_changed, changed = apply_exact(
            ledger, replay, self.requests, replay_results
        )
        self.assertFalse(changed)
        self.assertEqual(content_changed, original)

        invalid_replay_results = copy.deepcopy(self.results)
        invalid_replay_results[0]["request_id"] = "request:other"
        before = ledger.snapshot()
        with self.assertRaises(assimilation.ValidationError):
            apply_exact(ledger, replay, self.requests, invalid_replay_results)
        self.assertEqual(ledger.snapshot(), before)

        conflict = transition("transition:1", 0, [choices[0]])
        before = ledger.snapshot()
        with self.assertRaises(assimilation.TransitionIdConflict):
            apply_exact(ledger, conflict, self.requests, self.results)
        self.assertEqual(ledger.snapshot(), before)

    def test_failed_transition_changes_nothing(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        good = decision("request:rift", "logical:gw", "result:rift", 1, "assimilated")
        bad = decision("request:failed", "logical:retry", "result:failed", 1, "assimilated")
        before = ledger.snapshot()
        with self.assertRaises(assimilation.ValidationError):
            apply_exact(
                ledger,
                transition("transition:bad", 0, [good, bad]),
                self.requests,
                self.results,
            )
        self.assertEqual(ledger.snapshot(), before)
        self.assertEqual(ledger.receipts, [])

        with self.assertRaises(assimilation.RevisionConflict):
            apply_exact(
                ledger,
                transition("transition:stale", 2, [good]),
                self.requests,
                self.results,
            )
        self.assertEqual(ledger.snapshot(), before)
        self.assertEqual(ledger.receipts, [])

    def test_closed_transition_policy_and_record_identity_are_enforced(self):
        choice = decision(
            "request:rift", "logical:gw", "result:rift", 1, "assimilated"
        )
        mutations = []
        extra = transition("transition:extra", 0, [choice])
        extra["unknown"] = True
        mutations.append(extra)
        wrong_type = transition("transition:type", 0, [choice])
        wrong_type["record_type"] = "receipt"
        mutations.append(wrong_type)
        bad_iteration = transition("transition:iteration", 0, [choice])
        bad_iteration["iteration_id"] = ""
        mutations.append(bad_iteration)
        open_policy = transition("transition:policy", 0, [choice])
        open_policy["controller_policy"]["backend"] = "native"
        mutations.append(open_policy)

        for candidate in mutations:
            with self.subTest(transition_id=candidate["transition_id"]):
                ledger = assimilation.Ledger("campaign:synthetic")
                before = ledger.snapshot()
                with self.assertRaises(assimilation.ValidationError):
                    apply_exact(ledger, candidate, self.requests, self.results)
                self.assertEqual(ledger.snapshot(), before)
                self.assertEqual(ledger.receipts, [])

    def test_request_result_domain_correlation_and_record_closure_are_atomic(self):
        choice = decision(
            "request:rift", "logical:gw", "result:rift", 1, "assimilated"
        )
        cases = []

        request_mismatch = copy.deepcopy(self.requests)
        request_mismatch[0]["logical_evaluation_id"] = "logical:other"
        cases.append(("request correlation", request_mismatch, self.results))

        domain_mismatch = copy.deepcopy(self.results)
        domain_mismatch[0]["domain_contract"]["version"] = "v1"
        cases.append(("domain correlation", self.requests, domain_mismatch))

        open_request = copy.deepcopy(self.requests)
        open_request[0]["native_path"] = "forbidden"
        cases.append(("unknown request field", open_request, self.results))

        open_result = copy.deepcopy(self.results)
        open_result[0]["scheduler_id"] = "forbidden"
        cases.append(("unknown result field", self.requests, open_result))

        malformed_result = copy.deepcopy(self.results)
        del malformed_result[0]["outcome"]
        cases.append(("malformed result", self.requests, malformed_result))

        for label, request_records, result_records in cases:
            with self.subTest(case=label):
                ledger = assimilation.Ledger("campaign:synthetic")
                before = ledger.snapshot()
                with self.assertRaises(assimilation.ValidationError):
                    apply_exact(
                        ledger,
                        transition("transition:invalid", 0, [choice]),
                        request_records,
                        result_records,
                    )
                self.assertEqual(ledger.snapshot(), before)
                self.assertEqual(ledger.receipts, [])

    def test_evaluation_record_rules_match_the_draft_schema_subset(self):
        choice = decision(
            "request:rift", "logical:gw", "result:rift", 1, "assimilated"
        )
        candidate = transition("transition:invalid-record", 0, [choice])
        base_requests, base_results = exact_batches(
            candidate, self.requests, self.results
        )
        cases = []

        missing_payload = copy.deepcopy(base_results)
        del missing_payload[0]["payload"]
        cases.append(("success missing payload", base_requests, missing_payload))

        failed_payload = [
            result(
                "result:rift", "request:rift", "logical:gw", 1, "failed", {"bad": True},
                "gw.domain",
            )
        ]
        cases.append(("non-success with payload", base_requests, failed_payload))

        bad_request_producer = copy.deepcopy(base_requests)
        bad_request_producer[0]["producer"]["backend"] = "forbidden"
        cases.append(("request producer", bad_request_producer, base_results))

        bad_result_producer = copy.deepcopy(base_results)
        bad_result_producer[0]["producer"] = {"id": "synthetic"}
        cases.append(("result producer", base_requests, bad_result_producer))

        bad_uncertainty = copy.deepcopy(base_results)
        bad_uncertainty[0]["uncertainty"] = {"status": "reported"}
        cases.append(("uncertainty", base_requests, bad_uncertainty))

        bad_cost = copy.deepcopy(base_results)
        bad_cost[0]["cost"] = {}
        cases.append(("cost", base_requests, bad_cost))

        bad_diagnostics = copy.deepcopy(base_results)
        bad_diagnostics[0]["diagnostics"] = [
            {"category": "internal", "code": "bad", "native": True}
        ]
        cases.append(("diagnostics", base_requests, bad_diagnostics))

        bad_vocabulary = copy.deepcopy(base_results)
        bad_vocabulary[0]["vocabulary_version"] = "evaluation.record/v1"
        cases.append(("vocabulary", base_requests, bad_vocabulary))

        for label, request_records, result_records in cases:
            with self.subTest(case=label):
                ledger = assimilation.Ledger("campaign:synthetic")
                before = ledger.snapshot()
                with self.assertRaises(assimilation.ValidationError):
                    ledger.apply(candidate, request_records, result_records)
                self.assertEqual(ledger.snapshot(), before)
                self.assertEqual(ledger.receipts, [])

    def test_exact_batches_reject_unrelated_and_oversized_records(self):
        candidate = transition("transition:bounded", 0, [
            decision("request:rift", "logical:gw", "result:rift", 1, "assimilated")
        ])
        exact_requests, exact_results = exact_batches(
            candidate, self.requests, self.results
        )
        cases = (
            (self.requests, exact_results),
            (exact_requests, self.results),
            ([exact_requests[0]] * 10001, exact_results),
            (exact_requests, [exact_results[0]] * 10001),
        )
        for request_records, result_records in cases:
            ledger = assimilation.Ledger("campaign:synthetic")
            with self.assertRaises(assimilation.ValidationError):
                ledger.apply(candidate, request_records, result_records)
            self.assertEqual(ledger.snapshot()["campaign_revision"], 0)

    def test_overlong_supplied_ids_reject_without_state_change(self):
        candidate = transition("transition:bounded-id", 0, [
            decision("request:rift", "logical:gw", "result:rift", 1, "assimilated")
        ])
        exact_requests, exact_results = exact_batches(
            candidate, self.requests, self.results
        )
        overlong_request = copy.deepcopy(exact_requests)
        overlong_request[0]["request_id"] = "r" * 129
        overlong_result = copy.deepcopy(exact_results)
        overlong_result[0]["result_id"] = "r" * 129

        for request_records, result_records in (
            (overlong_request, exact_results),
            (exact_requests, overlong_result),
        ):
            ledger = assimilation.Ledger("campaign:synthetic")
            before = ledger.snapshot()
            with self.assertRaises(assimilation.ValidationError):
                ledger.apply(candidate, request_records, result_records)
            self.assertEqual(ledger.snapshot(), before)
            self.assertEqual(ledger.receipts, [])

    def test_deferred_or_rejected_attempt_can_be_followed_by_later_attempt(self):
        for initial_disposition in ("deferred", "rejected"):
            with self.subTest(initial_disposition=initial_disposition):
                ledger = assimilation.Ledger("campaign:synthetic")
                first = transition("transition:first", 0, [
                    decision("request:failed", "logical:retry", "result:failed", 1, initial_disposition)
                ])
                apply_exact(ledger, first, self.requests, self.results)
                second = transition("transition:second", 1, [
                    decision("request:retry", "logical:retry", "result:retry", 2, "assimilated")
                ])
                apply_exact(ledger, second, self.requests, self.results)
                self.assertEqual(ledger.assimilated["logical:retry"]["attempt_number"], 2)

    def test_one_assimilation_per_logical_id_and_strict_correlation(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        apply_exact(ledger, transition("transition:1", 0, [
            decision("request:rift", "logical:gw", "result:rift", 1, "assimilated")
        ]), self.requests, self.results)
        duplicate = transition("transition:2", 1, [
            decision("request:rift-2", "logical:gw", "result:rift-2", 2, "assimilated")
        ])
        before_duplicate = ledger.snapshot()
        with self.assertRaises(assimilation.AssimilationConflict):
            apply_exact(ledger, duplicate, self.requests, self.results)
        self.assertEqual(ledger.snapshot(), before_duplicate)

        mismatch = transition("transition:3", 1, [
            decision("request:r3", "logical:transport", "result:r3", 2, "deferred")
        ])
        with self.assertRaises(assimilation.ValidationError):
            apply_exact(ledger, mismatch, self.requests, self.results)
        self.assertEqual(ledger.revision, 1)

    def test_attempts_strictly_advance_for_every_disposition(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        later = transition("transition:later", 0, [
            decision("request:retry", "logical:retry", "result:retry", 2, "deferred")
        ])
        apply_exact(ledger, later, self.requests, self.results)
        before = ledger.snapshot()
        rollback = transition("transition:rollback", 1, [
            decision("request:failed", "logical:retry", "result:failed", 1, "rejected")
        ])
        with self.assertRaises(assimilation.AttemptConflict):
            apply_exact(ledger, rollback, self.requests, self.results)
        self.assertEqual(ledger.snapshot(), before)

        equal = transition("transition:equal", 1, [
            decision("request:retry", "logical:retry", "result:retry", 2, "rejected")
        ])
        with self.assertRaises(assimilation.AttemptConflict):
            apply_exact(ledger, equal, self.requests, self.results)
        self.assertEqual(ledger.snapshot(), before)

    def test_returned_values_and_properties_do_not_expose_state(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        candidate = transition("transition:immutable", 0, [
            decision("request:rift", "logical:gw", "result:rift", 1, "assimilated")
        ])
        receipt, _ = apply_exact(ledger, candidate, self.requests, self.results)
        receipt["transition"]["campaign_id"] = "campaign:mutated"
        snapshot = ledger.snapshot()
        snapshot["assimilated"].clear()
        snapshot["latest_attempts"].clear()
        exposed = ledger.assimilated
        exposed.clear()
        exposed_attempts = ledger.latest_attempts
        exposed_attempts.clear()
        self.assertEqual(ledger.campaign_id, "campaign:synthetic")
        self.assertEqual(ledger.revision, 1)
        self.assertIn("logical:gw", ledger.assimilated)
        self.assertEqual(ledger.latest_attempts["logical:gw"], 1)
        self.assertEqual(
            ledger.receipts[0]["transition"]["campaign_id"], "campaign:synthetic"
        )
        with self.assertRaises(AttributeError):
            ledger.revision = 99

    def test_two_threads_same_revision_commit_exactly_once(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        candidates = (
            transition("transition:thread-a", 0, [
                decision("request:rift", "logical:gw", "result:rift", 1, "assimilated")
            ]),
            transition("transition:thread-b", 0, [
                decision("request:r3", "logical:transport", "result:r3", 1, "assimilated")
            ]),
        )
        barrier = threading.Barrier(2)
        outcomes = []
        outcomes_lock = threading.Lock()

        def worker(candidate):
            barrier.wait()
            try:
                apply_exact(ledger, candidate, self.requests, self.results)
                outcome = "committed"
            except assimilation.RevisionConflict:
                outcome = "revision_conflict"
            with outcomes_lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=worker, args=(item,)) for item in candidates]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(sorted(outcomes), ["committed", "revision_conflict"])
        self.assertEqual(ledger.revision, 1)
        self.assertEqual(len(ledger.receipts), 1)

    def test_rebuild_from_receipts_recovers_identical_state(self):
        ledger = assimilation.Ledger("campaign:synthetic")
        first_transition = transition("transition:1", 0, [
            decision("request:failed", "logical:retry", "result:failed", 1, "deferred")
        ])
        first_receipt, _ = apply_exact(
            ledger, first_transition, self.requests, self.results
        )
        second_transition = transition("transition:2", 1, [
            decision("request:retry", "logical:retry", "result:retry", 2, "assimilated"),
            decision("request:r3", "logical:transport", "result:r3", 1, "assimilated"),
        ])
        second_receipt, _ = apply_exact(
            ledger, second_transition, self.requests, self.results
        )
        first_requests, first_results = exact_batches(
            first_transition, self.requests, self.results
        )
        second_requests, second_results = exact_batches(
            second_transition, self.requests, self.results
        )
        replay_items = (
            item for item in (
                {
                    "receipt": first_receipt,
                    "request_records": first_requests,
                    "result_records": first_results,
                },
                {
                    "receipt": second_receipt,
                    "request_records": second_requests,
                    "result_records": second_results,
                },
            )
        )
        rebuilt = assimilation.Ledger.rebuild(
            "campaign:synthetic", replay_items
        )
        self.assertEqual(rebuilt.snapshot(), ledger.snapshot())
        self.assertEqual(rebuilt.receipts, ledger.receipts)


if __name__ == "__main__":
    unittest.main()
