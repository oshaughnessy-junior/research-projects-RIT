#!/usr/bin/env python3
"""Focused checks for the RIFT-only record-batch projection proof."""

from __future__ import annotations

import copy
import importlib.util
import json
import math
from collections.abc import Mapping
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[4]
SIDECAR_PROOF = ROOT / "vision" / "proofs" / "rift-marg-record-sidecar-v0"
PROJECTION_PROOF = ROOT / "vision" / "proofs" / "rift-record-batch-projection-v0"
TRACE_FIXTURE = (
    ROOT
    / "vision"
    / "proofs"
    / "hyperpipe-proposal-boundary-trace-v0"
    / "fixtures"
    / "evaluated-grid.dat"
)
SCHEMA = ROOT / "vision" / "contracts" / "evaluation-record-draft-v0" / "envelope.schema.json"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sidecar = _load("rift_marg_record_sidecar_for_projection_v0", SIDECAR_PROOF / "sidecar.py")
projector = _load("rift_record_batch_projection_v0", PROJECTION_PROOF / "projector.py")


class _OversizedMapping(Mapping):
    """Prove size rejection happens before key materialization."""

    def __getitem__(self, key):
        raise AssertionError("oversized mapping must not be read")

    def __iter__(self):
        raise AssertionError("oversized mapping must not be iterated")

    def __len__(self):
        return projector.MAX_PARAMETERS + 1


def _build_pairs():
    lines = TRACE_FIXTURE.read_text(encoding="ascii").splitlines()
    header = lines[0]
    pairs = []
    for index, line in enumerate(lines[1:]):
        ln_l, sigma_ln_l, x, y = line.split()
        request_text = f"{header}\n0.0 0.0 {x} {y}\n"
        result_text = f"{header}\n{ln_l} {sigma_ln_l} {x} {y}\n"
        context = sidecar.RecordContext(
            request_id=f"request-{index:04d}",
            result_id=f"result-{index:04d}",
            logical_evaluation_id=f"logical-{index:04d}",
            attempt_number=1,
            domain_contract_id="rift.hyperpipe.marginal-log-likelihood-draft",
            domain_contract_version="v0",
            producer_id="rift.hyperpipe.marg-record-sidecar",
            producer_version="draft-v0",
            native_reference=f"synthetic-row-{index:04d}",
            uncertainty_schema_id="rift.hyperpipe.sigma-lnL-draft-v0",
        )
        pairs.append(sidecar.adapt_pair(request_text, result_text, context))
    return pairs


class RIFTRecordBatchProjectionTest(unittest.TestCase):
    def setUp(self):
        self.pairs = _build_pairs()

    def test_projects_25_pairs_to_exact_reviewed_fixture_without_mutation(self):
        before = copy.deepcopy(self.pairs)
        output = projector.project_batch(self.pairs, ("x", "y"))
        self.assertEqual(output.encode("ascii"), TRACE_FIXTURE.read_bytes())
        self.assertEqual(self.pairs, before)
        lines = output.splitlines()
        self.assertEqual(lines[0], "# lnL sigma_lnL x y")
        self.assertEqual(len(lines[1:]), 25)
        for line in lines[1:]:
            values = [float(token) for token in line.split()]
            self.assertEqual(len(values), 4)
            self.assertTrue(all(math.isfinite(value) for value in values))

    def test_explicit_pair_and_parameter_order_are_preserved(self):
        reversed_pairs = list(reversed(self.pairs))
        output = projector.project_batch(reversed_pairs, ("y", "x"))
        lines = output.splitlines()
        self.assertEqual(lines[0], "# lnL sigma_lnL y x")
        original_last = [float(value) for value in TRACE_FIXTURE.read_text().splitlines()[-1].split()]
        projected_first = [float(value) for value in lines[1].split()]
        self.assertEqual(projected_first, [original_last[0], original_last[1], original_last[3], original_last[2]])

    def test_records_validate_and_output_leaks_no_metadata(self):
        try:
            import jsonschema
        except ImportError:
            jsonschema = None
        if jsonschema is not None:
            schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
            validator = jsonschema.Draft202012Validator(schema)
            for pair in self.pairs:
                for record in pair:
                    self.assertEqual(list(validator.iter_errors(record)), [])
        output = projector.project_batch(self.pairs, ("x", "y"))
        for forbidden in (
            "request-", "result-", "logical-", "synthetic-row-", "producer",
            "native_reference", "/Users/", "/private/", "token=", "credential",
        ):
            self.assertNotIn(forbidden, output)

    def _assert_invalid(self, expected: str, pairs=None, order=("x", "y")):
        with self.assertRaisesRegex(projector.ProjectionError, expected):
            projector.project_batch(self.pairs if pairs is None else pairs, order)

    def test_rejects_cases_requiring_selection_or_record_policy(self):
        cases = []

        unsupported = copy.deepcopy(self.pairs[:1])
        unsupported[0][0]["vocabulary_version"] = "evaluation.record/v1"
        cases.append(("unsupported vocabulary", unsupported))

        for outcome in ("partial", "failed", "indeterminate", "unsupported"):
            incomplete = copy.deepcopy(self.pairs[:1])
            incomplete[0][1]["outcome"] = outcome
            cases.append(("outcome must be complete", incomplete))

        mismatch = copy.deepcopy(self.pairs[:1])
        mismatch[0][1]["request_id"] = "another-request"
        cases.append(("mismatched request_id", mismatch))

        logical_mismatch = copy.deepcopy(self.pairs[:1])
        logical_mismatch[0][1]["logical_evaluation_id"] = "another-logical"
        cases.append(("mismatched logical_evaluation_id", logical_mismatch))

        attempt_mismatch = copy.deepcopy(self.pairs[:1])
        attempt_mismatch[0][1]["attempt_number"] = 2
        cases.append(("mismatched attempt_number", attempt_mismatch))

        producer_mismatch = copy.deepcopy(self.pairs[:1])
        producer_mismatch[0][1]["producer"]["native_reference"] = "another-row"
        cases.append(("mismatched producer", producer_mismatch))

        mixed_domain = copy.deepcopy(self.pairs[:1])
        mixed_domain[0][1]["domain_contract"]["id"] = "other.domain"
        cases.append(("unsupported domain contract", mixed_domain))

        unsupported_producer = copy.deepcopy(self.pairs[:1])
        unsupported_producer[0][0]["producer"]["id"] = "other.producer"
        cases.append(("unsupported producer", unsupported_producer))

        duplicate_request = copy.deepcopy(self.pairs[:2])
        duplicate_request[1][0]["request_id"] = duplicate_request[0][0]["request_id"]
        duplicate_request[1][1]["request_id"] = duplicate_request[0][0]["request_id"]
        cases.append(("duplicate request_id", duplicate_request))

        duplicate_result = copy.deepcopy(self.pairs[:2])
        duplicate_result[1][1]["result_id"] = duplicate_result[0][1]["result_id"]
        cases.append(("duplicate result_id", duplicate_result))

        multiple_attempts = copy.deepcopy(self.pairs[:2])
        multiple_attempts[1][0]["logical_evaluation_id"] = multiple_attempts[0][0]["logical_evaluation_id"]
        multiple_attempts[1][1]["logical_evaluation_id"] = multiple_attempts[0][0]["logical_evaluation_id"]
        multiple_attempts[1][0]["attempt_number"] = 2
        multiple_attempts[1][1]["attempt_number"] = 2
        cases.append(("duplicate logical_evaluation_id", multiple_attempts))

        for expected, pairs in cases:
            with self.subTest(expected=expected):
                self._assert_invalid(expected, pairs)

    def test_rejects_malformed_scientific_values_and_parameter_sets(self):
        cases = []

        missing_parameter = copy.deepcopy(self.pairs[:1])
        del missing_parameter[0][0]["payload"]["parameters"]["y"]
        cases.append(("parameters differ", missing_parameter))

        extra_parameter = copy.deepcopy(self.pairs[:1])
        extra_parameter[0][0]["payload"]["parameters"]["z"] = 0.0
        cases.append(("parameters differ", extra_parameter))

        boolean_parameter = copy.deepcopy(self.pairs[:1])
        boolean_parameter[0][0]["payload"]["parameters"]["x"] = True
        cases.append(("numeric and not boolean", boolean_parameter))

        nonfinite_parameter = copy.deepcopy(self.pairs[:1])
        nonfinite_parameter[0][0]["payload"]["parameters"]["x"] = float("inf")
        cases.append(("parameter x must be finite", nonfinite_parameter))

        nonfinite_likelihood = copy.deepcopy(self.pairs[:1])
        nonfinite_likelihood[0][1]["payload"]["log_likelihood"] = float("nan")
        cases.append(("log_likelihood must be finite", nonfinite_likelihood))

        overflowing_likelihood = copy.deepcopy(self.pairs[:1])
        overflowing_likelihood[0][1]["payload"]["log_likelihood"] = 10**10000
        cases.append(("finite representable number", overflowing_likelihood))

        missing_likelihood = copy.deepcopy(self.pairs[:1])
        del missing_likelihood[0][1]["payload"]["log_likelihood"]
        cases.append(("result payload fields", missing_likelihood))

        unreported = copy.deepcopy(self.pairs[:1])
        unreported[0][1]["uncertainty"]["status"] = "not_reported"
        cases.append(("uncertainty must be reported", unreported))

        wrong_uncertainty = copy.deepcopy(self.pairs[:1])
        wrong_uncertainty[0][1]["uncertainty"]["schema_id"] = "other.schema"
        cases.append(("uncertainty schema", wrong_uncertainty))

        negative_uncertainty = copy.deepcopy(self.pairs[:1])
        negative_uncertainty[0][1]["uncertainty"]["value"]["sigma_lnL"] = -0.1
        cases.append(("sigma_lnL must be nonnegative", negative_uncertainty))

        boolean_uncertainty = copy.deepcopy(self.pairs[:1])
        boolean_uncertainty[0][1]["uncertainty"]["value"]["sigma_lnL"] = True
        cases.append(("numeric and not boolean", boolean_uncertainty))

        nonfinite_uncertainty = copy.deepcopy(self.pairs[:1])
        nonfinite_uncertainty[0][1]["uncertainty"]["value"]["sigma_lnL"] = float("inf")
        cases.append(("sigma_lnL must be finite", nonfinite_uncertainty))

        missing_uncertainty_value = copy.deepcopy(self.pairs[:1])
        del missing_uncertainty_value[0][1]["uncertainty"]["value"]
        cases.append(("uncertainty fields", missing_uncertainty_value))

        for expected, pairs in cases:
            with self.subTest(expected=expected):
                self._assert_invalid(expected, pairs)

    def test_rejects_unbounded_or_ambiguous_batch_and_order(self):
        self._assert_invalid("batch size", [])
        oversized = [self.pairs[0] for _ in range(projector.MAX_BATCH + 1)]
        self._assert_invalid("batch size", oversized)
        self._assert_invalid(
            "unsupported size", order=range(projector.MAX_PARAMETERS + 1)
        )
        self._assert_invalid("duplicates", order=("x", "x"))
        self._assert_invalid("invalid native column", order=("lnL", "x"))
        self._assert_invalid("invalid native column", order=([], "x"))
        unexpected_metadata = copy.deepcopy(self.pairs[:1])
        unexpected_metadata[0][1]["cost"] = {"wall_time_s": 1.0}
        self._assert_invalid("result fields", unexpected_metadata)
        oversized_parameters = copy.deepcopy(self.pairs[:1])
        oversized_parameters[0][0]["payload"]["parameters"] = _OversizedMapping()
        self._assert_invalid("parameters differ", oversized_parameters)


if __name__ == "__main__":
    unittest.main()
