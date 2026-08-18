#!/usr/bin/env python3
"""Tests for the non-conformant proposal-boundary observation vocabulary."""

import copy
import json
import math
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]
FIXTURES = ROOT / "vision" / "contracts" / "proposal-boundary-observation-draft-v0"
SCHEMA_PATH = FIXTURES / "envelope.schema.json"
VALID_PATH = FIXTURES / "valid" / "synthetic-observation.json"
INVALID = FIXTURES / "invalid"


def reject_non_json_number(value):
    raise ValueError(f"non-JSON numeric constant: {value}")


def load(path):
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream, parse_constant=reject_non_json_number)


class ProposalBoundaryObservationStructureTests(unittest.TestCase):
    def test_files_are_bounded_synthetic_json(self):
        paths = [SCHEMA_PATH, VALID_PATH] + sorted(INVALID.glob("*.json"))
        self.assertEqual(len(paths), 13)
        forbidden = (
            "supernu",
            "mmapipe",
            "github.com",
            "pull/",
            "commit",
            "sha256",
            "/users/",
            "credential",
            "bearer",
            "token",
            "http://",
            "https://",
        )
        for path in paths:
            raw = path.read_text(encoding="utf-8")
            self.assertLess(len(raw.encode("utf-8")), 64 * 1024)
            if path != SCHEMA_PATH:
                lowered = raw.lower()
                for marker in forbidden:
                    self.assertNotIn(marker, lowered, f"{marker!r} leaked in {path}")
            load(path)

    def test_valid_record_has_only_the_reviewed_boundary(self):
        record = load(VALID_PATH)
        self.assertEqual(
            set(record),
            {
                "vocabulary_version",
                "record_type",
                "domain_contract",
                "input_population",
                "proposal_policy",
                "rng_declaration",
                "candidate_population",
            },
        )
        serialized = json.dumps(record, sort_keys=True).lower()
        for excluded in (
            "observation_id",
            "population_id",
            "campaign",
            "iteration",
            "request_id",
            "result_id",
            "outcome",
            "uncertainty",
            "checkpoint",
            "native_table",
            "payload",
        ):
            self.assertNotIn(excluded, serialized)

    def test_configuration_is_flat_and_finite(self):
        record = load(VALID_PATH)
        for field in ("proposal_policy", "rng_declaration"):
            configuration = record[field]["configuration"]
            self.assertLessEqual(len(configuration), 32)
            for value in configuration.values():
                self.assertIsInstance(value, (str, int, float, bool))
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    self.assertTrue(math.isfinite(value))

    def test_readme_keeps_the_high_level_nonclaims(self):
        text = (FIXTURES / "README.md").read_text(encoding="utf-8")
        for phrase in (
            "non-conformant vocabulary experiment",
            "standalone, intentionally non-correlatable observation",
            "does not carry either population",
            "request/result shape would invent those semantics",
            "no:",
            "portable RNG replay or scientific validity",
            "demonstrates schema shape only",
        ):
            self.assertIn(phrase, text)


try:
    from jsonschema import Draft202012Validator
except ImportError:  # pragma: no cover - optional test-only dependency
    Draft202012Validator = None


@unittest.skipIf(Draft202012Validator is None, "jsonschema is not installed")
class ProposalBoundaryObservationSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = load(SCHEMA_PATH)
        Draft202012Validator.check_schema(cls.schema)
        cls.validator = Draft202012Validator(cls.schema)

    def test_valid_fixture_validates(self):
        errors = list(self.validator.iter_errors(load(VALID_PATH)))
        self.assertEqual(errors, [])

    @staticmethod
    def _leaf_errors(error):
        if not error.context:
            yield error
        for child in error.context:
            yield from ProposalBoundaryObservationSchemaTests._leaf_errors(child)

    def test_invalid_fixtures_fail_for_intended_reason_and_repair(self):
        def remove(field):
            return lambda document: document.pop(field)

        def remove_input_population_id(document):
            document["input_population"].pop("population_id")

        def add_rng_contract(document):
            document["rng_declaration"]["contract"] = {
                "id": "synthetic.rng",
                "version": "draft-0",
            }

        def flatten_schedule(document):
            document["proposal_policy"]["configuration"]["schedule"] = 0.2

        def shorten_algorithm(document):
            document["rng_declaration"]["configuration"]["algorithm"] = "demo"

        def add_one_candidate(document):
            document["candidate_population"]["cardinality"] = 1

        cases = {
            "campaign-id.json": ((), "additionalProperties", "campaign_id", remove("campaign_id")),
            "iteration-id.json": ((), "additionalProperties", "iteration_id", remove("iteration_id")),
            "population-id.json": (
                ("input_population",),
                "additionalProperties",
                "population_id",
                remove_input_population_id,
            ),
            "native-table.json": ((), "additionalProperties", "native_table", remove("native_table")),
            "top-level-seed.json": ((), "additionalProperties", "seed", remove("seed")),
            "missing-rng-contract.json": (
                ("rng_declaration",),
                "required",
                "'contract' is a required property",
                add_rng_contract,
            ),
            "nested-policy-configuration.json": (
                ("proposal_policy", "configuration", "schedule"),
                "type",
                "is not of type 'string'",
                flatten_schedule,
            ),
            "oversized-rng-string.json": (
                ("rng_declaration", "configuration", "algorithm"),
                "maxLength",
                "is too long",
                shorten_algorithm,
            ),
            "zero-candidates.json": (
                ("candidate_population", "cardinality"),
                "minimum",
                "is less than the minimum",
                add_one_candidate,
            ),
            "outcome.json": ((), "additionalProperties", "outcome", remove("outcome")),
            "checkpoint.json": ((), "additionalProperties", "checkpoint", remove("checkpoint")),
        }

        self.assertEqual({path.name for path in INVALID.glob("*.json")}, set(cases))
        for name, (path, validator, message, repair) in cases.items():
            with self.subTest(name=name):
                document = load(INVALID / name)
                leaves = [
                    leaf
                    for error in self.validator.iter_errors(document)
                    for leaf in self._leaf_errors(error)
                ]
                intended = [
                    leaf
                    for leaf in leaves
                    if tuple(leaf.absolute_path) == path
                    and leaf.validator == validator
                    and message in leaf.message
                ]
                self.assertTrue(intended, f"{name} failed for the wrong reason: {leaves}")

                repaired = copy.deepcopy(document)
                repair(repaired)
                repaired_errors = list(self.validator.iter_errors(repaired))
                self.assertEqual(repaired_errors, [], f"{name} repair failed: {repaired_errors}")

    def test_non_json_and_oversized_configuration_are_rejected(self):
        record = load(VALID_PATH)
        cases = []

        too_many = copy.deepcopy(record)
        too_many["proposal_policy"]["configuration"] = {
            f"setting_{index}": index for index in range(33)
        }
        cases.append(too_many)

        nested = copy.deepcopy(record)
        nested["rng_declaration"]["configuration"]["seed"] = [1729]
        cases.append(nested)

        null_value = copy.deepcopy(record)
        null_value["rng_declaration"]["configuration"]["seed"] = None
        cases.append(null_value)

        infinity = copy.deepcopy(record)
        infinity["rng_declaration"]["configuration"]["seed"] = math.inf
        cases.append(infinity)

        too_large = copy.deepcopy(record)
        too_large["rng_declaration"]["configuration"]["seed"] = 10**16
        cases.append(too_large)

        for document in cases:
            with self.subTest(document=document):
                self.assertTrue(list(self.validator.iter_errors(document)))


if __name__ == "__main__":
    unittest.main()
