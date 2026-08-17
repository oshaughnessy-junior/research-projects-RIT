#!/usr/bin/env python3
"""Tests for the non-conformant evaluation record vocabulary."""

import copy
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]
FIXTURES = ROOT / "vision" / "contracts" / "evaluation-record-draft-v0"
SCHEMA_PATH = FIXTURES / "envelope.schema.json"
VALID = FIXTURES / "valid"
INVALID = FIXTURES / "invalid"


def load(path):
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


class EvaluationRecordDraftStructureTests(unittest.TestCase):
    def test_fixtures_are_bounded_synthetic_json(self):
        paths = [SCHEMA_PATH] + sorted(VALID.glob("*.json")) + sorted(INVALID.glob("*.json"))
        self.assertEqual(len(paths), 7)
        forbidden = ("rift", "supernu", "ligo", "credential", "bearer", "token", "/users/")
        for path in paths:
            raw = path.read_text(encoding="utf-8")
            self.assertLess(len(raw.encode("utf-8")), 64 * 1024)
            if path != SCHEMA_PATH:
                lowered = raw.lower()
                for marker in forbidden:
                    self.assertNotIn(marker, lowered)
            load(path)

    def test_valid_result_correlation_is_only_structural(self):
        request = load(VALID / "request.json")
        complete = load(VALID / "result-complete.json")
        indeterminate = load(VALID / "result-indeterminate.json")
        for result in (complete, indeterminate):
            self.assertEqual(result["request_id"], request["request_id"])
            self.assertEqual(result["logical_evaluation_id"], request["logical_evaluation_id"])
            self.assertEqual(result["attempt_number"], request["attempt_number"])
            self.assertEqual(result["domain_contract"], request["domain_contract"])
        self.assertNotIn("payload", indeterminate)

    def test_readme_disclaims_operational_and_scientific_conformance(self):
        text = (FIXTURES / "README.md").read_text(encoding="utf-8")
        for phrase in (
            "non-conformant vocabulary experiment",
            "supplies no cross-domain evidence",
            "does not define parameter names",
            "execute RIFT, SuperNu, HyperPipe",
            "next adapter phase requires separate authorization",
        ):
            self.assertIn(phrase, text)


try:
    from jsonschema import Draft202012Validator
except ImportError:  # pragma: no cover - optional test-only dependency
    Draft202012Validator = None


@unittest.skipIf(Draft202012Validator is None, "jsonschema is not installed")
class EvaluationRecordDraftSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = load(SCHEMA_PATH)
        Draft202012Validator.check_schema(cls.schema)
        cls.validator = Draft202012Validator(cls.schema)

    def test_valid_fixtures_validate(self):
        for path in sorted(VALID.glob("*.json")):
            errors = list(self.validator.iter_errors(load(path)))
            self.assertEqual(errors, [], f"{path.name}: {errors}")

    @staticmethod
    def _leaf_errors(error):
        if not error.context:
            yield error
        for child in error.context:
            yield from EvaluationRecordDraftSchemaTests._leaf_errors(child)

    def test_invalid_fixtures_fail_for_intended_reason_and_repair(self):
        def add_domain_version(document):
            document["domain_contract"]["version"] = "draft-0"

        def add_complete_payload(document):
            document["payload"] = {"objective": -1.25}

        def remove_failed_payload(document):
            del document["payload"]

        cases = {
            "request-missing-domain-version.json": {
                "path": ("domain_contract",),
                "validator": "required",
                "message": "'version' is a required property",
                "repair": add_domain_version,
            },
            "result-complete-missing-payload.json": {
                "path": (),
                "validator": "required",
                "message": "'payload' is a required property",
                "repair": add_complete_payload,
            },
            "result-failed-with-payload.json": {
                "path": (),
                "validator": "not",
                "message": "should not be valid under",
                "repair": remove_failed_payload,
            },
        }

        self.assertEqual({path.name for path in INVALID.glob("*.json")}, set(cases))
        for name, expected in cases.items():
            document = load(INVALID / name)
            leaves = [
                leaf
                for error in self.validator.iter_errors(document)
                for leaf in self._leaf_errors(error)
            ]
            intended = [
                leaf
                for leaf in leaves
                if tuple(leaf.absolute_path) == expected["path"]
                and leaf.validator == expected["validator"]
                and expected["message"] in leaf.message
            ]
            self.assertTrue(intended, f"{name} did not fail for its intended reason: {leaves}")

            repaired = copy.deepcopy(document)
            expected["repair"](repaired)
            repaired_errors = list(self.validator.iter_errors(repaired))
            self.assertEqual(repaired_errors, [], f"{name} repair failed: {repaired_errors}")


if __name__ == "__main__":
    unittest.main()
