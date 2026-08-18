#!/usr/bin/env python3
"""Focused checks for the RIFT-only MargDriver record sidecar proof."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[4]
PROOF = ROOT / "vision" / "proofs" / "rift-marg-record-sidecar-v0"
SCHEMA = ROOT / "vision" / "contracts" / "evaluation-record-draft-v0" / "envelope.schema.json"


def _load_sidecar():
    spec = importlib.util.spec_from_file_location("rift_marg_record_sidecar_v0", PROOF / "sidecar.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sidecar = _load_sidecar()


class RIFTMargRecordSidecarTest(unittest.TestCase):
    def setUp(self):
        self.input_path = PROOF / "fixtures" / "input-grid.dat"
        self.output_path = PROOF / "fixtures" / "output-marg-integral+annotation.dat"
        self.input_bytes = self.input_path.read_bytes()
        self.output_bytes = self.output_path.read_bytes()
        self.context = sidecar.RecordContext(
            request_id="request-0001",
            result_id="result-0001",
            logical_evaluation_id="logical-0001",
            attempt_number=1,
            domain_contract_id="rift.hyperpipe.marginal-log-likelihood-draft",
            domain_contract_version="v0",
            producer_id="rift.hyperpipe.marg-record-sidecar",
            producer_version="draft-v0",
            native_reference="row-0000",
            uncertainty_schema_id="rift.hyperpipe.sigma-lnL-draft-v0",
        )

    def adapt(self):
        return sidecar.adapt_pair(
            self.input_bytes.decode("utf-8"),
            self.output_bytes.decode("utf-8"),
            self.context,
        )

    def test_maps_one_row_without_mutating_native_fixtures(self):
        request, result = self.adapt()
        self.assertEqual(request["payload"], {"parameters": {"mc": 1.21, "eta": 0.24}})
        self.assertEqual(result["payload"], {"log_likelihood": -12.345})
        self.assertEqual(result["uncertainty"]["value"], {"sigma_lnL": 0.031})
        self.assertNotIn("cost", result)
        self.assertEqual(self.input_path.read_bytes(), self.input_bytes)
        self.assertEqual(self.output_path.read_bytes(), self.output_bytes)

    def test_context_is_copied_explicitly(self):
        request, result = self.adapt()
        for field in ("request_id", "logical_evaluation_id", "attempt_number", "domain_contract"):
            self.assertEqual(result[field], request[field])
        self.assertEqual(request["producer"], result["producer"])
        self.assertIsNot(request["producer"], result["producer"])
        self.assertIsNot(request["domain_contract"], result["domain_contract"])
        rendered = json.dumps([request, result], sort_keys=True)
        for forbidden in ("/private/", "/Users/", "token=", "credential", "environment"):
            self.assertNotIn(forbidden, rendered)

    def test_records_validate_when_test_validator_is_available(self):
        try:
            import jsonschema
        except ImportError:
            self.skipTest("jsonschema is an optional test-only dependency")
        schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator.check_schema(schema)
        validator = jsonschema.Draft202012Validator(schema)
        for record in self.adapt():
            self.assertEqual(list(validator.iter_errors(record)), [])

    def test_rejects_malformed_or_scientifically_ambiguous_rows(self):
        bad_rows = {
            "multi-row": "# lnL sigma_lnL x\n0 0 1\n1 0 2\n",
            "wrong-leading-columns": "# sigma_lnL lnL x\n0 0 1\n",
            "duplicate-columns": "# lnL sigma_lnL x x\n0 0 1 1\n",
            "non-finite-result": "# lnL sigma_lnL x\nnan 0 1\n",
            "negative-uncertainty": "# lnL sigma_lnL x\n0 -1 1\n",
        }
        good = "# lnL sigma_lnL x\n0 0 1\n"
        for name, bad in bad_rows.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                sidecar.adapt_pair(good, bad, self.context)

    def test_rejects_parameter_mismatch_and_inferred_identity_shortcuts(self):
        with self.assertRaisesRegex(ValueError, "parameter"):
            sidecar.adapt_pair(
                "# lnL sigma_lnL x\n0 0 1\n",
                "# lnL sigma_lnL x\n-1 0.1 2\n",
                self.context,
            )
        invalid_context = sidecar.RecordContext(
            **{**self.context.__dict__, "native_reference": "/tmp/native-row"}
        )
        with self.assertRaisesRegex(ValueError, "native_reference"):
            sidecar.adapt_pair(
                "# lnL sigma_lnL x\n0 0 1\n",
                "# lnL sigma_lnL x\n-1 0.1 1\n",
                invalid_context,
            )
        non_integer_attempt = sidecar.RecordContext(
            **{**self.context.__dict__, "attempt_number": 1.5}
        )
        with self.assertRaisesRegex(ValueError, "attempt_number"):
            sidecar.adapt_pair(
                "# lnL sigma_lnL x\n0 0 1\n",
                "# lnL sigma_lnL x\n-1 0.1 1\n",
                non_integer_attempt,
            )


if __name__ == "__main__":
    unittest.main()
