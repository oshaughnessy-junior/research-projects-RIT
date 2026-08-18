#!/usr/bin/env python3
"""Focused checks for the observation-only HyperPipe proposal trace."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[4]
PROOF = ROOT / "vision" / "proofs" / "hyperpipe-proposal-boundary-trace-v0"


def _load_reproducer():
    spec = importlib.util.spec_from_file_location(
        "hyperpipe_proposal_boundary_trace_v0", PROOF / "reproduce.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


trace = _load_reproducer()


class HyperPipeProposalBoundaryTraceTest(unittest.TestCase):
    def test_runtime_trace_is_locally_deterministic_and_structurally_safe(self):
        try:
            observed = trace.reproduce_twice()
        except trace.TraceError as exc:
            if "import preflight failed" in str(exc):
                self.skipTest(str(exc))
            raise
        trace.validate_output_structure(observed)

    def test_manifest_binds_inputs_command_and_reviewed_output(self):
        trace.verify_manifest()
        manifest = json.loads(trace.MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema"], "rift.hyperpipe.proposal-boundary-trace/v0")
        self.assertEqual(manifest["input"]["row_count"], 25)
        self.assertEqual(manifest["input"]["header"], "# lnL sigma_lnL x y")
        self.assertEqual(manifest["command"]["seed"], 1729)
        self.assertEqual(manifest["command"]["method"], "smc-mala-bd")
        self.assertEqual(manifest["command"]["fit_method"], "quadratic")
        self.assertFalse(manifest["claims"]["controller_conformance"])
        self.assertFalse(manifest["claims"]["portable_byte_replay"])

    def test_manifest_is_redacted_and_state_free(self):
        rendered = trace.MANIFEST.read_text(encoding="utf-8")
        for forbidden in (
            "/Users/", "/private/", "hostname", "credential", "token=",
            "state-in", "state-out", "supplementary-coordinate", "downselect",
        ):
            self.assertNotIn(forbidden, rendered)

    def test_fixture_and_output_have_expected_structure(self):
        input_header, input_rows = trace.parse_table(trace.FIXTURE.read_bytes())
        output_header, output_rows = trace.parse_table(trace.REFERENCE.read_bytes())
        self.assertEqual(input_header, "# lnL sigma_lnL x y")
        self.assertEqual(output_header, "# lnL sigma_lnL x y")
        self.assertEqual(len(input_rows), 25)
        self.assertEqual(len(output_rows), 25)
        self.assertTrue(all(row[:2] == [0.0, 0.0] for row in output_rows))
        self.assertTrue(all(-2.4 <= value <= 2.4 for row in output_rows for value in row[2:]))
        trace.validate_output_structure(trace.REFERENCE.read_bytes())
        self.assertEqual(trace.FIXTURE.read_bytes(), (PROOF / "fixtures" / "evaluated-grid.dat").read_bytes())

    def test_synthetic_input_formula_and_declared_uncertainty(self):
        _header, rows = trace.parse_table(trace.FIXTURE.read_bytes())
        for ln_l, sigma_ln_l, x, y in rows:
            self.assertEqual(sigma_ln_l, 0.2)
            self.assertAlmostEqual(ln_l, -0.5 * (x * x + 2 * y * y + 0.3 * x * y))


if __name__ == "__main__":
    unittest.main()
