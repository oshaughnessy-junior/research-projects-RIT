#!/usr/bin/env python3
"""Opt-in byte-substitutability check for the neutral contract prerelease."""

import hashlib
import importlib
from importlib import metadata, resources
import json
import os
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
OPT_IN = os.environ.get("RIFT_TEST_NEUTRAL_CONTRACT_PRERELEASE") == "1"

DIST_NAME = "inference-campaign-contracts"
DIST_VERSION = "0.1.0a1"
PACKAGE_NAME = "inference_campaign_contracts"
REQUIREMENT = (
    "inference-campaign-contracts @ "
    "https://github.com/oshaughnessy-junior/inference-campaign-contracts/"
    "releases/download/v0.1.0a1/"
    "inference_campaign_contracts-0.1.0a1-py3-none-any.whl "
    "--hash=sha256:"
    "a3fb1ea97a0b6889918baa9ee856ce16f5b6c932ed470063f69da636b6dcfe4c"
)

SOURCES = {
    "reducer": {
        "incubation": ROOT
        / "vision"
        / "contracts"
        / "controller-assimilation-v0"
        / "reference.py",
        "sha256": "9bc6275b1018d8e3882b12d39b005a1296206b80e22fb24bd13e4dde262d9eef",
    },
    "evaluation_schema": {
        "incubation": ROOT
        / "vision"
        / "contracts"
        / "evaluation-record-draft-v0"
        / "envelope.schema.json",
        "sha256": "2bb0ade59efbf0308ac8886367cc0403f3c1299b03c705097037ebe10be04c59",
    },
    "assimilation_schema": {
        "incubation": ROOT
        / "vision"
        / "contracts"
        / "controller-assimilation-v0"
        / "transition.schema.json",
        "sha256": "12baba5b29764c07663d707215e3673d5ed870e10ed7c3764dee7cbe8d5009d5",
    },
}


def _sha256(payload):
    return hashlib.sha256(payload).hexdigest()


@unittest.skipUnless(
    OPT_IN,
    "neutral prerelease check is opt-in; this skip is not adoption evidence",
)
class NeutralContractPrereleaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.distribution = metadata.distribution(DIST_NAME)
        except metadata.PackageNotFoundError as exc:
            raise AssertionError(
                "opt-in requested but inference-campaign-contracts is not installed"
            ) from exc

    def test_exact_test_only_requirement_and_distribution_metadata(self):
        active = [
            line.strip()
            for line in (
                HERE / "requirements-neutral-contract-prerelease.txt"
            ).read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertEqual(active, [REQUIREMENT])
        self.assertEqual(self.distribution.version, DIST_VERSION)
        self.assertFalse(self.distribution.requires)

    def test_00_package_root_is_empty_before_explicit_submodule_import(self):
        package = importlib.import_module(PACKAGE_NAME)
        public_names = sorted(name for name in vars(package) if not name.startswith("_"))
        self.assertEqual(public_names, [])

        reducer = importlib.import_module(PACKAGE_NAME + ".assimilation_v0")
        self.assertEqual(Path(reducer.__file__).name, "assimilation_v0.py")

    def test_distributed_sources_equal_the_incubation_bytes(self):
        reducer = importlib.import_module(PACKAGE_NAME + ".assimilation_v0")
        package_root = resources.files(PACKAGE_NAME)
        installed = {
            "reducer": Path(reducer.__file__).read_bytes(),
            "evaluation_schema": package_root.joinpath(
                "schemas", "evaluation-record-draft-v0.schema.json"
            ).read_bytes(),
            "assimilation_schema": package_root.joinpath(
                "schemas", "campaign-assimilation-v0.schema.json"
            ).read_bytes(),
        }

        for name, source in SOURCES.items():
            with self.subTest(source=name):
                incubation = source["incubation"].read_bytes()
                self.assertEqual(_sha256(incubation), source["sha256"])
                self.assertEqual(_sha256(installed[name]), source["sha256"])
                self.assertEqual(installed[name], incubation)

        evaluation = json.loads(installed["evaluation_schema"])
        assimilation = json.loads(installed["assimilation_schema"])
        self.assertIn(b"evaluation.record-draft/v0", installed["evaluation_schema"])
        self.assertEqual(
            assimilation["properties"]["contract_version"]["const"],
            "campaign.assimilation/v0",
        )
        self.assertEqual(evaluation["$id"], "urn:rift:vision:evaluation-record-draft-v0")
        self.assertEqual(
            assimilation["$id"], "urn:rift:vision:campaign-assimilation-v0"
        )


if __name__ == "__main__":
    unittest.main()
