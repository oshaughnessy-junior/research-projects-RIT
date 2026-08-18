"""Standard-library reference reducer for ``campaign.assimilation/v0``.

This module is deliberately standalone and does not import RIFT or a schema
validator. Evaluation records remain separately owned input.
"""

from __future__ import annotations

import copy
import re
import threading


CONTRACT_VERSION = "campaign.assimilation/v0"
RECEIPT_VERSION = "campaign.assimilation-receipt/v0"
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")
_DISPOSITIONS = {"assimilated", "deferred", "rejected"}
_ASSIMILABLE_OUTCOMES = {"complete", "partial"}
_TRANSITION_FIELDS = {
    "contract_version",
    "record_type",
    "transition_id",
    "campaign_id",
    "iteration_id",
    "expected_campaign_revision",
    "committed_campaign_revision",
    "controller_policy",
    "decisions",
}
_DECISION_FIELDS = {
    "request_id",
    "logical_evaluation_id",
    "result_id",
    "attempt_number",
    "disposition",
}
_REQUEST_REQUIRED = {
    "vocabulary_version", "record_type", "request_id", "logical_evaluation_id",
    "attempt_number", "domain_contract", "payload", "producer",
}
_RESULT_REQUIRED = {
    "vocabulary_version", "record_type", "result_id", "request_id",
    "logical_evaluation_id", "attempt_number", "domain_contract", "outcome",
    "producer", "uncertainty",
}
_RESULT_OPTIONAL = {"payload", "cost", "diagnostics"}
_OUTCOMES = {"complete", "partial", "failed", "indeterminate", "unsupported"}
_PRODUCER_REQUIRED = {"id", "version"}
_PRODUCER_OPTIONAL = {"native_reference"}
_COST_FIELDS = {
    "wall_time_s",
    "cpu_time_s",
    "accelerator_time_s",
    "native_evaluation_count",
}
_DIAGNOSTIC_CATEGORIES = {"invalid", "unavailable", "unsupported", "internal", "science"}


class AssimilationError(ValueError):
    """Base class for rejected transitions."""


class ValidationError(AssimilationError):
    """The transition or supplied result records are malformed."""


class RevisionConflict(AssimilationError):
    """The transition did not compare against the current revision."""


class TransitionIdConflict(AssimilationError):
    """A transition ID was reused with different semantics."""


class AssimilationConflict(AssimilationError):
    """A logical evaluation would be assimilated more than once."""


class AttemptConflict(AssimilationError):
    """A new decision did not advance the logical evaluation attempt."""


def _opaque_id(value, field):
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise ValidationError(f"{field} is not a bounded opaque identifier")


def _integer(value, field, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} must be an integer")
    if not minimum <= value <= maximum:
        raise ValidationError(f"{field} is outside its allowed range")


def _domain_contract(value, field):
    if not isinstance(value, dict) or set(value) != {"id", "version"}:
        raise ValidationError(f"{field} must contain only id and version")
    _opaque_id(value["id"], f"{field}.id")
    version = value["version"]
    if not isinstance(version, str) or _VERSION.fullmatch(version) is None:
        raise ValidationError(f"{field}.version is not a bounded version")


def _policy(value):
    if not isinstance(value, dict) or set(value) != {"id", "version"}:
        raise ValidationError("controller_policy must contain only id and version")
    _opaque_id(value["id"], "controller_policy.id")
    if not isinstance(value["version"], str) or _VERSION.fullmatch(value["version"]) is None:
        raise ValidationError("controller_policy.version is not a bounded version")


def _producer(value, field):
    if not isinstance(value, dict):
        raise ValidationError(f"{field} must be an object")
    fields = set(value)
    if not _PRODUCER_REQUIRED <= fields or fields - _PRODUCER_REQUIRED - _PRODUCER_OPTIONAL:
        raise ValidationError(f"{field} fields do not match the evaluation record shape")
    _opaque_id(value["id"], f"{field}.id")
    version = value["version"]
    if not isinstance(version, str) or _VERSION.fullmatch(version) is None:
        raise ValidationError(f"{field}.version is not a bounded version")
    if "native_reference" in value:
        _opaque_id(value["native_reference"], f"{field}.native_reference")


def _uncertainty(value):
    if not isinstance(value, dict) or "status" not in value:
        raise ValidationError("result.uncertainty must contain status")
    status = value["status"]
    if status == "reported":
        if set(value) != {"status", "schema_id", "value"}:
            raise ValidationError("reported uncertainty requires only schema_id and value")
        _opaque_id(value["schema_id"], "result.uncertainty.schema_id")
    elif status in {"not_reported", "not_applicable"}:
        if set(value) != {"status"}:
            raise ValidationError("unreported uncertainty may contain only status")
    else:
        raise ValidationError("result.uncertainty.status is unsupported")


def _cost(value):
    if not isinstance(value, dict) or not value or set(value) - _COST_FIELDS:
        raise ValidationError("result.cost fields do not match the evaluation record shape")
    for field, observed in value.items():
        if isinstance(observed, bool) or not isinstance(observed, (int, float)):
            raise ValidationError(f"result.cost.{field} must be numeric")
        maximum = 1000000000000000 if field == "native_evaluation_count" else 1000000000000
        if field == "native_evaluation_count" and not isinstance(observed, int):
            raise ValidationError("result.cost.native_evaluation_count must be an integer")
        if not 0 <= observed <= maximum:
            raise ValidationError(f"result.cost.{field} is outside its allowed range")


def _diagnostics(value):
    if not isinstance(value, list) or len(value) > 16:
        raise ValidationError("result.diagnostics must be a list of at most 16 items")
    for diagnostic in value:
        if not isinstance(diagnostic, dict):
            raise ValidationError("each result diagnostic must be an object")
        fields = set(diagnostic)
        if not {"category", "code"} <= fields or fields - {"category", "code", "message"}:
            raise ValidationError("result diagnostic fields do not match the record shape")
        if diagnostic["category"] not in _DIAGNOSTIC_CATEGORIES:
            raise ValidationError("result diagnostic category is unsupported")
        _opaque_id(diagnostic["code"], "result.diagnostics.code")
        if "message" in diagnostic and (
            not isinstance(diagnostic["message"], str) or len(diagnostic["message"]) > 512
        ):
            raise ValidationError("result diagnostic message is not a bounded string")


def canonical_transition(transition):
    """Validate and return a decision-order-independent transition copy."""

    if not isinstance(transition, dict) or set(transition) != _TRANSITION_FIELDS:
        raise ValidationError("transition fields do not match the closed contract")
    if transition["contract_version"] != CONTRACT_VERSION:
        raise ValidationError("unsupported contract_version")
    if transition["record_type"] != "assimilation_transition":
        raise ValidationError("unsupported record_type")
    _opaque_id(transition["transition_id"], "transition_id")
    _opaque_id(transition["campaign_id"], "campaign_id")
    _opaque_id(transition["iteration_id"], "iteration_id")
    _policy(transition["controller_policy"])
    expected = transition["expected_campaign_revision"]
    committed = transition["committed_campaign_revision"]
    _integer(expected, "expected_campaign_revision", 0, 9007199254740990)
    _integer(committed, "committed_campaign_revision", 1, 9007199254740991)
    if committed != expected + 1:
        raise ValidationError("committed_campaign_revision must equal expected + 1")

    decisions = transition["decisions"]
    if not isinstance(decisions, list) or not 1 <= len(decisions) <= 10000:
        raise ValidationError("decisions must contain between 1 and 10000 items")
    normalized = []
    logical_ids = set()
    request_ids = set()
    result_ids = set()
    for index, decision in enumerate(decisions):
        if not isinstance(decision, dict) or set(decision) != _DECISION_FIELDS:
            raise ValidationError(f"decision {index} fields do not match the closed contract")
        logical_id = decision["logical_evaluation_id"]
        request_id = decision["request_id"]
        result_id = decision["result_id"]
        _opaque_id(logical_id, f"decision {index} logical_evaluation_id")
        _opaque_id(request_id, f"decision {index} request_id")
        _opaque_id(result_id, f"decision {index} result_id")
        _integer(decision["attempt_number"], f"decision {index} attempt_number", 1, 1000000)
        if decision["disposition"] not in _DISPOSITIONS:
            raise ValidationError(f"decision {index} has an unsupported disposition")
        if logical_id in logical_ids:
            raise ValidationError("a transition may decide each logical evaluation only once")
        if request_id in request_ids:
            raise ValidationError("a transition may reference each request only once")
        if result_id in result_ids:
            raise ValidationError("a transition may reference each result only once")
        logical_ids.add(logical_id)
        request_ids.add(request_id)
        result_ids.add(result_id)
        normalized.append(copy.deepcopy(decision))

    normalized.sort(
        key=lambda item: (
            item["logical_evaluation_id"],
            item["request_id"],
            item["result_id"],
            item["attempt_number"],
            item["disposition"],
        )
    )
    canonical = copy.deepcopy(transition)
    canonical["decisions"] = normalized
    return canonical


def _request_index(request_records, expected_ids):
    if not isinstance(request_records, (list, tuple)):
        raise ValidationError("request_records must be a separately supplied sequence")
    if len(request_records) != len(expected_ids) or len(request_records) > 10000:
        raise ValidationError("request_records must exactly match the bounded decision batch")
    index = {}
    for record in request_records:
        if not isinstance(record, dict) or set(record) != _REQUEST_REQUIRED:
            raise ValidationError("request fields do not match the evaluation record shape")
        if record.get("vocabulary_version") != "evaluation.record-draft/v0":
            raise ValidationError("request vocabulary_version is unsupported")
        if record.get("record_type") != "request":
            raise ValidationError("every supplied request record must have request record_type")
        request_id = record.get("request_id")
        _opaque_id(request_id, "request.request_id")
        _opaque_id(record.get("logical_evaluation_id"), "request.logical_evaluation_id")
        _integer(record.get("attempt_number"), "request.attempt_number", 1, 1000000)
        _domain_contract(record.get("domain_contract"), "request.domain_contract")
        _producer(record.get("producer"), "request.producer")
        if request_id in index:
            raise ValidationError("supplied request_id values must be unique")
        index[request_id] = record
    if set(index) != expected_ids:
        raise ValidationError("supplied request IDs must exactly match decision request IDs")
    return index


def _result_index(result_records, expected_ids):
    if not isinstance(result_records, (list, tuple)):
        raise ValidationError("result_records must be a separately supplied sequence")
    if len(result_records) != len(expected_ids) or len(result_records) > 10000:
        raise ValidationError("result_records must exactly match the bounded decision batch")
    index = {}
    for record in result_records:
        if not isinstance(record, dict):
            raise ValidationError("every supplied result record must be an object")
        fields = set(record)
        if not _RESULT_REQUIRED <= fields or fields - _RESULT_REQUIRED - _RESULT_OPTIONAL:
            raise ValidationError("result fields do not match the evaluation record shape")
        if record.get("vocabulary_version") != "evaluation.record-draft/v0":
            raise ValidationError("result vocabulary_version is unsupported")
        if record.get("record_type") != "result":
            raise ValidationError("every supplied result record must have result record_type")
        result_id = record.get("result_id")
        _opaque_id(result_id, "result.result_id")
        _opaque_id(record.get("request_id"), "result.request_id")
        _opaque_id(record.get("logical_evaluation_id"), "result.logical_evaluation_id")
        _integer(record.get("attempt_number"), "result.attempt_number", 1, 1000000)
        _domain_contract(record.get("domain_contract"), "result.domain_contract")
        _producer(record.get("producer"), "result.producer")
        _uncertainty(record.get("uncertainty"))
        if record.get("outcome") not in _OUTCOMES:
            raise ValidationError("result.outcome is unsupported")
        success = record["outcome"] in _ASSIMILABLE_OUTCOMES
        if success != ("payload" in record):
            raise ValidationError("complete/partial results require payload and other outcomes forbid it")
        if "cost" in record:
            _cost(record["cost"])
        if "diagnostics" in record:
            _diagnostics(record["diagnostics"])
        if result_id in index:
            raise ValidationError("supplied result_id values must be unique")
        index[result_id] = record
    if set(index) != expected_ids:
        raise ValidationError("supplied result IDs must exactly match decision result IDs")
    return index


def _validate_correlations(transition, request_index, result_index):
    for decision in transition["decisions"]:
        try:
            request = request_index[decision["request_id"]]
        except KeyError as error:
            raise ValidationError("a decision references an unavailable request") from error
        try:
            result = result_index[decision["result_id"]]
        except KeyError as error:
            raise ValidationError("a decision references an unavailable result") from error
        for field in ("request_id", "logical_evaluation_id", "attempt_number"):
            if result.get(field) != decision[field]:
                raise ValidationError(f"decision/result {field} correlation failed")
        for field in ("request_id", "logical_evaluation_id", "attempt_number"):
            if request.get(field) != decision[field]:
                raise ValidationError(f"decision/request {field} correlation failed")
        if request["domain_contract"] != result["domain_contract"]:
            raise ValidationError("request/result domain_contract correlation failed")
        outcome = result.get("outcome")
        if decision["disposition"] == "assimilated" and outcome not in _ASSIMILABLE_OUTCOMES:
            raise ValidationError("only complete or partial results may be assimilated")


class Ledger:
    """In-memory atomic reducer whose receipts can reconstruct its state."""

    def __init__(self, campaign_id):
        _opaque_id(campaign_id, "campaign_id")
        self._campaign_id = campaign_id
        self._campaign_revision = 0
        self._assimilated = {}
        self._latest_attempts = {}
        self._receipts_by_id = {}
        self._receipts = []
        self._lock = threading.RLock()

    def apply(self, transition, request_records, result_records):
        """Apply one transition and return ``(receipt, changed)``.

        Validation is complete before any member is mutated.
        """

        with self._lock:
            return self._apply_locked(transition, request_records, result_records)

    def _apply_locked(self, transition, request_records, result_records):
        """Validate and commit while the ledger lock is held."""

        canonical = canonical_transition(transition)
        transition_id = canonical["transition_id"]
        previous = self._receipts_by_id.get(transition_id)
        if previous is not None:
            if previous["transition"] != canonical:
                raise TransitionIdConflict("transition_id was reused with different semantics")

        if canonical["campaign_id"] != self._campaign_id:
            raise ValidationError("transition campaign_id does not match the ledger")
        request_ids = {item["request_id"] for item in canonical["decisions"]}
        result_ids = {item["result_id"] for item in canonical["decisions"]}
        request_index = _request_index(request_records, request_ids)
        result_index = _result_index(result_records, result_ids)
        _validate_correlations(canonical, request_index, result_index)
        if previous is not None:
            return copy.deepcopy(previous), False
        if canonical["expected_campaign_revision"] != self._campaign_revision:
            raise RevisionConflict("expected_campaign_revision does not match current state")

        additions = {}
        latest = {}
        for decision in canonical["decisions"]:
            logical_id = decision["logical_evaluation_id"]
            attempt = decision["attempt_number"]
            if attempt <= self._latest_attempts.get(logical_id, 0):
                raise AttemptConflict("attempt_number must strictly advance across decisions")
            latest[logical_id] = attempt
            if decision["disposition"] != "assimilated":
                continue
            if logical_id in self._assimilated:
                raise AssimilationConflict("logical evaluation was already assimilated")
            additions[logical_id] = {
                "result_id": decision["result_id"],
                "attempt_number": decision["attempt_number"],
            }

        receipt = {
            "receipt_version": RECEIPT_VERSION,
            "transition": canonical,
        }
        # Commit only after the complete candidate transition is valid.
        self._assimilated.update(copy.deepcopy(additions))
        self._latest_attempts.update(latest)
        self._campaign_revision = canonical["committed_campaign_revision"]
        stored = copy.deepcopy(receipt)
        self._receipts_by_id[transition_id] = stored
        self._receipts.append(stored)
        return copy.deepcopy(receipt), True

    @property
    def receipts(self):
        """Return committed receipts without exposing mutable ledger storage."""

        with self._lock:
            return copy.deepcopy(self._receipts)

    @property
    def campaign_id(self):
        with self._lock:
            return self._campaign_id

    @property
    def revision(self):
        with self._lock:
            return self._campaign_revision

    @property
    def assimilated(self):
        with self._lock:
            return copy.deepcopy(self._assimilated)

    @property
    def latest_attempts(self):
        with self._lock:
            return copy.deepcopy(self._latest_attempts)

    @classmethod
    def rebuild(cls, campaign_id, replay_items):
        """Reconstruct state from a stream of receipt plus exact record batches."""

        ledger = cls(campaign_id)
        try:
            iterator = iter(replay_items)
        except TypeError as error:
            raise ValidationError("replay_items must be iterable") from error
        for item in iterator:
            if not isinstance(item, dict) or set(item) != {
                "receipt", "request_records", "result_records"
            }:
                raise ValidationError("each replay item must contain one receipt and exact batches")
            receipt = item["receipt"]
            if not isinstance(receipt, dict) or set(receipt) != {"receipt_version", "transition"}:
                raise ValidationError("receipt fields do not match the closed receipt shape")
            if receipt["receipt_version"] != RECEIPT_VERSION:
                raise ValidationError("unsupported receipt_version")
            produced, changed = ledger.apply(
                receipt["transition"], item["request_records"], item["result_records"]
            )
            if not changed or produced != receipt:
                raise ValidationError("receipt is not canonical or appears more than once")
        return ledger

    def snapshot(self):
        """Return a JSON-compatible state snapshot without evaluation payloads."""

        with self._lock:
            return {
                "campaign_id": self._campaign_id,
                "campaign_revision": self._campaign_revision,
                "assimilated": copy.deepcopy(self._assimilated),
                "latest_attempts": copy.deepcopy(self._latest_attempts),
            }
