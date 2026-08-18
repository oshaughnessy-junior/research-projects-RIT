"""Project a preselected RIFT draft-record batch to native table text.

This repository-local proof is deliberately not an installed API. It accepts
no paths and performs no result selection, persistence, or controller work.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any


VOCABULARY = "evaluation.record-draft/v0"
DOMAIN = {"id": "rift.hyperpipe.marginal-log-likelihood-draft", "version": "v0"}
PRODUCER_ID = "rift.hyperpipe.marg-record-sidecar"
PRODUCER_VERSION = "draft-v0"
UNCERTAINTY_SCHEMA = "rift.hyperpipe.sigma-lnL-draft-v0"
MAX_BATCH = 256
MAX_PARAMETERS = 32

_OPAQUE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")


class ProjectionError(ValueError):
    """The supplied records require policy or are not safely projectable."""


def _mapping(name: str, value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProjectionError(f"{name} must be a mapping")
    return value


def _exact_keys(name: str, value: Mapping[str, Any], expected: set[str]) -> None:
    if len(value) != len(expected) or set(value) != expected:
        raise ProjectionError(f"{name} fields must be exactly {sorted(expected)!r}")


def _opaque_id(name: str, value: Any) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or not _OPAQUE_ID.fullmatch(value)
    ):
        raise ProjectionError(f"{name} must be a bounded opaque identifier")
    return value


def _finite(name: str, value: Any, *, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProjectionError(f"{name} must be numeric and not boolean")
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise ProjectionError(f"{name} must be a finite representable number") from exc
    if not math.isfinite(result):
        raise ProjectionError(f"{name} must be finite")
    if nonnegative and result < 0:
        raise ProjectionError(f"{name} must be nonnegative")
    return result


def _parameter_names(parameter_order: Sequence[str]) -> tuple[str, ...]:
    if isinstance(parameter_order, (str, bytes)) or not isinstance(parameter_order, Sequence):
        raise ProjectionError("parameter_order must be an explicit sequence")
    if not 1 <= len(parameter_order) <= MAX_PARAMETERS:
        raise ProjectionError("parameter_order has an unsupported size")
    names = tuple(parameter_order)
    if any(
        not isinstance(name, str)
        or len(name) > 64
        or not _COLUMN_NAME.fullmatch(name)
        or name in {"lnL", "sigma_lnL"}
        for name in names
    ):
        raise ProjectionError("parameter_order contains an invalid native column name")
    if len(set(names)) != len(names):
        raise ProjectionError("parameter_order must not contain duplicates")
    return names


def _validate_common(record_name: str, record: Mapping[str, Any]) -> None:
    if record.get("vocabulary_version") != VOCABULARY:
        raise ProjectionError(f"{record_name} uses an unsupported vocabulary")
    _opaque_id(f"{record_name}.request_id", record.get("request_id"))
    _opaque_id(
        f"{record_name}.logical_evaluation_id", record.get("logical_evaluation_id")
    )
    attempt = record.get("attempt_number")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= 1_000_000:
        raise ProjectionError(f"{record_name}.attempt_number is invalid")

    domain = _mapping(f"{record_name}.domain_contract", record.get("domain_contract"))
    _exact_keys(f"{record_name}.domain_contract", domain, {"id", "version"})
    if dict(domain) != DOMAIN:
        raise ProjectionError(f"{record_name} uses an unsupported domain contract")

    producer = _mapping(f"{record_name}.producer", record.get("producer"))
    _exact_keys(
        f"{record_name}.producer", producer, {"id", "version", "native_reference"}
    )
    if producer["id"] != PRODUCER_ID or producer["version"] != PRODUCER_VERSION:
        raise ProjectionError(f"{record_name} uses an unsupported producer")
    _opaque_id(f"{record_name}.producer.native_reference", producer["native_reference"])


def _validate_pair(
    pair_number: int,
    pair: Any,
    parameter_order: tuple[str, ...],
) -> tuple[str, str, str, list[float]]:
    if isinstance(pair, (str, bytes)) or not isinstance(pair, Sequence) or len(pair) != 2:
        raise ProjectionError(f"pair {pair_number} must contain request and result records")
    request = _mapping(f"pair {pair_number} request", pair[0])
    result = _mapping(f"pair {pair_number} result", pair[1])
    if request.get("record_type") != "request":
        raise ProjectionError(f"pair {pair_number} first record is not a request")
    if result.get("record_type") != "result":
        raise ProjectionError(f"pair {pair_number} second record is not a result")
    _exact_keys(
        f"pair {pair_number} request",
        request,
        {
            "vocabulary_version", "record_type", "request_id",
            "logical_evaluation_id", "attempt_number", "domain_contract",
            "producer", "payload",
        },
    )
    _exact_keys(
        f"pair {pair_number} result",
        result,
        {
            "vocabulary_version", "record_type", "result_id", "request_id",
            "logical_evaluation_id", "attempt_number", "domain_contract",
            "producer", "outcome", "payload", "uncertainty",
        },
    )
    _validate_common(f"pair {pair_number} request", request)
    _validate_common(f"pair {pair_number} result", result)

    for field in (
        "request_id", "logical_evaluation_id", "attempt_number", "domain_contract", "producer"
    ):
        if request.get(field) != result.get(field):
            raise ProjectionError(f"pair {pair_number} has mismatched {field}")

    result_id = _opaque_id(f"pair {pair_number}.result_id", result.get("result_id"))
    if result.get("outcome") != "complete":
        raise ProjectionError(f"pair {pair_number} outcome must be complete")

    request_payload = _mapping(f"pair {pair_number} request payload", request.get("payload"))
    _exact_keys(f"pair {pair_number} request payload", request_payload, {"parameters"})
    parameters = _mapping(
        f"pair {pair_number} request parameters", request_payload.get("parameters")
    )
    if len(parameters) != len(parameter_order) or set(parameters) != set(parameter_order):
        raise ProjectionError(f"pair {pair_number} parameters differ from parameter_order")
    parameter_values = [
        _finite(f"pair {pair_number} parameter {name}", parameters[name])
        for name in parameter_order
    ]

    result_payload = _mapping(f"pair {pair_number} result payload", result.get("payload"))
    _exact_keys(f"pair {pair_number} result payload", result_payload, {"log_likelihood"})
    log_likelihood = _finite(
        f"pair {pair_number} log_likelihood", result_payload.get("log_likelihood")
    )

    uncertainty = _mapping(f"pair {pair_number} uncertainty", result.get("uncertainty"))
    _exact_keys(
        f"pair {pair_number} uncertainty", uncertainty, {"status", "schema_id", "value"}
    )
    if uncertainty["status"] != "reported":
        raise ProjectionError(f"pair {pair_number} uncertainty must be reported")
    if uncertainty["schema_id"] != UNCERTAINTY_SCHEMA:
        raise ProjectionError(f"pair {pair_number} uncertainty schema is unsupported")
    uncertainty_value = _mapping(
        f"pair {pair_number} uncertainty value", uncertainty.get("value")
    )
    _exact_keys(
        f"pair {pair_number} uncertainty value", uncertainty_value, {"sigma_lnL"}
    )
    sigma_ln_l = _finite(
        f"pair {pair_number} sigma_lnL",
        uncertainty_value.get("sigma_lnL"),
        nonnegative=True,
    )

    return (
        request["request_id"],
        result_id,
        request["logical_evaluation_id"],
        [log_likelihood, sigma_ln_l, *parameter_values],
    )


def _format_number(value: float) -> str:
    return repr(value)


def project_batch(pairs: Sequence[Sequence[Mapping[str, Any]]], parameter_order: Sequence[str]) -> str:
    """Return native table text for one already-selected complete batch."""

    if isinstance(pairs, (str, bytes)) or not isinstance(pairs, Sequence):
        raise ProjectionError("pairs must be an explicitly ordered sequence")
    if not 1 <= len(pairs) <= MAX_BATCH:
        raise ProjectionError(f"batch size must be from 1 through {MAX_BATCH}")
    names = _parameter_names(parameter_order)

    request_ids: set[str] = set()
    result_ids: set[str] = set()
    logical_ids: set[str] = set()
    rows: list[list[float]] = []
    for pair_number, pair in enumerate(pairs, start=1):
        request_id, result_id, logical_id, row = _validate_pair(pair_number, pair, names)
        for label, value, seen in (
            ("request_id", request_id, request_ids),
            ("result_id", result_id, result_ids),
            ("logical_evaluation_id", logical_id, logical_ids),
        ):
            if value in seen:
                raise ProjectionError(f"duplicate {label} requires controller policy")
            seen.add(value)
        rows.append(row)

    header = "# lnL sigma_lnL " + " ".join(names)
    body = [" ".join(_format_number(value) for value in row) for row in rows]
    return "\n".join([header, *body]) + "\n"
