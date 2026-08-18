"""Read-only one-row RIFT MargDriver to draft-record mapping proof.

This module is deliberately outside the installed RIFT package.  It proves a
mapping around an existing file seam without becoming a runtime API.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Dict, Mapping, Tuple


_OPAQUE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_SHORT_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_MAX_TEXT_BYTES = 64 * 1024


@dataclass(frozen=True)
class RecordContext:
    request_id: str
    result_id: str
    logical_evaluation_id: str
    attempt_number: int
    domain_contract_id: str
    domain_contract_version: str
    producer_id: str
    producer_version: str
    native_reference: str
    uncertainty_schema_id: str


@dataclass(frozen=True)
class MargRow:
    log_likelihood: float
    sigma_ln_likelihood: float
    parameters: Mapping[str, float]


def _bounded_id(name: str, value: str, *, version: bool = False) -> str:
    pattern = _SHORT_VERSION if version else _OPAQUE_ID
    limit = 64 if version else 128
    if not isinstance(value, str) or not 1 <= len(value) <= limit or not pattern.fullmatch(value):
        raise ValueError(f"{name} is not a bounded opaque identifier")
    return value


def _finite_number(name: str, token: str) -> float:
    try:
        value = float(token)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def parse_single_marg_row(text: str) -> MargRow:
    """Parse exactly one native-style ``lnL sigma_lnL ...`` text row."""

    if not isinstance(text, str):
        raise TypeError("native row input must be text")
    if len(text.encode("utf-8")) > _MAX_TEXT_BYTES:
        raise ValueError("native row input exceeds the proof size limit")

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) != 2 or not lines[0].startswith("#"):
        raise ValueError("expected one header and exactly one data row")

    columns = lines[0][1:].split()
    values = lines[1].split()
    if len(columns) != len(values) or len(columns) < 3:
        raise ValueError("header and row must have the same three-or-more columns")
    if columns[:2] != ["lnL", "sigma_lnL"]:
        raise ValueError("leading columns must be exactly lnL and sigma_lnL")
    if len(set(columns)) != len(columns):
        raise ValueError("native row columns must be unique")
    if any(len(name) > 64 or not _COLUMN_NAME.fullmatch(name) for name in columns[2:]):
        raise ValueError("parameter column name is not bounded and portable")

    numbers = [_finite_number(name, token) for name, token in zip(columns, values)]
    if numbers[1] < 0:
        raise ValueError("sigma_lnL must be non-negative")
    parameters = dict(zip(columns[2:], numbers[2:]))
    return MargRow(numbers[0], numbers[1], parameters)


def adapt_pair(
    request_text: str,
    result_text: str,
    context: RecordContext,
) -> Tuple[Dict[str, object], Dict[str, object]]:
    """Return draft request/result mappings for one explicitly paired row."""

    request_row = parse_single_marg_row(request_text)
    result_row = parse_single_marg_row(result_text)
    if request_row.parameters != result_row.parameters:
        raise ValueError("request and result parameter columns/values differ")

    for name in (
        "request_id",
        "result_id",
        "logical_evaluation_id",
        "domain_contract_id",
        "producer_id",
        "native_reference",
        "uncertainty_schema_id",
    ):
        _bounded_id(name, getattr(context, name))
    for name in ("domain_contract_version", "producer_version"):
        _bounded_id(name, getattr(context, name), version=True)
    if (
        isinstance(context.attempt_number, bool)
        or not isinstance(context.attempt_number, int)
        or not 1 <= context.attempt_number <= 1_000_000
    ):
        raise ValueError("attempt_number must be an integer from 1 through 1000000")

    domain_contract = {
        "id": context.domain_contract_id,
        "version": context.domain_contract_version,
    }
    producer = {
        "id": context.producer_id,
        "version": context.producer_version,
        "native_reference": context.native_reference,
    }
    common = {
        "vocabulary_version": "evaluation.record-draft/v0",
        "request_id": context.request_id,
        "logical_evaluation_id": context.logical_evaluation_id,
        "attempt_number": context.attempt_number,
    }

    request: Dict[str, object] = {
        **common,
        "record_type": "request",
        "domain_contract": dict(domain_contract),
        "producer": dict(producer),
        "payload": {"parameters": dict(request_row.parameters)},
    }
    result: Dict[str, object] = {
        **common,
        "record_type": "result",
        "result_id": context.result_id,
        "domain_contract": dict(domain_contract),
        "producer": dict(producer),
        "outcome": "complete",
        "payload": {"log_likelihood": result_row.log_likelihood},
        "uncertainty": {
            "status": "reported",
            "schema_id": context.uncertainty_schema_id,
            "value": {"sigma_lnL": result_row.sigma_ln_likelihood},
        },
    }
    return request, result
