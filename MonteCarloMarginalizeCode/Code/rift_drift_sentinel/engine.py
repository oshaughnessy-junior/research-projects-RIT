"""Pure local comparison engine for one narrow schema contract."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

from .model import CORE_VERSION, REPORT_VERSION, Edge, Registry, ResolvedInputs


MAX_SCHEMA_BYTES = 1024 * 1024
MAX_SCHEMA_PROPERTIES = 4096
MAX_ANNOTATIONS = 64
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PUBLIC_SCALARS = {"array", "boolean", "integer", "null", "number", "object", "string"}


def _contained_file(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        raise ValueError("resolved path escapes node root")
    if not candidate.is_file():
        raise ValueError("registered file is missing or not a regular file")
    return candidate


def _load_json(root: Path, relative: str) -> Tuple[Mapping[str, Any], str]:
    path = _contained_file(root, relative)
    if path.stat().st_size > MAX_SCHEMA_BYTES:
        raise ValueError("registered schema exceeds 1 MiB core input limit")
    data = path.read_bytes()
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"registered file is not valid JSON: {exc}")
    if not isinstance(value, dict):
        raise ValueError("registered schema must be a JSON object")
    return value, "sha256:" + hashlib.sha256(data).hexdigest()


def _validate_schema_shape(schema: Mapping[str, Any], label: str) -> None:
    if schema.get("type") != "object":
        raise ValueError(f"{label} schema: v1 supports only type=object")
    properties = schema.get("properties", {})
    required = schema.get("required", [])
    if not isinstance(properties, dict):
        raise ValueError(f"{label} schema: properties must be an object")
    if len(properties) > MAX_SCHEMA_PROPERTIES:
        raise ValueError(f"{label} schema: properties exceed 4096-field limit")
    if not isinstance(required, list) or any(not isinstance(item, str) for item in required):
        raise ValueError(f"{label} schema: required must be an array of strings")
    if len(required) != len(set(required)):
        raise ValueError(f"{label} schema: required fields must be unique")
    for field, declaration in properties.items():
        if not isinstance(field, str) or not isinstance(declaration, dict):
            raise ValueError(f"{label} schema: properties must map string names to objects")
        if "type" in declaration and not isinstance(declaration["type"], str):
            raise ValueError(f"{label} schema: {field}.type must be a string")
        if "enum" in declaration and not isinstance(declaration["enum"], list):
            raise ValueError(f"{label} schema: {field}.enum must be an array")
        if "x-science" in declaration and not isinstance(declaration["x-science"], dict):
            raise ValueError(f"{label} schema: {field}.x-science must be an object")
        if isinstance(declaration.get("x-science"), dict) and len(declaration["x-science"]) > MAX_ANNOTATIONS:
            raise ValueError(f"{label} schema: {field}.x-science exceeds 64-key limit")
    if "x-contract" in schema and not isinstance(schema["x-contract"], dict):
        raise ValueError(f"{label} schema: x-contract must be an object")
    if isinstance(schema.get("x-contract"), dict) and len(schema["x-contract"]) > MAX_ANNOTATIONS:
        raise ValueError(f"{label} schema: x-contract exceeds 64-key limit")


def _safe_evidence(value: Any) -> Any:
    """Describe untrusted mismatch content without copying it into a report."""

    if value is None or (isinstance(value, str) and value in _PUBLIC_SCALARS):
        return value
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    kind = "array" if isinstance(value, list) else ("object" if isinstance(value, dict) else type(value).__name__)
    return {"kind": kind, "bytes": len(canonical), "sha256": hashlib.sha256(canonical).hexdigest()}


def _path_key(value: str) -> str:
    """Identify an untrusted schema key without copying it to a wider report."""

    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _compare_subset(producer: Mapping[str, Any], consumer: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    """Compare a deliberately small JSON-Schema-like contract subset."""

    mismatches: List[Mapping[str, Any]] = []
    producer_properties = producer.get("properties", {})
    consumer_properties = consumer.get("properties", {})
    producer_required = set(producer.get("required", []))
    consumer_required = set(consumer.get("required", []))
    for field in sorted(consumer_required):
        if field not in producer_required:
            mismatches.append(
                {"path": f"$.required[{_path_key(field)}]", "expected": "producer-required", "observed": "not required"}
            )
    for field, expected in sorted(consumer_properties.items()):
        observed = producer_properties.get(field)
        field_key = _path_key(field)
        if observed is None:
            mismatches.append({"path": f"$.properties[{field_key}]", "expected": "present", "observed": "missing"})
            continue
        if not isinstance(expected, dict) or not isinstance(observed, dict):
            mismatches.append({"path": f"$.properties[{field_key}]", "expected": "object", "observed": "non-object"})
            continue
        for key in ("type", "enum"):
            if key in expected and observed.get(key) != expected[key]:
                mismatches.append(
                    {
                        "path": f"$.properties[{field_key}].{key}",
                        "expected": _safe_evidence(expected[key]),
                        "observed": _safe_evidence(observed.get(key)),
                    }
                )
        expected_science = expected.get("x-science", {})
        observed_science = observed.get("x-science", {})
        if not isinstance(expected_science, dict) or not isinstance(observed_science, dict):
            mismatches.append(
                {"path": f"$.properties[{field_key}].x-science", "expected": "object", "observed": "non-object"}
            )
            continue
        for key, expected_value in sorted(expected_science.items()):
            if observed_science.get(key) != expected_value:
                mismatches.append(
                    {
                        "path": f"$.properties[{field_key}].x-science[{_path_key(key)}]",
                        "expected": _safe_evidence(expected_value),
                        "observed": _safe_evidence(observed_science.get(key)),
                    }
                )
    expected_contract = consumer.get("x-contract", {})
    observed_contract = producer.get("x-contract", {})
    if not isinstance(expected_contract, dict) or not isinstance(observed_contract, dict):
        mismatches.append({"path": "$.x-contract", "expected": "object", "observed": "non-object"})
    else:
        for key, expected_value in sorted(expected_contract.items()):
            if observed_contract.get(key) != expected_value:
                mismatches.append(
                    {
                        "path": f"$.x-contract[{_path_key(key)}]",
                        "expected": _safe_evidence(expected_value),
                        "observed": _safe_evidence(observed_contract.get(key)),
                    }
                )
    return mismatches


def _active_exception(edge: Edge, as_of: date):
    active = [item for item in edge.exceptions if date.fromisoformat(item.expires) >= as_of]
    if not active:
        return None
    # Validation preserves a deterministic order; accepting more than one exception
    # would make waiver authority ambiguous, so surface the first and count all.
    return active[0]


def evaluate(registry: Registry, resolved: ResolvedInputs, run_id: str, as_of: str) -> Mapping[str, Any]:
    """Evaluate every registered edge and return a deterministic report mapping."""

    if not _RUN_ID.fullmatch(run_id):
        raise ValueError("run_id must be 1-128 safe identifier characters")
    run_date = date.fromisoformat(as_of)
    checks: List[Dict[str, Any]] = []
    for group in sorted(registry.groups, key=lambda item: item.group_id):
        nodes = resolved.groups[group.group_id]
        for edge in sorted(group.edges, key=lambda item: item.edge_id):
            producer = nodes[edge.producer]
            consumer = nodes[edge.consumer]
            base: Dict[str, Any] = {
                "group": group.group_id,
                "edge": edge.edge_id,
                "producer": edge.producer,
                "consumer": edge.consumer,
                "owner": edge.owner,
                "severity": edge.severity,
                "contract": {"id": edge.contract_id, "version": edge.contract_version},
                "check": "json_schema_subset_v1",
                "revisions": {edge.producer: producer.revision, edge.consumer: consumer.revision},
                "evidence": {
                    "producer": {"node": edge.producer, "path": edge.check.producer_path},
                    "consumer": {"node": edge.consumer, "path": edge.check.consumer_path},
                },
                "reproduce": "rift-drift-sentinel check --registry REGISTRY --resolved-inputs RESOLVED --run-id RUN --as-of DATE",
            }
            try:
                provided, provided_hash = _load_json(producer.root, edge.check.producer_path)
                required, required_hash = _load_json(consumer.root, edge.check.consumer_path)
                _validate_schema_shape(provided, "producer")
                _validate_schema_shape(required, "consumer")
                base["evidence"]["producer"]["sha256"] = provided_hash
                base["evidence"]["consumer"]["sha256"] = required_hash
                mismatches = _compare_subset(provided, required)
                base["mismatches"] = mismatches
                if edge.verification == "inventory_only":
                    base["outcome"] = "indeterminate"
                    base["observation_status"] = "indeterminate"
                    base["reason"] = "contract inventory has not been owner-verified"
                elif mismatches:
                    exception = _active_exception(edge, run_date)
                    if exception is None:
                        base["outcome"] = "incompatible"
                        base["observation_status"] = "observed"
                    else:
                        base["outcome"] = "incompatible"
                        base["observation_status"] = "intentionally_divergent"
                        base["exception"] = {
                            "id": exception.exception_id,
                            "owner": exception.owner,
                            "rationale": exception.rationale,
                            "expires": exception.expires,
                            "approvers": list(exception.approvers),
                        }
                else:
                    base["outcome"] = "compatible"
                    base["observation_status"] = "observed"
            except OSError:
                base["outcome"] = "indeterminate"
                base["observation_status"] = "indeterminate"
                base["reason"] = "registered file could not be read"
            except ValueError as exc:
                base["outcome"] = "indeterminate"
                base["observation_status"] = "indeterminate"
                base["reason"] = str(exc)
            checks.append(base)
    counts = {name: sum(1 for check in checks if check["outcome"] == name) for name in ("compatible", "incompatible", "indeterminate")}
    counts["intentionally_divergent"] = sum(
        1 for check in checks if check["observation_status"] == "intentionally_divergent"
    )
    blocking_incompatible = sum(
        1
        for check in checks
        if check["outcome"] == "incompatible" and check["observation_status"] == "observed"
    )
    counts["blocking_incompatible"] = blocking_incompatible
    status = (
        "incompatible"
        if blocking_incompatible
        else (
            "indeterminate"
            if counts["indeterminate"]
            else ("intentionally_divergent" if counts["intentionally_divergent"] else "compatible")
        )
    )
    return {
        "report_version": REPORT_VERSION,
        "core_version": CORE_VERSION,
        "run_id": run_id,
        "as_of": as_of,
        "registry": {"id": registry.registry_id, "fingerprint": registry.fingerprint},
        "status": status,
        "summary": counts,
        "checks": checks,
    }
