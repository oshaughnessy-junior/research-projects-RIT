"""Versioned input models and validation for the drift sentinel."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple


REGISTRY_VERSION = "rift-drift-registry/v1"
RESOLVED_INPUTS_VERSION = "rift-drift-resolved-inputs/v1"
REPORT_VERSION = "rift-drift-report/v1"
CORE_VERSION = "0.1.0"
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_REVISION = re.compile(r"^(?:[0-9a-f]{40}|sha256:[0-9a-f]{64})$")
MAX_INPUT_BYTES = 1024 * 1024


class SentinelInputError(ValueError):
    """Raised when an input cannot be validated without guessing."""

    def __init__(self, errors: Sequence[str]):
        self.errors = tuple(errors)
        super().__init__("; ".join(self.errors))


@dataclass(frozen=True)
class Node:
    node_id: str
    role: str
    owner: str
    source_id: str
    default_ref: str
    visibility: str


@dataclass(frozen=True)
class ExceptionRecord:
    exception_id: str
    owner: str
    rationale: str
    expires: str
    approvers: Tuple[str, ...]


@dataclass(frozen=True)
class SchemaSubsetCheck:
    producer_path: str
    consumer_path: str


@dataclass(frozen=True)
class Edge:
    edge_id: str
    producer: str
    consumer: str
    owner: str
    severity: str
    contract_id: str
    contract_version: str
    verification: str
    semantics: Mapping[str, str]
    check: SchemaSubsetCheck
    exceptions: Tuple[ExceptionRecord, ...]


@dataclass(frozen=True)
class Group:
    group_id: str
    owner: str
    nodes: Tuple[Node, ...]
    edges: Tuple[Edge, ...]


@dataclass(frozen=True)
class Registry:
    registry_id: str
    groups: Tuple[Group, ...]
    fingerprint: str


@dataclass(frozen=True)
class ResolvedNode:
    root: Path
    revision: str
    source_id: str


@dataclass(frozen=True)
class ResolvedInputs:
    registry_fingerprint: str
    groups: Mapping[str, Mapping[str, ResolvedNode]]


def _require_object(value: Any, where: str, errors: List[str]) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{where}: expected object")
        return {}
    return value


def _require_list(value: Any, where: str, errors: List[str]) -> List[Any]:
    if not isinstance(value, list):
        errors.append(f"{where}: expected array")
        return []
    return value


def _string(obj: Mapping[str, Any], key: str, where: str, errors: List[str]) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        errors.append(f"{where}.{key}: expected non-empty string")
        return ""
    return value


def _identifier(obj: Mapping[str, Any], key: str, where: str, errors: List[str]) -> str:
    value = _string(obj, key, where, errors)
    if value and not _IDENTIFIER.fullmatch(value):
        errors.append(f"{where}.{key}: invalid identifier {value!r}")
    return value


def _reject_unknown(obj: Mapping[str, Any], allowed: Sequence[str], where: str, errors: List[str]) -> None:
    unknown = sorted(set(obj) - set(allowed))
    if unknown:
        errors.append(f"{where}: unknown fields {unknown}")


def _parse_date(value: str, where: str, errors: List[str]) -> None:
    # Lexical YYYY-MM-DD validation is sufficient here; the engine performs no
    # wall-clock reads and compares dates only after datetime.date parsing.
    try:
        from datetime import date

        date.fromisoformat(value)
    except ValueError:
        errors.append(f"{where}: expected ISO date YYYY-MM-DD")


def load_registry(path: Path) -> Registry:
    """Load and fully validate a registry JSON file."""

    import hashlib

    if path.stat().st_size > MAX_INPUT_BYTES:
        raise SentinelInputError(["registry: exceeds 1 MiB core input limit"])
    raw_bytes = path.read_bytes()
    try:
        raw = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SentinelInputError([f"registry: invalid JSON: {exc}"])
    errors: List[str] = []
    root = _require_object(raw, "registry", errors)
    _reject_unknown(root, ("registry_version", "registry_id", "groups"), "registry", errors)
    if root.get("registry_version") != REGISTRY_VERSION:
        errors.append(f"registry.registry_version: expected {REGISTRY_VERSION!r}")
    registry_id = _identifier(root, "registry_id", "registry", errors)
    groups: List[Group] = []
    group_ids = set()
    for gi, group_raw in enumerate(_require_list(root.get("groups"), "registry.groups", errors)):
        where = f"registry.groups[{gi}]"
        group_obj = _require_object(group_raw, where, errors)
        _reject_unknown(group_obj, ("id", "owner", "nodes", "edges"), where, errors)
        group_id = _identifier(group_obj, "id", where, errors)
        owner = _string(group_obj, "owner", where, errors)
        if group_id in group_ids:
            errors.append(f"{where}.id: duplicate group {group_id!r}")
        group_ids.add(group_id)
        nodes: List[Node] = []
        node_ids = set()
        for ni, node_raw in enumerate(_require_list(group_obj.get("nodes"), f"{where}.nodes", errors)):
            nwhere = f"{where}.nodes[{ni}]"
            node_obj = _require_object(node_raw, nwhere, errors)
            _reject_unknown(node_obj, ("id", "role", "owner", "source"), nwhere, errors)
            node_id = _identifier(node_obj, "id", nwhere, errors)
            if node_id in node_ids:
                errors.append(f"{nwhere}.id: duplicate node {node_id!r}")
            node_ids.add(node_id)
            source = _require_object(node_obj.get("source"), f"{nwhere}.source", errors)
            _reject_unknown(source, ("id", "default_ref", "visibility"), f"{nwhere}.source", errors)
            visibility = _string(source, "visibility", f"{nwhere}.source", errors)
            if visibility not in ("public", "private", "local"):
                errors.append(f"{nwhere}.source.visibility: expected public, private, or local")
            nodes.append(
                Node(
                    node_id=node_id,
                    role=_string(node_obj, "role", nwhere, errors),
                    owner=_string(node_obj, "owner", nwhere, errors),
                    source_id=_string(source, "id", f"{nwhere}.source", errors),
                    default_ref=_string(source, "default_ref", f"{nwhere}.source", errors),
                    visibility=visibility,
                )
            )
        edges: List[Edge] = []
        edge_ids = set()
        adjacency: Dict[str, List[str]] = {node_id: [] for node_id in node_ids}
        indegree: Dict[str, int] = {node_id: 0 for node_id in node_ids}
        for ei, edge_raw in enumerate(_require_list(group_obj.get("edges"), f"{where}.edges", errors)):
            ewhere = f"{where}.edges[{ei}]"
            edge_obj = _require_object(edge_raw, ewhere, errors)
            _reject_unknown(
                edge_obj,
                ("id", "producer", "consumer", "owner", "severity", "contract", "check", "exceptions"),
                ewhere,
                errors,
            )
            edge_id = _identifier(edge_obj, "id", ewhere, errors)
            if edge_id in edge_ids:
                errors.append(f"{ewhere}.id: duplicate edge {edge_id!r}")
            edge_ids.add(edge_id)
            producer = _identifier(edge_obj, "producer", ewhere, errors)
            consumer = _identifier(edge_obj, "consumer", ewhere, errors)
            if producer not in node_ids:
                errors.append(f"{ewhere}.producer: unknown node {producer!r}")
            if consumer not in node_ids:
                errors.append(f"{ewhere}.consumer: unknown node {consumer!r}")
            if producer in node_ids and consumer in node_ids:
                adjacency[producer].append(consumer)
                indegree[consumer] += 1
            severity = _string(edge_obj, "severity", ewhere, errors)
            if severity not in ("info", "warning", "error"):
                errors.append(f"{ewhere}.severity: expected info, warning, or error")
            contract = _require_object(edge_obj.get("contract"), f"{ewhere}.contract", errors)
            _reject_unknown(contract, ("id", "version", "verification", "semantics"), f"{ewhere}.contract", errors)
            verification = _string(contract, "verification", f"{ewhere}.contract", errors)
            if verification not in ("verified", "inventory_only"):
                errors.append(f"{ewhere}.contract.verification: expected verified or inventory_only")
            semantics_obj = _require_object(contract.get("semantics"), f"{ewhere}.contract.semantics", errors)
            semantics: Dict[str, str] = {}
            for key, value in sorted(semantics_obj.items()):
                if not isinstance(key, str) or not isinstance(value, str) or not value:
                    errors.append(f"{ewhere}.contract.semantics: keys and values must be non-empty strings")
                else:
                    semantics[key] = value
            check = _require_object(edge_obj.get("check"), f"{ewhere}.check", errors)
            _reject_unknown(check, ("kind", "producer_path", "consumer_path"), f"{ewhere}.check", errors)
            if check.get("kind") != "json_schema_subset_v1":
                errors.append(f"{ewhere}.check.kind: only 'json_schema_subset_v1' is supported")
            producer_path = _string(check, "producer_path", f"{ewhere}.check", errors)
            consumer_path = _string(check, "consumer_path", f"{ewhere}.check", errors)
            for key, relpath in (("producer_path", producer_path), ("consumer_path", consumer_path)):
                candidate = Path(relpath)
                if candidate.is_absolute() or ".." in candidate.parts:
                    errors.append(f"{ewhere}.check.{key}: must be a contained relative path")
            exceptions: List[ExceptionRecord] = []
            exception_ids = set()
            for xi, exception_raw in enumerate(_require_list(edge_obj.get("exceptions", []), f"{ewhere}.exceptions", errors)):
                xwhere = f"{ewhere}.exceptions[{xi}]"
                exception_obj = _require_object(exception_raw, xwhere, errors)
                _reject_unknown(exception_obj, ("id", "owner", "rationale", "expires", "approvers"), xwhere, errors)
                exception_id = _identifier(exception_obj, "id", xwhere, errors)
                if exception_id in exception_ids:
                    errors.append(f"{xwhere}.id: duplicate exception {exception_id!r}")
                exception_ids.add(exception_id)
                expires = _string(exception_obj, "expires", xwhere, errors)
                if expires:
                    _parse_date(expires, f"{xwhere}.expires", errors)
                approvers_raw = _require_list(exception_obj.get("approvers"), f"{xwhere}.approvers", errors)
                approvers: List[str] = []
                for ai, approver in enumerate(approvers_raw):
                    if not isinstance(approver, str) or not approver:
                        errors.append(f"{xwhere}.approvers[{ai}]: expected non-empty owner string")
                    else:
                        approvers.append(approver)
                if len(approvers) != len(set(approvers)):
                    errors.append(f"{xwhere}.approvers: duplicate owner")
                node_owners = {node.node_id: node.owner for node in nodes}
                required_approvers = {node_owners.get(producer), node_owners.get(consumer)} - {None}
                missing_approvers = sorted(required_approvers - set(approvers))
                if missing_approvers:
                    errors.append(f"{xwhere}.approvers: missing affected node owners {missing_approvers}")
                exceptions.append(
                    ExceptionRecord(
                        exception_id=exception_id,
                        owner=_string(exception_obj, "owner", xwhere, errors),
                        rationale=_string(exception_obj, "rationale", xwhere, errors),
                        expires=expires,
                        approvers=tuple(approvers),
                    )
                )
            if len(exceptions) > 1:
                errors.append(f"{ewhere}.exceptions: v1 permits at most one edge-wide exception")
            edges.append(
                Edge(
                    edge_id=edge_id,
                    producer=producer,
                    consumer=consumer,
                    owner=_string(edge_obj, "owner", ewhere, errors),
                    severity=severity,
                    contract_id=_identifier(contract, "id", f"{ewhere}.contract", errors),
                    contract_version=_string(contract, "version", f"{ewhere}.contract", errors),
                    verification=verification,
                    semantics=semantics,
                    check=SchemaSubsetCheck(producer_path, consumer_path),
                    exceptions=tuple(exceptions),
                )
            )
        queue = sorted(node_id for node_id, degree in indegree.items() if degree == 0)
        visited = 0
        while queue:
            node_id = queue.pop(0)
            visited += 1
            for target in sorted(adjacency[node_id]):
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)
                    queue.sort()
        if visited != len(nodes):
            errors.append(f"{where}: dependency graph contains a cycle")
        groups.append(Group(group_id, owner, tuple(nodes), tuple(edges)))
    if not groups:
        errors.append("registry.groups: at least one group is required")
    if errors:
        raise SentinelInputError(errors)
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return Registry(registry_id, tuple(groups), "sha256:" + hashlib.sha256(canonical).hexdigest())


def load_resolved_inputs(path: Path, registry: Registry) -> ResolvedInputs:
    """Load runner-resolved local roots without fetching or credential handling."""

    try:
        if path.stat().st_size > MAX_INPUT_BYTES:
            raise SentinelInputError(["resolved_inputs: exceeds 1 MiB core input limit"])
        raw = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SentinelInputError([f"resolved_inputs: invalid JSON: {exc}"])
    errors: List[str] = []
    root = _require_object(raw, "resolved_inputs", errors)
    _reject_unknown(root, ("resolved_inputs_version", "registry_fingerprint", "groups"), "resolved_inputs", errors)
    if root.get("resolved_inputs_version") != RESOLVED_INPUTS_VERSION:
        errors.append(f"resolved_inputs.resolved_inputs_version: expected {RESOLVED_INPUTS_VERSION!r}")
    fingerprint = _string(root, "registry_fingerprint", "resolved_inputs", errors)
    if fingerprint != registry.fingerprint:
        errors.append("resolved_inputs.registry_fingerprint: does not match the loaded registry")
    groups_obj = _require_object(root.get("groups"), "resolved_inputs.groups", errors)
    expected_groups = {group.group_id: group for group in registry.groups}
    if set(groups_obj) != set(expected_groups):
        errors.append("resolved_inputs.groups: group IDs must exactly match the registry")
    resolved_groups: Dict[str, Dict[str, ResolvedNode]] = {}
    manifest_base = path.resolve().parent
    for group_id, group in expected_groups.items():
        group_obj = _require_object(groups_obj.get(group_id), f"resolved_inputs.groups.{group_id}", errors)
        expected_nodes = {node.node_id: node for node in group.nodes}
        if set(group_obj) != set(expected_nodes):
            errors.append(f"resolved_inputs.groups.{group_id}: node IDs must exactly match the registry")
        resolved_nodes: Dict[str, ResolvedNode] = {}
        for node_id, node in expected_nodes.items():
            nwhere = f"resolved_inputs.groups.{group_id}.{node_id}"
            node_obj = _require_object(group_obj.get(node_id), nwhere, errors)
            _reject_unknown(node_obj, ("root", "revision", "source_id"), nwhere, errors)
            root_text = _string(node_obj, "root", nwhere, errors)
            revision = _string(node_obj, "revision", nwhere, errors)
            source_id = _string(node_obj, "source_id", nwhere, errors)
            if revision and not _REVISION.fullmatch(revision):
                errors.append(f"{nwhere}.revision: require immutable 40-hex git SHA or sha256 digest")
            if source_id != node.source_id:
                errors.append(f"{nwhere}.source_id: does not match registry node")
            local_root = Path(root_text)
            if not local_root.is_absolute():
                local_root = manifest_base / local_root
            resolved_nodes[node_id] = ResolvedNode(local_root.resolve(), revision, source_id)
        resolved_groups[group_id] = resolved_nodes
    if errors:
        raise SentinelInputError(errors)
    return ResolvedInputs(fingerprint, resolved_groups)
