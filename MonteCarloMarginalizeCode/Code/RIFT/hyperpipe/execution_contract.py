"""Open, backend-aware execution settings for Hyperpipe manifests.

The portable fields used by the pipeline writer are intentionally small and
typed.  Scheduler-specific submit commands are an open mapping so adding a
site policy does not require a RIFT release.  The historical
``condor_commands`` mapping remains supported as an alias for the HTCondor
backend.

For migration compatibility, an unknown *scalar* execution key is preserved
as a default backend command and emits a warning.  Unknown structured values
remain errors: callers must put those under ``backend_commands.<backend>`` so
their ownership and interpretation are explicit.
"""

from __future__ import annotations

import re
import warnings


EXECUTION_SCHEMA_V1 = "rift-execution-v1"
OPEN_EXECUTION_KEYS = {"schema", "backend_commands", "condor_commands"}
_BACKEND_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_SCALAR_TYPES = (str, int, float, bool, type(None))


def normalize_execution(execution, portable_keys, context):
    """Validate an execution object while preserving its extension surface."""
    normalized = dict(execution)
    schema = normalized.get("schema", EXECUTION_SCHEMA_V1)
    if schema != EXECUTION_SCHEMA_V1:
        raise ValueError(
            "{} has unsupported execution schema {!r}; expected {!r}".format(
                context, schema, EXECUTION_SCHEMA_V1))
    normalized["schema"] = schema

    condor_commands = normalized.get("condor_commands", {})
    if not isinstance(condor_commands, dict):
        raise ValueError("{} execution condor_commands must be an object".format(
            context))
    normalized["condor_commands"] = dict(condor_commands)

    backend_commands = normalized.get("backend_commands", {})
    if not isinstance(backend_commands, dict):
        raise ValueError("{} execution backend_commands must be an object".format(
            context))
    clean_backend_commands = {}
    for backend, commands in backend_commands.items():
        backend = str(backend)
        if not _BACKEND_NAME.match(backend):
            raise ValueError(
                "{} execution has unsafe backend name {!r}".format(
                    context, backend))
        if not isinstance(commands, dict):
            raise ValueError(
                "{} execution backend_commands.{} must be an object".format(
                    context, backend))
        clean_backend_commands[backend] = dict(commands)

    known = set(portable_keys) | OPEN_EXECUTION_KEYS
    unknown = sorted(set(normalized) - known)
    for key in unknown:
        value = normalized.pop(key)
        if not isinstance(value, _SCALAR_TYPES):
            raise ValueError(
                "{} execution setting {!r} has a structured value; put it "
                "under backend_commands.<backend>".format(context, key))
        warnings.warn(
            "{} execution setting {!r} is not a portable rift-execution-v1 "
            "field; preserving it as a default backend command".format(
                context, key),
            UserWarning,
        )
        clean_backend_commands.setdefault("default", {}).setdefault(key, value)
    normalized["backend_commands"] = clean_backend_commands
    return normalized


def apply_backend_commands(job, execution):
    """Attach open backend commands to a generic job without choosing a backend."""
    for backend, commands in execution.get("backend_commands", {}).items():
        for key, value in commands.items():
            job.add_backend_cmd(str(backend), str(key), value)


def apply_portable_resources(job, execution):
    """Apply portable resources not already consumed by a writer helper."""
    if execution.get("request_cpus") is not None:
        job.add_condor_cmd("request_cpus", execution["request_cpus"])
    request_gpu = execution.get("request_gpu")
    if request_gpu not in (None, False, 0):
        job.add_condor_cmd(
            "request_gpus", 1 if request_gpu is True else request_gpu)


def commands_for_backend(execution, backend):
    """Return native commands for a backend, including compatibility aliases."""
    commands = {}
    if backend in ("htcondor", "glue"):
        commands.update(execution.get("condor_commands", {}))
    backend_commands = execution.get("backend_commands", {})
    commands.update(backend_commands.get("default", {}))
    if backend == "glue":
        commands.update(backend_commands.get("htcondor", {}))
    commands.update(backend_commands.get(backend, {}))
    return commands
