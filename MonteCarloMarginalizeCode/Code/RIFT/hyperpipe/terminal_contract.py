"""Declarative terminal-stage contracts for the low-level Hyperpipe writer.

The iterative MARG/posterior loop is intentionally generic, but production
RIFT pipelines also need work after the final iteration.  This module models
that work without teaching the writer executable basenames or pseudo-pipe
policy.  A stage is either an indexed-grid fan-out (the same protocol used by
ILE-as-MARG) or a one-shot command, and dependencies form a small DAG rooted at
the reserved ``pipeline`` stage.
"""

from __future__ import annotations

import json
import os
import re
import shutil

from .marg_contract import INDEXED_GRID_V1, MargJobSpec


INDEXED_GRID_FANOUT_V1 = "indexed-grid-fanout-v1"
COMMAND_V1 = "command-v1"
SUPPORTED_KINDS = (INDEXED_GRID_FANOUT_V1, COMMAND_V1)


def _expand_path(value, base_dir):
    if value is None or value == "":
        return value
    value = os.path.expanduser(str(value))
    if os.path.isabs(value):
        return value
    return os.path.abspath(os.path.join(base_dir, value))


def _resolve_exe(value, base_dir):
    value = os.path.expanduser(str(value))
    if os.path.isabs(value):
        return value
    found = shutil.which(value)
    if found:
        return found
    candidate = os.path.abspath(os.path.join(base_dir, value))
    if os.path.exists(candidate):
        return candidate
    return value


def _read_args(raw, base_dir, name):
    args = raw.get("args")
    args_file = raw.get("args_file")
    if args is not None and args_file:
        raise ValueError(
            "terminal stage {!r} must set only one of args or args_file".format(
                name))
    if not args_file:
        return str(args or "").strip(), None
    args_file = _expand_path(args_file, base_dir)
    if not os.path.isfile(args_file):
        raise ValueError(
            "terminal stage {!r} args_file does not exist: {}".format(
                name, args_file))
    with open(args_file) as stream:
        text = " ".join(line.replace("\\", "").strip() for line in stream)
    words = text.split()
    if words and words[0] == "X":
        words = words[1:]
    return " ".join(words), args_file


class TerminalStageSpec(object):
    """One validated terminal DAG stage."""

    def __init__(self, raw, base_dir):
        if not isinstance(raw, dict):
            raise ValueError("each terminal stage must be a JSON object")
        if not raw.get("name"):
            raise ValueError("each terminal stage requires a name")
        self.name = str(raw["name"])
        if self.name == "pipeline" or not re.match(
                r"^[A-Za-z0-9][A-Za-z0-9_.-]*$", self.name):
            raise ValueError(
                "terminal stage name {!r} is reserved or unsafe".format(
                    self.name))
        self.kind = str(raw.get("kind") or COMMAND_V1)
        if self.kind not in SUPPORTED_KINDS:
            raise ValueError(
                "terminal stage {!r} has unsupported kind {!r}; expected one of {}"
                .format(self.name, self.kind, ", ".join(SUPPORTED_KINDS)))

        depends_on = raw.get("depends_on", ["pipeline"])
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        self.depends_on = [str(item) for item in depends_on]
        if not self.depends_on:
            raise ValueError(
                "terminal stage {!r} must have at least one dependency".format(
                    self.name))

        self.initial_dir = _expand_path(raw.get("initial_dir", "."), base_dir)
        self.log_dir = _expand_path(raw.get("log_dir", self.initial_dir), base_dir)
        self.execution = dict(raw.get("execution") or {})
        self.execution["condor_commands"] = dict(
            self.execution.get("condor_commands", {}))

        self.job = None
        self.grid = None
        self.output_file = None
        self.count = 1
        self.group_size = 1
        self.args_append = ""
        self.exe = None
        self.args = ""
        self.args_file = None
        self.instances = [{}]
        self.universe = str(raw.get("universe") or "vanilla")
        self.no_grid = bool(raw.get("no_grid", False))

        if self.kind == INDEXED_GRID_FANOUT_V1:
            job_raw = raw.get("job")
            if not isinstance(job_raw, dict):
                raise ValueError(
                    "indexed terminal stage {!r} requires a job object".format(
                        self.name))
            self.job = MargJobSpec(job_raw, base_dir)
            if self.job.protocol != INDEXED_GRID_V1:
                raise ValueError(
                    "indexed terminal stage {!r} requires protocol {}".format(
                        self.name, INDEXED_GRID_V1))
            self.grid = _expand_path(raw.get("grid"), base_dir)
            if not self.grid:
                raise ValueError(
                    "indexed terminal stage {!r} requires grid".format(self.name))
            self.output_file = str(raw.get("output_file") or "output.dat")
            fanout = raw.get("fanout") or {}
            self.count = int(fanout.get("count", 1))
            self.group_size = int(fanout.get("group_size", self.job.n_chunk))
            if self.count <= 0 or self.group_size <= 0:
                raise ValueError(
                    "indexed terminal stage {!r} fanout values must be positive"
                    .format(self.name))
            self.args_append = str(raw.get("args_append") or "").strip()
        else:
            if not raw.get("exe"):
                raise ValueError(
                    "command terminal stage {!r} requires exe".format(self.name))
            self.exe = _resolve_exe(raw["exe"], base_dir)
            self.args, self.args_file = _read_args(raw, base_dir, self.name)
            instances = raw.get("instances")
            if instances is not None:
                if not isinstance(instances, list) or not instances:
                    raise ValueError(
                        "terminal stage {!r} instances must be a non-empty list"
                        .format(self.name))
                reserved = {
                    "iteration", "iterationprev", "iterationnext",
                    "event", "ngroup",
                }
                self.instances = []
                for instance in instances:
                    if not isinstance(instance, dict):
                        raise ValueError(
                            "terminal stage {!r} instances must be objects"
                            .format(self.name))
                    macros = {}
                    for key, value in instance.items():
                        key = str(key)
                        if key in reserved or not re.match(
                                r"^[A-Za-z][A-Za-z0-9_]*$", key):
                            raise ValueError(
                                "terminal stage {!r} has unsafe or reserved "
                                "instance macro {!r}".format(self.name, key))
                        macros[key] = str(value)
                    self.instances.append(macros)
                self.count = len(self.instances)

    def execution_value(self, key, default=None):
        if self.job is not None and key in self.job.execution:
            return self.job.execution[key]
        return self.execution.get(key, default)


def load_terminal_stage_specs(path):
    """Load a versioned terminal-stage manifest and validate dependencies."""
    path = os.path.abspath(path)
    with open(path) as stream:
        raw = json.load(stream)
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise ValueError("terminal-stage manifest must be an object with version 1")
    raw_stages = raw.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise ValueError("terminal-stage manifest requires a non-empty stages list")
    base_dir = os.path.dirname(path)
    stages = [TerminalStageSpec(item, base_dir) for item in raw_stages]
    names = [stage.name for stage in stages]
    if len(set(names)) != len(names):
        raise ValueError("terminal stage names must be unique: {}".format(names))
    available = {"pipeline"}
    for stage in stages:
        missing = [name for name in stage.depends_on if name not in available]
        if missing:
            raise ValueError(
                "terminal stage {!r} has unknown or forward dependencies {}"
                .format(stage.name, missing))
        available.add(stage.name)
    return stages
