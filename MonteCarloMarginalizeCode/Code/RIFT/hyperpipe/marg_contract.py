"""Declarative contracts for hyperpipe MARG workers.

The hyperpipe DAG historically assumed every MARG executable implemented the
``--using-eos`` CIP-style interface.  That assumption prevents a generic
indexed-grid evaluator such as ILE from serving as MARG: ILE uses
``--sim-grid``, ``--event``, ``--n-events-to-analyze`` and ``--output-file``,
and it needs richer execution/transfer settings.

This module deliberately describes capabilities rather than executable names.
The low-level pipeline writer may therefore support ILE without inspecting its
basename, while other indexed-grid likelihood evaluators can use the same
contract.
"""

from __future__ import annotations

import json
import os
import shutil


HYPERPIPE_V1 = "hyperpipe-v1"
INDEXED_GRID_V1 = "indexed-grid-v1"
SUPPORTED_PROTOCOLS = (HYPERPIPE_V1, INDEXED_GRID_V1)
MARG_EXECUTION_KEYS = {
    "request_memory", "request_disk", "request_gpu",
    "request_cross_platform", "copies", "retries", "max_runtime_minutes",
    "use_osg", "use_singularity", "singularity_image",
    "use_simple_osg_requirements", "use_cvmfs_frames", "use_oauth_files",
    "frames_dir", "cache_file", "transfer_files", "transfer_output_files",
    "condor_commands",
}


def _read_args_file(path):
    """Read an args file and remove the conventional leading dummy token."""
    with open(path) as stream:
        text = " ".join(line.replace("\\", "").strip() for line in stream)
    words = text.split()
    if words and words[0] == "X":
        words = words[1:]
    return " ".join(words)


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
    # Preserve a bare command as a final fallback. Container/local activation
    # wrappers may intentionally resolve it only on the execute side.
    return value


class MargJobSpec(object):
    """Validated description of one MARG executable and its execution needs."""

    def __init__(self, raw, base_dir):
        if not isinstance(raw, dict):
            raise ValueError("each MARG job specification must be a JSON object")
        self.name = str(raw.get("name") or "marg")
        self.protocol = str(raw.get("protocol") or HYPERPIPE_V1)
        if self.protocol not in SUPPORTED_PROTOCOLS:
            raise ValueError(
                "MARG job {!r} has unsupported protocol {!r}; expected one of {}".format(
                    self.name, self.protocol, ", ".join(SUPPORTED_PROTOCOLS)))

        exe = raw.get("exe")
        if not exe:
            raise ValueError("MARG job {!r} is missing exe".format(self.name))
        self.exe = _resolve_exe(exe, base_dir)

        args = raw.get("args")
        args_file = raw.get("args_file")
        if args is not None and args_file:
            raise ValueError(
                "MARG job {!r} must set only one of args or args_file".format(
                    self.name))
        if args_file:
            self.args_file = _expand_path(args_file, base_dir)
            if not os.path.isfile(self.args_file):
                raise ValueError(
                    "MARG job {!r} args_file does not exist: {}".format(
                        self.name, self.args_file))
            self.args = _read_args_file(self.args_file)
        else:
            self.args_file = None
            self.args = str(args or "").strip()

        self.event_file = _expand_path(raw.get("event_file"), base_dir)
        self.n_chunk = int(raw.get("n_chunk", 1))
        if self.n_chunk <= 0:
            raise ValueError("MARG job {!r} n_chunk must be positive".format(self.name))

        execution = raw.get("execution") or {}
        if not isinstance(execution, dict):
            raise ValueError("MARG job {!r} execution must be an object".format(self.name))
        self.execution = dict(execution)
        unknown_execution = set(self.execution) - MARG_EXECUTION_KEYS
        if unknown_execution:
            raise ValueError(
                "MARG job {!r} has unsupported execution settings {}".format(
                    self.name, sorted(unknown_execution)))
        for key in ("frames_dir", "cache_file"):
            if self.execution.get(key):
                self.execution[key] = _expand_path(self.execution[key], base_dir)
        image = self.execution.get("singularity_image")
        if image and "://" not in str(image):
            self.execution["singularity_image"] = _expand_path(image, base_dir)
        transfer_files = self.execution.get("transfer_files", [])
        transfer_output_files = self.execution.get("transfer_output_files", [])
        if not isinstance(transfer_files, list):
            raise ValueError(
                "MARG job {!r} execution transfer_files must be a list".format(
                    self.name))
        if not isinstance(transfer_output_files, list):
            raise ValueError(
                "MARG job {!r} execution transfer_output_files must be a list"
                .format(self.name))
        if not isinstance(self.execution.get("condor_commands", {}), dict):
            raise ValueError(
                "MARG job {!r} execution condor_commands must be an object"
                .format(self.name))
        self.execution["transfer_files"] = [
            _expand_path(item, base_dir)
            for item in transfer_files
        ]
        self.execution["transfer_output_files"] = list(transfer_output_files)
        self.execution["condor_commands"] = dict(
            self.execution.get("condor_commands", {}))

    @property
    def result_glob(self):
        if self.protocol == INDEXED_GRID_V1:
            return "MARG*.dat"
        return "MARG*[0-9]+annotation.dat"

    @property
    def consolidation_mode(self):
        if self.protocol == INDEXED_GRID_V1:
            return "hyperpipeline"
        return "hypercombine"

    def execution_value(self, key, default=None):
        return self.execution.get(key, default)


def load_marg_job_specs(path):
    """Load and validate a JSON array of :class:`MargJobSpec` objects."""
    path = os.path.abspath(path)
    with open(path) as stream:
        raw = json.load(stream)
    if not isinstance(raw, list) or not raw:
        raise ValueError("MARG job specification file must contain a non-empty JSON array")
    base_dir = os.path.dirname(path)
    specs = [MargJobSpec(item, base_dir) for item in raw]
    names = [spec.name for spec in specs]
    if len(set(names)) != len(names):
        raise ValueError("MARG job names must be unique: {}".format(names))
    return specs
