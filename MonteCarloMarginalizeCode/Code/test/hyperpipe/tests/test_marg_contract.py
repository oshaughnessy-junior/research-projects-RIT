"""Tests for the low-level hyperpipe MARG and iteration contracts."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_indexed_grid_contract_loads_relative_inputs(hp_modules, tmp_path):
    exe = tmp_path / "fake_ile"
    exe.write_text("#!/bin/sh\n")
    exe.chmod(0o755)
    args_file = tmp_path / "args_ile.txt"
    args_file.write_text(
        "X --cache local.cache \\\n --adapt-weight-exponent 0.1\n"
    )
    cache = tmp_path / "local.cache"
    cache.write_text("cache\n")
    manifest = tmp_path / "marg_jobs.json"
    manifest.write_text(json.dumps([{
        "name": "ile",
        "protocol": "indexed-grid-v1",
        "exe": "fake_ile",
        "args_file": "args_ile.txt",
        "n_chunk": 7,
        "execution": {
            "cache_file": "local.cache",
            "request_memory": 8192,
            "singularity_image": "osdf://example.org/rift.sif",
            "transfer_files": ["local.cache"],
        },
    }]))

    specs = hp_modules.marg_contract.load_marg_job_specs(str(manifest))

    assert len(specs) == 1
    spec = specs[0]
    assert spec.protocol == hp_modules.marg_contract.INDEXED_GRID_V1
    assert spec.exe == str(exe)
    assert spec.args == "--cache local.cache --adapt-weight-exponent 0.1"
    assert spec.n_chunk == 7
    assert spec.result_glob == "MARG*.dat"
    assert spec.consolidation_mode == "hyperpipeline"
    assert spec.execution["cache_file"] == str(cache)
    assert spec.execution["singularity_image"] == "osdf://example.org/rift.sif"
    assert spec.execution["transfer_files"] == [str(cache)]


def test_marg_contract_rejects_unknown_protocol(hp_modules, tmp_path):
    manifest = tmp_path / "marg_jobs.json"
    manifest.write_text(json.dumps([{
        "name": "bad",
        "protocol": "executable-name-special-case",
        "exe": "/bin/true",
    }]))

    with pytest.raises(ValueError, match="unsupported protocol"):
        hp_modules.marg_contract.load_marg_job_specs(str(manifest))


def test_iteration_schedule_expands_repeat_groups(hp_modules):
    expand = hp_modules.cip_pipeline.expand_argument_schedule
    assert expand([
        "2 --sampler-method GMM\n",
        "1 --sampler-method AV\n",
    ], 5) == [
        "--sampler-method GMM",
        "--sampler-method GMM",
        "--sampler-method AV",
        "--sampler-method AV",
        "--sampler-method AV",
    ]


@pytest.mark.parametrize("line", ["G2 --sampler-method GMM", "Z"])
def test_iteration_schedule_rejects_unimplemented_special_groups(
    hp_modules, line
):
    with pytest.raises(ValueError, match="not supported"):
        hp_modules.cip_pipeline.expand_argument_schedule([line], 2)
