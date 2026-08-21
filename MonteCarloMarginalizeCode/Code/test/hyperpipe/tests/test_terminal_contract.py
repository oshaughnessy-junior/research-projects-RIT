import json
from pathlib import Path

import pytest

from RIFT.hyperpipe.terminal_contract import (
    COMMAND_V1,
    INDEXED_GRID_FANOUT_V1,
    load_terminal_stage_specs,
)


def _write_manifest(tmp_path, stages):
    path = tmp_path / "terminal.json"
    path.write_text(json.dumps({"version": 1, "stages": stages}))
    return path


def test_indexed_fanout_then_command_round_trip(tmp_path):
    exe = tmp_path / "worker"
    exe.write_text("#!/bin/sh\n")
    exe.chmod(0o755)
    collector = tmp_path / "collector"
    collector.write_text("#!/bin/sh\n")
    collector.chmod(0o755)
    grid = tmp_path / "grid.dat"
    grid.write_text("# lnL sigma_lnL m1 m2\n")
    path = _write_manifest(tmp_path, [
        {
            "name": "samples",
            "kind": INDEXED_GRID_FANOUT_V1,
            "job": {
                "name": "ile",
                "protocol": "indexed-grid-v1",
                "exe": str(exe),
                "args": "--cache local.cache",
                "n_chunk": 4,
                "execution": {"request_memory": 8192, "request_gpu": True},
            },
            "grid": str(grid),
            "output_file": "EXTR_out.xml",
            "fanout": {"count": 3, "group_size": 4},
            "args_append": "--save-samples",
        },
        {
            "name": "collect",
            "kind": COMMAND_V1,
            "depends_on": ["samples"],
            "exe": str(collector),
            "args": "--iteration $(macroiteration)",
            "universe": "local",
        },
    ])

    stages = load_terminal_stage_specs(str(path))

    assert [stage.name for stage in stages] == ["samples", "collect"]
    assert stages[0].count == 3
    assert stages[0].group_size == 4
    assert stages[0].execution_value("request_memory") == 8192
    assert stages[1].depends_on == ["samples"]
    assert stages[1].universe == "local"


@pytest.mark.parametrize("stages, match", [
    ([{"name": "x", "kind": "unknown", "exe": "/bin/true"}],
     "unsupported kind"),
    ([{"name": "x", "kind": COMMAND_V1, "exe": "/bin/true",
       "depends_on": ["later"]}], "unknown or forward"),
    ([{"name": "x", "kind": COMMAND_V1, "exe": "/bin/true"},
      {"name": "x", "kind": COMMAND_V1, "exe": "/bin/true"}],
     "names must be unique"),
    ([{"name": "pipeline", "kind": COMMAND_V1, "exe": "/bin/true"}],
     "reserved or unsafe"),
])
def test_invalid_terminal_manifests_fail_early(tmp_path, stages, match):
    path = _write_manifest(tmp_path, stages)
    with pytest.raises(ValueError, match=match):
        load_terminal_stage_specs(str(path))


def test_relative_paths_are_manifest_relative(tmp_path):
    (tmp_path / "worker").write_text("#!/bin/sh\n")
    (tmp_path / "grid.dat").write_text("# lnL sigma_lnL x\n")
    path = _write_manifest(tmp_path, [{
        "name": "samples",
        "kind": INDEXED_GRID_FANOUT_V1,
        "job": {
            "protocol": "indexed-grid-v1",
            "exe": "worker",
            "n_chunk": 1,
        },
        "grid": "grid.dat",
    }])

    stage = load_terminal_stage_specs(str(path))[0]

    assert stage.job.exe == str(tmp_path / "worker")
    assert stage.grid == str(tmp_path / "grid.dat")
    assert Path(stage.initial_dir) == tmp_path


def test_command_instances_define_generic_fanout_macros(tmp_path):
    path = _write_manifest(tmp_path, [{
        "name": "batches",
        "kind": COMMAND_V1,
        "exe": "/bin/true",
        "args": "--start $(macrostart) --end $(macroend)",
        "instances": [
            {"start": 0, "end": 20},
            {"start": 20, "end": 40},
        ],
    }])

    stage = load_terminal_stage_specs(str(path))[0]

    assert stage.count == 2
    assert stage.instances == [
        {"start": "0", "end": "20"},
        {"start": "20", "end": "40"},
    ]


@pytest.mark.parametrize("instances", [[], [{"iteration": 2}], [{"bad-key": 1}]])
def test_invalid_command_instances_fail_early(tmp_path, instances):
    path = _write_manifest(tmp_path, [{
        "name": "bad",
        "kind": COMMAND_V1,
        "exe": "/bin/true",
        "instances": instances,
    }])
    with pytest.raises(ValueError, match="instances|unsafe or reserved"):
        load_terminal_stage_specs(str(path))
