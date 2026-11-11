import importlib
import json
import shlex
from pathlib import Path
import sys
import types

import pytest

_CODE = Path(__file__).resolve().parents[3]
if str(_CODE) not in sys.path:
    sys.path.insert(0, str(_CODE))
if "RIFT" not in sys.modules:
    _RIFT = types.ModuleType("RIFT")
    _RIFT.__path__ = [str(_CODE / "RIFT")]
    sys.modules["RIFT"] = _RIFT
_CONTRACT = importlib.import_module("RIFT.hyperpipe.terminal_contract")
COMMAND_V1 = _CONTRACT.COMMAND_V1
INDEXED_GRID_FANOUT_V1 = _CONTRACT.INDEXED_GRID_FANOUT_V1
load_terminal_stage_specs = _CONTRACT.load_terminal_stage_specs


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
    assert stages[0].group_sizes == [4, 4, 4]
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


def test_indexed_fanout_accepts_remainder_group(tmp_path):
    path = _write_manifest(tmp_path, [{
        "name": "samples",
        "kind": INDEXED_GRID_FANOUT_V1,
        "job": {
            "protocol": "indexed-grid-v1",
            "exe": "/bin/true",
            "n_chunk": 3,
        },
        "grid": "grid.dat",
        "fanout": {"count": 3, "group_size": 3,
                   "group_sizes": [3, 3, 1]},
    }])
    (tmp_path / "grid.dat").write_text("# lnL sigma_lnL m1 m2\n")

    stage = load_terminal_stage_specs(str(path))[0]

    assert stage.count == 3
    assert stage.group_sizes == [3, 3, 1]


def test_command_preserves_unknown_scalar_execution_setting(tmp_path):
    path = _write_manifest(tmp_path, [{
        "name": "bad",
        "kind": COMMAND_V1,
        "exe": "/bin/true",
        "execution": {"silently_ignored_before": True},
    }])
    with pytest.warns(UserWarning, match="preserving it"):
        stage = load_terminal_stage_specs(str(path))[0]
    assert stage.execution["backend_commands"] == {
        "default": {"silently_ignored_before": True}}


def test_command_rejects_unknown_structured_execution_setting(tmp_path):
    path = _write_manifest(tmp_path, [{
        "name": "bad",
        "kind": COMMAND_V1,
        "exe": "/bin/true",
        "execution": {"site_policy": {"mode": "required"}},
    }])
    with pytest.raises(ValueError, match="backend_commands"):
        load_terminal_stage_specs(str(path))


@pytest.mark.parametrize("key, value, match", [
    ("transfer_files", "input.dat", "transfer_files must be a list"),
    ("transfer_output_files", "output.dat",
     "transfer_output_files must be a list"),
    ("condor_commands", "request_cpus=2",
     "condor_commands must be an object"),
])
def test_command_rejects_malformed_execution_types(
    tmp_path, key, value, match
):
    path = _write_manifest(tmp_path, [{
        "name": "bad",
        "kind": COMMAND_V1,
        "exe": "/bin/true",
        "execution": {key: value},
    }])
    with pytest.raises(ValueError, match=match):
        load_terminal_stage_specs(str(path))


def test_terminal_stage_rejects_non_object_execution(tmp_path):
    path = _write_manifest(tmp_path, [{
        "name": "bad",
        "kind": COMMAND_V1,
        "exe": "/bin/true",
        "execution": ["request_memory", 1024],
    }])
    with pytest.raises(ValueError, match="execution must be an object"):
        load_terminal_stage_specs(str(path))


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


SUBWORKFLOW_V1 = _CONTRACT.SUBWORKFLOW_V1


def test_subworkflow_stage_round_trip(tmp_path):
    """A sub-workflow is a stage: one node, wired by depends_on like any other.

    This is the composability primitive the manifest was missing.  Fan-out and
    barrier were already expressible -- a stage name denotes every node of its
    fan-out, so naming it in `depends_on` wires all of them -- and this is the
    third.  It carries no loop semantics on purpose: a bounded loop is a
    sub-workflow plus a convergence rule, and conflating them would make the
    simple case pay for the complicated one.
    """
    child = tmp_path / "child.dag"
    child.write_text("JOB a a.sub\n")
    exe = tmp_path / "post"
    exe.write_text("#!/bin/sh\n")
    exe.chmod(0o755)
    path = _write_manifest(tmp_path, [
        {"name": "inner", "kind": SUBWORKFLOW_V1, "dag": "child.dag"},
        {"name": "after", "kind": COMMAND_V1, "depends_on": ["inner"],
         "exe": str(exe), "args": "--x 1"},
    ])

    stages = load_terminal_stage_specs(str(path))

    assert [stage.name for stage in stages] == ["inner", "after"]
    assert stages[0].kind == SUBWORKFLOW_V1
    # Resolved against the manifest's directory, like every other path here.
    assert stages[0].dag == str(child)
    assert stages[0].count == 1
    assert stages[1].depends_on == ["inner"]


def test_subworkflow_stage_requires_a_dag_but_not_an_existing_one(tmp_path):
    """`dag` is required here; whether the file exists is the writer's call.

    Each backend lands a workflow at its own path -- `child.dag` on HTCondor,
    `child_local.sh` on the shell backend, `child_dag.sh` on Slurm -- so an
    existence check on the literal name in this backend-agnostic module would
    reject valid manifests on three backends out of four.  The writer checks
    it through the backend's own path mapping, still at build time.  This test
    pins the division, so a later "helpful" existence check here fails rather
    than quietly breaking every non-HTCondor manifest.
    """
    path = _write_manifest(tmp_path, [
        {"name": "inner", "kind": SUBWORKFLOW_V1, "dag": "no_such.dag"}])
    stages = load_terminal_stage_specs(str(path))
    assert stages[0].dag == str(tmp_path / "no_such.dag")

    path = _write_manifest(tmp_path, [
        {"name": "inner", "kind": SUBWORKFLOW_V1}])
    with pytest.raises(ValueError, match="requires dag"):
        load_terminal_stage_specs(str(path))


@pytest.mark.parametrize("field,value", [
    ("exe", "/bin/true"),
    ("args", "--x"),
    ("grid", "grid.dat"),
    ("fanout", {"count": 2}),
    ("instances", [{"a": "1"}]),
])
def test_subworkflow_stage_rejects_job_fields(tmp_path, field, value):
    """Silently ignoring these is how a stage does less than its author asked.

    Every one of them configures a job, and a sub-workflow stage has no job --
    its children are configured by the sub-workflow's own manifest.  Accepting
    and dropping them would leave an author with a workflow that builds, runs,
    and does not do what the file says.
    """
    child = tmp_path / "child.dag"
    child.write_text("JOB a a.sub\n")
    stage = {"name": "inner", "kind": SUBWORKFLOW_V1, "dag": "child.dag",
             field: value}
    path = _write_manifest(tmp_path, [stage])
    with pytest.raises(ValueError, match="does not take"):
        load_terminal_stage_specs(str(path))


@pytest.mark.parametrize("argv", [
    ["--probe", "/data/a b.xml"],
    ["--probe", "a  b"],
    ["--probe", "abc\\"],
    ["--probe", "a'b \"quoted\""],
    ["--probe", ""],
    [],
])
@pytest.mark.parametrize("mode", ["inline", "file"])
def test_terminal_args_preserve_actual_command_argv(hp_modules, tmp_path, argv, mode):
    import os
    import subprocess
    dag = hp_modules.dag_utils_generic
    recorder = tmp_path / "record.py"
    capture = tmp_path / "argv.json"
    recorder.write_text("#!" + sys.executable + "\nimport json,sys\n"
                        "from pathlib import Path\nPath(" + repr(str(capture))
                        + ").write_text(json.dumps(sys.argv[1:]))\n")
    recorder.chmod(0o755)
    text = dag.quote_arguments(argv)
    raw = {"name": "command", "kind": COMMAND_V1, "exe": str(recorder)}
    if mode == "file":
        args = tmp_path / "args.txt"
        args.write_text("X " + text + "\n")
        raw["args_file"] = str(args)
    else:
        raw["args"] = text
    stage = _CONTRACT.TerminalStageSpec(raw, str(tmp_path))
    job, _ = dag.write_command_sub(exe=stage.exe, arg_str=stage.args,
                                  log_dir=str(tmp_path) + "/")
    script = tmp_path / "command.sh"
    dag.LocalBackend().emit_job(job, str(script))
    completed = subprocess.run(["bash", str(script)], env=os.environ.copy(),
                               capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert json.loads(capture.read_text()) == argv


def test_command_transfer_uris_remain_verbatim_and_paths_manifest_relative(tmp_path):
    uris = ["osdf:///ospool/test/pool.dat", "https://example.invalid/data",
            "custom+stage://host/path"]
    stage = _CONTRACT.TerminalStageSpec(
        {"name": "collect", "kind": COMMAND_V1, "exe": "/bin/true",
         "execution": {"transfer_files": uris + ["local.dat", "/absolute.dat"]}},
        str(tmp_path))
    assert stage.execution["transfer_files"] == uris + [
        str(tmp_path / "local.dat"), "/absolute.dat"]
    assert stage.execution["transfer_output_files"] == []


@pytest.mark.parametrize("transfer_policy", ["omitted", [], [
    "osdf:///ospool/test/pool.dat", "https://example.invalid/data",
    "custom+stage://host/path", "local.dat"]])
def test_generated_command_preserves_transfer_policy(
        hp_modules, rift_bin, tmp_path, monkeypatch, transfer_policy):
    import runpy
    import shutil
    import subprocess

    run = tmp_path / "run"
    run.mkdir()
    seed = run / "seed.dat"
    seed.write_text("# RIFT_HYPERPIPELINE_V1\n# lnL sigma_lnL m1 m2\n0 0 36 29\n")
    (run / "local.dat").write_text("fixture\n")
    worker = run / "candidate-command"
    worker.write_text("#!/bin/sh\nexit 0\n")
    worker.chmod(0o755)
    (run / "jobs.json").write_text(json.dumps([{
        "name": "build", "protocol": "indexed-grid-v1", "exe": "/bin/true",
        "execution": {"requires_data_inputs": False}}]))
    execution = {"use_osg": True}
    if transfer_policy != "omitted":
        execution["transfer_files"] = transfer_policy
    terminal = _write_manifest(run, [{
        "name": "collect", "kind": COMMAND_V1, "exe": str(worker),
        "execution": execution}])
    (run / "post.args").write_text("1 --parameter m1 --parameter m2\n")
    monkeypatch.chdir(run)
    monkeypatch.setenv("RIFT_DAG_BACKEND", "htcondor")
    monkeypatch.setenv("RIFT_HYPERPIPELINE_FORMAT", "1")
    monkeypatch.setenv("RIFT_REQUIRE_NONWORKER", "HAS_CVMFS_software_igwn_org")
    monkeypatch.setattr(sys, "argv", [
        str(rift_bin / "create_eos_posterior_pipeline"),
        "--working-directory", str(run), "--input-grid", str(seed),
        "--marg-job-spec-file", str(run / "jobs.json"),
        "--terminal-stage-spec-file", str(terminal),
        "--eos-post-args-list", str(run / "post.args"), "--eos-post-exe", "/bin/true",
        "--n-iterations", "1", "--n-samples-per-job", "1",
        "--eos-post-explode-jobs", "1", "--eos-post-explode-jobs-last", "1",
        "--use-full-submit-paths"])
    runpy.run_path(str(rift_bin / "create_eos_posterior_pipeline"), run_name="__main__")
    submit = run / "TERMINAL_collect.sub"

    def ads(path):
        return dict((k.strip().lower(), v.strip()) for k, v in (
            line.split("=", 1) for line in path.read_text().splitlines() if "=" in line))

    expected = ([] if transfer_policy in ("omitted", []) else
                transfer_policy[:-1] + [str(run / "local.dat")])
    # OSG command staging also appends its executable, including for no inputs.
    expected.append(str(worker))
    if expected:
        assert ads(submit)["transfer_input_files"] == ",".join(expected)
    else:
        assert "transfer_input_files" not in ads(submit)
    assert ads(submit)["transfer_output_files"] == ""
    condor_submit = shutil.which("condor_submit")
    if condor_submit:
        out = run / "command.classad"
        result = subprocess.run(
            [condor_submit, "-dry-run", str(out), str(submit)],
            capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stdout + result.stderr
        parsed = ads(out)
        if expected:
            assert parsed["transferinput"].strip('"') == ",".join(expected)
        else:
            assert "transferinput" not in parsed
