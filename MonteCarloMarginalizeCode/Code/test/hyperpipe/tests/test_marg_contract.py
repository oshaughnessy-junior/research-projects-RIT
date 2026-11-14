"""Tests for the low-level hyperpipe MARG and iteration contracts."""
from __future__ import annotations

import json
import shlex
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
    assert shlex.split(spec.args) == [
        "--cache", "local.cache", "--adapt-weight-exponent", "0.1"]
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


@pytest.mark.parametrize("key, value, match", [
    ("transfer_files", "cache.dat", "transfer_files must be a list"),
    ("transfer_output_files", "result.dat",
     "transfer_output_files must be a list"),
    ("condor_commands", "request_cpus=2",
     "condor_commands must be an object"),
])
def test_marg_contract_rejects_malformed_execution_settings(
    hp_modules, tmp_path, key, value, match
):
    manifest = tmp_path / "marg_jobs.json"
    manifest.write_text(json.dumps([{
        "name": "bad",
        "protocol": "indexed-grid-v1",
        "exe": "/bin/true",
        "execution": {key: value},
    }]))

    with pytest.raises(ValueError, match=match):
        hp_modules.marg_contract.load_marg_job_specs(str(manifest))


def test_unknown_scalar_execution_key_is_preserved_as_default_command(
    hp_modules, tmp_path
):
    manifest = tmp_path / "marg_jobs.json"
    manifest.write_text(json.dumps([{
        "name": "open",
        "protocol": "indexed-grid-v1",
        "exe": "/bin/true",
        "execution": {"site_policy_knob": "required"},
    }]))

    with pytest.warns(UserWarning, match="preserving it"):
        spec = hp_modules.marg_contract.load_marg_job_specs(str(manifest))[0]

    assert spec.execution["schema"] == "rift-execution-v1"
    assert spec.execution["backend_commands"] == {
        "default": {"site_policy_knob": "required"}}


def test_open_backend_namespace_and_condor_alias_coexist(hp_modules, tmp_path):
    manifest = tmp_path / "marg_jobs.json"
    manifest.write_text(json.dumps([{
        "name": "open",
        "protocol": "indexed-grid-v1",
        "exe": "/bin/true",
        "execution": {
            "schema": "rift-execution-v1",
            "condor_commands": {"+PreCmd": "ile_pre.sh"},
            "backend_commands": {
                "htcondor": {"requirements": "HAS_SINGULARITY"},
                "future.batch": {"native_option": 7},
            },
        },
    }]))

    spec = hp_modules.marg_contract.load_marg_job_specs(str(manifest))[0]

    assert spec.execution["condor_commands"]["+PreCmd"] == "ile_pre.sh"
    assert spec.execution["backend_commands"]["future.batch"] == {
        "native_option": 7}


def test_unknown_structured_execution_key_requires_backend_namespace(
    hp_modules, tmp_path
):
    manifest = tmp_path / "marg_jobs.json"
    manifest.write_text(json.dumps([{
        "name": "bad",
        "protocol": "indexed-grid-v1",
        "exe": "/bin/true",
        "execution": {"site_policy": {"mode": "required"}},
    }]))

    with pytest.raises(ValueError, match="backend_commands"):
        hp_modules.marg_contract.load_marg_job_specs(str(manifest))


@pytest.mark.parametrize("kwargs, match", [
    ({"singularity_image": None, "cache_file": "cache", "transfer_files": []},
     "requires singularity_image"),
    ({"singularity_image": "/tmp/rift.sif", "transfer_files": []},
     "requires frames_dir or cache_file"),
    ({"singularity_image": "/tmp/rift.sif", "cache_file": "cache",
      "transfer_files": None}, "requires transfer_files"),
])
def test_ile_submit_builder_rejects_incomplete_singularity_configuration(
    hp_modules, kwargs, match
):
    with pytest.raises(ValueError, match=match):
        hp_modules.dag_utils_generic.write_ILE_sub_simple(
            exe="/bin/true", arg_str="--zero-likelihood",
            use_singularity=True, **kwargs)


def test_ile_submit_builder_allows_explicit_data_free_container(
    hp_modules, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SINGULARITY_BASE_EXE_DIR", "/usr/bin")
    job, _ = hp_modules.dag_utils_generic.write_ILE_sub_simple(
        tag="data_free", exe="/bin/true",
        arg_str="--zero-likelihood-data-free", use_singularity=True,
        singularity_image="/tmp/rift.sif", transfer_files=[],
        requires_data_inputs=False)
    job.set_sub_file(str(tmp_path / "data_free.sub"))
    job.write_sub_file()
    submit = (tmp_path / "data_free.sub").read_text()
    assert "frames_dir" not in submit
    assert "ile_pre.sh" not in submit


def test_ile_pre_transfer_survives_nested_initialdir(
    hp_modules, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SINGULARITY_BASE_EXE_DIR", "/container/bin")
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    (frames / "H1-test.gwf").write_bytes(b"")
    job, _ = hp_modules.dag_utils_generic.write_ILE_sub_simple(
        tag="nested", exe="/host/bin/ile", arg_str="--cache local.cache",
        use_singularity=True, singularity_image="/tmp/rift.sif",
        use_osg=True, frames_dir=str(frames), transfer_files=[],
        condor_commands={"+PreCmd": '"ile_pre.sh"'},
    )
    nested = tmp_path / "iteration_0_marg" / "event_0"
    nested.mkdir(parents=True)
    job.add_condor_cmd("initialdir", str(nested))
    job.set_sub_file(str(tmp_path / "nested.sub"))
    job.write_sub_file()

    submit = (tmp_path / "nested.sub").read_text()
    assert str(tmp_path / "ile_pre.sh") in submit
    assert "../ile_pre.sh" not in submit


def test_ile_submit_builder_can_stage_candidate_executable(
    hp_modules, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SINGULARITY_BASE_EXE_DIR", "/container/bin")
    worker = tmp_path / "candidate-worker"
    worker.write_text("#!/bin/sh\nexit 0\n")
    job, _ = hp_modules.dag_utils_generic.write_ILE_sub_simple(
        tag="staged", exe=str(worker), arg_str="--constant",
        use_singularity=True, singularity_image="/tmp/rift.sif",
        transfer_files=[], requires_data_inputs=False,
        transfer_executable=True)
    job.set_sub_file(str(tmp_path / "staged.sub"))
    job.write_sub_file()
    submit = (tmp_path / "staged.sub").read_text()
    assert "executable = ./candidate-worker" in submit
    assert "executable = /container/bin/candidate-worker" not in submit
    assert "transfer_executable = False" in submit
    assert str(worker) in submit


def test_container_executable_base_does_not_require_trailing_slash(
    hp_modules, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SINGULARITY_BASE_EXE_DIR", "/usr/bin")
    job, _ = hp_modules.dag_utils_generic.write_CIP_sub(
        tag="container_path", exe="/host/bin/cip-tool",
        arg_str="--parameter m1", input_net="all.net", output="posterior",
        out_dir=str(tmp_path), use_singularity=True,
        singularity_image="/tmp/rift.sif", transfer_files=["all.net"])
    job.set_sub_file(str(tmp_path / "container_path.sub"))
    job.write_sub_file()
    submit = (tmp_path / "container_path.sub").read_text()
    assert "executable = /usr/bin/cip-tool" in submit
    assert "executable = /usr/bincip-tool" not in submit


def test_hyperpost_submit_can_stage_candidate_executable(
    hp_modules, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SINGULARITY_BASE_EXE_DIR", "/container/bin")
    worker = tmp_path / "candidate-post"
    worker.write_text("#!/bin/sh\nexit 0\n")
    job, _ = hp_modules.dag_utils_generic.write_hyperpost_sub(
        tag="staged_post", exe=str(worker), input_net="all.marg_net",
        output="posterior", out_dir=str(tmp_path), arg_str="--no-plots",
        use_singularity=True, singularity_image="/tmp/rift.sif",
        transfer_files=["all.marg_net", "hyperpipeline_io.py"],
        transfer_executable=True)
    job.set_sub_file(str(tmp_path / "staged_post.sub"))
    job.write_sub_file()
    submit = (tmp_path / "staged_post.sub").read_text()
    assert "executable = ./candidate-post" in submit
    assert "executable = /container/bin/candidate-post" not in submit
    assert "transfer_executable = False" in submit
    assert str(worker) in submit
    assert "hyperpipeline_io.py" in submit


def test_hyperpost_stages_osdf_container_with_oauth(
    hp_modules, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SINGULARITY_BASE_EXE_DIR", "/usr/bin")
    job, _ = hp_modules.dag_utils_generic.write_hyperpost_sub(
        tag="hyperpost_osg", exe="/host/bin/post-tool",
        arg_str="--parameter m1", input_net="all.marg_net",
        output="posterior", out_dir=str(tmp_path), use_osg=True,
        use_singularity=True,
        singularity_image="osdf://example.invalid/rift/test.sif",
        use_oauth_files="scitokens", transfer_files=["all.marg_net"],
        request_disk="4G")
    job.set_sub_file(str(tmp_path / "hyperpost_osg.sub"))
    job.write_sub_file()
    submit = (tmp_path / "hyperpost_osg.sub").read_text()
    assert "executable = /usr/bin/post-tool" in submit
    assert 'MY.SingularityImage = "./test.sif"' in submit
    assert "use_oauth_services = scitokens" in submit
    assert "osdf://example.invalid/rift/test.sif" in submit
    assert "request_disk = 4G" in submit


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


def test_iteration_schedule_can_preserve_z_marker_for_capable_writer(
    hp_modules,
):
    assert hp_modules.cip_pipeline.expand_argument_schedule(
        ["2 --sampler-method GMM", "Z --sampler-method AV"], 3,
        allow_special=True, include_prefix=True,
    ) == [
        ("2", "--sampler-method GMM"),
        ("2", "--sampler-method GMM"),
        ("Z", "--sampler-method AV"),
    ]


@pytest.mark.parametrize("body, expected", [
    ("--cache plain.xml", ["--cache", "plain.xml"]),
    (r"--cache /data/a\ b.xml", ["--cache", "/data/a b.xml"]),
    ("--psd-file 'H1:/data/PSD  v1.xml'", ["--psd-file", "H1:/data/PSD  v1.xml"]),
    ('--label ' + shlex.quote(r'{"value": "a\\b"}'), ["--label", r'{"value": "a\\b"}']),
    (r"--label 'literal\path'", ["--label", r"literal\path"]),
    ("--empty ''", ["--empty", ""]),
    ("--label 'X'", ["--label", "X"]),
    ("--label a" + "\\\n" + "b", ["--label", "ab"]),
    ('--label "a' + "\\\n" + 'b"', ["--label", "ab"]),
    ("--cache a.xml\n --flag", ["--cache", "a.xml", "--flag"]),
    ("", []),
])
@pytest.mark.parametrize("dummy", ["", "X "])
def test_marg_args_file_preserves_shell_argv(hp_modules, tmp_path, body, expected, dummy):
    path = tmp_path / "args.txt"
    path.write_text(dummy + body + "\n")
    loaded = hp_modules.marg_contract.MargJobSpec(
        {"exe": "/bin/true", "args_file": str(path)}, str(tmp_path))
    assert shlex.split(loaded.args) == expected
    # Inline input has the same argv after shell continuation folding.
    inline_body = body.replace("\\\n", "")
    inline = hp_modules.marg_contract.MargJobSpec(
        {"exe": "/bin/true", "args": inline_body}, str(tmp_path))
    assert shlex.split(inline.args) == expected


def test_marg_args_file_preserves_backslash_newline_in_single_quotes(hp_modules, tmp_path):
    path = tmp_path / "args.txt"
    body = "--label 'a" + "\\\n" + "b'"
    path.write_text("X " + body)
    loaded = hp_modules.marg_contract.MargJobSpec(
        {"exe": "/bin/true", "args_file": str(path)}, str(tmp_path))
    assert loaded.args == body


@pytest.mark.parametrize("value, expected", [
    ("input.dat", ["input.dat"]),
    ("a.dat,b.dat", ["a.dat", "b.dat"]),
    (["a.dat", "b.dat"], ["a.dat", "b.dat"]),
])
@pytest.mark.parametrize("stage_candidate", [False, True])
def test_hyperpost_preserves_transfer_input_entries(
    hp_modules, tmp_path, value, expected, stage_candidate
):
    original = value[:] if isinstance(value, list) else value
    image = "osdf://example.invalid/rift/test.sif"
    worker = str(tmp_path / "candidate-post")
    job, _ = hp_modules.dag_utils_generic.write_hyperpost_sub(
        exe=worker, arg_str="--parameter x", out_dir=str(tmp_path),
        use_singularity=True, singularity_image=image,
        transfer_files=value, transfer_executable=stage_candidate)
    commands = dict(job.condor_cmds)
    wanted = expected + [image] + ([worker] if stage_candidate else [])
    assert commands["transfer_input_files"].split(",") == wanted
    assert commands["should_transfer_files"] == "YES"
    assert value == original


@pytest.mark.parametrize("value", [None, []])
def test_hyperpost_unspecified_transfer_inputs_keep_default(hp_modules, tmp_path, value):
    job, _ = hp_modules.dag_utils_generic.write_hyperpost_sub(
        exe="/bin/true", arg_str="--parameter x", out_dir=str(tmp_path),
        transfer_files=value)
    assert "transfer_input_files" not in dict(job.condor_cmds)


@pytest.mark.parametrize("text, expected", [
    ("X", []),
    ("--label a\u00a0b", ["--label", "a\u00a0b"]),
    ("'X' --flag", ["X", "--flag"]),
    (r"\X --flag", ["X", "--flag"]),
    ("--label abc" + "\\\\\n" + " --flag", ["--label", "abc\\", "--flag"]),
    ("--label abc\\ ", ["--label", "abc "]),
])
def test_marg_args_file_token_boundaries(hp_modules, tmp_path, text, expected):
    path = tmp_path / "args.txt"
    path.write_text(text)
    loaded = hp_modules.marg_contract.MargJobSpec(
        {"exe": "/bin/true", "args_file": str(path)}, str(tmp_path))
    assert shlex.split(loaded.args) == expected


def test_hyperpost_preserves_raw_condor_quoted_commas(hp_modules, tmp_path):
    raw = '\"a,b.dat\",https://example.invalid/data.dat'
    image = "osdf://example.invalid/rift/test.sif"
    worker = str(tmp_path / "candidate-post")
    job, _ = hp_modules.dag_utils_generic.write_hyperpost_sub(
        exe=worker, arg_str="--parameter x", out_dir=str(tmp_path),
        use_singularity=True, singularity_image=image,
        transfer_files=raw, transfer_executable=True)
    assert dict(job.condor_cmds)["transfer_input_files"] == ",".join([raw, image, worker])


@pytest.mark.parametrize("body, expected", [
    (r"--probe /data/a\ b.xml", ["--probe", "/data/a b.xml"]),
    ("--probe 'a  b'", ["--probe", "a  b"]),
    (r"--probe 'literal\path'", ["--probe", r"literal\path"]),
    ("--probe abc\\ ", ["--probe", "abc "]),
    ("--probe ''", ["--probe", ""]),
])
@pytest.mark.parametrize("mode", ["inline", "file", "append"])
def test_indexed_args_preserve_actual_worker_argv(hp_modules, tmp_path, body, expected, mode):
    import importlib
    import os
    import subprocess
    import sys
    dag = hp_modules.dag_utils_generic
    recorder = tmp_path / "record.py"
    capture = tmp_path / "argv.json"
    recorder.write_text("#!" + sys.executable + "\nimport json,sys\n"
                        "from pathlib import Path\nPath(" + repr(str(capture))
                        + ").write_text(json.dumps(sys.argv[1:]))\n")
    recorder.chmod(0o755)
    raw = {"exe": str(recorder), "protocol": "indexed-grid-v1"}
    if mode == "file":
        args = tmp_path / "args.txt"
        args.write_text("X " + body + "\n")
        raw["args_file"] = str(args)
    elif mode == "inline":
        raw["args"] = body
    else:
        terminal = importlib.import_module("RIFT.hyperpipe.terminal_contract")
        stage = terminal.TerminalStageSpec(
            {"name": "samples", "kind": terminal.INDEXED_GRID_FANOUT_V1,
             "job": raw, "grid": "grid.dat", "args_append": body}, str(tmp_path))
        raw = None
    if raw is not None:
        spec = hp_modules.marg_contract.MargJobSpec(raw, str(tmp_path))
        text = spec.args
    else:
        spec = stage.job
        text = spec.args + " " + stage.args_append
    job, _ = dag.write_ILE_sub_simple(
        exe=spec.exe, arg_str=text, log_dir=str(tmp_path) + "/")
    script = tmp_path / "worker.sh"
    dag.LocalBackend().emit_job(job, str(script))
    env = os.environ.copy()
    env["RIFT_VAR_MACROEVENT"] = "0"
    completed = subprocess.run(["bash", str(script)], env=env,
                               capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert json.loads(capture.read_text()) == expected + ["--event=0"]
