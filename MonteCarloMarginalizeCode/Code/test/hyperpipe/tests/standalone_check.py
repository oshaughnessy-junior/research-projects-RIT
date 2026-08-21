"""
Standalone (no-pytest) smoke test --- useful for ``pixi run test-minimal``.

Verifies the same things as the pytest suite, but runs as a regular
script so it's easy to invoke from a make rule or a CI job that doesn't
want a pytest dependency.

Locates RIFT_ROOT by:
  1. honoring the ``$RIFT_ROOT`` env var if set;
  2. otherwise walking up from this file's location
     (``parents[5]`` of the canonical
     ``test/hyperpipe/tests/standalone_check.py`` path is the RIFT root).
"""
from __future__ import annotations

import importlib.util
import json
import os
import runpy
import sys
import tempfile
import types
from pathlib import Path


_HERE = Path(__file__).resolve()


def _is_rift_root(p: Path) -> bool:
    return (p / "MonteCarloMarginalizeCode" / "Code" / "RIFT" / "hyperpipe").exists()


def _rift_root() -> Path:
    env = os.environ.get("RIFT_ROOT")
    if env:
        p = Path(env).resolve()
        if _is_rift_root(p):
            return p
    if len(_HERE.parents) > 5:
        candidate = _HERE.parents[5]
        if _is_rift_root(candidate):
            return candidate
    raise SystemExit(
        "Could not locate RIFT root. Set $RIFT_ROOT or run from "
        "$RIFT_ROOT/MonteCarloMarginalizeCode/Code/test/hyperpipe/tests/."
    )


RIFT_ROOT = _rift_root()
HP = RIFT_ROOT / "MonteCarloMarginalizeCode" / "Code" / "RIFT" / "hyperpipe"
RIFT_PY = RIFT_ROOT / "MonteCarloMarginalizeCode" / "Code"

# Bypass RIFT/__init__.py so we don't need the full lalsuite stack to run
# the smoke test --- this script is intended to be runnable from any
# Python env that has numpy.
fake_rift = types.ModuleType("RIFT")
fake_rift.__path__ = [str(RIFT_PY / "RIFT")]
sys.modules["RIFT"] = fake_rift
fake_hp = types.ModuleType("RIFT.hyperpipe")
fake_hp.__path__ = [str(HP)]
sys.modules["RIFT.hyperpipe"] = fake_hp
fake_misc = types.ModuleType("RIFT.misc")
fake_misc.__path__ = [str(RIFT_PY / "RIFT" / "misc")]
sys.modules["RIFT.misc"] = fake_misc


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


coords = _load("RIFT.hyperpipe.coords", HP / "coords.py")
config = _load("RIFT.hyperpipe.config", HP / "config.py")
marg_list = _load("RIFT.hyperpipe.marg_list", HP / "marg_list.py")
marg_contract = _load("RIFT.hyperpipe.marg_contract", HP / "marg_contract.py")
cip_pipeline = _load(
    "RIFT.misc.cip_pipeline", RIFT_PY / "RIFT" / "misc" / "cip_pipeline.py"
)
drivers_base = _load("RIFT.hyperpipe.drivers.base", HP / "drivers" / "base.py")


def _check_pipeline_render(terminal_evidence: bool = False,
                           terminal_stages: bool = False) -> None:
    """Render a tiny indexed-grid DAG without importing the LAL stack."""
    with tempfile.TemporaryDirectory(prefix="rift-hyperpipe-render-") as tmpd:
        run = Path(tmpd)
        (run / "grid.dat").write_text("# lnL sigma_lnL m1 m2\n0 0 30 20\n")
        (run / "args_post.txt").write_text(
            "2 --parameter m1 --parameter m2\n")
        (run / "args_ile.txt").write_text("X --cache local.cache\n")
        (run / "local.cache").write_text("cache\n")
        (run / "jobs.json").write_text(json.dumps([{
            "name": "ile",
            "protocol": "indexed-grid-v1",
            "exe": "/bin/true",
            "args_file": "args_ile.txt",
            "n_chunk": 2,
            "execution": {
                "cache_file": "local.cache",
                "request_memory": 4096,
                "request_disk": "2G",
                "retries": 3,
            },
        }]))
        if terminal_stages:
            (run / "terminal").mkdir()
            (run / "terminal.json").write_text(json.dumps({
                "version": 1,
                "stages": [
                    {
                        "name": "samples",
                        "kind": "indexed-grid-fanout-v1",
                        "job": {
                            "protocol": "indexed-grid-v1",
                            "exe": "/bin/true",
                            "args": "--terminal-worker",
                            "n_chunk": 2,
                            "execution": {
                                "request_memory": 6144,
                                "request_disk": "3G",
                                "retries": 4,
                            },
                        },
                        "grid": str(run / "grid-$(macroiteration).dat"),
                        "output_file": "EXTR_out.xml",
                        "fanout": {"count": 3, "group_size": 2},
                        "initial_dir": str(run / "terminal"),
                        "log_dir": str(run / "terminal"),
                    },
                    {
                        "name": "collect",
                        "kind": "command-v1",
                        "depends_on": ["samples"],
                        "exe": "/bin/true",
                        "args": "--collect $(macroiteration)",
                        "initial_dir": str(run),
                    },
                ],
            }))
        old_cwd = os.getcwd()
        old_argv = sys.argv[:]
        try:
            os.chdir(run)
            sys.argv = [
                "create_eos_posterior_pipeline",
                "--working-directory", str(run),
                "--input-grid", str(run / "grid.dat"),
                "--marg-job-spec-file", str(run / "jobs.json"),
                "--eos-post-args-list", str(run / "args_post.txt"),
                "--eos-post-exe", "/bin/true",
                "--n-iterations", "2",
                "--n-samples-per-job", "4",
                "--eos-post-explode-jobs", "1",
                "--eos-post-explode-jobs-last", "1",
                "--use-full-submit-paths",
            ]
            if terminal_evidence:
                sys.argv.append("--terminal-evidence")
            if terminal_stages:
                sys.argv.extend([
                    "--terminal-stage-spec-file", str(run / "terminal.json")])
            runpy.run_path(
                str(RIFT_PY / "bin" / "create_eos_posterior_pipeline"),
                run_name="__main__",
            )
        finally:
            sys.argv = old_argv
            os.chdir(old_cwd)

        dag = (run / "marginalize_hyperparameters.dag").read_text()
        sub = (run / "MARG_0.sub").read_text()
        consolidator = (run / "con_marg_0.sh").read_text()
        assert "macrongroup=\"2\"" in dag
        assert "RETRY" in dag and " 3" in dag
        assert "PARENT " in dag and " CHILD " in dag
        assert "--sim-grid" in sub
        assert sub.count("--event=$(macroevent)") == 1
        assert "--n-events-to-analyze $(macrongroup)" in sub
        assert "--output-file" in sub
        assert "request_memory = 4096" in sub
        assert "request_disk = 2G" in sub
        assert "util_CleanILE_hyperpipeline.py" in consolidator
        if terminal_evidence:
            prior_sub = (run / "EOS_POST_prior.sub").read_text()
            evidence_sub = (run / "evidence_final.sub").read_text()
            assert "--integrate-prior" in prior_sub
            assert "--n-output-samples 1" in prior_sub
            assert "prior-integral-$(macroiteration)" in prior_sub
            assert "--strict" in evidence_sub
            assert "--annotation-glob" in evidence_sub
            assert "output-$(macroiterationnext)-*+annotation.dat" in evidence_sub
            assert ("prior-integral-$(macroiteration)_withpriorchange+annotation.dat"
                    in evidence_sub)
            assert "evidence_$(macroiteration)_normalized" in evidence_sub
            assert "EOS_POST_prior.sub" in dag
            assert "evidence_final.sub" in dag
            assert "CATEGORY" in dag and "CIP_PRIOR" in dag
            assert "CATEGORY" in dag and "EVIDENCE" in dag
        else:
            assert not (run / "EOS_POST_prior.sub").exists()
            assert not (run / "evidence_final.sub").exists()
            assert "EOS_POST_prior.sub" not in dag
            assert "evidence_final.sub" not in dag
        if terminal_stages:
            terminal_sub = (run / "TERMINAL_samples.sub").read_text()
            collect_sub = (run / "TERMINAL_collect.sub").read_text()
            assert "--terminal-worker" in terminal_sub
            assert "--sim-grid" in terminal_sub
            assert "grid-$(macroiteration).dat" in terminal_sub
            assert "--n-events-to-analyze $(macrongroup)" in terminal_sub
            assert "request_memory = 6144M" in terminal_sub
            assert "request_disk = 3G" in terminal_sub
            assert "--collect $(macroiteration)" in collect_sub
            assert dag.count("TERMINAL_samples.sub") == 3
            assert dag.count("macrongroup=\"2\"") >= 3
            assert "TERMINAL_collect.sub" in dag
            assert "TERMINAL_samples" in dag
            assert "TERMINAL_collect" in dag
            jobs = {}
            edges = set()
            for line in dag.splitlines():
                fields = line.split()
                if fields and fields[0] == "JOB":
                    jobs.setdefault(Path(fields[2]).name, []).append(fields[1])
                elif fields and fields[0] == "PARENT":
                    split_at = fields.index("CHILD")
                    for parent in fields[1:split_at]:
                        for child in fields[split_at + 1:]:
                            edges.add((parent, child))
            final_join = jobs["JOIN_POST.sub"][-1]
            workers = jobs["TERMINAL_samples.sub"]
            collector = jobs["TERMINAL_collect.sub"][0]
            assert len(workers) == 3
            assert all((final_join, worker) in edges for worker in workers)
            assert all((worker, collector) in edges for worker in workers)
        else:
            assert not (run / "TERMINAL_samples.sub").exists()
            assert not (run / "TERMINAL_collect.sub").exists()


def run_checks() -> None:
    # 1. coord-spec
    spec = coords.HyperCoordSpec.from_strings(
        coords_fit="x y z",
        coords_sample="x:[-8,8] y:[-8,8] z:[-8,8]",
    )
    spec.validate(strict_import=False)
    assert "--integration-parameter-range x:[-8,8]" in spec.to_post_args()

    # 2. mono marg-list assembly
    with tempfile.TemporaryDirectory() as tmpd:
        base = Path(tmpd) / "base"
        run = Path(tmpd) / "run"
        base.mkdir()
        run.mkdir()
        (base / "example.py").write_text("#!/usr/bin/env python\n")
        os.chmod(base / "example.py", 0o755)
        cfg = {
            "marg-list": [
                {"name": "g", "exe": "example.py", "args": "--ok",
                 "event-file": None, "n-chunk": 100, "coord-module": None}
            ]
        }
        m = marg_list.assemble_marg_list(cfg, base_dir=str(base), run_dir=str(run))
        assert m.n_chunks == [100]
        assert (run / "event-0.net").read_text().strip() == "empty_event_file"

    # 3. validate_config rejects empty config
    try:
        config.validate_config({})
    except ValueError:
        pass
    else:
        raise AssertionError("validate_config({}) should have raised")

    # 4. generic indexed-grid MARG contract and posterior schedule
    with tempfile.TemporaryDirectory() as tmpd:
        base = Path(tmpd)
        exe = base / "fake_ile"
        exe.write_text("#!/bin/sh\n")
        os.chmod(exe, 0o755)
        (base / "args_ile.txt").write_text("X --cache local.cache\n")
        (base / "jobs.json").write_text(
            '[{"name":"ile","protocol":"indexed-grid-v1",'
            '"exe":"fake_ile","args_file":"args_ile.txt","n_chunk":4}]'
        )
        jobs = marg_contract.load_marg_job_specs(str(base / "jobs.json"))
        assert jobs[0].exe == str(exe)
        assert jobs[0].args == "--cache local.cache"
        assert jobs[0].result_glob == "MARG*.dat"
    assert cip_pipeline.expand_argument_schedule(
        ["2 --sampler-method GMM", "1 --sampler-method AV"], 4
    ) == [
        "--sampler-method GMM",
        "--sampler-method GMM",
        "--sampler-method AV",
        "--sampler-method AV",
    ]
    try:
        cip_pipeline.expand_argument_schedule(["G2 --sampler-method GMM"], 2)
    except ValueError:
        pass
    else:
        raise AssertionError("unsupported special schedules must fail explicitly")

    # 5. base driver round-trip
    with tempfile.TemporaryDirectory() as tmpd:
        grid = Path(tmpd) / "g.dat"
        grid.write_text("# lnL sigma_lnL x y z\n0 0 1.0 2.0 3.0\n")
        rows, cols = drivers_base.read_grid(f"file:{grid}")
        assert cols == ["x", "y", "z"]
        rows[0, 0] = "-3.1415926535"
        out = drivers_base.write_marg_output(
            rows, cols,
            fname_output_integral="f.txt",
            outdir=str(Path(tmpd) / "out"),
            fname=None,
            conforming_output_name=True,
        )
        assert "-3.1415926535" in Path(out).read_text()

    # 6. build-only integration: preserve the legacy render, then opt in to
    # terminal prior-normalized evidence using the same low-level writer.
    _check_pipeline_render()
    _check_pipeline_render(terminal_evidence=True)
    _check_pipeline_render(terminal_stages=True)

    print(f"standalone_check: ALL OK  (RIFT_ROOT={RIFT_ROOT})")


if __name__ == "__main__":
    run_checks()
