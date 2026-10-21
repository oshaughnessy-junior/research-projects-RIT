"""New upstream FD-only paths must refuse incompatible TD requests at startup."""
import os
from pathlib import Path
import subprocess
import sys
import pytest


@pytest.mark.parametrize("flags,message", [
    (["--rotation-slow"], "td-toeplitz does not support --rotation-slow"),
    (["--freqresponse"], "td-toeplitz does not support --rotation-slow"),
    (["--log-noise-evidence-only"], "td-toeplitz does not support FD"),
    (["--log-noise-evidence-output", "noise.json"], "td-toeplitz does not support FD"),
])
def test_refuse_fd_only_path_with_td(flags, message, tmp_path):
    code = Path(__file__).resolve().parents[1]
    env = dict(os.environ, GW_SURROGATE="", OMP_NUM_THREADS="1",
               OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1")
    env["PYTHONPATH"] = str(code)
    proc = subprocess.run([sys.executable, str(code / "bin/integrate_likelihood_extrinsic_batchmode"),
                           "--internal-ile-inner-product", "td-toeplitz", *flags],
                          cwd=tmp_path, env=env, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=120)
    assert proc.returncode != 0
    assert message in proc.stdout, proc.stdout[-3000:]
    assert not (tmp_path / "noise.json").exists()
    assert not (tmp_path / "log_noise_evidence.json").exists()


@pytest.mark.parametrize("flags", [
    ["--td-analysis-start", "0"], ["--td-analysis-end", ".1"],
    ["--td-gap", "-.1,.1"], ["--td-acf-duration", "1"],
    ["--td-solver", "pcg"], ["--glitch-basis", "H1:0,.1,40,100"],
    ["--glitch-basis-file", "H1:unused.npz"],
])
def test_td_requests_are_not_ignored_by_fd(flags, tmp_path):
    code = Path(__file__).resolve().parents[1]
    env = dict(os.environ, GW_SURROGATE="", OMP_NUM_THREADS="1",
               OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", PYTHONPATH=str(code))
    proc = subprocess.run([sys.executable, str(code / "bin/integrate_likelihood_extrinsic_batchmode"),
                           *flags], cwd=tmp_path, env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
    assert proc.returncode != 0
    assert "--internal-ile-inner-product td-toeplitz" in proc.stdout, proc.stdout[-3000:]


@pytest.mark.parametrize("same_source,osg,existing_aux,refused", [
    (False, True, False, True),
    (True, True, False, False),
    (False, True, True, True),
    (False, False, False, False),
])
def test_pipeline_basis_transfer_names(tmp_path, same_source, osg, existing_aux, refused):
    """Execute the actual forwarding block without launching a pipeline."""
    import ast
    from types import SimpleNamespace
    code = Path(__file__).resolve().parents[1]
    tree = ast.parse((code / "bin/util_RIFT_pseudo_pipe.py").read_text())
    helper = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_as_list")
    block = next(n for n in tree.body if isinstance(n, ast.If)
                 and isinstance(n.test, ast.Attribute) and n.test.attr == "internal_ile_inner_product")
    paths = []
    for detector in ("H1", "L1"):
        path = tmp_path / detector / "basis.npz"
        path.parent.mkdir()
        path.touch()
        paths.append(path)
    if same_source:
        paths[1] = paths[0]
    opts = SimpleNamespace(internal_ile_inner_product="td-toeplitz",
        internal_ile_td_solver=None, internal_ile_td_acf_duration=None,
        internal_ile_td_analysis_start=None, internal_ile_td_analysis_end=None,
        internal_ile_td_gap=None, internal_ile_glitch_basis=None,
        internal_ile_glitch_sigma=None, use_osg=osg,
        internal_ile_td_mref=None, internal_ile_td_piece=None,
        internal_ile_td_ifo_time_offset=None, internal_ile_td_acf_file=None,
        internal_ile_td_dense_max_n=None, internal_ile_td_u_stride=None,
        internal_ile_td_u_method=None, internal_ile_td_acf_cond=None,
        internal_ile_td_whiteness_check=False,
        ile_additional_files_to_transfer=str(paths[0]) if existing_aux else None,
        internal_ile_glitch_basis_file=(["L1:" + str(paths[1])] if existing_aux else
                                      [ifo + ":" + str(path) for ifo, path in zip(("H1", "L1"), paths)]))
    namespace = dict(opts=opts, os=os, sys=sys, line="")
    module = ast.fix_missing_locations(ast.Module(body=[helper, block], type_ignores=[]))
    if refused:
        with pytest.raises(SystemExit) as exc:
            exec(compile(module, "pipeline_td_forwarding", "exec"), namespace)
        assert exc.value.code == 1
    else:
        exec(compile(module, "pipeline_td_forwarding", "exec"), namespace)
        assert "--glitch-basis-file" in namespace["line"]
