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
