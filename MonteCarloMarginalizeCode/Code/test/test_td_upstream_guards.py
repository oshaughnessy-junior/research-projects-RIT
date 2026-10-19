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
