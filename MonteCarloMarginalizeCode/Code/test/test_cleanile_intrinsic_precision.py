"""Keep narrow BNS masses distinct through both standard RIFT cleaner passes."""

import os
import subprocess
import sys
from pathlib import Path

import numpy as np

CODE = Path(__file__).resolve().parents[1]
BIN = CODE / "bin"
CLEAN = BIN / "util_CleanILE.py"
BUILDER = BIN / "create_event_parameter_pipeline_BasicIteration"


def env():
    result = dict(os.environ)
    result["PYTHONPATH"] = str(CODE) + os.pathsep + result.get("PYTHONPATH", "")
    result["PATH"] = str(BIN) + os.pathsep + result.get("PATH", "")
    result["GW_SURROGATE"] = ""
    return result


def test_cleaner_keeps_narrow_mass_grid_when_requested(tmp_path):
    rows = np.array([
        [-1, 1.2292711, 1.25, 0, 0, 0, 0, 0, 0, 10, .01, 100, 50],
        [-1, 1.2292722, 1.25, 0, 0, 0, 0, 0, 0, 11, .01, 100, 50],
    ])
    path = tmp_path / "ile.dat"
    np.savetxt(path, rows, fmt="%.17g")

    def run(*options):
        proc = subprocess.run([sys.executable, str(CLEAN), *options, str(path)],
                              cwd=tmp_path, env=env(), text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert proc.returncode == 0, proc.stderr
        return [line.split() for line in proc.stdout.splitlines() if line.strip()]

    assert len(run()) == 1  # historical five-decimal coalescing
    precise = run("--intrinsic-digits", "12")
    assert len(precise) == 2
    assert sorted(float(row[1]) for row in precise) == [1.2292711, 1.2292722]


def test_basic_builder_applies_precision_to_join_and_unify(tmp_path):
    import lal
    import RIFT.lalsimutils as lsu

    point = lsu.ChooseWaveformParams()
    point.m1, point.m2 = 35 * lal.MSUN_SI, 30 * lal.MSUN_SI
    previous = Path.cwd()
    os.chdir(tmp_path)
    try:
        lsu.ChooseWaveformParams_array_to_xml([point, point], "grid")
    finally:
        os.chdir(previous)
    (tmp_path / "args_ile.txt").write_text(
        "integrate_likelihood_extrinsic_batchmode --time-marginalization "
        "--vectorized --gpu --srate 4096 --n-eff 50\n")
    (tmp_path / "args_cip_list.txt").write_text(
        "2 --parameter mc --parameter delta_mc --n-output-samples 5000\n")
    (tmp_path / "args_test.txt").write_text("X --always-succeed\n")
    command = [
        sys.executable, str(BUILDER),
        "--ile-n-events-to-analyze", "1", "--input-grid", str(tmp_path / "grid.xml.gz"),
        "--ile-exe", str(BIN / "integrate_likelihood_extrinsic_batchmode"),
        "--ile-args", str(tmp_path / "args_ile.txt"),
        "--cip-args-list", str(tmp_path / "args_cip_list.txt"),
        "--test-args", str(tmp_path / "args_test.txt"),
        "--working-directory", str(tmp_path), "--n-iterations", "2",
        "--n-samples-per-job", "500", "--last-iteration-extrinsic",
        "--last-iteration-extrinsic-samples-per-ile", "200",
        "--clean-ile-intrinsic-digits", "12",
    ]
    proc = subprocess.run(command, cwd=tmp_path, env=env(), text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert proc.returncode == 0, proc.stdout[-4000:]
    assert "--intrinsic-digits 12" in (tmp_path / "join.sub").read_text()
    assert "--intrinsic-digits 12" in (tmp_path / "unify.sh").read_text()
