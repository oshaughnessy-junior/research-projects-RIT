"""The Hyperpipe route truncates frames before any ILE.sub exists, so the
frame helper must read its data interval and channels from args_ile.txt in
both shapes it meets: the raw ``X --option ...`` record current pseudo_pipe
writes, and the older ``arguments = ...`` wrapper.

PR #181 pinned this against the shell implementation.  rift_O4d replaced that
with util_ForOSG_MakeTruncatedLocalFramesDir.py (the .sh is now a thin exec
wrapper), so this exercises the real helper on a real frame.
"""
import os
from pathlib import Path
import subprocess

import numpy as np
import pytest

pytest.importorskip("gwpy")
pytest.importorskip("lalframe")

BIN = Path(__file__).resolve().parents[3] / "bin"
SCRIPT = BIN / "util_ForOSG_MakeTruncatedLocalFramesDir.sh"


@pytest.mark.parametrize(
    "args_record",
    [
        "X --data-start-time 100.25 --data-end-time 164.25 "
        "--channel-name H1=FAKE-STRAIN\n",
        "arguments = --data-start-time 100.25 --data-end-time 164.25 "
        "--channel-name H1=FAKE-STRAIN\n",
    ],
)
def test_truncation_helper_accepts_raw_and_legacy_ile_args(tmp_path, args_record):
    from gwpy.timeseries import TimeSeries

    frame = tmp_path / "H-FAKE-0-256.gwf"
    TimeSeries(np.arange(0, 256 * 16, dtype=float), t0=0, sample_rate=16,
               channel="H1:FAKE-STRAIN", name="H1:FAKE-STRAIN").write(str(frame))
    (tmp_path / "args_ile.txt").write_text(args_record)
    (tmp_path / "local.cache").write_text(
        "H FAKE 0 256 {}\n".format(frame.as_uri()))

    env = os.environ.copy()
    env["PATH"] = str(BIN) + os.pathsep + env["PATH"]
    result = subprocess.run(["bash", str(SCRIPT), str(tmp_path)], env=env,
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr

    # floor(100.25)-1 = 99, ceil(164.25)+1 = 166 -> 67 s.
    staged = list((tmp_path / "frames_dir").glob("H1-FAKE_STRAIN-99-67.gwf"))
    assert staged, sorted(p.name for p in (tmp_path / "frames_dir").iterdir())
    rows = (tmp_path / "local.cache").read_text().split()
    assert rows[:4] == ["H1", "FAKE_STRAIN", "99", "67"]
