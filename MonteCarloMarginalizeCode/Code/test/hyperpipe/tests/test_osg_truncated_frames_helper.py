import os
from pathlib import Path
import subprocess

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "bin"
    / "util_ForOSG_MakeTruncatedLocalFramesDir.sh"
)


@pytest.mark.parametrize(
    "args_record",
    [
        (
            "X --data-start-time 100.25 --data-end-time 164.25 "
            "--channel-name H1=FAKE-STRAIN\n"
        ),
        (
            "arguments = --data-start-time 100.25 --data-end-time 164.25 "
            "--channel-name H1=FAKE-STRAIN\n"
        ),
    ],
)
def test_truncation_helper_accepts_raw_and_legacy_ile_args(tmp_path, args_record):
    (tmp_path / "args_ile.txt").write_text(args_record)
    (tmp_path / "local.cache").write_text(
        "H H1_TEST 0 4096 file://localhost/input.gwf\n"
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    switcheroo = bin_dir / "switcheroo"
    switcheroo.write_text(
        "#!/bin/sh\n"
        "sed -i 's#file://localhost##g' \"$3\"\n"
    )
    switcheroo.chmod(0o755)
    truncate = bin_dir / "util_TruncateMergeFrames.py"
    truncate.write_text(
        "#!/bin/sh\n"
        "while [ $# -gt 0 ]; do\n"
        "  if [ \"$1\" = --output ]; then shift; touch \"$1\"; exit 0; fi\n"
        "  shift\n"
        "done\n"
        "exit 1\n"
    )
    truncate.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = str(bin_dir) + os.pathsep + env["PATH"]
    subprocess.run(["bash", str(SCRIPT), str(tmp_path)], check=True, env=env)

    assert "data-start-time 100.25" in (tmp_path / "my_time_args").read_text()
    assert (tmp_path / "my_channel_pairs").read_text().strip() == "H1 FAKE-STRAIN"
    assert list((tmp_path / "frames_dir").glob("H1-FAKE_STRAIN-99-66.gwf"))
