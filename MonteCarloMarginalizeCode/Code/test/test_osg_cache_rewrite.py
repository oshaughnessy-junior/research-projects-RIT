"""Regression tests for the OSG frame-cache rewrite (PR #181 item A13).

`--use-osg-file-transfer --internal-truncate-files-for-osg-file-transfer` is in
the standard OSG recipe, and until PR #181 the shell it ran was broken in two
independent ways:

    cat local.cache > awk '{print $1, $2, $3, $4}' > local_stripped.cache
    for i in `ls frames_dir/*.gwf`; do echo frames_local/${i} ; done > base_paths.dat

The first redirects `cat` into a file named `awk`, hands the awk program to
`cat` as a missing filename, and leaves every column in place -- so the pasted
cache still carried the submit-host path.  The second doubled the prefix into
`frames_local/frames_dir/<name>.gwf`.  `os.system` discards the exit status, so
both failed silently and the damage only appeared on a worker.

These tests pin the repaired behaviour, and `test_legacy_shell_was_broken`
executes the ORIGINAL shell so the claim above is demonstrated rather than
asserted.
"""

import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))

from RIFT.misc.hyperpipeline_io import rewrite_cache_for_worker_transfer


def _make_run_dir(tmp_path, n=3):
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    cache = tmp_path / "local.cache"
    lines = []
    for index in range(n):
        name = "H-H1_HOFT_C00-{}-4096.gwf".format(1000000000 + index * 4096)
        (frames / name).write_text("frame")
        lines.append("H H1_HOFT_C00 {} 4096 /submit/host/path/{}".format(
            1000000000 + index * 4096, name))
    cache.write_text("\n".join(lines) + "\n")
    return cache, frames


def test_cache_names_the_path_the_worker_will_see(tmp_path, monkeypatch):
    """pseudo_pipe runs in the run directory and passes RELATIVE names.

    The helper preserves the caller's spelling of frames_dir, because that
    spelling is what determines the path inside the job sandbox: pass
    'frames_dir' and the cache names 'frames_dir/<frame>.gwf', which is where
    file transfer puts it.
    """
    _make_run_dir(tmp_path)
    monkeypatch.chdir(tmp_path)
    cache = tmp_path / "local.cache"
    lines = rewrite_cache_for_worker_transfer(
        "local.cache", "frames_dir", backup_path="local_orig.cache")

    assert len(lines) == 3
    for line in cache.read_text().splitlines():
        fields = line.split()
        assert len(fields) == 5, "cache line must keep its 5 columns: " + line
        assert fields[4].startswith("frames_dir/"), fields[4]
        assert "/submit/host/path/" not in line
        # The doubled-prefix bug produced exactly this.
        assert "frames_local/frames_dir" not in line
    assert (tmp_path / "local_orig.cache").is_file()
    assert "/submit/host/path/" in (tmp_path / "local_orig.cache").read_text()


def test_frames_are_paired_in_time_order(tmp_path, monkeypatch):
    _make_run_dir(tmp_path, n=4)
    monkeypatch.chdir(tmp_path)
    lines = rewrite_cache_for_worker_transfer("local.cache", "frames_dir")
    starts = [int(line.split()[2]) for line in lines]
    assert starts == sorted(starts)
    for line in lines:
        assert str(line.split()[2]) in line.split()[4], (
            "cache entry paired with a frame from a different GPS time: " + line)


def test_refuses_to_pair_a_mismatched_directory(tmp_path, monkeypatch):
    """A positional pairing is only safe if the counts match; say so loudly."""
    _, frames = _make_run_dir(tmp_path, n=3)
    (frames / "H-H1_HOFT_C00-9999999999-4096.gwf").write_text("extra")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        rewrite_cache_for_worker_transfer("local.cache", "frames_dir")


def test_absolute_frames_dir_yields_absolute_entries(tmp_path):
    """The complement: an absolute frames_dir gives absolute cache entries.

    Not a corner case to tolerate -- it is the contract.  A shared-filesystem
    site (Slurm, a local run) wants the real path, not a sandbox-relative one.
    """
    cache, frames = _make_run_dir(tmp_path)
    lines = rewrite_cache_for_worker_transfer(str(cache), str(frames))
    assert all(line.split()[4].startswith(str(frames)) for line in lines)


def test_legacy_shell_was_broken(tmp_path):
    """Run the original commands and show they produce a wrong cache.

    Without this, "the old code was broken" is a claim about code nobody will
    read again.  With it, the fix has a demonstrated failure to point at.
    """
    cache, frames = _make_run_dir(tmp_path)
    script = (
        "cd {d} && "
        "cat local.cache > awk '{{print $1, $2, $3, $4}}' > local_stripped.cache; "
        "for i in `ls frames_dir/*.gwf`; do echo frames_local/${{i}} ; done "
        "> base_paths.dat; "
        "paste local_stripped.cache base_paths.dat > local_relative.cache"
    ).format(d=tmp_path)
    subprocess.run(["bash", "-c", script], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    legacy = (tmp_path / "local_relative.cache").read_text()
    # 1. the stray file the broken redirect creates
    assert (tmp_path / "awk").exists(), (
        "expected the legacy redirect to create a file named 'awk'")
    # 2. the submit-host path survives, which is the whole point of the stage
    assert "/submit/host/path/" in legacy
    # 3. the prefix is doubled
    assert "frames_local/frames_dir/" in legacy
