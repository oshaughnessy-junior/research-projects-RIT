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


def _make_multi_ifo(tmp_path, cache_order, frame_ifos=("H1", "L1", "V1")):
    """A cache in *cache_order* against one merged frame per detector.

    This is the shape util_ForOSG_MakeTruncatedLocalFramesDir.sh actually
    produces: one frame per IFO, whose GPS span is slightly wider than the
    cache entry it serves.
    """
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    for ifo in frame_ifos:
        (frames / "{}-O4MDC-1372203665-1186.gwf".format(ifo)).write_text("f")
    cache = tmp_path / "local.cache"
    cache.write_text("\n".join(
        "{} O4MDC 1372203666 1184 /submit/host/{}.gwf".format(ifo, ifo)
        for ifo in cache_order) + "\n")
    return cache, frames


@pytest.mark.parametrize("cache_order", [
    ("H1", "L1", "V1"),
    ("H1", "V1", "L1"),   # the common real ordering, and the one that broke
    ("V1", "L1", "H1"),
    ("L1", "H1"),
])
def test_entries_are_matched_by_detector_not_by_position(tmp_path, monkeypatch,
                                                         cache_order):
    """Zipping the cache against sorted(listdir) mispairs, and does it silently.

    Caches are frequently ordered H1 V1 L1 while a sorted directory listing is
    H1 L1 V1.  A count check cannot see the swap -- the counts agree -- so the
    cache would name a frame from the wrong detector and only fail much later,
    on a worker, if at all.
    """
    _make_multi_ifo(tmp_path, cache_order)
    monkeypatch.chdir(tmp_path)
    lines = rewrite_cache_for_worker_transfer("local.cache", "frames_dir")
    assert len(lines) == len(cache_order)
    for line in lines:
        fields = line.split()
        observatory, frame = fields[0], os.path.basename(fields[4])
        assert frame.startswith(observatory + "-"), (
            "cache entry for {} was paired with {}".format(observatory, frame))


def test_several_segments_per_detector_share_the_merged_frame(tmp_path,
                                                              monkeypatch):
    """More cache lines than frames is legitimate, not an error.

    The truncation script writes ONE merged frame per detector, so a cache with
    two segments for V1 has four lines and three frames.  A count check would
    reject this run at build time.
    """
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    for ifo in ("H1", "L1", "V1"):
        (frames / "{}-O4MDC-1372203665-1186.gwf".format(ifo)).write_text("f")
    (tmp_path / "local.cache").write_text(
        "H1 O4MDC 1372203666 500 /submit/a.gwf\n"
        "L1 O4MDC 1372203666 500 /submit/b.gwf\n"
        "V1 O4MDC 1372203666 500 /submit/c.gwf\n"
        "V1 O4MDC 1372204200 500 /submit/d.gwf\n")
    monkeypatch.chdir(tmp_path)
    lines = rewrite_cache_for_worker_transfer("local.cache", "frames_dir")
    assert len(lines) == 4
    assert sum(1 for line in lines if line.startswith("V1 ")) == 2
    for line in lines:
        assert os.path.basename(line.split()[4]).startswith(line.split()[0])


def test_single_letter_observatory_column_still_matches(tmp_path, monkeypatch):
    """Caches spell the observatory 'H' or 'H1' depending on their origin."""
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    (frames / "H-H1_HOFT_C00-1000000000-4096.gwf").write_text("f")
    (tmp_path / "local.cache").write_text(
        "H H1_HOFT_C00 1000000000 4096 /submit/x.gwf\n")
    monkeypatch.chdir(tmp_path)
    lines = rewrite_cache_for_worker_transfer("local.cache", "frames_dir")
    assert lines[0].endswith("frames_dir/H-H1_HOFT_C00-1000000000-4096.gwf")


def test_refuses_when_no_frame_matches(tmp_path, monkeypatch):
    """An unmatched detector must fail loudly, not pick something plausible."""
    _make_multi_ifo(tmp_path, ("H1", "L1", "K1"))
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError) as excinfo:
        rewrite_cache_for_worker_transfer("local.cache", "frames_dir")
    assert "K1" in str(excinfo.value)


def test_refuses_an_ambiguous_multi_frame_detector(tmp_path, monkeypatch):
    """Two frames for one detector, neither covering the entry: do not guess."""
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    (frames / "H1-O4MDC-1000000000-16.gwf").write_text("f")
    (frames / "H1-O4MDC-1000000100-16.gwf").write_text("f")
    (tmp_path / "local.cache").write_text(
        "H1 O4MDC 1000000050 16 /submit/x.gwf\n")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        rewrite_cache_for_worker_transfer("local.cache", "frames_dir")


def test_missing_frames_directory_is_a_clear_error(tmp_path, monkeypatch):
    (tmp_path / "local.cache").write_text("H1 O4MDC 1 2 /submit/x.gwf\n")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError) as excinfo:
        rewrite_cache_for_worker_transfer("local.cache", "frames_dir")
    assert "does not exist" in str(excinfo.value)


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


# ---------------------------------------------------------------------------
# The shape production actually produces.
#
# The tests above pair a cache against frames named after the cache's own
# frame TYPE.  Real runs never look like that: a datafind cache carries the
# datafind type (`H1_HOFT_AR01`) while
# `util_ForOSG_MakeTruncatedLocalFramesDir.sh` names its merged output after
# the CHANNEL -- `${IFO}-${CHANNEL_NO_DASH}-${TSTART}-${SEGLEN}.gwf`.  Keying
# the join on (observatory, type) therefore matched nothing on every real OSG
# run and aborted the build, while the fixtures and fake-data runs passed.
# These pin the real shape so the fixtures cannot drift back to a convenient one.
# ---------------------------------------------------------------------------

def _write(path, text=""):
    with open(path, "w") as handle:
        handle.write(text)


def test_real_datafind_cache_against_channel_named_frames(tmp_path):
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    _write(str(frames / "H1-GDS_CALIB_STRAIN_CLEAN_AR-1398132736-4096.gwf"))
    _write(str(frames / "L1-GDS_CALIB_STRAIN_CLEAN_AR-1398132736-4096.gwf"))
    cache = tmp_path / "local.cache"
    _write(str(cache),
           "H H1_HOFT_AR01 1398132736 4096 file://localhost/ceph/a.gwf\n"
           "L L1_HOFT_AR01 1398132736 4096 file://localhost/ceph/b.gwf\n")

    lines = rewrite_cache_for_worker_transfer(str(cache), str(frames))

    assert len(lines) == 2
    assert lines[0].startswith("H H1_HOFT_AR01 1398132736 4096 ")
    assert lines[0].endswith("H1-GDS_CALIB_STRAIN_CLEAN_AR-1398132736-4096.gwf")
    assert lines[1].startswith("L L1_HOFT_AR01 1398132736 4096 ")
    assert lines[1].endswith("L1-GDS_CALIB_STRAIN_CLEAN_AR-1398132736-4096.gwf")


def test_detectors_are_never_crossed_even_when_only_the_letter_matches(tmp_path):
    """The failure that matters most here is a SILENT one: pairing H's cache
    entry with L's frame would analyse the wrong detector's data and report
    nothing. Order the listing so a positional or first-match implementation
    would cross them."""
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    _write(str(frames / "L1-CHAN-1000000000-64.gwf"))
    _write(str(frames / "H1-CHAN-1000000000-64.gwf"))
    _write(str(frames / "V1-CHAN-1000000000-64.gwf"))
    cache = tmp_path / "local.cache"
    _write(str(cache),
           "H H1_TYPE 1000000000 64 file://x/a.gwf\n"
           "V V1_TYPE 1000000000 64 file://x/b.gwf\n"
           "L L1_TYPE 1000000000 64 file://x/c.gwf\n")

    lines = rewrite_cache_for_worker_transfer(str(cache), str(frames))

    for line in lines:
        observatory = line.split()[0]
        assert "/" + observatory + "1-" in line, line


def test_an_unstaged_detector_is_named_in_the_error(tmp_path):
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    _write(str(frames / "H1-CHAN-1000000000-64.gwf"))
    cache = tmp_path / "local.cache"
    _write(str(cache),
           "H H1_TYPE 1000000000 64 file://x/a.gwf\n"
           "L L1_TYPE 1000000000 64 file://x/c.gwf\n")

    with pytest.raises(ValueError, match="no staged frame for detector 'L'"):
        rewrite_cache_for_worker_transfer(str(cache), str(frames))
