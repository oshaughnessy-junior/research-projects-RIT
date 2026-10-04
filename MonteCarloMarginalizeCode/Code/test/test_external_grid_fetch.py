"""Regression tests for external-grid selection (PR #181 item A9).

`util_FetchExternalGrid.py` picks the grid to hand to the next run.  PR #181
changed that choice in two ways, and neither had a test:

1. The sort key was ``int(re.sub('\\D', '', fname))`` -- every digit in the path,
   concatenated into one integer.  Writing this test corrected my account of
   when that is wrong: for single-number basenames in one directory it happens
   to agree with the numeric order, because the shared parent contributes an
   identical prefix and a longer suffix is also a larger number.  It breaks on
   MULTI-number basenames, where concatenation loses the field structure:
   ``overlap-grid-2-10`` becomes 210 and outranks ``overlap-grid-10-1`` at 101,
   so the fetch takes iteration 2 instead of iteration 10.  The key is now the
   tuple of numbers in the BASENAME, which compares field by field.
2. ``n_max`` and ``base_pattern`` in the JSON config were accepted and then
   silently ignored: ``retrieve_native`` was called with neither.  PR #181
   passes them through -- but ``n_max`` was STILL inert, because both branches
   sampled into ``P_list_reduced`` and then wrote the full list anyway.  Since
   BasicIteration sets ``"n_max": 3000`` on the external-fetch subdag, that is
   a production path where a documented cap has never applied.  It is honoured
   now, which is a behaviour change for grids larger than the cap.

The script parses its arguments at import, so these drive it as a subprocess --
which is also what the pipeline does.
"""

import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
SCRIPT = os.path.join(CODE, "bin", "util_FetchExternalGrid.py")


def _run(tmp_path, config, out_name="merged_grid.dat"):
    config_path = tmp_path / "fetch.json"
    config_path.write_text(json.dumps(config))
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + os.pathsep + env.get("PYTHONPATH", "")
    env["RIFT_HYPERPIPELINE_FORMAT"] = "1"
    result = subprocess.run(
        [sys.executable, SCRIPT, "--input-json", str(config_path),
         "--inj-file-out", str(tmp_path / out_name)],
        cwd=str(tmp_path), env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
    return result, tmp_path / out_name


def _write_grids(source, indices):
    """One self-describing hyperpipeline grid per iteration index."""
    source.mkdir(parents=True, exist_ok=True)
    for index in indices:
        (source / "overlap-grid-{}.dat".format(index)).write_text(
            "# RIFT_HYPERPIPELINE_V1\n"
            "# lnL sigma_lnL m1 m2 a1x a1y a1z a2x a2y a2z\n"
            + "".join(
                "0.0 0.0 3{}.0 20.0 0 0 0 0 0 0\n".format(index)
                for _ in range(4)))


def test_picks_the_highest_iteration(tmp_path):
    source = tmp_path / "run_1264316116_v2"     # digits in the PARENT path
    _write_grids(source, [0, 1, 9, 10])
    result, out = _run(tmp_path, {"method": "native", "source": str(source)})
    assert result.returncode == 0, result.stdout
    assert out.is_file()
    assert "31" in out.read_text().split("\n")[2], (
        "expected the grid from iteration 10; got:\n" + out.read_text())


def test_parent_directory_digits_do_not_change_the_choice(tmp_path):
    """The same grids under two differently-named parents must agree.

    This is the property the old key lacked: it read the parent path's digits.
    """
    chosen = []
    for parent in ("aaa", "run_999999999"):
        source = tmp_path / parent / "grids"
        _write_grids(source, [0, 1, 2])
        _, out = _run(tmp_path, {"method": "native", "source": str(source)},
                      out_name="out_{}.dat".format(parent))
        chosen.append(out.read_text())
    assert chosen[0] == chosen[1]


def test_missing_grid_fails_loudly(tmp_path):
    """An empty source used to raise IndexError from fnames[-1]; be explicit."""
    (tmp_path / "empty").mkdir()
    result, _ = _run(tmp_path, {"method": "native",
                                "source": str(tmp_path / "empty")})
    assert result.returncode != 0
    assert "No external grids matching" in result.stdout


def test_config_n_max_is_honoured(tmp_path):
    """BasicIteration sets n_max=3000 on the external-fetch subdag.

    It was sampled into a discarded variable, so the cap never applied.  This
    is the behaviour change: a grid larger than the cap is now truncated.
    """
    source = tmp_path / "grids"
    _write_grids(source, [0, 1])
    result, out = _run(tmp_path, {"method": "native", "source": str(source),
                                  "n_max": 2})
    assert result.returncode == 0, result.stdout
    rows = [line for line in out.read_text().splitlines()
            if line.strip() and not line.startswith("#")]
    assert len(rows) == 2, (
        "n_max in the config was ignored; got {} rows".format(len(rows)))


def test_n_max_larger_than_the_grid_is_not_an_error(tmp_path):
    """random.sample raises when asked for more than the population."""
    source = tmp_path / "grids"
    _write_grids(source, [0])
    result, out = _run(tmp_path, {"method": "native", "source": str(source),
                                  "n_max": 10_000})
    assert result.returncode == 0, result.stdout
    rows = [line for line in out.read_text().splitlines()
            if line.strip() and not line.startswith("#")]
    assert len(rows) == 4


def test_config_base_pattern_is_honoured(tmp_path):
    source = tmp_path / "grids"
    _write_grids(source, [0])
    (source / "something-else-7.dat").write_text(
        "# RIFT_HYPERPIPELINE_V1\n"
        "# lnL sigma_lnL m1 m2 a1x a1y a1z a2x a2y a2z\n"
        "0.0 0.0 99.0 20.0 0 0 0 0 0 0\n")
    result, out = _run(tmp_path, {"method": "native", "source": str(source),
                                  "base_pattern": "something-else-*.dat"})
    assert result.returncode == 0, result.stdout
    assert "99.0" in out.read_text()


def test_legacy_key_is_wrong_on_multi_number_basenames():
    """Demonstrate the failure rather than assert it.

    Note what this does NOT show: for single-number basenames the legacy key
    agrees with the numeric order, so the fix is a no-op there.  The first
    version of this test claimed otherwise and failed, which is the only
    reason the claim above is now accurate.
    """
    import re
    parent = "/scratch/run_1264316116_v2/"
    names = ["overlap-grid-2-10.dat", "overlap-grid-10-1.dat"]

    legacy = sorted(parent + n for n in names)
    legacy.sort(key=lambda f: int(re.sub(r"\D", "", f)))
    fixed = sorted(parent + n for n in names)
    fixed.sort(key=lambda f: tuple(
        int(x) for x in re.findall(r"\d+", os.path.basename(f))))

    assert os.path.basename(fixed[-1]) == "overlap-grid-10-1.dat"
    assert os.path.basename(legacy[-1]) == "overlap-grid-2-10.dat", (
        "expected the concatenating key to pick iteration 2 over iteration 10")


def test_single_number_basenames_are_unaffected():
    """The no-op half of the claim, pinned so it stays honest."""
    import re
    parent = "/scratch/run_1264316116_v2/"
    names = ["overlap-grid-9.dat", "overlap-grid-10.dat", "overlap-grid-2.dat"]
    legacy = sorted(parent + n for n in names)
    legacy.sort(key=lambda f: int(re.sub(r"\D", "", f)))
    fixed = sorted(parent + n for n in names)
    fixed.sort(key=lambda f: tuple(
        int(x) for x in re.findall(r"\d+", os.path.basename(f))))
    assert os.path.basename(legacy[-1]) == os.path.basename(fixed[-1])
