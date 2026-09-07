"""util_HyperCombine must not silently mix on-disk formats (item A10).

PR #181 made `util_HyperCombine.py` detect the self-describing hyperpipeline
format from its first input and then require every other input to match, and
to emit the two-line hyperpipeline header rather than echoing the first input's
first comment line.

Mixing matters because the two layouts put the likelihood in different columns:
hyperpipeline is `lnL sigma_lnL m1 m2 ...` while the legacy composite is
positional with lnL late.  Combining one of each does not fail -- it produces a
file whose rows disagree about what column zero means, and every consumer
downstream reads it happily.

The legacy-only path must stay byte-identical, because that is what existing
EOS/population runs use.
"""

import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
EXE = os.path.join(CODE, "bin", "util_HyperCombine.py")


def _run(args, cwd):
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run([sys.executable, EXE] + args, cwd=str(cwd), env=env,
                          text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=300)


def _write_hyperpipe(path, rows):
    with open(path, "w") as stream:
        stream.write("# RIFT_HYPERPIPELINE_V1\n")
        stream.write("# lnL sigma_lnL m1 m2\n")
        for row in rows:
            stream.write(" ".join("{:.18e}".format(v) for v in row) + "\n")


def _write_legacy(path, rows):
    with open(path, "w") as stream:
        stream.write("# m1 m2 lnL sigma_lnL\n")
        for row in rows:
            stream.write(" ".join("{:.18e}".format(v) for v in row) + "\n")


def test_mixed_formats_are_rejected(tmp_path):
    a = tmp_path / "a.dat"; _write_hyperpipe(a, [[1.0, 0.1, 30.0, 20.0]])
    b = tmp_path / "b.dat"; _write_legacy(b, [[30.0, 20.0, 1.0, 0.1]])
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode != 0, (
        "mixing a hyperpipeline input with a legacy one was accepted; the two "
        "layouts disagree about which column is lnL:\n" + result.stdout[:2000])
    assert "mixed" in (result.stderr + result.stdout).lower()


def test_column_mismatch_between_hyperpipe_inputs_is_rejected(tmp_path):
    a = tmp_path / "a.dat"; _write_hyperpipe(a, [[1.0, 0.1, 30.0, 20.0]])
    b = tmp_path / "b.dat"
    with open(b, "w") as stream:
        stream.write("# RIFT_HYPERPIPELINE_V1\n# lnL sigma_lnL m1 m2 s1z\n")
        stream.write(" ".join(["1.0", "0.1", "30.0", "20.0", "0.5"]) + "\n")
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode != 0
    assert "column mismatch" in (result.stderr + result.stdout).lower()


def test_hyperpipe_only_emits_the_self_describing_header(tmp_path):
    a = tmp_path / "a.dat"; _write_hyperpipe(a, [[1.0, 0.1, 30.0, 20.0]])
    b = tmp_path / "b.dat"; _write_hyperpipe(b, [[2.0, 0.2, 31.0, 21.0]])
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    assert lines[0].strip() == "# RIFT_HYPERPIPELINE_V1", lines[:3]
    assert lines[1].strip() == "# lnL sigma_lnL m1 m2", lines[:3]


def test_legacy_only_still_echoes_its_own_header(tmp_path):
    """The path existing EOS/population runs use must not change."""
    a = tmp_path / "a.dat"; _write_legacy(a, [[30.0, 20.0, 1.0, 0.1]])
    b = tmp_path / "b.dat"; _write_legacy(b, [[31.0, 21.0, 2.0, 0.2]])
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    assert lines[0].strip() == "# m1 m2 lnL sigma_lnL", lines[:3]
    assert "RIFT_HYPERPIPELINE_V1" not in result.stdout


def test_empty_first_shard_does_not_disable_the_format_contract(tmp_path):
    """Format detection keys off the first READABLE input, not argv[1].

    An empty first shard (a failed worker) used to make the merge fall back
    to the legacy path: mixed-input and column-mismatch checks silently
    disabled, and the output emitted with no header at all -- downstream
    sniffs it as legacy and column zero changes meaning.
    """
    dead = tmp_path / "dead.dat"; dead.touch()
    b = tmp_path / "b.dat"; _write_hyperpipe(b, [[2.0, 0.2, 31.0, 21.0]])
    result = _run([str(dead), str(b)], tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    assert lines[0].strip() == "# RIFT_HYPERPIPELINE_V1", lines[:3]
    assert lines[1].strip() == "# lnL sigma_lnL m1 m2", lines[:3]

    legacy = tmp_path / "c.dat"; _write_legacy(legacy, [[30.0, 20.0, 1.0, 0.1]])
    mixed = _run([str(dead), str(b), str(legacy)], tmp_path)
    assert mixed.returncode != 0, (
        "an empty first shard disabled the mixed-format check:\n"
        + mixed.stdout[:2000])


def test_no_readable_inputs_is_an_error_not_an_empty_success(tmp_path):
    dead = tmp_path / "dead.dat"; dead.touch()
    result = _run([str(dead), str(tmp_path / "absent.dat")], tmp_path)
    assert result.returncode != 0, (
        "a merge with zero readable inputs exited 0 with empty output:\n"
        + result.stdout[:500])
