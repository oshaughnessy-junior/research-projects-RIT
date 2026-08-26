"""The hyperparameter posterior tool must map its columns correctly.

`util_ConstructEOSPosterior.py` reads column names from its input's header.  A
hyperpipeline-format table opens with `# RIFT_HYPERPIPELINE_V1` and may carry a
metadata line before the header, so a first-line-only read returned `[]` and the
run died several steps later with `ValueError: 'x' is not in list` -- naming the
parameter the user asked for rather than the file that lacked it.

**These tests assert the parsed column MAPPING, not the absence of an error
string.**  The first version asserted only that two strings did not appear in
the output, and an adversarial review showed it passed on a byte-identical
revert of the fix: without `--integration-parameter-range` the tool aborts
before it reaches the column lookup, so neither string appeared whatever the
parse did.  Any unrelated failure -- an import error, a timeout -- satisfied it
too.  The tool prints the mapping it derived, so that is what is checked.
"""

import os
import re
import subprocess
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
TOOL = os.path.join(CODE, "bin", "util_ConstructEOSPosterior.py")

#: Header shapes a RIFT table can legitimately carry.  The double-hash form is
#: what `np.savetxt(..., header='# ' + names)` writes, the idiom used by
#: `util_shuffle_file.py` and `convert_ascii_framechange_xphm.py`.
HEADERS = {
    "plain": ["# lnL sigma_lnL x y z"],
    "magic": ["# RIFT_HYPERPIPELINE_V1", "# lnL sigma_lnL x y z"],
    "magic+meta": ["# RIFT_HYPERPIPELINE_V1",
                   "# RIFT_HYPERPIPELINE_META ampO=-1 fmin=20.0",
                   "# lnL sigma_lnL x y z"],
    "double-hash": ["# # lnL sigma_lnL x y z"],
}
EXPECTED_MAP = {"x": 2, "y": 3, "z": 4}


def _write(path, header_lines, n=60):
    rng = np.random.default_rng(3)
    points = rng.uniform(0.0, 1.0, (n, 3))
    lnL = -10.0 * ((points - 0.5) ** 2).sum(axis=1)
    with open(path, "w") as handle:
        for line in header_lines:
            handle.write(line + "\n")
        for value, row in zip(lnL, points):
            handle.write("{:.6f} 0.010000 {}\n".format(
                value, " ".join("{:.6f}".format(v) for v in row)))
    return path


def _column_map(tmp_path, header_lines):
    """Run the tool and return the column mapping it reports."""
    fname = _write(str(tmp_path / "in.dat"), header_lines)
    result = subprocess.run(
        [sys.executable, TOOL, "--fname", fname,
         "--parameter", "x", "--parameter", "y", "--parameter", "z",
         "--integration-parameter-range", "x:[0,1]",
         "--integration-parameter-range", "y:[0,1]",
         "--integration-parameter-range", "z:[0,1]",
         "--n-output-samples", "4",
         "--fname-output-samples", str(tmp_path / "out"),
         "--fname-output-integral", str(tmp_path / "out_int")],
        cwd=str(tmp_path), capture_output=True, text=True, timeout=1800)
    combined = result.stdout + result.stderr
    match = re.search(r"indexed as\s+(\{[^}]*\})", combined)
    assert match, (
        "the tool never reported a column mapping, so this test cannot say "
        "whether the parse was right:\n" + combined[-3000:])
    return eval(match.group(1)), combined      # noqa: S307 - our own output


@pytest.mark.parametrize("label", sorted(HEADERS))
def test_the_columns_map_correctly_for_every_header_shape(tmp_path, label):
    """Same table, four header spellings, one correct answer.

    `double-hash` is the case the format-aware reader gets WRONG: it strips a
    single leading '#', so the names come out shifted one column right. It is
    here because the historical parse handled it correctly, and a fix that
    breaks it is a regression on files two shipped tools produce.
    """
    mapping, output = _column_map(tmp_path, HEADERS[label])
    assert mapping == EXPECTED_MAP, (
        "{} header parsed to {} instead of {}; every parameter is read from "
        "the wrong column:\n{}".format(
            label, mapping, EXPECTED_MAP, output[-2000:]))


def test_a_table_with_no_header_at_all_is_refused(tmp_path):
    """The remaining failure must name the file, not a parameter.

    With no `#` line the historical parse reads a DATA row, gets a non-empty
    list of numbers, and dies with the same `'x' is not in list` the fix was
    meant to replace -- so an explicit refusal is the point, and it has to fire
    for a table with no comment line at all, not only for one with a magic
    marker.
    """
    fname = str(tmp_path / "headerless.dat")
    _write(fname, [])
    result = subprocess.run(
        [sys.executable, TOOL, "--fname", fname,
         "--parameter", "x", "--integration-parameter-range", "x:[0,1]",
         "--n-output-samples", "4",
         "--fname-output-samples", str(tmp_path / "out"),
         "--fname-output-integral", str(tmp_path / "out_int")],
        cwd=str(tmp_path), capture_output=True, text=True, timeout=1800)
    combined = result.stdout + result.stderr
    assert "no column header in" in combined, combined[-3000:]
    assert os.path.basename(fname) in combined, (
        "the refusal does not name the offending file:\n" + combined[-2000:])
