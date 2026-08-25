"""The hyperparameter posterior tool must find its column names.

`util_ConstructEOSPosterior.py` read them from the FIRST line of its input.
A hyperpipeline-format table opens with a `# RIFT_HYPERPIPELINE_V1` marker and
may carry a `# RIFT_HYPERPIPELINE_META ...` line before the column header, so
the parse returned `[]` and the run died several steps later with
`ValueError: 'x' is not in list` -- naming the parameter the user asked for
rather than the file that lacked it.

Found by running the hyperpipeline demonstration disseminated with paper 4,
which is the first thing to feed this tool a magic-prefixed table.  The format
dates from May 2026, so the defect is old and simply unexercised.

These tests go through the tool's own parsing path rather than asserting on
source, because the failure was a parse returning the wrong answer, not a line
being absent.
"""

import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
TOOL = os.path.join(CODE, "bin", "util_ConstructEOSPosterior.py")

pytest.importorskip("RIFT.misc.hyperpipeline_io")


def _table(path, header_lines, rows=6):
    with open(path, "w") as handle:
        for line in header_lines:
            handle.write(line + "\n")
        for i in range(rows):
            handle.write("{} 0.01 {} {} {}\n".format(
                -0.5 * i, 0.1 * i, 0.2 * i, 0.3 * i))
    return path


@pytest.mark.parametrize("header_lines,label", [
    (["# lnL sigma_lnL x y z"], "plain"),
    (["# RIFT_HYPERPIPELINE_V1", "# lnL sigma_lnL x y z"], "magic"),
    (["# RIFT_HYPERPIPELINE_V1",
      "# RIFT_HYPERPIPELINE_META ampO=-1 fmin=20.0",
      "# lnL sigma_lnL x y z"], "magic+metadata"),
])
def test_the_column_names_are_found_whatever_precedes_them(
        tmp_path, header_lines, label):
    """Every header shape the writer can emit must parse to the same names.

    The tool is invoked for real and only its *diagnosis* is inspected: a
    successful fit needs far more setup than this, but the parse happens first,
    and its failure is unambiguous in the output.
    """
    fname = _table(str(tmp_path / "in.dat"), header_lines)
    result = subprocess.run(
        [sys.executable, TOOL, "--fname", fname,
         "--parameter", "x", "--parameter", "y", "--parameter", "z",
         "--n-output-samples", "4",
         "--fname-output-samples", str(tmp_path / "out"),
         "--fname-output-integral", str(tmp_path / "out_int")],
        cwd=str(tmp_path), capture_output=True, text=True, timeout=900)
    combined = result.stdout + result.stderr
    assert "is not in list" not in combined, (
        "the {} header did not parse; the tool looked for the requested "
        "parameters among the wrong strings:\n{}".format(
            label, combined[-2500:]))
    assert "no column names found in the header" not in combined, combined[-2000:]


def test_a_table_with_no_column_header_says_so(tmp_path):
    """The failure that remains must name the file, not a parameter.

    Silently proceeding with no column names is what produced a message
    pointing at `--parameter x`; a table that genuinely lacks a header should
    say that instead.
    """
    fname = str(tmp_path / "headerless.dat")
    with open(fname, "w") as handle:
        handle.write("# RIFT_HYPERPIPELINE_V1\n")
        for i in range(4):
            handle.write("{} 0.01 {} {} {}\n".format(-0.5 * i, i, i, i))
    result = subprocess.run(
        [sys.executable, TOOL, "--fname", fname,
         "--parameter", "x", "--n-output-samples", "4",
         "--fname-output-samples", str(tmp_path / "out"),
         "--fname-output-integral", str(tmp_path / "out_int")],
        cwd=str(tmp_path), capture_output=True, text=True, timeout=900)
    combined = result.stdout + result.stderr
    assert "no column names found in the header" in combined, combined[-2500:]
    assert os.path.basename(fname) in combined, (
        "the error does not name the offending file:\n" + combined[-2000:])
