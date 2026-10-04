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
    "magic+double-hash": ["# RIFT_HYPERPIPELINE_V1",
                          "# # lnL sigma_lnL x y z"],
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


# ---------------------------------------------------------------------------
# The OTHER reader.
#
# `util_HyperparameterTracerUpdate.py` consumes the same `all.marg_net` as the
# posterior step, in the same workflow, and had the same first-line-only parse.
# Routing it through the shared rule was half of this change and had NO test:
# reverting that half byte-for-byte left the whole corpus green -- the same
# failure that got the first version of this work rejected, reproduced on the
# other half. These close it.
# ---------------------------------------------------------------------------

import importlib.util

TRACER = os.path.join(CODE, "bin", "util_HyperparameterTracerUpdate.py")


def _tracer():
    spec = importlib.util.spec_from_file_location("_tracer_mod", TRACER)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass          # argv-driven script; we only want its helpers
    return module


@pytest.mark.parametrize("label", sorted(HEADERS))
def test_the_tracer_reads_every_header_shape(tmp_path, label):
    """Both readers of these tables must agree about what the columns are.

    Disagreement is the actual defect class: one lane of a workflow completing
    while another aborts on the same file.
    """
    fname = _write(str(tmp_path / "in.dat"), HEADERS[label])
    cols, rows, _ = _tracer()._read_dat(fname)
    assert list(cols) == ["lnL", "sigma_lnL", "x", "y", "z"], (
        "{} header parsed to {}".format(label, list(cols)))
    assert rows.shape[1] == 5


def test_the_tracer_refuses_a_headerless_table(tmp_path):
    fname = _write(str(tmp_path / "headerless.dat"), [])
    with pytest.raises(SystemExit) as caught:
        _tracer()._read_dat(fname)
    assert "headerless.dat" in str(caught.value), str(caught.value)


def test_the_tracer_round_trip_keeps_the_format_preamble(tmp_path):
    """Reading a format it cannot write would be silent data loss.

    Before this change the tracer refused hyperpipeline tables outright.  Now
    it consumes them -- so it must also reproduce the marker AND the metadata
    line, which records the waveform-generation settings whose absence once
    made ILE build templates from defaults.
    """
    hyperpipeline_io = pytest.importorskip("RIFT.misc.hyperpipeline_io")
    fname = _write(str(tmp_path / "in.dat"), HEADERS["magic+meta"])
    module = _tracer()
    cols, rows, preamble = module._read_dat(fname)
    out = str(tmp_path / "out.dat")
    module._write_dat(out, cols, rows, preamble)

    assert hyperpipeline_io.sniff(out), (
        "the output no longer declares the format the input did:\n"
        + open(out).read()[:300])
    assert hyperpipeline_io.parse_metadata(out) == \
        hyperpipeline_io.parse_metadata(fname), "metadata lost in the round trip"


def test_a_provenance_comment_where_the_header_belongs_is_refused(tmp_path):
    """The other half of the refusal: a first line that IS a comment but is
    not a header.

    Without this only the `startswith('#')` half is exercised, and dropping the
    `lnL` check reintroduces the headline symptom -- a provenance line like
    "# produced by util_HyperCombine on 2026-08-20" parses to
    ['produced','by',...] and the run dies with "'x' is not in list", pointing
    at the user's --parameter rather than at the file.
    """
    fname = str(tmp_path / "provenance.dat")
    _write(fname, ["# produced by util_HyperCombine on 2026-08-20",
                   "# lnL sigma_lnL x y z"])
    result = subprocess.run(
        [sys.executable, TOOL, "--fname", fname,
         "--parameter", "x", "--integration-parameter-range", "x:[0,1]",
         "--n-output-samples", "4",
         "--fname-output-samples", str(tmp_path / "out"),
         "--fname-output-integral", str(tmp_path / "out_int")],
        cwd=str(tmp_path), capture_output=True, text=True, timeout=1800)
    combined = result.stdout + result.stderr
    assert "no column header in" in combined, combined[-2500:]
    assert "is not in list" not in combined, (
        "the run got past the header and failed on the parameter instead:\n"
        + combined[-2500:])


# ---------------------------------------------------------------------------
# The THIRD reader: the driver contract itself.
#
# `RIFT.hyperpipe.drivers.base.read_grid` reads the `--using-eos file:<grid>`
# grid -- the very interface this change is about -- and had the same
# first-line-only parse. It mattered more there than in the consumers: a
# shifted column set propagates into whatever the driver writes back.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("label", sorted(HEADERS))
def test_the_driver_grid_reader_agrees_with_the_others(tmp_path, label):
    base = pytest.importorskip("RIFT.hyperpipe.drivers.base")
    fname = _write(str(tmp_path / "grid.dat"), HEADERS[label])
    _, columns = base.read_grid("file:" + fname)
    assert list(columns) == ["x", "y", "z"], (
        "{} grid parsed to {}; a shift here reaches whatever the driver "
        "writes back".format(label, list(columns)))


def test_the_driver_grid_reader_refuses_a_headerless_grid(tmp_path):
    base = pytest.importorskip("RIFT.hyperpipe.drivers.base")
    fname = _write(str(tmp_path / "grid.dat"), [])
    with pytest.raises(ValueError, match="read_grid"):
        base.read_grid("file:" + fname)


def test_a_header_naming_no_parameters_is_refused(tmp_path):
    """`# lnL sigma_lnL` and nothing else is a header, but not a usable one.

    Untested until now, so disabling the guard passed: the tool would carry an
    empty parameter list forward and fail later somewhere less informative.
    Distinct from the no-header case -- here the line IS a header, it just
    declares no parameters, and the message says so rather than claiming the
    header is missing.
    """
    fname = str(tmp_path / "noparams.dat")
    with open(fname, "w") as handle:
        handle.write("# lnL sigma_lnL\n")
        for index in range(20):
            handle.write("{:.4f} 0.0100\n".format(-0.1 * index))
    result = subprocess.run(
        [sys.executable, TOOL, "--fname", fname,
         "--parameter", "x", "--integration-parameter-range", "x:[0,1]",
         "--n-output-samples", "4",
         "--fname-output-samples", str(tmp_path / "out"),
         "--fname-output-integral", str(tmp_path / "out_int")],
        cwd=str(tmp_path), capture_output=True, text=True, timeout=1800)
    combined = result.stdout + result.stderr
    assert "no parameters" in combined, combined[-2500:]
    assert os.path.basename(fname) in combined, combined[-2000:]


def test_a_second_read_cannot_steal_the_first_grids_metadata(tmp_path):
    """`--inj-file-prev` makes `main` read TWO grids before it writes one.

    The preamble was briefly a module-level "most recently read" value, so the
    second read overwrote the first and the output carried the PREVIOUS
    iteration's waveform settings.  Not merely losing the current ones:
    attaching different ones, and `ampO=0` -- leading order only -- is exactly
    the value whose silent use made ILE generate too few modes.

    This drives the two reads in the order `main` does and asserts the output
    describes the grid it is actually about.
    """
    hyperpipeline_io = pytest.importorskip("RIFT.misc.hyperpipeline_io")
    current = _write(str(tmp_path / "cur.dat"),
                     ["# RIFT_HYPERPIPELINE_V1",
                      "# RIFT_HYPERPIPELINE_META ampO=-1 fmin=20.0",
                      "# lnL sigma_lnL x y z"])
    previous = _write(str(tmp_path / "prev.dat"),
                      ["# RIFT_HYPERPIPELINE_V1",
                       "# RIFT_HYPERPIPELINE_META ampO=0 fmin=99.0",
                       "# lnL sigma_lnL x y z"])
    module = _tracer()
    cols, rows, preamble = module._read_dat(current)
    module._read_dat(previous)            # as --inj-file-prev does
    out = str(tmp_path / "out.dat")
    module._write_dat(out, cols, rows, preamble)

    assert hyperpipeline_io.parse_metadata(out) == \
        hyperpipeline_io.parse_metadata(current), (
            "the output carries {} but describes the grid whose settings are "
            "{}".format(hyperpipeline_io.parse_metadata(out),
                        hyperpipeline_io.parse_metadata(current)))
