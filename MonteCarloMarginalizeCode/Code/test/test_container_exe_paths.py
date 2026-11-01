"""Container executable paths must not be built by string concatenation (A11).

PR #181 replaced `singularity_base_exe_path + exe_base` with
`os.path.join(...)` in four submit writers.  With a trailing slash on
`SINGULARITY_BASE_EXE_DIR` the two are identical, which is why the old form
survived; without one it produced `/usr/local/binutil_CIP.py`, a path that does
not exist, and the job died on the execute node rather than at build time.
Both spellings occur in the wild -- the variable is set by hand in run scripts
and by `~/RIFT_develUWM/bin/activate`.

This is a SOURCE check, and deliberately so.  The first version of this file
called the writers directly, and every case skipped: each has layered
preconditions (a cache or frames dir, a transfer list) that would have to be
satisfied writer by writer, and a suite that skips everything reads green while
testing nothing -- which is worse than not having it.  The property here is
textual and the regression would be textual, so check it textually and say so.
"""

import os
import re

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
MISC = os.path.abspath(os.path.join(HERE, "..", "RIFT", "misc"))

WRITER_MODULES = [
    "dag_utils.py",
    "dag_utils_generic.py",
    "dag_utils_htcondor.py",
    os.path.join("..", "simulation_manager", "CondorManager.py"),
]

#: `<base> + <exe>` where base is one of the container prefix variables.
CONCAT = re.compile(
    r"singularity_base_exe_path\s*\+|"
    r"\+\s*singularity_base_exe_path|"
    # f-string interpolation is concatenation by another spelling, and the
    # original pattern could not see it: rewriting os.path.join(base, exe) as
    # f"{base}{exe}" reintroduces the exact defect -- `/usr/binile.py` -- with
    # this file green.
    r"f['\"][^'\"]*\{\s*singularity_base_exe_path\s*\}|"
    r"['\"]\s*\.format\([^)]*singularity_base_exe_path|"
    r"%\s*\(?\s*singularity_base_exe_path")


@pytest.mark.parametrize("relative", WRITER_MODULES)
def test_no_writer_concatenates_the_container_prefix(relative):
    path = os.path.join(MISC, relative)
    if not os.path.isfile(path):
        pytest.fail("expected writer module is missing: " + path)
    with open(path) as stream:
        offenders = [(i + 1, line.strip())
                     for i, line in enumerate(stream)
                     if CONCAT.search(line) and not line.strip().startswith("#")]
    assert not offenders, (
        "{} builds a container executable path by concatenation; use "
        "os.path.join so a missing trailing slash on SINGULARITY_BASE_EXE_DIR "
        "cannot produce '/usr/local/binutil_X.py': {}".format(
            relative, offenders))


@pytest.mark.parametrize("relative", WRITER_MODULES)
def test_each_writer_module_actually_handles_the_prefix(relative):
    """Guard the guard: if the variable disappears, the check above passes
    vacuously and would keep passing while the handling moved elsewhere."""
    path = os.path.join(MISC, relative)
    with open(path) as stream:
        text = stream.read()
    assert "singularity_base_exe_path" in text, (
        "{} no longer mentions singularity_base_exe_path; the concatenation "
        "check above has become vacuous and this file needs revisiting"
        .format(relative))
    assert "os.path.join(singularity_base_exe_path" in text, (
        "{} does not join the container prefix with os.path.join".format(
            relative))
