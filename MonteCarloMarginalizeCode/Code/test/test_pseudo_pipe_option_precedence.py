"""An explicitly passed option must beat a helper-generated file (item A5).

`--internal-force-puff-iterations` used to be `default=4`, and pseudo_pipe read
it BEFORE `helper_puff_max_it.txt`:

    puff_max_it = opts.internal_force_puff_iterations
    try:  puff_max_it = int(open("helper_puff_max_it.txt").readline())
    except: pass

so a value the user typed was overwritten by the helper's and had no effect.
PR #181 made the default None and applies the explicit value last.  That is a
behaviour change for anyone who passed the flag -- previously it did nothing.

This is a source-order check.  A behavioural one would need two full pipeline
builds to compare a single emitted number, and the property at issue really is
the order of three statements.  What it cannot see is a change that preserves
the order but breaks the semantics, so the constant-likelihood gate's use of
`--internal-force-puff-iterations -1` (which must produce no puff nodes) is the
companion evidence -- `_assert_no_puff_nodes`, which reads the built workflow
and fails if any node runs PUFF.sub.

That delegation was empty when first written: the gate asserted nothing about
puff, so an order-preserving semantic break -- assigning the parsed value to a
dead variable -- passed this test AND the gate, and `helper_puff_max_it.txt`
silently won again. Naming evidence that does not exist is worse than naming
none, because it stops the next reader looking.
"""

import os
import re

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PSEUDO = os.path.abspath(os.path.join(
    HERE, "..", "bin", "util_RIFT_pseudo_pipe.py"))
OPTION = "internal_force_puff_iterations"


@pytest.fixture(scope="module")
def lines():
    with open(PSEUDO) as stream:
        return stream.read().splitlines()


def _first(lines, pattern):
    rx = re.compile(pattern)
    for i, line in enumerate(lines):
        if rx.search(line):
            return i
    return None


def test_the_option_defaults_to_none(lines):
    where = _first(lines, r'--internal-force-puff-iterations"?,\s*default=')
    assert where is not None, "the option is gone"
    assert "default=None" in lines[where], (
        "a non-None default cannot be distinguished from 'the user said "
        "nothing', which is what made the flag inert: " + lines[where].strip())


def test_the_explicit_value_is_applied_after_the_helper_file(lines):
    helper = _first(lines, r'helper_puff_max_it\.txt')
    override = _first(lines, r'if opts\.%s is not None' % OPTION)
    assert helper is not None and override is not None
    assert override > helper, (
        "the explicit --internal-force-puff-iterations is applied at line {} "
        "and the helper file is read at line {}; whichever runs last wins, and "
        "the user's value must".format(override + 1, helper + 1))


def test_the_option_is_not_also_consumed_earlier(lines):
    """The original bug was an assignment BEFORE the file read."""
    helper = _first(lines, r'helper_puff_max_it\.txt')
    earlier = [i + 1 for i, line in enumerate(lines[:helper])
               if OPTION in line and "add_argument" not in line]
    assert not earlier, (
        "opts.{} is consulted at line(s) {} before the helper file is read, "
        "so the helper would overwrite it again".format(OPTION, earlier))
