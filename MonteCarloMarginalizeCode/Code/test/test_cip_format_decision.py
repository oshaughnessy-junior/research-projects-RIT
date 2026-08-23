"""CIP must decide its on-disk format ONCE (item A8).

`util_ConstructIntrinsicPosterior_GenericCoordinates.py` reads a composite in
either the legacy positional-ASCII layout or the self-describing hyperpipeline
layout, and writes the next grid in one of the same two.  Before PR #181 the
READ decision was `env-var OR sniff(input)` while the two WRITE decisions were
`env-var` alone, so a CIP handed a hyperpipeline composite with the environment
variable unset consumed one format and emitted the other.

The fix was to route all three through the single `_use_hpip`.  This test pins
that invariant rather than the specific behaviour, because the invariant is
what stops the two halves drifting apart again -- which is the same failure
shape as the ILE grid-loader divergence (see test_grid_loader_parity.py).
"""

import ast
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CIP = os.path.abspath(os.path.join(
    HERE, "..", "bin", "util_ConstructIntrinsicPosterior_GenericCoordinates.py"))

DECISION_NAME = "_use_hpip"


@pytest.fixture(scope="module")
def source():
    with open(CIP) as stream:
        return stream.read()


def test_the_format_decision_is_made_exactly_once(source):
    tree = ast.parse(source)
    assignments = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == DECISION_NAME
                for t in node.targets)]
    assert len(assignments) == 1, (
        "{} is assigned {} times; the format decision must be made once or the "
        "reader and the writers can disagree".format(
            DECISION_NAME, len(assignments)))


def test_no_site_re_derives_the_decision_from_the_environment(source):
    """`is_active()` outside the single decision is how the halves diverged."""
    lines = [(i + 1, line) for i, line in enumerate(source.splitlines())
             if "is_active()" in line]
    offenders = [(i, line.strip()) for i, line in lines
                 if DECISION_NAME not in line]
    assert not offenders, (
        "these lines consult the environment variable directly instead of the "
        "single decision, so they can disagree with how the input was read: "
        "{}".format(offenders))


def test_the_decision_consults_both_the_env_var_and_the_input(source):
    line = next(l for l in source.splitlines()
                if l.strip().startswith(DECISION_NAME + " ="))
    assert "is_active()" in line and "sniff(" in line, (
        "the format decision must consider both the opt-in environment "
        "variable and the actual input file: " + line.strip())


def test_every_use_is_the_shared_decision(source):
    """At least one reader site and one writer site, all on the same name."""
    uses = [i + 1 for i, line in enumerate(source.splitlines())
            if DECISION_NAME in line]
    assert len(uses) >= 3, (
        "expected the decision to be consulted by the reader and both writers; "
        "found {} references".format(len(uses)))
