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


# ---------------------------------------------------------------------------
# Two mutations defeated the checks above, and both left the text intact:
#
#   * inverting ONE writer's guard (`if _use_hpip:` -> `if not _use_hpip:`)
#     restores exactly the pre-#181 split -- read one format, write the other --
#     while every mention count is unchanged;
#   * deleting the other writer's guard (`if _use_hpip:` -> `if False:`) drops
#     the mentions from 4 to 3, and the floor was `>= 3`.
#
# A count of mentions cannot see either. What distinguishes them is the SHAPE
# of each guard: the invariant is not "the name appears" but "every branch on
# it branches the same way".
# ---------------------------------------------------------------------------


def test_every_guard_on_the_decision_has_the_same_polarity(source):
    """`if _use_hpip:` everywhere -- never `not`, never a comparison.

    An inverted guard is the original defect verbatim: the reader and one
    writer then disagree about the format for the same input, which is how a
    CIP handed a hyperpipeline composite consumed one layout and emitted the
    other.
    """
    tree = ast.parse(source)
    guards = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        names = {child.id for child in ast.walk(node.test)
                 if isinstance(child, ast.Name)}
        if DECISION_NAME in names:
            guards.append(node.test)
    assert guards, "no branch on {} at all".format(DECISION_NAME)
    for test in guards:
        assert isinstance(test, ast.Name) and test.id == DECISION_NAME, (
            "a guard on {name} at line {line} is not the bare name but {kind}."
            " Every branch must read `if {name}:` -- an inverted or compound "
            "guard splits the read and write decisions again, which is the "
            "defect this file exists for.".format(
                name=DECISION_NAME, line=getattr(test, "lineno", "?"),
                kind=type(test).__name__))


def test_the_number_of_guards_is_pinned_exactly(source):
    """An exact count, not a floor.

    With a floor, deleting a writer's guard is invisible: the mentions drop
    from four to three and `>= 3` still holds, so the format decision silently
    stops applying to one of the two writers. If a guard is legitimately added
    or removed, update this number deliberately -- that is the point.
    """
    tree = ast.parse(source)
    guards = [node for node in ast.walk(tree)
              if isinstance(node, ast.If)
              and any(isinstance(child, ast.Name)
                      and child.id == DECISION_NAME
                      for child in ast.walk(node.test))]
    assert len(guards) == 3, (
        "expected exactly 3 branches on {} (one reader, two writers); found "
        "{} at lines {}".format(
            DECISION_NAME, len(guards),
            [getattr(node, "lineno", "?") for node in guards]))
