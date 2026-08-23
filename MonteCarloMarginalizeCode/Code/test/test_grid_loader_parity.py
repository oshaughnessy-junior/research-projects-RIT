"""ILE's two grid loaders must configure the template identically.

`integrate_likelihood_extrinsic_batchmode` can be handed its intrinsic points
either as `--sim-xml` (ligolw) or `--sim-grid` (ASCII).  Each branch loads the
points and then fixes up the resulting ChooseWaveformParams.  If one branch
sets something the other does not, the SAME analysis means different things
depending on which format it was handed -- silently, because nothing compares
them.

That is not hypothetical.  `--approximant` was applied only on the XML side, so
a --sim-grid run ignored the requested waveform and used whatever the grid's P
carried, in practice ChooseWaveformParams' TaylorT4 default.  Since RIFT PR
#181 makes the ASCII grid the only format the Hyperpipe builder emits, every
real-likelihood run through it would have been analyzed with the wrong
waveform.  Where TaylorT4 then produced too few modes, ILE raised
`KeyError: (2, -1)`, printed "FAILED ANALYSIS", wrote no output -- and exited
zero.

This test reads the source rather than running ILE: the divergence is a
property of the code, it costs nothing to check, and a runtime test would need
frames, a PSD and a minute of CPU per point.  The companion runtime evidence is
in the paper repo under analyses/hyperpipe_builder_fidelity.
"""

import ast
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ILE = os.path.join(HERE, "..", "bin", "integrate_likelihood_extrinsic_batchmode")

#: Attributes the XML branch sets that the grid branch legitimately need not.
#: Every entry needs a reason; an empty exception list is the goal.
DECLARED_EXCEPTIONS = {
    # The XML branch reads masses already in SI from the sim_inspiral table.
    # The grid branch converts them itself and then applies the historical
    # `if P.m1 < 1e15` safety net, so it assigns m1/m2 for a different reason.
    "m1", "m2",
}


def _assigned_attributes(tree, marker_call, marker_attr):
    """Names assigned as ``P.<name> = ...`` inside the block that mentions *marker*."""
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if (isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "P"):
                found.add(target.attr)
    return found


def _branch_source(text, start_marker, end_marker):
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    return text[start:end]


@pytest.fixture(scope="module")
def sources():
    with open(ILE) as stream:
        text = stream.read()
    xml_block = _branch_source(text, "if opts.sim_xml:", "elif opts.sim_grid:")
    grid_block = _branch_source(text, "  # Common per-P setup",
                                "  P = P_list[0]")
    return xml_block, grid_block


def _attrs(block):
    return _assigned_attributes(ast.parse(_dedent(block)), None, None)


def _dedent(block):
    lines = block.splitlines()
    body = [l for l in lines if l.strip()]
    indent = min(len(l) - len(l.lstrip()) for l in body) if body else 0
    return "\n".join(l[indent:] if len(l) >= indent else l for l in lines)


def test_grid_branch_sets_everything_the_xml_branch_sets(sources):
    xml_block, grid_block = sources
    xml_attrs = _attrs(xml_block)
    grid_attrs = _attrs(grid_block)
    missing = sorted(xml_attrs - grid_attrs - DECLARED_EXCEPTIONS)
    assert not missing, (
        "the --sim-xml branch configures {} and the --sim-grid branch does "
        "not. The same analysis therefore means different things depending on "
        "the grid format it was handed. Either set them in the common block "
        "or add each to DECLARED_EXCEPTIONS with a reason.".format(missing))


def test_the_approximant_override_is_present_in_both(sources):
    """Pin the specific divergence that motivated this file."""
    for name, block in zip(("sim_xml", "sim_grid"), sources):
        assert "GetApproximantFromString(opts.approximant)" in block, (
            "the {} branch does not honour --approximant".format(name))


def test_exception_list_is_not_a_dumping_ground():
    assert len(DECLARED_EXCEPTIONS) <= 4, (
        "the declared-exceptions list has grown; each entry is a place the two "
        "loaders are allowed to disagree, so it should shrink, not grow")
