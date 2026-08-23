"""The extrinsic policy must exist once (item M7, step 1).

Both pipeline builders turn the intrinsic ILE arguments into the extrinsic ones
and both generate the shell that collects the result.  They used to do it
separately, and had already drifted: BasicIteration takes the LAST `--n-eff` in
the argument string, while the Hyperpipe branch maxed over EVERY `--n-eff` in
the string and the `--ile-n-eff` option as well.  Those agree only when the
option and the string agree; when they disagree the two builders request a
different number of extrinsic samples per job, which is the deliverable.

`RIFT.misc.extrinsic_stage` is now the single implementation, with
BasicIteration's semantics -- it is the builder production runs on.  These
tests pin the semantics, and pin that both call sites actually use it, since a
shared helper nobody calls is worse than no helper at all.

The parity ledger's `allinone_convert.sh` / `terminal_collect_extrinsic.sh`
entry can move from "artifact" (two generators verified to agree once) to
"shared-code" on the strength of this file.
"""

import ast
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, CODE)

from RIFT.misc import extrinsic_stage

BUILDERS = {
    "BasicIteration": os.path.join(
        CODE, "bin", "create_event_parameter_pipeline_BasicIteration"),
    "Hyperpipe (pseudo_pipe)": os.path.join(
        CODE, "bin", "util_RIFT_pseudo_pipe.py"),
}


# ---------------------------------------------------------------- semantics

def test_n_eff_takes_the_last_value_in_the_argument_string():
    """Last wins: a later override in a composed string beats an earlier one."""
    assert extrinsic_stage.extrinsic_n_eff("--n-eff 50 --n-eff 3", 5) == 5
    assert extrinsic_stage.extrinsic_n_eff("--n-eff 3 --n-eff 50", 5) == 50


def test_n_eff_floors_at_the_per_worker_sample_count():
    assert extrinsic_stage.extrinsic_n_eff("--no-n-eff-here", 40) == 40
    assert extrinsic_stage.extrinsic_n_eff("--n-eff 10", 40) == 40
    assert extrinsic_stage.extrinsic_n_eff("--n-eff 100", 40) == 100


def test_n_eff_does_not_consult_a_maximum_over_all_values():
    """The drifted Hyperpipe form would return 50 here; BasicIteration's is 5."""
    assert extrinsic_stage.extrinsic_n_eff("--n-eff 50 --n-eff 3", 5) == 5


@pytest.mark.parametrize("kwargs", [
    {"export_marginal_distance_grid": True},
    {"export_distance_slices": 4},
    {"time_resampling": True},
])
def test_distance_marginalization_is_stripped_by_every_reason(kwargs):
    """Three separate stages each need the per-distance likelihood."""
    args, _ = extrinsic_stage.derive_extrinsic_ile_args(
        "--distance-marginalization --foo", 5, **kwargs)
    assert "--distance-marginalization " not in args


def test_cross_point_adaptation_is_disabled():
    args, _ = extrinsic_stage.derive_extrinsic_ile_args(
        "--no-adapt-after-first --foo", 5)
    assert "--no-adapt-after-first" not in args


def test_a_false_or_none_slice_option_is_omitted():
    """The callers' predicates differ per option; the helper must not add its own."""
    args, _ = extrinsic_stage.derive_extrinsic_ile_args(
        "--foo", 5, export_distance_slices=3,
        distance_slice_options={"--n-distance-slice-core": None,
                                "--distance-slice-randomize": False,
                                "--distance-slice-wing-neff": 0,
                                "--distance-slice-all-fresh": True})
    assert "--n-distance-slice-core" not in args
    assert "--distance-slice-randomize" not in args
    assert "--distance-slice-wing-neff 0" in args   # 0 is a real value here
    assert "--distance-slice-all-fresh" in args


def test_the_collector_emits_the_glob_verbatim():
    """The caller owns the quoting; quoting here broke `$1` expansion."""
    lines = extrinsic_stage.extrinsic_collect_commands(
        "join", "convert", "--conv", "./iteration_$1_ile/'EXTR_out-*'",
        "tmp.xml.gz", "tmp.dat", "out.dat", "| shuf")
    assert "./iteration_$1_ile/'EXTR_out-*'" in lines[0]
    assert lines[0].count("'") == 2


def test_the_collector_shuffles_the_body_but_keeps_the_header():
    lines = extrinsic_stage.extrinsic_collect_commands(
        "join", "convert", "", "g", "t.xml", "t.dat", "o.dat", "| shuf")
    assert lines[2].startswith("head -n 1 ")
    assert lines[3].startswith("sed 1d ") and "| shuf" in lines[3]


def test_frame_rotation_is_guarded_against_a_second_pass():
    lines = extrinsic_stage.frame_rotation_commands(
        "rotate", "post.dat", "post_orig.dat", 20)
    body = "\n".join(lines)
    assert body.startswith("if [ ! -e post_orig.dat ]; then")
    assert "post_orig.dat" in body
    assert "post_orig .dat" not in body   # the stray space that made it inert


# ------------------------------------------------------------- both callers

@pytest.mark.parametrize("label", sorted(BUILDERS))
def test_the_builder_uses_the_shared_module(label):
    with open(BUILDERS[label]) as stream:
        text = stream.read()
    assert "extrinsic_stage" in text, (
        "{} does not import RIFT.misc.extrinsic_stage".format(label))
    assert "derive_extrinsic_ile_args" in text, (
        "{} does not call the shared argument transform".format(label))
    assert "extrinsic_collect_commands" in text, (
        "{} does not call the shared collector generator".format(label))


def _regex_literals_mentioning(text, needle):
    """String literals passed to `re.*` whose content mentions *needle*.

    Parsed, not grepped.  A line-based version of this check missed the very
    mutation it was written for, because `re.findall(` and the pattern were on
    different lines -- which is how the code is actually formatted.
    """
    found = []
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = None
        if isinstance(func, ast.Attribute):
            name = func.attr
            if not (isinstance(func.value, ast.Name) and func.value.id == "re"):
                name = None
        elif isinstance(func, ast.Name):
            name = func.id
        if name not in ("findall", "search", "match", "fullmatch", "sub",
                        "parse_ile_args_lazy"):
            continue
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                if needle in arg.value:
                    found.append((getattr(node, "lineno", "?"), arg.value))
    return found


@pytest.mark.parametrize("label", sorted(BUILDERS))
def test_the_builder_does_not_re_derive_n_eff(label):
    """A local --n-eff scrape is how the two drifted in the first place."""
    with open(BUILDERS[label]) as stream:
        text = stream.read()
    offenders = _regex_literals_mentioning(text, "n-eff")
    assert not offenders, (
        "{} matches --n-eff with its own regex instead of using "
        "extrinsic_stage: {}".format(label, offenders))


@pytest.mark.parametrize("label", sorted(BUILDERS))
def test_the_builder_does_not_re_derive_the_extrinsic_flag_ladder(label):
    """The distance-slice option ladder must not be duplicated either."""
    with open(BUILDERS[label]) as stream:
        text = stream.read()
    for flag in ("--distance-slice-wing-neff", "--n-distance-slice-core",
                 "--fairdraw-extrinsic-output-n-max"):
        assert text.count('"{}'.format(flag)) + text.count("'{}".format(flag)) \
            <= 1, (
            "{} still builds the {} argument itself; it belongs in "
            "extrinsic_stage".format(label, flag))
