"""The archived PESummary page must publish every posterior it labels.

RO's decision (2026-08-23) was to publish BOTH the pre-calibration and the
calibration-reweighted posterior rather than choose between them.  The first
implementation emitted one ``--samples`` flag per file, which reads correctly
and is wrong: pesummary's ``--samples`` is a plain store action with
``nargs='+'``, not ``append``, so the second occurrence REPLACES the first.
The page then showed a single posterior carrying two labels -- the reweighted
one, labelled as if it were the pre-calibration one.

The gate that was supposed to cover this asserted on the argument STRING, so it
passed: both filenames were present, just not in a form pesummary would honour.
These tests parse the string with pesummary's own parser instead, because the
question is what pesummary does with it, not what it contains.
"""

import os
import shlex

import pytest

pesummary_parser = pytest.importorskip(
    "pesummary.core.cli.parser",
    reason="pesummary is required to check what pesummary does with our args")


def _parse(arg_string):
    parser = pesummary_parser.ArgumentParser()
    parser.add_known_options_to_parser(["--samples", "--labels"])
    namespace, _ = parser.parse_known_args(shlex.split(arg_string))
    return namespace


def _sample_files(tmp_path, count):
    paths = []
    for index in range(count):
        path = tmp_path / "posterior_{}.dat".format(index)
        path.write_text("# lnL\n")
        paths.append(str(path))
    return paths


def test_repeated_samples_flag_silently_drops_all_but_the_last(tmp_path):
    """Pin the upstream behaviour this guards against, so it stays visible.

    If a future pesummary makes --samples an append action this fails, and the
    right response is to delete this test -- not to reintroduce the old form
    on the strength of a remembered behaviour.
    """
    first, second = _sample_files(tmp_path, 2)
    namespace = _parse(
        " --samples {} --samples {} --labels a b".format(first, second))
    assert namespace.samples == [second]
    assert len(namespace.labels) == 2


def test_one_samples_flag_carries_every_file(tmp_path):
    first, second = _sample_files(tmp_path, 2)
    namespace = _parse(
        " --samples {} {} --labels a b".format(first, second))
    assert namespace.samples == [first, second]
    assert len(namespace.samples) == len(namespace.labels)


@pytest.mark.parametrize("count", [1, 2, 3])
def test_generated_plot_arguments_label_every_posterior(tmp_path, count):
    """The invariant, in the form pseudo_pipe builds it.

    Mirrors the construction in util_RIFT_pseudo_pipe.py: one --samples flag
    taking every file, one --labels flag taking every label.  What matters is
    that pesummary ends up with as many posteriors as names for them.
    """
    samples = _sample_files(tmp_path, count)
    labels = ["run"] + ["run_extra{}".format(i) for i in range(count - 1)]
    arg_string = (" --samples {} ".format(" ".join(samples))
                  + " --labels {} ".format(" ".join(labels)))
    namespace = _parse(arg_string)
    assert namespace.samples == samples
    assert len(namespace.samples) == len(namespace.labels) == count
