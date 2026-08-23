"""The stable terminal-stage names, and the file each one produces.

`util_RIFT_pseudo_pipe.py --pipeline-builder Hyperpipe` compiles GW policy into
a `terminal_stage_specs.json` manifest.  With `--terminal-stage-extra-file`, a
user manifest can attach its own stages to that graph by naming a generated
stage in `depends_on` and reading the file that stage writes.  Those two things
-- the stage name and the product filename -- are therefore an interface, not an
implementation detail, and renaming either silently detaches every downstream
user stage: the DAG still builds, the stage still runs, and it reads a file
that is now stale or absent.

Nothing consumes this table at build time; pseudo_pipe does not read it to
decide anything.  It exists so that a *test* can fail when a name changes,
which is the only mechanism that makes the promise real.  Keep it in step with
the manifest emitted by pseudo_pipe -- `test_terminal_stage_products.py`
compares the two and fails on either direction of drift.

`product` is relative to the run directory, or None where a stage's output is
not a single named file (a fan-out, or a directory of pages).  Stages appear
only when the options that create them are given; `condition` records which.
"""

from __future__ import annotations

import collections


StageProduct = collections.namedtuple(
    "StageProduct", ["product", "condition", "note"])


#: stage name -> what it writes, and when the stage exists at all.
TERMINAL_STAGE_PRODUCTS = {
    "extrinsic_samples": StageProduct(
        None, "--add-extrinsic",
        "fan-out; writes EXTR_out.xml_*_.dat under extrinsic_dir, one set per"
        " worker.  No single product file, so a user stage should depend on"
        " extrinsic_collect instead unless it globs."),
    "extrinsic_collect": StageProduct(
        "extrinsic_posterior_samples.dat", "--add-extrinsic",
        "joins the fan-out into the extrinsic posterior."),
    "frame_rotation": StageProduct(
        "extrinsic_posterior_samples.dat", "--internal-rotate-frames",
        "rewrites extrinsic_collect's product in place; depend on this rather"
        " than on extrinsic_collect when it is enabled, or you read the"
        " unrotated frame."),
    "bilby_pickle": StageProduct(
        "calmarg/data/calmarg_data_dump.pickle",
        "--calibration-reweighting without --bilby-pickle-file",
        "generates the data dump calibration reweighting consumes."),
    "calibration_reweight": StageProduct(
        "reweighted_posterior_samples.dat", "--calibration-reweighting",
        "with --calibration-reweighting-batchsize this instead writes"
        " per-batch files under weight_files/ and calibration_merge makes the"
        " named product."),
    "calibration_merge": StageProduct(
        "reweighted_posterior_samples.dat",
        "--calibration-reweighting-batchsize",
        "rejection-samples the batched weights into one posterior."),
    "posterior_hdf5": StageProduct(
        "posterior_samples.h5", "--distance-reweighting",
        "ASCII posterior converted for the skymap tools."),
    "comoving_distance_reweight": StageProduct(
        "cosmo_reweight.h5", "--distance-reweighting",
        "uniform-in-comoving-volume reweighting of posterior_samples.h5."),
    "pesummary": StageProduct(
        None, "--archive-pesummary-label",
        "writes a directory of pages, not a file; the paths come from"
        " args_plot.txt."),
    "distance_grid": StageProduct(
        "all_dgrid.dat", "--export-marginal-distance-grid", ""),
    "distance_slices": StageProduct(
        "all_dslice.dat", "--export-distance-slices", ""),
}

#: The stage that carries the final posterior, as a function of what ran.
#: The last entry whose option is active is the one to depend on.  This is the
#: same precedence pseudo_pipe applies when it wires `pesummary`.
POSTERIOR_PRODUCT_PRECEDENCE = (
    "extrinsic_collect",
    "frame_rotation",
    "calibration_reweight",
    "calibration_merge",
)


def stage_names():
    """Every stage name pseudo_pipe may emit, sorted."""
    return sorted(TERMINAL_STAGE_PRODUCTS)


def product_of(name):
    """The file a stage writes, or None.  KeyError for an unknown stage."""
    return TERMINAL_STAGE_PRODUCTS[name].product
