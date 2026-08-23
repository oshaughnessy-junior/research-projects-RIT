"""One implementation of the terminal extrinsic policy.

Both pipeline builders turn the intrinsic ILE argument string into the
*extrinsic* one, and both generate the shell that collects what those jobs
produce.  Until this module existed each did it separately --
``create_event_parameter_pipeline_BasicIteration`` by string surgery on its
``ile_args``, and the Hyperpipe branch of ``util_RIFT_pseudo_pipe.py`` by
re-deriving the same transform with regular expressions over the args file it
had just written.

They had already drifted.  BasicIteration takes the LAST ``--n-eff`` appearing
in the argument string and maxes it against the per-worker sample count; the
Hyperpipe branch maxed over *every* ``--n-eff`` in the string *and* the
``--ile-n-eff`` pseudo-pipe option.  Those agree only when the option and the
argument string agree, and when they disagree the two builders request
different numbers of extrinsic samples per job -- which is the deliverable.

The semantics here are BasicIteration's, deliberately.  It is the builder
production runs on and the one whose outputs the published analyses were made
with; the generic writer is what moves.

Nothing in this module knows about schedulers, submit files or DAG topology.
The two builders place these commands differently -- BasicIteration inside its
per-iteration ILE directory, Hyperpipe in a terminal stage graph -- and that
difference is structural.  What must not differ is what gets run.
"""

from __future__ import absolute_import, division, print_function

import re


#: Matches ``--n-eff <value>`` or ``--n-eff=<value>``.
_N_EFF = re.compile(r"--n-eff(?:=|\s+)([0-9.eE+-]+)")


def parse_last_n_eff(ile_args):
    """The last ``--n-eff`` in *ile_args*, or None.

    Mirrors ``parse_ile_args_lazy(args, 'n-eff')``: last occurrence wins, so a
    later override in a composed argument string beats an earlier default.
    """
    matches = _N_EFF.findall(str(ile_args))
    if not matches:
        return None
    try:
        return int(float(matches[-1]))
    except ValueError:
        return None


def extrinsic_n_eff(ile_args, samples_per_ile):
    """Convergence target for the extrinsic ILE jobs.

    ``min(2*N, N)`` is ``N``: both operands are the same option.  The comment
    at the original site says the cap exists "in case user requests very few
    extrinsic workers", so the intent was presumably a cap against a different
    quantity and the guard has always been a no-op.  It is reproduced rather
    than repaired, because repairing it would change how many samples every
    production run requests, which is a decision for whoever owns that number.
    """
    n_eff = min(2 * int(samples_per_ile), int(samples_per_ile))
    parsed = parse_last_n_eff(ile_args)
    if parsed is not None:
        n_eff = max(n_eff, parsed)
    return int(n_eff)


def derive_extrinsic_ile_args(ile_args, samples_per_ile,
                              export_marginal_distance_grid=False,
                              export_distance_slices=None,
                              distance_slice_options=None,
                              time_resampling=False):
    """Transform intrinsic ILE arguments into the extrinsic ones.

    Returns ``(args, n_eff)``.  *distance_slice_options* is a mapping of
    ``flag -> value`` (value ``True`` for a bare flag, ``None`` or ``False`` to
    omit), keeping the per-option ladder in the callers where the option names
    live.

    **The caller decides what to omit, and must.**  Upstream the predicates are
    not uniform: ``--n-distance-slice-core`` and ``--n-distance-slice-wing``
    are guarded on truthiness, so a value of 0 is dropped, while
    ``--distance-slice-wing-neff`` and friends are guarded on ``is not None``,
    so a 0 is emitted.  A first version of this function applied one rule to
    all of them and silently added ``--n-distance-slice-core 0`` to every
    distance-slice run; the byte-comparison against the pre-extraction build is
    what caught it.

    Three separate places strip ``--distance-marginalization``: distance-grid
    export, distance-slice export and time resampling each need the
    per-distance likelihood rather than the marginalized one.  The strip is
    idempotent, so applying it once per reason is harmless and keeps each
    reason legible.
    """
    n_eff = extrinsic_n_eff(ile_args, samples_per_ile)
    args = "{} --save-P 0.01 --save-samples --n-eff  {}".format(ile_args, n_eff)
    # Each extrinsic point must be independent -- of the sky location in
    # particular -- so the cross-point adaptation carried over from the
    # intrinsic run is disabled.
    args = args.replace('--no-adapt-after-first', '')

    if export_marginal_distance_grid:
        args += " --export-marginal-distance-grid "
        args = args.replace("--distance-marginalization ", ' ')

    if export_distance_slices:
        args += " --export-distance-slices {} ".format(export_distance_slices)
        for flag, value in (distance_slice_options or {}).items():
            if value is None or value is False:
                continue
            if value is True:
                args += " {} ".format(flag)
            else:
                args += " {} {} ".format(flag, value)
        if "--internal-use-lnL" not in args:
            args += " --internal-use-lnL "
        args = args.replace("--distance-marginalization ", ' ')

    if time_resampling:
        args += (" --resample-time-marginalization --fairdraw-extrinsic-output"
                 " --fairdraw-extrinsic-output-n-max {} ".format(
                     int(samples_per_ile)))
        args = args.replace("--distance-marginalization ", ' ')

    return args, n_eff


def extrinsic_collect_commands(join_exe, convert_exe, convert_args,
                               input_glob, tmp_xml, tmp_dat, output_dat,
                               shuffle_clause):
    """The four commands that turn extrinsic shards into a posterior file.

    join the per-shard XML, convert to the inference ASCII layout, keep the
    header, then shuffle the body.  The shuffle matters: downstream consumers
    truncate, and an unshuffled file is ordered by worker.

    *shuffle_clause* is a full pipeline fragment (``| shuf``, ``| sort -R``,
    or ``| cat``) because what is available differs by host, and the caller
    already resolves it.

    *input_glob* is emitted VERBATIM, quoting included, and the caller owns
    that quoting.  It differs between the two builders for a reason:
    BasicIteration's collector takes the iteration number as ``$1`` and must
    quote only the wildcard part (``./iteration_$1_ile/'EXTR_out-*'``) so the
    shell still expands the variable, while Hyperpipe passes a fully-resolved
    absolute path and quotes the lot.  Quoting it here broke the first of
    those, silently -- the collector would have looked in a directory
    literally named ``iteration_$1_ile``.
    """
    return [
        "{} {} --output {}".format(join_exe, input_glob, tmp_xml),
        "{} {} {} > {}".format(convert_exe, convert_args, tmp_xml, tmp_dat),
        "head -n 1 {} > {}".format(tmp_dat, output_dat),
        "sed 1d {} {} >> {}".format(tmp_dat, shuffle_clause, output_dat),
    ]


def frame_rotation_commands(rotate_exe, posterior, original, reference_freq):
    """Rotate the extrinsic posterior out of the J frame, once.

    Guarded on the ``_orig`` file so a DAG retry does not rotate twice.  The
    guard is why this is worth sharing: BasicIteration's copy carried a stray
    space in that filename (``..._orig .dat``) and the stage was therefore
    inert -- it "ran" on every calibration analysis and did nothing.
    """
    return [
        "if [ ! -e {} ]; then".format(original),
        "  mv {} {}".format(posterior, original),
        "  {} --extrinsic-posterior-file {} --fname-out {} --fref {}".format(
            rotate_exe, original, posterior, reference_freq),
        "fi",
    ]
