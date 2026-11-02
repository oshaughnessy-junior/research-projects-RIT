"""
RIFT.misc.waveform_transform_pipeline
=====================================

Pipeline-build side of ILE's extra-parameter seam (``--supplementary-waveform-transform-*``,
see :mod:`RIFT.likelihood.hlm_transform`), for ``util_RIFT_pseudo_pipe.py``'s
``--internal-ile-supplementary-waveform-transform-*`` passthroughs.

Everything here runs on the SUBMIT host, at DAG-build time, so that a configuration ILE
would refuse on a worker is refused before a workflow exists:

  * :func:`build_time_refusals` -- builder and option combinations the seam cannot serve;
  * :func:`declared_parameters` -- the extra-parameter (lambda) names, from the plugin's
    ``EXTRA_PARAMETERS`` plus the explicit ``-parameter`` list (the plugin module is
    imported to read them; ``prepare`` is NOT called);
  * :func:`check_grid` -- the initial grid carries every declared column, finite;
  * :func:`ile_arguments` -- the ILE flags, with the plugin and ini added to the worker
    transfer list (absolute path) and referenced by BASENAME under ``--use-osg``, the
    convention ``--internal-ile-glitch-basis-file`` and the PSD files use;
  * :func:`ile_argument_refusals` -- the final ILE argument line reaches NoLoop and carries
    nothing ILE's seam refuses.

The iterative loop's posterior (EOS_POST = GenericCoordinates CIP) and PUFF stages do not
carry lambda columns (generic-grid CIP input is separate work);
:data:`POSTERIOR_STAGE_BLOCKER` is the message pseudo_pipe uses to
refuse the DAG once the MARG worker inputs are written.
"""

import os
import shlex

from RIFT.misc import plugin_loader

PREFIX = "--internal-ile-supplementary-waveform-transform"
ILE_PREFIX = "--supplementary-waveform-transform"
NOLOOP_FLAGS = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")

POSTERIOR_STAGE_BLOCKER = (
    "pseudo_pipe: {prefix}-*: the Hyperpipe loop's posterior stage (EOS_POST, run by "
    "util_ConstructIntrinsicPosterior_GenericCoordinates.py) and its puff stage "
    "(util_ParameterPuffball.py) cannot yet carry the extra parameters {names}: CIP's "
    "Hyperpipe reader keeps intrinsic columns only, so it would pool rows with different "
    "values, and PUFF reads intrinsic parameters only.  No DAG was built.  The MARG worker inputs are written for inspection "
    "or for a hand-built create_eos_posterior_pipeline with a lambda-aware posterior/puff "
    "stage: {files}.")


def requested(opts):
    """True if any ``--internal-ile-supplementary-waveform-transform-*`` option is set."""
    return bool(opts.internal_ile_supplementary_waveform_transform_code
                or opts.internal_ile_supplementary_waveform_transform_function
                or opts.internal_ile_supplementary_waveform_transform_ini
                or _as_list(opts.internal_ile_supplementary_waveform_transform_parameter))


def _as_list(value):
    # append-action gives a list on the CLI; an ini's eval() of a quoted scalar gives a str.
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


def build_time_refusals(opts):
    """Reasons this pseudo_pipe configuration cannot carry the seam (empty: none)."""
    code = opts.internal_ile_supplementary_waveform_transform_code
    bad = []
    if not code:
        bad.append("{0}-function/-ini/-parameter require {0}-code".format(PREFIX))
        return bad
    if opts.pipeline_builder != "Hyperpipe":
        bad.append("--pipeline-builder {} (extra parameters are columns of self-describing "
                   "Hyperpipe grids; only --pipeline-builder Hyperpipe can carry them)".format(
                       opts.pipeline_builder or "BasicIteration (default)"))
    if getattr(opts, "use_jax_ile", False):
        bad.append("--use-jax-ile (the JAX ILE driver refuses the seam)")
    if getattr(opts, "add_extrinsic", False):
        bad.append("--add-extrinsic (the terminal extrinsic fan-out would inherit the seam, but "
                   "ILE refuses --save-samples with it: the XML sample export cannot carry "
                   "extra parameters)")
    if not getattr(opts, "manual_initial_grid", None):
        bad.append("no --manual-initial-grid (the generated initial grid has no extra-parameter "
                   "columns; supply a Hyperpipe .dat grid that carries them)")
    if plugin_loader.spec_looks_like_path(code):
        if not os.path.isfile(os.path.expanduser(code)):
            bad.append("{}-code {}: no such file".format(PREFIX, code))
    elif getattr(opts, "use_osg", False):
        bad.append("{}-code {} is a dotted module name, which cannot be staged to OSG workers; "
                   "pass the plugin's file path (it is transferred and loaded by path)".format(
                       PREFIX, code))
    ini = opts.internal_ile_supplementary_waveform_transform_ini
    if ini and not os.path.isfile(os.path.expanduser(ini)):
        bad.append("{}-ini {}: no such file".format(PREFIX, ini))
    return bad


def declared_parameters(opts):
    """Extra-parameter names: plugin ``EXTRA_PARAMETERS`` then ``-parameter``, de-duplicated.

    Imports the plugin (by path, or by dotted name on a shared-filesystem build) to read the
    attribute; the plugin's ``prepare`` is not called here."""
    code = opts.internal_ile_supplementary_waveform_transform_code
    spec = os.path.abspath(os.path.expanduser(code)) if plugin_loader.spec_looks_like_path(code) else code
    module = plugin_loader.load_module_from_spec(
        spec, flag=PREFIX + "-code", module_prefix="_rift_pp_wf_transform_")
    function = opts.internal_ile_supplementary_waveform_transform_function or "transfer_function"
    if not callable(getattr(module, function, None)):
        raise ValueError("{}-code {}: no callable {!r}".format(PREFIX, code, function))
    names = []
    for name in (list(getattr(module, "EXTRA_PARAMETERS", ()) or ())
                 + _as_list(opts.internal_ile_supplementary_waveform_transform_parameter)):
        if name not in names:
            names.append(name)
    return names


def check_grid(grid_path, names, hpio):
    """Errors (empty: none) for a Hyperpipe grid that must carry every name in *names*."""
    import numpy
    if not names:
        return ["the plugin declares no EXTRA_PARAMETERS and no {}-parameter was given".format(PREFIX)]
    table, columns = hpio.read_table(grid_path)
    missing = [n for n in names if n not in columns]
    if missing:
        return ["initial grid {} lacks extra-parameter column(s) {} (columns: {})".format(
            grid_path, missing, list(columns))]
    bad = [n for n in names if not numpy.all(numpy.isfinite(numpy.atleast_1d(table[n])))]
    if bad:
        return ["initial grid {} has non-finite values in {}".format(grid_path, bad)]
    return []


def ile_arguments(opts, transfer_files, use_osg):
    """``(argument_text, transfer_files)`` for the ILE line.

    *transfer_files* is pseudo_pipe's comma-separated ``ile_additional_files_to_transfer``
    (or ``None``); the plugin and ini are appended to it as absolute paths and referenced by
    basename when *use_osg*.  A basename collision with an existing transfer entry is an
    error, as for the glitch-basis files."""
    files = [f for f in (transfer_files or "").split(",") if f.strip()]
    text = ""

    def _stage(path, flag):
        nonlocal files
        source = os.path.abspath(os.path.expanduser(path))
        for other in files:
            other_abs = os.path.abspath(os.path.expanduser(other.strip()))
            if (use_osg and os.path.basename(other_abs) == os.path.basename(source)
                    and os.path.realpath(other_abs) != os.path.realpath(source)):
                raise ValueError("{}: OSG transfer basename collision: {} and {}".format(
                    flag, other_abs, source))
        files.append(source)
        return os.path.basename(source) if use_osg else source

    code = opts.internal_ile_supplementary_waveform_transform_code
    if plugin_loader.spec_looks_like_path(code):
        code = _stage(code, PREFIX + "-code")
    text += " {}-code {} ".format(ILE_PREFIX, shlex.quote(code))
    if opts.internal_ile_supplementary_waveform_transform_function:
        text += " {}-function {} ".format(
            ILE_PREFIX, shlex.quote(opts.internal_ile_supplementary_waveform_transform_function))
    if opts.internal_ile_supplementary_waveform_transform_ini:
        text += " {}-ini {} ".format(ILE_PREFIX, shlex.quote(
            _stage(opts.internal_ile_supplementary_waveform_transform_ini, PREFIX + "-ini")))
    for name in _as_list(opts.internal_ile_supplementary_waveform_transform_parameter):
        text += " {}-parameter {} ".format(ILE_PREFIX, shlex.quote(name))
    return text, (",".join(files) if files else None)


# ILE flags the seam refuses (bin/integrate_likelihood_extrinsic_batchmode, the
# "unsupported in v1" list), as they would appear on a pseudo_pipe-built ILE line.
_REFUSED_ILE_FLAGS = (
    "--sim-xml", "--calibration-envelope-directory", "--calibration-dump-responsibilities",
    "--rotation-slow", "--freqresponse", "--early-time-multirate", "--zero-likelihood",
    "--save-samples", "--export-marginal-distance-grid", "--export-distance-slices",
    "--rom-group", "--rom-param", "--rom-use-basis", "--rom-integrate-intrinsic",
    "--nr-lookup", "--nr-group",
)


def ile_argument_refusals(line):
    """Reasons the final ILE argument *line* would be refused by the seam (empty: none)."""
    tokens = shlex.split(line)
    flags = set(t.split("=", 1)[0] for t in tokens if t.startswith("--"))
    bad = ["missing {} (the seam runs only on the NoLoop likelihood)".format(f)
           for f in NOLOOP_FLAGS if f not in flags]
    bad += ["{} on the ILE line".format(f) for f in _REFUSED_ILE_FLAGS if f in flags]
    for i, tok in enumerate(tokens):
        value = tokens[i + 1] if i + 1 < len(tokens) else None
        if tok.startswith("--internal-ile-inner-product"):
            value = tok.split("=", 1)[1] if "=" in tok else value
            if value != "fd-diagonal":
                bad.append("--internal-ile-inner-product {}".format(value))
        if tok.startswith("--time-marginalization-quadrature"):
            value = tok.split("=", 1)[1] if "=" in tok else value
            if value == "peak-local":
                bad.append("--time-marginalization-quadrature peak-local")
    return bad
