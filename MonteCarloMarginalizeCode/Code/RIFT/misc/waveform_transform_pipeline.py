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

The iterative loop's posterior (EOS_POST = GenericCoordinates CIP) and PUFF stages carry
lambda as named extra columns (see the CIP/PUFF ``--integration-parameter-range``,
``--extra-column-pin`` options).  For each declared lambda:

  * :func:`option_treatments` / :func:`cip_injection` / :func:`puff_injection` -- the
    ``--internal-supplementary-waveform-transform-parameter-{range,pin,reflect}``
    conveniences, turned into CIP/PUFF arguments that pseudo_pipe appends to every CIP
    iteration and to the puff line;
  * :func:`check_posterior_stage` -- the final ``args_cip_list.txt`` / ``args_puff.txt``
    lines (whatever produced them: the conveniences, ``--manual-extra-cip-args``,
    ``--manual-extra-puff-args``, an ini) give every lambda a treatment ILE can use next
    iteration: CIP fits it (``--parameter NAME`` + ``--integration-parameter-range
    NAME:[a,b]``) or pins it (``--extra-column-pin NAME:VALUE``), the same way on every
    line; ``--extra-column-ignore NAME`` is refused (CIP would drop the column and ILE needs
    it back); PUFF puffs a fit lambda in the same range or passes it through;
  * :func:`check_grid_treatments` -- the initial grid's lambda values lie in the ranges
    (and reach each pin).
"""

import ast
import math
import os
import shlex

from RIFT.misc import plugin_loader

PREFIX = "--internal-ile-supplementary-waveform-transform"
ILE_PREFIX = "--supplementary-waveform-transform"
NOLOOP_FLAGS = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")

RANGE_OPTION = "--internal-supplementary-waveform-transform-parameter-range"
PIN_OPTION = "--internal-supplementary-waveform-transform-parameter-pin"
REFLECT_OPTION = "--internal-supplementary-waveform-transform-parameter-reflect"


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
    # CIP and PUFF refuse lambda columns with these (EOS tables, eccentric-phase coordinates,
    # the mu-coordinate puff); refuse here, before any helper runs.
    if getattr(opts, "internal_tabular_eos_file", None):
        bad.append("--internal-tabular-eos-file (CIP refuses EOS fits with extra columns)")
    if (getattr(opts, "eccentric_phase_coordinates", False)
            or getattr(opts, "eccentric_phase_sampling", False)):
        bad.append("eccentric-phase coordinates (CIP and PUFF refuse them with extra columns)")
    if getattr(opts, "internal_puff_mu_random_transverse", False):
        bad.append("--internal-puff-mu-random-transverse (PUFF refuses the mu puff with extra "
                   "columns)")
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


def _fmt(x):
    return "{:.17g}".format(float(x))


def parse_range(spec, flag):
    """``NAME:[a,b]`` -> ``(NAME, (a, b))`` with finite a < b; ValueError otherwise."""
    name, sep, rng = str(spec).partition(":")
    try:
        if not (name and sep):
            raise ValueError
        lo, hi = [float(v) for v in ast.literal_eval(rng.strip())]
    except (ValueError, TypeError, SyntaxError):
        raise ValueError("{} {}: expected NAME:[a,b]".format(flag, spec))
    if not (math.isfinite(lo) and math.isfinite(hi) and lo < hi):
        raise ValueError("{} {}: expected finite a < b".format(flag, spec))
    return name, (lo, hi)


def parse_pin(spec, flag):
    """``NAME:VALUE`` -> ``(NAME, VALUE)``, VALUE finite; ValueError otherwise."""
    name, sep, val = str(spec).partition(":")
    try:
        if not (name and sep):
            raise ValueError
        value = float(val)
    except ValueError:
        raise ValueError("{} {}: expected NAME:VALUE".format(flag, spec))
    if not math.isfinite(value):
        raise ValueError("{} {}: VALUE must be finite".format(flag, spec))
    return name, value


def option_treatments(opts, names):
    """``(ranges, pins, reflect, errors)`` from the pseudo_pipe convenience options.

    *names* are the declared lambda; an option naming anything else, a name given twice, or
    a name both ranged and pinned is an error.  A reflected name must also be ranged."""
    ranges, pins, errors = {}, {}, []
    for spec in _as_list(getattr(opts, "internal_supplementary_waveform_transform_parameter_range", None)):
        try:
            name, rng = parse_range(spec, RANGE_OPTION)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if name in ranges:
            errors.append("{} {}: {} given twice".format(RANGE_OPTION, spec, name))
        ranges[name] = rng
    for spec in _as_list(getattr(opts, "internal_supplementary_waveform_transform_parameter_pin", None)):
        try:
            name, value = parse_pin(spec, PIN_OPTION)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if name in pins:
            errors.append("{} {}: {} given twice".format(PIN_OPTION, spec, name))
        pins[name] = value
    reflect = []
    for name in _as_list(getattr(opts, "internal_supplementary_waveform_transform_parameter_reflect", None)):
        if name not in ranges:
            errors.append("{} {}: reflection needs {} {}:[a,b]".format(REFLECT_OPTION, name, RANGE_OPTION, name))
        elif name not in reflect:
            reflect.append(name)
    for name in list(ranges) + list(pins):
        if name not in names:
            errors.append("{} is not a declared extra parameter (declared: {})".format(name, list(names)))
    for name in set(ranges) & set(pins):
        errors.append("{} is both ranged ({}) and pinned ({})".format(name, RANGE_OPTION, PIN_OPTION))
    return ranges, pins, reflect, errors


def cip_injection(ranges, pins):
    """CIP arguments for the convenience options (appended to every CIP iteration)."""
    text = ""
    for name, (lo, hi) in ranges.items():
        text += " --parameter {0} --integration-parameter-range {0}:[{1},{2}] ".format(name, _fmt(lo), _fmt(hi))
    for name, value in pins.items():
        text += " --extra-column-pin {}:{} ".format(name, _fmt(value))
    return text


def puff_injection(ranges, reflect):
    """PUFF arguments for the convenience options.  Pinned lambda are not puffed (pass-through)."""
    text = ""
    for name, (lo, hi) in ranges.items():
        text += " --parameter {0} --integration-parameter-range {0}:[{1},{2}] ".format(name, _fmt(lo), _fmt(hi))
        if name in reflect:
            text += " --reflect-parameter {} ".format(name)
    return text


def _option_pairs(line):
    """``[(flag, value_or_None), ...]`` for an argument line; a leading schedule prefix (CIP
    count / ``Z`` / ``G<n>``, PUFF ``X``) is dropped.  ``--flag=value`` and ``--flag value``
    are both recognized."""
    tokens = shlex.split(line)
    if tokens and not tokens[0].startswith("-"):
        tokens = tokens[1:]
    pairs = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.startswith("--") and "=" in tok:
            flag, value = tok.split("=", 1)
            pairs.append((flag, value))
        elif tok.startswith("--"):
            value = tokens[i + 1] if i + 1 < len(tokens) and not tokens[i + 1].startswith("--") else None
            pairs.append((tok, value))
            if value is not None:
                i += 1
        i += 1
    return pairs


# CIP refuses to FIT extra columns with these (util_ConstructIntrinsicPosterior_GenericCoordinates.py).
_CIP_REFUSED_WITH_FIT = ("--using-eos", "--tabular-eos-file", "--eccentric-phase-coordinates",
                         "--prior-in-integrand-correction", "--report-best-point")
# PUFF refuses any extra column with these (util_ParameterPuffball.py).
_PUFF_REFUSED = ("--puff-coordinate-system", "--eccentric-phase-coordinates")


def _named_values(pairs, flag):
    return [v for f, v in pairs if f == flag and v is not None]


def cip_line_treatments(line, names, where="CIP line"):
    """``(treatments, errors)`` for one CIP argument line.

    ``treatments[name]`` is ``("fit", (a, b))`` or ``("pin", value)``."""
    pairs = _option_pairs(line)
    errors, treat = [], {}
    params = _named_values(pairs, "--parameter")
    implied = _named_values(pairs, "--parameter-implied") + _named_values(pairs, "--parameter-nofit")
    ranges, pins = {}, {}
    for spec in _named_values(pairs, "--integration-parameter-range"):
        try:
            name, rng = parse_range(spec, "--integration-parameter-range")
        except ValueError as exc:
            errors.append("{}: {}".format(where, exc))
            continue
        if name in ranges and ranges[name] != rng:
            errors.append("{}: conflicting --integration-parameter-range for {}: {} and {}".format(
                where, name, list(ranges[name]), list(rng)))
        ranges[name] = rng
    for spec in _named_values(pairs, "--extra-column-pin"):
        try:
            name, value = parse_pin(spec, "--extra-column-pin")
        except ValueError as exc:
            errors.append("{}: {}".format(where, exc))
            continue
        if name in pins and pins[name] != value:
            errors.append("{}: conflicting --extra-column-pin for {}".format(where, name))
        pins[name] = value
    ignored = _named_values(pairs, "--extra-column-ignore")
    for name in names:
        if name in ignored:
            errors.append("{}: --extra-column-ignore {} (CIP would drop the column, and the next "
                          "iteration's ILE needs it; fit or pin it instead)".format(where, name))
            continue
        if name in implied:
            errors.append("{}: {} given as --parameter-implied/--parameter-nofit (an extra "
                          "parameter must be fit and sampled: --parameter {})".format(where, name, name))
            continue
        n_fit = params.count(name)
        if n_fit > 1:
            errors.append("{}: --parameter {} given {} times (option and manual args both?)".format(
                where, name, n_fit))
        if n_fit and name in pins:
            errors.append("{}: {} is both fit (--parameter) and pinned (--extra-column-pin)".format(where, name))
        elif n_fit:
            if name not in ranges:
                errors.append("{}: {} is fit (--parameter) without --integration-parameter-range "
                              "{}:[a,b] (or {} {}:[a,b])".format(where, name, name, RANGE_OPTION, name))
            else:
                treat[name] = ("fit", ranges[name])
        elif name in pins:
            treat[name] = ("pin", pins[name])
        else:
            errors.append("{}: no treatment for extra parameter {}: pass {} {}:[a,b] (fit) or {} "
                          "{}:VALUE, or by hand --parameter {} --integration-parameter-range "
                          "{}:[a,b] / --extra-column-pin {}:VALUE".format(
                              where, name, RANGE_OPTION, name, PIN_OPTION, name, name, name, name))
        if name in ranges and not n_fit:
            errors.append("{}: --integration-parameter-range for {} which is not fit (--parameter)".format(
                where, name))
    stray = [n for n in list(ranges) + list(pins) + ignored if n not in names]
    if stray:
        errors.append("{}: extra-column options for undeclared name(s) {} (declared: {})".format(
            where, sorted(set(stray)), list(names)))
    if any(t[0] == "fit" for t in treat.values()):
        flags = set(f for f, _ in pairs)
        errors += ["{}: {} (CIP refuses to fit extra columns with it)".format(where, f)
                   for f in _CIP_REFUSED_WITH_FIT if f in flags]
    return treat, errors


def puff_line_errors(line, names, treatments):
    """Errors (empty: none) for the PUFF argument line given the CIP *treatments*."""
    pairs = _option_pairs(line)
    errors = []
    flags = set(f for f, _ in pairs)
    errors += ["PUFF: {} (PUFF refuses it with extra columns)".format(f) for f in _PUFF_REFUSED if f in flags]
    params = _named_values(pairs, "--parameter")
    ranges = {}
    for spec in _named_values(pairs, "--integration-parameter-range"):
        try:
            name, rng = parse_range(spec, "--integration-parameter-range")
        except ValueError as exc:
            errors.append("PUFF: {}".format(exc))
            continue
        ranges[name] = rng
    for name in names:
        for flag in ("--random-parameter", "--append-with-random-parameter", "--downselect-parameter"):
            if name in _named_values(pairs, flag):
                errors.append("PUFF: {} {} (refused for an extra parameter; its range is "
                              "--integration-parameter-range)".format(flag, name))
        kind, value = treatments.get(name, (None, None))
        n_puff = params.count(name)
        if n_puff > 1:
            errors.append("PUFF: --parameter {} given {} times".format(name, n_puff))
        if n_puff:
            if kind == "pin":
                errors.append("PUFF: {} is pinned in CIP; puffing it would move rows off the pin, "
                              "which CIP then discards (omit it from PUFF: it passes through)".format(name))
            elif name not in ranges:
                errors.append("PUFF: {} is puffed (--parameter) without --integration-parameter-range "
                              "{}:[a,b]".format(name, name))
            elif kind == "fit" and tuple(ranges[name]) != tuple(value):
                errors.append("PUFF: --integration-parameter-range for {} is {} but CIP's prior range "
                              "is {}".format(name, list(ranges[name]), list(value)))
        elif name in ranges:
            errors.append("PUFF: --integration-parameter-range for {} which is not puffed (--parameter)".format(name))
        if name in _named_values(pairs, "--reflect-parameter") and not n_puff:
            errors.append("PUFF: --reflect-parameter {} without --parameter {}".format(name, name))
    return errors


def check_posterior_stage(cip_lines, puff_line, names):
    """``(treatments, errors)`` for the final CIP lines (every iteration) and PUFF line.

    Every declared lambda must be treated the same way on every CIP line (fit in one range,
    or pinned at one value), so the grid each CIP writes carries it back to ILE."""
    treatments, errors = {}, []
    lines = [l for l in cip_lines if l.strip()]
    if not lines:
        errors.append("no CIP argument lines")
    for i, line in enumerate(lines):
        treat, bad = cip_line_treatments(line, names, where="CIP line {}".format(i))
        errors += bad
        for name, t in treat.items():
            if name in treatments and treatments[name] != t:
                errors.append("CIP line {}: {} treated as {} but as {} on an earlier line (use one "
                              "range or pin throughout)".format(i, name, t, treatments[name]))
            treatments.setdefault(name, t)
    if puff_line is not None and not errors:
        errors += puff_line_errors(puff_line, names, treatments)
    return treatments, errors


def check_grid_treatments(grid_path, treatments, hpio, label="initial grid"):
    """Errors (empty: none): fit lambda inside its range on every row; each pin reached."""
    import numpy
    table, columns = hpio.read_table(grid_path)
    table = numpy.atleast_1d(table)
    errors = []
    for name, (kind, value) in treatments.items():
        if name not in columns:
            errors.append("{} {} lacks column {}".format(label, grid_path, name))
            continue
        col = numpy.asarray(table[name], dtype=float)
        if kind == "fit":
            lo, hi = value
            out = col[(col < lo) | (col > hi)]
            if len(out):
                errors.append("{} {}: {} values {} outside the declared range [{}, {}]".format(
                    label, grid_path, name, sorted(set(out.tolist()))[:5], lo, hi))
        else:
            if not numpy.any(numpy.abs(col - value) <= 1e-9 * max(1.0, abs(value))):
                errors.append("{} {}: no row has {} = {} (CIP keeps only rows at the pin)".format(
                    label, grid_path, name, value))
    return errors


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
