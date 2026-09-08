"""Pipeline-side validation and emission-refusal for --q-time-pregrid-factor.

WHAT THIS IS.  bin/integrate_likelihood_extrinsic_batchmode carries a certified, opt-in
Q_lm pregrid (PR #261): factor 8 reflects each finite Q window, FFT-interpolates it onto
an 8x finer grid once after packing, and evaluates detector arrival times off that dense
grid with four-tap cubic interpolation, while leaving the geocentric time-integration grid
at the data deltaT.  Factor 1 (the default) is the historical, unchanged path.  The driver
enforces this itself, at first-job time:

    if opts.q_time_pregrid_factor not in (1, 8):
        raise ValueError(...)
    if opts.q_time_pregrid_factor == 8:
        if not opts.vectorized or opts.rotation_slow or opts.freqresponse or opts.calibration_envelope_directory:
            raise NotImplementedError(...)
        if not opts._interp_time_from_default and opts._noloop_time_interp != "cubic":
            raise ValueError(...)

This module is the pipeline-side MIRROR of that guard -- the same discipline
RIFT.likelihood.time_marginalization_quadrature applies to --time-marginalization-quadrature
-- so a workflow build refuses an unhonourable request before a whole queue-slot cycle is
spent discovering it at the driver.  The choice tuple and the exact conflict wording are
defined HERE, not retyped at each pipeline call site, so the pipeline-side and driver-side
checks cannot silently drift apart.

THE ONE ENTANGLEMENT.  Factor 8 forces the arrival-time stencil to cubic.  That is silent
and harmless when the caller never named a stencil (the driver's own default path), but it
is refused when the caller passed an EXPLICIT --interpolate-time that is not itself cubic --
"remove the explicit --interpolate-time option or set it to cubic" -- because silently
overriding a stencil the user asked for by name is exactly the kind of inert-flag failure
this whole family of checks exists to rule out.
"""

Q_TIME_PREGRID_CHOICES = (1, 8)

ILE_Q_TIME_PREGRID_FLAG = '--q-time-pregrid-factor'
ILE_INTERPOLATE_TIME_FLAG = '--interpolate-time'
ILE_CALIBRATION_ENVELOPE_DIRECTORY_FLAG = '--calibration-envelope-directory'

# Legacy --interpolate-time spellings the ILE driver itself still accepts (see
# bin/integrate_likelihood_extrinsic_batchmode's _TI_LEGACY_BOOLEAN): a truthy value meant
# 'cubic', a falsy one meant 'nearest'.  Reproduced here only so a hand-passed
# --manual-extra-ile-args using the legacy spelling is not misread as "no stencil requested".
_LEGACY_TRUTHY = ("1", "true", "t", "yes", "y", "on")
_LEGACY_FALSY = ("0", "false", "f", "no", "n", "off", "none")

# The driver's own wording (bin/integrate_likelihood_extrinsic_batchmode, q_time_pregrid_factor
# == 8 branch), reproduced VERBATIM so a workflow-build-time refusal and the driver's own
# first-job refusal read identically.
STENCIL_CONFLICT_MESSAGE = (
    "--q-time-pregrid-factor 8 uses four-tap cubic interpolation; remove the "
    "explicit --interpolate-time option or set it to cubic")

_PIPELINE_REQUIRED_ILE_FLAGS = (
    ('--vectorized',
     'the driver restricts --q-time-pregrid-factor 8 to ordinary vectorized NoLoop'),
)
# Pure boolean flags: the driver reads these as opts.rotation_slow / opts.freqresponse
# directly, so presence on the command line is exactly the excluded condition.
_PIPELINE_EXCLUDING_ILE_FLAGS = (
    ('--rotation-slow',
     'the driver restricts --q-time-pregrid-factor 8 to ordinary vectorized NoLoop without rotation'),
    ('--freqresponse',
     'the driver restricts --q-time-pregrid-factor 8 to ordinary vectorized NoLoop without '
     'frequency-dependent response'),
)
# --calibration-envelope-directory takes a VALUE, and the driver's own guard
# (bin/integrate_likelihood_extrinsic_batchmode:509, and the same idiom at lines 774/880/920)
# reads it as `opts.calibration_envelope_directory` truthiness, not presence: an empty string
# is falsy, so the driver treats `--calibration-envelope-directory ""` as "not set" and does
# NOT refuse.  Matching that (rather than refusing on token presence alone, as this module did
# before PR #281's follow-up review) keeps this module an exact mirror of the driver instead of
# a stricter one.
_PIPELINE_EXCLUDING_VALUE_ILE_FLAGS = (
    (ILE_CALIBRATION_ENVELOPE_DIRECTORY_FLAG,
     'the driver restricts --q-time-pregrid-factor 8 to ordinary vectorized NoLoop without '
     'calibration marginalization'),
)


def validate_q_time_pregrid_factor(value):
    """Return the canonical int factor, or raise ValueError.

    Mirrors the driver's own ``if opts.q_time_pregrid_factor not in (1, 8): raise`` exactly,
    so this module and the driver can never disagree about the legal set.
    """
    try:
        factor = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            "--q-time-pregrid-factor must be an integer, got %r" % (value,))
    if factor not in Q_TIME_PREGRID_CHOICES:
        raise ValueError(
            "--q-time-pregrid-factor currently accepts only %s, got %r (same restriction as "
            "bin/integrate_likelihood_extrinsic_batchmode)."
            % ("|".join(str(c) for c in Q_TIME_PREGRID_CHOICES), factor))
    return factor


def _ile_tokens(ile_args):
    """Tokenise an ILE argument string the way optparse will see it.

    Splits ``--flag=value`` (optparse accepts it, and a naive split does not) and strips the
    quotes an ini file leaves behind.  Copied from
    RIFT.likelihood.time_marginalization_quadrature rather than imported, so this leaf module
    has no dependency on that one's numpy/scipy-facing internals.
    """
    raw = str(ile_args).split()
    toks = []
    for t in raw:
        t = t.strip().strip('"').strip("'")
        if not t:
            continue
        if t.startswith('--') and '=' in t:
            k, v = t.split('=', 1)
            toks.append(k)
            toks.append(v)
        else:
            toks.append(t)
    return toks


def _matches(flag, token):
    """True if ``token`` is ``flag`` or a possible optparse abbreviation of it."""
    if token == flag:
        return True
    return (flag.startswith(token) and token.startswith('--')
            and len(token) > 2)


def find_q_time_pregrid_in_ile_args(ile_args):
    """Every value given to ``--q-time-pregrid-factor`` in ``ile_args``, in order."""
    toks = _ile_tokens(ile_args)
    out = []
    for n, t in enumerate(toks):
        if _matches(ILE_Q_TIME_PREGRID_FLAG, t):
            out.append(toks[n + 1] if n + 1 < len(toks) else None)
    return out


def find_interpolate_time_in_ile_args(ile_args):
    """Every value given to ``--interpolate-time`` in ``ile_args``, in order."""
    toks = _ile_tokens(ile_args)
    out = []
    for n, t in enumerate(toks):
        if _matches(ILE_INTERPOLATE_TIME_FLAG, t):
            out.append(toks[n + 1] if n + 1 < len(toks) else None)
    return out


def find_calibration_envelope_directory_in_ile_args(ile_args):
    """Every value given to ``--calibration-envelope-directory`` in ``ile_args``, in order."""
    toks = _ile_tokens(ile_args)
    out = []
    for n, t in enumerate(toks):
        if _matches(ILE_CALIBRATION_ENVELOPE_DIRECTORY_FLAG, t):
            out.append(toks[n + 1] if n + 1 < len(toks) else None)
    return out


def _resolve_stencil_token(value):
    """Canonical stencil name for an --interpolate-time VALUE, or None if unrecognised.

    Mirrors the driver's own resolution (nearest|cubic|sinc verbatim, or a legacy boolean).
    An unrecognised spelling is left for the driver's own parser to reject; it is not this
    module's job to duplicate that error.
    """
    v = str(value).strip().lower()
    if v in ("nearest", "cubic", "sinc"):
        return v
    if v in _LEGACY_TRUTHY:
        return "cubic"
    if v in _LEGACY_FALSY:
        return "nearest"
    return None


def q_time_pregrid_pipeline_prereqs(factor, ile_args):
    """Missing/violated prerequisites for ``factor`` in an ILE argument string.

    ``ile_args`` is the assembled ILE command line the workflow is about to write
    (``args_ile.txt`` / ``helper_ile_args.txt``).  Returns a list of human-readable reasons;
    empty means the configuration can honour the request.  Factor 1 -- the default -- always
    returns an empty list, since it is what ILE does anyway.
    """
    factor = validate_q_time_pregrid_factor(factor)
    if factor == 1:
        return []
    toks = _ile_tokens(ile_args)
    missing = []
    for flag, why in _PIPELINE_REQUIRED_ILE_FLAGS:
        if not any(_matches(flag, t) for t in toks):
            missing.append("missing {} ({})".format(flag, why))
    for flag, why in _PIPELINE_EXCLUDING_ILE_FLAGS:
        if any(_matches(flag, t) for t in toks):
            missing.append("incompatible {} ({})".format(flag, why))
    for flag, why in _PIPELINE_EXCLUDING_VALUE_ILE_FLAGS:
        values = [toks[n + 1] if n + 1 < len(toks) else '' for n, t in enumerate(toks)
                  if _matches(flag, t)]
        # optparse takes the LAST occurrence, matching find_calibration_envelope_directory_in_
        # ile_args / find_interpolate_time_in_ile_args elsewhere in this module.
        if values and values[-1]:
            missing.append("incompatible {} (value {!r}) ({})".format(flag, values[-1], why))
    interp_values = find_interpolate_time_in_ile_args(ile_args)
    if interp_values:
        # optparse takes the LAST occurrence, so that is the one the driver will actually see.
        resolved = _resolve_stencil_token(interp_values[-1])
        if resolved is not None and resolved != "cubic":
            missing.append(STENCIL_CONFLICT_MESSAGE)
    return missing


def refuse_unhonourable_q_time_pregrid(factor, ile_args, where):
    """Raise unless ``ile_args`` can honour ``factor``.

    The raise lives HERE, not at the call sites, so it is executable in a unit test: both
    pipeline scripts are top-level scripts that need real data before they reach their guard.
    """
    missing = q_time_pregrid_pipeline_prereqs(factor, ile_args)
    if missing:
        raise ValueError(
            "--q-time-pregrid-factor {!r} was requested, but {} cannot honour it: {}.  "
            "Refusing rather than running the historical factor=1 grid while reporting that "
            "you asked for something else.".format(factor, where, "; ".join(missing)))


def refuse_unless_q_time_pregrid_emitted(factor, ile_args, where):
    """Raise unless the REQUESTED factor is the one the bytes actually carry.

    ``factor`` of ``None`` or ``1`` means "nothing forced": the flag may legitimately be
    absent (the pipeline option was never set) or may equal the historical default.  If
    something is on the line anyway -- --manual-extra-ile-args, or an ini -- it is validated
    and prerequisite-checked exactly like a pipeline-driven request, which is the same
    "hold a hand-passed value to the same standard" discipline
    RIFT.likelihood.time_marginalization_quadrature.refuse_unless_time_quadrature_emitted uses.
    """
    found = find_q_time_pregrid_in_ile_args(ile_args)
    if len(found) > 1:
        raise ValueError(
            "{} carries {} occurrences of {} ({!r}).  optparse takes the LAST, so the factor "
            "actually used would not be the one this workflow reports -- and the .sub file "
            "would read as though it were.  Refusing.".format(
                where, len(found), ILE_Q_TIME_PREGRID_FLAG, found))
    if factor is None or int(factor) == 1:
        if found:
            refuse_unhonourable_q_time_pregrid(
                validate_q_time_pregrid_factor(found[0]), ile_args, where)
        return
    factor = validate_q_time_pregrid_factor(factor)
    if not found:
        raise ValueError(
            "--q-time-pregrid-factor {!r} was requested, but {} contains no {} at all.  The "
            "request was lost between the pipeline and the ILE arguments -- a stale or "
            "version-skewed helper path can do exactly this.  Refusing rather than submitting "
            "a campaign that would silently run the historical factor=1 grid.".format(
                factor, where, ILE_Q_TIME_PREGRID_FLAG))
    found_val = validate_q_time_pregrid_factor(found[0])
    if found_val != factor:
        raise ValueError(
            "--q-time-pregrid-factor {!r} was requested but {} carries {!r}.  "
            "Refusing.".format(factor, where, found[0]))
    refuse_unhonourable_q_time_pregrid(factor, ile_args, where)
