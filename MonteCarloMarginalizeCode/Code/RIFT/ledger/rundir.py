"""What a ledger run takes from a pseudo_pipe run directory, and what it refuses.

A ledger run needs the ILE argument line, the prior box and the grid header.  The prior
is read from the last line of `args_cip_list.txt`: the line that defines the reported
posterior of a conventional run.  Only the configuration of `space.py` is implemented,
so every option on that line must be one known not to change the prior, and anything
else is refused by name.
"""
import re
import shlex

from .engine import REDLINE_FLAGS
from .space import GRID_COLUMNS, Box

# Options of the final CIP line that leave the target prior alone, with their value counts.
CIP_INERT = {
    "--lnL-protect-overflow": 0, "--no-plots": 0, "--fit-method": 1, "--n-output-samples": 1, "--n-eff": 1,
    "--n-max": 1, "--fail-unless-n-eff": 1, "--sampler-method": 1, "--posterior-unique-draw": 0,
    "--eccentricity-prior": 1, "--lnL-offset": 1, "--cap-points": 1, "--n-step": 1, "--no-adapt-parameter": 1,
    "--internal-use-lnL": 0, "--internal-correlate-parameters": 1, "--fit-save-gp": 1, "--fname-output-samples": 1,
    "--fname-output-integral": 1, "--verbose": 0, "--lnL-shift-prevent-overflow": 1, "--sigma-cut": 1,
    "--pool-size": 1, "--rf-n-estimators": 1, "--fit-uncertainty-added": 0, "--no-downselect": 0,
    "--internal-n-comp": 1, "--tripwire-fraction": 1,
}
CIP_PRIOR = {"--mc-range": 1, "--eta-range": 1, "--chi-max": 1, "--chi-small-max": 1, "--use-precessing": 0,
             "--parameter": 1, "--parameter-implied": 1, "--parameter-nofit": 1,
             "--downselect-parameter": 1, "--downselect-parameter-range": 1}
POLAR_SPINS = {"chi1", "chi2", "cos_theta1", "cos_theta2", "phi1", "phi2"}
ILE_REFUSED = ("--auto-logarithm-offset", "--zero-likelihood", "--calibration-burn-in-neff",
               "--sampler-warmstart-retry-neff", "--sampler-load-state", "--use-eccentricity",
               "--export-eos-index", "--pin-distance-to-sim", "--rom-integrate-intrinsic")


def read_args_line(path, which=-1):
    """Tokens of one line of an args file, without the leading iteration-count field."""
    lines = [x.strip() for x in open(path) if x.strip()]
    if not lines:
        raise SystemExit("ledger run: %s is empty" % path)
    tokens = shlex.split(lines[which])
    return tokens[1:] if tokens and not tokens[0].startswith("-") else tokens


def _options(tokens):
    """[(option, [values])] of a token list."""
    out = []
    for t in tokens:
        if t.startswith("--"):
            out.append((t.split("=", 1)[0], [t.split("=", 1)[1]] if "=" in t else []))
        elif out:
            out[-1][1].append(t)
        else:
            raise SystemExit("ledger run: stray argument %r" % t)
    return out


def _pair(text):
    vals = [float(x) for x in re.sub(r"[\[\]\s]", "", text).split(",")]
    if len(vals) != 2:
        raise SystemExit("ledger run: expected a [low,high] pair, got %r" % text)
    return vals


def box_from_cip_tokens(tokens):
    """The prior box of a final CIP line, or SystemExit naming what is not supported."""
    seen, params, down = {}, {"--parameter": [], "--parameter-implied": [], "--parameter-nofit": []}, []
    refused = []
    for opt, vals in _options(tokens):
        if opt in params:
            params[opt] += vals
        elif opt in ("--downselect-parameter", "--downselect-parameter-range"):
            down += vals
        elif opt in CIP_PRIOR:
            seen[opt] = vals
        elif opt not in CIP_INERT:
            refused.append(opt)
    if refused:
        raise SystemExit("ledger run: the final posterior arguments carry options this stage does not implement: %s. "
                         "It supports one prior (see RIFT/ledger/space.py) and refuses the rest." % ", ".join(sorted(set(refused))))
    for need in ("--mc-range", "--eta-range", "--use-precessing"):
        if need not in seen:
            raise SystemExit("ledger run: the final posterior arguments lack %s" % need)
    sampled = set(params["--parameter"] + params["--parameter-nofit"])
    if not {"mc", "delta_mc"} <= sampled or not POLAR_SPINS <= sampled or sampled - POLAR_SPINS - {"mc", "delta_mc"}:
        raise SystemExit("ledger run: the final posterior line must sample mc, delta_mc and the polar spin "
                         "coordinates %s (uniform magnitude, isotropic); it samples %s" % (sorted(POLAR_SPINS), sorted(sampled)))
    chi_max = float(seen.get("--chi-max", ["1"])[0])
    if "--chi-small-max" in seen and float(seen["--chi-small-max"][0]) != chi_max:
        raise SystemExit("ledger run: --chi-small-max differs from --chi-max")
    box = Box(_pair(seen["--mc-range"][0]), _pair(seen["--eta-range"][0]), chi_max)
    # a downselect is a cut on the prior support; accept it only where it cannot bind
    eta_lo = box.eta_range[0]
    m_lo = 0.5 * box.mc_range[0] * eta_lo ** -0.6 * (1 - box.delta_max)      # smallest m2 in the box
    m_hi = 0.5 * box.mc_range[1] * eta_lo ** -0.6 * (1 + box.delta_max)      # largest m1
    for name, rng in zip(down[0::2], down[1::2]):
        lo, hi = _pair(rng)
        if name not in ("m1", "m2") or lo > m_lo or hi < m_hi:
            raise SystemExit("ledger run: --downselect-parameter %s %s cuts the prior box; not implemented" % (name, rng))
    return box


def check_ile_tokens(tokens):
    """Refuse ILE arguments the engine cannot honour.  Returns ln L offset."""
    missing = [f for f in REDLINE_FLAGS if f not in tokens]
    if missing:
        raise SystemExit("ledger run: the ILE arguments lack %s (the NoLoop likelihood needs all of %s)"
                         % (" ".join(missing), " ".join(REDLINE_FLAGS)))
    opts = dict((o, v) for o, v in _options(tokens))
    bad = [o for o in opts if o in ILE_REFUSED or o.startswith("--calibration")]
    if bad:
        raise SystemExit("ledger run: ILE options not supported by the ledger engine: %s" % ", ".join(sorted(bad)))
    if opts.get("--sampler-method") != ["AV"]:
        raise SystemExit("ledger run: the ILE arguments must carry --sampler-method AV")
    return float(opts.get("--manual-logarithm-offset", ["0"])[0])


def replace_option(tokens, option, value):
    """Tokens with `option VALUE` set to value (appended when absent)."""
    out, n, found = [], 0, False
    while n < len(tokens):
        if tokens[n] == option:
            out += [option, str(value)]
            n += 2
            found = True
        elif tokens[n].startswith(option + "="):
            out.append("%s=%s" % (option, value))
            n += 1
            found = True
        else:
            out.append(tokens[n])
            n += 1
    return out if found else out + [option, str(value)]


def grid_header(path):
    """Header lines of a hyperpipeline grid, checked for the supported columns."""
    head = []
    for line in open(path):
        if not line.startswith("#"):
            break
        head.append(line.rstrip("\n"))
    if not head or head[0].strip() != "# RIFT_HYPERPIPELINE_V1":
        raise SystemExit("ledger run: %s is not a hyperpipeline grid (build with --pipeline-builder Hyperpipe)" % path)
    if head[-1].split()[1:] != ["lnL", "sigma_lnL"] + list(GRID_COLUMNS):
        raise SystemExit("ledger run: grid columns %r are not the supported set %s"
                         % (head[-1], " ".join(GRID_COLUMNS)))
    return head
