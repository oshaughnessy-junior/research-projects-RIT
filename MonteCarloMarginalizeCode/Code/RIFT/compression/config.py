"""Resolve the generating run's prior before fitting any posterior rows."""
import ast
import configparser
import hashlib
from pathlib import Path
import shlex
import numpy as np

TARGETS = ("precessing_source_redshift", "aligned_distance")


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_ini(path):
    cfg = configparser.ConfigParser(interpolation=None)
    if not cfg.read(path):
        raise ValueError("Cannot read INI: " + str(path))
    return cfg


def enabled(path):
    cfg = read_ini(path)
    return cfg.getboolean("rift-auto-fit", "enabled", fallback=False)


def options(text):
    tokens = shlex.split(text.replace("\\\n", " "))
    result = {}
    for i, token in enumerate(tokens):
        if token.startswith("--"):
            key, sep, value = token[2:].partition("=")
            if not sep:
                value = tokens[i+1] if i+1 < len(tokens) and not tokens[i+1].startswith("--") else True
            result.setdefault(key, []).append(value)
    return result


def last(opts, key, default=None):
    return opts.get(key, [default])[-1]


def interval(value):
    a = np.asarray(ast.literal_eval(str(value)), float)
    if a.shape != (2,) or not np.isfinite(a).all() or a[0] >= a[1]:
        raise ValueError("Expected finite increasing bounds: " + str(value))
    return a.tolist()


def resolve(config, rundir):
    """Prefer executed args to INI suggestions; reject inconsistent explicit priors."""
    cfg = read_ini(config)
    run = Path(rundir).resolve()
    auto = dict(cfg["rift-auto-fit"]) if cfg.has_section("rift-auto-fit") else {}
    engine = dict(cfg["engine"]) if cfg.has_section("engine") else {}
    def unquote(v):
        try:
            parsed = ast.literal_eval(v)
            return parsed if isinstance(parsed, str) else v
        except (ValueError, SyntaxError):
            return v
    pseudo = {k.replace("_", "-"): unquote(v) for k, v in cfg.items("rift-pseudo-pipe")} if cfg.has_section("rift-pseudo-pipe") else {}
    ilepath, cippath = run/"args_ile.txt", run/"args_cip_list.txt"
    if not ilepath.is_file() or not cippath.is_file():
        raise ValueError("Existing run requires executed args_ile.txt and args_cip_list.txt; an INI alone cannot establish the native prior")
    ile = options(ilepath.read_text())
    ciptext = cippath.read_text().strip().splitlines()[-1]
    cip = options(ciptext)
    forbidden = [k for k in cip if any(s in k for s in ("supplementary", "import-prior", "prior-gaussian", "prior-tapered", "pseudo-gaussian"))]
    if forbidden:
        raise ValueError("Unsupported retained-prior modification: " + ", ".join(forbidden))
    if any(k in ile for k in ("pin-distance-to-sim", "pin-to-sim", "malmquistprior")):
        raise ValueError("Pinned/selection-modified distance is not supported")
    if float(last(cip, "source-redshift", 0)) != 0:
        raise ValueError("Fixed source-redshift CIP analysis requires a separate support adapter")
    dprior = last(ile, "d-prior", "Euclidean")
    if "ile-distance-prior" in pseudo and pseudo["ile-distance-prior"] != dprior:
        raise ValueError("INI distance prior conflicts with executed ILE arguments")
    if auto.get("distance_prior", dprior) != dprior:
        raise ValueError("auto-fit distance_prior conflicts with the generating analysis")
    if dprior not in ("Euclidean", "pseudo_cosmo", "cosmo", "cosmo_sourceframe", "uniform"):
        raise ValueError("Unsupported native distance prior: " + str(dprior))
    bounds = {
        "distance": [float(last(ile, "d-min", 1)), float(last(ile, "d-max", 10000))],
        "mc": interval(last(cip, "mc-range", "[.9,250]")),
        "eta": interval(last(cip, "eta-range", "[.01,.2499999]")),
    }
    if bounds["eta"][0] <= 0 or bounds["eta"][1] > .25 or bounds["mc"][0] <= 0 or bounds["distance"][0] <= 0:
        raise ValueError("Unphysical retained bounds")
    for value in bounds.values():
        interval(str(value))
    spin_cap = float(last(cip, "chi-max", 1))
    spin_caps = [spin_cap, float(last(cip, "chi-small-max", spin_cap))]
    if not all(0 < v <= 1 for v in spin_caps):
        raise ValueError("Spin caps must be in (0,1]")
    params = cip.get("parameter", []) + cip.get("parameter-nofit", [])
    paramset = set(params)
    cartesian = {"s1x", "s1y", "s1z", "s2x", "s2y", "s2z"} <= paramset
    polar = {"chi1", "chi2", "phi1", "phi2"} <= paramset and all("theta"+str(i) in paramset or "cos_theta"+str(i) in paramset for i in (1,2))
    cylindrical = {"s1z_bar", "s2z_bar", "chi1_perp_u", "chi2_perp_u", "phi1", "phi2"} <= paramset
    aligned = {"s1z", "s2z"} <= paramset and not (cartesian or polar or cylindrical)
    if not any((cartesian, polar, cylindrical, aligned)):
        raise ValueError("Unsupported or incomplete native spin-coordinate set")
    precessing = not aligned
    aligned_prior = last(cip, "aligned-prior", "uniform")
    transverse_prior = last(cip, "transverse-prior", "uniform")
    if aligned_prior not in ("uniform", "alignedspin-zprior") or transverse_prior != "uniform":
        raise ValueError("Unsupported CIP spin prior")
    if precessing and aligned_prior != "uniform":
        raise ValueError("Precessing Cartesian alignedspin-zprior is not an isotropic native spin prior")
    correction = last(cip, "prior-in-integrand-correction")
    spin_prior = "uniform_mag_prec" if polar else "volumetric" if cartesian else "zprior_aligned" if aligned_prior == "alignedspin-zprior" else "uniform_aligned"
    if cylindrical:
        if correction != "uniform_over_rbar_singular":
            raise ValueError("Cylindrical spin chart requires its verified uniform-magnitude correction")
        spin_prior = "uniform_mag_prec"
    elif correction == "uniform_over_volumetric" and cartesian:
        spin_prior = "uniform_mag_prec"
    elif correction == "volumetric_over_uniform" and polar:
        spin_prior = "volumetric"
    elif correction is not None:
        raise ValueError("Unsupported spin prior-in-integrand correction/coordinate pair")
    if "pseudo-uniform-magnitude-prior" in cip or "pseudo-uniform-magnitude-prior-alternate-sampling" in cip:
        if not cartesian or correction is not None:
            raise ValueError("Unsupported composition of post-sampling spin corrections")
        spin_prior = "uniform_mag_prec"
    # Standard CIP mass coordinates produce a density uniform in detector component masses.
    if params and not ({"mc", "eta"} <= set(params) or {"mc", "delta_mc"} <= set(params) or {"mtot", "q"} <= set(params) or {"m1", "m2"} <= set(params)):
        raise ValueError("Unsupported CIP mass-coordinate prior; use standard mass coordinates")
    cuts = {}
    if {"mtot", "q"} <= paramset:
        cuts["mtot"] = interval(last(cip, "mtot-range", "[1,300]"))
    if {"m1", "m2"} <= paramset:
        cuts.update(m1=[.9, 1000.], m2=[.9, 1000.])
    # Direct s1z/s2z sampling uses CIP's 0.999 spin-cap box.
    if cartesian or aligned:
        cuts.update(s1z=[-.999*spin_caps[0], .999*spin_caps[0]],
                    s2z=[-.999*spin_caps[1], .999*spin_caps[1]])
    for name, value in zip(cip.get("downselect-parameter", []), cip.get("downselect-parameter-range", [])):
        if name not in ("m1", "m2", "mtot", "mc", "eta", "q", "s1z", "s2z", "chi1", "chi2"):
            raise ValueError("Unsupported coupled support cut: " + name)
        cuts[name] = interval(value)
    analysis_cosmology = unquote(engine.get("cosmology", engine.get("with-cosmology", pseudo.get("cosmology", "Planck15"))))
    cosmology = auto.get("cosmology", analysis_cosmology)
    if cosmology != analysis_cosmology:
        raise ValueError("auto-fit cosmology conflicts with the analysis; supply a different evaluation cosmology through the API")
    # The current ILE cosmo branches explicitly call get_astropy_cosmology('Planck15').
    if dprior in ("cosmo", "cosmo_sourceframe") and cosmology != "Planck15":
        raise ValueError("Executed RIFT cosmological distance prior uses Planck15; INI declaration conflicts")
    targets = [t.strip() for t in auto.get("targets", ",".join(TARGETS)).split(",")]
    if not targets or any(t not in TARGETS for t in targets) or len(set(targets)) != len(targets):
        raise ValueError("Unknown or duplicate auto-fit target")
    components = [int(v) for v in auto.get("component_counts", "1,2,4,8").split(",")]
    if not components or any(v < 1 or v > 128 for v in components):
        raise ValueError("component_counts must be in [1,128]")
    contract = dict(bounds=bounds, spin_caps=spin_caps, spin_prior=spin_prior,
                    distance_prior=dprior, cosmology=cosmology, cuts=cuts,
                    mass_prior="uniform_detector_component_masses", precessing=precessing,
                    fref=float(last(ile, "reference-freq", 100)),
                    nuisance_prior="Generating run conditional extrinsic and omitted-intrinsic priors",
                    input_hashes={str(p.name): file_hash(p) for p in (Path(config), ilepath, cippath) if p.exists()})
    return cfg, contract, dict(targets=targets, components=components,
        seed=int(auto.get("seed", 711)), maxiter=int(auto.get("max_iterations", 200)),
        tolerance=float(auto.get("validation_ks", .08)),
        output=auto.get("output_directory", "auto_fit"),
        posterior=auto.get("posterior", "extrinsic_posterior_samples.dat"),
        weight_column=auto.get("weight_column"), group_column=auto.get("group_column"))
