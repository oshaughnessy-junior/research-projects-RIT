#! /usr/bin/env python
"""util_TransverseSeedClimb.py: hillclimb a fast marginalized likelihood to seed a grid.

Climbs jointly in (mu1, mu2, q, s2z) -- the chart in which chirp mass, mass ratio
and aligned spin are close to uncorrelated -- and in transverse spin
(chi1_perp, phi1[, chi2_perp, phi2]).  One or more FREE climbs find the peak;
a ladder of climbs with chi1_perp pinned at fixed values then follows the ridge
out to large transverse spin.  Points within --delta-lnL of the best, plus draws
from each climb's final Gaussian, are written as a sim_inspiral grid.

The objective is the JAX ILE kernel marginalized over time, distance, orbital
phase and polarization, averaged over a fixed adaptive (sky, inclination) batch
(RIFT.likelihood.jax_ile.intrinsic_objective).  It is a proposal objective only:
the output must be re-scored by ILE.

Settings for data, PSDs and waveform can be taken from an ILE argument file
(--ile-args args_ile.txt, as pseudo_pipe writes it) and overridden by explicit
options.

  Setup-time use :  --inj-file proposed-grid.xml.gz --inj-file-out seeds
  DAG first layer:  --inj-file overlap-grid-0.xml.gz --append-to-input
                    --inj-file-out overlap-grid-0   (rewrites the iteration-0 grid)
"""
import argparse
import ast
import json
import os
import shlex
import sys
import time

import numpy as np

import lal
import lalsimulation as lalsim

from RIFT import lalsimutils
import RIFT.misc.tools as tools
from RIFT.integrators.unreliable_oracle import LadderClimbingOracle

MSUN = lal.MSUN_SI

# options we read from an ILE argument line (name -> (type, append))
_ILE_KEYS = {
    "cache": (str, False), "channel-name": (str, True), "psd-file": (str, True),
    "fmin-ifo": (str, True), "event-time": (float, False),
    "data-start-time": (float, False), "data-end-time": (float, False),
    "srate": (int, False), "window-shape": (float, False),
    "psd-window-shape": (float, False), "fmax": (float, False),
    "fmin-template": (float, False), "reference-freq": (float, False),
    "approx": (str, False), "l-max": (int, False), "d-min": (float, False),
    "d-max": (float, False), "internal-waveform-extra-lalsuite-args": (str, False),
    "data-integration-window-half": (float, False),
}
_ILE_FLAGS = ("internal-waveform-fd-L-frame",)


def parse_ile_args(fname):
    """Pick the data/waveform settings out of an ILE argument file."""
    with open(fname) as f:
        toks = shlex.split(f.read().replace("\\\n", " "))
    out = {}
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.startswith("--"):
            key = t[2:]
            if key in _ILE_FLAGS:
                out[key.replace("-", "_")] = True
            elif key in _ILE_KEYS and i + 1 < len(toks):
                typ, app = _ILE_KEYS[key]
                val = typ(toks[i + 1])
                k = key.replace("-", "_")
                if app:
                    out.setdefault(k, []).append(val)
                else:
                    out[k] = val
                i += 1
        i += 1
    return out


def build_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ile-args", help="ILE argument file supplying data/waveform settings")
    p.add_argument("--working-directory", default=None,
                   help="chdir here first (relative paths in --ile-args resolve here)")
    for key, (typ, app) in _ILE_KEYS.items():
        p.add_argument("--" + key, type=typ, action="append" if app else "store", default=None)
    p.add_argument("--internal-waveform-fd-L-frame", action="store_true", default=None)
    p.add_argument("--climb-approx", default=None,
                   help="approximant for the climb (default: --approx). The climb only pays "
                        "off if this model's precession matches the analysis model's.")
    # grids
    p.add_argument("--inj-file", required=True, help="starting grid (sim_inspiral XML)")
    p.add_argument("--inj-file-out", required=True, help="output grid (.xml.gz appended)")
    p.add_argument("--append-to-input", action="store_true",
                   help="write the input grid followed by the seeds")
    p.add_argument("--n-max-output", type=int, default=None,
                   help="cap on the total number of output points (input + seeds)")
    p.add_argument("--n-start-eval", type=int, default=150,
                   help="input-grid points scored to choose the climb starts")
    p.add_argument("--n-free-starts", type=int, default=2)
    p.add_argument("--chi1-perp-ladder", default="0.2,0.4,0.6,0.8")
    p.add_argument("--climb-chi2-perp", action="store_true",
                   help="also climb in (chi2_perp, phi2); default holds chi2_perp=0")
    p.add_argument("--popsize", type=int, default=10)
    p.add_argument("--n-generations", type=int, default=30, help="CMA-ES generations per free climb")
    p.add_argument("--n-generations-ladder", type=int, default=12,
                   help="generations per ladder rung (rungs warm-start from a neighbour)")
    p.add_argument("--delta-lnL", type=float, default=10.0,
                   help="keep climb points within this of the best")
    p.add_argument("--n-seed-points", type=int, default=500,
                   help="target number of seed points (near-peak history + Gaussian draws)")
    p.add_argument("--mu-margin", type=float, default=0.5,
                   help="widen the (mu1, mu2) box of the input grid by this fraction")
    # objective
    p.add_argument("--n-sky", type=int, default=256)
    p.add_argument("--n-dist", type=int, default=48)
    p.add_argument("--nphi", type=int, default=16)
    p.add_argument("--npsi", type=int, default=8)
    p.add_argument("--objective", choices=["marginal", "profile"], default="marginal")
    p.add_argument("--no-sky-readapt", action="store_true",
                   help="adapt the sky batch once, not before every climb")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--output-dat", default=None,
                   help="write every climb evaluation (coordinates, lnL, label) here")
    p.add_argument("--output-json", default=None, help="run summary")
    p.add_argument("--verbose", action="store_true")
    return p


# ---------------------------------------------------------------------------
# chart: x = (mu1, mu2, q, s2z, chi1_perp, phi1[, chi2_perp, phi2])
# ---------------------------------------------------------------------------
def P_to_x(P, with_chi2):
    mc = lalsimutils.mchirp(P.m1, P.m2) / MSUN
    q = P.m2 / P.m1
    mu1, mu2, _ = tools.Mcqchi1chi2Tomu1mu2mu3(mc, q, P.s1z, P.s2z)
    x = [mu1, mu2, q, P.s2z, np.hypot(P.s1x, P.s1y), np.mod(np.arctan2(P.s1y, P.s1x), 2 * np.pi)]
    if with_chi2:
        x += [np.hypot(P.s2x, P.s2y), np.mod(np.arctan2(P.s2y, P.s2x), 2 * np.pi)]
    return np.array(x)


def x_to_P(x, P_base):
    """Return a ChooseWaveformParams, or None outside the Kerr bound."""
    mu1, mu2, q, s2z, c1p, phi1 = x[:6]
    c2p, phi2 = (x[6], x[7]) if len(x) > 6 else (0.0, 0.0)
    try:
        mc, q, s1z, s2z = tools.mu1mu2qchi2ToMcqchi1chi2(mu1, mu2, q, s2z)
    except Exception:
        return None
    if not np.isfinite(mc) or s1z ** 2 + c1p ** 2 > 1 or s2z ** 2 + c2p ** 2 > 1:
        return None
    m1, m2 = tools.McqTom1m2(mc, q)
    P = P_base.copy()
    P.m1, P.m2 = m1 * MSUN, m2 * MSUN
    P.s1x, P.s1y, P.s1z = c1p * np.cos(phi1), c1p * np.sin(phi1), s1z
    P.s2x, P.s2y, P.s2z = c2p * np.cos(phi2), c2p * np.sin(phi2), s2z
    return P


def main(argv=None):
    opts = build_parser().parse_args(argv)
    if opts.working_directory:
        os.chdir(opts.working_directory)
    if opts.ile_args:
        for k, v in parse_ile_args(opts.ile_args).items():
            if getattr(opts, k, None) is None:
                setattr(opts, k, v)
    for k, d in (("srate", 4096), ("window_shape", 0.0), ("psd_window_shape", 0.0),
                 ("l_max", 2), ("d_min", 1.0), ("data_integration_window_half", 0.075),
                 ("fmax", None), ("reference_freq", 20.0)):
        if getattr(opts, k, None) is None:
            setattr(opts, k, d)
    missing = [k for k in ("cache", "channel_name", "psd_file", "event_time", "data_start_time",
                           "data_end_time", "fmin_template", "approx", "d_max")
               if getattr(opts, k, None) is None]
    if missing:
        sys.exit("util_TransverseSeedClimb: missing settings %s (give --ile-args or options)" % missing)

    # JAX is imported only now, after argument checking
    from RIFT.likelihood.jax_ile.intrinsic_objective import (
        load_frame_data, IntrinsicMarginalObjective)
    import jax
    print(" [seedclimb] JAX backend:", jax.default_backend(), jax.devices())

    t_start = time.time()
    fmin_ifo = dict((s.split("=")[0], float(s.split("=")[1])) for s in (opts.fmin_ifo or []))
    data_dict, psd_dict = load_frame_data(
        opts.cache, opts.channel_name, opts.psd_file, opts.data_start_time,
        opts.data_end_time, opts.srate, opts.window_shape, opts.psd_window_shape, fmin_ifo)
    det0 = list(data_dict)[0]

    approx = opts.climb_approx or opts.approx
    P_base = lalsimutils.ChooseWaveformParams()
    P_base.approx = lalsim.GetApproximantFromString(approx)
    P_base.fmin = opts.fmin_template
    P_base.fref = opts.reference_freq
    P_base.deltaT = 1.0 / opts.srate
    P_base.deltaF = data_dict[det0].deltaF
    wf_extra = {"fd_alignment_postevent_time": 2}
    if opts.internal_waveform_fd_L_frame:
        wf_extra["fd_L_frame"] = True
    if opts.internal_waveform_extra_lalsuite_args:
        s = opts.internal_waveform_extra_lalsuite_args
        wf_extra["extra_waveform_args"] = (eval(s, {"dict": dict}) if s.startswith("dict(")
                                           else ast.literal_eval(s))
    obj = IntrinsicMarginalObjective(
        data_dict, psd_dict, P_base, opts.event_time, opts.d_min, opts.d_max,
        l_max=opts.l_max, fmax=opts.fmax, n_sky=opts.n_sky, nphi=opts.nphi,
        npsi=opts.npsi, n_dist=opts.n_dist, reduce=opts.objective,
        integration_window_half=opts.data_integration_window_half,
        waveform_kwargs=dict(ignore_threshold=None, extra_waveform_kwargs=wf_extra),
        seed=opts.seed, verbose=opts.verbose)

    # --- starting grid: score a subset, pick distinct starts --------------------
    rng = np.random.default_rng(opts.seed)
    P_in = lalsimutils.xml_to_ChooseWaveformParams_array(opts.inj_file)
    with_chi2 = opts.climb_chi2_perp
    X_in = np.array([P_to_x(P, with_chi2) for P in P_in])
    idx = rng.choice(len(P_in), size=min(opts.n_start_eval, len(P_in)), replace=False)
    obj.adapt_sky(P_in[idx[0]])
    lnL_start = np.array([obj(x_to_P(X_in[i], P_base) or P_in[i]) for i in idx])
    order = idx[np.argsort(-lnL_start)]
    print(" [seedclimb] start grid: scored %d of %d points, best lnL %.2f (%.1f s)"
          % (len(idx), len(P_in), np.max(lnL_start), time.time() - t_start))

    names = ["mu1", "mu2", "q", "s2z", "chi1_perp", "phi1"] + (["chi2_perp", "phi2"] if with_chi2 else [])
    lo = X_in.min(axis=0); hi = X_in.max(axis=0)
    span = np.maximum(hi - lo, 1e-6)
    box = {"mu1": (lo[0] - opts.mu_margin * span[0], hi[0] + opts.mu_margin * span[0]),
           "mu2": (lo[1] - opts.mu_margin * span[1], hi[1] + opts.mu_margin * span[1]),
           "q": (max(1e-3, lo[2] - 0.2 * span[2]), 1.0), "s2z": (-1.0, 1.0),
           "chi1_perp": (0.0, 1.0), "phi1": (0.0, 2 * np.pi),
           "chi2_perp": (0.0, 1.0), "phi2": (0.0, 2 * np.pi)}

    def lnL_cols(*cols):
        X = np.column_stack(cols)
        out = np.empty(len(X))
        for j, x in enumerate(X):
            P = x_to_P(x, P_base)
            out[j] = -np.inf if P is None else obj(P)
        return out

    oracle = LadderClimbingOracle()
    for n in names:
        oracle.add_parameter(n, left_limit=box[n][0], right_limit=box[n][1])
    ladder = [float(v) for v in opts.chi1_perp_ladder.split(",")] if opts.chi1_perp_ladder else []
    oracle.setup(lnL=lnL_cols, periodic=[n for n in names if n.startswith("phi")],
                 ladder_param="chi1_perp", ladder_values=ladder, popsize=opts.popsize,
                 n_generations=opts.n_generations,
                 n_generations_ladder=opts.n_generations_ladder, seed=opts.seed, verbose=opts.verbose)

    # scale: weighted spread of the scored start points in the mass/aligned chart
    lw = lnL_start - lnL_start.max()
    w = np.exp(lw); w /= w.sum()
    Xs = X_in[idx]
    sd = np.sqrt(np.sum(w[:, None] * (Xs - w @ Xs) ** 2, axis=0))
    width = np.array([box[n][1] - box[n][0] for n in names])
    scale = np.where(sd > 1e-3 * width, sd, 0.05 * width)
    for k, n in enumerate(names):
        if n.startswith("phi") or n.endswith("perp"):
            scale[k] = 0.25 * width[k]

    # distinct free starts: best scored points, separated by > 1 scale in mu1
    starts = []
    for i in order:
        x = X_in[i].copy()
        x[4], x[5] = 0.3, rng.uniform(0, 2 * np.pi)   # start off the aligned-spin edge
        if all(np.abs(x[0] - s[0]) > scale[0] for s in starts) or not starts:
            starts.append(x)
        if len(starts) >= opts.n_free_starts:
            break

    def project_kerr(x):
        """Pull s1z inside |s1| < 1 at fixed (mc, q), recomputing (mu1, mu2)."""
        if x_to_P(x, P_base) is not None:
            return x
        mc, q, s1z, s2z = tools.mu1mu2qchi2ToMcqchi1chi2(x[0], x[1], x[2], x[3])
        s1z_max = 0.98 * np.sqrt(max(0.0, 1 - x[4] ** 2))
        mu1, mu2, _ = tools.Mcqchi1chi2Tomu1mu2mu3(mc, q, np.clip(s1z, -s1z_max, s1z_max), s2z)
        y = x.copy(); y[0], y[1] = mu1, mu2
        return y

    def readapt(x):
        x = project_kerr(np.asarray(x, dtype=float))
        P = x_to_P(x, P_base)
        if P is not None and not opts.no_sky_readapt:
            obj.adapt_sky(P)
        return x

    oracle.run(np.array(starts), scale, between_climbs=readapt)
    t_climb = time.time() - t_start

    # --- collect seeds ----------------------------------------------------------
    Xh, lnLh, labels = oracle.near_peak(opts.delta_lnL)
    best = float(np.max(oracle.reference_lnL))
    # equal quota per climb: a plain top-N by lnL is dominated by the low-chi1_perp
    # climbs and starves the high rungs, which are the point of the ladder
    labels = np.array(labels)
    groups = [np.flatnonzero(labels == lab) for lab in dict.fromkeys(labels)]
    quota = int(np.ceil(opts.n_seed_points / max(1, len(groups))))
    sel = np.concatenate([g[np.argsort(-lnLh[g])][:quota] for g in groups]) if groups else np.array([], int)
    sel = sel[np.argsort(-lnLh[sel])][:opts.n_seed_points]
    n_hist = len(sel)
    X_seed = Xh[sel]
    n_draw = max(0, opts.n_seed_points - n_hist)
    if n_draw:
        _, _, X_draw = oracle.draw_simplified(n_draw, delta_lnL=opts.delta_lnL)
        X_seed = np.concatenate([X_seed, X_draw])
    P_seed = [P for P in (x_to_P(x, P_base) for x in X_seed) if P is not None]
    for P in P_seed:
        P.approx = lalsim.GetApproximantFromString(opts.approx)
    print(" [seedclimb] %d evaluations, best lnL %.2f; %d history points within %.1f; "
          "%d Gaussian draws; %d seeds written (%.1f s)"
          % (len(oracle.history), best, len(Xh), opts.delta_lnL, n_draw, len(P_seed), t_climb))
    for lab, rec in oracle.climbs.items():
        bx = rec["best_x"]
        Pb = x_to_P(bx, P_base)
        print("   %-16s best lnL %8.2f  mc %.4f q %.3f s1z %+.3f chi1_perp %.3f phi1 %.2f"
              % (lab, rec["best_lnL"], lalsimutils.mchirp(Pb.m1, Pb.m2) / MSUN if Pb else np.nan,
                 bx[2], Pb.s1z if Pb else np.nan, bx[4], bx[5]))

    P_out = list(P_in) + P_seed if opts.append_to_input else P_seed
    if opts.n_max_output and len(P_out) > opts.n_max_output:
        if opts.append_to_input:
            # keep every seed; thin the input grid
            n_keep = max(0, opts.n_max_output - len(P_seed))
            keep = rng.choice(len(P_in), size=n_keep, replace=False)
            P_out = [P_in[i] for i in sorted(keep)] + P_seed[:opts.n_max_output]
        P_out = P_out[:opts.n_max_output]
    fname_out = opts.inj_file_out.replace(".xml.gz", "")
    lalsimutils.ChooseWaveformParams_array_to_xml(P_out, fname=fname_out, fref=opts.reference_freq)

    if opts.output_dat:
        lab_all = [h[0] for h in oracle.history]
        Xa = np.array([h[1] for h in oracle.history]); la = np.array([h[2] for h in oracle.history])
        with open(opts.output_dat, "w") as f:
            f.write("# " + " ".join(names) + " lnL_fast label\n")
            for x, v, lab in zip(Xa, la, lab_all):
                f.write(" ".join("%.10g" % t for t in x) + " %.6f %s\n" % (v, lab))
    summary = dict(n_eval=len(oracle.history), best_lnL=best, n_seed=len(P_seed),
                   n_output=len(P_out), wall_s=time.time() - t_start, objective=obj.stats(),
                   climbs={k: dict(best_lnL=v["best_lnL"], best_x=v["best_x"].tolist())
                           for k, v in oracle.climbs.items()},
                   names=names, approx_climb=approx)
    print(" [seedclimb] summary", json.dumps({k: summary[k] for k in ("n_eval", "best_lnL", "n_seed", "n_output", "wall_s")}))
    if opts.output_json:
        with open(opts.output_json, "w") as f:
            json.dump(summary, f, indent=1)


if __name__ == "__main__":
    main()
