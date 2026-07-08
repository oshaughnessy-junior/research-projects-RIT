#!/usr/bin/env python
"""mk_av_inject.py -- reproduce a pp_one SEED draw and write KEEPABLE noisy native frames
into OUTDIR, plus a truth.json.  Used to feed a full-extrinsic AV ILE convergence/pp run.
"""
import os, sys, json, argparse
import numpy as np
import lal
import RIFT.lalsimutils as lalsimutils
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import native_inject
MTSUN = lal.MTSUN_SI


def mc_eta_to_m1m2(mc, eta):
    eta = min(eta, 0.25)
    Mtot = mc / (eta ** 0.6)
    delta = np.sqrt(max(0.0, 1.0 - 4.0 * eta))
    return Mtot * (1 + delta) / 2, Mtot * (1 - delta) / 2


def mc_eta(m1, m2):
    return (lalsimutils.mchirp(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN,
            lalsimutils.symRatio(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--start", type=float, default=999999996.0)
    ap.add_argument("--seglen", type=float, default=8.0)
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--tref0", type=float, default=1000000000.0)
    ap.add_argument("--lmax", type=int, default=2)
    # injection prior box (default = matched-to-grid box)
    ap.add_argument("--mc-inj-lo", type=float, default=42.0)
    ap.add_argument("--mc-inj-hi", type=float, default=45.0)
    ap.add_argument("--eta-inj-lo", type=float, default=0.225)
    ap.add_argument("--eta-inj-hi", type=float, default=0.25)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    mc_t = float(rng.uniform(args.mc_inj_lo, args.mc_inj_hi))
    eta_t = float(rng.uniform(args.eta_inj_lo, args.eta_inj_hi))
    m1, m2 = mc_eta_to_m1m2(mc_t, eta_t)
    mc_t, eta_t = mc_eta(m1, m2)
    dist = float(rng.uniform(600.0, 820.0))
    incl = float(rng.uniform(0.4, 1.0)); phiref = float(rng.uniform(0.8, 1.6))
    theta = float(rng.uniform(0.1, 0.6)); phi = float(rng.uniform(1.7, 2.5))
    psi = float(rng.uniform(0.5, 1.1))
    tref = float(args.tref0 + rng.uniform(-0.004, 0.004))
    Mtot = m1 + m2
    peak_start = 10.0 * MTSUN * Mtot
    framedir = os.path.join(args.outdir, "frames")
    cache = os.path.join(args.outdir, "frames.cache")
    native_inject.native_inject(m1, m2, dist, incl, phiref, theta, phi, psi, tref, framedir, cache,
        instruments=("H1", "L1"), fmin=args.fmin, fref=args.fmin, srate=args.srate,
        seglen=args.seglen, start=args.start, noise_seed=args.seed, fmax=args.fmax,
        lmax=args.lmax, verbose=True)
    truth = {"seed": args.seed, "m1": m1, "m2": m2, "mc_true": mc_t, "eta_true": eta_t,
             "dist": dist, "incl": incl, "phiref": phiref, "ra": phi, "dec": theta,
             "psi": psi, "tref": tref, "peak_start_s": peak_start, "cache": cache}
    json.dump(truth, open(os.path.join(args.outdir, "truth.json"), "w"), indent=2)
    print("TRUTH", json.dumps(truth))


if __name__ == "__main__":
    main()
