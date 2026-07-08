#!/usr/bin/env python
"""pp_one.py -- ONE self-consistent pp injection for the ringdown-only TD-segment likelihood.

Takes a SEED and an OUTDIR.  It:
  1. draws a truth (mchirp, eta) modestly around the 50+50 fiducial, plus distance / coalescence
     time / sky (RA,DEC) / inclination / phase / polarization within the analysis window,
     deterministically from SEED;
  2. NATIVE-injects that source (native_inject.py -- the SAME hlmoft modes the templates use,
     integer-sample detector delays) into a FRESH PSD-colored Gaussian-noise realization keyed
     on SEED (zero-noise signal + independent noise per seed);
  3. runs the RINGDOWN-ONLY config (template start = peak + 10 M) over the fixed intrinsic
     (mchirp x eta) grid through the real RIFT Precompute + factored-likelihood NoLoop code
     path (solver="auto" == the T7d FFT precision matvec), evaluating the DETERMINISTIC
     time-marginalized network lnL_marg at the injection's known extrinsics;
  4. forms the grid posterior P(mc,eta) ~ exp(lnL_marg) with a flat prior, marginalizes to
     P(mc) and P(eta), and records the quantile-of-truth for each;
  5. writes {"q_mc":..., "q_eta":..., "seed":..., ...} to OUTDIR and reports wall time.

WHY DETERMINISTIC / KNOWN-EXTRINSICS (scope, stated honestly).  The production likelihood is
the NoLoop path (--gpu --vectorized --force-xpy); this driver uses exactly that Precompute +
factored-likelihood code, but marginalizes TIME deterministically at the injection's known
sky/distance/inclination/phase instead of running the full-extrinsic GMM Monte Carlo.  On this
cheap smooth-PSD demo the full-extrinsic MC under-converges (neff ~ 1-4 even at n=80000; a
pre-existing GMM-sampler limitation, NOT a TD-segment bug), so its lnL_marg is MC-noise- and
bias-dominated and unsuitable for a calibration pp.  The deterministic time-marginalized lnL is
the reliable "quantitative backbone" (T4/T5/T7 reports) and is the statistic pp_run.py already
validated.  Repeated over many SEEDs (a batch of pp_one calls, one per point) the q_mc / q_eta
should be ~Uniform(0,1) for a calibrated ringdown-only segment likelihood.  Caveat: the ringdown
is few-cycle and weakly constrains eta (broad, degeneracy-ridden -- see run_gate.py), so q_eta is
expected to be noisier than q_mc, and a posterior wider than the grid clips the quantile.

Env: ~/.conda/envs/junior_rift/bin/python, PYTHONPATH=<checkout>/MonteCarloMarginalizeCode/Code.

Usage:
  pp_one.py --seed SEED --outdir OUTDIR [--grid ppgrid.xml.gz]
"""
import os
import sys
import json
import time
import argparse
import numpy as np

import lal
import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_equivalence"))
import run_equivalence_study as t4              # noqa: E402
import native_inject                            # noqa: E402
MTSUN = lal.MTSUN_SI


def mc_eta_to_m1m2(mc, eta):
    eta = min(eta, 0.25)
    Mtot = mc / (eta ** 0.6)
    delta = np.sqrt(max(0.0, 1.0 - 4.0 * eta))
    return Mtot * (1 + delta) / 2, Mtot * (1 - delta) / 2


def mc_eta(m1, m2):
    return (lalsimutils.mchirp(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN)
            / lalsimutils.lsu_MSUN,
            lalsimutils.symRatio(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN))


def quantile_of_truth(vals_1d, ln_marg_1d, x_true):
    """Quantile CDF(x_true) of a 1D flat-prior grid posterior P ~ exp(lnL) on grid ``vals_1d``."""
    order = np.argsort(vals_1d)
    v = np.asarray(vals_1d)[order]
    ln = np.asarray(ln_marg_1d)[order]
    P = np.exp(ln - ln.max())
    if v.size < 2:
        return float("nan")
    area = np.trapz(P, v)
    if area <= 0:
        return float("nan")
    P = P / area
    cdf = np.concatenate([[0.0], np.cumsum(0.5 * (P[1:] + P[:-1]) * np.diff(v))])
    cdf = cdf / cdf[-1]
    return float(np.interp(x_true, v, cdf))


def main():
    t0 = time.time()
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--grid", default=os.path.join(HERE, "ppgrid.xml.gz"))
    ap.add_argument("--psd-H1", default=os.path.join(HERE, "H1-psd.xml.gz"))
    ap.add_argument("--psd-L1", default=os.path.join(HERE, "L1-psd.xml.gz"))
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--seglen", type=float, default=8.0)
    ap.add_argument("--start", type=float, default=999999996.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--tref0", type=float, default=1000000000.0)
    ap.add_argument("--lmax", type=int, default=2)
    ap.add_argument("--t-window", type=float, default=0.075)
    ap.add_argument("--peak-start-fac", type=float, default=10.0,
                    help="ringdown template start = peak + this * M [in units of M=G Mtot/c^3]")
    ap.add_argument("--keep-frames", action="store_true",
                    help="keep the per-injection frame files (default: delete after evaluation)")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    stop = args.start + args.seglen
    deltaT = 1.0 / args.srate

    # ---- draw the truth deterministically from SEED (modest ranges around the fiducial) ----
    rng = np.random.default_rng(args.seed)
    mc_t = float(rng.uniform(42.6, 44.5))
    eta_t = float(rng.uniform(0.225, 0.2498))
    m1, m2 = mc_eta_to_m1m2(mc_t, eta_t)
    mc_t, eta_t = mc_eta(m1, m2)                    # exact (mc,eta) of the drawn masses
    dist = float(rng.uniform(600.0, 820.0))
    incl = float(rng.uniform(0.4, 1.0))
    phiref = float(rng.uniform(0.8, 1.6))
    theta = float(rng.uniform(0.1, 0.6))            # DEC
    phi = float(rng.uniform(1.7, 2.5))              # RA
    psi = float(rng.uniform(0.5, 1.1))
    tref = float(args.tref0 + rng.uniform(-0.004, 0.004))   # within the +-window
    Mtot = m1 + m2
    peak_start = args.peak_start_fac * MTSUN * Mtot         # peak + 10 M [s]

    # ---- native injection into a fresh colored-noise realization keyed on SEED ----
    framedir = os.path.join(args.outdir, "frames_seed%d" % args.seed)
    cache = os.path.join(args.outdir, "frames_seed%d.cache" % args.seed)
    native_inject.native_inject(
        m1, m2, dist, incl, phiref, theta, phi, psi, tref, framedir, cache,
        instruments=("H1", "L1"), fmin=args.fmin, fref=args.fmin, srate=args.srate,
        seglen=args.seglen, start=args.start, noise_seed=args.seed, fmax=args.fmax,
        lmax=args.lmax, verbose=False)

    # ---- ringdown-only deterministic surface over the grid (NoLoop code path, FFT solver) ----
    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    psd_files = {"H1": args.psd_H1, "L1": args.psd_L1}
    data_dict, psd_dict, fmax_psd = t4.load_data_and_psd(
        cache, channels, psd_files, args.start, stop, deltaT)
    deltaF = data_dict["H1"].deltaF
    fmax = min(args.fmax, fmax_psd)
    tvals = np.linspace(-args.t_window, args.t_window, int(2 * args.t_window / deltaT))
    Plist = lalsimutils.xml_to_ChooseWaveformParams_array(str(args.grid))
    fac = TDInnerProductFactory(solver="auto", template_start_offset=peak_start,
                                template_end_offset=None)

    def build_P(mm1, mm2):
        return t4.make_P(mm1, mm2, args.fmin, args.fmin, tref, deltaT, deltaF, dist_mpc=dist,
                         incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)

    mcs, etas, lnls = [], [], []
    for P in Plist:
        gm1 = P.m1 / lalsimutils.lsu_MSUN
        gm2 = P.m2 / lalsimutils.lsu_MSUN
        mc_g, eta_g = mc_eta(gm1, gm2)
        pc = t4.precompute(build_P(gm1, gm2), data_dict, psd_dict, fmax, args.lmax, False, 0.0,
                           IP_factory=fac)
        ri, ct, ctv, rho, snr, _ = pc
        lm = t4.lnL_marg_time(build_P(gm1, gm2), ri, rho, ct, ctv, args.lmax, tvals)
        mcs.append(mc_g)
        etas.append(eta_g)
        lnls.append(lm)
    mcs = np.array(mcs)
    etas = np.array(etas)
    lnls = np.array(lnls)

    # ---- flat-prior grid posterior -> 1D marginals -> quantile of truth ----
    P2 = np.exp(lnls - lnls.max())
    mc_u = np.unique(np.round(mcs, 6))
    eta_u = np.unique(np.round(etas, 6))
    # marginalize eta -> P(mc): sum posterior over eta at each mc node (log-domain safe)
    lnmc = []
    for mc in mc_u:
        sel = np.isclose(mcs, mc)
        s = lnls[sel]
        lnmc.append(s.max() + np.log(np.sum(np.exp(s - s.max()))))
    lneta = []
    for et in eta_u:
        sel = np.isclose(etas, et)
        s = lnls[sel]
        lneta.append(s.max() + np.log(np.sum(np.exp(s - s.max()))))
    q_mc = quantile_of_truth(mc_u, lnmc, mc_t)
    q_eta = quantile_of_truth(eta_u, lneta, eta_t)

    wall = time.time() - t0
    out = {"seed": int(args.seed), "q_mc": q_mc, "q_eta": q_eta,
           "mc_true": mc_t, "eta_true": eta_t, "dist": dist,
           "incl": incl, "phiref": phiref, "ra": phi, "dec": theta, "psi": psi,
           "tref": tref, "peak_start_s": peak_start,
           "grid_mc_range": [float(mc_u.min()), float(mc_u.max())],
           "grid_eta_range": [float(eta_u.min()), float(eta_u.max())],
           "n_grid": int(len(Plist)), "wall_s": wall,
           "method": "ringdown-only (peak+10M) deterministic time-marginalized lnL, "
                     "native self-consistent injection + fresh colored noise, solver=auto(FFT)"}
    outfile = os.path.join(args.outdir, "pp_one_seed%d.json" % args.seed)
    with open(outfile, "w") as fh:
        json.dump(out, fh, indent=2)
    if not args.keep_frames:
        import glob
        for g in glob.glob(os.path.join(framedir, "*.gwf")):
            os.remove(g)
        try:
            os.rmdir(framedir)
        except OSError:
            pass
        if os.path.exists(cache):
            os.remove(cache)
    print("[pp_one seed=%d] mc_true=%.4f eta_true=%.4f dist=%.0f -> q_mc=%.3f q_eta=%.3f  "
          "wall=%.1fs -> %s" % (args.seed, mc_t, eta_t, dist, q_mc, q_eta, wall, outfile))
    return 0


if __name__ == "__main__":
    sys.exit(main())
