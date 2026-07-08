#!/usr/bin/env python
"""pp_diag.py -- single-injection diagnostic for the T7b pp defect.

For one SEED, native-inject (identical to pp_one) and evaluate the ringdown-only
deterministic time-marginalized grid posterior TWO ways per grid point, reusing the
SAME Precompute:
  (a) interpolate=False  -- the OLD pp path (nearest-integer-sample rhoTS lookup)
  (b) interpolate=True   -- cubic-spline rholms_intp evaluated at the exact t_det+tvals

Optionally on a FINER grid (--nmc/--neta over the ppgrid box) to also probe the
grid-coarseness / prior-support defect.  Reports q_mc, q_eta for each mode.
"""
import os, sys, json, time, argparse
import numpy as np
import lal
import RIFT.lalsimutils as lalsimutils
import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_equivalence"))
import run_equivalence_study as t4      # noqa: E402
import native_inject                    # noqa: E402
MTSUN = lal.MTSUN_SI


def mc_eta_to_m1m2(mc, eta):
    eta = min(eta, 0.25)
    Mtot = mc / (eta ** 0.6)
    delta = np.sqrt(max(0.0, 1.0 - 4.0 * eta))
    return Mtot * (1 + delta) / 2, Mtot * (1 - delta) / 2


def mc_eta(m1, m2):
    return (lalsimutils.mchirp(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN,
            lalsimutils.symRatio(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN))


def quantile_of_truth(vals_1d, ln_1d, x_true):
    order = np.argsort(vals_1d)
    v = np.asarray(vals_1d)[order]; ln = np.asarray(ln_1d)[order]
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


def marginals_and_q(mcs, etas, lnls, mc_t, eta_t):
    mcs = np.array(mcs); etas = np.array(etas); lnls = np.array(lnls)
    mc_u = np.unique(np.round(mcs, 6)); eta_u = np.unique(np.round(etas, 6))
    lnmc = []
    for mc in mc_u:
        s = lnls[np.isclose(mcs, mc)]
        lnmc.append(s.max() + np.log(np.sum(np.exp(s - s.max()))))
    lneta = []
    for et in eta_u:
        s = lnls[np.isclose(etas, et)]
        lneta.append(s.max() + np.log(np.sum(np.exp(s - s.max()))))
    return quantile_of_truth(mc_u, lnmc, mc_t), quantile_of_truth(eta_u, lneta, eta_t)


def build_ppgrid(nmc, neta, mc_lo, mc_hi, eta_lo, eta_hi):
    """Return list of (m1,m2) msun over the box."""
    out = []
    for eta in np.linspace(eta_lo, eta_hi, neta):
        for mc in np.linspace(mc_lo, mc_hi, nmc):
            out.append(mc_eta_to_m1m2(mc, min(eta, 0.25)))
    return out


def main():
    t0 = time.time()
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--outdir", default=os.path.join(HERE, "diag"))
    ap.add_argument("--nmc", type=int, default=0, help="if >0 use a fresh nmc x neta grid instead of ppgrid.xml.gz")
    ap.add_argument("--neta", type=int, default=0)
    ap.add_argument("--mc-lo", type=float, default=42.0)
    ap.add_argument("--mc-hi", type=float, default=45.0)
    ap.add_argument("--eta-lo", type=float, default=0.225)
    ap.add_argument("--eta-hi", type=float, default=0.25)
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--seglen", type=float, default=8.0)
    ap.add_argument("--start", type=float, default=999999996.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--tref0", type=float, default=1000000000.0)
    ap.add_argument("--lmax", type=int, default=2)
    ap.add_argument("--t-window", type=float, default=0.075)
    ap.add_argument("--peak-start-fac", type=float, default=10.0)
    # draw ranges (default = pp_one's original narrow ranges)
    ap.add_argument("--mc-inj-lo", type=float, default=42.6)
    ap.add_argument("--mc-inj-hi", type=float, default=44.5)
    ap.add_argument("--eta-inj-lo", type=float, default=0.225)
    ap.add_argument("--eta-inj-hi", type=float, default=0.2498)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    stop = args.start + args.seglen
    deltaT = 1.0 / args.srate

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
    peak_start = args.peak_start_fac * MTSUN * Mtot

    framedir = os.path.join(args.outdir, "frames_seed%d" % args.seed)
    cache = os.path.join(args.outdir, "frames_seed%d.cache" % args.seed)
    native_inject.native_inject(m1, m2, dist, incl, phiref, theta, phi, psi, tref, framedir, cache,
        instruments=("H1", "L1"), fmin=args.fmin, fref=args.fmin, srate=args.srate,
        seglen=args.seglen, start=args.start, noise_seed=args.seed, fmax=args.fmax,
        lmax=args.lmax, verbose=False)

    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    psd_files = {"H1": os.path.join(HERE, "H1-psd.xml.gz"), "L1": os.path.join(HERE, "L1-psd.xml.gz")}
    data_dict, psd_dict, fmax_psd = t4.load_data_and_psd(cache, channels, psd_files, args.start, stop, deltaT)
    deltaF = data_dict["H1"].deltaF
    fmax = min(args.fmax, fmax_psd)
    tvals = np.linspace(-args.t_window, args.t_window, int(2 * args.t_window / deltaT))
    fac = TDInnerProductFactory(solver="auto", template_start_offset=peak_start, template_end_offset=None)

    if args.nmc > 0:
        m1m2 = build_ppgrid(args.nmc, args.neta, args.mc_lo, args.mc_hi, args.eta_lo, args.eta_hi)
    else:
        Plist = lalsimutils.xml_to_ChooseWaveformParams_array(os.path.join(HERE, "ppgrid.xml.gz"))
        m1m2 = [(P.m1 / lalsimutils.lsu_MSUN, P.m2 / lalsimutils.lsu_MSUN) for P in Plist]

    def build_P(mm1, mm2):
        return t4.make_P(mm1, mm2, args.fmin, args.fmin, tref, deltaT, deltaF, dist_mpc=dist,
                         incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)

    mcs, etas, lnF, lnT = [], [], [], []
    for (gm1, gm2) in m1m2:
        mc_g, eta_g = mc_eta(gm1, gm2)
        pc = t4.precompute(build_P(gm1, gm2), data_dict, psd_dict, fmax, args.lmax, False, 0.0, IP_factory=fac)
        ri, ct, ctv, rho, snr, _ = pc
        lmF = float(fl.FactoredLogLikelihoodTimeMarginalized(tvals, build_P(gm1, gm2), ri, rho, ct, ctv, args.lmax, interpolate=False))
        lmT = float(fl.FactoredLogLikelihoodTimeMarginalized(tvals, build_P(gm1, gm2), ri, rho, ct, ctv, args.lmax, interpolate=True))
        mcs.append(mc_g); etas.append(eta_g); lnF.append(lmF); lnT.append(lmT)

    qF = marginals_and_q(mcs, etas, lnF, mc_t, eta_t)
    qT = marginals_and_q(mcs, etas, lnT, mc_t, eta_t)
    wall = time.time() - t0
    out = {"seed": int(args.seed), "mc_true": mc_t, "eta_true": eta_t,
           "q_mc_nearest": qF[0], "q_eta_nearest": qF[1],
           "q_mc_interp": qT[0], "q_eta_interp": qT[1],
           "n_grid": len(m1m2), "grid": ("ppgrid" if args.nmc == 0 else "%dx%d" % (args.nmc, args.neta)),
           "wall_s": wall}
    # cleanup frames
    import glob
    for g in glob.glob(os.path.join(framedir, "*.gwf")):
        os.remove(g)
    try:
        os.rmdir(framedir)
    except OSError:
        pass
    if os.path.exists(cache):
        os.remove(cache)
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
