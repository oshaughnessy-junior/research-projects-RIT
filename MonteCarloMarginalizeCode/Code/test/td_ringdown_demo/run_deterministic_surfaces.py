#!/usr/bin/env python
"""run_deterministic_surfaces.py -- clean (zero-MC-noise) three-configuration consistency
surfaces for the T7 ringdown/inspiral demo.

The full-extrinsic Monte-Carlo lnL_marg (the NoLoop ILE path) is the headline *capability*
but under-converges (neff~1) at the SNRs of this cheap smooth-PSD demo -- a pre-existing
GMM-sampler limitation flagged in the T4/T5 reports, NOT a TD-segment bug.  This script
provides the rigorous quantitative backbone the way T4 did: it drives the identical RIFT
Precompute + factored-likelihood code (RIFT.likelihood.factored_likelihood) and reports the
DETERMINISTIC time-marginalized network lnL (FactoredLogLikelihoodTimeMarginalized) at the
injected sky/inclination/phase/distance -- exact, no Monte-Carlo error.

For each of the three template-frame configurations
    (a) full      : no template cut,
    (b) ringdown  : --td-analysis-start = peak+10M  (template kept from peak+10M onward),
    (c) inspiral  : --td-analysis-end   = peak-0.05s (template kept up to peak-0.05s),
it evaluates lnL_marg(time) over the SAME intrinsic (mchirp,eta) grid and writes a table.
The segment truncation is applied through the real TDInnerProductFactory template offsets,
exactly as the ILE flags --td-analysis-start/--td-analysis-end do.

Usage:
  run_deterministic_surfaces.py --cache frames.cache --grid grid.xml.gz \
      --dist 700 --peak-start 0.0049255 --insp-end -0.05 --out zn_det.json
"""
import os, sys, json, argparse
import numpy as np
import lal
import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_equivalence"))
import run_equivalence_study as t4   # load_data_and_psd, make_P, precompute, lnL_marg_time


def mc_eta(m1, m2):
    mc = lalsimutils.mchirp(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN
    eta = lalsimutils.symRatio(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN)
    return mc, eta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=os.path.join(HERE, "frames.cache"))
    ap.add_argument("--grid", default=os.path.join(HERE, "grid.xml.gz"))
    ap.add_argument("--psd-H1", default=os.path.join(HERE, "H1-psd.xml.gz"))
    ap.add_argument("--psd-L1", default=os.path.join(HERE, "L1-psd.xml.gz"))
    ap.add_argument("--start", type=float, default=999999996.0)
    ap.add_argument("--stop", type=float, default=1000000004.0)
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--dist", type=float, default=700.0)
    ap.add_argument("--tref", type=float, default=1000000000.0)
    ap.add_argument("--lmax", type=int, default=2)
    ap.add_argument("--t-window", type=float, default=0.075)
    ap.add_argument("--peak-start", type=float, default=0.0049255,
                    help="ringdown-only: template start = peak + this many seconds (peak+10M)")
    ap.add_argument("--insp-end", type=float, default=-0.05,
                    help="inspiral-only: template end = peak + this many seconds (negative)")
    # injected extrinsics (evaluation point for the deterministic time-marginal lnL)
    ap.add_argument("--incl", type=float, default=0.7)
    ap.add_argument("--phiref", type=float, default=1.2)
    ap.add_argument("--theta", type=float, default=0.3)
    ap.add_argument("--phi", type=float, default=2.1)
    ap.add_argument("--psi", type=float, default=0.8)
    ap.add_argument("--out", default=os.path.join(HERE, "zn_det.json"))
    args = ap.parse_args()

    deltaT = 1.0 / args.srate
    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    psd_files = {"H1": args.psd_H1, "L1": args.psd_L1}
    data_dict, psd_dict, fmax_psd = t4.load_data_and_psd(
        args.cache, channels, psd_files, args.start, args.stop, deltaT)
    deltaF = data_dict["H1"].deltaF
    fmax = min(args.fmax, fmax_psd)
    N2 = data_dict["H1"].data.length
    tvals = np.linspace(-args.t_window, args.t_window, int(2 * args.t_window / deltaT))

    Plist = lalsimutils.xml_to_ChooseWaveformParams_array(str(args.grid))
    print("[setup] N2=%d deltaF=%.5f fmax=%.1f grid=%d pts tvals=%d" %
          (N2, deltaF, fmax, len(Plist), len(tvals)))

    def build_P(m1, m2):
        return t4.make_P(m1, m2, args.fmin, args.fmin, args.tref, deltaT, deltaF,
                         dist_mpc=args.dist, incl=args.incl, phiref=args.phiref,
                         theta=args.theta, phi=args.phi, psi=args.psi)

    configs = {
        "full":     dict(template_start_offset=None,           template_end_offset=None),
        "ringdown": dict(template_start_offset=args.peak_start, template_end_offset=None),
        "inspiral": dict(template_start_offset=None,           template_end_offset=args.insp_end),
    }

    results = {"setup": {"N_two_sided": int(N2), "deltaF": deltaF, "deltaT": deltaT,
                         "fmax": fmax, "dist": args.dist, "t_window": args.t_window,
                         "peak_start": args.peak_start, "insp_end": args.insp_end,
                         "cache": args.cache, "grid": args.grid},
               "configs": {}}

    def build_P_dist(m1, m2, dist):
        return t4.make_P(m1, m2, args.fmin, args.fmin, args.tref, deltaT, deltaF,
                         dist_mpc=dist, incl=args.incl, phiref=args.phiref,
                         theta=args.theta, phi=args.phi, psi=args.psi)

    D1 = args.dist            # reference distance
    D2 = args.dist / 2.0      # second distance (x=2) for the analytic distance profile
    for cname, off in configs.items():
        # ONE factory per config: the memoized ToeplitzInnerProductContext (and, for the
        # dense solver, its N x N precision matrix) is built ONCE and reused for every grid
        # point of this config -- otherwise a fresh 2 GB dense matrix is rebuilt per point.
        factory = TDInnerProductFactory(solver="dense", **off)
        rows = []
        for i, Pgrid in enumerate(Plist):
            m1 = Pgrid.m1 / lalsimutils.lsu_MSUN
            m2 = Pgrid.m2 / lalsimutils.lsu_MSUN
            mc, eta = mc_eta(m1, m2)
            # Precompute is DISTANCE-INDEPENDENT (rholms/cross-terms carry no distance); the
            # distance enters only in SingleDetectorLogLikelihood as an overall 1/D on h.
            # So precompute ONCE, then evaluate lnL(t) at two distances and analytically
            # PROFILE OVER DISTANCE (removes the fixed-distance "louder = higher-mass" bias):
            #   lnL(t;x) = x L(t) - 0.5 x^2 Q,  x = D_ref/D,  Q = <h|h> (t-indep scalar)
            #   from lnL1=L-0.5Q (x=1) and lnL2=2L-2Q (x=2):  Q = 2 lnL1 - lnL2,  L = lnL1+0.5Q
            #   distance-maximized:  lnL_dmax(t) = 0.5 L(t)^2 / Q   (best-fit template = truth)
            # then time-marginalize lnL_dmax over the window (logsumexp).
            P = build_P_dist(m1, m2, D1)
            pc = t4.precompute(P, data_dict, psd_dict, fmax, args.lmax,
                               inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=factory)
            ri, ct, ctv, rho, snr, _ = pc
            lnLt1, _ = t4.network_lnL_of_t(build_P_dist(m1, m2, D1), ri, rho, ct, ctv,
                                           args.lmax, tvals, args.tref)
            lnLt2, _ = t4.network_lnL_of_t(build_P_dist(m1, m2, D2), ri, rho, ct, ctv,
                                           args.lmax, tvals, args.tref)
            Q = float(np.median(2.0 * lnLt1 - lnLt2))          # <h|h> at D_ref, scalar
            L = lnLt1 + 0.5 * Q                                  # <d|h>(t) at D_ref
            good = Q > 1e-8
            lnL_dmax = np.where((L > 0) & good, 0.5 * L * L / max(Q, 1e-8), lnLt1)
            # time-marginalize (flat prior over the window): logsumexp
            m = np.max(lnL_dmax)
            lm = float(m + np.log(np.sum(np.exp(lnL_dmax - m)) * (tvals[1] - tvals[0])))
            # also fixed-distance time-marg (for reference/back-compat)
            lm_fixed = t4.lnL_marg_time(build_P_dist(m1, m2, D1), ri, rho, ct, ctv,
                                        args.lmax, tvals)
            rows.append({"idx": i, "m1": m1, "m2": m2, "mchirp": mc, "eta": eta,
                         "lnL_marg": lm, "lnL_marg_fixeddist": lm_fixed,
                         "hh_ref": Q, "guess_snr": float(snr)})
        results["configs"][cname] = {"offsets": off, "rows": rows}
        lms = np.array([r["lnL_marg"] for r in rows])
        ipk = int(np.argmax(lms))
        print("[%s] dist-profiled peak idx=%d (mc=%.3f eta=%.4f) lnL_marg=%.4f  range=[%.3f,%.3f]"
              % (cname, ipk, rows[ipk]["mchirp"], rows[ipk]["eta"], lms[ipk],
                 lms.min(), lms.max()))

    with open(args.out, "w") as fh:
        json.dump(results, fh, indent=2)
    print("[done] wrote", args.out)


if __name__ == "__main__":
    sys.exit(main())
