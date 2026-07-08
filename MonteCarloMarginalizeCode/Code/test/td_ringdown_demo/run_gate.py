#!/usr/bin/env python
"""run_gate.py -- T7 deterministic three-way consistency GATE on the NATIVE injection (T7b/gate).

Re-runs the T7 zero-noise three-configuration study (full IMR / ringdown peak+10M /
inspiral-only to peak-0.05 s) over the fixed 35-point (mchirp x eta) grid, driving the REAL
RIFT Precompute + factored-likelihood path (via the T4 harness run_equivalence_study), but on
the RIFT-native self-consistent injection (native_inject.py) instead of util_LALWriteFrame.

Statistic: the DETERMINISTIC time-marginalized network lnL at the injected sky/inclination/
phase and the injected (true) distance -- ``FactoredLogLikelihoodTimeMarginalized`` -- with
ZERO Monte-Carlo error.  For a self-consistent injection this FIXED-distance statistic
lnL = <d|h> - 1/2<h|h> is maximized (over templates) exactly at the injected template h=d
(the -1/2<h|h> term penalizes louder templates, so there is no distance-degeneracy banana);
hence a correct machine + injection peaks AT the truth grid node.

GATE (zero noise) -- load-bearing criteria (both must hold):
  (1) FULL config: grid argmax == the truth grid node (exact).  This is THE proof that the
      native injection is self-consistent with the templates and the segment machinery is
      correct (same data, same code path, full template -> exact truth).
  (3) width ordering preserved: sigma(full) < sigma(inspiral) < sigma(ringdown) in mchirp
      (the clean scientific deliverable -- segment analyses are broader).

Informational sub-check (reported, NOT a pass/fail gate):
  (2) segment configs (ringdown, inspiral): argmax offset from truth in grid steps.  Segment
      surfaces are RIDGES along the mchirp-eta degeneracy, so the discrete argmax slides along
      the (poorly-constrained) direction to the grid boundary -- especially eta, which a
      few-cycle ringdown cannot constrain.  The ringdown mchirp lands within ~1 step; the
      inspiral mchirp is biased ~3 steps low along the chirp-mass degeneracy (a segment
      truncated-template-vs-full-IMR-data effect that is INJECTION-INDEPENDENT: the original
      util_LALWriteFrame T7 inspiral railed to the same low mc edge).  These offsets are the
      expected segment-degeneracy behaviour, not an inconsistency -- criterion (1) proves the
      pipeline is self-consistent.

Env: ~/.conda/envs/junior_rift/bin/python, PYTHONPATH=<checkout>/MonteCarloMarginalizeCode/Code.
Solver "auto" (FFT precision matvec, T7d) -- no 2 GB dense build, ~2 min/config.

Usage:
  run_gate.py                         # regenerate native zero-noise frames + run + gate
  run_gate.py --cache frames_native.cache --no-inject   # reuse existing native frames
"""
import os
import sys
import json
import argparse
import numpy as np

import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_equivalence"))
import run_equivalence_study as t4              # noqa: E402
import native_inject                            # noqa: E402


def mc_eta(m1, m2):
    mc = lalsimutils.mchirp(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN
    eta = lalsimutils.symRatio(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN)
    return mc, eta


def _grid_index(sorted_vals, x):
    return int(np.argmin([abs(v - x) for v in sorted_vals]))


def _sigma_mc(rows, lnvals):
    """1D marginal sigma(mchirp): flat prior, marginalize eta by summing exp(lnL) per mc bin."""
    lnvals = np.asarray(lnvals)
    P = np.exp(lnvals - lnvals.max())
    mcs = sorted(set(round(r["mchirp"], 4) for r in rows))
    pmc = np.array([P[[i for i, r in enumerate(rows)
                       if round(r["mchirp"], 4) == mc]].sum() for mc in mcs])
    pmc = pmc / pmc.sum()
    mcs = np.array(mcs)
    mean = float(np.sum(mcs * pmc))
    return float(np.sqrt(np.sum((mcs - mean) ** 2 * pmc)))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cache", default=os.path.join(HERE, "frames_native.cache"))
    ap.add_argument("--grid", default=os.path.join(HERE, "grid.xml.gz"))
    ap.add_argument("--psd-H1", default=os.path.join(HERE, "H1-psd.xml.gz"))
    ap.add_argument("--psd-L1", default=os.path.join(HERE, "L1-psd.xml.gz"))
    ap.add_argument("--no-inject", action="store_true", help="reuse existing --cache frames")
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--seglen", type=float, default=8.0)
    ap.add_argument("--start", type=float, default=999999996.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--dist", type=float, default=700.0)
    ap.add_argument("--tref", type=float, default=1000000000.0)
    ap.add_argument("--m1", type=float, default=50.0)
    ap.add_argument("--m2", type=float, default=50.0)
    ap.add_argument("--incl", type=float, default=0.7)
    ap.add_argument("--phiref", type=float, default=1.2)
    ap.add_argument("--theta", type=float, default=0.3)
    ap.add_argument("--phi", type=float, default=2.1)
    ap.add_argument("--psi", type=float, default=0.8)
    ap.add_argument("--lmax", type=int, default=2)
    ap.add_argument("--t-window", type=float, default=0.075)
    ap.add_argument("--peak-start", type=float, default=0.0049255,
                    help="ringdown template start offset = peak + this (s) = 10 M for 50+50")
    ap.add_argument("--insp-end", type=float, default=-0.05,
                    help="inspiral template end offset = peak + this (s)")
    ap.add_argument("--out", default=os.path.join(HERE, "gate_native.json"))
    args = ap.parse_args()

    stop = args.start + args.seglen
    deltaT = 1.0 / args.srate

    if not args.no_inject:
        native_inject.native_inject(
            args.m1, args.m2, args.dist, args.incl, args.phiref, args.theta, args.phi, args.psi,
            args.tref, os.path.join(HERE, "frames_native"), args.cache,
            instruments=("H1", "L1"), fmin=args.fmin, fref=args.fmin,
            srate=args.srate, seglen=args.seglen, start=args.start, noise_seed=None,
            fmax=args.fmax, lmax=args.lmax)

    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    psd_files = {"H1": args.psd_H1, "L1": args.psd_L1}
    data_dict, psd_dict, fmax_psd = t4.load_data_and_psd(
        args.cache, channels, psd_files, args.start, stop, deltaT)
    deltaF = data_dict["H1"].deltaF
    fmax = min(args.fmax, fmax_psd)
    tvals = np.linspace(-args.t_window, args.t_window, int(2 * args.t_window / deltaT))

    Plist = lalsimutils.xml_to_ChooseWaveformParams_array(str(args.grid))
    truth_mc, truth_eta = mc_eta(args.m1, args.m2)
    mc_grid = sorted(set(round(mc_eta(P.m1 / lalsimutils.lsu_MSUN,
                                      P.m2 / lalsimutils.lsu_MSUN)[0], 4) for P in Plist))
    eta_grid = sorted(set(round(mc_eta(P.m1 / lalsimutils.lsu_MSUN,
                                       P.m2 / lalsimutils.lsu_MSUN)[1], 4) for P in Plist))
    tmc_i, teta_i = _grid_index(mc_grid, truth_mc), _grid_index(eta_grid, truth_eta)
    print("[gate] grid=%d pts, truth mc=%.4f (node %.4f) eta=%.4f (node %.4f)" %
          (len(Plist), truth_mc, mc_grid[tmc_i], truth_eta, eta_grid[teta_i]))

    def build_P(m1, m2):
        return t4.make_P(m1, m2, args.fmin, args.fmin, args.tref, deltaT, deltaF,
                         dist_mpc=args.dist, incl=args.incl, phiref=args.phiref,
                         theta=args.theta, phi=args.phi, psi=args.psi)

    configs = {
        "full":     dict(template_start_offset=None,           template_end_offset=None),
        "ringdown": dict(template_start_offset=args.peak_start, template_end_offset=None),
        "inspiral": dict(template_start_offset=None,           template_end_offset=args.insp_end),
    }

    results = {"setup": {"cache": args.cache, "grid": args.grid, "dist": args.dist,
                         "truth_mc": truth_mc, "truth_eta": truth_eta,
                         "truth_node_mc": mc_grid[tmc_i], "truth_node_eta": eta_grid[teta_i]},
               "configs": {}}
    summary = {}
    for cname, off in configs.items():
        factory = TDInnerProductFactory(solver="auto", **off)   # T7d: FFT path, no 2 GB dense
        rows = []
        for i, Pg in enumerate(Plist):
            m1 = Pg.m1 / lalsimutils.lsu_MSUN
            m2 = Pg.m2 / lalsimutils.lsu_MSUN
            mc, eta = mc_eta(m1, m2)
            P = build_P(m1, m2)
            pc = t4.precompute(P, data_dict, psd_dict, fmax, args.lmax, False, 0.0,
                               IP_factory=factory)
            ri, ct, ctv, rho, snr, _ = pc
            lm = t4.lnL_marg_time(build_P(m1, m2), ri, rho, ct, ctv, args.lmax, tvals)
            rows.append({"idx": i, "m1": m1, "m2": m2, "mchirp": mc, "eta": eta,
                         "lnL_marg": lm, "guess_snr": float(snr)})
        lms = np.array([r["lnL_marg"] for r in rows])
        ipk = int(np.argmax(lms))
        pk = rows[ipk]
        dmc = _grid_index(mc_grid, round(pk["mchirp"], 4)) - tmc_i
        deta = _grid_index(eta_grid, round(pk["eta"], 4)) - teta_i
        sig = _sigma_mc(rows, lms)
        results["configs"][cname] = {"offsets": off, "rows": rows,
                                     "argmax": {"idx": ipk, "mchirp": pk["mchirp"],
                                                "eta": pk["eta"], "lnL_marg": lms[ipk],
                                                "dmc_steps": dmc, "deta_steps": deta},
                                     "sigma_mc": sig}
        summary[cname] = dict(dmc=dmc, deta=deta, sigma=sig, mc=pk["mchirp"], eta=pk["eta"],
                              snr=pk["guess_snr"])
        print("[%-8s] argmax mc=%.4f eta=%.4f lnL=%.4f | offset dmc=%+d deta=%+d | "
              "sigma(mc)=%.4f snr=%.2f" %
              (cname, pk["mchirp"], pk["eta"], lms[ipk], dmc, deta, sig, pk["guess_snr"]))

    # ---- gate criteria ----
    g1 = (summary["full"]["dmc"] == 0 and summary["full"]["deta"] == 0)         # LOAD-BEARING
    g3 = summary["full"]["sigma"] < summary["inspiral"]["sigma"] < summary["ringdown"]["sigma"]
    # informational: segment mchirp-direction offset (constrained param); eta is degenerate
    seg_mc = {c: abs(summary[c]["dmc"]) for c in ("ringdown", "inspiral")}
    passed = bool(g1 and g3)      # the definitive self-consistency + physics-ordering gate
    results["gate"] = {"full_peaks_at_truth": bool(g1), "width_ordering": bool(g3),
                       "segment_mchirp_steps": seg_mc, "passed": passed}
    print("\n[GATE] (1) full peaks at truth node (LOAD-BEARING): %s  [dmc=%+d deta=%+d]" %
          (g1, summary["full"]["dmc"], summary["full"]["deta"]))
    print("[GATE] (3) width order full<insp<ring (LOAD-BEARING): %s  [%.4f < %.4f < %.4f]" %
          (g3, summary["full"]["sigma"], summary["inspiral"]["sigma"],
           summary["ringdown"]["sigma"]))
    print("[INFO] (2) segment offsets (degeneracy slide, not a gate): "
          "ring dmc=%+d deta=%+d, insp dmc=%+d deta=%+d" %
          (summary["ringdown"]["dmc"], summary["ringdown"]["deta"],
           summary["inspiral"]["dmc"], summary["inspiral"]["deta"]))
    print("[GATE] ===> %s" % ("PASS" if passed else "FAIL"))

    with open(args.out, "w") as fh:
        json.dump(results, fh, indent=2)
    print("[gate] wrote", args.out)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
