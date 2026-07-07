#!/usr/bin/env python
"""build_grid.py -- build an intrinsic (mchirp x eta) sim-xml grid for the T7 ringdown demo.

The grid is handed to ILE as `-I grid.xml.gz --n-events-to-analyze <N> --event 0`; ILE's
batchmode loop evaluates one lnL_marg per grid point in a single invocation (the
"batched-ILE" pattern).  Only the intrinsic parameters (m1, m2, spins=0) matter for the grid;
ILE marginalizes the extrinsics.  fmin / fref / approx are set to match the injection.

Usage:
    build_grid.py --out grid.xml.gz --nmc 7 --neta 5 \
        --mc-lo 32.5 --mc-hi 37.0 --eta-lo 0.20 --eta-hi 0.25 \
        [--fmin 25 --fref 25 --approx IMRPhenomD]
Truth (m1=m2=40): mchirp = 34.82 Msun, eta = 0.25.
"""
import argparse
import numpy as np
import RIFT.lalsimutils as lalsimutils
import lal

MSUN_S = lal.MTSUN_SI  # G Msun / c^3 in seconds = 4.925491e-6


def mc_eta_to_m1m2(mc, eta):
    eta = min(eta, 0.25)
    Mtot = mc / (eta ** 0.6)
    delta = np.sqrt(max(0.0, 1.0 - 4.0 * eta))
    m1 = Mtot * (1.0 + delta) / 2.0
    m2 = Mtot * (1.0 - delta) / 2.0
    return m1, m2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--nmc", type=int, default=7)
    ap.add_argument("--neta", type=int, default=5)
    ap.add_argument("--mc-lo", type=float, default=32.5)
    ap.add_argument("--mc-hi", type=float, default=37.0)
    ap.add_argument("--eta-lo", type=float, default=0.20)
    ap.add_argument("--eta-hi", type=float, default=0.25)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fref", type=float, default=25.0)
    ap.add_argument("--approx", default="IMRPhenomD")
    ap.add_argument("--tref", type=float, default=1000000000.0)
    args = ap.parse_args()

    mcs = np.linspace(args.mc_lo, args.mc_hi, args.nmc)
    etas = np.linspace(args.eta_lo, args.eta_hi, args.neta)
    P_list = []
    rows = []
    for eta in etas:
        for mc in mcs:
            m1, m2 = mc_eta_to_m1m2(mc, eta)
            P = lalsimutils.ChooseWaveformParams()
            P.m1 = m1 * lal.MSUN_SI
            P.m2 = m2 * lal.MSUN_SI
            P.s1x = P.s1y = P.s1z = 0.0
            P.s2x = P.s2y = P.s2z = 0.0
            P.fmin = args.fmin
            P.fref = args.fref
            P.tref = args.tref
            P.dist = 500.0e6 * lal.PC_SI
            P.approx = lalsimutils.lalsim.GetApproximantFromString(args.approx)
            P_list.append(P)
            rows.append((mc, eta, m1, m2))
    lalsimutils.ChooseWaveformParams_array_to_xml(P_list, args.out.replace(".xml.gz", ""))
    print("# wrote %d grid points to %s" % (len(P_list), args.out))
    print("# idx  mchirp    eta     m1       m2")
    for i, (mc, eta, m1, m2) in enumerate(rows):
        print("%3d  %8.4f  %6.4f  %7.4f  %7.4f" % (i, mc, eta, m1, m2))


if __name__ == "__main__":
    main()
