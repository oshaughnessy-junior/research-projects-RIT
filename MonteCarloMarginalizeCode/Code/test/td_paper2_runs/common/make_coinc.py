#!/usr/bin/env python
r"""make_coinc.py -- build a sim.xml + coinc.xml for a real event from its catalog parameters.

util_RIFT_pseudo_pipe.py --use-coinc needs a coinc.xml carrying the event time, a fiducial
mass point, per-IFO channels and an SNR hint. For a real GWOSC event (no pipeline coinc on the
submit host) we synthesize one from the catalog: write a one-row sim_inspiral with the
detector-frame masses/spins at the geocenter GPS, then convert it with the stock
util_SimInspiralToCoinc.py (the same path the PoC/wiring smoke used).

Usage:
  make_coinc.py --m1 40 --m2 35 --tref 1420878141.2 --ifos H1,L1 --snr 78.6 --outdir .
      [--s1z 0 --s2z 0 --fmin 20 --approx IMRPhenomXPHM]
"""
import argparse
import os
import subprocess

import numpy as np

import RIFT.lalsimutils as lalsimutils
import lal


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--m1", type=float, required=True, help="detector-frame m1 [Msun]")
    ap.add_argument("--m2", type=float, required=True, help="detector-frame m2 [Msun]")
    ap.add_argument("--tref", type=float, required=True, help="geocenter GPS")
    ap.add_argument("--ifos", default="H1,L1")
    ap.add_argument("--snr", type=float, default=20.0)
    ap.add_argument("--s1z", type=float, default=0.0)
    ap.add_argument("--s2z", type=float, default=0.0)
    ap.add_argument("--dist", type=float, default=500.0, help="fiducial distance [Mpc]")
    ap.add_argument("--fmin", type=float, default=20.0)
    ap.add_argument("--fref", type=float, default=20.0)
    ap.add_argument("--approx", default="IMRPhenomXPHM")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    P = lalsimutils.ChooseWaveformParams()
    P.m1 = args.m1 * lalsimutils.lsu_MSUN
    P.m2 = args.m2 * lalsimutils.lsu_MSUN
    P.s1z = args.s1z
    P.s2z = args.s2z
    P.dist = args.dist * 1e6 * lal.PC_SI
    P.fmin = args.fmin
    P.fref = args.fref
    P.tref = args.tref
    try:
        P.approx = lalsimutils.lalsim.GetApproximantFromString(args.approx)
    except Exception:
        pass
    sim = os.path.join(args.outdir, "sim.xml.gz")
    lalsimutils.ChooseWaveformParams_array_to_xml([P], fname=sim.replace(".xml.gz", ""))

    coinc = os.path.join(args.outdir, "coinc.xml")
    here = os.path.dirname(os.path.abspath(__file__))
    exe = os.path.join(here, "..", "..", "..", "bin", "util_SimInspiralToCoinc.py")
    if not os.path.exists(exe):
        exe = "util_SimInspiralToCoinc.py"     # rely on PATH
    import sys
    cmd = [sys.executable, exe, "--sim-xml", sim, "--event", "0", "--output", coinc,
           "--injected-snr", str(args.snr)]
    for ifo in [x.strip() for x in args.ifos.split(",") if x.strip()]:
        cmd += ["--ifo", ifo]
    print("[coinc]", " ".join(cmd))
    subprocess.check_call(cmd)
    print("[coinc] wrote %s and %s (m1=%.3f m2=%.3f tref=%.3f snr=%.1f)"
          % (sim, coinc, args.m1, args.m2, args.tref, args.snr))
    _ = np                                        # keep numpy import (future spins)


if __name__ == "__main__":
    main()
