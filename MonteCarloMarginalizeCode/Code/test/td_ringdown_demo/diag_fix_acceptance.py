#!/usr/bin/env python
"""Acceptance test for the windowing fix.  Repeat the lnL(distance) orientation scan with a
Tukey window applied to the data (--window-shape) AND the T1900249 PSD window-factor
correction (exactly as ILE integrate_likelihood_extrinsic_batchmode lines 742-746), for
WS in {0.0, 0.4}.  A correct fix makes: guess_snr physical (~60-80), recovered mf_snr peak
near the published 78.6 at the best-fit orientation and FALL OFF at wrong orientations, and
d* = Dref*Q/L sensible (hundreds of Mpc, > d_min) at good orientations.
"""
import os, sys
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, CODE); sys.path.insert(0, os.path.join(CODE, "test", "td_equivalence"))
import run_equivalence_study as t4
import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

T7C = os.environ.get("T7C_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "t7c_gw250114"))
GPS = 1420878141.2; dT = 1.0/2048.0; DREF = 405.0
CACHE = os.path.join(T7C, "data", "frames.cache")
chan = {"H1": "GWOSC-STRAIN", "L1": "GWOSC-STRAIN"}
psdf = {"H1": os.path.join(T7C, "psd", "H1-psd.xml.gz"), "L1": os.path.join(T7C, "psd", "L1-psd.xml.gz")}
Mtot = 71.94; eta = 0.2499; dd_ = np.sqrt(1-4*eta)
m1 = 0.5*Mtot*(1+dd_); m2 = 0.5*Mtot*(1-dd_)
tvals = np.linspace(-0.03, 0.03, int(2*0.03/dT))

def load_windowed(ws, psd_ws=0.0):
    data, psd, fmax_psd = t4.load_data_and_psd(CACHE, chan, psdf, GPS-4, GPS+4, dT, window_shape=ws)
    # T1900249 PSD correction (ILE lines 742-746): psd *= wfac_data / wfac_psd
    if ws > 0 or psd_ws > 0:
        for inst in psd:
            n = len(data[inst].data.data)
            wfd = lalsimutils.psd_windowing_factor(ws, n)
            wfp = lalsimutils.psd_windowing_factor(psd_ws, len(psd[inst].data.data))
            psd[inst].data.data *= wfd/wfp
    return data, psd, min(1000.0, fmax_psd)

def make(data, dist, incl, phi, theta, psi, phiref=1.2):
    return t4.make_P(m1, m2, 25.0, 25.0, GPS, dT, data["H1"].deltaF, dist_mpc=dist,
                     incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)

for ws in (0.0, 0.4):
    data, psd, fmax = load_windowed(ws)
    fac = TDInnerProductFactory(solver="auto")
    P0 = make(data, DREF, 0.7, 1.9, 1.2, 0.6)
    ri, ct, ctv, rho, gs, _ = t4.precompute(P0, data, psd, fmax, 2, False, 0.0, IP_factory=fac)
    print("\n===== window_shape=%.2f  =>  guess_snr = %.2f  (published netSNR 78.6) =====" % (ws, gs))
    def LQ(incl, phi, theta, psi):
        P1 = make(data, DREF, incl, phi, theta, psi)
        P2 = make(data, DREF/2, incl, phi, theta, psi)
        l1, _ = t4.network_lnL_of_t(P1, ri, rho, ct, ctv, 2, tvals, GPS)
        l2, _ = t4.network_lnL_of_t(P2, ri, rho, ct, ctv, 2, tvals, GPS)
        Q = 2*l1 - l2; L = l1 + 0.5*Q; ipk = int(np.argmax(l1))
        Lp, Qp = L[ipk], Q[ipk]
        if Qp <= 0 or Lp <= 0: return Lp, Qp, np.nan, np.nan, np.nan
        return Lp, Qp, DREF*Qp/Lp, np.sqrt(Qp), Lp/np.sqrt(Qp)
    print("   %-30s %8s %8s %9s %8s %8s" % ("orientation", "L", "Q", "d*_Mpc", "rho_opt", "mf_snr"))
    cases = [("fiducial(report)", 0.7, 1.9, 1.2, 0.6)]
    for inc in [0.0, 0.4, 0.8, 1.2, 1.5708, 2.4, 3.0]:
        cases.append(("inc=%.2f" % inc, inc, 1.9, 1.2, 0.6))
    for (ph, th) in [(0.5, 0.5), (2.0, 2.0), (4.0, 2.5), (5.0, 0.8)]:
        cases.append(("sky ph=%.1f th=%.1f inc0" % (ph, th), 0.2, ph, th, 0.6))
    best = -1; bestn = None; bestrow = None
    for name, inc, ph, th, ps in cases:
        Lp, Qp, ds, ro, mf = LQ(inc, ph, th, ps)
        print("   %-30s %8.1f %8.1f %9.1f %8.2f %8.2f" % (name, Lp, Qp, ds, ro, mf))
        if np.isfinite(mf) and mf > best: best = mf; bestn = name; bestrow = (ds, ro)
    print("   >> best recovered mf_snr = %.2f at '%s'  (d*=%.1f Mpc, rho_opt=%.2f)"
          % (best, bestn, bestrow[0], bestrow[1]))
