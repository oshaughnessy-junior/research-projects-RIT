#!/usr/bin/env python
"""Verify the full-IMR truth-node gate still passes with time-INTERPOLATION on.
Regenerates the native zero-noise injection, evaluates the deterministic time-marginalized
full-config surface over grid.xml.gz with interpolate={False,True}, prints argmax offset."""
import os, sys
import numpy as np
import RIFT.lalsimutils as lalsimutils
import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood.td_inner_product import TDInnerProductFactory
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_equivalence"))
import run_equivalence_study as t4
import native_inject


def mc_eta(m1, m2):
    return (lalsimutils.mchirp(m1*lalsimutils.lsu_MSUN, m2*lalsimutils.lsu_MSUN)/lalsimutils.lsu_MSUN,
            lalsimutils.symRatio(m1*lalsimutils.lsu_MSUN, m2*lalsimutils.lsu_MSUN))


start, seglen, srate = 999999996.0, 8.0, 2048.0
deltaT = 1.0/srate; stop = start+seglen; tref = 1000000000.0
cache = os.path.join(HERE, "frames_native.cache")
native_inject.native_inject(50.0, 50.0, 700.0, 0.7, 1.2, 0.3, 2.1, 0.8, tref,
    os.path.join(HERE, "frames_native"), cache, instruments=("H1", "L1"),
    fmin=25.0, fref=25.0, srate=srate, seglen=seglen, start=start, noise_seed=None, verbose=False)
channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
psd_files = {"H1": os.path.join(HERE, "H1-psd.xml.gz"), "L1": os.path.join(HERE, "L1-psd.xml.gz")}
data, psd, fmax_psd = t4.load_data_and_psd(cache, channels, psd_files, start, stop, deltaT)
deltaF = data["H1"].deltaF; fmax = min(1000.0, fmax_psd)
tvals = np.linspace(-0.075, 0.075, int(2*0.075/deltaT))
Plist = lalsimutils.xml_to_ChooseWaveformParams_array(os.path.join(HERE, "grid.xml.gz"))
truth_mc, truth_eta = mc_eta(50.0, 50.0)
mc_grid = sorted(set(round(mc_eta(P.m1/lalsimutils.lsu_MSUN, P.m2/lalsimutils.lsu_MSUN)[0], 4) for P in Plist))
eta_grid = sorted(set(round(mc_eta(P.m1/lalsimutils.lsu_MSUN, P.m2/lalsimutils.lsu_MSUN)[1], 4) for P in Plist))
tmc = int(np.argmin([abs(v-truth_mc) for v in mc_grid])); teta = int(np.argmin([abs(v-truth_eta) for v in eta_grid]))

def build_P(m1, m2):
    return t4.make_P(m1, m2, 25.0, 25.0, tref, deltaT, deltaF, dist_mpc=700.0,
                     incl=0.7, phiref=1.2, theta=0.3, phi=2.1, psi=0.8)

fac = TDInnerProductFactory(solver="auto")
mcs, etas, lnF, lnT = [], [], [], []
for P in Plist:
    m1 = P.m1/lalsimutils.lsu_MSUN; m2 = P.m2/lalsimutils.lsu_MSUN
    mc, eta = mc_eta(m1, m2)
    pc = t4.precompute(build_P(m1, m2), data, psd, fmax, 2, False, 0.0, IP_factory=fac)
    ri, ct, ctv, rho, snr, _ = pc
    lmF = float(fl.FactoredLogLikelihoodTimeMarginalized(tvals, build_P(m1, m2), ri, rho, ct, ctv, 2, interpolate=False))
    lmT = float(fl.FactoredLogLikelihoodTimeMarginalized(tvals, build_P(m1, m2), ri, rho, ct, ctv, 2, interpolate=True))
    mcs.append(mc); etas.append(eta); lnF.append(lmF); lnT.append(lmT)
for name, ln in [("interp=False", lnF), ("interp=True", lnT)]:
    ip = int(np.argmax(ln))
    pmc, peta = mcs[ip], etas[ip]
    dmc = int(np.argmin([abs(v-round(pmc, 4)) for v in mc_grid])) - tmc
    deta = int(np.argmin([abs(v-round(peta, 4)) for v in eta_grid])) - teta
    print("full %-12s argmax mc=%.4f eta=%.4f lnL=%.4f | offset dmc=%+d deta=%+d | %s" %
          (name, pmc, peta, ln[ip], dmc, deta, "PASS(truth node)" if (dmc == 0 and deta == 0) else "OFF-TRUTH"))
