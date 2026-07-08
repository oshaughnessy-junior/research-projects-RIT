#!/usr/bin/env python
"""Where does the enormous <d|d> live?  Break down |d(f)|^2/S(f) by frequency band and
compare the DATA amplitude spectrum to the stored PSD, for the RIFT-loaded data.
Also compare RIFT's own window_shape=0 load to a windowed load, and re-estimate a PSD
from the on-source data in RIFT's convention to expose any data-vs-PSD normalization gap.
"""
import os, sys
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, CODE); sys.path.insert(0, os.path.join(CODE, "test", "td_equivalence"))
import run_equivalence_study as t4
import RIFT.lalsimutils as lalsimutils

T7C = os.environ.get("T7C_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "t7c_gw250114"))
GPS = 1420878141.2; dT = 1.0/2048.0
CACHE = os.path.join(T7C, "data", "frames.cache")
chan = {"H1": "GWOSC-STRAIN", "L1": "GWOSC-STRAIN"}
psdf = {"H1": os.path.join(T7C, "psd", "H1-psd.xml.gz"), "L1": os.path.join(T7C, "psd", "L1-psd.xml.gz")}

for ws in (0.0, 0.1):
    print("\n==================== window_shape = %.2f ====================" % ws)
    data, psd, fmax_psd = t4.load_data_and_psd(CACHE, chan, psdf, GPS-4, GPS+4, dT, window_shape=ws)
    for det in ("H1", "L1"):
        d = data[det]; S = psd[det]
        f = d.f0 + d.deltaF*np.arange(d.data.length)
        Sarr = np.zeros(d.data.length)
        for i in range(d.data.length):
            af = abs(f[i]); j = int(round((af - S.f0)/S.deltaF))
            if 0 <= j < S.data.length: Sarr[i] = S.data.data[j]
        pos = (f > 0) & (Sarr > 0)
        # <d|d> integrand per bin (RIFT: 4 df Re sum d*d/S over one-sided ~ use 2*df*|d|^2/S on 2-sided)
        integ = 2.0*d.deltaF*np.abs(d.data.data)**2/np.where(Sarr > 0, Sarr, np.inf)
        print(" [%s ws=%.2f] <d|d>_band[25,1000]=%.3e" % (det, ws,
              np.sum(integ[(np.abs(f) >= 25) & (np.abs(f) <= 1000)])))
        # banded contributions (positive freq)
        for lo, hi in [(0,25),(25,60),(60,120),(120,300),(300,1000),(1000,1024)]:
            m = pos & (f >= lo) & (f < hi)
            if np.any(m):
                # empirical data ASD in RIFT convention: S_data ~ 2*|d|^2*df*<something>; report ratio
                dpow = np.sum(integ[m])
                # ratio of data |d|^2 to PSD expectation per bin: |d|^2 / (S/(4 df)) ... report median ratio
                ratio = np.median(np.abs(d.data.data[m])**2 / (Sarr[m]/(4.0*d.deltaF)))
                print("     band[%4d,%4d): sum d/d=%.3e  median(|d|^2 / (S/4df))=%.2f  (=1 if data~PSD)"
                      % (lo, hi, dpow, ratio))
