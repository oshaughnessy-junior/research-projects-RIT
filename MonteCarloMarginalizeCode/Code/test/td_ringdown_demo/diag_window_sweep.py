#!/usr/bin/env python
"""GW250114 window-shape sweep: compare the extreme 0.4 taper against the PRODUCTION
window convention (helper_LDG_Events: window_shape = data_tukey_window_time*2/T_window,
default data_tukey_window_time=0.4 s -> 0.1 for an 8 s segment).

Replicates the ILE data+PSD load faithfully, INCLUDING the T1900249 <w^2> PSD-window
rescaling ILE applies when --window-shape>0 and --psd-window-shape unset
(psd *= psd_windowing_factor(window_shape)/psd_windowing_factor(0)).

Reports, per window_shape: guess_snr, in-band <d|d>, and the analytic best-fit-orientation
d* = Dref*Q/L and recovered mf-SNR at the published node (physics/inputs first, as the
original diagnosis did).
"""
import os, sys, json
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, CODE)
sys.path.insert(0, os.path.join(CODE, "test", "td_equivalence"))
import run_equivalence_study as t4
import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

T7C = "/home/richard.oshaughnessy/rift_td/analyses/t7c_gw250114"
GPS = 1420878141.2
SRATE = 2048.0
dT = 1.0/SRATE
DREF = 405.0
CACHE = os.path.join(T7C, "data", "frames.cache")
chan = {"H1": "GWOSC-STRAIN", "L1": "GWOSC-STRAIN"}
psdf = {"H1": os.path.join(T7C, "psd", "H1-psd.xml.gz"),
        "L1": os.path.join(T7C, "psd", "L1-psd.xml.gz")}

Mtot_det = 71.94; eta_pub = 0.2499
delta = np.sqrt(1.0 - 4.0*eta_pub)
m1_pub = 0.5*Mtot_det*(1.0+delta)
m2_pub = 0.5*Mtot_det*(1.0-delta)

def make(m1, m2, dist, incl, phi, theta, psi, deltaF, phiref=1.2):
    return t4.make_P(m1, m2, 25.0, 25.0, GPS, dT, deltaF, dist_mpc=dist,
                     incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)

def band_dd(data, psd, fmin, fmax):
    """in-band <d|d> = 4 Re sum |d|^2/S df over [fmin,fmax], one-sided-equivalent."""
    tot = 0.0
    for det in data:
        d = data[det].data.data
        S = psd[det].data.data
        Nf = len(S); df = psd[det].deltaF; f0 = psd[det].f0
        farr = f0 + df*np.arange(Nf)
        # non-herm hoff: length matches; take positive-freq half corresponding to psd
        dd = data[det].data.data
        # align lengths (psd resampled to data deltaF)
        n = min(len(dd), Nf)
        f = farr[:n]; Sn = S[:n]; dn = dd[:n]
        m = (f >= fmin) & (f <= fmax) & (Sn > 0)
        tot += 4.0*np.sum((np.abs(dn[m])**2)/Sn[m])*df
    return tot

tvals = np.linspace(-0.03, 0.03, int(2*0.03/dT))
fac = TDInnerProductFactory(solver="auto")

results = []
for w in [0.0, 0.1, 0.25, 0.4]:
    data, psd0, fmax_psd = t4.load_data_and_psd(CACHE, chan, psdf, GPS-4, GPS+4, dT, window_shape=w)
    deltaF = data["H1"].deltaF
    fmax = min(1000.0, fmax_psd)
    # ILE PSD-window rescaling (psd_window_shape unset -> factor(0)=1)
    import copy
    psd = {}
    for det in psd0:
        psd[det] = psd0[det]
        N = len(psd0[det].data.data)
        wfac = lalsimutils.psd_windowing_factor(w, N) if w > 0 else 1.0
        psd[det].data.data = psd0[det].data.data * wfac
    dd = band_dd(data, psd, 25.0, fmax)
    P0 = make(m1_pub, m2_pub, DREF, 0.7, 1.9, 1.2, 0.6, deltaF)
    ri, ct, ctv, rho, guess_snr, _ = t4.precompute(
        P0, data, psd, fmax, 2, inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=fac)

    def LQ(P):
        P.dist = DREF*1e6*lalsimutils.lsu_PC
        P2 = make(P.m1/lalsimutils.lsu_MSUN, P.m2/lalsimutils.lsu_MSUN, DREF/2.0,
                  P.incl, P.phi, P.theta, P.psi, deltaF, P.phiref)
        l1, _ = t4.network_lnL_of_t(P, ri, rho, ct, ctv, 2, tvals, GPS)
        l2, _ = t4.network_lnL_of_t(P2, ri, rho, ct, ctv, 2, tvals, GPS)
        Q = 2.0*l1 - l2; L = l1 + 0.5*Q
        i = int(np.argmax(l1)); Lp, Qp = L[i], Q[i]
        if Qp <= 0 or Lp <= 0: return np.nan, np.nan, np.nan
        return DREF*Qp/Lp, np.sqrt(Qp), Lp/np.sqrt(Qp)
    best_mf = 0.0; best_dstar = np.nan; best_ro = np.nan
    for inc in [0.0, 0.4, 0.8, 1.2, 2.0, 2.4, 3.0]:
        for (ph, th) in [(1.9,1.2),(1.0,1.0),(2.0,2.0),(4.0,2.5)]:
            P = make(m1_pub, m2_pub, DREF, inc, ph, th, 0.6, deltaF)
            ds, ro, mf = LQ(P)
            if np.isfinite(mf) and mf > best_mf:
                best_mf, best_dstar, best_ro = mf, ds, ro
    print("w=%.2f  guess_snr=%8.3f  <d|d>_inband=%10.4g  d*=%8.1f Mpc  rho_opt=%6.2f  mf_snr=%6.2f"
          % (w, guess_snr, dd, best_dstar, best_ro, best_mf))
    results.append(dict(window_shape=w, guess_snr=float(guess_snr), dd_inband=float(dd),
                        dstar=float(best_dstar), rho_opt=float(best_ro), mf_snr=float(best_mf)))

json.dump(results, open(os.path.join(HERE, "diag_window_sweep.json"), "w"), indent=2)
print("\n[done] -> diag_window_sweep.json")
print("Production window for 8 s segment = data_tukey_window_time*2/T_window = 0.4*2/8 = 0.1")
