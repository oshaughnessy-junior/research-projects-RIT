#!/usr/bin/env python
"""GW250114 distance-runaway diagnosis (physics/inputs first).

At the published intrinsic node, reconstruct the exact analytic lnL(distance) from the
precomputed Q/U/V terms (lnL is quadratic in x=D_ref/D):

    lnL(x) = x * L  -  0.5 * x^2 * Q ,   x = D_ref/D
      L = <d|h_proj>_{Dref}   (linear, cross term)
      Q = <h_proj|h_proj>_{Dref}   (quadratic, template self-norm = optimal SNR^2 at Dref)
    argmax over distance:  d* = D_ref * Q / L   (PSD-normalization INVARIANT)
    recovered mf SNR = L / sqrt(Q)   (PSD-normalization scales as 1/sqrt(S))
    optimal SNR at Dref/orientation = sqrt(Q)

L,Q are obtained the same way the deterministic surface driver does it: evaluate the
network lnL(t) at two distances D1 and D1/2 and solve the 2x2 (Q = 2 l1 - l2, L = l1 + Q/2).

We scan inclination + sky to map d*, recovered SNR, and optimal SNR, to decide whether the
d->d_min runaway is REAL in the likelihood (d* < d_min at the best-fit orientation) or an
artifact of evaluating at a poorly-matched fixed extrinsic (antenna null / wrong inclination).
Also decomposes guess_snr and cross-checks the PSD amplitude.
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
DREF = 405.0                      # published luminosity distance
CACHE = os.path.join(T7C, "data", "frames.cache")
chan = {"H1": "GWOSC-STRAIN", "L1": "GWOSC-STRAIN"}
psdf = {"H1": os.path.join(T7C, "psd", "H1-psd.xml.gz"),
        "L1": os.path.join(T7C, "psd", "L1-psd.xml.gz")}

# published detector-frame masses (Mtot_det=71.94, eta=0.2499)
Mtot_det = 71.94; eta_pub = 0.2499
delta = np.sqrt(1.0 - 4.0*eta_pub)
m1_pub = 0.5*Mtot_det*(1.0+delta)
m2_pub = 0.5*Mtot_det*(1.0-delta)
print("[params] published detector-frame m1=%.4f m2=%.4f Mc=%.4f eta=%.5f"
      % (m1_pub, m2_pub, lalsimutils.mchirp(m1_pub*lalsimutils.lsu_MSUN, m2_pub*lalsimutils.lsu_MSUN)/lalsimutils.lsu_MSUN, eta_pub))

data, psd, fmax_psd = t4.load_data_and_psd(CACHE, chan, psdf, GPS-4, GPS+4, dT)
deltaF = data["H1"].deltaF
fmax = min(1000.0, fmax_psd)
N2 = data["H1"].data.length
print("[setup] N=%d deltaF=%.6f fmax=%.1f fmax_psd=%.1f" % (N2, deltaF, fmax, fmax_psd))

# ---- PSD amplitude sanity (ASD at 100 Hz) ----
for det in ("H1", "L1"):
    S = psd[det].data.data
    f0 = psd[det].f0; df = psd[det].deltaF
    farr = f0 + df*np.arange(len(S))
    i100 = np.argmin(np.abs(farr-100.0))
    print("  [PSD %s] f0=%.3f df=%.4f len=%d  S(100Hz)=%.3e  ASD(100Hz)=%.3e /rtHz"
          % (det, f0, df, len(S), S[i100], np.sqrt(S[i100])))

tvals = np.linspace(-0.03, 0.03, int(2*0.03/dT))

def make(m1, m2, dist, incl, phi, theta, psi, phiref=1.2):
    return t4.make_P(m1, m2, 25.0, 25.0, GPS, dT, deltaF, dist_mpc=dist,
                     incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)

# Precompute once at published node with full IMR template (no segmentation), Dref.
fac = TDInnerProductFactory(solver="auto")   # full config
P0 = make(m1_pub, m2_pub, DREF, 0.7, 1.9, 1.2, 0.6)
pc = t4.precompute(P0, data, psd, fmax, 2, inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=fac)
ri, ct, ctv, rho, guess_snr, _ = pc
print("\n[guess_snr] full-config guess_snr = %.3f  (=> raw sqrt(sum delta)=%.1f, /2.3)"
      % (guess_snr, guess_snr*2.3))
# decompose guess_snr: delta = max|rho|^2 / Re<hlm|hlm>
print("  per-detector / per-mode delta = max|<hlm|d>|^2 / Re<hlm|hlm>  (matched-filter SNR^2):")
raw2 = 0.0
for det in rho:
    for mode in rho[det]:
        cc = np.real(ct[det][(mode, mode)])
        if cc > 0:
            rmax = np.max(np.abs(rho[det][mode].data.data))
            d_here = rmax**2/cc
            raw2 += d_here
            print("    %s mode%s : max|rho|=%.4g  <hlm|hlm>=%.4g  delta=%.2f  sqrt(delta)=%.2f"
                  % (det, mode, rmax, cc, d_here, np.sqrt(d_here)))
print("  sum delta = %.1f  sqrt = %.2f  /2.3 = %.2f" % (raw2, np.sqrt(raw2), np.sqrt(raw2)/2.3))

def LQ_at(P):
    """Return (L_peak, Q_peak, d_star, rho_opt, mf_snr) from two-distance eval at P's orientation."""
    P1 = P; P1.dist = DREF*1e6*lalsimutils.lsu_PC
    P2 = make(P.m1/lalsimutils.lsu_MSUN, P.m2/lalsimutils.lsu_MSUN, DREF/2.0,
              P.incl, P.phi, P.theta, P.psi, P.phiref)
    l1, _ = t4.network_lnL_of_t(P1, ri, rho, ct, ctv, 2, tvals, GPS)
    l2, _ = t4.network_lnL_of_t(P2, ri, rho, ct, ctv, 2, tvals, GPS)
    Q = 2.0*l1 - l2
    L = l1 + 0.5*Q
    ipk = int(np.argmax(l1))          # peak time of the D1 lnL
    Lp, Qp = L[ipk], Q[ipk]
    if Qp <= 0 or Lp <= 0:
        return Lp, Qp, np.nan, np.nan, np.nan
    dstar = DREF*Qp/Lp
    rho_opt = np.sqrt(Qp)
    mf = Lp/np.sqrt(Qp)
    return Lp, Qp, dstar, rho_opt, mf

# ---- Orientation/sky scan at published node ----
print("\n[lnL(d) scan] published node, full IMR.  d* = Dref*Q/L (PSD-invariant);")
print("   rho_opt = sqrt(Q) (optimal SNR at Dref); mf_snr = L/sqrt(Q) (recovered).")
print("   %-38s %8s %8s %10s %9s %9s" % ("orientation (incl,phi,theta,psi)", "L", "Q", "d*_Mpc", "rho_opt", "mf_snr"))
scan = []
# the report's fixed fiducial
cases = [("fiducial(report)", 0.7, 1.9, 1.2, 0.6)]
# inclination scan at a representative sky (overhead-ish for network)
for inc in [0.0, 0.4, 0.8, 1.2, 1.5708, 2.0, 2.4, 3.0]:
    cases.append(("inc=%.2f" % inc, inc, 1.9, 1.2, 0.6))
# sky scan at face-on
for (ph, th) in [(0.5, 0.5), (1.0, 1.0), (2.0, 2.0), (3.0, 1.5), (4.0, 2.5), (5.0, 0.8)]:
    cases.append(("sky phi=%.1f th=%.1f (inc0)" % (ph, th), 0.2, ph, th, 0.6))
best_mf = 0.0; best_case = None
for name, inc, ph, th, ps in cases:
    P = make(m1_pub, m2_pub, DREF, inc, ph, th, ps)
    Lp, Qp, ds, ro, mf = LQ_at(P)
    scan.append(dict(name=name, incl=inc, phi=ph, theta=th, psi=ps,
                     L=float(Lp), Q=float(Qp), dstar=float(ds), rho_opt=float(ro), mf_snr=float(mf)))
    flag = ""
    if np.isfinite(mf) and mf > best_mf:
        best_mf = mf; best_case = name
    print("   %-38s %8.1f %8.1f %10.1f %9.2f %9.2f" % (name, Lp, Qp, ds, ro, mf))
print("\n[best-fit orientation] max recovered mf_snr = %.2f at '%s'  (published netSNR=78.6)"
      % (best_mf, best_case))
mx = max(scan, key=lambda r: (r["mf_snr"] if np.isfinite(r["mf_snr"]) else -1))
print("   at that orientation: d* = %.1f Mpc, rho_opt(Dref) = %.2f" % (mx["dstar"], mx["rho_opt"]))

json.dump({"guess_snr": float(guess_snr), "guess_snr_raw_sqrt_sumdelta": float(np.sqrt(raw2)),
           "best_mf_snr": float(best_mf), "best_case": best_case, "scan": scan,
           "published_netSNR": 78.6, "Dref": DREF},
          open(os.path.join(HERE, "diag_lnL_distance.json"), "w"), indent=2)
print("\n[done] -> diag_lnL_distance.json")
