#!/usr/bin/env python
"""Localize the <d|h> vs <h|h> normalization inconsistency found by diag_lnL_distance.py.

(a) FD vs TD guess_snr on the SAME real data+PSD (IP_factory=None = standard FD inner
    product, vs td-toeplitz). If FD also inflates -> shared data/PSD handling; if only TD
    inflates -> the TD kernel inner product normalization.
(b) Independent optimal SNR rho_opt^2 = 4 int |h_det|^2/S df from a fresh FD detector
    strain (lalsimutils) at the published params/orientation, compared to Q=<h_proj|h_proj>.
(c) <d|d> = 4 int |d|^2/S df vs the noise expectation (~ #bins) + published signal^2.
(d) data amplitude sanity (whitened RMS, raw strain peak).
"""
import os, sys
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, CODE)
sys.path.insert(0, os.path.join(CODE, "test", "td_equivalence"))
import run_equivalence_study as t4
import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.td_inner_product import TDInnerProductFactory
import RIFT.likelihood.factored_likelihood as fl

T7C = os.environ.get("T7C_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "t7c_gw250114"))
GPS = 1420878141.2; SRATE = 2048.0; dT = 1.0/SRATE; DREF = 405.0
CACHE = os.path.join(T7C, "data", "frames.cache")
chan = {"H1": "GWOSC-STRAIN", "L1": "GWOSC-STRAIN"}
psdf = {"H1": os.path.join(T7C, "psd", "H1-psd.xml.gz"), "L1": os.path.join(T7C, "psd", "L1-psd.xml.gz")}
Mtot_det = 71.94; eta = 0.2499; d_ = np.sqrt(1-4*eta)
m1 = 0.5*Mtot_det*(1+d_); m2 = 0.5*Mtot_det*(1-d_)

data, psd, fmax_psd = t4.load_data_and_psd(CACHE, chan, psdf, GPS-4, GPS+4, dT)
deltaF = data["H1"].deltaF; fmax = min(1000.0, fmax_psd); N2 = data["H1"].data.length
fmin = 25.0
print("[setup] N=%d deltaF=%.5f fmax=%.1f m1=%.3f m2=%.3f" % (N2, deltaF, fmax, m1, m2))

def make(dist, incl=0.7, phi=1.9, theta=1.2, psi=0.6, phiref=1.2):
    return t4.make_P(m1, m2, fmin, 25.0, GPS, dT, deltaF, dist_mpc=dist,
                     incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)

# ---------- (a) FD vs TD guess_snr ----------
print("\n(a) guess_snr on identical data/PSD:")
P0 = make(DREF)
for tag, ipf in [("TD-toeplitz", TDInnerProductFactory(solver="auto")), ("FD-standard", None)]:
    P = make(DREF)
    out = t4.precompute(P, data, psd, fmax, 2, inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=ipf)
    ri, ct, ctv, rho, gs, _ = out
    # per-det unprojected optimal SNR of bare mode = sqrt<hlm|hlm>, and mf = max|rho|/sqrt<hlm|hlm>
    line = []
    for det in rho:
        m = (2, 2)
        cc = np.real(ct[det][(m, m)]); rmax = np.max(np.abs(rho[det][m].data.data))
        line.append("%s: sqrt<hh>=%.1f max|rho|/sqrt<hh>=%.1f" % (det, np.sqrt(cc), rmax/np.sqrt(cc)))
    print("   %-12s guess_snr=%.2f   %s" % (tag, gs, " | ".join(line)))

# ---------- (b) independent optimal SNR of projected detector strain ----------
print("\n(b) independent optimal SNR (4 int |h_det|^2/S df) vs Q=<h_proj|h_proj>:")
# Build FD detector strain h(f) exactly like ILE templates: non-herm hoff projected to detector.
incl, phi, theta, psi, phiref = 0.7, 1.9, 1.2, 0.6, 1.2
P = make(DREF, incl, phi, theta, psi, phiref)
for det in ("H1", "L1"):
    Pd = make(DREF, incl, phi, theta, psi, phiref); Pd.detector = det
    hf = lalsimutils.complex_hoff(Pd)   # projected detector strain h(f), two-sided
    # align PSD grid to hf
    S = psd[det]
    fvals = hf.f0 + hf.deltaF*np.arange(hf.data.length)
    # two-sided |h|^2/S sum; RIFT inner product convention: 4 Re int_0^inf, use one-sided band
    df = hf.deltaF
    Sarr = np.zeros(hf.data.length)
    # map psd (one-sided, f>=0) onto hf grid (two-sided). Use |f|.
    psd_f0 = S.f0; psd_df = S.deltaF; psd_len = S.data.length
    for i in range(hf.data.length):
        f = fvals[i]
        af = abs(f)
        j = int(round((af - psd_f0)/psd_df))
        if 0 <= j < psd_len and fmin <= af <= fmax:
            Sarr[i] = S.data.data[j]
    mask = Sarr > 0
    rho2 = np.sum(np.abs(hf.data.data[mask])**2 / Sarr[mask]) * 2.0 * df   # 2-sided -> factor accounts
    print("   %s: independent rho_opt(4int|h|^2/S) = %.2f" % (det, np.sqrt(rho2)))

# Q from precompute path at same orientation (projected optimal SNR^2 at Dref)
ri, ct, ctv, rho, gs, _ = t4.precompute(make(DREF), data, psd, fmax, 2, False, 0.0,
                                        IP_factory=TDInnerProductFactory(solver="auto"))
tvals = np.linspace(-0.03, 0.03, int(2*0.03/dT))
P1 = make(DREF, incl, phi, theta, psi, phiref)
P2 = make(DREF/2, incl, phi, theta, psi, phiref)
l1, per1 = t4.network_lnL_of_t(P1, ri, rho, ct, ctv, 2, tvals, GPS)
l2, _ = t4.network_lnL_of_t(P2, ri, rho, ct, ctv, 2, tvals, GPS)
Q = 2*l1 - l2; L = l1 + 0.5*Q; ipk = int(np.argmax(l1))
print("   network Q=<h_proj|h_proj>@Dref = %.1f  -> sqrt(Q)=rho_opt_proj = %.2f" % (Q[ipk], np.sqrt(Q[ipk])))
print("   network L=<d|h_proj>@Dref      = %.1f  -> L/sqrt(Q)=mf_snr = %.2f  d*=%.1f Mpc"
      % (L[ipk], L[ipk]/np.sqrt(Q[ipk]), DREF*Q[ipk]/L[ipk]))
# per-detector single-detector optimal SNR from crossTerms directly:
print("   per-det <h_proj|h_proj> via crossTerms+Ylm+F:")
from RIFT.likelihood.factored_likelihood import ComputeYlms, ComplexAntennaFactor
Ylms = ComputeYlms(2, incl, -phiref, selected_modes=list(rho["H1"].keys()))
for det in ("H1", "L1"):
    F = ComplexAntennaFactor(det, phi, theta, psi, GPS)
    U = 0.0
    for (a, b), val in ct[det].items():
        U += np.conj(Ylms[a])*Ylms[b]*val
    hh = np.real(np.conj(F)*F*U) / 4.0   # SingleDetectorLogLikelihood uses (1/4) x^2 U with x=Dref/D; at D=Dref proj SNR^2 = |F|^2 * sum ...
    print("     %s |F|=%.3f  raw sum_lm conj(Y)Y U = %.1f" % (det, np.abs(F), np.real(U)))

# ---------- (c) <d|d> ----------
print("\n(c) <d|d> = 4 int |d|^2/S df (dominated by noise; compare to ~#bins + 78.6^2):")
dd_net = 0.0
for det in ("H1", "L1"):
    dfd = data[det]; S = psd[det]
    fvals = dfd.f0 + dfd.deltaF*np.arange(dfd.data.length)
    Sarr = np.zeros(dfd.data.length)
    for i in range(dfd.data.length):
        af = abs(fvals[i]); j = int(round((af - S.f0)/S.deltaF))
        if 0 <= j < S.data.length and fmin <= af <= fmax:
            Sarr[i] = S.data.data[j]
    mask = Sarr > 0
    nbins = np.sum(fvals[mask] > 0)   # positive-freq bins in band
    dd = np.sum(np.abs(dfd.data.data[mask])**2/Sarr[mask])*2.0*dfd.deltaF
    print("   %s: <d|d>=%.1f  sqrt=%.1f  (#posbins=%d, noise-expect <d|d>~2*nbins=%d)"
          % (det, dd, np.sqrt(dd), nbins, 2*nbins))
    dd_net += dd
print("   network sqrt<d|d> = %.1f  (Cauchy-Schwarz bound on any mf_snr = L/sqrt(Q))" % np.sqrt(dd_net))

# ---------- (d) data amplitude sanity ----------
print("\n(d) raw strain sanity (inverse FFT to time domain peak):")
for det in ("H1", "L1"):
    dfd = data[det]
    td = np.fft.ifft(dfd.data.data)*dfd.deltaF*dfd.data.length  # rough
    print("   %s: |h(f)| median=%.3e  strain-TD rms~%.3e peak~%.3e"
          % (det, np.median(np.abs(dfd.data.data)), np.std(np.real(td)), np.max(np.abs(np.real(td)))))
