#!/usr/bin/env python
"""
run_equivalence_study.py -- T4 full-segment TD-vs-FD equivalence study (the GO/NO-GO gate).

Drives the REAL RIFT Precompute + factored log-likelihood code path
(RIFT.likelihood.factored_likelihood) on synthetic zero-noise (and optionally
Gaussian-noise) BBH data, once with the classic frequency-domain-diagonal inner
product (IP_factory=None, == fd-diagonal) and once with the time-domain Toeplitz
precision kernel (IP_factory=TDInnerProductFactory, == td-toeplitz).  It reproduces
exactly what `integrate_likelihood_extrinsic_batchmode` does up to (but not
including) the Monte-Carlo extrinsic marginalization -- so every comparison here is
DETERMINISTIC (no MC error).

It loads data with `frame_data_to_non_herm_hoff` and PSDs with
`get_psd_series_from_xmldoc` + `resample_psd_series`, exactly the ILE data path
(ILE:704-770).  The intrinsic point and detector/PSD handling are identical between
the two runs; only the inner product differs.

Three comparisons (mapping to the task's PRIMARY / GATE / INSTRUCTIVE-EXTRA):

  PRIMARY  (deterministic lnL(t)):  for the TRUE intrinsic point, compare the
     per-detector single-detector lnL(t) time series and the network lnL(t)
     produced by DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop pointwise.
     fd-diagonal uses inv_spec_trunc_time=0 (exact FD kernel) for the fair
     apples-to-apples comparison against the exact TD kernel.

  GATE (deterministic lnL_marg):  over a small intrinsic grid (true point +
     perturbations in mchirp/eta), compute the TIME-marginalized network
     log-likelihood FactoredLogLikelihoodTimeMarginalized (a deterministic
     quadrature over the time window at the injected sky/phase/distance -- this is
     the |Delta lnL_marg| the AC gates on, with ZERO MC error because we do not run
     the extrinsic MC).  Report |Delta lnL_marg| per point vs the 0.05 gate.

  INSTRUCTIVE-EXTRA (inv-spec-trunc):  fd-diagonal with DEFAULT inv_spec_trunc_time=8
     vs td-toeplitz -- quantify how much inverse-spectrum truncation itself moves lnL
     on this smooth PSD (expected tiny; the point is the TD path needs no such knob).

Env: ~/.conda/envs/junior_rift/bin/python with
     PYTHONPATH=<worktree>/MonteCarloMarginalizeCode/Code (lal 7.7.0).
"""
import os
import sys
import json
import argparse

import numpy as np

import lal
import RIFT.lalsimutils as lalsimutils
import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

HERE = os.path.dirname(os.path.abspath(__file__))


def load_data_and_psd(cache, channels, psd_files, start, stop, deltaT,
                      window_shape=0.0):
    """Replicate the ILE data/PSD load (ILE:704-770).  Returns (data_dict, psd_dict, fmax)."""
    data_dict, psd_dict = {}, {}
    for inst, chan in channels.items():
        data_dict[inst] = lalsimutils.frame_data_to_non_herm_hoff(
            cache, inst + ":" + chan, start=start, stop=stop,
            window_shape=window_shape, deltaT=deltaT)
    fmax = None
    for inst, psdf in psd_files.items():
        psd_dict[inst] = lalsimutils.get_psd_series_from_xmldoc(psdf, inst)
        deltaF = data_dict[inst].deltaF
        psd_dict[inst] = lalsimutils.resample_psd_series(psd_dict[inst], deltaF)
        assert abs(psd_dict[inst].deltaF - deltaF) < 1e-9
        this_fmax = psd_dict[inst].f0 + deltaF * (psd_dict[inst].data.length - 1)
        fmax = this_fmax if fmax is None else min(fmax, this_fmax)
    return data_dict, psd_dict, fmax


def make_P(m1_msun, m2_msun, fmin, fref, tref, deltaT, deltaF, approx="IMRPhenomD",
           dist_mpc=None, incl=0.7, phiref=1.2, theta=0.3, phi=2.1, psi=0.8):
    P = lalsimutils.ChooseWaveformParams()
    P.m1 = m1_msun * lalsimutils.lsu_MSUN
    P.m2 = m2_msun * lalsimutils.lsu_MSUN
    P.fmin = fmin
    P.fref = fref
    P.tref = tref
    P.deltaT = deltaT
    P.deltaF = deltaF
    P.approx = lalsimutils.lalsim.GetApproximantFromString(approx)
    P.radec = True
    P.taper = lalsimutils.lsu_TAPER_START
    P.incl = incl
    P.phiref = phiref
    P.theta = theta
    P.phi = phi
    P.psi = psi
    if dist_mpc is not None:
        P.dist = dist_mpc * 1e6 * lalsimutils.lsu_PC
    return P


def precompute(P, data_dict, psd_dict, fmax, Lmax, inv_spec_trunc_Q, T_spec,
               IP_factory=None):
    """Run PrecomputeLikelihoodTerms exactly as analyze_event does (ILE:1527)."""
    fiducial_epoch = P.tref
    t_window = 0.15  # ILE default internal_data_storage_window_half
    extra_kwargs = {}
    if IP_factory is not None:
        extra_kwargs['IP_factory'] = IP_factory
    out = fl.PrecomputeLikelihoodTerms(
        fiducial_epoch, t_window, P, data_dict, psd_dict, Lmax, fmax,
        False, inv_spec_trunc_Q, T_spec,
        ignore_threshold=None, verbose=False, quiet=True,
        skip_interpolation=False, **extra_kwargs)
    # returns rholms_intp, cross_terms, cross_terms_V, rholms, guess_snr, rest
    return out


def network_lnL_of_t(P, rholms_intp, rholms, cross_terms, cross_terms_V, Lmax,
                     tvals, fiducial_epoch):
    """Deterministic network lnL(t) at the given sky/phase/distance (nearest-neighbor,
    exactly the code path used inside FactoredLogLikelihoodTimeMarginalized)."""
    from RIFT.likelihood.factored_likelihood import (
        ComputeYlms, ComplexAntennaFactor, ComputeArrivalTimeAtDetector,
        SingleDetectorLogLikelihood)
    RA, DEC = P.phi, P.theta
    tref = P.tref
    incl, psi, dist, phiref = P.incl, P.psi, P.dist, P.phiref
    detectors = list(rholms_intp.keys())
    Ylms = ComputeYlms(Lmax, incl, -phiref,
                       selected_modes=rholms_intp[detectors[0]].keys())
    lnL = np.zeros(len(tvals), dtype=np.float128)
    per_det = {}
    for det in detectors:
        CT, CTV = cross_terms[det], cross_terms_V[det]
        F = ComplexAntennaFactor(det, RA, DEC, psi, tref)
        t_det = ComputeArrivalTimeAtDetector(det, RA, DEC, tref)
        det_rholms = {}
        for key, rhoTS in rholms[det].items():
            tfirst = float(t_det) + tvals[0]
            ifirst = int(np.round((float(tfirst) - float(rhoTS.epoch)) / rhoTS.deltaT) + 0.5)
            ilast = ifirst + len(tvals)
            det_rholms[key] = rhoTS.data.data[ifirst:ilast]
        this = SingleDetectorLogLikelihood(det_rholms, CT, CTV, Ylms, F, dist)
        per_det[det] = np.asarray(this, dtype=np.float64)
        lnL += this
    return np.asarray(lnL, dtype=np.float64), per_det


def lnL_marg_time(P, rholms_intp, rholms, cross_terms, cross_terms_V, Lmax, tvals):
    """Deterministic time-marginalized network lnL (the GATE quantity)."""
    return float(fl.FactoredLogLikelihoodTimeMarginalized(
        tvals, P, rholms_intp, rholms, cross_terms, cross_terms_V, Lmax,
        interpolate=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=os.path.join(HERE, "frames.cache"))
    ap.add_argument("--psd-H1", default=os.path.join(HERE, "H1-psd.xml.gz"))
    ap.add_argument("--psd-L1", default=os.path.join(HERE, "L1-psd.xml.gz"))
    # window strictly INSIDE the 8s frame [999999998.335, 1000000006.335]; the
    # frame epoch is non-integer (writer padding), so read an interior segment.
    # frame_data_to_non_herm_hoff pads this 6s request back up to N=2^14 (8s).
    ap.add_argument("--start", type=float, default=999999999.0)
    ap.add_argument("--stop", type=float, default=1000000005.0)
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--m1", type=float, default=35.0)
    ap.add_argument("--m2", type=float, default=35.0)
    ap.add_argument("--dist", type=float, default=500.0)
    ap.add_argument("--tref", type=float, default=1000000000.0)
    ap.add_argument("--lmax", type=int, default=2)
    ap.add_argument("--t-window", type=float, default=0.10,
                    help="half-width [s] of the deterministic lnL(t) / lnL_marg window")
    ap.add_argument("--out-json", default=os.path.join(HERE, "equivalence_results.json"))
    ap.add_argument("--out-lnLt", default=os.path.join(HERE, "lnLt_dump.txt"))
    args = ap.parse_args()

    deltaT = 1.0 / args.srate
    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    psd_files = {"H1": args.psd_H1, "L1": args.psd_L1}

    data_dict, psd_dict, fmax_psd = load_data_and_psd(
        args.cache, channels, psd_files, args.start, args.stop, deltaT)
    N2 = data_dict["H1"].data.length
    deltaF = data_dict["H1"].deltaF
    fmax = min(args.fmax, fmax_psd)
    print("[setup] N(two-sided)=%d deltaF=%.6f deltaT=%.3e fmax=%.1f (dense TD path if N<=%d)"
          % (N2, deltaF, deltaT, fmax, 1 << 14))

    results = {"setup": {"N_two_sided": int(N2), "deltaF": deltaF, "deltaT": deltaT,
                         "fmin": args.fmin, "fmax": fmax, "srate": args.srate,
                         "m1": args.m1, "m2": args.m2, "dist": args.dist,
                         "lmax": args.lmax, "t_window": args.t_window,
                         "detectors": list(channels.keys())}}

    def build_P(m1, m2):
        return make_P(m1, m2, args.fmin, args.fmin, args.tref, deltaT, deltaF,
                      dist_mpc=args.dist)

    tvals = np.linspace(-args.t_window, args.t_window,
                        int(2 * args.t_window / deltaT))

    # ---------------- PRIMARY: deterministic lnL(t) at the TRUE point ----------------
    print("\n=== PRIMARY: deterministic lnL(t) at true point (fd inv-spec-trunc=0 vs td) ===")
    P_true = build_P(args.m1, args.m2)
    # fd-diagonal, inv_spec_trunc OFF (exact FD kernel) -- fair vs exact TD kernel
    pc_fd = precompute(P_true, data_dict, psd_dict, fmax, args.lmax,
                       inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=None)
    P_true = build_P(args.m1, args.m2)
    td_factory = TDInnerProductFactory(solver="auto")
    pc_td = precompute(P_true, data_dict, psd_dict, fmax, args.lmax,
                       inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=td_factory)

    ri_fd, ct_fd, ctv_fd, rho_fd, snr_fd, _ = pc_fd
    ri_td, ct_td, ctv_td, rho_td, snr_td, _ = pc_td
    print("[primary] guess_snr fd=%.6f td=%.6f  |diff|=%.3e" %
          (snr_fd, snr_td, abs(snr_fd - snr_td)))

    P_eval = build_P(args.m1, args.m2)
    lnLt_fd, per_fd = network_lnL_of_t(P_eval, ri_fd, rho_fd, ct_fd, ctv_fd,
                                       args.lmax, tvals, args.tref)
    lnLt_td, per_td = network_lnL_of_t(P_eval, ri_td, rho_td, ct_td, ctv_td,
                                       args.lmax, tvals, args.tref)
    absdev = np.abs(lnLt_fd - lnLt_td)
    scale = max(np.max(np.abs(lnLt_fd)), 1e-30)
    reldev = absdev / scale
    imax = int(np.argmax(lnLt_fd))
    print("[primary] max|lnL_fd| = %.4f (peak at t=%.5f s)" % (scale, tvals[imax]))
    print("[primary] network lnL(t): max abs dev = %.3e ; max rel dev = %.3e" %
          (absdev.max(), reldev.max()))
    per_det_dev = {}
    for det in per_fd:
        d = np.abs(per_fd[det] - per_td[det])
        s = max(np.max(np.abs(per_fd[det])), 1e-30)
        per_det_dev[det] = {"max_abs": float(d.max()), "max_rel": float(d.max() / s)}
        print("[primary]   %s single-det lnL(t): max abs=%.3e rel=%.3e" %
              (det, d.max(), d.max() / s))
    np.savetxt(args.out_lnLt,
               np.column_stack([tvals, lnLt_fd, lnLt_td, lnLt_fd - lnLt_td]),
               header="t_rel  lnLt_fd(inv0)  lnLt_td  residual")
    results["primary"] = {
        "guess_snr_fd": float(snr_fd), "guess_snr_td": float(snr_td),
        "guess_snr_absdiff": float(abs(snr_fd - snr_td)),
        "peak_lnL": float(scale), "peak_t": float(tvals[imax]),
        "network_max_abs_dev": float(absdev.max()),
        "network_max_rel_dev": float(reldev.max()),
        "per_det": per_det_dev,
    }

    # ---------------- GATE: deterministic lnL_marg over intrinsic grid ----------------
    print("\n=== GATE: deterministic |Delta lnL_marg| over intrinsic grid (gate 0.05) ===")
    # true point + perturbations in mchirp/eta
    mc_true = lalsimutils.mchirp(args.m1 * lalsimutils.lsu_MSUN,
                                 args.m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN
    grid = [("true", args.m1, args.m2)]
    # perturb by scaling both masses (mchirp shift) and by mass ratio (eta shift)
    for f in (0.98, 1.02):
        grid.append(("mc*%.2f" % f, args.m1 * f, args.m2 * f))
    for dq in (0.85, 1.0 / 0.85):
        # keep mchirp roughly fixed while changing eta: m1*dq, m2/dq
        grid.append(("q*%.2f" % dq, args.m1 * dq, args.m2 / dq))

    gate_rows = []
    worst = 0.0
    for label, m1, m2 in grid:
        P_fd = build_P(m1, m2)
        pcfd = precompute(P_fd, data_dict, psd_dict, fmax, args.lmax,
                          inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=None)
        P_td = build_P(m1, m2)
        tf = TDInnerProductFactory(solver="auto")
        pctd = precompute(P_td, data_dict, psd_dict, fmax, args.lmax,
                          inv_spec_trunc_Q=False, T_spec=0.0, IP_factory=tf)
        rfi, cfd, cvfd, rfd, _, _ = pcfd
        rti, ctd, cvtd, rtd, _, _ = pctd
        Pe = build_P(m1, m2)
        lm_fd = lnL_marg_time(Pe, rfi, rfd, cfd, cvfd, args.lmax, tvals)
        Pe = build_P(m1, m2)
        lm_td = lnL_marg_time(Pe, rti, rtd, ctd, cvtd, args.lmax, tvals)
        d = abs(lm_fd - lm_td)
        worst = max(worst, d)
        gate_rows.append({"label": label, "m1": m1, "m2": m2,
                          "lnL_marg_fd": lm_fd, "lnL_marg_td": lm_td,
                          "abs_delta": d})
        print("[gate] %-10s m1=%.3f m2=%.3f  lnL_marg fd=%.5f td=%.5f  |Delta|=%.3e %s"
              % (label, m1, m2, lm_fd, lm_td, d, "OK" if d < 0.05 else "**OVER GATE**"))
    results["gate"] = {"gate": 0.05, "worst_abs_delta": worst,
                       "mc_true": mc_true, "rows": gate_rows,
                       "note": "deterministic time-marginalization: no MC error"}
    print("[gate] worst |Delta lnL_marg| = %.3e  (gate 0.05)  -> %s"
          % (worst, "PASS" if worst < 0.05 else "FAIL"))

    # ------------- INSTRUCTIVE EXTRA: inv-spec-trunc vs TD -------------
    # NOTE: RIFT's DEFAULT inv-spec-trunc-time is 8 s, but lsu.InnerProduct asserts
    # N_spec < len2side/2, i.e. T_spec < seglen/2 = 4 s for our 8 s segment -- the 8 s
    # default is PHYSICALLY INAPPLICABLE to an 8 s segment.  This is itself a finding:
    # the FD path needs the segment to be > 2*T_spec, while the TD kernel is exact and
    # carries no such knob.  We use the largest realistic value that fits (T_spec=2 s).
    T_spec_extra = 2.0
    print("\n=== INSTRUCTIVE EXTRA: fd inv-spec-trunc=%.0f s vs td-toeplitz ===" % T_spec_extra)
    print("    (default 8 s is inapplicable: lsu requires T_spec < seglen/2 = 4 s)")
    P_fd8 = build_P(args.m1, args.m2)
    pc_fd8 = precompute(P_fd8, data_dict, psd_dict, fmax, args.lmax,
                        inv_spec_trunc_Q=True, T_spec=T_spec_extra, IP_factory=None)
    rfi8, cfd8, cvfd8, rfd8, _, _ = pc_fd8
    Pe = build_P(args.m1, args.m2)
    lm_fd8 = lnL_marg_time(Pe, rfi8, rfd8, cfd8, cvfd8, args.lmax, tvals)
    Pe = build_P(args.m1, args.m2)
    lm_td0 = lnL_marg_time(Pe, ri_td, rho_td, ct_td, ctv_td, args.lmax, tvals)
    Pe = build_P(args.m1, args.m2)
    lm_fd0 = lnL_marg_time(Pe, ri_fd, rho_fd, ct_fd, ctv_fd, args.lmax, tvals)
    print("[extra] lnL_marg: fd(inv=0)=%.6f  fd(inv=%.0f)=%.6f  td=%.6f"
          % (lm_fd0, T_spec_extra, lm_fd8, lm_td0))
    print("[extra] |lnL_marg(fd inv=%.0f) - lnL_marg(td)| = %.3e"
          % (T_spec_extra, abs(lm_fd8 - lm_td0)))
    print("[extra] |lnL_marg(fd inv=%.0f) - lnL_marg(fd inv=0)| (inv-spec-trunc self-shift) = %.3e"
          % (T_spec_extra, abs(lm_fd8 - lm_fd0)))
    results["inv_spec_trunc"] = {
        "T_spec_used": T_spec_extra,
        "note_default_inapplicable": "RIFT default inv-spec-trunc-time=8s requires "
                                     "seglen>2*T_spec (=16s); inapplicable to 8s segment.",
        "lnL_marg_fd_inv0": lm_fd0, "lnL_marg_fd_invT": lm_fd8, "lnL_marg_td": lm_td0,
        "abs_fdT_vs_td": abs(lm_fd8 - lm_td0),
        "abs_fdT_vs_fd0_self_shift": abs(lm_fd8 - lm_fd0),
    }

    with open(args.out_json, "w") as fh:
        json.dump(results, fh, indent=2)
    print("\n[done] wrote %s and %s" % (args.out_json, args.out_lnLt))

    verdict = "GO" if (results["primary"]["network_max_abs_dev"] < 0.05
                       and results["gate"]["worst_abs_delta"] < 0.05) else "NO-GO"
    print("\n================ VERDICT: %s ================" % verdict)
    return 0


if __name__ == "__main__":
    sys.exit(main())
