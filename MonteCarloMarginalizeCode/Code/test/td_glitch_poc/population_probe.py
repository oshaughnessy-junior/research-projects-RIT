#!/usr/bin/env python
r"""population_probe.py -- flat vs population-prior comparison on the G4 PoC scene
(paper 2, Sec. 3.4 "What the population prior buys": identifiability + suppressed signal
absorption), plus the glitch-products comparison for the PoC-rerun table.

Runs three coefficient priors on the SAME PoC L1 scene at the context level (no full ILE):
  * FLAT       : diagonal Lambda0 (paper-1 default)                    glitch_basis_L1.npz
  * COV-ONLY   : anisotropic Sigma_shape, mu=0 (identifiability shape) *_pop_covarianceonly.npz
  * FULL       : Sigma_shape + population mean mu_hat                   *_pop_full.npz

Reports, on the REAL frames the PoC produced:

(A) GLITCH PRODUCTS on the GLITCHY L1 data (signal+blip+noise): profiled sigma_g, Delta lnZ,
    and the reconstruction whitened overlap of g_hat against the injected real blip -- the
    columns of the PoC-rerun table (flat vs population).

(B) IDENTIFIABILITY / SIGNAL ABSORPTION (Sec. 3.4 central claim).  The physical chirp template
    is slid across the +-0.15 s arrival window and the fraction of TEMPLATE power the glitch
    subspace absorbs, beta(t)^H M^-1 beta(t) / U0, is measured (diag_uprime metric).  Because
    U' is purely covariance-side (the mean does NOT enter), FLAT vs COV-ONLY isolates the SHAPE
    effect: a chirp projects OFF the population ridge, so the anisotropic prior must absorb LESS.
    Measured at MATCHED sigma_g (the flat-profiled value) so the difference is purely the prior
    shape, and also at each prior's own profiled sigma_g.

(C) NO-GLITCH CONTROL on the REFERENCE L1 data (signal+noise, NO blip): the profiled Delta lnZ
    and absorbed power the glitch model claims when there is no glitch -- lower is safer.

Usage: population_probe.py --workdir <scene work dir> --popdir <dir with the pop bases> \
                           --outdir <out>
"""
import argparse
import json
import os
import sys

import numpy as np

import lal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_ringdown_demo"))
import native_inject                                         # noqa: E402
import RIFT.lalsimutils as lsu                               # noqa: E402
from RIFT.likelihood.td_inner_product import TDInnerProductFactory  # noqa: E402


def _data_fd(cache, ifo="L1:FAKE-STRAIN"):
    d_td = lsu.frame_data_to_hoft(cache, ifo, verbose=False)
    ts = lal.CreateCOMPLEX16TimeSeries("x", d_td.epoch, 0., d_td.deltaT,
                                       lsu.lsu_DimensionlessUnit, d_td.data.length)
    ts.data.data = np.asarray(d_td.data.data, dtype=complex)
    return lsu.DataFourier(ts), d_td


def _psd(fs):
    n1 = int(fs / 2 / 0.125) + 1
    psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0.), 0., 0.125,
                                         lsu.lsu_HertzUnit, n1)
    psd.data.data = native_inject.analytic_psd(np.arange(n1) * 0.125)
    return psd


def _context(basis, psd, data_fd, truth, sigma):
    fac = TDInnerProductFactory(solver="auto", glitch_basis_files={"L1": basis},
                                glitch_sigma=sigma)
    fac.register_detector_data("L1", psd, data_fd, event_time_geo=truth["tref"])
    ctx = fac.context_for(truth["fmin"], 1000.0, truth["srate"] / 2.0, data_fd.deltaF, psd,
                          analyticPSD_Q=False)
    return fac, ctx


def _template(truth, fs):
    P = native_inject.build_injection_P(
        truth["m1"], truth["m2"], truth["dist"], truth["incl"], truth["phiref"],
        truth["dec"], truth["ra"], truth["psi"], truth["tref"], truth["fmin"],
        truth["fmin"], "IMRPhenomD", fs, truth["seglen"], "L1")
    h = native_inject.native_detector_strain(P, 2)
    h_seg = native_inject._place_in_window(h, truth["start"], fs, truth["seglen"])
    return np.asarray(h_seg.data.data)


def _whitened_overlap(ctx, a, b):
    """Real whitened overlap <a|b>/sqrt(<a|a><b|b>) using the precision operator P=ip_norm*K."""
    aa = float(np.real(ctx.dot_td(a, a)))
    bb = float(np.real(ctx.dot_td(b, b)))
    ab = float(np.real(ctx.dot_td(a, b)))
    return ab / np.sqrt(aa * bb) if aa > 0 and bb > 0 else float("nan")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--popdir", required=True,
                    help="dir holding glitch_basis_L1_pop_full.npz / _pop_covarianceonly.npz")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--shifts", default="-0.15,-0.075,0.0,0.075,0.15")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    W = args.workdir
    truth = json.load(open(os.path.join(W, "truth.json")))
    fs = truth["srate"]; dt = 1.0 / fs
    psd = _psd(fs)
    shifts = [float(x) for x in args.shifts.split(",")]

    bases = {
        "flat": os.path.join(W, "glitch_basis_L1.npz"),
        "cov-only": os.path.join(args.popdir, "glitch_basis_L1_pop_covarianceonly.npz"),
        "full": os.path.join(args.popdir, "glitch_basis_L1_pop_full.npz"),
    }
    bases = {k: v for k, v in bases.items() if os.path.exists(v)}

    glitchy_fd, _ = _data_fd(os.path.join(W, "frames_glitch.cache"))
    ref_fd, _ = _data_fd(os.path.join(W, "frames_ref.cache"))
    blip = np.load(os.path.join(W, "blip_injected_L1.npy"))
    h0 = _template(truth, fs)

    # ---------- (A) glitch products on GLITCHY data (profile each prior) ----------
    resA, sigma_flat = {}, None
    for name, path in bases.items():
        fac, ctx = _context(path, psd, glitchy_fd, truth, "profile")
        sig = ctx._glitch_sigma_g
        ghat = np.real(ctx.glitch_reconstruction_td())
        ovl = _whitened_overlap(ctx, ghat, blip)
        resA[name] = dict(sigma_g=float(sig), delta_lnZ=float(ctx.glitch_delta_lnZ),
                          recon_overlap=float(ovl))
        if name == "flat":
            sigma_flat = float(sig)
        print("[A/%s] glitchy: sigma_g=%.4e  Delta_lnZ=%.4f  recon-overlap=%.4f"
              % (name, sig, ctx.glitch_delta_lnZ, ovl))

    # ---------- (B) template-power absorption, MATCHED sigma_g (flat-profiled) ----------
    resB = {}
    for name, path in bases.items():
        fac, ctx = _context(path, psd, glitchy_fd, truth, sigma_flat)  # pinned sigma_g
        U0 = float(np.real(ctx.ip_normalization * ctx.dot_td(h0, h0)))
        fr = {}
        for s in shifts:
            hs = np.roll(h0, int(round(s / dt)))
            corr = float(np.real(ctx.glitch_ip_correction(hs, hs)))
            fr[s] = corr / U0
        resB[name] = dict(U0=U0, sigma_g=sigma_flat, absorbed_fraction=fr,
                          at_arrival=fr.get(0.0), max_fraction=max(fr.values()))
        print("[B/%s] matched sigma_g=%.3e: template-power absorbed at arrival=%.4e  "
              "max-over-window=%.4e" % (name, sigma_flat, fr.get(0.0), max(fr.values())))

    # ---------- (C) no-glitch control on REFERENCE data (profile each) ----------
    # This is the task's signal-only identifiability probe: inject signal (+noise), NO glitch,
    # and see how much the glitch model turns on.  The honest number is at EACH prior's OWN
    # profiled amplitude (the prior is allowed to resist), not a matched one.
    resC = {}
    for name, path in bases.items():
        fac, ctx = _context(path, psd, ref_fd, truth, "profile")
        sig = ctx._glitch_sigma_g
        U0 = float(np.real(ctx.ip_normalization * ctx.dot_td(h0, h0)))
        absorb_prof = float(np.real(ctx.glitch_ip_correction(h0, h0))) / U0
        # absorbed template-power at the reconstruction MAP amplitude the prior actually chose
        resC[name] = dict(profiled_sigma_g=float(sig),
                          profiled_delta_lnZ=float(ctx.glitch_delta_lnZ),
                          absorbed_at_arrival_profiled=absorb_prof)
        print("[C/%s] no-glitch ref: profiled sigma_g=%.4e Delta_lnZ=%.4f ; "
              "template-absorbed(@profiled sigma_g)=%.4e" % (name, sig, ctx.glitch_delta_lnZ, absorb_prof))

    out = dict(scene=os.path.abspath(W), truth=truth, bases=list(bases),
               A_glitch_products=resA, B_absorption_matched_sigma=resB,
               C_noglitch_control=resC, sigma_flat=sigma_flat)
    with open(os.path.join(args.outdir, "population_probe.json"), "w") as fh:
        json.dump(out, fh, indent=2)
    print("[probe] wrote %s" % os.path.join(args.outdir, "population_probe.json"))

    # concise comparison table
    print("\n=== PoC-rerun comparison (glitchy L1) ===")
    print("%-10s %12s %12s %14s" % ("prior", "sigma_g", "Delta_lnZ", "recon-overlap"))
    for name in bases:
        a = resA[name]
        print("%-10s %12.4e %12.4f %14.4f"
              % (name, a["sigma_g"], a["delta_lnZ"], a["recon_overlap"]))
    print("\n=== Signal absorption @ matched sigma_g (template power at arrival) ===")
    print("%-10s %18s %18s" % ("prior", "absorbed@arrival", "max-over-window"))
    for name in bases:
        b = resB[name]
        print("%-10s %18.4e %18.4e" % (name, b["at_arrival"], b["max_fraction"]))
    if "flat" in resB:
        base = resB["flat"]["at_arrival"]
        for name in bases:
            if name != "flat":
                r = resB[name]["at_arrival"] / base if base else float("nan")
                print("  %s / flat absorbed-at-arrival ratio = %.3f (%.1f%% %s)"
                      % (name, r, 100 * (1 - r), "less" if r < 1 else "more"))


if __name__ == "__main__":
    main()
