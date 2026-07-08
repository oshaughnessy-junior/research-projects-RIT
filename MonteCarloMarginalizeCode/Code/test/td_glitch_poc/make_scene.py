#!/usr/bin/env python
"""make_scene.py -- the G4 PoC injection scene (step 3): native BBH + real blip overlap.

Builds the three-way analysis inputs from the PCA products of build_pca.py:

  * REFERENCE frames  (frames_ref/):    35+35 native BBH injection (native_inject.py, the
    T7b RIFT-self-consistent injection with integer-sample detector delays) + one fixed
    colored-Gaussian-noise realization (the standard smooth demo PSD), H1+L1, 8 s @ 2048 Hz.
  * GLITCHY frames    (frames_glitch/): the SAME strain+noise realization with a REAL
    HELD-OUT L1 blip (never seen by the PCA) added to L1, centred --blip-offset seconds
    after the L1 merger sample, amplitude scaled to --blip-snr (whitened SNR against the
    analysis PSD).  H1 identical to REFERENCE.
  * GLITCH BASIS      (glitch_basis_L1.npz): the r_sel principal blip shapes, colored to
    strain by the SAME PSD coloring applied to the injected blip, embedded per the
    --glitch-basis-file npz contract (basis / lambda0 / t0 / fs).

Domain bookkeeping (documented in the PoC README): the corpus, PCA, and held-out mismatch
live in the WHITENED domain (glitschen convention).  Injection into strain requires a
coloring; both the injected blip and every basis column are colored by the SAME operation
    strain(f) = white(f) * sqrt(S_demo(f)),   band-limited to [25, 900] Hz,
so basis-vs-glitch consistency is exact by construction and the whitened mismatch measured
in build_pca.py is preserved.  Because g = W a  =>  g_strain = (A W) a with the coloring A
linear, Lambda_0 carries over UNCHANGED (the coefficient prior is basis-frame); the single
overall column normalization is absorbed by sigma_g (Sec. 2b units split).

Also emits truth.json (all injection parameters + blip placement + measured SNRs).
"""
import argparse
import json
import os
import sys

import numpy as np

import lal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_ringdown_demo"))
import native_inject                                       # noqa: E402
import RIFT.lalsimutils as lalsimutils                     # noqa: E402


def color_white_snippet(snip_white, fs, f_lo=25.0, f_hi=900.0, n_buf=4096, n_out=1024):
    """Color a whitened-domain snippet to strain shape with the demo PSD (see docstring).

    The snippet is zero-embedded at the centre of an n_buf buffer, multiplied by
    sqrt(S(f)) on the rfft grid (band-limited), inverse-transformed, and the central
    n_out samples returned with a light Tukey taper (overall scale is arbitrary --
    absorbed by the SNR scaling / sigma_g)."""
    import scipy.signal as ss
    n = len(snip_white)
    buf = np.zeros(n_buf)
    i0 = n_buf // 2 - n // 2
    buf[i0:i0 + n] = snip_white
    f = np.fft.rfftfreq(n_buf, d=1.0 / fs)
    asd = np.sqrt(native_inject.analytic_psd(f))
    band = (f >= f_lo) & (f <= f_hi)
    g_f = np.fft.rfft(buf) * asd * band
    g = np.fft.irfft(g_f, n=n_buf)
    out = g[n_buf // 2 - n_out // 2:n_buf // 2 + n_out // 2]
    return out * ss.windows.tukey(n_out, 0.1)


def snr_of(strain_8s, fs, f_lo=25.0, f_hi=1000.0):
    """Whitened matched-filter SNR of a deterministic strain against the demo PSD."""
    n = len(strain_8s)
    df = fs / n
    f = np.fft.rfftfreq(n, d=1.0 / fs)
    gt = np.fft.rfft(strain_8s) / fs                 # continuous-FT convention
    S = native_inject.analytic_psd(f)
    band = (f >= f_lo) & (f <= f_hi)
    return float(np.sqrt(4.0 * df * np.sum(np.abs(gt[band]) ** 2 / S[band])))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pca", required=True, help="blip_pca_white.npz from build_pca.py")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--m1", type=float, default=35.0)
    ap.add_argument("--m2", type=float, default=35.0)
    ap.add_argument("--dist", type=float, default=450.0)
    ap.add_argument("--incl", type=float, default=0.7)
    ap.add_argument("--phiref", type=float, default=1.2)
    ap.add_argument("--theta", type=float, default=0.3, help="DEC [rad]")
    ap.add_argument("--phi", type=float, default=2.1, help="RA [rad]")
    ap.add_argument("--psi", type=float, default=0.8)
    ap.add_argument("--tref", type=float, default=1000000000.0)
    ap.add_argument("--start", type=float, default=999999996.0)
    ap.add_argument("--seglen", type=float, default=8.0)
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--noise-seed", type=int, default=4242)
    ap.add_argument("--blip-holdout", type=int, default=0,
                    help="index into the holdout set for the INJECTED blip")
    ap.add_argument("--blip-snr", type=float, default=15.0)
    ap.add_argument("--blip-offset", type=float, default=0.005,
                    help="blip centre relative to the L1 merger sample [s]")
    ap.add_argument("--r", type=int, default=None,
                    help="basis rank override (default: r_sel from the PCA)")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    fs, dt = args.srate, 1.0 / args.srate

    # ---------------- reference frames: native BBH + fixed noise realization ----------
    ref_dir = os.path.join(args.outdir, "frames_ref")
    ref_cache = os.path.join(args.outdir, "frames_ref.cache")
    native_inject.native_inject(
        args.m1, args.m2, args.dist, args.incl, args.phiref, args.theta, args.phi,
        args.psi, args.tref, ref_dir, ref_cache, instruments=("H1", "L1"),
        fmin=args.fmin, fref=args.fmin, srate=fs, seglen=args.seglen, start=args.start,
        noise_seed=args.noise_seed, fmax=1000.0, lmax=2)

    # ---------------- the real held-out blip, colored + scaled ------------------------
    pca = np.load(args.pca)
    n_win = int(pca["n_win"])
    snip_white = np.asarray(pca["holdout_snippets"])[args.blip_holdout]
    blip_1s = color_white_snippet(snip_white, fs)          # 1024 samples, peak at centre

    # L1 merger sample: geocenter tref + integer-sample L1 delay (native_inject convention)
    det = lalsimutils.lalsim.DetectorPrefixToLALDetector("L1")
    delay = lal.TimeDelayFromEarthCenter(det.location, args.phi, args.theta, args.tref)
    n_delay = int(round(delay / dt))
    t_merger_L1 = args.tref + n_delay * dt
    i_center = int(round((t_merger_L1 + args.blip_offset - args.start) / dt))

    n_seg = int(round(args.seglen * fs))
    blip_seg = np.zeros(n_seg)
    i0 = i_center - len(blip_1s) // 2
    blip_seg[i0:i0 + len(blip_1s)] = blip_1s
    rho_raw = snr_of(blip_seg, fs)
    scale = args.blip_snr / rho_raw
    blip_seg *= scale
    print("[scene] blip holdout idx %d (gps %.3f): raw SNR %.3g -> scaled to %.1f "
          "(scale %.3e), centre GPS %.6f (L1 merger %+.1f ms)"
          % (args.blip_holdout, float(pca["holdout_gps"][args.blip_holdout]), rho_raw,
             args.blip_snr, scale, args.start + i_center * dt, 1e3 * args.blip_offset))

    # ---------------- glitchy frames: same realization + blip in L1 -------------------
    gl_dir = os.path.join(args.outdir, "frames_glitch")
    os.makedirs(gl_dir, exist_ok=True)
    gl_lines = []
    for line in open(ref_cache):
        obs, tag, tstart, dur, url = line.split()
        path = url.replace("file://localhost", "")
        ifo = obs + "1"
        hoft = lalsimutils.frame_data_to_hoft(ref_cache, ifo + ":FAKE-STRAIN",
                                              verbose=False)
        if ifo == "L1":
            hoft.data.data[:] = np.asarray(hoft.data.data) + blip_seg
        fname = os.path.basename(path).replace("native_strain", "glitch_strain")
        fpath = os.path.abspath(os.path.join(gl_dir, fname))
        lalsimutils.hoft_to_frame_data(fpath, ifo + ":FAKE-STRAIN", hoft)
        gl_lines.append("%s FAKE %s %s file://localhost%s\n" % (obs, tstart, dur, fpath))
    gl_cache = os.path.join(args.outdir, "frames_glitch.cache")
    with open(gl_cache, "w") as fh:
        fh.writelines(gl_lines)
    print("[scene] wrote %s" % gl_cache)

    # ---------------- basis npz (--glitch-basis-file contract) ------------------------
    r = int(args.r if args.r is not None else pca["r_sel"])
    comps = np.asarray(pca["components"])[:r]              # (r, n_win) whitened shapes
    cols = np.stack([color_white_snippet(cw, fs) for cw in comps], axis=1)  # (1024, r)
    cols /= np.max(np.linalg.norm(cols, axis=0))           # one overall scale -> sigma_g
    lambda0 = np.asarray(pca["lambda0"])[:r]
    lambda0 = lambda0 / np.max(lambda0)
    t0_rel_event = (args.start + (i0 * dt)) - args.tref    # snippet sample 0, geocenter-rel-event
    basis_path = os.path.join(args.outdir, "glitch_basis_L1.npz")
    np.savez(basis_path, basis=cols, lambda0=lambda0, t0=t0_rel_event, fs=fs)
    print("[scene] wrote %s (r=%d, t0=%+.6f s rel event)" % (basis_path, r, t0_rel_event))

    # ---------------- truth ------------------------------------------------------------
    mc = lalsimutils.mchirp(args.m1 * lalsimutils.lsu_MSUN,
                            args.m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN
    eta = lalsimutils.symRatio(args.m1 * lalsimutils.lsu_MSUN,
                               args.m2 * lalsimutils.lsu_MSUN)
    truth = dict(m1=args.m1, m2=args.m2, mc=mc, eta=eta, dist=args.dist, incl=args.incl,
                 phiref=args.phiref, ra=args.phi, dec=args.theta, psi=args.psi,
                 tref=args.tref, start=args.start, seglen=args.seglen, srate=fs,
                 fmin=args.fmin, noise_seed=args.noise_seed,
                 blip_holdout_idx=args.blip_holdout,
                 blip_gps_o3=float(pca["holdout_gps"][args.blip_holdout]),
                 blip_snr=args.blip_snr, blip_offset_s=args.blip_offset,
                 blip_center_gps=args.start + i_center * dt,
                 t_merger_L1=t_merger_L1, basis_r=r,
                 basis_t0_rel_event=t0_rel_event, n_win_white=n_win)
    with open(os.path.join(args.outdir, "truth.json"), "w") as fh:
        json.dump(truth, fh, indent=2)
    # keep the scaled injected blip for the reconstruction figure
    np.save(os.path.join(args.outdir, "blip_injected_L1.npy"), blip_seg)
    print("[scene] truth:", json.dumps(truth))


if __name__ == "__main__":
    main()
