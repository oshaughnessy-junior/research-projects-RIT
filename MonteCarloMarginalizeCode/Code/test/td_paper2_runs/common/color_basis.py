#!/usr/bin/env python
r"""color_basis.py -- color a whitened PCA glitch basis to STRAIN for a real event.

The corpus + PCA (fetch_class.py -> build_pca.py) live in the WHITENED domain (glitschen
convention). The ILE --glitch-basis-file contract wants STRAIN-domain basis columns placed at
the glitch's time in the analysis segment. This is exactly the coloring make_scene.py does for
the synthetic PoC, generalized to (a) the REAL event PSD (interpolated onto the rfft grid,
instead of the analytic demo PSD) and (b) an arbitrary glitch centre GPS.

Because g = W a (whitened) and the coloring A is LINEAR, g_strain = (A W) a carries the SAME
coefficient prior Lambda0 / mu / Sigma_shape unchanged (the prior is basis-frame; the one
overall column normalization is absorbed by sigma_g -- Sec. 2b units split). So the population
moments fit in the whitened domain (fit_population.py) attach to the strain basis verbatim; run
fit_population.py --write-basis on this output to add mu/sigma_shape (FULL / COVARIANCE-ONLY).

npz contract written (paper-1 flat): basis (n_out x r), lambda0 (r,), t0, fs.
  t0 = snippet sample-0 GPS relative to the event geocenter time; with the basis centred on
  --glitch-center-gps, t0 = (glitch_center - event_gps) - (n_out//2)/fs.

Usage:
  color_basis.py --pca work/scattered_light_pca_white.npz --psd psd/L1-psd-ascii.txt \
      --event-gps 1420878141.2 --glitch-center-gps 1420878141.16 --ifo L1 \
      --n-out 8192 --f-lo 10 --f-hi 60 --out work/glitch_basis_L1.npz
"""
import argparse
import os

import numpy as np


def color_white_snippet(snip_white, fs, S_of_f, f_lo, f_hi, n_buf, n_out):
    """Color one whitened snippet to strain: embed, x sqrt(S(f)) band-limited, irfft, crop."""
    import scipy.signal as ss
    n = len(snip_white)
    buf = np.zeros(n_buf)
    i0 = n_buf // 2 - n // 2
    buf[i0:i0 + n] = snip_white
    f = np.fft.rfftfreq(n_buf, d=1.0 / fs)
    asd = np.sqrt(S_of_f(f))
    band = (f >= f_lo) & (f <= f_hi)
    g_f = np.fft.rfft(buf) * asd * band
    g = np.fft.irfft(g_f, n=n_buf)
    out = g[n_buf // 2 - n_out // 2:n_buf // 2 + n_out // 2]
    return out * ss.windows.tukey(n_out, 0.1)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pca", required=True, help="<class>_pca_white.npz from build_pca.py")
    ap.add_argument("--psd", required=True, help="event PSD ascii (f, S) from fetch_event_data.py")
    ap.add_argument("--event-gps", type=float, required=True, help="event geocenter merger GPS")
    ap.add_argument("--glitch-center-gps", type=float, required=True,
                    help="GPS at which the basis support is centred (glitch time in this IFO)")
    ap.add_argument("--ifo", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--r", type=int, default=None, help="rank (default: r_sel from the PCA)")
    ap.add_argument("--n-out", type=int, default=1024,
                    help="strain snippet length [samples] (1024=0.5s blip; ~8192=4s scattering)")
    ap.add_argument("--n-buf", type=int, default=None,
                    help="FFT buffer for coloring (default 4*n_out)")
    ap.add_argument("--f-lo", type=float, default=25.0)
    ap.add_argument("--f-hi", type=float, default=1000.0)
    args = ap.parse_args()

    pca = np.load(args.pca)
    fs = float(pca["fs"])
    r = int(args.r if args.r is not None else pca["r_sel"])
    comps = np.asarray(pca["components"])[:r]          # (r, n_win) whitened shapes
    lambda0 = np.asarray(pca["lambda0"])[:r]
    lambda0 = lambda0 / np.max(lambda0)
    n_buf = args.n_buf if args.n_buf else 4 * args.n_out

    fS = np.loadtxt(args.psd)
    f_psd, S_psd = fS[:, 0], fS[:, 1]
    # log-interp the PSD onto any rfft grid; clamp outside the measured band to the edge values
    logS = np.log(S_psd)

    def S_of_f(f):
        out = np.exp(np.interp(f, f_psd, logS, left=logS[0], right=logS[-1]))
        return np.where(np.isfinite(out) & (out > 0), out, np.max(S_psd))

    cols = np.stack([color_white_snippet(cw, fs, S_of_f, args.f_lo, args.f_hi, n_buf, args.n_out)
                     for cw in comps], axis=1)       # (n_out, r)
    cols /= np.max(np.linalg.norm(cols, axis=0))      # one overall scale -> sigma_g
    dt = 1.0 / fs
    t0 = (args.glitch_center_gps - args.event_gps) - (args.n_out // 2) * dt

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    np.savez(args.out, basis=cols, lambda0=lambda0, t0=t0, fs=fs)
    print("[color] %s: wrote %s  r=%d  n_out=%d (%.3f s)  t0=%+.6f s rel event  band=[%g,%g] Hz"
          % (args.ifo, args.out, r, args.n_out, args.n_out * dt, t0, args.f_lo, args.f_hi))


if __name__ == "__main__":
    main()
