#!/usr/bin/env python
"""make_noise_frames.py -- add PSD-colored Gaussian noise to an existing zero-noise frame
cache, writing a noise-variant frame set + cache.  Colored by the SAME smooth analytic PSD
used for the likelihood (n~(f)=sqrt(S/(4 df))*(g1+i g2), band-limited [fmin,fmax], irfft).
Reproducible per --seed.  Adapted from test/td_equivalence/make_gaussian_noise_frames.py.

Usage: make_noise_frames.py --in-cache frames.cache --out-dir frames_noise \
          --out-cache frames_noise.cache --seed 2024
"""
import os
import argparse
import numpy as np
import RIFT.lalsimutils as lalsimutils

HERE = os.path.dirname(os.path.abspath(__file__))


def analytic_psd(f):
    fsafe = np.maximum(np.abs(f), 1.0)
    return 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-cache", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--out-cache", required=True)
    ap.add_argument("--seed", type=int, default=2024)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    out_lines = []
    for inst, chan in channels.items():
        hoft = lalsimutils.frame_data_to_hoft(args.in_cache, inst + ":" + chan, verbose=False)
        N = hoft.data.length
        dt = hoft.deltaT
        fs = 1.0 / dt
        df = fs / N
        f = np.fft.rfftfreq(N, d=dt)
        S = analytic_psd(f)
        band = (f >= args.fmin) & (f <= args.fmax)
        sigma = np.zeros_like(f)
        sigma[band] = np.sqrt(S[band] / (4.0 * df))
        ntilde = (rng.standard_normal(len(f)) + 1j * rng.standard_normal(len(f))) * sigma
        ntilde[0] = ntilde[0].real
        if N % 2 == 0:
            ntilde[-1] = ntilde[-1].real
        noise = np.fft.irfft(ntilde, n=N) * (N * df)
        hoft.data.data[:] = np.asarray(hoft.data.data) + noise
        tstart = int(hoft.epoch)
        duration = int(round(N * dt))
        fname = inst[0] + "-fake_strain_noise-" + str(tstart) + "-" + str(duration) + ".gwf"
        fpath = os.path.join(args.out_dir, fname)
        lalsimutils.hoft_to_frame_data(fpath, inst + ":" + chan, hoft)
        out_lines.append("%s FAKE %d %d file://localhost%s\n" % (inst[0], tstart, duration, os.path.abspath(fpath)))
        print("[noise] %s wrote %s N=%d seed=%d rms=%.3e" %
              (inst, fpath, N, args.seed, np.sqrt(np.mean(np.asarray(noise) ** 2))))
    with open(args.out_cache, "w") as fh:
        fh.writelines(out_lines)
    print("[noise] wrote cache", args.out_cache)


if __name__ == "__main__":
    main()
