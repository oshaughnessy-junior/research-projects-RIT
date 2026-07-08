#!/usr/bin/env python
"""make_gaussian_noise_frames.py -- add PSD-colored Gaussian noise to the zero-noise
32 s frames, producing a Gaussian-noise variant frame set (same channel names) so the
equivalence study can be re-run on noisy data.  The FD-vs-TD comparison holds on ANY
data (both inner products see the identical noisy series), so this checks that the
equivalence is not an artifact of the exactly-band-limited zero-noise waveform.

Noise is colored by the SAME smooth analytic PSD used for the likelihood, generated in
the frequency domain (n~(f) = sqrt(S(f)/(4 df)) * (g1 + i g2)) and inverse-FFT'd, then
added to the zero-noise strain.  Reproducible via --seed.
"""
import os
import argparse
import numpy as np
import lal
import RIFT.lalsimutils as lalsimutils

HERE = os.path.dirname(os.path.abspath(__file__))


def analytic_psd(f):
    fsafe = np.maximum(np.abs(f), 1.0)
    return 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-cache", default=os.path.join(HERE, "frames32.cache"))
    ap.add_argument("--out-dir", default=os.path.join(HERE, "frames32_noise"))
    ap.add_argument("--seed", type=int, default=2024)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    out_cache_lines = []
    for inst, chan in channels.items():
        hoft = lalsimutils.frame_data_to_hoft(args.in_cache, inst + ":" + chan,
                                              verbose=False)
        N = hoft.data.length
        dt = hoft.deltaT
        fs = 1.0 / dt
        df = fs / N
        # colored Gaussian noise in FD (one-sided rfft grid)
        f = np.fft.rfftfreq(N, d=dt)
        S = analytic_psd(f)
        band = (f >= args.fmin) & (f <= args.fmax)
        sigma = np.zeros_like(f)
        sigma[band] = np.sqrt(S[band] / (4.0 * df))  # amplitude spectral density scaling
        ntilde = (rng.standard_normal(len(f)) + 1j * rng.standard_normal(len(f))) * sigma
        ntilde[0] = ntilde[0].real
        if N % 2 == 0:
            ntilde[-1] = ntilde[-1].real
        noise = np.fft.irfft(ntilde, n=N) * (N * df)  # inverse with matching normalization
        # add noise
        hoft.data.data[:] = np.asarray(hoft.data.data) + noise
        # write frame
        tstart = int(hoft.epoch)
        duration = int(round(N * dt))
        fname = inst.replace("1", "") + "-fake_strain_noise-" + str(tstart) + "-" + str(duration) + ".gwf"
        fpath = os.path.join(args.out_dir, fname)
        lalsimutils.hoft_to_frame_data(fpath, inst + ":" + chan, hoft)
        obs = inst[0]
        out_cache_lines.append("%s FAKE %d %d file://localhost%s\n" %
                               (obs, tstart, duration, fpath))
        rms_sig = np.sqrt(np.mean(np.asarray(noise) ** 2))
        print("[noise] %s wrote %s  N=%d noise_rms=%.3e" % (inst, fpath, N, rms_sig))

    cache_path = os.path.join(HERE, "frames32_noise.cache")
    with open(cache_path, "w") as fh:
        fh.writelines(out_cache_lines)
    print("[noise] wrote cache", cache_path)


if __name__ == "__main__":
    main()
