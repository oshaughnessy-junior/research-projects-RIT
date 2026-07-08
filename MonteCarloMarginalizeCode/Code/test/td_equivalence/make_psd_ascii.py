#!/usr/bin/env python
"""Regenerate the smooth analytic two-column PSD ascii used by the TD equivalence
(T4) and ringdown (T7) demos.

    S(f) = 1e-46 * (1 + (20/f)^4 + (f/250)^2),   f floored at 1 Hz

Grid: f = 0 .. 1024 Hz, deltaF = 0.125 (8193 rows), matching the ILE data path.
Emits ``psd_smooth_ascii.txt`` (freq, S) which ``convert_psd_ascii2xml`` then turns
into the ``{H1,L1}-psd.xml.gz`` the study loads.  Regenerable on purpose: the raw
ascii is bulky and gitignored, this script is the source of truth.

Usage:
    python make_psd_ascii.py [--fmax 1024] [--df 0.125] [--out psd_smooth_ascii.txt]
"""
import argparse
import numpy as np

def smooth_psd(f):
    fc = np.maximum(f, 1.0)
    return 1e-46 * (1.0 + (20.0 / fc) ** 4 + (fc / 250.0) ** 2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fmax", type=float, default=1024.0)
    ap.add_argument("--df", type=float, default=0.125)
    ap.add_argument("--out", default="psd_smooth_ascii.txt")
    args = ap.parse_args()
    n = int(round(args.fmax / args.df)) + 1
    f = np.arange(n) * args.df
    s = smooth_psd(f)
    with open(args.out, "w") as fp:
        for fi, si in zip(f, s):
            fp.write("%.18e %.18e\n" % (fi, si))
    print("wrote %s (%d rows, df=%g, fmax=%g)" % (args.out, n, args.df, args.fmax))

if __name__ == "__main__":
    main()
