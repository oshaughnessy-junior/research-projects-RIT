#!/usr/bin/env python
"""Regenerate the zero-noise frames + caches for the T4 equivalence study.

Drives ``bin/util_LALWriteFrame.py`` (the tool that produced the original committed
frames) for H1 and L1 at both segment lengths, then writes the ``file://localhost``
caches the study loads.  Bulky and gitignored on purpose; this script is the source
of truth.

  * 8 s  frames  -> ./                + frames.cache        (deterministic study)
  * 32 s frames  -> ./frames32/       + frames32.cache      (real ILE run + noise base)

Usage:
    python make_frames.py            # both 8 s and 32 s
    python make_frames.py --seglen 8
"""
import argparse
import glob
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.abspath(os.path.join(HERE, "..", "..", "bin", "util_LALWriteFrame.py"))


def build(seglen, outdir, cache_path, inj, approx, srate):
    os.makedirs(outdir, exist_ok=True)
    # remove stale frames so the glob below is unambiguous
    for old in glob.glob(os.path.join(outdir, "*-fake_strain-*-%d.gwf" % seglen)):
        os.remove(old)
    lines = []
    for ifo in ("H1", "L1"):
        cmd = [sys.executable, BIN, "--inj", inj, "--event", "0",
               "--instrument", ifo, "--srate", str(srate), "--seglen", str(seglen),
               "--approx", approx]
        print("  [%s %ss] %s" % (ifo, seglen, " ".join(cmd)))
        subprocess.check_call(cmd, cwd=outdir)
    for gwf in sorted(glob.glob(os.path.join(outdir, "*-fake_strain-*-%d.gwf" % seglen))):
        base = os.path.basename(gwf)
        obs = base[0]                       # 'H' or 'L'
        parts = base.rsplit("-", 2)         # [..., tstart, 'dur.gwf']
        tstart = int(parts[1])
        dur = int(parts[2].split(".")[0])
        lines.append("%s FAKE %d %d file://localhost%s\n" %
                     (obs, tstart, dur, os.path.abspath(gwf)))
    with open(cache_path, "w") as fh:
        fh.writelines(lines)
    print("  wrote %s (%d frames)" % (cache_path, len(lines)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inj", default=os.path.join(HERE, "inj_bbh35.xml.gz"))
    ap.add_argument("--approx", default="IMRPhenomD")
    ap.add_argument("--srate", type=int, default=2048)
    ap.add_argument("--seglen", type=int, default=None,
                    help="only this seglen (8 or 32); default builds both")
    args = ap.parse_args()
    jobs = [(8, HERE, os.path.join(HERE, "frames.cache")),
            (32, os.path.join(HERE, "frames32"), os.path.join(HERE, "frames32.cache"))]
    if args.seglen is not None:
        jobs = [j for j in jobs if j[0] == args.seglen]
    for seglen, outdir, cache in jobs:
        build(seglen, outdir, cache, args.inj, args.approx, args.srate)


if __name__ == "__main__":
    main()
