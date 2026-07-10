#!/usr/bin/env python
r"""fetch_event_data.py -- fetch open strain + off-source PSDs + analysis frames for one event.

Generalizes analyses/t7c_gw250114/fetch_data.py (the validated GW250114 recipe) to an
arbitrary event via CLI, so every paper-2 workspace uses the SAME data/PSD convention:

  * ONE fetch per IFO over [gps - fetch-pre, gps + fetch-post];
  * off-source median-averaged Welch PSD over [gps - fetch-pre, gps - psd-gap] with
    fftlength = seglen (so df matches the ILE FFT, e.g. 8 s -> df = 0.125 Hz);
  * an analysis frame of length seglen centred (or placed) on the event, written as
    <IFO>:GWOSC-STRAIN at --srate;
  * an absolute file:// frame cache and a provenance json.

The off-source median-Welch PSD + production Tukey window are the conventions the T7c report
pinned (a rectangular window leaks the sub-20 Hz seismic wall and inflates <d|d> 1e5-1e6x; the
pipeline applies --window-shape = data_tukey_window_time*2/T_window downstream at data load).

Products under <outdir>:
  data/{IFO}-{tag}-{start}-{dur}.gwf   analysis frames (channel <IFO>:GWOSC-STRAIN)
  data/frames.cache                    absolute file:// cache
  psd/{IFO}-psd.xml.gz                  written by convert_psd_ascii2xml (see fetch_data.sh)
  psd/{IFO}-psd-ascii.txt               off-source median-Welch ascii (f, S)
  data/fetch_meta.json                 provenance

Usage:
  fetch_event_data.py --tag GW250114 --gps 1420878141.2 --ifos H1,L1 \
      --srate 2048 --seglen 16 --outdir . [--fetch-pre 300 --fetch-post 8 --psd-gap 40]
"""
import argparse
import json
import os

import numpy as np


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", required=True, help="short event tag for filenames, e.g. GW250114")
    ap.add_argument("--gps", type=float, required=True, help="geocenter merger GPS")
    ap.add_argument("--ifos", default="H1,L1", help="comma list, e.g. H1,L1")
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--seglen", type=float, default=16.0,
                    help="analysis-FRAME length [s]; PSD fftlength = min(seglen, 8) for df")
    ap.add_argument("--psd-fftlen", type=float, default=None,
                    help="PSD Welch fftlength [s] (default = the ILE analysis seglen, e.g. 8)")
    ap.add_argument("--ile-seglen", type=float, default=8.0,
                    help="the ILE analysis segment length; sets PSD df (default 8 -> 0.125 Hz)")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--fetch-pre", type=float, default=300.0)
    ap.add_argument("--fetch-post", type=float, default=8.0)
    ap.add_argument("--psd-gap", type=float, default=40.0,
                    help="off-source PSD segment ends this many s before the event")
    args = ap.parse_args()

    from gwpy.timeseries import TimeSeries

    data = os.path.join(args.outdir, "data")
    psd = os.path.join(args.outdir, "psd")
    os.makedirs(data, exist_ok=True); os.makedirs(psd, exist_ok=True)
    ifos = [x.strip() for x in args.ifos.split(",") if x.strip()]
    gps = args.gps
    fftlen = args.psd_fftlen if args.psd_fftlen else args.ile_seglen

    fetch_start = int(np.floor(gps)) - int(args.fetch_pre)
    fetch_end = int(np.ceil(gps)) + int(args.fetch_post)
    psd_start = fetch_start
    psd_end = int(np.floor(gps)) - int(args.psd_gap)
    ana_start = int(np.floor(gps)) - int(args.seglen // 2)
    ana_end = ana_start + int(args.seglen)

    meta = {"event": args.tag, "gps": gps, "srate": args.srate, "seglen": args.seglen,
            "ile_seglen": args.ile_seglen, "psd_fftlength": fftlen,
            "fetch": [fetch_start, fetch_end], "psd_seg": [psd_start, psd_end],
            "ana_frame": [ana_start, ana_end], "ifos": ifos,
            "frames": {}, "psd_ascii": {}}

    for ifo in ifos:
        print("[%s] fetching open data [%d,%d]..." % (ifo, fetch_start, fetch_end), flush=True)
        ts = TimeSeries.fetch_open_data(ifo, fetch_start, fetch_end, cache=True)
        print("   native srate=%.0f n=%d" % (ts.sample_rate.value, len(ts)), flush=True)
        if ts.sample_rate.value != args.srate:
            ts = ts.resample(args.srate)
        # ---- off-source PSD (median-averaged Welch, df = 1/fftlen) ----
        off = ts.crop(psd_start, psd_end)
        ov = np.asarray(off.value, dtype=float)
        finite = np.isfinite(ov)
        if not finite.all():
            # Real open data can carry NaN gaps (e.g. GW191109 H1 has a ~12-14% off-source
            # gap).  gwpy's median PSD is NOT NaN-aware, so a single gapped Welch window
            # poisons EVERY frequency bin -> all-NaN S -> the old np.max(S[finite]) hit a
            # zero-size reduction.  Use the LONGEST contiguous finite stretch for the PSD (no
            # fabricated samples); require >= 4 PSD windows.
            fint = finite.astype(np.int8)
            d = np.diff(np.concatenate([[0], fint, [0]]))
            starts = np.where(d == 1)[0]
            ends = np.where(d == -1)[0]
            k = int(np.argmax(ends - starts))
            i0, i1 = int(starts[k]), int(ends[k])
            need = int(4 * fftlen * args.srate)
            print("   [psd] %s off-source has %.1f%% NaN; longest finite run = %.1f s"
                  % (ifo, 100.0 * (1.0 - finite.mean()), (i1 - i0) / args.srate), flush=True)
            if (i1 - i0) < need:
                raise SystemExit("[psd] %s longest finite off-source run too short (%d < %d "
                                 "samples) -- widen --fetch-pre or move --psd-gap"
                                 % (ifo, i1 - i0, need))
            off = off[i0:i1]
        p = off.psd(fftlength=fftlen, overlap=fftlen / 2.0, method="median")
        f = p.frequencies.value
        S = p.value
        finS = np.isfinite(S) & (S > 0)
        if not finS.any():
            raise SystemExit("[psd] %s PSD all non-finite after gap handling" % ifo)
        S = np.where(finS, S, np.max(S[finS]))
        asc = os.path.join(psd, "%s-psd-ascii.txt" % ifo)
        np.savetxt(asc, np.column_stack([f, S]))
        meta["psd_ascii"][ifo] = asc
        print("   PSD df=%.5f nfreq=%d fmax=%.1f S@100Hz=%.3e"
              % (f[1] - f[0], len(f), f[-1], S[np.argmin(np.abs(f - 100))]), flush=True)
        # ---- analysis frame ----
        seg = ts.crop(ana_start, ana_end)
        if not np.all(np.isfinite(np.asarray(seg.value))):
            raise SystemExit("[frame] %s analysis frame [%d,%d] contains NaN -- the analysis "
                             "segment overlaps a data gap; shift the event window or pick a "
                             "different frame placement" % (ifo, ana_start, ana_end))
        seg.name = "%s:GWOSC-STRAIN" % ifo
        seg.channel = "%s:GWOSC-STRAIN" % ifo
        fn = os.path.join(data, "%s-%s-%d-%d.gwf" % (ifo, args.tag, ana_start, ana_end - ana_start))
        seg.write(fn)
        meta["frames"][ifo] = {"file": fn, "channel": "%s:GWOSC-STRAIN" % ifo,
                               "t0": float(seg.t0.value), "dur": float(seg.duration.value),
                               "n": len(seg)}
        print("   wrote frame %s t0=%.3f dur=%.1f n=%d"
              % (fn, seg.t0.value, seg.duration.value, len(seg)), flush=True)

    cache = os.path.join(data, "frames.cache")
    with open(cache, "w") as fh:
        for ifo in ifos:
            fr = meta["frames"][ifo]
            obs = ifo[0]
            t0 = int(np.floor(fr["t0"])); dur = int(round(fr["dur"]))
            fh.write("%s %s %d %d file://localhost%s\n" % (obs, args.tag, t0, dur, fr["file"]))
    meta["cache"] = cache
    with open(os.path.join(data, "fetch_meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)
    print("[done] cache -> %s" % cache)
    print(open(cache).read())


if __name__ == "__main__":
    main()
