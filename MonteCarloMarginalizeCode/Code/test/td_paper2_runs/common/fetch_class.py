#!/usr/bin/env python
r"""fetch_class.py -- assemble a REAL per-CLASS glitch corpus from GWOSC open strain.

Generalizes test/td_glitch_poc/fetch_blips.py (which was hard-wired to L1 O3a "Blip") to an
ARBITRARY Gravity Spy class, detector, and snippet window, so the same conditioning pipeline
can build a BLIP corpus (for GW250114 / weak-O4a EV-4) or a SCATTERING corpus (for the
scattered-light / slow-scattering glitches in GW191109 and GW200129).

Corpus source: Gravity Spy machine-learning classifications (Glanzer et al., arXiv:2208.12849,
Zenodo record 5649212 -- <IFO>_<RUN>.csv, e.g. L1_O3a.csv, ~142 MB; download separately). The
ml_label strings for the classes this paper uses:
    Blip            short (~tens of ms), 30-300 Hz          -> --window-half 0.125 --f-lo 25 --f-hi 1000
    Scattered_Light long arches (~2-2.5 s), below ~30-50 Hz -> --window-half 1.5  --f-lo 10 --f-hi 60
(the O3 CSVs use "Scattered_Light" for the SLOW scattering arches; "Fast_Scattering" is a
different, short class -- do NOT use it for GW191109/GW200129.)

Conditioning per instance (applied identically to every snippet, documented for the report):
  1. fetch 2*--fetch-span s of open strain at 4096 Hz centred on the Gravity Spy peak time;
  2. resample to --fs (the analysis sample rate, default 2048 Hz);
  3. WHITEN with gwpy's median-PSD whitener (fftlength 4 s, overlap 2 s) -- the corpus, the
     PCA, and the held-out mismatch all live in the WHITENED domain (glitschen convention,
     Merritt et al. 2108.12044);
  4. band-pass [--f-lo, --f-hi] Hz (the class-appropriate band), 8th-order butterworth filtfilt;
  5. crop +-(--window-half) s around the |peak| within +-(--search-half) s of the catalog time;
  6. sign-align (peak sample > 0) and normalize to unit L2 norm.
The crop yields n_snip = round(2*window_half*fs) samples per instance; build_pca.py then works
on a central sub-window.

Output: <outdir>/<class>_corpus.npz with
    snippets (n x n_snip)  whitened, aligned, unit-norm snippet matrix
    gps      (n,)          Gravity Spy peak GPS of each instance
    snr, peak_frequency, ml_confidence (n,)  catalog metadata
    fs       scalar        analysis sample rate
    ifo, ml_label, window_half, f_lo, f_hi   provenance

Usage (blip, matches the paper-1 PoC defaults):
    fetch_class.py --catalog L1_O3a.csv --ifo L1 --ml-label Blip --outdir work \
        --window-half 0.125 --f-lo 25 --f-hi 1000 --snr-lo 12 --snr-hi 60 \
        --peakf-lo 100 --peakf-hi 450 --n-target 45
Usage (scattered light):
    fetch_class.py --catalog L1_O3a.csv --ifo L1 --ml-label Scattered_Light --outdir work \
        --window-half 1.5 --f-lo 10 --f-hi 60 --snr-lo 8 --snr-hi 100 \
        --peakf-lo 10 --peakf-hi 60 --duration-max 6 --n-target 60 --min-per-window 5
"""
import argparse
import os

import numpy as np


def select_glitches(catalog_csv, ml_label, n_target, snr_lo, snr_hi, peakf_lo, peakf_hi,
                    duration_max, min_conf, min_per_window, seed=1234, near_gps=None,
                    exclude_gps=None, exclude_half=0.0):
    """Select high-confidence, well-localized instances of ONE class, grouped by GWOSC file.

    Grouping by the 4096 s GWOSC file that contains each trigger minimizes network transfer:
    the densest windows (>= min_per_window selected triggers) are downloaded once and reused.
    """
    import pandas as pd
    df = pd.read_csv(catalog_csv)
    sel = df[(df.ml_label == ml_label) & (df.ml_confidence >= min_conf)
             & (df.snr >= snr_lo) & (df.snr <= snr_hi)
             & (df.peak_frequency.between(peakf_lo, peakf_hi))
             & (df.duration <= duration_max)].copy()
    if len(sel) == 0:
        raise SystemExit("[fetch] no %s triggers pass the selection in %s "
                         "(check --ml-label against the CSV's unique ml_label values)"
                         % (ml_label, catalog_csv))
    sel["gps"] = sel.peak_time + 1e-9 * sel.peak_time_ns
    sel = sel.drop_duplicates(subset="gps")
    if exclude_gps is not None and exclude_half > 0:   # never train on the event's own segment
        sel = sel[np.abs(sel.gps - float(exclude_gps)) > float(exclude_half)]
    sel["window"] = (sel.gps // 4096).astype(int)
    counts = sel.window.value_counts()
    windows = counts[counts >= min_per_window].index.tolist()
    if near_gps is not None:                        # detector-state relevance: nearest in time
        windows.sort(key=lambda w: abs(w * 4096 + 2048 - float(near_gps)))
    rows = []
    for w in windows:
        rows.append(sel[sel.window == w])
        if sum(len(r) for r in rows) >= n_target:
            break
    if not rows:                                   # fall back to the densest single window
        rows = [sel[sel.window == counts.index[0]]]
    import pandas as pd
    out = pd.concat(rows).sort_values("gps").head(n_target)
    return out


def _raw_gwosc(gps, ifo, fetch_span):
    from gwpy.timeseries import TimeSeries
    t0 = float(gps)
    return TimeSeries.fetch_open_data(ifo, t0 - fetch_span, t0 + fetch_span, sample_rate=4096,
                                      cache=True, verbose=False)


_DATAFIND_CACHE = {}


def _raw_datafind(gps, ifo, fetch_span, frametype, channel, server):
    """Read raw strain from the LOCAL (proprietary) frame archive via gwdatafind.

    Needed because Gravity Spy classifies Omicron triggers over ALL locked time, while GWOSC
    publishes only the observing-mode segments: the DENSEST scattered-light windows in
    L1_O3b.csv fall outside GWOSC coverage (this is the July-2026 fetch blocker -- every
    candidate returned "Cannot find a GWOSC dataset for L1 covering ...").  On an LDG cluster
    the same times are directly readable from the C01 archive.
    """
    import gwdatafind
    from gwpy.timeseries import TimeSeries
    t0 = float(gps)
    s, e = int(np.floor(t0 - fetch_span)) - 1, int(np.ceil(t0 + fetch_span)) + 1
    key = (ifo[0], frametype, s // 4096)
    urls = _DATAFIND_CACHE.get(key)
    if urls is None:
        urls = gwdatafind.find_urls(ifo[0], frametype, s, e, host=server)
        _DATAFIND_CACHE[key] = urls
    if not urls:
        raise RuntimeError("no frames for %s %s [%d,%d)" % (ifo, frametype, s, e))
    paths = [u.replace("file://localhost", "") for u in urls]
    return TimeSeries.read(paths, "%s:%s" % (ifo, channel), start=t0 - fetch_span,
                           end=t0 + fetch_span).resample(4096)


def condition_one(gps, ifo, fs_out, window_half, search_half, f_lo, f_hi, fetch_span,
                  source="gwosc", frametype=None, channel=None, datafind_server=None):
    """Fetch + condition one instance; returns the unit-norm whitened snippet or None."""
    import scipy.signal as ss
    t0 = float(gps)
    if source == "datafind":
        ts = _raw_datafind(gps, ifo, fetch_span, frametype, channel, datafind_server)
    else:
        ts = _raw_gwosc(gps, ifo, fetch_span)
    if np.any(np.isnan(ts.value)):
        return None
    ts = ts.resample(fs_out)
    w = ts.whiten(fftlength=4, overlap=2)
    # band-pass via scipy sos (gwpy's own design trips a wp/ws normalization bug against this
    # scipy version); zero-phase filtfilt, 8th-order butterworth.
    sos = ss.butter(8, [f_lo, min(f_hi, 0.9 * fs_out / 2.0)], btype="band",
                    fs=fs_out, output="sos")
    dt = float(w.dt.value)
    i_c = int(round((t0 - float(w.t0.value)) / dt))
    n_s = int(round(search_half / dt))
    seg = ss.sosfiltfilt(sos, np.asarray(w.value))
    lo, hi = max(0, i_c - n_s), min(len(seg), i_c + n_s)
    i_pk = lo + int(np.argmax(np.abs(seg[lo:hi])))
    n_h = int(round(window_half / dt))
    if i_pk - n_h < 0 or i_pk + n_h > len(seg):
        return None
    snip = seg[i_pk - n_h:i_pk + n_h].astype(np.float64)
    snip = snip * np.sign(snip[n_h])               # sign-align on the peak sample
    nrm = np.linalg.norm(snip)
    if nrm == 0:
        return None
    return snip / nrm


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", required=True, help="Gravity Spy <IFO>_<RUN>.csv (Zenodo 5649212)")
    ap.add_argument("--ifo", required=True, choices=["H1", "L1", "V1"])
    ap.add_argument("--ml-label", required=True, help="Gravity Spy class, e.g. Blip / Scattered_Light")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--fs", type=float, default=2048.0, help="analysis sample rate")
    ap.add_argument("--window-half", type=float, default=0.125,
                    help="crop half-width [s] (0.125 blip / ~1.5 scattering)")
    ap.add_argument("--search-half", type=float, default=0.15,
                    help="peak search half-window [s] around the catalog time")
    ap.add_argument("--f-lo", type=float, default=25.0)
    ap.add_argument("--f-hi", type=float, default=1000.0)
    ap.add_argument("--fetch-span", type=float, default=16.0, help="half-length of the raw fetch [s]")
    ap.add_argument("--snr-lo", type=float, default=12.0)
    ap.add_argument("--snr-hi", type=float, default=60.0)
    ap.add_argument("--peakf-lo", type=float, default=100.0)
    ap.add_argument("--peakf-hi", type=float, default=450.0)
    ap.add_argument("--duration-max", type=float, default=0.5)
    ap.add_argument("--min-conf", type=float, default=0.99)
    ap.add_argument("--n-target", type=int, default=45)
    ap.add_argument("--min-per-window", type=int, default=3)
    ap.add_argument("--source", choices=["gwosc", "datafind"], default="gwosc",
                    help="gwosc = open strain (observing segments only); datafind = the local "
                         "LDG frame archive (all locked time -- required for the dense O3b "
                         "scattered-light windows, which GWOSC does not publish)")
    ap.add_argument("--frametype", default="L1_HOFT_C01")
    ap.add_argument("--channel", default="DCS-CALIB_STRAIN_CLEAN_C01")
    ap.add_argument("--datafind-server", default="datafind.ldas.cit:80")
    ap.add_argument("--near-gps", type=float, default=None,
                    help="prefer catalog windows nearest this GPS (detector-state relevance)")
    ap.add_argument("--exclude-gps", type=float, default=None,
                    help="drop triggers within --exclude-half of this GPS (the event itself)")
    ap.add_argument("--exclude-half", type=float, default=0.0)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    sel = select_glitches(args.catalog, args.ml_label, args.n_target, args.snr_lo, args.snr_hi,
                          args.peakf_lo, args.peakf_hi, args.duration_max, args.min_conf,
                          args.min_per_window, near_gps=args.near_gps,
                          exclude_gps=args.exclude_gps, exclude_half=args.exclude_half)
    print("[fetch] %s %s: selected %d candidates in %d GWOSC windows"
          % (args.ifo, args.ml_label, len(sel), sel.window.nunique()))
    snippets, gps_l, snr_l, pf_l, conf_l = [], [], [], [], []
    for _, row in sel.iterrows():
        try:
            snip = condition_one(row.gps, args.ifo, args.fs, args.window_half,
                                 args.search_half, args.f_lo, args.f_hi, args.fetch_span,
                                 source=args.source, frametype=args.frametype,
                                 channel=args.channel, datafind_server=args.datafind_server)
        except Exception as exc:                   # data gap / fetch failure: skip
            print("[fetch] gps %.3f SKIP (%s)" % (row.gps, exc))
            continue
        if snip is None:
            print("[fetch] gps %.3f SKIP (nan / edge)" % row.gps)
            continue
        snippets.append(snip)
        gps_l.append(row.gps); snr_l.append(row.snr)
        pf_l.append(row.peak_frequency); conf_l.append(row.ml_confidence)
        print("[fetch] gps %.3f OK  snr=%.1f  fpeak=%.0f Hz  (%d so far)"
              % (row.gps, row.snr, row.peak_frequency, len(snippets)))
    if not snippets:
        raise SystemExit("[fetch] no instances survived conditioning")
    out = os.path.join(args.outdir, "%s_corpus.npz" % args.ml_label.lower())
    np.savez(out, snippets=np.array(snippets), gps=np.array(gps_l),
             snr=np.array(snr_l), peak_frequency=np.array(pf_l),
             ml_confidence=np.array(conf_l), fs=args.fs, ifo=args.ifo,
             ml_label=args.ml_label, window_half=args.window_half,
             f_lo=args.f_lo, f_hi=args.f_hi)
    print("[fetch] wrote %s : %d instances (n_snip=%d)"
          % (out, len(snippets), snippets[0].size))


if __name__ == "__main__":
    main()
