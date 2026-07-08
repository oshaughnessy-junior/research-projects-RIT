#!/usr/bin/env python
"""fetch_blips.py -- assemble a REAL L1 blip corpus from GWOSC open strain (G4 PoC step 1).

Acquisition (design GLITCH_MARG_DESIGN.md Sec. 3 basis modes 3/4; PoC plan step 1, rung b):
Gravity Spy machine-learning glitch classifications (Glanzer et al., Zenodo record 5649212,
L1_O3a.csv) supply GPS times of high-confidence L1 O3a 'Blip' glitches; the open strain
around each is fetched from GWOSC with gwpy.  To minimize network transfer the blips are
GROUPED by the 4096 s GWOSC file that contains them (windows holding >= --min-per-window
selected blips are downloaded once and reused, cache=True).

Conditioning per blip (documented, applied identically to every instance):
  1. fetch 32 s of open strain at 4096 Hz centred on the Gravity Spy peak time;
  2. resample to 2048 Hz (the PoC analysis rate);
  3. WHITEN with gwpy's median-PSD whitener (fftlength 4 s, overlap 2 s) -- the corpus,
     the PCA, and the held-out mismatch all live in the WHITENED domain, exactly the
     glitschen (Merritt et al. 2108.12044) convention;
  4. band-pass 25-1000 Hz (the analysis band), 8th-order butterworth filtfilt;
  5. crop +-0.125 s (512 samples) around the |peak| within +-0.15 s of the catalog time;
  6. sign-align (peak sample > 0) and normalize to unit L2 norm.

Output: <outdir>/blip_corpus.npz with
    snippets (n_blip x 512)  whitened, aligned, unit-norm snippet matrix
    gps      (n_blip,)       Gravity Spy peak GPS of each instance
    snr, peak_frequency, ml_confidence (n_blip,)  catalog metadata
    fs       scalar (2048.0)

Usage:
    fetch_blips.py --catalog L1_O3a.csv --outdir <workdir> [--n-target 45]
"""
import argparse
import os

import numpy as np


def select_blips(catalog_csv, n_target, min_per_window=3, seed=1234):
    """Select high-confidence, moderate-SNR, well-localized blips grouped by GWOSC file."""
    import pandas as pd
    df = pd.read_csv(catalog_csv)
    sel = df[(df.ml_label == "Blip") & (df.ml_confidence >= 0.99)
             & (df.snr >= 12) & (df.snr <= 60)
             & (df.peak_frequency.between(100, 450)) & (df.duration <= 0.5)].copy()
    sel["gps"] = sel.peak_time + 1e-9 * sel.peak_time_ns
    sel = sel.drop_duplicates(subset="gps")
    sel["window"] = (sel.gps // 4096).astype(int)
    counts = sel.window.value_counts()
    # densest 4096 s windows first: fewest GWOSC file downloads for the corpus
    windows = counts[counts >= min_per_window].index.tolist()
    rows = []
    for w in windows:
        rows.append(sel[sel.window == w])
        if sum(len(r) for r in rows) >= n_target:
            break
    out = None
    if rows:
        out = rows[0].iloc[0:0]
        out = pd_concat(rows).sort_values("gps").head(n_target)
    return out


def pd_concat(rows):
    import pandas as pd
    return pd.concat(rows)


def condition_one(gps, fs_out=2048.0, span=16.0, crop_half=0.125, search_half=0.15,
                  f_lo=25.0, f_hi=1000.0):
    """Fetch + condition one blip instance; returns (snippet, ok) per the module docstring."""
    import scipy.signal as ss
    from gwpy.timeseries import TimeSeries
    t0 = float(gps)
    ts = TimeSeries.fetch_open_data("L1", t0 - span, t0 + span, sample_rate=4096,
                                    cache=True, verbose=False)
    if np.any(np.isnan(ts.value)):
        return None
    ts = ts.resample(fs_out)
    w = ts.whiten(fftlength=4, overlap=2)
    # band-pass via scipy sos (gwpy's own filter design trips a wp/ws normalization bug
    # against this scipy version); zero-phase filtfilt, 8th-order butterworth.
    sos = ss.butter(8, [f_lo, min(f_hi, 0.9 * fs_out / 2.0)], btype="band",
                    fs=fs_out, output="sos")
    dt = float(w.dt.value)
    i_c = int(round((t0 - float(w.t0.value)) / dt))
    n_s = int(round(search_half / dt))
    seg = ss.sosfiltfilt(sos, np.asarray(w.value))
    lo, hi = max(0, i_c - n_s), min(len(seg), i_c + n_s)
    i_pk = lo + int(np.argmax(np.abs(seg[lo:hi])))
    n_h = int(round(crop_half / dt))
    if i_pk - n_h < 0 or i_pk + n_h > len(seg):
        return None
    snip = seg[i_pk - n_h:i_pk + n_h].astype(np.float64)
    snip = snip * np.sign(snip[n_h])          # sign-align on the peak sample
    nrm = np.linalg.norm(snip)
    if nrm == 0:
        return None
    return snip / nrm


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", required=True, help="Gravity Spy L1_O3a.csv (Zenodo 5649212)")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--n-target", type=int, default=45)
    ap.add_argument("--min-per-window", type=int, default=3)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    sel = select_blips(args.catalog, args.n_target, args.min_per_window)
    print("[fetch] selected %d blip candidates in %d GWOSC windows"
          % (len(sel), sel.window.nunique()))
    snippets, gps_l, snr_l, pf_l, conf_l = [], [], [], [], []
    for _, row in sel.iterrows():
        try:
            snip = condition_one(row.gps)
        except Exception as exc:                      # data gap / fetch failure: skip
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
    out = os.path.join(args.outdir, "blip_corpus.npz")
    np.savez(out, snippets=np.array(snippets), gps=np.array(gps_l),
             snr=np.array(snr_l), peak_frequency=np.array(pf_l),
             ml_confidence=np.array(conf_l), fs=2048.0)
    print("[fetch] wrote %s : %d instances" % (out, len(snippets)))


if __name__ == "__main__":
    main()
