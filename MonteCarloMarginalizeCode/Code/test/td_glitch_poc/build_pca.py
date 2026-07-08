#!/usr/bin/env python
"""build_pca.py -- data-informed blip PCA basis from the real-blip corpus (G4 PoC step 2).

PPCA a la glitschen (Merritt et al. 2108.12044), basis modes 3/4 of
GLITCH_MARG_DESIGN.md Sec. 3: the whitened, peak-aligned, unit-norm blip snippets from
fetch_blips.py are decomposed with an UNCENTERED SVD (no mean subtraction: the basis must
span the glitch itself, g = B a, not deviations from a mean -- the mean blip shape is then
simply the leading principal component).  The Gaussian coefficient prior shape is read off
the singular values: Lambda_0 = s^2 / max(s^2) (dimensionless, max eigenvalue 1 per the
Sec. 2b units convention; sigma_g carries the strain scale downstream).

Held-out validation (the G4 acceptance idea): --n-holdout instances are excluded from the
SVD (deterministic RNG); each is reconstructed by projection onto the r-column basis and
the WHITENED mismatch 1 - <g, g_r>/(|g| |g_r|) reported.  Because the corpus lives in the
whitened domain and the analysis colors basis and injection with the SAME PSD, this equals
the analysis-whitened mismatch (up to band edges).

r is chosen as the smallest rank capturing >= --min-energy of the training energy
(default 0.90); the full r-vs-captured-energy curve is written for the report.

Output <outdir>/blip_pca_white.npz:
    components (r_max x n_win)  whitened-domain principal components (rows, unit norm)
    svals      (r_max,)         singular values
    energy     (r_max,)         cumulative captured energy fraction
    r_sel      scalar           selected rank (>= min-energy)
    lambda0    (r_max,)         s^2 / max(s^2)
    holdout_idx, holdout_snippets, holdout_mismatch (at r_sel), holdout_gps
    fs, n_win
"""
import argparse
import json
import os

import numpy as np


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", required=True, help="blip_corpus.npz from fetch_blips.py")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--n-window", type=int, default=64,
                    help="central PCA window [samples] (64 = 31.2 ms at 2048 Hz; window "
                         "sweep 256/128/96/64 showed the held-out mismatch tracks the "
                         "whitened-noise share, so the tightest window covering the blip "
                         "core is best -- and 64 reproduces glitschen's r=6 blip rank)")
    ap.add_argument("--n-holdout", type=int, default=5)
    ap.add_argument("--min-energy", type=float, default=0.90)
    ap.add_argument("--seed", type=int, default=20260708)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    dat = np.load(args.corpus)
    X_full = np.asarray(dat["snippets"])              # (n, 512), peak at sample 256
    fs = float(dat["fs"])
    n, n_full = X_full.shape
    c = n_full // 2
    h = args.n_window // 2
    X = X_full[:, c - h:c + h]                        # central window, peak at n_window/2
    X = X / np.linalg.norm(X, axis=1)[:, None]        # re-normalize after crop

    rng = np.random.default_rng(args.seed)
    holdout_idx = np.sort(rng.choice(n, size=args.n_holdout, replace=False))
    train_idx = np.setdiff1d(np.arange(n), holdout_idx)
    Xtr, Xho = X[train_idx], X[holdout_idx]
    print("[pca] corpus n=%d -> train %d / holdout %d, window %d samples (%.1f ms)"
          % (n, len(train_idx), len(holdout_idx), args.n_window, 1e3 * args.n_window / fs))

    # uncentered SVD: rows of Vt are the principal blip shapes
    U, s, Vt = np.linalg.svd(Xtr, full_matrices=False)
    energy = np.cumsum(s ** 2) / np.sum(s ** 2)
    r_sel = int(np.searchsorted(energy, args.min_energy) + 1)
    lambda0 = s ** 2 / np.max(s ** 2)
    print("[pca] r-vs-energy: " + "  ".join(
        "r=%d:%.3f" % (j + 1, energy[j]) for j in range(min(12, len(energy)))))
    print("[pca] selected r=%d (captured %.3f >= %.2f)"
          % (r_sel, energy[r_sel - 1], args.min_energy))

    # held-out reconstruction at r_sel.  NOTE the noise floor: each real snippet is
    # blip + WHITENED DETECTOR NOISE (unit variance/sample => expected noise power n_win
    # against blip power ~ SNR_cat^2).  A perfect basis still leaves the (1 - r/n_win)
    # share of that noise in the residual, so the honest acceptance is "mismatch
    # consistent with the per-instance noise floor", not an absolute few-%.
    B = Vt[:r_sel].T                                  # (n_win, r_sel), orthonormal columns
    mm, floors = [], []
    snr_cat = np.asarray(dat["snr"])[holdout_idx]
    for k, g in enumerate(Xho):
        g_r = B.dot(B.T.dot(g))
        ovl = float(np.dot(g, g_r) / (np.linalg.norm(g) * np.linalg.norm(g_r)))
        mm.append(1.0 - ovl)
        res_pow = 1.0 - np.linalg.norm(g_r) ** 2 / np.linalg.norm(g) ** 2
        # expected residual power if the basis captured the glitch exactly (catalog SNR
        # is Omicron's, over a different band/whitening -- approximate by construction)
        noise_frac = args.n_window / (snr_cat[k] ** 2 + args.n_window)
        floor = noise_frac * (1.0 - r_sel / float(args.n_window))
        floors.append(floor)
        print("[pca] holdout %d (gps %.3f, cat SNR %.1f): whitened mismatch %.4f "
              "(overlap %.4f), residual power %.4f vs noise-floor expectation ~%.4f"
              % (holdout_idx[k], dat["gps"][holdout_idx[k]], snr_cat[k], 1.0 - ovl,
                 ovl, res_pow, floor))
    out = os.path.join(args.outdir, "blip_pca_white.npz")
    np.savez(out, components=Vt, svals=s, energy=energy, r_sel=r_sel, lambda0=lambda0,
             holdout_idx=holdout_idx, holdout_snippets=Xho,
             holdout_mismatch=np.array(mm), holdout_gps=dat["gps"][holdout_idx],
             train_gps=dat["gps"][train_idx], fs=fs, n_win=args.n_window)
    summary = dict(n_corpus=int(n), n_train=int(len(train_idx)),
                   n_holdout=int(args.n_holdout), r_sel=r_sel,
                   energy_at_r=[float(e) for e in energy[:min(20, len(energy))]],
                   captured=float(energy[r_sel - 1]),
                   holdout_mismatch=[float(x) for x in mm],
                   holdout_noise_floor_power=[float(x) for x in floors],
                   holdout_cat_snr=[float(x) for x in snr_cat],
                   holdout_gps=[float(x) for x in dat["gps"][holdout_idx]])
    with open(os.path.join(args.outdir, "pca_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print("[pca] wrote %s" % out)


if __name__ == "__main__":
    main()
