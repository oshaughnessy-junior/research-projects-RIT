#!/usr/bin/env python
r"""fit_population.py -- fit a population-informed coefficient prior for the glitch basis
(paper 2, Sec. 3.3 "Fitting the population prior").

Given the real-blip corpus (fetch_blips.py -> blip_corpus.npz) and the PCA basis
(build_pca.py -> blip_pca_white.npz), this projects the CONDITIONED training snippets onto
the r-column basis and fits a Gaussian population prior on the coefficients:

    mu_hat     dimensionless population MEAN direction   (scale mixture: mu_a = sigma_g mu_hat)
    Sigma_shape dimensionless population SHAPE covariance (max-eig 1; Sigma_a = sigma_g^2 Sigma_shape)

so a ~ N(mu_a, Sigma_a) drives the paper-2 marginalization at a SINGLE profiled amplitude
sigma_g.  These are appended (optionally) to a basis npz alongside the existing
basis/lambda0/t0/fs contract, consumed by RIFT via --glitch-basis-file (COVARIANCE-ONLY or
FULL mode; absent keys => paper-1 flat behavior).

PROJECTION METRIC (Sec. 3.3 design question).  We project with the plain pseudo-inverse
a_i = B^+ g_i (whitened-domain Euclidean least squares; for the orthonormal PCA basis this is
a_i = B^T g_i).  This is the paper's Sec. 3.3 DEFAULT.  The likelihood-consistent C-weighted
projection a_i = M0^-1 B^T C^-1 g_i is flagged as a TODO: for the whitened corpus with white-
ish conditioning it nearly coincides, but for a strongly colored analysis segment they differ.

SIGN-ALIGNMENT AUDIT (paper-2 Sec. 3.2 / g4_poc REPORT Sec. 1).  The corpus was sign-aligned
before PCA, so the RAW corpus mean may be an ALIGNMENT ARTIFACT rather than physics.  We audit:
  (1) the mean-to-spread ratio ||mu_hat|| (mean amplitude relative to the shape scale);
  (2) a sign-flip NULL: randomly flip each snippet's sign many times and rebuild ||mu_hat||;
      the observed value is compared to that null distribution (percentile).
If the observed mean is NOT clearly above the arbitrary-sign null (or the ratio is tiny), the
mean is consistent with an alignment artifact and we DEFAULT to COVARIANCE-ONLY (mu = 0).

HELD-OUT VALIDATION.  The same held-out split as build_pca.py is reconstructed under the FLAT
prior (LS projection) and under the POPULATION prior (coefficient-space MAP at the per-instance
whitened noise floor); the population prior must not DEGRADE reconstruction.  Mahalanobis
coverage under Sigma_shape is reported (chi^2_r calibration).

Output (default): <outdir>/population_fit.json  (all audited numbers) and, with --write-basis,
a copy of the input basis npz with mu/sigma_shape added per the chosen mode.
"""
import argparse
import json
import os

import numpy as np


# ------------------------------------------------------------------ robust moments

def robust_covariance(A):
    """Return (mu, Sigma, method) with a robust covariance if sklearn is available.

    Prefers Ledoit-Wolf shrinkage (well-behaved when r ~ n_train); falls back to a small
    diagonal-loaded sample covariance.  mu is always the (non-robust) sample mean for the
    mean direction -- MCD would also robustify the location; we report the sample mean and
    let the sign-audit decide whether the mean is trustworthy at all.
    """
    A = np.asarray(A, dtype=float)
    n, r = A.shape
    mu = A.mean(axis=0)
    try:
        from sklearn.covariance import LedoitWolf
        lw = LedoitWolf(assume_centered=False).fit(A)
        return mu, lw.covariance_, "ledoit-wolf(shrink=%.3f)" % float(lw.shrinkage_)
    except Exception as exc:
        Ac = A - mu
        S = (Ac.T @ Ac) / max(n - 1, 1)
        S = S + 1e-6 * np.trace(S) / r * np.eye(r)
        return mu, S, "sample-cov+diag-load (%s)" % type(exc).__name__


# ------------------------------------------------------------------ sign-alignment audit

def sign_alignment_audit(A, n_null=4000, seed=7):
    r"""Audit whether the corpus mean is a genuine population direction or an alignment artifact.

    Returns a dict with the observed mean-to-spread ratio ||mu_hat||, the arbitrary-sign NULL
    distribution of that ratio (random per-snippet +-1), and the observed percentile.  A large
    percentile (observed >> arbitrary sign) => a real common mean (adopt FULL).  A percentile
    near the bulk of the null => the mean is consistent with the sign convention (COVARIANCE-ONLY).
    """
    A = np.asarray(A, dtype=float)
    n, r = A.shape
    # scale = sqrt(max-eig of the sample covariance) = the shape amplitude the mean is measured against
    Ac = A - A.mean(axis=0)
    S = (Ac.T @ Ac) / max(n - 1, 1)
    scale = float(np.sqrt(np.max(np.linalg.eigvalsh(S))))
    obs_ratio = float(np.linalg.norm(A.mean(axis=0)) / scale)

    rng = np.random.default_rng(seed)
    null = np.empty(n_null)
    for i in range(n_null):
        signs = rng.choice([-1.0, 1.0], size=n)
        As = signs[:, None] * A
        # recompute the shape scale under the flipped signs (covariance changes under flips)
        Asc = As - As.mean(axis=0)
        Ss = (Asc.T @ Asc) / max(n - 1, 1)
        sc = float(np.sqrt(np.max(np.linalg.eigvalsh(Ss))))
        null[i] = np.linalg.norm(As.mean(axis=0)) / sc
    pct = float((null < obs_ratio).mean() * 100.0)
    return dict(obs_ratio=obs_ratio, null_median=float(np.median(null)),
                null_p95=float(np.percentile(null, 95)),
                null_p99=float(np.percentile(null, 99)),
                observed_percentile=pct, n_null=n_null, r=r, n_train=n)


# ------------------------------------------------------------------ held-out validation

def heldout_reconstruction(B, g, mu_pop, Sigma_pop, noise_var):
    r"""Flat-LS and population-MAP reconstruction mismatch for one held-out snippet.

    Coefficient model a_ls = B^T g ~ N(a_true, noise_var I) (orthonormal B, unit-variance
    whitened noise projected onto r columns).  Flat: g_flat = B a_ls (uninformative prior).
    Population: a_MAP = (Sigma_pop^-1 + I/noise_var)^-1 (Sigma_pop^-1 mu_pop + a_ls/noise_var),
    g_pop = B a_MAP.  Returns (mismatch_flat, mismatch_pop, mahalanobis2).
    """
    a_ls = B.T @ g
    g_flat = B @ a_ls
    Sinv = np.linalg.inv(Sigma_pop)
    Aprec = Sinv + np.eye(len(a_ls)) / noise_var
    a_map = np.linalg.solve(Aprec, Sinv @ mu_pop + a_ls / noise_var)
    g_pop = B @ a_map

    def mism(gr):
        num = float(np.dot(g, gr))
        den = float(np.linalg.norm(g) * np.linalg.norm(gr))
        return 1.0 - num / den if den > 0 else 1.0
    d = a_ls - mu_pop
    maha2 = float(d @ Sinv @ d)
    return mism(g_flat), mism(g_pop), maha2


# ------------------------------------------------------------------ main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", required=True, help="blip_corpus.npz from fetch_blips.py")
    ap.add_argument("--pca", required=True, help="blip_pca_white.npz from build_pca.py")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--r", type=int, default=None, help="rank (default: r_sel from the PCA)")
    ap.add_argument("--seed", type=int, default=20260708,
                    help="MUST match build_pca.py --seed to reproduce the train/holdout split")
    ap.add_argument("--n-holdout", type=int, default=5,
                    help="MUST match build_pca.py --n-holdout")
    ap.add_argument("--ratio-floor", type=float, default=0.35,
                    help="min ||mu_hat|| (mean-to-spread) to consider the mean physical")
    ap.add_argument("--null-percentile", type=float, default=95.0,
                    help="observed sign-audit percentile above which the mean is adopted (FULL)")
    ap.add_argument("--mode", choices=["auto", "full", "covariance-only", "flat"],
                    default="auto", help="prior mode; 'auto' = decide from the sign audit")
    ap.add_argument("--write-basis", default=None,
                    help="input basis npz to copy + augment with mu/sigma_shape (e.g. "
                         "glitch_basis_L1.npz); the r must match")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    corpus = np.load(args.corpus)
    pca = np.load(args.pca)
    n_win = int(pca["n_win"])
    r = int(args.r if args.r is not None else pca["r_sel"])
    Vt = np.asarray(pca["components"])
    B = Vt[:r].T                                   # (n_win, r), orthonormal columns
    lambda0 = np.asarray(pca["lambda0"])[:r]
    lambda0 = lambda0 / np.max(lambda0)

    # rebuild the SAME central-window, unit-norm snippets and the SAME split as build_pca.py
    X_full = np.asarray(corpus["snippets"])        # (n, 512), peak at 256
    n, n_full = X_full.shape
    c = n_full // 2
    h = n_win // 2
    X = X_full[:, c - h:c + h]
    X = X / np.linalg.norm(X, axis=1)[:, None]
    rng = np.random.default_rng(args.seed)
    holdout_idx = np.sort(rng.choice(n, size=args.n_holdout, replace=False))
    train_idx = np.setdiff1d(np.arange(n), holdout_idx)
    Xtr, Xho = X[train_idx], X[holdout_idx]
    snr_cat = np.asarray(corpus["snr"])

    # ---- projections (plain pseudo-inverse = B^T for orthonormal B) ----
    A = Xtr @ B                                    # (n_train, r) coefficient sample
    print("[fit] r=%d, n_train=%d, n_holdout=%d, |a_i| mean=%.4f (unit-norm snippets)"
          % (r, len(train_idx), len(holdout_idx), np.mean(np.linalg.norm(A, axis=1))))

    # ---- robust moments ----
    mu_raw, S_raw, cov_method = robust_covariance(A)
    evals = np.linalg.eigvalsh(S_raw)
    max_eig = float(evals[-1])
    # shared-scale normalization (coherent scale mixture): Sigma_shape max-eig 1, and mu_hat
    # normalized by the SAME sqrt(max_eig) so ||mu_hat|| = mean-to-shape-scale ratio survives.
    scale = np.sqrt(max_eig)
    Sigma_shape = S_raw / max_eig
    mu_hat_full = mu_raw / scale
    print("[fit] robust cov: %s ; eig(S_raw)=[%.3e .. %.3e] ; ||mu_hat||=%.4f"
          % (cov_method, evals[0], evals[-1], np.linalg.norm(mu_hat_full)))

    # ---- sign-alignment audit ----
    audit = sign_alignment_audit(A)
    ratio = audit["obs_ratio"]
    pct = audit["observed_percentile"]
    print("[audit] mean-to-spread ||mu_hat||=%.4f ; sign-flip NULL median=%.4f p95=%.4f ; "
          "observed at %.1f-th percentile of the arbitrary-sign null"
          % (ratio, audit["null_median"], audit["null_p95"], pct))

    # ---- mode decision ----
    if args.mode == "auto":
        mean_is_physical = (ratio >= args.ratio_floor) and (pct >= args.null_percentile)
        chosen = "full" if mean_is_physical else "covariance-only"
        audit_verdict = ("mean ADOPTED (ratio %.2f >= %.2f AND percentile %.1f >= %.1f)"
                         % (ratio, args.ratio_floor, pct, args.null_percentile)) \
            if mean_is_physical else \
            ("mean REJECTED as alignment-ambiguous (ratio %.2f, percentile %.1f) -> "
             "COVARIANCE-ONLY" % (ratio, pct))
    else:
        chosen = args.mode
        audit_verdict = "mode forced to %s by --mode" % chosen
    print("[mode] chosen = %s ; %s" % (chosen.upper(), audit_verdict))

    if chosen == "full":
        mu_hat = mu_hat_full
    else:
        mu_hat = np.zeros(r)

    # ---- held-out validation: flat vs population reconstruction ----
    ho_rows = []
    mu_pop_scaled = mu_hat * scale                 # back to raw-coefficient units for MAP
    Sigma_pop_scaled = Sigma_shape * max_eig       # == S_raw (raw units)
    if chosen == "flat":
        Sigma_pop_scaled = np.diag((scale ** 2) * lambda0)  # flat diagonal in raw units
        mu_pop_scaled = np.zeros(r)
    for k, g in enumerate(Xho):
        # per-instance whitened noise share (same model as build_pca held-out floor)
        noise_frac = n_win / (snr_cat[holdout_idx[k]] ** 2 + n_win)
        # coefficient-noise variance ~ noise_frac spread over the r retained modes
        noise_var = max(noise_frac / r * (np.linalg.norm(g) ** 2), 1e-12)
        mm_flat, mm_pop, maha2 = heldout_reconstruction(
            B, g, mu_pop_scaled, Sigma_pop_scaled, noise_var)
        ho_rows.append(dict(gps=float(corpus["gps"][holdout_idx[k]]),
                            cat_snr=float(snr_cat[holdout_idx[k]]),
                            mismatch_flat=mm_flat, mismatch_pop=mm_pop,
                            mahalanobis2=maha2))
        print("[held-out] gps %.3f (SNR %.1f): mism flat=%.4f pop=%.4f  D^2=%.2f (r=%d)"
              % (ho_rows[-1]["gps"], ho_rows[-1]["cat_snr"], mm_flat, mm_pop, maha2, r))
    mean_flat = float(np.mean([x["mismatch_flat"] for x in ho_rows]))
    mean_pop = float(np.mean([x["mismatch_pop"] for x in ho_rows]))
    print("[held-out] MEAN mismatch: flat=%.4f  population=%.4f  (pop should be <= flat)"
          % (mean_flat, mean_pop))

    # ---- write summary + optional augmented basis ----
    summary = dict(
        r=r, n_train=int(len(train_idx)), n_holdout=int(len(holdout_idx)),
        projection_metric="pseudo-inverse (whitened Euclidean; C-weighted = TODO)",
        cov_method=cov_method, max_eig_S_raw=max_eig,
        mu_hat_full=[float(x) for x in mu_hat_full],
        mu_hat_norm_full=float(np.linalg.norm(mu_hat_full)),
        sigma_shape_eigs=[float(x) for x in np.linalg.eigvalsh(Sigma_shape)],
        sign_audit=audit, ratio_floor=args.ratio_floor,
        null_percentile_threshold=args.null_percentile,
        chosen_mode=chosen, audit_verdict=audit_verdict,
        mu_hat_written=[float(x) for x in mu_hat],
        heldout=ho_rows, heldout_mean_mismatch_flat=mean_flat,
        heldout_mean_mismatch_pop=mean_pop)
    out_json = os.path.join(args.outdir, "population_fit.json")
    with open(out_json, "w") as fh:
        json.dump(summary, fh, indent=2)
    print("[fit] wrote %s" % out_json)

    if args.write_basis:
        src = np.load(args.write_basis)
        if src["basis"].shape[1] != r:
            raise ValueError("--write-basis rank %d != fit rank %d"
                             % (src["basis"].shape[1], r))
        payload = dict(basis=src["basis"], lambda0=src["lambda0"],
                       t0=src["t0"], fs=src["fs"])
        if chosen in ("full", "covariance-only"):
            payload["sigma_shape"] = Sigma_shape
        if chosen == "full":
            payload["mu"] = mu_hat
        base = os.path.basename(args.write_basis).replace(".npz", "")
        out_npz = os.path.join(args.outdir, "%s_pop_%s.npz" % (base, chosen.replace("-", "")))
        np.savez(out_npz, **payload)
        print("[fit] wrote augmented basis (%s mode): %s [keys: %s]"
              % (chosen, out_npz, ", ".join(sorted(payload))))


if __name__ == "__main__":
    main()
