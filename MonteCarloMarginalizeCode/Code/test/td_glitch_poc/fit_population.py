#!/usr/bin/env python
r"""fit_population.py -- fit a population-informed coefficient prior for the glitch basis
(paper 2, Sec. 3.3 "Fitting the population prior").

Given the real-blip corpus (fetch_blips.py -> blip_corpus.npz) and the PCA basis
(build_pca.py -> blip_pca_white.npz), this projects the CONDITIONED training snippets onto
the r-column basis and fits a Gaussian population prior on the coefficients:

    Sigma_shape dimensionless population SHAPE covariance (max-eig 1; Sigma_a = sigma_g^2 Sigma_shape)
    mu_hat     dimensionless population MEAN direction (OPT-IN, DISCOURAGED; mu_a = sigma_g mu_hat)

so a ~ N(mu_a, Sigma_a) drives the paper-2 marginalization at a SINGLE profiled amplitude
sigma_g.  These are appended (optionally) to a basis npz alongside the existing
basis/lambda0/t0/fs contract, consumed by RIFT via --glitch-basis-file (absent keys =>
paper-1 flat behavior).

DEFAULT = M2 (COVARIANCE-ONLY, ZERO MEAN).  PI decision (2026-07-08).  Physics: there are no
phase-coherent glitches in strain -- a nonzero prior MEAN asserts that glitches are coherently
present with a fixed sign/phase at our reference time, which is wrong regardless of what the
corpus statistics say.  The corpus was sign-aligned before PCA, so its raw mean is a property
of the ALIGNED-CORPUS FRAME (conditioning sign-aligned the snippets), NOT of physical arrivals;
it is inadmissible as a strain-domain prior mean.  The M2 default therefore sets mu = 0 and
takes Sigma_shape from the SIGN-SYMMETRIZED UNCENTERED second moment E[a a^T] over the corpus
(equivalently the covariance over corpus {a_i} U {-a_i}).  On the sign-aligned corpus this equals
Sigma_central + mu mu^T -- i.e. the aligned-frame mean DIRECTION's power is RETAINED as variance
(a legitimate anisotropy of the shape), never used as a mean.  Max-eig normalized to 1 as before.

FULL mode (mu + Sigma_shape) is available ONLY via --include-mean-UNPHYSICAL and reproduces the
superseded FULL-mode record; its help text states the physics objection.  Do not use it for the
paper.

PROJECTION METRIC (Sec. 3.3 design question -- "the hack", kept pragmatically).  We project with
the plain pseudo-inverse a_i = B^+ g_i (whitened-domain Euclidean least squares; for the
orthonormal PCA basis this is a_i = B^T g_i).  This is the paper's Sec. 3.3 DEFAULT.  The
long-term-correct model traces glitches to raw/unwhitened witness channels with physical coupling
functions (likelihood-consistent C-weighted projection a_i = M0^-1 B^T C^-1 g_i is the near-term
refinement); both are future work, flagged as a TODO, not done here.

SIGN-ALIGNMENT DIAGNOSTIC (paper-2 Sec. 3.2 / g4_poc REPORT Sec. 3).  The corpus was sign-aligned
before PCA, so the RAW corpus mean is (at least partly) an alignment artifact.  We no longer USE
the mean; we report the aligned-frame mean-to-spread ratio ||mu_hat|| and its arbitrary-sign NULL
(random per-snippet +-1) purely as a DIAGNOSTIC of how anisotropic the second moment is: a large
ratio means the leading population mode carries a lot of the shape's power, and in M2 that power
is folded into Sigma_shape (as the mu mu^T contribution), never used as a mean.

HELD-OUT VALIDATION.  The same held-out split as build_pca.py is reconstructed under the FLAT
prior (LS projection) and under the POPULATION prior (coefficient-space MAP at the per-instance
whitened noise floor); the population prior must not DEGRADE reconstruction.  Mahalanobis
coverage under Sigma_shape is reported (chi^2_r calibration).

Output (default): <outdir>/population_fit.json  (all fitted numbers) and, with --write-basis,
a copy of the input basis npz with sigma_shape added (M2) -- no mu key unless
--include-mean-UNPHYSICAL.
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


def symmetrized_second_moment(A):
    r"""Sign-symmetrized UNCENTERED second moment E[a a^T] over the corpus (M2 construction).

    Fits the covariance on the sign-augmented corpus {a_i} U {-a_i}, whose mean is exactly zero,
    so the returned matrix is E[a a^T] = Sigma_central + mu mu^T (the aligned-frame mean
    direction's power RETAINED as variance, per the PI decision -- no phase-coherent strain mean).
    Uses the same robust (Ledoit-Wolf) estimator as ``robust_covariance`` for parity with the
    centered path, applied to the symmetrized corpus.  Returns (S2, method).
    """
    A = np.asarray(A, dtype=float)
    A_sym = np.vstack([A, -A])
    mu_sym, S2, method = robust_covariance(A_sym)
    # mu_sym is 0 up to round-off by construction; keep S2 symmetric.
    S2 = 0.5 * (S2 + S2.T)
    return S2, method


# ------------------------------------------------------------------ sign-alignment audit

def sign_alignment_audit(A, n_null=4000, seed=7):
    r"""DIAGNOSTIC: how anisotropic is the second moment along the aligned-frame mean direction?

    The corpus was sign-aligned before PCA, so its raw mean is a property of the aligned frame,
    NOT of physical arrivals; we do not use it as a prior mean (M2).  This routine reports the
    aligned-frame mean-to-spread ratio ||mu_hat||, the arbitrary-sign NULL distribution of that
    ratio (random per-snippet +-1), and the observed percentile, purely as a measure of how much
    of the shape's power the leading population mode carries -- power that M2 folds into
    Sigma_shape (the mu mu^T contribution), never into a mean.  A large ratio simply means a
    strongly anisotropic second moment; it does NOT justify a nonzero strain-domain prior mean.
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
    ap.add_argument("--mode", choices=["m2", "flat"], default="m2",
                    help="prior mode. 'm2' (DEFAULT) = covariance-only, ZERO mean; Sigma_shape "
                         "from the sign-symmetrized uncentered second moment E[a a^T]. "
                         "'flat' = paper-1 diagonal lambda0 (no population keys written).")
    ap.add_argument("--include-mean-UNPHYSICAL", action="store_true", dest="include_mean",
                    help="OPT-IN, DISCOURAGED: also write a nonzero population MEAN (FULL mode, "
                         "Sigma_central + mu_hat).  PHYSICS OBJECTION: there are no phase-coherent "
                         "glitches in strain -- a nonzero prior mean asserts glitches are "
                         "coherently present with a fixed sign/phase at our reference time, which "
                         "is wrong regardless of corpus statistics.  The aligned-corpus mean is a "
                         "property of the sign-aligned frame, not of physical arrivals, and is "
                         "inadmissible as a strain-domain prior mean.  Use ONLY to reproduce the "
                         "superseded FULL-mode record; never for the paper.")
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

    # ---- mode decision (PI 2026-07-08: default M2 = covariance-only, zero mean) ----
    if args.include_mean:
        chosen = "full"
    else:
        chosen = args.mode                         # "m2" or "flat"

    # ---- centered moments (diagnostic; FULL mode) ----
    mu_raw, S_central, cov_method = robust_covariance(A)
    evals_c = np.linalg.eigvalsh(S_central)
    max_eig_c = float(evals_c[-1])
    scale_c = np.sqrt(max_eig_c)
    mu_hat_full = mu_raw / scale_c                 # diagnostic aligned-frame mean direction

    # ---- M2: sign-symmetrized UNCENTERED second moment E[a a^T] = Sigma_central + mu mu^T ----
    S2, cov_method_m2 = symmetrized_second_moment(A)
    evals2 = np.linalg.eigvalsh(S2)
    max_eig2 = float(evals2[-1])
    Sigma_shape_m2 = S2 / max_eig2                 # DEFAULT population shape (max-eig 1)
    # how much of the second moment's power the aligned-frame mean direction carries
    mean_power_frac = float((mu_raw @ mu_raw) / np.trace(S2)) if np.trace(S2) > 0 else 0.0

    if chosen == "full":
        Sigma_shape = S_central / max_eig_c        # superseded FULL: centered covariance shape
        mu_hat = mu_hat_full
        scale = scale_c
        Sigma_pop_scaled = S_central               # raw units for held-out MAP
        cov_report = cov_method
    else:                                          # "m2" (default) or "flat"
        Sigma_shape = Sigma_shape_m2               # M2 covariance-only shape
        mu_hat = np.zeros(r)
        scale = np.sqrt(max_eig2)
        Sigma_pop_scaled = S2
        cov_report = cov_method_m2
    print("[fit] centered cov: %s ; eig[%.3e..%.3e] ; ||mu_hat(aligned-frame)||=%.4f"
          % (cov_method, evals_c[0], evals_c[-1], np.linalg.norm(mu_hat_full)))
    print("[fit] M2 second moment E[aa^T]: %s ; eig[%.3e..%.3e] ; mean-power fraction "
          "||mu||^2/tr(S2)=%.3f (folded into Sigma_shape, NOT used as a mean)"
          % (cov_method_m2, evals2[0], evals2[-1], mean_power_frac))

    # ---- sign-alignment DIAGNOSTIC (mean is NOT used as a prior mean) ----
    audit = sign_alignment_audit(A)
    ratio = audit["obs_ratio"]
    pct = audit["observed_percentile"]
    print("[diag] aligned-frame mean-to-spread ||mu_hat||=%.4f ; sign-flip NULL median=%.4f "
          "p95=%.4f ; observed at %.1f-th percentile.  Anisotropy of E[aa^T] -- its power is "
          "FOLDED INTO Sigma_shape, never used as a mean." % (ratio, audit["null_median"],
                                                              audit["null_p95"], pct))
    if chosen == "full":
        print("[mode] chosen = FULL (mu + Sigma_central) via --include-mean-UNPHYSICAL.  "
              "WARNING: a nonzero strain-domain prior mean is UNPHYSICAL (no phase-coherent "
              "glitches); reproduces the superseded record only.")
    else:
        print("[mode] chosen = %s (zero mean; Sigma_shape from E[aa^T])" % chosen.upper())

    # ---- held-out validation: flat vs population reconstruction ----
    ho_rows = []
    mu_pop_scaled = mu_hat * scale                 # back to raw-coefficient units for MAP
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
        chosen_mode=chosen,
        mean_policy=("ZERO mean (M2: covariance-only; Sigma_shape from sign-symmetrized "
                     "uncentered second moment E[aa^T])" if chosen != "full" else
                     "NONZERO mean written (FULL mode via --include-mean-UNPHYSICAL; UNPHYSICAL)"),
        cov_method=cov_report,
        cov_method_centered=cov_method, cov_method_m2=cov_method_m2,
        max_eig_S_central=max_eig_c, max_eig_S2=max_eig2,
        sigma_shape_eigs=[float(x) for x in np.linalg.eigvalsh(Sigma_shape)],
        sigma_shape_m2_eigs=[float(x) for x in evals2 / max_eig2],
        mu_hat_aligned_frame_DIAGNOSTIC=[float(x) for x in mu_hat_full],
        mu_hat_aligned_frame_norm=float(np.linalg.norm(mu_hat_full)),
        mean_power_fraction=mean_power_frac,
        sign_diag=audit,
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
        if chosen in ("m2", "full"):
            payload["sigma_shape"] = Sigma_shape
        if chosen == "full":
            payload["mu"] = mu_hat                 # UNPHYSICAL; opt-in only
        base = os.path.basename(args.write_basis).replace(".npz", "")
        out_npz = os.path.join(args.outdir, "%s_pop_%s.npz" % (base, chosen))
        np.savez(out_npz, **payload)
        print("[fit] wrote augmented basis (%s mode): %s [keys: %s]"
              % (chosen, out_npz, ", ".join(sorted(payload))))


if __name__ == "__main__":
    main()
