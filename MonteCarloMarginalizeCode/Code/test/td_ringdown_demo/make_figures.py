#!/usr/bin/env python
"""make_figures.py -- analysis + figures for the T7 ringdown/inspiral demo.

Inputs (produced by run_deterministic_surfaces.py / run_config.sh / pp_run.py):
  zn_det.json, noise_det.json   : deterministic 3-config time-marginalized lnL surfaces
  zn/<cfg>/collated.txt         : Monte-Carlo NoLoop lnL_marg surfaces (full-extrinsic)
  pp/pp_results.json            : ringdown-only pp-check quantiles

Outputs (figures/ + printed tables):
  fig_consistency_det.png       : HEADLINE -- 1D marginal lnL_marg(mchirp) for the three
                                  configs (deterministic), truth marked, width ordering.
  fig_consistency_2d.png        : 2D lnL_marg contours in (mchirp,eta) for the three configs.
  fig_mc_vs_det.png             : MC full-extrinsic vs deterministic 1D marginals.
  fig_pp.png                    : ringdown-only pp-plot (mchirp quantiles vs uniform).
"""
import os, sys, json, argparse
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(HERE, "figures")
os.makedirs(FIG, exist_ok=True)
MC_TRUTH = 43.5285   # fiducial 50+50 chirp mass
ETA_TRUTH = 0.25
CFGS = ["full", "inspiral", "ringdown"]
COL = {"full": "k", "inspiral": "tab:blue", "ringdown": "tab:red"}


def surface_arrays(rows):
    mc = np.array([r["mchirp"] for r in rows])
    eta = np.array([r["eta"] for r in rows])
    lnL = np.array([r["lnL_marg"] for r in rows])
    return mc, eta, lnL


def marg_mc(mc, lnL):
    """1D posterior over mchirp (marginalize eta), flat prior."""
    mcs = np.unique(mc)
    p = []
    for m in mcs:
        l = lnL[np.isclose(mc, m)]
        p.append(np.sum(np.exp(l - lnL.max())))
    p = np.array(p); p = p / np.trapz(p, mcs)
    return mcs, p


def summarize(mcs, p, truth=MC_TRUTH):
    mean = np.trapz(p * mcs, mcs)
    var = np.trapz(p * (mcs - mean) ** 2, mcs)
    sig = np.sqrt(max(var, 1e-12))
    # CDF quantile of truth
    cdf = np.concatenate([[0], np.cumsum(0.5 * (p[1:] + p[:-1]) * np.diff(mcs))])
    cdf /= cdf[-1]
    q = float(np.interp(truth, mcs, cdf))
    peak = mcs[np.argmax(p)]
    return dict(peak=float(peak), mean=float(mean), sigma=float(sig),
                offset=float(mean - truth), offset_sigma=float((mean - truth) / sig),
                quantile=q)


def load_det(path):
    with open(path) as f:
        d = json.load(f)
    return d


def load_mc(cfgdir):
    f = os.path.join(cfgdir, "collated.txt")
    if not os.path.exists(f):
        return None
    d = np.loadtxt(f)
    if d.ndim == 1:
        d = d[None, :]
    # idx m1 m2 lnL sigma ntot neff
    m1, m2, lnL = d[:, 1], d[:, 2], d[:, 3]
    mc = (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2
    eta = (m1 * m2) / (m1 + m2) ** 2
    return mc, eta, lnL


def fig_consistency_det(det, tag):
    plt.figure(figsize=(7, 5))
    tbl = {}
    for c in CFGS:
        mc, eta, lnL = surface_arrays(det["configs"][c]["rows"])
        mcs, p = marg_mc(mc, lnL)
        s = summarize(mcs, p)
        tbl[c] = s
        plt.plot(mcs, p, "-o", color=COL[c], ms=3,
                 label="%s (peak=%.2f, sigma=%.2f, %.2f-sig off)"
                       % (c, s["peak"], s["sigma"], s["offset_sigma"]))
    plt.axvline(MC_TRUTH, color="green", ls="--", label="truth mc=%.2f" % MC_TRUTH)
    plt.xlabel("chirp mass  Mc  [Msun]")
    plt.ylabel("marginal posterior  p(Mc)  (flat prior)")
    plt.title("Deterministic time-marginalized lnL: IMR-consistency instrument (%s)" % tag)
    plt.legend(fontsize=8); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(os.path.join(FIG, "fig_consistency_det_%s.png" % tag), dpi=130)
    plt.close()
    return tbl


def fig_consistency_2d(det, tag):
    fig, axes = plt.subplots(1, 3, figsize=(13, 4), sharex=True, sharey=True)
    for ax, c in zip(axes, CFGS):
        mc, eta, lnL = surface_arrays(det["configs"][c]["rows"])
        mcs = np.unique(mc); etas = np.unique(eta)
        Z = np.full((len(etas), len(mcs)), np.nan)
        for m, e, l in zip(mc, eta, lnL):
            Z[np.argmin(abs(etas - e)), np.argmin(abs(mcs - m))] = l
        Zr = Z - np.nanmax(Z)
        cs = ax.contourf(mcs, etas, Zr, levels=np.linspace(-20, 0, 21), cmap="viridis", extend="min")
        ax.contour(mcs, etas, Zr, levels=[-4.5, -2.0, -0.5], colors="w", linewidths=0.8)
        ax.plot(MC_TRUTH, ETA_TRUTH, "r*", ms=14)
        ax.set_title("%s" % c); ax.set_xlabel("mchirp")
    axes[0].set_ylabel("eta")
    fig.colorbar(cs, ax=axes, label="lnL_marg - max", shrink=0.85)
    fig.suptitle("Segment lnL_marg contours (deterministic, %s)  -- red star = truth" % tag)
    plt.savefig(os.path.join(FIG, "fig_consistency_2d_%s.png" % tag), dpi=120)
    plt.close()


def fig_mc_vs_det(det, tag):
    plt.figure(figsize=(7, 5))
    for c in CFGS:
        mc, eta, lnL = surface_arrays(det["configs"][c]["rows"])
        mcs, p = marg_mc(mc, lnL)
        plt.plot(mcs, p / p.max(), "-", color=COL[c], label="%s det" % c)
        mcmc = load_mc(os.path.join(HERE, "zn", c))
        if mcmc is not None:
            mcm, etam, lNm = mcmc
            mcs2, p2 = marg_mc(mcm, lNm)
            plt.plot(mcs2, p2 / p2.max(), "o--", color=COL[c], ms=4, alpha=0.6,
                     label="%s MC (full-extr)" % c)
    plt.axvline(MC_TRUTH, color="green", ls="--")
    plt.xlabel("chirp mass  [Msun]"); plt.ylabel("marginal p(mc), peak-normalized")
    plt.title("MC full-extrinsic vs deterministic time-marg (%s)" % tag)
    plt.legend(fontsize=7); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(os.path.join(FIG, "fig_mc_vs_det_%s.png" % tag), dpi=130)
    plt.close()


def fig_pp(pp_path):
    with open(pp_path) as f:
        pp = json.load(f)
    qs = np.sort(np.array(pp["quantiles"])); n = len(qs)
    plt.figure(figsize=(5.5, 5.5))
    plt.plot([0, 1], [0, 1], "k--", label="uniform")
    plt.step(np.concatenate([[0], qs, [1]]),
             np.concatenate([[0], np.arange(1, n + 1) / n, [1]]), where="post",
             color="tab:red", label="ringdown-only (n=%d)" % n)
    # 1-sigma binomial band
    x = np.linspace(0, 1, 100)
    band = np.sqrt(x * (1 - x) / n)
    plt.fill_between(x, x - band, x + band, color="gray", alpha=0.25, label="1-sig")
    plt.xlabel("truth quantile in mchirp"); plt.ylabel("cumulative fraction")
    plt.title("pp-check: ringdown-only  (KS D=%.3f)" % pp["KS_D"])
    plt.legend(fontsize=8); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(os.path.join(FIG, "fig_pp.png"), dpi=130)
    plt.close()
    return pp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zn-det", default=os.path.join(HERE, "zn_det.json"))
    ap.add_argument("--noise-det", default=os.path.join(HERE, "noise_det.json"))
    ap.add_argument("--pp", default=os.path.join(HERE, "pp", "pp_results.json"))
    args = ap.parse_args()

    for tag, path in [("zeronoise", args.zn_det), ("noise", args.noise_det)]:
        if not os.path.exists(path):
            print("[skip] %s (%s missing)" % (tag, path)); continue
        det = load_det(path)
        tbl = fig_consistency_det(det, tag)
        fig_consistency_2d(det, tag)
        fig_mc_vs_det(det, tag)
        print("\n==== %s : deterministic 1D-marginal(mchirp) consistency table ====" % tag)
        print("%-9s %8s %8s %8s %10s %9s %9s" %
              ("config", "peak", "mean", "sigma", "off[Msun]", "off/sig", "quantile"))
        for c in CFGS:
            s = tbl[c]
            print("%-9s %8.3f %8.3f %8.3f %10.3f %9.2f %9.3f"
                  % (c, s["peak"], s["mean"], s["sigma"], s["offset"], s["offset_sigma"], s["quantile"]))
        # guess_snr per config (truth-ish point = closest grid pt to truth)
        print("guess_snr per config (grid point nearest truth):")
        for c in CFGS:
            rows = det["configs"][c]["rows"]
            j = int(np.argmin([abs(r["mchirp"] - MC_TRUTH) + abs(r["eta"] - ETA_TRUTH) for r in rows]))
            print("   %-9s guess_snr=%.3f (mc=%.3f eta=%.4f)"
                  % (c, rows[j]["guess_snr"], rows[j]["mchirp"], rows[j]["eta"]))

    if os.path.exists(args.pp):
        pp = fig_pp(args.pp)
        qs = np.array(pp["quantiles"])
        print("\n==== pp-check (ringdown-only) ====")
        print("n=%d  KS D=%.3f  mean quantile=%.3f  quantiles=%s"
              % (pp["n_inj"], pp["KS_D"], qs.mean(), np.round(qs, 3).tolist()))
    print("\n[figures] written to", FIG)


if __name__ == "__main__":
    sys.exit(main())
