#!/usr/bin/env python
"""make_figures.py -- G4 PoC money numbers + the three publication figures (steps 5-6).

Consumes the three run_three_way.sh output dirs (ref / unmit / marg), the scene products
(truth.json, blip_injected_L1.npy, glitch_basis_L1.npz, frames), and writes into --outdir:

    figures/g4poc_spectrogram.png    Q-scan strip: L1 data with blip | residual d - g_hat |
                                     glitch-free reference (whitened, same color scale)
    figures/g4poc_posteriors.png     grid-posterior credible contours (mchirp x eta),
                                     three configs overlaid, truth marked
    figures/g4poc_reconstruction.png g_hat vs the injected real blip (whitened TD)
    results_summary.json             peaks/widths/neff per config, overlap, Delta-lnZ,
                                     sigma_g, T6 diagnostic, wall-clock

Posterior summaries are GRID posteriors: exp(lnL_marg) on the 7x5 (mchirp, eta) grid
(uniform grid prior), spline-refined 20x; peak = argmax of the refined surface, widths =
marginal standard deviations.  This matches the T7-demo instrumentation (the object under
test is the lnL_marg surface, not a full production PE chain).
"""
import argparse
import glob
import json
import os
import sys

import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_ringdown_demo"))
import native_inject                                       # noqa: E402


def load_collated(run_dir):
    rows = np.loadtxt(os.path.join(run_dir, "collated.txt"))
    return dict(mc=rows[:, 3], eta=rows[:, 4], lnL=rows[:, 5], sigma=rows[:, 6],
                neff=rows[:, 7])


def grid_posterior_summary(dat, refine=20):
    """Spline-refined grid posterior peak + marginal widths in (mc, eta)."""
    from scipy.interpolate import RectBivariateSpline
    mcs = np.unique(dat["mc"]); etas = np.unique(dat["eta"])
    lnL = np.full((len(etas), len(mcs)), -np.inf)
    for m, e, l in zip(dat["mc"], dat["eta"], dat["lnL"]):
        lnL[np.argmin(np.abs(etas - e)), np.argmin(np.abs(mcs - m))] = l
    sp = RectBivariateSpline(etas, mcs, lnL, kx=3, ky=3)
    ef = np.linspace(etas[0], etas[-1], refine * len(etas))
    mf = np.linspace(mcs[0], mcs[-1], refine * len(mcs))
    Lf = sp(ef, mf)
    P = np.exp(Lf - Lf.max()); P /= P.sum()
    iy, ix = np.unravel_index(np.argmax(Lf), Lf.shape)
    p_mc = P.sum(axis=0); p_eta = P.sum(axis=1)
    mc_mean = float(np.sum(p_mc * mf)); mc_sd = float(np.sqrt(np.sum(p_mc * (mf - mc_mean) ** 2)))
    eta_mean = float(np.sum(p_eta * ef)); eta_sd = float(np.sqrt(np.sum(p_eta * (ef - eta_mean) ** 2)))
    return dict(mc_peak=float(mf[ix]), eta_peak=float(ef[iy]),
                mc_mean=mc_mean, mc_sd=mc_sd, eta_mean=eta_mean, eta_sd=eta_sd,
                lnL_max=float(Lf.max()), grid=(mf, ef, Lf))


def whiten_td(x, fs, f_lo=25.0, f_hi=1000.0):
    """Whiten a real strain segment with the demo analytic PSD (FD divide by ASD).

    SHAPE-ONLY whitening (arbitrary overall units): every series compared in the figures
    goes through this identical operation, so relative amplitudes are meaningful."""
    n = len(x)
    f = np.fft.rfftfreq(n, d=1.0 / fs)
    S = native_inject.analytic_psd(f)
    xt = np.fft.rfft(x)
    band = (f >= f_lo) & (f <= f_hi)
    w = np.zeros_like(xt)
    w[band] = xt[band] / np.sqrt(S[band])
    return np.fft.irfft(w, n=n)


def qscan(ax, x, fs, t0, t_evt, title, vmax=None):
    """gwpy Q-transform panel of a strain array (whitened externally or not)."""
    from gwpy.timeseries import TimeSeries
    ts = TimeSeries(x, dt=1.0 / fs, t0=t0)
    q = ts.q_transform(qrange=(4, 64), frange=(25, 512), whiten=False,
                       outseg=(t_evt - 0.35, t_evt + 0.2))
    tt = q.times.value - t_evt
    ff = q.frequencies.value
    im = ax.pcolormesh(tt, ff, q.value.T, cmap="viridis", vmin=0,
                       vmax=vmax, shading="auto", rasterized=True)
    ax.set_yscale("log"); ax.set_ylim(25, 512)
    ax.set_title(title, fontsize=10)
    ax.set_xlabel("time - merger [s]")
    return im, float(np.max(q.value))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", required=True, help="scene + run_{ref,unmit,marg} dir")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    W = args.workdir
    figdir = os.path.join(args.outdir, "figures")
    os.makedirs(figdir, exist_ok=True)
    truth = json.load(open(os.path.join(W, "truth.json")))
    fs = truth["srate"]

    # ------------------------------------------------ per-config summaries
    labels = ["ref", "unmit", "marg"]
    nice = {"ref": "reference (no glitch)", "unmit": "unmitigated (blip)",
            "marg": "glitch-marginalized (blip)"}
    res = {}
    for lab in labels:
        rd = os.path.join(W, "run_" + lab)
        dat = load_collated(rd)
        s = grid_posterior_summary(dat)
        s_out = {k: v for k, v in s.items() if k != "grid"}
        s_out["neff_median"] = float(np.median(dat["neff"]))
        s_out["neff_min"] = float(np.min(dat["neff"]))
        s_out["n_conv"] = int(np.sum(dat["neff"] >= 30))
        s_out["elapsed_s"] = float(open(os.path.join(rd, "elapsed_s.txt")).read())
        s_out["mc_offset_from_truth"] = s_out["mc_peak"] - truth["mc"]
        s_out["eta_offset_from_truth"] = s_out["eta_peak"] - truth["eta"]
        res[lab] = (s, s_out)

    # glitch diagnostics from the marg log ([G3] line) + T6 lines
    g3 = {}
    for line in open(os.path.join(W, "run_marg", "ile.log")):
        if "[G3]" in line and "sigma_g" in line:
            g3["line"] = line.strip()
            g3["sigma_g"] = float(line.split("sigma_g=")[1].split(",")[0])
            g3["delta_lnZ"] = float(line.split("Delta_lnZ=")[1].split()[0])
        if "[T6]" in line and "[L1]" in line:
            g3.setdefault("t6_L1", line.strip())
            g3["t6_rel_var_L1"] = float(line.split("rel_var=")[1].split()[0])

    # ------------------------------------------------ reconstruction overlap (context-level)
    import lal
    import RIFT.lalsimutils as lsu
    from RIFT.likelihood.td_inner_product import TDInnerProductFactory
    psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0.), 0., 0.125,
                                         lsu.lsu_HertzUnit, 8193)
    fpsd = np.arange(8193) * 0.125
    psd.data.data = native_inject.analytic_psd(fpsd)
    d_td = lsu.frame_data_to_hoft(os.path.join(W, "frames_glitch.cache"),
                                  "L1:FAKE-STRAIN", verbose=False)
    ts = lal.CreateCOMPLEX16TimeSeries("x", d_td.epoch, 0., d_td.deltaT,
                                       lsu.lsu_DimensionlessUnit, d_td.data.length)
    ts.data.data = np.asarray(d_td.data.data, dtype=complex)
    data_fd = lsu.DataFourier(ts)
    fac = TDInnerProductFactory(solver="auto",
                                glitch_basis_files={"L1": os.path.join(W, "glitch_basis_L1.npz")},
                                glitch_sigma="profile")
    fac.register_detector_data("L1", psd, data_fd, event_time_geo=truth["tref"])
    ctx = fac.context_for(25.0, 1000.0, fs / 2.0, data_fd.deltaF, psd, analyticPSD_Q=False)
    ghat = np.real(ctx.glitch_reconstruction_td())
    inj = np.load(os.path.join(W, "blip_injected_L1.npy"))
    num = np.real(ctx.dot_td(ghat, inj))
    den = np.sqrt(np.real(ctx.dot_td(ghat, ghat)) * np.real(ctx.dot_td(inj, inj)))
    overlap = float(num / den)
    g3["recon_whitened_overlap"] = overlap
    g3["sigma_g_ctx"] = float(ctx._glitch_sigma_g)
    g3["delta_lnZ_ctx"] = float(ctx.glitch_delta_lnZ)

    # ------------------------------------------------ figure (a): Q-scan strip
    d_gl = np.asarray(d_td.data.data)
    d_ref = np.asarray(lsu.frame_data_to_hoft(os.path.join(W, "frames_ref.cache"),
                                              "L1:FAKE-STRAIN", verbose=False).data.data)
    t0 = float(d_td.epoch)
    t_evt = truth["t_merger_L1"]
    w_gl = whiten_td(d_gl, fs)
    w_res = whiten_td(d_gl - ghat, fs)
    w_ref = whiten_td(d_ref, fs)
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.4), sharey=True,
                             constrained_layout=True)
    _, vmax = qscan(axes[0], w_gl, fs, t0, t_evt, "L1 data: signal + real blip")
    qscan(axes[1], w_res, fs, t0, t_evt,
          r"after marginalization: $d - \hat{g}$", vmax=vmax)
    qscan(axes[2], w_ref, fs, t0, t_evt, "glitch-free reference", vmax=vmax)
    axes[0].set_ylabel("frequency [Hz]")
    fig.suptitle("G4 PoC: real O3 blip overlapping a 35+35 merger in L1 "
                 "(normalized Q-scan energy, common scale)", fontsize=11)
    fig.savefig(os.path.join(figdir, "g4poc_spectrogram.png"), dpi=200)
    plt.close(fig)

    # ------------------------------------------------ figure (b): posterior contours
    fig, ax = plt.subplots(figsize=(6.4, 5.0), constrained_layout=True)
    colors = {"ref": "tab:green", "unmit": "tab:red", "marg": "tab:blue"}
    ls = {"ref": "-", "unmit": "--", "marg": "-"}
    for lab in labels:
        mf, ef, Lf = res[lab][0]["grid"]
        P = np.exp(Lf - Lf.max()); P /= P.sum()
        # credible levels from the sorted-density construction
        ps = np.sort(P.ravel())[::-1]
        cs = np.cumsum(ps)
        lev = [ps[np.searchsorted(cs, c)] for c in (0.9, 0.5)]
        ax.contour(mf, ef, P, levels=sorted(lev), colors=colors[lab],
                   linestyles=ls[lab], linewidths=1.6)
        ax.plot([], [], color=colors[lab], ls=ls[lab], label=nice[lab])
        ax.plot(res[lab][1]["mc_peak"], res[lab][1]["eta_peak"], marker="o",
                color=colors[lab], ms=5)
    ax.plot(truth["mc"], truth["eta"], marker="*", color="k", ms=16, ls="none",
            label="truth (35+35)")
    ax.set_xlabel(r"$\mathcal{M}_c\ [M_\odot]$"); ax.set_ylabel(r"$\eta$")
    ax.legend(loc="lower left", fontsize=9)
    ax.set_title("Grid posterior (50/90% credible), three configurations", fontsize=11)
    fig.savefig(os.path.join(figdir, "g4poc_posteriors.png"), dpi=200)
    plt.close(fig)

    # ------------------------------------------------ figure (c): g_hat vs injected blip
    dt = 1.0 / fs
    i_c = int(round((truth["blip_center_gps"] - t0) / dt))
    n_h = int(0.06 * fs)
    sl = slice(i_c - n_h, i_c + n_h)
    trel = (np.arange(len(inj)) * dt + t0 - truth["blip_center_gps"])[sl] * 1e3
    w_inj = whiten_td(inj, fs)[sl]
    w_ghat = whiten_td(ghat, fs)[sl]
    fig, ax = plt.subplots(figsize=(6.8, 3.6), constrained_layout=True)
    ax.plot(trel, w_inj, color="k", lw=1.4,
            label="injected real blip (L1 O3a, held out of PCA)")
    ax.plot(trel, w_ghat, color="tab:blue", lw=1.4, ls="--",
            label=r"marginalization reconstruction $\hat{g}$ (r=%d)" % truth["basis_r"])
    ax.set_xlabel("time - blip centre [ms]")
    ax.set_ylabel("whitened strain [arb.]")
    ax.legend(fontsize=9)
    ax.set_title("Whitened overlap $\\langle \\hat{g}, g_{\\rm inj}\\rangle$ = %.3f"
                 % overlap, fontsize=11)
    fig.savefig(os.path.join(figdir, "g4poc_reconstruction.png"), dpi=200)
    plt.close(fig)

    # ------------------------------------------------ summary json + table
    summary = dict(truth={k: truth[k] for k in ("mc", "eta", "m1", "m2", "dist",
                                                "blip_snr", "blip_offset_s",
                                                "blip_gps_o3", "basis_r")},
                   glitch=g3,
                   configs={lab: res[lab][1] for lab in labels})
    with open(os.path.join(args.outdir, "results_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps(summary, indent=2))
    print("\n| config | mc peak | d(mc) | sigma(mc) | eta peak | d(eta) | sigma(eta) "
          "| lnL max | neff med | wall [s] |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for lab in labels:
        r = res[lab][1]
        print("| %s | %.3f | %+.3f | %.3f | %.4f | %+.4f | %.4f | %.2f | %.0f | %.0f |"
              % (nice[lab], r["mc_peak"], r["mc_offset_from_truth"], r["mc_sd"],
                 r["eta_peak"], r["eta_offset_from_truth"], r["eta_sd"],
                 r["lnL_max"], r["neff_median"], r["elapsed_s"]))


if __name__ == "__main__":
    main()
