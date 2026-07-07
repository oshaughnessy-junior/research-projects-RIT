#!/usr/bin/env python
"""pp_run.py -- reduced-scope pp-check for the ringdown-only (config b) TD-segment likelihood.

For each of N injections it (i) draws a truth (mchirp, eta, distance, sky, time, inclination,
phase) modestly around the fiducial 50+50 BBH, (ii) writes zero-noise frames (util_LALWriteFrame)
and adds an independent PSD-colored Gaussian-noise realization, (iii) evaluates the DETERMINISTIC
ringdown-only (template start = peak+10M) time-marginalized network lnL over the FIXED intrinsic
(mchirp,eta) grid at the injection's true extrinsics, (iv) forms the grid posterior
P(mc,eta) ∝ exp(lnL_marg) (flat prior), marginalizes eta -> P(mc), and records the truth-mchirp
quantile.  A KS test of the quantiles vs Uniform(0,1) is the uniformity check.

SCOPE (stated honestly): the marginalization here is over TIME (deterministic quadrature) at the
injection's KNOWN sky/distance/inclination/phase.  The full-extrinsic Monte-Carlo NoLoop path
under-converges (neff~1) at these SNRs in this cheap smooth-PSD demo (a pre-existing GMM-sampler
limitation, not a TD-segment bug), so a full-extrinsic MC pp-check of this many injections is not
affordable/trustworthy here.  This deterministic, known-extrinsics pp-check still exercises the
ringdown-only SEGMENT likelihood (the novel object) end-to-end on noisy data with a scattering
peak, and detects gross miscalibration.  With n~15 only gross failures are detectable.

Usage: pp_run.py --n-inj 15 --grid ppgrid.xml.gz --out pp/  [--seed0 7000]
"""
import os, sys, json, argparse, subprocess, glob
import numpy as np
import lal
import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.td_inner_product import TDInnerProductFactory

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_equivalence"))
import run_equivalence_study as t4
PY = sys.executable
BIN = os.path.abspath(os.path.join(HERE, "..", "..", "bin"))
MTSUN = lal.MTSUN_SI


def analytic_psd(f):
    fsafe = np.maximum(np.abs(f), 1.0)
    return 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)


def mc_eta_to_m1m2(mc, eta):
    eta = min(eta, 0.25)
    Mtot = mc / (eta ** 0.6)
    delta = np.sqrt(max(0.0, 1.0 - 4.0 * eta))
    return Mtot * (1 + delta) / 2, Mtot * (1 - delta) / 2


def mc_eta(m1, m2):
    return (lalsimutils.mchirp(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN,
            lalsimutils.symRatio(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN))


def write_injection(fn, m1, m2, dist, incl, phiref, theta, phi, psi, tref, fmin=25.0):
    cmd = [PY, os.path.join(BIN, "util_WriteInjectionFile.py"),
           "--parameter", "m1", "--parameter-value", "%.6f" % m1,
           "--parameter", "m2", "--parameter-value", "%.6f" % m2,
           "--parameter", "dist", "--parameter-value", "%.4f" % dist,
           "--parameter", "fmin", "--parameter-value", "%.3f" % fmin,
           "--parameter", "incl", "--parameter-value", "%.5f" % incl,
           "--parameter", "phiref", "--parameter-value", "%.5f" % phiref,
           "--parameter", "theta", "--parameter-value", "%.5f" % theta,
           "--parameter", "phi", "--parameter-value", "%.5f" % phi,
           "--parameter", "psi", "--parameter-value", "%.5f" % psi,
           "--parameter", "tref", "--parameter-value", "%.6f" % tref,
           "--approximant", "IMRPhenomD", "--fname", fn]
    subprocess.run(cmd, check=True, capture_output=True)


def write_frames(injfn, outdir, start=999999984, seglen=32):
    os.makedirs(outdir, exist_ok=True)
    for g in glob.glob(os.path.join(outdir, "*.gwf")):
        os.remove(g)
    injabs = os.path.abspath(injfn + ".xml.gz")
    for ifo in ("H1", "L1"):
        subprocess.run([PY, os.path.join(BIN, "util_LALWriteFrame.py"),
                        "--inj", injabs, "--event", "0", "--instrument", ifo,
                        "--srate", "2048", "--seglen", str(seglen), "--start", str(start),
                        "--approx", "IMRPhenomD"], check=True, capture_output=True, cwd=outdir)
    # add noise + build cache
    cache = os.path.join(outdir, "frames.cache")
    lines = []
    return outdir


def add_noise_and_cache(framedir, seed, cache_path, fmin=25.0, fmax=1000.0):
    rng = np.random.default_rng(seed)
    lines = []
    for f in sorted(glob.glob(os.path.join(framedir, "*.gwf"))):
        b = os.path.basename(f)
        ifo = "H1" if b[0] == "H" else "L1"
        # load, add noise, rewrite
        tmp_cache = f + ".cache"
        # parse start,dur from name X-...-<start>-<dur>.gwf
        parts = b[:-4].split("-")
        start, dur = int(parts[-2]), int(parts[-1])
        with open(tmp_cache, "w") as fh:
            fh.write("%s FAKE %d %d file://localhost%s\n" % (ifo[0], start, dur, os.path.abspath(f)))
        hoft = lalsimutils.frame_data_to_hoft(tmp_cache, ifo + ":FAKE-STRAIN", verbose=False)
        N = hoft.data.length; dt = hoft.deltaT; df = (1.0/dt)/N
        fr = np.fft.rfftfreq(N, d=dt); S = analytic_psd(fr)
        band = (fr >= fmin) & (fr <= fmax); sig = np.zeros_like(fr)
        sig[band] = np.sqrt(S[band]/(4.0*df))
        nt = (rng.standard_normal(len(fr)) + 1j*rng.standard_normal(len(fr)))*sig
        nt[0] = nt[0].real
        if N % 2 == 0: nt[-1] = nt[-1].real
        noise = np.fft.irfft(nt, n=N)*(N*df)
        hoft.data.data[:] = np.asarray(hoft.data.data) + noise
        lalsimutils.hoft_to_frame_data(f, ifo + ":FAKE-STRAIN", hoft)  # overwrite in place
        lines.append("%s FAKE %d %d file://localhost%s\n" % (ifo[0], start, dur, os.path.abspath(f)))
        os.remove(tmp_cache)
    with open(cache_path, "w") as fh:
        fh.writelines(lines)
    return cache_path


def ringdown_surface(cache, Plist, dist, incl, phiref, theta, phi, psi, tref,
                     peak_start, psd_files, fmin=25.0, fmax=1000.0, srate=2048.0,
                     lmax=2, t_window=0.075, start=999999996.0, stop=1000000004.0):
    deltaT = 1.0/srate
    channels = {"H1": "FAKE-STRAIN", "L1": "FAKE-STRAIN"}
    data_dict, psd_dict, fmax_psd = t4.load_data_and_psd(cache, channels, psd_files, start, stop, deltaT)
    deltaF = data_dict["H1"].deltaF
    fmx = min(fmax, fmax_psd)
    tvals = np.linspace(-t_window, t_window, int(2*t_window/deltaT))
    # ONE factory reused across all grid points (memoized dense context built once).
    fac = TDInnerProductFactory(solver="dense", template_start_offset=peak_start, template_end_offset=None)
    out = []
    for P in Plist:
        m1 = P.m1/lalsimutils.lsu_MSUN; m2 = P.m2/lalsimutils.lsu_MSUN
        mc, eta = mc_eta(m1, m2)
        Pp = t4.make_P(m1, m2, fmin, fmin, tref, deltaT, deltaF, dist_mpc=dist,
                       incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)
        pc = t4.precompute(Pp, data_dict, psd_dict, fmx, lmax, False, 0.0, IP_factory=fac)
        ri, ct, ctv, rho, snr, _ = pc
        Pe = t4.make_P(m1, m2, fmin, fmin, tref, deltaT, deltaF, dist_mpc=dist,
                       incl=incl, phiref=phiref, theta=theta, phi=phi, psi=psi)
        lm = t4.lnL_marg_time(Pe, ri, rho, ct, ctv, lmax, tvals)
        out.append((mc, eta, lm))
    return out


def mc_quantile(rows, mc_true):
    """Marginalize eta -> P(mc); return CDF(mc_true) via linear interpolation of the CDF."""
    rows = np.array(rows)  # (mc, eta, lnL)
    mcs = np.unique(rows[:, 0])
    pm = []
    for mc in mcs:
        sel = rows[np.isclose(rows[:, 0], mc)]
        lnl = sel[:, 2]
        pm.append(np.sum(np.exp(lnl - lnl.max())) * np.exp(lnl.max()))
    pm = np.array(pm)
    pm = pm / np.trapz(pm, mcs)
    cdf = np.concatenate([[0.0], np.cumsum(0.5*(pm[1:]+pm[:-1])*np.diff(mcs))])
    cdf = cdf / cdf[-1]
    return float(np.interp(mc_true, mcs, cdf)), mcs, pm


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-inj", type=int, default=15)
    ap.add_argument("--grid", default=os.path.join(HERE, "ppgrid.xml.gz"))
    ap.add_argument("--out", default=os.path.join(HERE, "pp"))
    ap.add_argument("--seed0", type=int, default=7000)
    ap.add_argument("--peak-start-fac", type=float, default=10.0, help="template start = peak + this*M [xM]")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    psd_files = {"H1": os.path.join(HERE, "H1-psd.xml.gz"), "L1": os.path.join(HERE, "L1-psd.xml.gz")}
    Plist = lalsimutils.xml_to_ChooseWaveformParams_array(str(args.grid))
    tref0 = 1000000000.0
    rng = np.random.default_rng(args.seed0)
    recs = []
    for i in range(args.n_inj):
        # draw truth modestly around fiducial (mc=43.53, eta=0.25 fiducial 50+50)
        mc_t = rng.uniform(42.6, 44.5)
        eta_t = rng.uniform(0.225, 0.2498)
        m1, m2 = mc_eta_to_m1m2(mc_t, eta_t)
        dist = rng.uniform(600.0, 820.0)
        incl = rng.uniform(0.4, 1.0); phiref = rng.uniform(0.8, 1.6)
        theta = rng.uniform(0.1, 0.6); phi = rng.uniform(1.7, 2.5); psi = rng.uniform(0.5, 1.1)
        tref = tref0 + rng.uniform(-0.004, 0.004)
        Mtot = m1 + m2
        peak_start = args.peak_start_fac * MTSUN * Mtot   # peak+10M in seconds
        injfn = os.path.join(args.out, "inj_%02d" % i)
        write_injection(injfn, m1, m2, dist, incl, phiref, theta, phi, psi, tref)
        fdir = os.path.join(args.out, "frames_%02d" % i)
        write_frames(injfn, fdir)
        cache = add_noise_and_cache(fdir, args.seed0 + 1000 + i, os.path.join(fdir, "frames.cache"))
        rows = ringdown_surface(cache, Plist, dist, incl, phiref, theta, phi, psi, tref, peak_start, psd_files)
        q, mcs, pm = mc_quantile(rows, mc_t)
        recs.append({"i": i, "mc_true": mc_t, "eta_true": eta_t, "dist": dist,
                     "quantile_mc": q, "surface": rows})
        print("[pp %02d] mc_true=%.3f eta=%.4f dist=%.0f -> mc-quantile=%.3f" % (i, mc_t, eta_t, dist, q))
        sys.stdout.flush()
    qs = np.array([r["quantile_mc"] for r in recs])
    # KS vs uniform
    qs_sorted = np.sort(qs); n = len(qs)
    ecdf = np.arange(1, n+1)/n
    Dp = np.max(ecdf - qs_sorted); Dm = np.max(qs_sorted - (np.arange(0, n)/n))
    D = max(Dp, Dm)
    results = {"n_inj": n, "quantiles": qs.tolist(), "KS_D": float(D),
               "records": [{k: v for k, v in r.items() if k != "surface"} for r in recs]}
    with open(os.path.join(args.out, "pp_results.json"), "w") as fh:
        json.dump(results, fh, indent=2)
    print("\n[pp] n=%d  KS D=%.3f  mean quantile=%.3f (expect ~0.5)  quantiles=%s"
          % (n, D, qs.mean(), np.round(qs, 3).tolist()))


if __name__ == "__main__":
    sys.exit(main())
