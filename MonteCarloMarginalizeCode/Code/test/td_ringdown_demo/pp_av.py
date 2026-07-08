#!/usr/bin/env python
"""pp_av.py -- ONE properly extrinsic-MARGINALIZED (AV) pp injection for the ringdown-only
TD-segment likelihood.

Unlike pp_one.py (deterministic time-marg at KNOWN/fixed extrinsics -- a conditional/profiled
surface that is NOT a Bayesian posterior and need not be calibrated), this driver runs the REAL
ILE with the Adaptive-Volume (AV) extrinsic integrator, which marginalizes distance / sky /
inclination / phase / polarization / time with their proper priors -- a genuine marginal
posterior over the (mc,eta) grid.  q_mc, q_eta are the truth quantiles of the 1D grid marginals.

Steps: (1) draw truth from SEED over the grid box (prior = injection = flat over the box);
(2) native-inject into fresh colored noise; (3) run ILE --sampler-method AV --time-marginalization
ringdown config over the grid (one batched call); (4) read the per-point marginalized lnL +
n_eff; (5) form flat-prior grid posterior, marginalize to P(mc)/P(eta), record q of truth and the
min/median n_eff.
"""
import os, sys, json, time, argparse, subprocess
import numpy as np
import lal
import RIFT.lalsimutils as lalsimutils
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import native_inject
MTSUN = lal.MTSUN_SI
PY = sys.executable
ILE = os.path.abspath(os.path.join(HERE, "..", "..", "bin", "integrate_likelihood_extrinsic_batchmode"))


def mc_eta_to_m1m2(mc, eta):
    eta = min(eta, 0.25)
    Mtot = mc / (eta ** 0.6)
    delta = np.sqrt(max(0.0, 1.0 - 4.0 * eta))
    return Mtot * (1 + delta) / 2, Mtot * (1 - delta) / 2


def mc_eta(m1, m2):
    return (lalsimutils.mchirp(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN) / lalsimutils.lsu_MSUN,
            lalsimutils.symRatio(m1 * lalsimutils.lsu_MSUN, m2 * lalsimutils.lsu_MSUN))


def quantile_of_truth(vals, ln, x):
    order = np.argsort(vals); v = np.asarray(vals)[order]; ln = np.asarray(ln)[order]
    P = np.exp(ln - ln.max())
    if v.size < 2:
        return float("nan")
    area = np.trapz(P, v)
    if area <= 0:
        return float("nan")
    P = P / area
    cdf = np.concatenate([[0.0], np.cumsum(0.5 * (P[1:] + P[:-1]) * np.diff(v))])
    cdf = cdf / cdf[-1]
    return float(np.interp(x, v, cdf))


def marg_q(mcs, etas, lnls, mc_t, eta_t):
    mcs = np.array(mcs); etas = np.array(etas); lnls = np.array(lnls)
    mc_u = np.unique(np.round(mcs, 6)); eta_u = np.unique(np.round(etas, 6))
    lnmc = [ (lambda s: s.max()+np.log(np.sum(np.exp(s-s.max()))))(lnls[np.isclose(mcs, mc)]) for mc in mc_u ]
    lneta = [ (lambda s: s.max()+np.log(np.sum(np.exp(s-s.max()))))(lnls[np.isclose(etas, et)]) for et in eta_u ]
    return quantile_of_truth(mc_u, lnmc, mc_t), quantile_of_truth(eta_u, lneta, eta_t)


def main():
    t0 = time.time()
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--grid", default=os.path.join(HERE, "avgrid.xml.gz"))
    ap.add_argument("--npts", type=int, default=45)
    ap.add_argument("--nmax", type=int, default=100000)
    ap.add_argument("--neff", type=int, default=3000)
    ap.add_argument("--start", type=float, default=999999996.0)
    ap.add_argument("--seglen", type=float, default=8.0)
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--tref0", type=float, default=1000000000.0)
    ap.add_argument("--lmax", type=int, default=2)
    ap.add_argument("--mc-inj-lo", type=float, default=42.0)
    ap.add_argument("--mc-inj-hi", type=float, default=45.0)
    ap.add_argument("--eta-inj-lo", type=float, default=0.225)
    ap.add_argument("--eta-inj-hi", type=float, default=0.25)
    ap.add_argument("--d-min", type=float, default=300.0)
    ap.add_argument("--d-max", type=float, default=1100.0)
    ap.add_argument("--config", default="ringdown", choices=["ringdown", "full"])
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    # ---- draw (mc,eta) from the flat grid-box prior; draw extrinsics from the SAME priors
    #      the ILE marginalizes over (volumetric distance, isotropic sky+inclination, uniform
    #      psi/phase) so the (mc,eta) marginal pp is rigorous (prior == injection distribution).
    rng = np.random.default_rng(args.seed)
    mc_t = float(rng.uniform(args.mc_inj_lo, args.mc_inj_hi))
    eta_t = float(rng.uniform(args.eta_inj_lo, args.eta_inj_hi))
    m1, m2 = mc_eta_to_m1m2(mc_t, eta_t)
    mc_t, eta_t = mc_eta(m1, m2)
    dlo, dhi = args.d_min, args.d_max
    dist = float((rng.uniform(0, 1) * (dhi ** 3 - dlo ** 3) + dlo ** 3) ** (1.0 / 3.0))  # volumetric
    incl = float(np.arccos(rng.uniform(-1.0, 1.0)))          # isotropic (uniform in cos)
    phiref = float(rng.uniform(0.0, 2 * np.pi))              # uniform phase
    theta = float(np.arcsin(rng.uniform(-1.0, 1.0)))         # DEC, isotropic (uniform in sin)
    phi = float(rng.uniform(0.0, 2 * np.pi))                 # RA, uniform
    psi = float(rng.uniform(0.0, 2 * np.pi))                 # polarization, uniform
    tref = float(args.tref0 + rng.uniform(-0.01, 0.01))
    peak_start = 10.0 * MTSUN * (m1 + m2)

    framedir = os.path.join(args.outdir, "frames"); cache = os.path.join(args.outdir, "frames.cache")
    native_inject.native_inject(m1, m2, dist, incl, phiref, theta, phi, psi, tref, framedir, cache,
        instruments=("H1", "L1"), fmin=args.fmin, fref=args.fmin, srate=args.srate,
        seglen=args.seglen, start=args.start, noise_seed=args.seed, fmax=args.fmax,
        lmax=args.lmax, verbose=False)

    outpt = os.path.join(args.outdir, "pt")
    cmd = [PY, ILE, "-c", cache, "-C", "H1=FAKE-STRAIN", "-C", "L1=FAKE-STRAIN",
           "-p", "H1=" + os.path.join(HERE, "H1-psd.xml.gz"),
           "-p", "L1=" + os.path.join(HERE, "L1-psd.xml.gz"),
           "-I", args.grid, "--event", "0", "--n-events-to-analyze", str(args.npts),
           "--event-time", "1000000000.0",
           "--data-start-time", "999999996", "--data-end-time", "1000000004",
           "--srate", "2048", "--fmin-template", "25", "--fmax", "1000", "--reference-freq", "25",
           "--l-max", "2", "--approximant", "IMRPhenomD",
           "--time-marginalization", "--vectorized", "--gpu", "--force-xpy",
           "--internal-use-lnL", "--sampler-method", "AV",
           "--d-min", str(args.d_min), "--d-max", str(args.d_max),
           "--n-max", str(args.nmax), "--n-eff", str(args.neff), "--n-chunk", "10000",
           "--seed", str(args.seed),
           "--internal-ile-inner-product", "td-toeplitz", "--td-solver", "auto",
           "-o", outpt]
    if args.config == "ringdown":
        cmd += ["--td-analysis-start", "%.12g" % float(peak_start)]
    logf = os.path.join(args.outdir, "ile.log")
    with open(logf, "w") as lf:
        rc = subprocess.call(cmd, stdout=lf, stderr=subprocess.STDOUT, env={**os.environ, "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS", "2")})

    import glob
    mcs, etas, lnls, neffs = [], [], [], []
    for f in sorted(glob.glob(outpt + "_*_.dat"), key=lambda p: int(p.split("_")[-2])):
        if os.path.getsize(f) == 0:
            continue
        d = np.loadtxt(f)
        gm1, gm2, lnL, neff = d[1], d[2], d[9], d[12]
        mc_g, eta_g = mc_eta(gm1, gm2)
        mcs.append(mc_g); etas.append(eta_g); lnls.append(lnL); neffs.append(neff)
    ok = len(lnls) >= 4
    q_mc, q_eta = (marg_q(mcs, etas, lnls, mc_t, eta_t) if ok else (float("nan"), float("nan")))
    out = {"seed": int(args.seed), "config": args.config, "mc_true": mc_t, "eta_true": eta_t,
           "dist": dist, "q_mc": q_mc, "q_eta": q_eta,
           "n_pts": len(lnls), "neff_min": float(np.min(neffs)) if neffs else 0.0,
           "neff_med": float(np.median(neffs)) if neffs else 0.0,
           "neff_max": float(np.max(neffs)) if neffs else 0.0,
           "ile_rc": rc, "wall_s": time.time() - t0}
    json.dump(out, open(os.path.join(args.outdir, "pp_av_seed%d.json" % args.seed), "w"), indent=2)
    if not args.keep:
        for g in glob.glob(os.path.join(framedir, "*.gwf")):
            os.remove(g)
        try:
            os.rmdir(framedir)
        except OSError:
            pass
        for g in glob.glob(outpt + "_*_.dat"):
            os.remove(g)
        if os.path.exists(cache):
            os.remove(cache)
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
