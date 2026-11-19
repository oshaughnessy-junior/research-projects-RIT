"""The frozen-round estimate, its checks, and the rule that sizes the round.

Every draw of the frozen proposal counts: a draw outside the prior support or with a
failed evaluation has weight zero.  The estimate is the importance-weighted one over
those draws.  It is reported with its effective sample size, the tail index of the
weights, and the agreement between the streams.
"""
import numpy as np
from scipy import special

from .space import PARAMS, summaries


def weighted_quantile(x, w, qs):
    o = np.argsort(x)
    c = np.cumsum(w[o])
    return np.interp(qs, (c - 0.5 * w[o]) / c[-1], x[o])


def pareto_k(w):
    """Tail index of the largest weights (Zhang and Stephens 2009, as used for PSIS)."""
    w = np.sort(np.asarray(w, dtype=float))
    w = w[w > 0]
    m = int(min(0.2 * len(w), 3 * np.sqrt(len(w))))
    if m < 5:
        return float("nan")
    x = w[-m:] - w[-m - 1]
    x = x[x > 0]
    n = len(x)
    if n < 5:
        return float("nan")
    prior, mgrid = 3.0, 30 + int(np.sqrt(n))
    b = 1.0 - np.sqrt(mgrid / (np.arange(1, mgrid + 1) - 0.5))
    b = b / (prior * x[int(n / 4 + 0.5) - 1]) + 1.0 / x[-1]
    k = np.log1p(-b[:, None] * x).mean(axis=1)
    L = n * (np.log(-b / k) - k - 1.0)
    wt = np.exp(-special.logsumexp(L - L[:, None], axis=1))
    bhat = np.sum(b * wt)
    khat = np.log1p(-bhat * x).mean()
    return float((n * khat + 10 * 0.5) / (n + 10))


def estimate(ln_w, params, n_total=None):
    """ln_w: ln weights of the rows with non-zero weight.  n_total counts every draw.
    Returns ln Z (relative to the offset in ln_w), its error, n_eff, the tail index and,
    per parameter, mean, sd, W90 and the standard error of the mean."""
    ln_w = np.asarray(ln_w, dtype=float)
    n_total = n_total or len(ln_w)
    top = ln_w.max()
    w = np.exp(ln_w - top)
    n_eff = w.sum() ** 2 / np.sum(w ** 2)
    out = dict(lnZ=float(top + np.log(w.sum() / n_total)), lnZ_err=float(np.sqrt(max(1 / n_eff - 1 / n_total, 0))),
               n_eff=float(n_eff), n_total=int(n_total), n_evaluated=int(len(w)), k_hat=pareto_k(w), params={})
    wn = w / w.sum()
    for name, x in params.items():
        mean = float(np.sum(wn * x))
        sd = float(np.sqrt(np.sum(wn * (x - mean) ** 2)))
        q5, q95 = weighted_quantile(x, wn, [0.05, 0.95])
        out["params"][name] = dict(mean=mean, sd=sd, w90=float(q95 - q5),
                                   se_mean=float(np.sqrt(np.sum(wn ** 2 * (x - mean) ** 2))))
    return out


def chi2(values, errors):
    v, e = np.asarray(values), np.asarray(errors)
    mean = np.sum(v / e ** 2) / np.sum(1 / e ** 2)
    return float(np.sum(((v - mean) / e) ** 2))


def frozen_estimate(cfg, pl, lnl):
    """Estimate over a complete frozen plan, its stream groups, and the stop decision.

    pl: the plan (stream, grid, ok, ln_pq).  lnl: ln L minus the offset, aligned with
    the plan, -inf where the evaluation failed."""
    sel = pl["ok"] & np.isfinite(lnl)
    par = summaries(pl["grid"][sel])
    ln_w = lnl[sel] + pl["ln_pq"][sel]
    est = estimate(ln_w, par, n_total=len(sel))
    stream = pl["stream"][sel]
    per = len(sel) // cfg["n_streams"]
    groups = [estimate(ln_w[stream == s], {k: v[stream == s] for k, v in par.items()}, n_total=per)
              for s in range(cfg["n_streams"])]
    c2 = dict(lnZ=chi2([g["lnZ"] for g in groups], [g["lnZ_err"] for g in groups]),
              **{k: chi2([g["params"][k]["mean"] for g in groups], [g["params"][k]["se_mean"] for g in groups])
                 for k in PARAMS})
    checks = dict(n_eff=est["n_eff"] >= cfg["target_n_eff"], lnZ_err=est["lnZ_err"] <= cfg["lnz_tol"],
                  tail=bool(est["k_hat"] < cfg["k_max"]), groups=max(c2.values()) < cfg["chi2_max"])
    eff = est["n_eff"] / est["n_total"]
    more = (cfg["target_n_eff"] - est["n_eff"]) / eff if not checks["n_eff"] else 0.25 * est["n_total"]
    est["lnZ"] += cfg["lnl_offset"]
    for g in groups:
        g["lnZ"] += cfg["lnl_offset"]
    return dict(estimate=est, groups=groups, group_chi2=c2, checks={k: bool(v) for k, v in checks.items()},
                stop=bool(all(checks.values())), efficiency=float(eff), forecast_more=float(more),
                failed_rows=int((pl["ok"] & ~np.isfinite(lnl)).sum()), outside_support=int((~pl["ok"]).sum()))


def extend(cfg, per_stream, more):
    """Draws per stream after one extension.

    The forecast `more` is the number of further draws that would reach the target at
    the measured efficiency.  One heavy draw can cut the measured efficiency several
    fold, so a step is limited to `extension_factor` times the round's present size."""
    step = int(np.ceil(cfg["extension_margin"] * more / cfg["n_streams"]))
    cap = int(np.ceil((cfg["extension_factor"] - 1.0) * per_stream)) if cfg.get("extension_factor") else step
    step = max(cfg["min_extension"], min(step, cap))
    return int(min(cfg["max_frozen_per_stream"], per_stream + step))
