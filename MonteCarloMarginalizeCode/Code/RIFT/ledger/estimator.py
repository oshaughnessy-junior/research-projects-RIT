"""The frozen-round estimate, its checks, and the rule that sizes the round.

Every draw of the frozen proposal counts: a draw outside the prior support or with a
failed evaluation has weight zero.  The estimate is the importance-weighted one over
those draws.  It is reported with its effective sample size, the tail index of the
weights, and the agreement between the streams.

The weights are used raw, and a heavy tail makes ln Z read low: a sample of n draws
from weights of tail index k typically misses a fraction of the evidence that falls
only as n^-(1-k).  `lnZ_tail` estimates that fraction from the generalized Pareto
fitted to the largest weights.  It is reported beside the error and is not a check:
in simulation, stopping on it (or on a stricter tail-index line, or on Pareto-smoothed
weights) selected the samples without a heavy draw and made the certified ln Z worse.
Read the certificate as ln Z within [-2 err, +2 err + lnZ_tail] of the value; that
interval held in 91 to 97% of certified runs for true tail indices up to 0.6
(`m0_delivery/tail_coverage.py` in the paper repository).
"""
import numpy as np
from scipy import special

from .space import PARAMS, summaries


def weighted_quantile(x, w, qs):
    o = np.argsort(x)
    c = np.cumsum(w[o])
    return np.interp(qs, (c - 0.5 * w[o]) / c[-1], x[o])


def tail_fit(w):
    """Generalized-Pareto fit to the largest weights (Zhang and Stephens 2009, with the
    weak prior of PSIS on the index).

    Returns (k, unseen): the tail index, and the expected weight per positive draw that
    lies beyond the largest weight seen, under the fitted tail.  A sample of n draws from
    a tail of index k typically misses a fraction of the evidence that falls only as
    n^-(1-k), so `unseen` over the mean weight estimates how far ln Z reads low.  Both
    are nan when there are too few positive weights to fit."""
    w = np.sort(np.asarray(w, dtype=float))
    w = w[w > 0]
    m = int(min(0.2 * len(w), 3 * np.sqrt(len(w))))
    if m < 5 or len(w) <= m:
        return float("nan"), float("nan")
    cut = w[-m - 1]
    x = w[-m:] - cut
    x = x[x > 0]
    n = len(x)
    if n < 5:
        return float("nan"), float("nan")
    prior, mgrid = 3.0, 30 + int(np.sqrt(n))
    b = 1.0 - np.sqrt(mgrid / (np.arange(1, mgrid + 1) - 0.5))
    b = b / (prior * x[int(n / 4 + 0.5) - 1]) + 1.0 / x[-1]
    kk = np.log1p(-b[:, None] * x).mean(axis=1)
    L = n * (np.log(-b / kk) - kk - 1.0)
    wt = np.exp(-special.logsumexp(L - L[:, None], axis=1))
    bhat = np.sum(b * wt)
    kraw = np.log1p(-bhat * x).mean()
    k, sigma = float((n * kraw + 10 * 0.5) / (n + 10)), float(-kraw / bhat)
    if k >= 1 or sigma <= 0:
        return k, float("inf")
    top = x[-1]
    scale = sigma + k * top                       # the tail's scale at the largest excess
    if scale <= 0:
        return k, 0.0                             # a bounded tail that ends at or below the largest weight
    survive = (scale / sigma) ** (-1.0 / k) if abs(k) > 1e-9 else float(np.exp(-top / sigma))
    unseen = (m / len(w)) * survive * (cut + top + scale / (1.0 - k))
    return k, float(unseen)


def pareto_k(w):
    return tail_fit(w)[0]


def estimate(ln_w, params, n_total=None):
    """ln_w: ln weights of the rows with non-zero weight.  n_total counts every draw.
    Returns ln Z (relative to the offset in ln_w), its error, n_eff, the tail index and,
    per parameter, mean, sd, W90 and the standard error of the mean."""
    ln_w = np.asarray(ln_w, dtype=float)
    n_total = n_total or len(ln_w)
    if len(ln_w) == 0:
        return dict(lnZ=float("-inf"), lnZ_err=float("inf"), n_eff=0.0, n_total=int(n_total), n_evaluated=0,
                    k_hat=float("nan"), lnZ_tail=float("nan"),
                    params={name: dict(mean=float("nan"), sd=float("nan"), w90=float("nan"), se_mean=float("nan"))
                            for name in params})
    top = ln_w.max()
    w = np.exp(ln_w - top)
    n_eff = w.sum() ** 2 / np.sum(w ** 2)
    k_hat, unseen = tail_fit(w)
    # `unseen` is per positive draw; as a fraction of the evidence it is unseen x n_positive / sum(w)
    tail = float(np.log1p(unseen * np.sum(w > 0) / w.sum())) if np.isfinite(unseen) else float("nan")
    out = dict(lnZ=float(top + np.log(w.sum() / n_total)), lnZ_err=float(np.sqrt(max(1 / n_eff - 1 / n_total, 0))),
               n_eff=float(n_eff), n_total=int(n_total), n_evaluated=int(len(w)), k_hat=k_hat, lnZ_tail=tail, params={})
    wn = w / w.sum()
    for name, x in params.items():
        mean = float(np.sum(wn * x))
        sd = float(np.sqrt(np.sum(wn * (x - mean) ** 2)))
        q5, q95 = weighted_quantile(x, wn, [0.05, 0.95])
        out["params"][name] = dict(mean=mean, sd=sd, w90=float(q95 - q5),
                                   se_mean=float(np.sqrt(np.sum(wn ** 2 * (x - mean) ** 2))))
    return out


def chi2(values, errors):
    """Chi-squared of values about their weighted mean; nan when any input is not finite."""
    v, e = np.asarray(values, dtype=float), np.asarray(errors, dtype=float)
    if not (np.all(np.isfinite(v)) and np.all(np.isfinite(e)) and np.all(e > 0)):
        return float("nan")
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
    failed = int((pl["ok"] & ~np.isfinite(lnl)).sum())
    # a comparison with nan is False, so a check that cannot be computed does not pass
    checks = dict(n_eff=est["n_eff"] >= cfg["target_n_eff"], lnZ_err=est["lnZ_err"] <= cfg["lnz_tol"],
                  tail=bool(est["k_hat"] < cfg["k_max"]),
                  groups=bool(all(v < cfg["chi2_max"] for v in c2.values())),
                  failed=failed <= cfg["max_failed_fraction"] * len(sel))
    eff = max(est["n_eff"] / est["n_total"], 1.0 / est["n_total"])
    more = (cfg["target_n_eff"] - est["n_eff"]) / eff if not checks["n_eff"] else 0.25 * est["n_total"]
    for e in [est] + groups:
        e["lnZ"] += cfg["lnl_offset"]
    return dict(estimate=est, groups=groups, group_chi2=c2, checks={k: bool(v) for k, v in checks.items()},
                stop=bool(all(checks.values())), efficiency=float(eff), forecast_more=float(more),
                failed_rows=failed, outside_support=int((~pl["ok"]).sum()))


def extend(cfg, per_stream, more):
    """Draws per stream after one extension.

    The forecast `more` is the number of further draws that would reach the target at
    the measured efficiency.  One heavy draw can cut the measured efficiency several
    fold, so a step is limited to `extension_factor` times the round's present size."""
    step = int(np.ceil(cfg["extension_margin"] * more / cfg["n_streams"]))
    cap = int(np.ceil((cfg["extension_factor"] - 1.0) * per_stream)) if cfg.get("extension_factor") else step
    step = max(cfg["min_extension"], min(step, cap))
    return int(min(cfg["max_frozen_per_stream"], per_stream + step))
