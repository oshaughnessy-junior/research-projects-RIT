"""The next proposal version from the rows in hand, and the rule that freezes it.

On block A (d = 4), with every earlier version drawn in equal number:

    psi   = mean over earlier versions of their densities
    w     = (p Lhat / psi)^beta, beta the largest value <= 1 with n_eff >= 5 d (d + 3) / 2
    g     = Gaussian mixture, at most 4 components by BIC, fitted to the rows with weights w
    t     = Student-t, 4 degrees of freedom, centre and 1.5^2 x covariance of the weighted rows
    a     = one EM step on the mixture weight of t, kept in [0.1, 0.5]

The proposal is frozen when beta = 1 on two consecutive fits and the tail index of
p Lhat / psi is below 0.7.  Rows of a frozen version never enter a fit.
"""
import numpy as np
from scipy import special, stats

from .estimator import pareto_k
from .proposal import BlockProposal
from .space import A_DIM, REFLECT

N_FIT = 5 * A_DIM * (A_DIM + 3) // 2
K_FREEZE = 0.7
MAX_COMPONENTS = 4


def kish(ln_w):
    return float(np.exp(2 * special.logsumexp(ln_w) - special.logsumexp(2 * ln_w)))


def temper(ln_w):
    if kish(ln_w) >= N_FIT:
        return 1.0
    lo, hi = 0.0, 1.0
    for _ in range(40):
        mid = 0.5 * (lo + hi)
        lo, hi = (mid, hi) if kish(mid * ln_w) >= N_FIT else (lo, mid)
    return lo


def weighted_gmm(x, w, k, seed, floor):
    """Weighted EM.  Each component covariance has `floor` added, so none collapses on a row."""
    from sklearn.mixture import GaussianMixture
    rng = np.random.default_rng([seed, k])
    start = GaussianMixture(k, covariance_type="full", n_init=2, reg_covar=1e-4, random_state=seed).fit(
        x[rng.choice(len(x), size=4000, p=w)])
    pi, mu, cov = start.weights_, start.means_, start.covariances_ + floor
    prev = -np.inf
    for _ in range(300):
        lp = np.array([np.log(pi[j]) + stats.multivariate_normal(mu[j], cov[j]).logpdf(x) for j in range(k)]).T
        tot = special.logsumexp(lp, axis=1)
        ll = float(np.sum(w * tot))
        if ll - prev < 1e-8:
            break
        prev = ll
        r = np.exp(lp - tot[:, None]) * w[:, None]
        nk = np.maximum(r.sum(axis=0), 1e-12)
        pi = nk / nk.sum()
        mu = (r.T @ x) / nk[:, None]
        cov = np.array([((x - mu[j]).T * r[:, j]) @ (x - mu[j]) / nk[j] + floor for j in range(k)])
    return pi, mu, cov, ll


def ln_psi(props, a):
    return special.logsumexp([p.ln_q(a) for p in props], axis=0) - np.log(len(props))


def next_version(a, ln_pl, props, seed):
    """a: block-A rows of every learning version, inside the support.  ln_pl: ln p(A) + ln Lhat.
    Returns the proposal and a record of the fit."""
    ln_w = ln_pl - ln_psi(props, a)
    beta = temper(ln_w)
    w = np.exp(beta * (ln_w - ln_w.max()))
    n_fit = w.sum() ** 2 / np.sum(w ** 2)
    x, ww = np.vstack([a, a * REFLECT]), np.concatenate([w, w]) / (2 * w.sum())
    total = np.cov(x.T, aweights=ww)
    best = None
    for k in range(1, MAX_COMPONENTS + 1):
        pi, mu, cov, ll = weighted_gmm(x, ww, k, seed, 0.01 * total)
        bic = -2 * n_fit * ll + (k - 1 + k * A_DIM + k * A_DIM * (A_DIM + 1) / 2) * np.log(n_fit)
        if best is None or bic < best[0]:
            best = (bic, pi, mu, cov)
    _, pi, mu, cov = best
    prop = BlockProposal.from_covariances(pi, mu, cov, ww @ x, 1.5 ** 2 * total, a=props[-1].a, seed=seed)
    ln_g, ln_t = prop.ln_gmm(x), prop.ln_t(x)
    resp = np.exp(np.log(prop.a) + ln_t - np.logaddexp(np.log1p(-prop.a) + ln_g, np.log(prop.a) + ln_t))
    prop.a = float(np.clip(np.sum(ww * resp), 0.1, 0.5))
    info = dict(beta=float(beta), n_eff_all=kish(ln_w), n_fit=float(n_fit), rows=int(len(a)),
                k_hat_all=pareto_k(np.exp(ln_w - ln_w.max())), components=int(len(pi)), a=prop.a)
    return prop, info


def frozen(history):
    """history: the record of each fit so far, oldest first."""
    return (len(history) >= 2 and history[-1]["beta"] == 1.0 == history[-2]["beta"]
            and history[-1]["k_hat_all"] < K_FREEZE)
