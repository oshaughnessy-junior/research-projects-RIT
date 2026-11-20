"""Proposals on block A with an evaluable density and index-addressable draws.

Draw `index` of stream `stream` uses numpy's default generator seeded with
[seed, stream, index] and consumes a fixed number of variates, so any process can
produce any draw without producing the ones before it.

A Gaussian or Student-t draw is `mean + L z` with L the stored Cholesky factor and z
standard normal, summed in a fixed order.  The factor is computed once, when the
proposal is fitted, and published with it.  (Drawing through
`Generator.multivariate_normal` goes through an SVD whose vector signs depend on the
LAPACK build: one published covariance then gives different points on two machines.)
Two machines can still differ in the last bits of a draw, so the controller writes
every plan to disk and treats that file, not a regenerated plan, as the record.
"""
import numpy as np
from scipy import linalg, special

from .space import A_DIM, REFLECT

_LN_2PI = float(np.log(2.0 * np.pi))


def _affine(mean, chol, z):
    """mean + chol @ z for a lower-triangular chol, in a fixed order of operations."""
    out = []
    for i in range(len(mean)):
        acc = float(mean[i])
        for j in range(i + 1):
            acc += float(chol[i][j]) * float(z[j])
        out.append(acc)
    return out


def ln_normal(x, mean, chol):
    y = linalg.solve_triangular(chol, (np.atleast_2d(x) - mean).T, lower=True)
    return -0.5 * np.sum(y * y, axis=0) - float(np.sum(np.log(np.diag(chol)))) - 0.5 * len(mean) * _LN_2PI


def ln_student(x, loc, chol, df):
    d = len(loc)
    y = linalg.solve_triangular(chol, (np.atleast_2d(x) - loc).T, lower=True)
    return (special.gammaln(0.5 * (df + d)) - special.gammaln(0.5 * df) - 0.5 * d * np.log(df * np.pi)
            - float(np.sum(np.log(np.diag(chol)))) - 0.5 * (df + d) * np.log1p(np.sum(y * y, axis=0) / df))


class PriorProposal:
    """Version 1: block A from the prior, so a weight is the likelihood alone."""
    family = "prior"
    a = 0.1          # defensive fraction handed to the first fitted proposal

    def __init__(self, box, seed):
        self.box, self.seed = box, int(seed)

    def ln_q(self, a):
        return self.box.ln_prior_A(a)

    def draw(self, stream, index):
        rng = np.random.default_rng([self.seed, int(stream), int(index)])
        return self.box.prior_A(rng.random(6)), rng.random(4)

    def to_json(self):
        return dict(family=self.family, seed=self.seed, a=self.a)


class BlockProposal:
    """h = (1 - a) Gaussian mixture + a Student-t on (mc, delta', s1z, s2z); delta = |delta'|."""
    family = "gmm+t, cholesky"

    def __init__(self, weights, means, chols, t_loc, t_chol, t_df=4.0, a=0.1, seed=0):
        self.weights = np.asarray(weights, dtype=float)
        self.means = np.asarray(means, dtype=float)
        self.chols = np.asarray(chols, dtype=float)
        self.t_loc, self.t_chol = np.asarray(t_loc, dtype=float), np.asarray(t_chol, dtype=float)
        self.t_df, self.a, self.seed = float(t_df), float(a), int(seed)
        if self.means.shape != (len(self.weights), A_DIM) or self.chols.shape != (len(self.weights), A_DIM, A_DIM):
            raise ValueError("inconsistent proposal shapes")
        self._cum = np.cumsum(self.weights / self.weights.sum())

    @classmethod
    def from_covariances(cls, weights, means, covs, t_loc, t_shape, **kw):
        return cls(weights, means, [np.linalg.cholesky(c) for c in np.asarray(covs)], t_loc,
                   np.linalg.cholesky(np.asarray(t_shape)), **kw)

    def ln_gmm(self, x):
        parts = [np.log(w) + ln_normal(x, m, c) for w, m, c in zip(self.weights / self.weights.sum(), self.means, self.chols)]
        return special.logsumexp(np.array(parts), axis=0)

    def ln_t(self, x):
        return ln_student(x, self.t_loc, self.t_chol, self.t_df)

    def ln_h(self, x):
        return np.logaddexp(np.log1p(-self.a) + self.ln_gmm(x), np.log(self.a) + self.ln_t(x))

    def ln_q(self, a):
        """Folded density at rows with delta >= 0."""
        a = np.atleast_2d(np.asarray(a, dtype=float))
        return np.logaddexp(self.ln_h(a), self.ln_h(a * REFLECT))

    def draw(self, stream, index):
        """One draw: block A (folded) and the four block-B uniforms."""
        rng = np.random.default_rng([self.seed, int(stream), int(index)])
        u = rng.random(2)
        z = rng.standard_normal(A_DIM)
        g = float(rng.chisquare(self.t_df))
        ub = rng.random(4)
        if u[0] < self.a:
            scale = float(np.sqrt(self.t_df / g))
            x = _affine(self.t_loc, self.t_chol, [scale * float(v) for v in z])
        else:
            k = min(int(np.searchsorted(self._cum, u[1], side="right")), len(self.weights) - 1)
            x = _affine(self.means[k], self.chols[k], z)
        x[1] = abs(x[1])
        return np.array(x), ub

    def to_json(self):
        return dict(family=self.family, weights=self.weights.tolist(), means=self.means.tolist(),
                    chols=self.chols.tolist(), t_loc=self.t_loc.tolist(), t_chol=self.t_chol.tolist(),
                    t_df=self.t_df, a=self.a, seed=self.seed)

    @classmethod
    def from_json(cls, d):
        return cls(d["weights"], d["means"], d["chols"], d["t_loc"], d["t_chol"], d["t_df"], d["a"], d["seed"])


def proposal_from_json(d, box):
    if d["family"] == PriorProposal.family:
        return PriorProposal(box, d["seed"])
    if d["family"] == BlockProposal.family:
        return BlockProposal.from_json(d)
    raise ValueError("unknown proposal family %r" % (d["family"],))


def draw_plan(prop, box, n_streams, lo, hi):
    """Draws [lo, hi) of every stream: keys, block A, grid row, support flag, ln p - ln q.

    Rows are ordered by stream, then index.  A draw outside the prior support has
    ok False, a grid row of nan and ln p - ln q of -inf; it counts in the estimate with
    weight zero and is never evaluated."""
    from .space import to_grid
    n = (hi - lo) * n_streams
    stream = np.repeat(np.arange(n_streams), hi - lo)
    index = np.tile(np.arange(lo, hi), n_streams)
    a, u = np.empty((n, A_DIM)), np.empty((n, 4))
    for j in range(n):
        a[j], u[j] = prop.draw(stream[j], index[j])
    ln_p = box.ln_prior_A(a) if n else np.empty(0)
    ok = np.isfinite(ln_p)
    grid = np.full((n, 8), np.nan)
    ln_pq = np.full(n, -np.inf)
    if ok.any():
        grid[ok] = to_grid(a[ok], box.draw_B(a[ok], u[ok]))
        ln_pq[ok] = ln_p[ok] - prop.ln_q(a[ok])
    return dict(stream=stream, index=index, a=a, grid=grid, ok=ok, ln_pq=ln_pq)
