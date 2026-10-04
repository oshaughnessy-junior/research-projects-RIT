"""Physical per-point distance model for distance-slice grids.

At fixed intrinsic parameters and fixed angles the log likelihood ratio is exactly quadratic in
x = u/u*, u = 1/d, through the origin:  lnL = R (2 f x - f^2 x^2) = R (1 - (1 - f x)^2), with f the
angle-dependent amplitude factor. Averaging exp(lnL) over f uniform on [f_min, 1] gives

    lnL(u) = C + log[ (1/(1-f_min)) * integral_{f_min}^{1} exp(R (1 - (1 - f x)^2)) df ]

in closed form (an erf difference). Properties: lnL -> C as d -> infinity (C = 0 for an exact
likelihood ratio), flat across the distance-inclination degenerate range x in [1, 1/f_min], Gaussian
fall-off outside it. Four parameters per intrinsic point: R (> 0), u* (> 0), f_min in (0, 1), C.
"""
import numpy as np
from scipy.optimize import least_squares
from scipy.special import log_ndtr

LOG_SQRT_PI_2 = 0.5 * np.log(np.pi) - np.log(2.0)


def log_model(u, R, ustar, fmin, C):
    """Vectorized lnL(u) of the averaged-amplitude model."""
    u = np.asarray(u, dtype=float)
    x = np.maximum(u / ustar, 1e-300)
    sR = np.sqrt(R)
    b = sR * (1 - x)            # f = 1 end
    a = sR * (1 - fmin * x)     # f = fmin end; a >= b
    # erf(a) - erf(b) = 2 [Phi(sqrt2 a) - Phi(sqrt2 b)] = 2 [Phi(-sqrt2 b) - Phi(-sqrt2 a)].
    # Take whichever form keeps both arguments in the lower tail, where log_ndtr is accurate.
    upper = a > 0
    hi_ = np.where(upper, log_ndtr(-np.sqrt(2) * b), log_ndtr(np.sqrt(2) * a))
    lo_ = np.where(upper, log_ndtr(-np.sqrt(2) * a), log_ndtr(np.sqrt(2) * b))
    log_diff = np.log(2.0) + hi_ + np.log1p(-np.exp(np.minimum(lo_ - hi_, -1e-300)))
    logI = R - np.log(x) + LOG_SQRT_PI_2 - np.log(sR) + log_diff - np.log(1 - fmin)
    return C + logI


def fit_point(u, y, w, n_restart=2, fix_C=False):
    """Weighted least-squares fit of (R, u*, f_min, C) to one point's slices. Returns (params, rms).

    fix_C pins C = 0, the exact d -> infinity limit; otherwise C absorbs model mismatch, and over a
    narrow slice range it trades off against f_min, which changes the extrapolation."""
    order = np.argsort(u)
    u, y, w = u[order], y[order], w[order]
    ymax = y.max()
    u10, u90 = np.percentile(u, [10, 90])
    best = None
    for k in range(n_restart):
        fmin0 = min(0.9, max(0.05, u10 / u90 * (0.8 if k else 1.0)))
        p0 = np.array([np.log(max(ymax, 1.0)), np.log(u10 * (0.9 if k else 1.0)), np.log(fmin0 / (1 - fmin0)), 0.0])

        if fix_C:
            p0 = p0[:3]

        def resid(p):
            R, us, fm = np.exp(p[0]), np.exp(p[1]), 1 / (1 + np.exp(-p[2]))
            C = 0.0 if fix_C else p[3]
            return np.sqrt(w) * (log_model(u, R, us, fm, C) - y)

        try:
            r = least_squares(resid, p0, method="trf", max_nfev=200)
        except Exception:
            continue
        if best is None or r.cost < best.cost:
            best = r
    if best is None or not np.all(np.isfinite(best.x)):
        return None, np.inf
    p = best.x
    params = np.array([np.exp(p[0]), np.exp(p[1]), 1 / (1 + np.exp(-p[2])), 0.0 if fix_C else p[3]])
    rms = float(np.sqrt(np.sum(w * (log_model(u, *params) - y) ** 2) / np.sum(w)))
    return params, rms


def fit_all_points(key, u, y, sig, min_slices=5, fix_C=False):
    """Fit every unique intrinsic row of `key`. Returns (unique keys, params (n,4), rms, nslices)."""
    uk, inv, counts = np.unique(key, axis=0, return_inverse=True, return_counts=True)
    inv = inv.reshape(-1)
    order = np.argsort(inv, kind="stable")
    starts = np.r_[0, np.cumsum(counts)]
    w = 1.0 / np.maximum(sig, 1e-3) ** 2
    params = np.full((len(uk), 4), np.nan)
    rms = np.full(len(uk), np.nan)
    for g in range(len(uk)):
        r = order[starts[g]:starts[g + 1]]
        if len(r) < min_slices:
            continue
        p, e = fit_point(u[r], y[r], w[r], fix_C=fix_C)
        if p is not None:
            params[g], rms[g] = p, e
    return uk, params, rms, counts


class DistanceAmplitudeModel:
    """lnL(x, d) = log_model(1/d; R(x), u*(x), f_min(x), C=0) with the three fields interpolated over
    the intrinsic fit coordinates x by an ExtraTrees regressor fit to the per-point fits.

    Fields are interpolated as log R, log u*, logit f_min. Points whose fit failed are left out."""

    def __init__(self, dist_index, n_estimators=100, n_jobs=-1, min_slices=5):
        self.dist_index = int(dist_index)
        self.n_estimators, self.n_jobs, self.min_slices = n_estimators, n_jobs, min_slices

    @staticmethod
    def _to_fields(P):
        return np.column_stack([np.log(P[:, 0]), np.log(P[:, 1]), np.log(P[:, 2] / (1 - P[:, 2]))])

    @staticmethod
    def _from_fields(F):
        return np.exp(F[:, 0]), np.exp(F[:, 1]), 1 / (1 + np.exp(-F[:, 2]))

    def fit(self, x, y, y_errors):
        from sklearn.ensemble import ExtraTreesRegressor
        x = np.asarray(x, dtype=float)
        xi = np.delete(x, self.dist_index, axis=1)
        key = np.round(xi, 10)
        uk, P, rms, counts = fit_all_points(key, 1.0 / x[:, self.dist_index], np.asarray(y, dtype=float),
                                            np.asarray(y_errors, dtype=float), self.min_slices, fix_C=True)
        good = np.all(np.isfinite(P), axis=1)
        self.report = dict(points=int(len(uk)), points_fit=int(good.sum()),
                           per_point_rms_median=float(np.nanmedian(rms)), slices_median=float(np.median(counts)))
        self.rf = ExtraTreesRegressor(n_estimators=self.n_estimators, n_jobs=self.n_jobs)
        self.rf.fit(uk[good], self._to_fields(P[good]))
        return self

    def predict(self, x):
        x = np.asarray(x, dtype=float)
        xi = np.delete(x, self.dist_index, axis=1)
        R, us, fm = self._from_fields(self.rf.predict(xi))
        return log_model(1.0 / x[:, self.dist_index], R, us, fm, 0.0)
