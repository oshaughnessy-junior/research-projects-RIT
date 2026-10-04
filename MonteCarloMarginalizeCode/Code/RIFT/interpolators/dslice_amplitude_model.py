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


def fit_point(u, y, w, n_restart=2, fix_C=False, loss="linear", f_scale=1.0):
    """Weighted least-squares fit of (R, u*, f_min, C) to one point's slices. Returns (params, rms).

    fix_C pins C = 0, the exact d -> infinity limit; otherwise C absorbs model mismatch, and over a
    narrow slice range it trades off against f_min, which changes the extrapolation.
    loss/f_scale are passed to scipy least_squares on residuals in nats (weights still apply); a robust
    loss such as 'cauchy' with f_scale ~ 1 discounts slices where the integrator missed the peak."""
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
            r = least_squares(resid, p0, method="trf", max_nfev=200, loss=loss,
                              f_scale=f_scale * float(np.sqrt(np.median(w))))
        except Exception:
            continue
        if best is None or r.cost < best.cost:
            best = r
    if best is None or not np.all(np.isfinite(best.x)):
        return None, np.inf
    p = best.x
    params = np.array([np.exp(p[0]), np.exp(p[1]), 1 / (1 + np.exp(-p[2])), 0.0 if fix_C else p[3]])
    res_ = log_model(u, *params) - y
    rms = float(np.sqrt(np.sum(w * res_ ** 2) / np.sum(w))) if loss == "linear" else float(1.4826 * np.median(np.abs(res_)))
    return params, rms


def fit_all_points(key, u, y, sig, min_slices=5, fix_C=False, loss="linear", f_scale=1.0):
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
        p, e = fit_point(u[r], y[r], w[r], fix_C=fix_C, loss=loss, f_scale=f_scale)
        if p is not None:
            params[g], rms[g] = p, e
    return uk, params, rms, counts


class DistanceAmplitudeModel:
    """lnL(x, d) = log_model(1/d; R(x), u*(x), f_min(x), C=0) with the three fields interpolated over
    the intrinsic fit coordinates x by an ExtraTrees regressor fit to the per-point fits.

    Fields are interpolated as log R, log u*, logit f_min. Points whose fit failed are left out."""

    def __init__(self, dist_index, n_estimators=100, n_jobs=-1, min_slices=5, max_overshoot=1.0, mass_index=None,
                 prior=None, d_range=None, n_dgrid=256, loss="linear", f_scale=1.0):
        """mass_index: column (in the full fit coordinates) holding a mass M. If given, the distance
        scale is interpolated as log(u* M): the horizon distance scales with mass, so u* M varies far
        less across the grid than u* itself."""
        self.dist_index = int(dist_index)
        self.mass_index = None if mass_index is None else int(mass_index)
        # prior + d_range switch on the marginal/conditional decomposition:
        #   lnL(x,d) = M(x) + [log_model(1/d; theta(x)) - N(theta(x))],
        # M = log int exp(log_model) prior dd over d_range per point, interpolated directly, N the same
        # integral evaluated at the interpolated shape. The intrinsic marginal is then M(x) alone, so
        # interpolation error in the distance-shape fields cannot move intrinsic weights.
        self.prior, self.d_range, self.n_dgrid = prior, d_range, int(n_dgrid)
        self.loss, self.f_scale = loss, float(f_scale)
        self.max_overshoot = float(max_overshoot)
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
                                            np.asarray(y_errors, dtype=float), self.min_slices, fix_C=True,
                                            loss=self.loss, f_scale=self.f_scale)
        good = np.all(np.isfinite(P), axis=1)
        # Guard: a point whose slices do not resolve its flat top (e.g. all slices at nearly one d)
        # can fit a peak far above anything it measured; the sampler then piles onto that spike.
        # Drop points whose fitted maximum over d exceeds their highest slice by > max_overshoot.
        _, inv = np.unique(key, axis=0, return_inverse=True)
        ymax = np.full(len(uk), -np.inf)
        np.maximum.at(ymax, inv.reshape(-1), np.asarray(y, dtype=float))
        ug = np.geomspace(1e-5, 1e-2, 2000)
        over = np.full(len(uk), np.inf)
        for k in np.flatnonzero(good):
            over[k] = log_model(ug, *P[k]).max() - ymax[k]
        n_over = int(np.sum(good & (over > self.max_overshoot)))
        good &= over <= self.max_overshoot
        self.report = dict(points=int(len(uk)), points_fit=int(good.sum()), dropped_overshoot=n_over,
                           per_point_rms_median=float(np.nanmedian(rms)), slices_median=float(np.median(counts)))
        self.rf = ExtraTreesRegressor(n_estimators=self.n_estimators, n_jobs=self.n_jobs)
        F = self._to_fields(P[good])
        if self.mass_index is not None:
            F[:, 1] += np.log(self._mass(uk[good]))
        if self.prior is not None:
            self._dgrid = np.linspace(self.d_range[0], self.d_range[1], self.n_dgrid)
            self._lw = np.log(np.maximum(np.asarray(self.prior(self._dgrid), dtype=float), 1e-300)) \
                + np.log(np.gradient(self._dgrid))
            M = self._lognorm(P[good, 0], P[good, 1], P[good, 2])
            F = np.column_stack([F, M])
            self.prior = "tabulated"     # keep only the tabulated weights: closures do not pickle
        self.rf.fit(uk[good], F)
        return self

    def _lognorm(self, R, us, fm):
        """log int exp(log_model(1/d)) prior(d) dd over the d grid, vectorized over points."""
        out = np.empty(len(R))
        for s0 in range(0, len(R), 4096):
            sl = slice(s0, s0 + 4096)
            L = log_model(1.0 / self._dgrid[None, :], R[sl, None], us[sl, None], fm[sl, None], 0.0) + self._lw[None, :]
            m = L.max(axis=1)
            out[sl] = m + np.log(np.exp(L - m[:, None]).sum(axis=1))
        return out

    def _mass(self, xi):
        # mass column index refers to the full fit coordinates; xi has the distance column removed
        j = self.mass_index - (1 if self.mass_index > self.dist_index else 0)
        return xi[:, j]

    def predict(self, x):
        x = np.asarray(x, dtype=float)
        xi = np.delete(x, self.dist_index, axis=1)
        F = self.rf.predict(xi)
        if self.mass_index is not None:
            F[:, 1] -= np.log(self._mass(xi))
        R, us, fm = self._from_fields(F[:, :3])
        ll = log_model(1.0 / x[:, self.dist_index], R, us, fm, 0.0)
        if self.prior is not None:
            ll = F[:, 3] + ll - self._lognorm(R, us, fm)
        return ll
