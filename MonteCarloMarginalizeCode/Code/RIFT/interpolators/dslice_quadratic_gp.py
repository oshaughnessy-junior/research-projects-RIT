"""Distance-slice likelihood model: per-point quadratic in u = 1/d, Matérn GPs over intrinsic coordinates.

A distance-export grid carries many distance slices at each intrinsic point x. At fixed x the
distance-marginal likelihood is close to Gaussian in u = 1/d_L (amplitude scales as 1/d):

    lnL(x, d) = A(x) - (1/d - u0(x))^2 / (2 s(x)^2)

Fitting (A, u0, s) per intrinsic point from its own slices averages the per-slice Monte-Carlo noise,
and the three smooth fields A, u0, log s are then interpolated over x with the bounded Matérn recipe
(CuPy fit by default). The interpolant never has to resolve the steep small-distance falloff itself.
"""
import numpy as np


def fit_points(xi, u, lnL, sig, min_slices=5):
    """Weighted least-squares quadratic in u per unique intrinsic row of xi.

    Returns dict of per-point arrays: x (unique intrinsic coords), A, u0, log_s, their standard errors,
    n (slices), rms (weighted residual rms, nats); plus a count of points rejected because the fitted
    curvature was not negative or fewer than min_slices slices were available.
    """
    xi = np.asarray(xi, dtype=np.float64)
    _, inverse, counts = np.unique(xi, axis=0, return_inverse=True, return_counts=True)
    inverse = inverse.reshape(-1)
    order = np.argsort(inverse, kind="stable")
    starts = np.r_[0, np.cumsum(counts)]
    out = {k: [] for k in ("x", "A", "u0", "log_s", "eA", "eu0", "elog_s", "n", "rms")}
    rejected = dict(few=0, convex=0)
    w_all = 1.0 / np.maximum(np.asarray(sig, dtype=np.float64), 1e-3) ** 2
    for g in range(len(counts)):
        rows = order[starts[g]:starts[g + 1]]
        if len(rows) < min_slices:
            rejected["few"] += 1
            continue
        uu, yy, ww = u[rows], lnL[rows], w_all[rows]
        uc, us = uu.mean(), uu.std() if uu.std() > 0 else 1.0
        t = (uu - uc) / us                       # conditioned design
        F = np.column_stack([np.ones_like(t), t, t * t])
        FtW = F.T * ww
        try:
            cov = np.linalg.inv(FtW @ F)
        except np.linalg.LinAlgError:
            rejected["few"] += 1
            continue
        a, b, c = cov @ (FtW @ yy)
        if not c < 0:
            rejected["convex"] += 1
            continue
        res = yy - F @ np.array([a, b, c])
        rms = float(np.sqrt(np.sum(ww * res ** 2) / np.sum(ww)))
        # inflate the LS covariance by the empirical scatter (reported sigma understates true noise)
        chi2 = float(np.sum(ww * res ** 2)) / max(len(rows) - 3, 1)
        cov = cov * max(chi2, 1.0)
        t0 = -b / (2 * c)
        A = a - b * b / (4 * c)
        s_t = np.sqrt(-1.0 / (2 * c))
        # gradients for error propagation in (a, b, c)
        gA = np.array([1.0, -b / (2 * c), b * b / (4 * c * c)])
        gt0 = np.array([0.0, -1 / (2 * c), b / (2 * c * c)])
        gls = np.array([0.0, 0.0, -0.5 / c])      # d log s / dc  (s ~ (-c)^-1/2)
        out["x"].append(xi[rows[0]])
        out["A"].append(A)
        out["u0"].append(uc + us * t0)
        out["log_s"].append(np.log(us * s_t))
        out["eA"].append(np.sqrt(max(gA @ cov @ gA, 0)))
        out["eu0"].append(us * np.sqrt(max(gt0 @ cov @ gt0, 0)))
        out["elog_s"].append(np.sqrt(max(gls @ cov @ gls, 0)))
        out["n"].append(len(rows))
        out["rms"].append(rms)
    res = {k: np.asarray(v) for k, v in out.items()}
    res["rejected"] = rejected
    return res


class DistanceSliceQuadraticGP:
    """lnL(x, d) from three interpolated per-point fields; predict takes rows (x..., d) in fit order."""

    def __init__(self, gp_A, gp_u0, gp_log_s, dist_index, dist_is_inverse=False):
        self.gp_A, self.gp_u0, self.gp_log_s = gp_A, gp_u0, gp_log_s
        self.dist_index = int(dist_index)
        self.dist_is_inverse = bool(dist_is_inverse)

    def split(self, x):
        x = np.asarray(x, dtype=np.float64)
        dcol = x[:, self.dist_index]
        u = dcol if self.dist_is_inverse else 1.0 / dcol
        return np.delete(x, self.dist_index, axis=1), u

    def predict(self, x):
        xi, u = self.split(x)
        A = self.gp_A.predict(xi)
        u0 = self.gp_u0.predict(xi)
        s = np.exp(self.gp_log_s.predict(xi))
        return A - 0.5 * ((u - u0) / s) ** 2


def fit_dslice_quadratic_gp(x, y, y_errors, dist_index, dist_is_inverse=False, *, max_train_points=4800,
                            optimizer_maxiter=25, seed=25062842, min_slices=5, fit_backend="cupy",
                            feature_names=None, provenance=None):
    """Fit the model on grid rows x (n, d) in fit coordinates containing one distance column.

    fit_backend 'cupy' uses RIFT.interpolators.cupy_matern_fit; 'numpy' runs the same code on CPU.
    Training rows for each field are selected by matern_gp.select_training_rows on A (lnL strata).
    """
    from RIFT.interpolators.cupy_matern_fit import fit_matern_gp_cupy
    x = np.asarray(x, dtype=np.float64)
    xi = np.delete(x, dist_index, axis=1)
    u = x[:, dist_index] if dist_is_inverse else 1.0 / x[:, dist_index]
    pts = fit_points(xi, u, np.asarray(y, dtype=np.float64), np.asarray(y_errors, dtype=np.float64), min_slices)
    if len(pts["A"]) < 10:
        raise ValueError("dslice quadratic GP: fewer than 10 intrinsic points with a concave 1/d fit")
    xp = None
    if fit_backend == "numpy":
        xp = np
    from RIFT.interpolators.matern_gp import select_training_rows
    idx, selection = select_training_rows(pts["A"], max_train_points, seed)
    gps, records = {}, {}
    for field, err in (("A", "eA"), ("u0", "eu0"), ("log_s", "elog_s")):
        model, rec = fit_matern_gp_cupy(pts["x"][idx], pts[field][idx], pts[err][idx],
                                        max_train_points=len(idx), optimizer_maxiter=optimizer_maxiter,
                                        seed=seed, xp=xp)
        rec.pop("selected_indices", None)
        gps[field], records[field] = model, rec
    model = DistanceSliceQuadraticGP(gps["A"], gps["u0"], gps["log_s"], dist_index, dist_is_inverse)
    record = dict(recipe="per-point quadratic in 1/d + Matern GPs on (A, u0, log s)", native_rows=len(x),
                  intrinsic_points=int(len(pts["A"]) + sum(pts["rejected"].values())),
                  points_fit=int(len(pts["A"])), rejected=pts["rejected"], training_points=int(len(idx)),
                  selection=selection, per_point_rms_median=float(np.median(pts["rms"])),
                  slices_median=float(np.median(pts["n"])), fields=records,
                  feature_names=None if feature_names is None else list(feature_names),
                  dist_index=int(dist_index), dist_is_inverse=bool(dist_is_inverse),
                  provenance=dict(provenance or {}))
    return model, record, pts
