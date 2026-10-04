"""Physical distance continuation for a fitted lnL(x, d) beyond each grid point's exported slices.

A tree fit is piecewise constant outside its training envelope, so beyond each intrinsic point's
nearest and farthest slice it holds lnL flat, and the volumetric distance prior turns that into
spurious mass at small or large d. This wrapper keeps the base fit inside the slice range of the
nearest grid point and, outside it, continues from the base fit's edge value with the shape of the
averaged-amplitude model (dslice_amplitude_model.log_model, C = 0) fitted to that point's slices:

    lnL(x, d) = base(x, d_edge) + [A(1/d; theta_j) - A(1/d_edge; theta_j)]      for d outside [d_min_j, d_max_j]

with j the nearest grid point in standardized intrinsic coordinates. f_min may be shared by the whole
event (the median of free per-point fits): the inclination degeneracy it encodes is set by the network.
"""
import numpy as np
from scipy.spatial import cKDTree

from RIFT.interpolators.dslice_amplitude_model import fit_all_points_batched, log_model


class RFDistanceTails:
    def __init__(self, base_fit, dist_index, sides="both", shared_fmin=True, xp=np):
        if sides not in ("near", "far", "both"):
            raise ValueError("sides must be near, far or both")
        self.base_fit, self.dist_index, self.sides = base_fit, int(dist_index), sides
        self.shared_fmin, self.xp = bool(shared_fmin), xp

    def fit(self, x, y_unshifted, y_errors):
        """x: training rows in fit coordinates; y_unshifted: lnL with no shift (the model's d -> inf
        limit is 0); y_errors: per-row sigma."""
        x = np.asarray(x, dtype=float)
        d = x[:, self.dist_index]
        key = np.round(np.delete(x, self.dist_index, axis=1), 10)
        uk, inv = np.unique(key, axis=0, return_inverse=True)
        inv = inv.reshape(-1)
        self.dmin = np.full(len(uk), np.inf)
        self.dmax = np.zeros(len(uk))
        np.minimum.at(self.dmin, inv, d)
        np.maximum.at(self.dmax, inv, d)
        args = (key, 1.0 / d, np.asarray(y_unshifted, dtype=float), np.asarray(y_errors, dtype=float))
        uk2, P, _, _ = fit_all_points_batched(*args, fix_C=True, xp=self.xp)
        fmin = None
        if self.shared_fmin:
            fmin = float(np.nanmedian(P[:, 2]))
            uk2, P, _, _ = fit_all_points_batched(*args, fix_C=True, xp=self.xp, fmin_fixed=fmin)
        assert np.array_equal(uk2, uk)
        self.P = P
        self.good = np.all(np.isfinite(P), axis=1)
        self.mu, self.sd = uk.mean(0), uk.std(0)
        self.sd[self.sd == 0] = 1.0
        self.tree = cKDTree((uk - self.mu) / self.sd)
        self.report = dict(points=int(len(uk)), points_fit=int(self.good.sum()), sides=self.sides,
                           fmin_shared=fmin)
        return self

    def __call__(self, x_in):
        # keep the caller's array type (the GPU driver passes and expects cupy); neighbour lookup and
        # the continuation run on the host
        dev = hasattr(x_in, "get")
        xmod = __import__("cupy") if dev else np
        x = x_in.get() if dev else np.asarray(x_in, dtype=float)
        to_host = (lambda a: a.get()) if dev else (lambda a: np.asarray(a, dtype=float))
        out = np.empty(len(x))
        fin = np.all(np.isfinite(x), axis=1)
        if not np.all(fin):                      # leave non-finite rows to the base fit's own guard
            out[~fin] = to_host(self.base_fit(xmod.asarray(x[~fin])))
        xf = x[fin]
        d = xf[:, self.dist_index]
        _, j = self.tree.query((np.delete(xf, self.dist_index, axis=1) - self.mu) / self.sd)
        g = self.good[j]
        lo = g & (d < self.dmin[j]) if self.sides in ("near", "both") else np.zeros(len(d), bool)
        hi = g & (d > self.dmax[j]) if self.sides in ("far", "both") else np.zeros(len(d), bool)
        d_edge = np.where(lo, self.dmin[j], np.where(hi, self.dmax[j], d))
        xe = xf.copy()
        xe[:, self.dist_index] = d_edge
        val = to_host(self.base_fit(xmod.asarray(xe))).copy()
        off = lo | hi
        if np.any(off):
            Pj = self.P[j[off]]
            val[off] = val[off] + log_model(1.0 / d[off], Pj[:, 0], Pj[:, 1], Pj[:, 2], 0.0) \
                - log_model(1.0 / d_edge[off], Pj[:, 0], Pj[:, 1], Pj[:, 2], 0.0)
        out[fin] = val
        return xmod.asarray(out)
