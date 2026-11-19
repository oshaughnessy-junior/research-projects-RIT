"""Prior, coordinates and reported parameters of a ledger run.

One configuration is supported: a quasi-circular precessing binary whose prior is
uniform in (m1, m2) inside a chirp-mass and symmetric-mass-ratio box, with each spin
uniform in magnitude on [0, chi_max] and isotropic.  This is the prior of the last
line of a pseudo_pipe `args_cip_list.txt` built with `--assume-precessing`.

An intrinsic point is 8 numbers in two blocks:

    block A   mc, delta = (m1 - m2)/(m1 + m2), s1z, s2z
    block B   s1p, phi1, s2p, phi2      (in-plane spin magnitude and azimuth)

In these coordinates the prior factorises:

    p(mc, delta)  = mc (1 - delta^2)^(-6/5) / norm
    p(sz)         = ln(chi_max / |sz|) / (2 chi_max)
    p(sp | sz)    = sp / (sz^2 + sp^2) / ln(chi_max / |sz|)
    p(phi)        = 1 / (2 pi)

A proposal acts on block A only.  Block B is drawn from the prior conditional on block
A, so its factors cancel in a weight.  Grid rows are `m1 m2 a1x a1y a1z a2x a2y a2z`,
masses in solar masses, as in a hyperpipeline grid.
"""
import numpy as np
from scipy import integrate

GRID_COLUMNS = ("m1", "m2", "a1x", "a1y", "a1z", "a2x", "a2y", "a2z")
PARAMS = ("mc", "q", "chi_eff", "chi_p")
A_DIM = 4
REFLECT = np.array([1.0, -1.0, 1.0, 1.0])     # the fold in delta


class Box:
    """The prior box.  All densities are functions of block A."""

    def __init__(self, mc_range, eta_range, chi_max):
        self.mc_range = (float(mc_range[0]), float(mc_range[1]))
        self.eta_range = (float(eta_range[0]), float(eta_range[1]))
        self.chi_max = float(chi_max)
        if not (0 < self.mc_range[0] < self.mc_range[1]):
            raise ValueError("bad chirp-mass range %r" % (self.mc_range,))
        if not (0 < self.eta_range[0] < self.eta_range[1] <= 0.25):
            raise ValueError("bad eta range %r" % (self.eta_range,))
        if not (0 < self.chi_max <= 1):
            raise ValueError("bad chi_max %r" % (self.chi_max,))
        # pseudo_pipe ends the eta range just below 0.25, so delta starts just above zero
        self.delta_min = float(np.sqrt(max(0.0, 1.0 - 4.0 * self.eta_range[1])))
        self.delta_max = float(np.sqrt(1.0 - 4.0 * self.eta_range[0]))
        self._delta_norm = integrate.quad(lambda x: (1.0 - x * x) ** -1.2, self.delta_min, self.delta_max)[0]
        self._mc_norm = 0.5 * (self.mc_range[1] ** 2 - self.mc_range[0] ** 2)
        grid = np.linspace(self.delta_min, self.delta_max, 200001)
        dens = (1.0 - grid ** 2) ** -1.2
        cdf = np.concatenate([[0.0], np.cumsum(0.5 * np.diff(grid) * (dens[1:] + dens[:-1]))])
        self._delta_grid, self._delta_cdf = grid, cdf / cdf[-1]

    def to_json(self):
        return dict(mc_range=list(self.mc_range), eta_range=list(self.eta_range), chi_max=self.chi_max)

    @classmethod
    def from_json(cls, d):
        return cls(d["mc_range"], d["eta_range"], d["chi_max"])

    def ln_prior_A(self, a):
        """ln p(mc, delta, s1z, s2z); -inf outside the support."""
        a = np.atleast_2d(np.asarray(a, dtype=float))
        mc, d, s1z, s2z = a.T
        ok = ((mc > self.mc_range[0]) & (mc < self.mc_range[1]) & (d >= self.delta_min) & (d < self.delta_max)
              & (np.abs(s1z) < self.chi_max) & (np.abs(s2z) < self.chi_max) & (s1z != 0) & (s2z != 0))
        out = np.full(len(a), -np.inf)
        with np.errstate(divide="ignore", invalid="ignore"):
            val = (np.log(mc / self._mc_norm) - 1.2 * np.log1p(-d * d) - np.log(self._delta_norm)
                   + np.log(np.log(self.chi_max / np.abs(s1z)) / (2 * self.chi_max))
                   + np.log(np.log(self.chi_max / np.abs(s2z)) / (2 * self.chi_max)))
        out[ok] = val[ok]
        return out

    def prior_A(self, u):
        """Block A from the prior.  u: 6 uniforms."""
        lo, hi = self.mc_range
        return np.array([np.sqrt(lo * lo + u[0] * (hi * hi - lo * lo)),
                         np.interp(u[1], self._delta_cdf, self._delta_grid),
                         self.chi_max * u[2] * (2 * u[3] - 1), self.chi_max * u[4] * (2 * u[5] - 1)])

    def draw_B(self, a, u):
        """Block B from the prior conditional on block A.  u: (n, 4) uniforms."""
        a, u = np.atleast_2d(a), np.atleast_2d(u)
        out = np.empty((len(a), 4))
        for j, col in ((0, 2), (2, 3)):
            sz = np.abs(a[:, col])
            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                out[:, j] = sz * np.sqrt(np.expm1(2.0 * u[:, j] * np.log(self.chi_max / sz)))
            out[:, j + 1] = 2 * np.pi * u[:, j + 1]
        return out


def to_grid(a, b):
    """Grid rows from blocks A and B."""
    a, b = np.atleast_2d(a), np.atleast_2d(b)
    mc, d, s1z, s2z = a.T
    eta = 0.25 * (1.0 - d * d)
    mtot = mc * eta ** -0.6
    return np.column_stack([0.5 * mtot * (1 + d), 0.5 * mtot * (1 - d),
                            b[:, 0] * np.cos(b[:, 1]), b[:, 0] * np.sin(b[:, 1]), s1z,
                            b[:, 2] * np.cos(b[:, 3]), b[:, 2] * np.sin(b[:, 3]), s2z])


def from_grid(g):
    """Blocks A and B from grid rows."""
    g = np.atleast_2d(g)
    m1, m2 = g[:, 0], g[:, 1]
    a = np.column_stack([(m1 * m2) ** 0.6 / (m1 + m2) ** 0.2, (m1 - m2) / (m1 + m2), g[:, 4], g[:, 7]])
    b = np.column_stack([np.hypot(g[:, 2], g[:, 3]), np.arctan2(g[:, 3], g[:, 2]) % (2 * np.pi),
                         np.hypot(g[:, 5], g[:, 6]), np.arctan2(g[:, 6], g[:, 5]) % (2 * np.pi)])
    return a, b


def summaries(g):
    """Reported parameters from grid rows."""
    g = np.atleast_2d(g)
    m1, m2 = g[:, 0], g[:, 1]
    q = m2 / m1
    c1p, c2p = np.hypot(g[:, 2], g[:, 3]), np.hypot(g[:, 5], g[:, 6])
    return dict(mc=(m1 * m2) ** 0.6 / (m1 + m2) ** 0.2, q=q,
                chi_eff=(m1 * g[:, 4] + m2 * g[:, 7]) / (m1 + m2),
                chi_p=np.maximum(c1p, (4 * q + 3) / (4 + 3 * q) * q * c2p))
