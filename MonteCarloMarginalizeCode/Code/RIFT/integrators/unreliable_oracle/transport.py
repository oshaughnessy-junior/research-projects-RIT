"""Small transport-inspired TRAINING oracle; not a WTF reproduction.

Fits a Gaussian-kernel reward surrogate to scored history, then performs a few
projected ascent steps on r_hat(y) - ||y-z||^2/(2*transport_scale) in unit-box
coordinates. There is no pretrained two-time flow map, neural rollout or log q.
Never use this object as a portfolio density member or as posterior output.
"""
import numpy as np
from RIFT.integrators.mcsampler_generic import MCSamplerGeneric


class TransportOracle(MCSamplerGeneric):
    def setup(self, seed=0, periodic=(), max_fit=96, n_anchors=32, steps=5,
              step_size=0.02, transport_scale=0.2, length_scale=0.2,
              ledger=None, **kwargs):
        super().setup(**kwargs)
        if any(isinstance(p, tuple) for p in self.params_ordered):
            raise ValueError("TransportOracle requires scalar coordinate names")
        self.lo = np.array([self.llim[p] for p in self.params_ordered])
        self.width = np.array([self.rlim[p] - self.llim[p] for p in self.params_ordered])
        if not len(self.lo) or not np.all(np.isfinite(self.lo + self.width)) or np.any(self.width <= 0):
            raise ValueError("finite nonempty box required")
        if set(periodic) - set(self.params_ordered):
            raise ValueError("unknown periodic coordinate")
        if min(max_fit, n_anchors, steps) < 1 or min(step_size, transport_scale, length_scale) <= 0:
            raise ValueError("positive sizes/scales required")
        self.periodic = np.array([p in periodic for p in self.params_ordered])
        self.rng = np.random.RandomState(seed)
        self.max_fit, self.n_anchors, self.steps = int(max_fit), int(n_anchors), int(steps)
        self.step_size, self.transport_scale = float(step_size), float(transport_scale)
        self.length_scale = float(length_scale)
        self.reference_samples = None
        self.centers = self.coefficients = None
        self._gradient = self._reward_gradient
        if ledger is not None:
            self._gradient = ledger.wrap(self._gradient, kind="surrogate", layout="matrix")

    def _delta(self, a, b):
        d = a - b
        d[..., self.periodic] = (d[..., self.periodic] + 0.5) % 1.0 - 0.5
        return d

    def _reward_gradient(self, y):
        delta = self._delta(y[:, None, :], self.centers[None, :, :])
        kernel = np.exp(-np.sum(delta**2, axis=-1) / (2*self.length_scale**2))
        return -np.einsum('nk,nkd,k->nd', kernel, delta, self.coefficients) / self.length_scale**2

    def update_sampling_prior(self, ln_weights, n_history, external_rvs=None, **kwargs):
        # ln_weights are deliberately NOT treated as oracle/posterior weights.
        rvs = external_rvs if external_rvs is not None else self._rvs
        count = min(int(n_history), len(rvs.get('log_integrand', [])))
        if count < 2:
            raise ValueError("scored history required")
        x = np.column_stack([np.asarray(rvs[p])[-count:] for p in self.params_ordered])
        reward = np.asarray(rvs['log_integrand'])[-count:]
        good = np.isfinite(reward) & np.all(np.isfinite(x), axis=1)
        x, reward = x[good], reward[good]
        if len(x) < 2:
            raise ValueError("finite scored history required")
        selected = self.rng.choice(len(x), min(len(x), self.max_fit), replace=False)
        self.centers = (x[selected] - self.lo) / self.width
        reward = reward[selected]
        # Clip deep tails for numerical conditioning; this is a heuristic reward.
        reward = np.maximum(reward - np.max(reward), -50.0)
        delta = self._delta(self.centers[:, None, :], self.centers[None, :, :])
        kernel = np.exp(-np.sum(delta**2, axis=-1)/(2*self.length_scale**2))
        self.coefficients = np.linalg.solve(kernel + np.eye(len(kernel))*1e-3, reward)
        z = self.centers[self.rng.choice(len(self.centers), self.n_anchors)].copy()
        y = z.copy()
        for _ in range(self.steps):
            gradient = self._gradient(y) - self._delta(y, z)/self.transport_scale
            gradient /= np.maximum(1.0, np.linalg.norm(gradient, axis=1, keepdims=True)/10.0)
            y += self.step_size * gradient
            y[:, self.periodic] %= 1.0
            y[:, ~self.periodic] = np.clip(y[:, ~self.periodic], 0.0, 1.0)
        self.reference_samples = self.lo + y*self.width

    def draw_simplified(self, n_samples, *args, **kwargs):
        if self.reference_samples is None:
            raise ValueError("update from scored history before drawing")
        rows = self.rng.choice(len(self.reference_samples), int(n_samples))
        return None, None, self.reference_samples[rows].copy()
