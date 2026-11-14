#!/usr/bin/env python
"""
test_AV2_varaha2.py -- invariants of mcsamplerVaraha2 (--sampler-method AV2).

Known-answer and contract checks only; the efficiency comparison against AV and the
measurements behind the defaults live in RIFT_roboto_paper
analyses/varaha_v2_evaluation_20261114 (see DESIGN_varaha2.md).

  1. lnZ on an analytic narrow correlated Gaussian (peak lnL=200) within 5 sigma + 0.01.
  2. sampling_density is a normalized density on the box, and draw_simplified reports it.
  3. A stalled pass is reported as collapsed, not as a quiet number.
  4. A pinned parameter integrates over the others only.
  5. A portfolio [AV2, AV] recovers a known lnZ.

Usage:
  CUDA_VISIBLE_DEVICES= python -m pytest -q MonteCarloMarginalizeCode/Code/test/integrators/test_AV2_varaha2.py
"""
import numpy as np
import pytest
from scipy.special import logsumexp

from RIFT.integrators import mcsamplerVaraha2 as V2

BOX = 5.0


def _uniform(w):
    return lambda x, w=w: np.ones_like(np.asarray(x, dtype=float)) / w


def _build(ndim, cls=V2.MCSampler, **kw):
    s = cls(n_chunk=5000, **kw)
    for i in range(ndim):
        s.add_parameter("x%d" % i, _uniform(2 * BOX), prior_pdf=_uniform(2 * BOX),
                        left_limit=-BOX, right_limit=BOX, adaptive_sampling=True)
    return s


class Needle(object):
    """exp(lnLmax - r^T C^-1 r / 2); integral over the box times the uniform prior is analytic."""
    def __init__(self, ndim=4, sigma=0.03, lnLmax=200.0, seed=3):
        rng = np.random.RandomState(seed)
        self.mu = rng.uniform(-2, 2, ndim)
        s = sigma * rng.uniform(0.5, 2.0, ndim)
        C = np.diag(s ** 2)
        for i in range(0, ndim - 1, 2):
            C[i, i + 1] = C[i + 1, i] = 0.9 * s[i] * s[i + 1]
        self.Ci = np.linalg.inv(C)
        self.lnZ = lnLmax + 0.5 * ndim * np.log(2 * np.pi) + 0.5 * np.linalg.slogdet(C)[1] - ndim * np.log(2 * BOX)
        self.lnLmax = lnLmax

    def __call__(self, *cols):
        r = np.array(cols, dtype=float).T - self.mu
        return self.lnLmax - 0.5 * np.einsum("ij,jk,ik->i", r, self.Ci, r)


def test_known_answer_needle():
    np.random.seed(11)
    T = Needle()
    s = _build(4)
    lnI, logvar, neff, d = s.integrate_log(T, *["x%d" % i for i in range(4)], no_protect_names=True,
                                           n=5000, nmax=8e5, neff=1e9)
    sigma = d['sigma_lnZ_mc']
    assert not d['live_volume_collapsed'], d
    assert d['av2_converged'], d
    assert abs(lnI - T.lnZ) < 5 * sigma + 0.01, (lnI, T.lnZ, sigma)
    # the exported rows reproduce the estimate under the RIFT convention
    lw = s._rvs['log_integrand'] + s._rvs['log_joint_prior'] - s._rvs['log_joint_s_prior']
    assert abs(logsumexp(lw) - np.log(len(lw)) - lnI) < 1e-8
    assert s.ntotal <= 8e5 + 5000


def test_sampling_density_normalized_and_reported():
    np.random.seed(5)
    T = Needle(ndim=3, sigma=0.05)
    s = _build(3)
    s.setup()
    for _ in range(40):                                   # portfolio-style selfish cycles
        s.update_sampling_prior_selfish(T)
    assert len(s._mixture()[0]) > 3
    # (a) integral of q over the box: E_uniform[q] * V_box = 1
    X = np.random.uniform(-BOX, BOX, size=(400000, 3))
    q = s.sampling_density(X)
    assert abs(q.mean() * (2 * BOX) ** 3 - 1.0) < 0.03
    # (b) draw_simplified reports exactly sampling_density at its draws
    ps, p, rv = s.draw_simplified(2000)
    assert np.allclose(ps, s.sampling_density(rv.T), rtol=1e-10)
    # (c) importance estimate of a known integral under the member's own draws
    ps, p, rv = s.draw_simplified(200000)
    lw = T(*rv) + np.log(p) - np.log(ps)
    assert abs(logsumexp(lw) - np.log(len(lw)) - T.lnZ) < 0.05
    assert s.has_unbounded_support and s.joint_p_s_is_normalized_density


def test_stall_is_reported_as_collapse():
    np.random.seed(2)
    T = Needle(ndim=4, sigma=1e-4)                       # far too narrow for this budget
    s = _build(4)
    lnI, logvar, neff, d = s.integrate_log(T, *["x%d" % i for i in range(4)], no_protect_names=True,
                                           n=2000, nmax=4e4, neff=100)
    assert d['live_volume_collapsed'], d
    assert 'collapse_reason' in d


def test_pinned_parameter():
    np.random.seed(7)
    T = Needle(ndim=2, sigma=0.05, lnLmax=20.0)
    s = _build(2)
    x1 = T.mu[1]
    lnI, logvar, neff, d = s.integrate_log(T, "x0", "x1", no_protect_names=True, n=5000,
                                           nmax=3e5, neff=1e9, x1=x1)
    assert np.all(s._rvs["x1"] == x1)
    # integral over x0 of exp(lnL(x0, x1)) /(2 BOX), times p(x1)*width(x1) = 1 (AV convention)
    xs = np.linspace(-BOX, BOX, 400001)
    f = np.exp(T(xs, np.full_like(xs, x1)) - T.lnLmax)
    truth = T.lnLmax + np.log((getattr(np, 'trapezoid', None) or np.trapz)(f, xs) / (2 * BOX))
    assert abs(lnI - truth) < 5 * d['sigma_lnZ_mc'] + 0.01, (lnI, truth)


def test_portfolio_member():
    from RIFT.integrators import mcsamplerPortfolio as P
    from RIFT.integrators import mcsamplerAdaptiveVolume as AV
    np.random.seed(13)
    T = Needle(ndim=3, sigma=0.05, lnLmax=30.0)
    names = ["x%d" % i for i in range(3)]
    port = P.MCSampler(portfolio=[V2.MCSampler(n_chunk=4000), AV.MCSampler(n_chunk=4000)], n_chunk=4000)
    for nm in names:
        port.add_parameter(nm, _uniform(2 * BOX), prior_pdf=_uniform(2 * BOX),
                           left_limit=-BOX, right_limit=BOX, adaptive_sampling=True)
    port.setup(n=4000)
    lnI, logvar, neff, d = port.integrate_log(T, *names, no_protect_names=True, n=4000,
                                              nmax=4e5, neff=1e9, use_lnL=True, save_intg=True)
    lnI = float(np.asarray(lnI))
    err = 0.5 * (float(np.asarray(logvar)) - 2 * lnI)
    assert abs(lnI - T.lnZ) < 5 * np.exp(err) + 0.02, (lnI, T.lnZ)
