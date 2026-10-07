"""Weighted GMM refits must localize separated modes, and must not write into caller arrays.

Regression tests for three measured defects:

1. estimator.fit() converged on the UNWEIGHTED log-likelihood of the draws (tolerance
   d*k*n*1e-3 nats) from an identity-covariance init, so a refit to well-separated weighted
   modes stopped after ~3 EM iterations at one broad blob: 0/40 seeds localized the case below.
2. gmm._merge() wrote merged weights element-wise into self.weights, which np.asarray handed
   back unchanged when a caller had assigned its own float array -- the caller's array drifted.
3. mcsamplerEnsemble's fair-draw export did `ln_wt = integrator.cumulative_values; ln_wt += ...`,
   rewriting the stored lnL (and _rvs['log_integrand'], which aliases it) in place.
"""
import numpy as np
from scipy.stats import norm

from RIFT.integrators import gaussian_mixture_model as GMM
from RIFT.integrators import mcsamplerEnsemble


def _two_mode_weighted_draws(n=10000, beta=0.55):
    # broad proposal draws in the normalized frame, weighted by a tempered two-mode target
    rng = np.random.default_rng(1)
    x = rng.uniform(-1, 1, (n, 1))
    lw = beta * np.log(0.573 * norm.pdf(x[:, 0], -0.557, 0.06) + 0.427 * norm.pdf(x[:, 0], 0.461, 0.18))
    return x, lw


def test_weighted_refit_localizes_separated_1d_modes():
    x, lw = _two_mode_weighted_draws()
    n_ok = 0
    for seed in range(20):
        np.random.seed(seed)
        est = GMM.estimator(2, max_iters=1000)
        est.fit(x, lw)
        order = np.argsort(np.ravel(est.means))
        means = np.ravel(est.means)[order]
        stds = np.sqrt([est.covariances[i][0, 0] for i in order])
        # tempered target: sigma/sqrt(beta) = 0.081, 0.243
        n_ok += bool(np.allclose(means, [-0.557, 0.461], atol=0.03) and
                     np.allclose(stds, [0.081, 0.243], rtol=0.15))
    assert n_ok == 20, "only {}/20 refits localized both modes".format(n_ok)


def test_weighted_convergence_criterion_localizes_3d_modes():
    # Pins the convergence criterion separately from the init: with the k-means++ init kept and
    # the legacy unweighted criterion restored, 32/40 of these refits localize both modes.
    from scipy.stats import multivariate_normal as mvn
    rng = np.random.default_rng(302)
    x = rng.uniform(-1, 1, (10000, 3))
    c1, c2 = -0.5 * np.ones(3), 0.45 * np.ones(3)
    lw = 0.55 * np.log(0.57 * mvn.pdf(x, c1, 0.012 * np.eye(3)) + 0.43 * mvn.pdf(x, c2, 0.0324 * np.eye(3)))
    n_ok = 0
    for seed in range(40):
        np.random.seed(seed)
        est = GMM.estimator(2, max_iters=1000)
        est.fit(x, lw)
        mu = np.array(est.means)
        mu = mu[np.argsort(mu[:, 0])]
        n_ok += bool(np.allclose(mu[0], c1, atol=0.05) and np.allclose(mu[1], c2, atol=0.05))
    assert n_ok == 40, "only {}/40 refits localized both modes".format(n_ok)


def test_update_does_not_write_into_caller_arrays():
    model = GMM.gmm(2, np.array([[-5.0, 5.0]]))
    weights = np.array([0.6, 0.4])
    means = np.array([[-0.5], [0.5]])
    model.means = means
    model.covariances = [np.array([[0.01]]), np.array([[0.04]])]
    model.weights = weights
    model.d = 1
    model.N = 10000
    w0, m0 = weights.copy(), means.copy()
    rng = np.random.default_rng(2)
    x = np.concatenate([rng.normal(-2.0, 0.6, 5000), rng.normal(2.5, 1.0, 5000)])[:, None]
    np.random.seed(0)
    model.update(x, np.zeros(len(x)))
    assert not np.allclose(np.asarray(model.weights), w0)   # the update did move the model
    np.testing.assert_array_equal(weights, w0)
    np.testing.assert_array_equal(means, m0)


def test_fairdraw_export_keeps_stored_lnL():
    sampler = mcsamplerEnsemble.MCSampler()
    sampler.add_parameter("x", pdf=lambda x: np.ones_like(x) / 2.0,
                          prior_pdf=lambda x: np.ones_like(x) / 2.0,
                          left_limit=-1.0, right_limit=1.0, adaptive_sampling=True)
    lnL = lambda x: 50.0 - 0.5 * (np.asarray(x) / 0.2) ** 2
    np.random.seed(3)
    sampler.integrate_log(lnL, "x", n=2000, nmax=2000, neff=1, min_iter=1, max_iter=1,
                          correlate_all_dims=True, n_comp=1,
                          igrand_fairdraw_samples=True, igrand_fairdraw_samples_max=10**6)
    assert getattr(sampler, "_rvs_is_fairdraw", False)
    np.testing.assert_allclose(sampler._rvs["log_integrand"], lnL(sampler._rvs["x"]), atol=1e-10)
    integ = sampler.integrator
    np.testing.assert_allclose(integ.cumulative_values, lnL(integ.cumulative_samples[:, 0]), atol=1e-10)
