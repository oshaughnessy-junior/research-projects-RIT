#!/usr/bin/env python3
"""gmm.score() must be a deterministic function of the model and the points, and normalized.

Each component is normalized by its Gaussian mass inside the box.  For d>=3 that mass used to
come from scipy's mvnun, whose lattice shifts come from a Fortran RNG that no seed reaches, so
the same model scored the same points differently depending on earlier calls in the process.
Normalization is checked against brute force, not against the old code.
"""
import numpy as np
import pytest

from RIFT.integrators import gaussian_mixture_model as GMM


def _host(a):
    return a.get() if hasattr(a, 'get') else np.asarray(a)


def _cov(d, rng, scale):
    A = rng.normal(size=(d, d))
    return scale * (A @ A.T) / d + 0.1 * scale * np.eye(d)


def _model(d, seed):
    '''A broad component plus a narrow, correlated one at a corner, mostly outside the box.'''
    rng = np.random.default_rng(seed)
    bounds = np.column_stack([-1.0 - rng.uniform(0, 2, d), 1.0 + rng.uniform(0, 3, d)])
    m = GMM.gmm(2, bounds)
    m.d = d
    m.means = [rng.uniform(-0.3, 0.3, d), np.full(d, 0.95)]
    # The broad component is well conditioned, so 1/q has a finite, modest variance.
    m.covariances = [0.4 * np.eye(d) + 0.1, _cov(d, rng, 0.02)]
    m.weights = np.array([0.6, 0.4])
    m.adapt = [False, False]
    return m


@pytest.mark.parametrize('d', [3, 4, 6])
def test_score_repeats_bitwise_after_other_calls(d):
    m = _model(d, seed=d)
    other = _model(d, seed=100 + d)
    x = _host(m.sample(50))
    first = _host(m.score(x))
    for _ in range(3):
        other.score(x)          # advances any hidden RNG state the mass computation uses
    second = _host(m.score(x))
    np.testing.assert_array_equal(first, second)


CASES = [  # mean offset, covariance scale, lower, upper
    (0.0, 0.3, -1.0, 1.0),
    (0.9, 0.05, -1.0, 1.0),
    (1.5, 0.4, -1.0, 1.0),
    (-0.4, 0.2, -2.0, 0.3),
]


@pytest.mark.parametrize('d', [2, 3, 4])
@pytest.mark.parametrize('case', range(len(CASES)))
def test_box_mass_matches_brute_force(d, case):
    off, scale, lo, hi = CASES[case]
    rng = np.random.default_rng(10 * d + case)
    mean = off + rng.uniform(-0.2, 0.2, d)
    cov = _cov(d, rng, scale)
    lower = np.full(d, lo) + rng.uniform(0, 0.3, d)   # unequal widths
    upper = np.full(d, hi) - rng.uniform(0, 0.3, d)
    n = 4_000_000
    x = rng.multivariate_normal(mean, cov, size=n)
    p = np.mean(np.all((x > lower) & (x < upper), axis=1))
    sigma = np.sqrt(max(p, 1.0 / n) * (1 - p) / n)
    got = GMM._box_mass(lower, upper, mean, cov)
    assert abs(got - p) < 5 * sigma + 1e-5 * p, (got, p, sigma)


@pytest.mark.parametrize('d', [2, 3, 4])
def test_score_normalized_over_box(d):
    '''E_q[1/q] over the box is the box volume when q is the normalized sampling density.'''
    m = _model(d, seed=d)
    np.random.seed(7)
    x = _host(m.sample(400_000))
    inv = 1.0 / _host(m.score(x))
    vol = float(np.prod(m.bounds[:, 1] - m.bounds[:, 0]))
    est, err = inv.mean(), inv.std() / np.sqrt(len(inv))
    assert abs(est / vol - 1) < 5 * err / vol, (est / vol, err / vol)
