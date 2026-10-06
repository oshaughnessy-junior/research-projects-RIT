#!/usr/bin/env python
"""
mcsamplerAdaptiveVolume.update_sampling_prior_selfish is how mcsamplerPortfolio drives an
AV member: one VARAHA cycle per portfolio chunk.  It must carry the cycle state (live set,
likelihood threshold, truncated probability) between calls, as integrate_log carries it
between loop iterations.  When it restarted that state on every call, each call kept only
the top nsel of its own fresh draws, so V fell by n_chunk/nsel per chunk with no floor and
the live volume shrank onto a sliver that excluded the posterior.  In the JAX ILE portfolio
that left AV with escaped_mass = 1 and the evidence on one GMM sample (n_eff ~ 1, logZ nan).

The carried state must also not outlive its pass (a new integrand starts a fresh
threshold) and must stay bounded in size.

Assertion thresholds sit between values measured on the restarted-state base and on this
fix over fresh seeds; the measurements are recorded with the PR.
"""

import numpy as np
import pytest

import RIFT.integrators.mcsamplerAdaptiveVolume as mcsamplerAV
import RIFT.integrators.mcsamplerEnsemble as mcsamplerGMM
import RIFT.integrators.mcsamplerPortfolio as mcsamplerPortfolio

SIGMA = 0.03
LO, HI = -1.0, 1.0
N_CHUNK = 4000


@pytest.fixture(autouse=True)
def _restore_global_rng():
    state = np.random.get_state()
    yield
    np.random.set_state(state)


def _target(ndim):
    mu = np.linspace(0.31, -0.42, ndim)

    def lnF(*cols):
        r2 = sum((np.asarray(c) - m) ** 2 for c, m in zip(cols, mu))
        return -0.5 * r2 / SIGMA ** 2
    return mu, lnF


def _names(ndim):
    return ['x%d' % i for i in range(ndim)]


def _uniform(x):
    return np.ones_like(np.asarray(x, dtype=float)) / (HI - LO)


def _add_params(sampler, ndim):
    for name in _names(ndim):
        sampler.add_parameter(name, pdf=None, left_limit=LO, right_limit=HI,
                              prior_pdf=_uniform, adaptive_sampling=True)


def _covered_fraction(av, mu, n=20000, seed=3):
    rng = np.random.default_rng(seed)
    draws = mu[None, :] + SIGMA * rng.standard_normal((n, len(mu)))
    return float(np.mean(av.sampling_density(draws) > 0))


def _exact_lnZ(ndim):
    return ndim * np.log(np.sqrt(2 * np.pi) * SIGMA / (HI - LO))


def _av(ndim, seed):
    np.random.seed(seed)
    av = mcsamplerAV.MCSampler(n_chunk=N_CHUNK)
    _add_params(av, ndim)
    av.setup()
    return av


def _portfolio(ndim, seed):
    np.random.seed(seed)
    av = mcsamplerAV.MCSampler(n_chunk=N_CHUNK)
    port = mcsamplerPortfolio.MCSampler(portfolio=[av, mcsamplerGMM.MCSampler()],
                                        n_chunk=N_CHUNK)
    _add_params(port, ndim)
    port.setup(portfolio_args=[{}, {'n_comp': 2}])
    return av, port


def _integrate(port, lnF, ndim, n_chunks=40):
    return port.integrate_log(lnF, *_names(ndim), nmax=n_chunks * N_CHUNK, neff=1e9,
                              n=N_CHUNK, no_protect_names=True, save_intg=True,
                              tempering_exp=1.0)


def test_selfish_steps_keep_the_posterior_in_the_live_volume():
    mu, lnF = _target(2)
    av = _av(2, 11)
    lnV = []
    for _ in range(40):
        av.draw_simplified(N_CHUNK, *_names(2))
        av.update_sampling_prior_selfish(lnF)
        lnV.append(np.log(av.V))
    assert _covered_fraction(av, mu) > 0.995, (_covered_fraction(av, mu), lnV[-1])
    # contraction stops once the final threshold is reached
    assert lnV[-1] - lnV[-10] > -0.05, lnV


def test_portfolio_av_member_covers_the_posterior():
    mu, lnF = _target(2)
    av, port = _portfolio(2, 7)
    logZ, log_var, neff, info = _integrate(port, lnF, 2)
    assert _covered_fraction(av, mu) > 0.995
    esc = np.asarray(info['portfolio_escaped_mass'], dtype=float)
    assert esc[0] < 1.5e-3, esc
    assert abs(logZ - _exact_lnZ(2)) < 0.2, (logZ, neff)


def test_portfolio_4d_av_member_carries_the_evidence():
    # The headline symptom: in 4-D the restarted state cut the posterior out of the AV
    # live volume, so the evidence came from a few GMM draws.
    mu, lnF = _target(4)
    av, port = _portfolio(4, 7)
    logZ, log_var, neff, info = _integrate(port, lnF, 4)
    esc = np.asarray(info['portfolio_escaped_mass'], dtype=float)
    assert _covered_fraction(av, mu) > 0.99, _covered_fraction(av, mu)
    assert esc[0] < 0.01, esc
    assert abs(logZ - _exact_lnZ(4)) < 0.3, (logZ, neff)


def test_new_pass_restarts_the_threshold(capsys):
    # A second integrate_log on a different integrand (the calmarg burn-in pattern) must
    # not inherit a threshold that excludes every new draw.
    mu, lnF = _target(2)
    av, port = _portfolio(2, 5)
    _integrate(port, lnF, 2, n_chunks=30)
    capsys.readouterr()

    def lnF_shifted(*cols):
        return lnF(*cols) - 20.0
    _integrate(port, lnF_shifted, 2, n_chunks=10)
    out = capsys.readouterr().out
    assert out.count('no finite in-volume samples') == 0
    assert _covered_fraction(av, mu) > 0.995


def test_carried_live_set_is_bounded_and_keeps_every_bin():
    mu, lnF = _target(2)
    av = _av(2, 13)
    for _ in range(80):
        av.draw_simplified(N_CHUNK, *_names(2))
        av.update_sampling_prior_selfish(lnF)
        n_bins = len(av.binunique)
        assert len(av._selfish_state['allloglkl']) <= max(4 * N_CHUNK, n_bins + 1000)
    assert _covered_fraction(av, mu) > 0.995


def test_setup_and_new_warm_seed_restart_the_selfish_state():
    mu, lnF = _target(2)
    av = _av(2, 5)
    av.draw_simplified(N_CHUNK, *_names(2))
    av.update_sampling_prior_selfish(lnF)
    assert av._selfish_state is not None
    av.setup()
    assert av._selfish_state is None
    av.update_sampling_prior_selfish(lnF)
    rng = np.random.default_rng(1)
    av.bootstrap_from_samples(mu[None, :] + SIGMA * rng.standard_normal((2000, 2)),
                              params=_names(2))
    av.draw_simplified(N_CHUNK, *_names(2))       # installs the seed
    assert av._selfish_state is None
