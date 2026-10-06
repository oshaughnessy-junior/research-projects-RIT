#!/usr/bin/env python
"""
mcsamplerAdaptiveVolume.update_sampling_prior_selfish is how mcsamplerPortfolio drives an
AV member: one VARAHA cycle per portfolio chunk.  It must carry the cycle state (live set,
likelihood threshold, truncated probability) between calls, as integrate_log carries it
between loop iterations.  When it restarted that state on every call, each call kept only
the top nsel of its own fresh draws, so V fell by n_chunk/nsel per chunk with no floor and
the live volume shrank onto a sliver that excluded the posterior.  In the JAX ILE portfolio
that left AV with escaped_mass = 1 and the evidence on one GMM sample (n_eff ~ 1, logZ nan).

The predicates are behavioural: the live volume must still cover the posterior after many
calls, and must stop contracting.
"""

import numpy as np
import pytest

import RIFT.integrators.mcsamplerAdaptiveVolume as mcsamplerAV
import RIFT.integrators.mcsamplerEnsemble as mcsamplerGMM
import RIFT.integrators.mcsamplerPortfolio as mcsamplerPortfolio

SIGMA = 0.03
MU = np.array([0.31, -0.42])
LO, HI = -1.0, 1.0
N_CHUNK = 4000


def lnF(x, y):
    x = np.asarray(x); y = np.asarray(y)
    return -0.5 * ((x - MU[0]) ** 2 + (y - MU[1]) ** 2) / SIGMA ** 2


def _uniform(x):
    return np.ones_like(np.asarray(x, dtype=float)) / (HI - LO)


def _add_params(sampler):
    for name in ('x', 'y'):
        sampler.add_parameter(name, pdf=None, left_limit=LO, right_limit=HI,
                              prior_pdf=_uniform, adaptive_sampling=True)


def _posterior_draws(n=20000, seed=3):
    rng = np.random.default_rng(seed)
    return MU[None, :] + SIGMA * rng.standard_normal((n, 2))


def _covered_fraction(av):
    return float(np.mean(av.sampling_density(_posterior_draws()) > 0))


def test_selfish_steps_keep_the_posterior_in_the_live_volume():
    np.random.seed(11)
    av = mcsamplerAV.MCSampler(n_chunk=N_CHUNK)
    _add_params(av)
    av.setup()
    lnV = []
    for _ in range(40):
        av.draw_simplified(N_CHUNK, 'x', 'y')
        av.update_sampling_prior_selfish(lnF)
        lnV.append(np.log(av.V))
    # Measured, seeds 11-14: fixed 0.9996 covered and lnV flat at -4.6 from call 10 on;
    # restarted state 0.965-0.989 covered and lnV still falling ~0.37 over calls 31-40.
    # In 2-D the bin floor slows the ratchet; in the 6-D extrinsic problem it was total.
    assert _covered_fraction(av) > 0.995, (_covered_fraction(av), lnV[-1])
    assert lnV[-1] - lnV[-10] > -0.05, lnV


def test_setup_and_new_warm_seed_restart_the_selfish_state():
    np.random.seed(5)
    av = mcsamplerAV.MCSampler(n_chunk=N_CHUNK)
    _add_params(av)
    av.setup()
    av.draw_simplified(N_CHUNK, 'x', 'y')
    av.update_sampling_prior_selfish(lnF)
    assert av._selfish_state is not None
    av.setup()
    assert av._selfish_state is None
    av.update_sampling_prior_selfish(lnF)
    av.bootstrap_from_samples(_posterior_draws(2000), params=['x', 'y'])
    av.draw_simplified(N_CHUNK, 'x', 'y')       # installs the seed
    assert av._selfish_state is None


def test_portfolio_av_member_covers_the_posterior():
    np.random.seed(7)
    av = mcsamplerAV.MCSampler(n_chunk=N_CHUNK)
    gmm = mcsamplerGMM.MCSampler()
    port = mcsamplerPortfolio.MCSampler(portfolio=[av, gmm], n_chunk=N_CHUNK)
    _add_params(port)
    port.setup(portfolio_args=[{}, {'n_comp': 2}])
    logZ, log_var, neff, info = port.integrate_log(
        lnF, 'x', 'y', nmax=40 * N_CHUNK, neff=1e9, n=N_CHUNK,
        no_protect_names=True, save_intg=True, tempering_exp=1.0)
    exact = np.log(2 * np.pi * SIGMA ** 2 / (HI - LO) ** 2)
    # Measured, seeds 7-9: fixed 0.9996 covered, AV escaped mass <= 3e-4; restarted
    # state 0.976-0.977 covered, escaped mass >= 3.4e-3.
    assert _covered_fraction(av) > 0.995
    esc = np.asarray(info['portfolio_escaped_mass'], dtype=float)
    assert esc[0] < 1.5e-3, esc
    assert abs(logZ - exact) < 0.2, (logZ, exact, neff)
