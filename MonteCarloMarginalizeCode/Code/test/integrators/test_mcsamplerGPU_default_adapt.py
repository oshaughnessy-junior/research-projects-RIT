#!/usr/bin/env python
"""
test_mcsamplerGPU_default_adapt.py

mcsamplerGPU adapts whenever n_adapt > 0, but it only caches the integrand history
when tempering_exp > 0.  With the default tempering_exp = 0 the adaptation block
therefore ran with no cache to read, and crashed before adapting anything:
integrate() raised NameError('int_vals') and integrate_log() raised
KeyError('log_weights').  Both were reachable from a bare
MCSampler().add_parameter(..., adaptive_sampling=True) followed by integrate(),
which is what test/demo_mcsampler_foridiots.py -- the README's "easy-to-read" demo
-- does, so the demo died on its first mcsamplerGPU call.

A third case sat behind those: a sampler reused for a second integrate() keeps
_rvs[p] from the first pass while "integrand" restarts, so slicing points to
n_history and weights to the (shorter) integrand record handed numpy.bincount two
different lengths.

The integrals here are deliberately crude -- these pin that the adaptive path runs
and stays unbiased, not how well it converges.
"""
from __future__ import print_function

import numpy as np
import pytest

from RIFT.integrators import mcsamplerGPU

LO, HI = -1.5, 1.0
SIG = 0.3
# Normalized prior 1/(HI-LO), so integrate() returns the prior-weighted average.
TRUTH = np.sqrt(2 * np.pi) * SIG / (HI - LO)


def _sampler():
    s = mcsamplerGPU.MCSampler()
    s.add_parameter(
        'x',
        pdf=np.vectorize(lambda x: 1.0 / (HI - LO)),
        cdf_inv=None,
        prior_pdf=np.vectorize(lambda x: 1.0 / (HI - LO)),
        left_limit=LO, right_limit=HI,
        adaptive_sampling=True,
    )
    return s


def _gaussian(x):
    return np.exp(-x ** 2 / (2 * SIG ** 2))


def _lngaussian(x):
    return -x ** 2 / (2 * SIG ** 2)


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_integrate_default_tempering_runs_and_is_unbiased(seed):
    """integrate() with the default tempering_exp=0: used to raise NameError('int_vals')."""
    np.random.seed(seed)
    ret, var, neff, _ = _sampler().integrate(
        np.vectorize(_gaussian), 'x', n=500, nmax=10000, neff=1e9, full_output=True)
    ret = float(ret)
    assert np.isfinite(ret)
    # 4x the sampler's own error estimate: this is a bias check, not a precision check.
    assert abs(ret - TRUTH) < 4 * np.sqrt(float(var)) + 0.05 * TRUTH
    assert float(neff) > 1


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_integrate_log_default_tempering_runs(seed):
    """integrate_log() with the default tempering_exp=0: used to raise KeyError('log_weights')."""
    np.random.seed(seed)
    res = _sampler().integrate_log(
        np.vectorize(_lngaussian), 'x', n=500, nmax=10000, neff=1e9)
    ln_ret = float(res[0])
    assert np.isfinite(ln_ret)
    assert abs(np.exp(ln_ret) - TRUTH) < 0.2 * TRUTH


def test_reused_sampler_adapts_without_length_mismatch():
    """Second integrate() on the same sampler: weights and points must line up.

    The first pass leaves len(_rvs['x']) == nmax with no "integrand" record; the
    second starts that record from zero.  Slicing both to n_history handed
    numpy.bincount 2*n points against n weights ("The weights and list don't have
    the same length").
    """
    np.random.seed(0)
    s = _sampler()
    s.integrate(np.vectorize(_gaussian), 'x', n=500, nmax=2000, neff=1e9,
                full_output=True)                       # no cache: save_intg stays False
    n_first = len(s._rvs['x'])
    assert n_first >= 2000, "first pass should leave a parameter record behind"
    s.reset_sampling('x')
    ret, var, neff, _ = s.integrate(np.vectorize(_gaussian), 'x', n=500, nmax=4000,
                                    neff=1e9, full_output=True, tempering_exp=0.2)
    ret = float(ret)
    assert np.isfinite(ret)
    assert abs(ret - TRUTH) < 0.25 * TRUTH


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
