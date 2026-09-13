"""Importance evidence must average over every draw from the proposal."""

import numpy as np
import pytest

from RIFT.likelihood.jax_ile.samplers import evidence_from_logweights


def test_out_of_support_draws_are_zero_weight_not_removed():
    # Two of four Gaussian proposal draws miss the prior.  Their weights are
    # zero, so Z is (2 + 2 + 0 + 0) / 4 = 1, not 2.
    logZ, sigma_over_Z, neff = evidence_from_logweights(
        [np.log(2.), np.log(2.), -np.inf, -np.inf])
    assert logZ == pytest.approx(0.)
    assert sigma_over_Z == pytest.approx(0.5)
    assert neff == pytest.approx(2.)


def test_constant_and_all_zero_weights():
    logZ, sigma_over_Z, neff = evidence_from_logweights([3., 3.])
    assert logZ == pytest.approx(3.)
    assert sigma_over_Z == pytest.approx(0.)
    assert neff == pytest.approx(2.)
    logZ, sigma_over_Z, neff = evidence_from_logweights([-np.inf, -np.inf])
    assert logZ == -np.inf
    assert sigma_over_Z == np.inf
    assert neff == 0.


@pytest.mark.parametrize("bad", [np.nan, np.inf])
def test_nonzero_nonfinite_weights_fail_closed(bad):
    logZ, sigma_over_Z, neff = evidence_from_logweights([0., bad])
    assert np.isnan(logZ)
    assert np.isnan(sigma_over_Z)
    assert neff == 0.
