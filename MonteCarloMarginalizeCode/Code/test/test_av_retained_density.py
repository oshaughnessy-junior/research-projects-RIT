#!/usr/bin/env python
"""
AV (mcsamplerAdaptiveVolume.integrate_log) evidence normalization.

The retained set's sampling density used to be 1/(V * box volume), with V the product
of per-cycle survival fractions nrec/ninj.  That product is biased high: each cycle's
occupied bins miss part of the previous live region -- its low-likelihood rim -- so new
draws over-sample the interior and survive the next threshold too often.  The bias
compounds over cycles, so lnZ came out high on any sharply peaked integrand.

integrate_log now gives each retained point the density of the mixture of every grid it
drew from (log_retained_density).  These tests pin that on a target with a closed-form
evidence, and the mixture density itself on hand-built grids.
Measurements: RIFT_roboto_paper analyses/av_lnz_normalization_20261006 (2026-10).
"""

import numpy as np
import pytest

import RIFT.integrators.mcsamplerAdaptiveVolume as mcsamplerAV
from RIFT.integrators.mcsamplerAdaptiveVolume import log_retained_density


def _gaussian_lnZ_error(seed, sig=0.003, ndim=4, n_chunk=2000, nmax=200000, pinned=None):
    """lnZ(AV) - lnZ(exact) for an isotropic Gaussian of width sig centred in the unit box."""
    np.random.seed(seed)
    names = ['x%d' % i for i in range(ndim)]

    def lnF(*xs):
        X = np.array(xs).T - 0.5
        if pinned is not None:
            X = X[:, 1:]          # the pinned coordinate does not enter the integrand
        return -0.5 * np.sum(X**2, axis=1) / sig**2

    s = mcsamplerAV.MCSampler(n_chunk=n_chunk)
    s.xpy = np
    s.identity_convert = lambda x: x
    for name in names:
        s.add_parameter(name, pdf=None, left_limit=0.0, right_limit=1.0,
                        prior_pdf=lambda x: np.ones(np.shape(x)), adaptive_sampling=True)
    kw = {} if pinned is None else {names[0]: pinned}
    out = s.integrate_log(lnF, *names, nmax=nmax, neff=50, n=n_chunk,
                          no_protect_names=True, verbose=False, **kw)
    n_free = ndim if pinned is None else ndim - 1
    return out[0] - n_free * np.log(np.sqrt(2 * np.pi) * sig)


def test_narrow_gaussian_lnZ_matches_closed_form():
    """Fails with the scalar-V normalization, which is biased high on this target."""
    err = np.array([_gaussian_lnZ_error(seed) for seed in (1, 2, 3)])
    assert abs(err.mean()) < 0.15, "lnZ - exact = {} (mean {:.3f})".format(err, err.mean())
    assert np.all(np.abs(err) < 0.3), "lnZ - exact = {}".format(err)


def test_pinned_coordinate_keeps_normalization():
    """A pinned coordinate is drawn at one value; with a unit-width uniform prior on it the
    evidence equals that of the remaining free coordinates."""
    err = _gaussian_lnZ_error(1, ndim=4, pinned=0.37)
    assert abs(err) < 0.3, "pinned lnZ - exact = {:.3f}".format(err)


def test_density_single_full_box_grid():
    """One cycle drawn uniformly over a box of volume 2*3: density n/(N_ret * 6) everywhere."""
    X = np.array([[0.1, 0.2], [1.9, 2.9], [1.0, 1.5]])
    grids = [(np.array([2.0, 3.0]), np.array([[0, 0]]), 100)]
    out = log_retained_density(X, np.zeros(3, dtype=int), grids, np.zeros(2), np.array([2.0, 3.0]))
    assert np.allclose(out, np.log(100) - np.log(3) - np.log(6.0))


def test_density_sums_the_grids_that_cover_each_point():
    """Box grid (n=10, volume 1) plus a 2x2 grid with one occupied bin (n=30, volume 1/4)."""
    X = np.array([[0.2, 0.2],    # inside the occupied fine bin (0, 0)
                  [0.8, 0.8]])   # outside it: only the box grid covers this point
    grids = [(np.array([1.0, 1.0]), np.array([[0, 0]]), 10),
             (np.array([0.5, 0.5]), np.array([[0, 0]]), 30)]
    out = log_retained_density(X, np.array([1, 0]), grids, np.zeros(2), np.ones(2))
    expect = np.log([10 / 1.0 + 30 / 0.25, 10 / 1.0]) - np.log(2)
    assert np.allclose(out, expect)


def test_density_counts_a_point_in_its_own_grid_at_a_bin_edge():
    """A draw that rounds onto its bin's upper edge still carries its own grid's density."""
    X = np.array([[0.5, 0.25]])          # floor(0.5/0.5) = 1: outside occupied bin (0, 0)
    grids = [(np.array([0.5, 0.5]), np.array([[0, 0]]), 8)]
    out = log_retained_density(X, np.array([0]), grids, np.zeros(2), np.ones(2))
    assert np.isfinite(out[0])
    assert np.isclose(out[0], np.log(8 / 0.25))


def test_density_pinned_dimension_uses_full_width():
    """Pinned dims are excluded from membership and enter with dx0."""
    X = np.array([[0.2, 1.0]])           # second coordinate pinned at the box's upper edge
    grids = [(np.array([0.5, 1.0]), np.array([[0, 0]]), 4)]
    out = log_retained_density(X, np.array([0]), grids, np.zeros(2), np.array([1.0, 1.0]),
                               pinned_dims=[1])
    assert np.isclose(out[0], np.log(4 / 0.5))
