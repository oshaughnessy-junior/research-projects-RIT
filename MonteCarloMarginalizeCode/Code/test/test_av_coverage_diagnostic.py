#!/usr/bin/env python
"""
Check the default diagnostic and opt-in guard against an analytic narrow Gaussian.

The default diagnostic flags sparse flat-axis occupancy. The enabled guard must
activate and recover the known integral, while leaving the resolved control idle.
"""

import numpy as np

import RIFT.integrators.mcsamplerAdaptiveVolume as mcsamplerAV

BOUNDS = {"declination": (-1.0, 1.0), "inclination": (-1.0, 1.0),
          "right_ascension": (0.0, 2 * np.pi), "psi": (0.0, np.pi), "phi_orb": (0.0, 2 * np.pi)}
MU = {"declination": 0.2, "inclination": -0.3}
SIG = 0.001


def _run(names, seed, **kwargs):
    np.random.seed(seed)
    mcsamplerAV.xpy_default.random.seed(seed)   # draws use the active backend (cupy on a GPU host)
    s = mcsamplerAV.MCSampler()
    for p in names:
        lo, hi = BOUNDS[p]
        f = lambda x, n=1 / (hi - lo): np.full(np.shape(x), n)
        s.add_parameter(p, pdf=f, prior_pdf=f, left_limit=lo, right_limit=hi,
                        adaptive_sampling=True)

    def lnF(**x):
        return -0.5 * ((x["declination"] - MU["declination"]) ** 2
                       + (x["inclination"] - MU["inclination"]) ** 2) / SIG ** 2
    r = s.integrate_log(lnF, *names, nmax=4000000, neff=300, n=10000, tempering_exp=0.1,
                        enforce_bounds=True, verbose=False, dict_return=True, **kwargs)
    return r


def _unseen(names, seed):
    return _run(names, seed)[3]["unseen_frac_final"]


def test_diagnostic_flags_narrow_peak_with_flat_axes():
    u = [_unseen(list(BOUNDS), seed) for seed in (11, 12)]
    assert min(u) > mcsamplerAV.COVERAGE_UNSEEN_WARN, u


def test_diagnostic_quiet_when_every_axis_is_resolved():
    names = ["declination", "inclination"]
    default = _run(names, 11)
    off = _run(names, 11, coverage_unseen_tol=0)
    on = _run(names, 11, coverage_unseen_tol=0.05)
    if mcsamplerAV.cupy_ok:
        np.testing.assert_allclose(default[:3], off[:3], rtol=1e-12, atol=1e-11)
        np.testing.assert_allclose(default[:3], on[:3], rtol=1e-12, atol=1e-11)
    else:
        assert default[:3] == off[:3] == on[:3]
    assert on[3]["n_coarsen_cycles"] == 0
    assert default[3]["unseen_frac_final"] < mcsamplerAV.COVERAGE_UNSEEN_WARN


def test_optin_guard_recovers_narrow_peak_with_flat_axes():
    exact = 2 * np.log(np.sqrt(2 * np.pi) * SIG / 2)
    results = [_run(list(BOUNDS), seed, coverage_unseen_tol=0.05) for seed in (11, 12, 13)]
    assert all(r[3]["n_coarsen_cycles"] > 0 for r in results)
    errors = np.array([float(r[0]) - exact for r in results])
    assert np.all(np.abs(errors) < 0.3), errors
    assert abs(errors.mean()) < 0.15, errors
