#!/usr/bin/env python3
"""A stand-in for integrate_likelihood_extrinsic_batchmode, for tests of RIFT.ledger.engine.

It keeps the parts of ILE the engine depends on: a function `analyze_event` with locals
`indx_event` and `P`, one call of the real AV sampler per point, and a loop that catches
an exception in one point, prints FAILED ANALYSIS and goes on.  The integrand is a
Gaussian of width WIDTH in three dimensions times a factor that depends on the point, so

    ln L(point) = 3 ln(sqrt(2 pi) WIDTH) - (mc - 30)^2 / 2

    --stub-skip K      point K raises before it reaches the sampler
    --stub-double      a second sampler call per point, as a burn-in or replica option makes
"""
import contextlib
import io
import sys

import lal
import numpy as np

import RIFT.integrators.mcsamplerAdaptiveVolume as av

WIDTH, CHUNK = 0.05, 2000
NAMES = ["x0", "x1", "x2"]


def analytic(grid_row):
    m1, m2 = grid_row[0], grid_row[1]
    mc = (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2
    return 3 * np.log(np.sqrt(2 * np.pi) * WIDTH) - 0.5 * (mc - 30.0) ** 2


class Point:
    def __init__(self, g):
        self.m1, self.m2 = g[0] * lal.MSUN_SI, g[1] * lal.MSUN_SI
        self.s1x, self.s1y, self.s1z, self.s2x, self.s2y, self.s2z = g[2:]
        self.row = g


def analyze_event(P_list, indx_event):
    P = P_list[indx_event]
    if indx_event == SKIP:
        raise ValueError("no waveform for this point")
    offset = analytic(P.row) - 3 * np.log(np.sqrt(2 * np.pi) * WIDTH)

    def ln_f(*xs):
        return -0.5 * np.sum((np.array(xs).T - 0.5) ** 2, axis=1) / WIDTH ** 2 + offset

    sampler = av.MCSampler(n_chunk=CHUNK)
    sampler.xpy = np
    sampler.identity_convert = lambda x: x
    for name in NAMES:
        sampler.add_parameter(name, pdf=None, left_limit=0.0, right_limit=1.0,
                              prior_pdf=lambda x: np.ones(np.shape(x)), adaptive_sampling=True)
    kw = dict(nmax=4000000, neff=50, n=CHUNK, no_protect_names=True, verbose=False, enforce_bounds=True)
    with contextlib.redirect_stdout(io.StringIO()), np.errstate(invalid="ignore"):
        out = sampler.integrate(ln_f, *NAMES, **kw)
        if DOUBLE:
            sampler.integrate(ln_f, *NAMES, **kw)
    return out


if __name__ == "__main__":
    SKIP = int(sys.argv[sys.argv.index("--stub-skip") + 1]) if "--stub-skip" in sys.argv else -1
    DOUBLE = "--stub-double" in sys.argv
    grid = np.loadtxt(sys.argv[sys.argv.index("--sim-grid") + 1], comments="#", ndmin=2)[:, 2:]
    points = [Point(g) for g in grid]
    for indx in range(len(points)):
        try:
            analyze_event(points, indx)
        except Exception as exc:
            print("  ===> FAILED ANALYSIS <==== ", exc)
