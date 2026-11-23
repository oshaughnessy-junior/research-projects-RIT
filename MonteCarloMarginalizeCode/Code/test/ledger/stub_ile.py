#!/usr/bin/env python3
"""A stand-in for integrate_likelihood_extrinsic_batchmode, for tests of RIFT.ledger.engine.

It keeps the parts of ILE the engine depends on: a function `analyze_event` with locals
`indx_event` and `P`, one call of the real AV sampler per point, and a loop that catches
an exception in one point, prints FAILED ANALYSIS and goes on.  The integrand is a
Gaussian of width WIDTH in three dimensions times a factor that depends on the point, so

    ln L(point) = 3 ln(sqrt(2 pi) WIDTH) - (mc - 30)^2 / 2

    --stub-skip K      point K raises before it reaches the sampler
    --stub-double      a second sampler call per point, as a burn-in or replica option makes
    --stub-exit K      the script exits with status 3 before point K

Environment, for faults tied to a point and not to its place in a lease (M1 is the point's
m1 in solar masses, as printed with repr; `all` means every point):

    STUB_ABORT_PRE=M1       the process aborts before the sampler, as a GSL abort in waveform code does
    STUB_ABORT_SAMPLER=M1   the process aborts inside the sampler, as a lost GPU context does
    STUB_RAISE_SAMPLER=M1   the integrand raises, and the loop goes on to the next point
    STUB_GPU_SAMPLER=M1     the integrand raises MemoryError, which the loop re-raises as ILE does a GPU error
    STUB_REJECT_AFTER=M1    analyze_event raises after the sampler returned, as ILE does for a result it
                            rejects, and the loop goes on
    STUB_EMPTY_FIRST=M1     the integrand is -inf on its first chunk, and the sampler reports a collapsed
                            live volume
    STUB_CUDA_SAMPLER=M1    the integrand raises a CUDA error, on which the loop exits with status 62 as ILE does
    STUB_NAN_RESULT=M1      analyze_event returns nan after the sampler returned
    STUB_NO_SAMPLER=M1      analyze_event returns before it calls the sampler
    STUB_EXIT0=M1           the script exits with status 0 inside analyze_event
    STUB_CAUGHT_INSIDE=M1   analyze_event raises and catches an exception of its own, then goes on as usual
    STUB_LOST_AFTER=N       with STUB_COUNT_FILE=PATH: once N points have finished, counted in that file over
                            every process, each integrand call raises MemoryError, as after a lost device

M1 may be a comma list.
"""
import contextlib
import io
import os
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


def hit(name, P):
    want = os.environ.get(name)
    return want is not None and (want == "all" or repr(float(P.row[0])) in want.split(","))


def finished(add=0):
    """Points finished so far by every process that shares STUB_COUNT_FILE."""
    path = os.environ.get("STUB_COUNT_FILE")
    if not path:
        return 0
    n = (int(open(path).read() or 0) if os.path.exists(path) else 0) + add
    if add:
        with open(path, "w") as f:
            f.write(str(n))
    return n


def analyze_event(P_list, indx_event):
    P = P_list[indx_event]
    if indx_event == SKIP:
        raise ValueError("no waveform for this point")
    if hit("STUB_ABORT_PRE", P):
        os.abort()
    if hit("STUB_EXIT0", P):
        sys.exit(0)
    if hit("STUB_NO_SAMPLER", P):
        return 0.0
    if hit("STUB_CAUGHT_INSIDE", P):
        try:
            raise KeyError("a lookup ILE recovers from")
        except KeyError:
            pass
    lost = "STUB_LOST_AFTER" in os.environ and finished() >= int(os.environ["STUB_LOST_AFTER"])
    offset = analytic(P.row) - 3 * np.log(np.sqrt(2 * np.pi) * WIDTH)

    calls = [0]

    def ln_f(*xs):
        calls[0] += 1
        if hit("STUB_ABORT_SAMPLER", P):
            os.abort()
        if hit("STUB_RAISE_SAMPLER", P):
            raise FloatingPointError("the integrand failed")
        if hit("STUB_GPU_SAMPLER", P) or lost:
            raise MemoryError("the device is gone")
        if hit("STUB_CUDA_SAMPLER", P):
            raise RuntimeError("CUDA error: an illegal memory access was encountered")
        if calls[0] == 1 and hit("STUB_EMPTY_FIRST", P):
            return np.full(len(xs[0]), -np.inf)
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
    if hit("STUB_REJECT_AFTER", P):
        raise ValueError("this event is being dropped rather than exported")
    finished(add=1)
    return float("nan") if hit("STUB_NAN_RESULT", P) else float(out[0])


if __name__ == "__main__":
    SKIP = int(sys.argv[sys.argv.index("--stub-skip") + 1]) if "--stub-skip" in sys.argv else -1
    DOUBLE = "--stub-double" in sys.argv
    grid = np.loadtxt(sys.argv[sys.argv.index("--sim-grid") + 1], comments="#", ndmin=2)[:, 2:]
    points = [Point(g) for g in grid]
    stop_at = int(sys.argv[sys.argv.index("--stub-exit") + 1]) if "--stub-exit" in sys.argv else -1
    for indx in range(len(points)):
        if indx == stop_at:
            sys.exit(3)
        try:
            analyze_event(points, indx)
        except Exception as exc:
            if isinstance(exc, MemoryError):          # ILE re-raises what is_gpu_resource_error recognises
                raise
            if "CUDA" in str(exc):
                sys.exit(62)
            print("  ===> FAILED ANALYSIS <==== ", exc)
