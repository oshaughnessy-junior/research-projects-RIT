#!/usr/bin/env python3
"""Stand-in MARG worker for run_lambda_loop_gate.py: an analytic lnL(lambda), no data.

It speaks ILE's indexed-grid contract (``--sim-grid --event --n-events-to-analyze
--output-file``, one Hyperpipe row per point in ``<output-file>_<event>_.dat``, every grid
column echoed), and refuses unless its command line is the one pseudo_pipe built for the
extra-parameter seam: all four NoLoop flags and ``--supplementary-waveform-transform-code``.
Every declared lambda (plugin ``EXTRA_PARAMETERS`` plus ``-parameter``) must be a finite
column of the grid it is handed -- the property the loop must preserve.

lnL = -(A - A_PEAK - SLOPE (mc - MC_REF))^2 / (2 SIGMA^2) - (mc - MC_REF)^2 / (2 MC_W^2), with
A = lens_amplitude: a Gaussian in (mc, A) with a strong mc-A correlation, so a row whose
lambda is paired with the wrong intrinsic point changes lnL and breaks the correlation.
"""
import argparse
import importlib.util
import sys

import numpy as np

from RIFT.misc import hyperpipeline_io as hpio

A_PEAK, SIGMA = 1.2, 0.05
SLOPE, MC_REF, MC_W = 0.2, 23.0, 1.0
NOLOOP = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sim-grid", required=True)
    p.add_argument("--output-file", required=True)
    p.add_argument("--event", type=int, default=0)
    p.add_argument("--n-events-to-analyze", type=int, default=1)
    p.add_argument("--supplementary-waveform-transform-code", required=True)
    p.add_argument("--supplementary-waveform-transform-parameter", action="append", default=[])
    opts, rest = p.parse_known_args()
    missing = [f for f in NOLOOP if f not in sys.argv]
    if missing:
        sys.exit("lambda_marg_stub: not a NoLoop line, missing {}".format(missing))
    spec = importlib.util.spec_from_file_location("_stub_plugin", opts.supplementary_waveform_transform_code)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    names = list(getattr(mod, "EXTRA_PARAMETERS", ())) + list(opts.supplementary_waveform_transform_parameter)
    if not hpio.sniff(opts.sim_grid):
        sys.exit("lambda_marg_stub: --sim-grid {} is not a Hyperpipe grid".format(opts.sim_grid))
    grid, cols = hpio.read_table(opts.sim_grid)
    grid = np.atleast_1d(grid)
    lost = [n for n in names if n not in cols]
    if lost:
        sys.exit("lambda_marg_stub: grid {} lost extra parameter column(s) {} (columns {})".format(
            opts.sim_grid, lost, list(cols)))
    stop = min(len(grid), opts.event + opts.n_events_to_analyze)
    for i in range(opts.event, stop):
        row = grid[i]
        lam = dict((n, float(row[n])) for n in names)
        if not all(np.isfinite(v) for v in lam.values()):
            sys.exit("lambda_marg_stub: non-finite lambda {} in row {}".format(lam, i))
        values = [float(row[c]) for c in cols]
        m1, m2 = float(row["m1"]), float(row["m2"])
        mc = (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2
        values[cols.index("lnL")] = (-0.5 * ((lam["lens_amplitude"] - A_PEAK - SLOPE * (mc - MC_REF)) / SIGMA) ** 2
                                     - 0.5 * ((mc - MC_REF) / MC_W) ** 2)
        values[cols.index("sigma_lnL")] = 0.01
        out = "{}_{}_.dat".format(opts.output_file, i)
        hpio.write_row(out, cols, values)
        print("lambda stub output:", out, lam)


if __name__ == "__main__":
    main()
