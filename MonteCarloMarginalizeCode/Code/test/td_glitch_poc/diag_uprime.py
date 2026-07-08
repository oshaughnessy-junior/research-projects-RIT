#!/usr/bin/env python
"""diag_uprime.py -- the scalar-U' (glitch-overlap) honesty diagnostic for the G4 PoC.

WHAT IS BEING TESTED.  The G3 wiring applies the glitch U' correction as a SCALAR
(evaluated at the template's storage placement) while the exact correction is
time-dependent:  U'(t) = U - beta(t)^H M^{-1} beta(t),  beta(t) = Phi^T T_t h.  When the
glitch overlaps the signal's arrival, beta(arrival) != beta(storage placement) and the
scalar approximation under-corrects U by exactly the "glitch absorbs template power"
fraction at the arrival time.  (The Q'(t) data-side correction is a full time series and
is EXACT; only U' is approximated.  See GLITCH_MARG_DESIGN.md Sec. 2 and NEXT.md T6/G3.)

THIS SCRIPT measures the quantity the approximation drops: with the PoC basis attached to
the L1 context, it slides the physical truth template across the +-0.15 s marginalization
window around its true L1 arrival and reports

    frac(t) = [beta(t)^H M^{-1} beta(t)] / U0     (fraction of template power absorbed)

at the window extremes, the arrival, and the maximum over the window.  max|frac| is the
relative lnL-scale error the scalar-U' path can make for arrival-time samples where the
template sits on the glitch; compare with the disjoint-blip configuration where frac ~ 0.

Usage: diag_uprime.py --workdir <scene dir> [--basis <npz>] [--shifts -0.15,0,0.15,...]
"""
import argparse
import json
import os
import sys

import numpy as np

import lal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "td_ringdown_demo"))
import native_inject                                       # noqa: E402
import RIFT.lalsimutils as lsu                             # noqa: E402
from RIFT.likelihood.td_inner_product import TDInnerProductFactory  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", required=True,
                    help="scene dir (truth.json, frames_glitch.cache, glitch_basis_L1.npz)")
    ap.add_argument("--basis", default=None, help="basis npz (default <workdir>/glitch_basis_L1.npz)")
    ap.add_argument("--shifts", default="-0.15,-0.075,0.0,0.075,0.15",
                    help="template time shifts [s] around the true L1 arrival")
    args = ap.parse_args()
    W = args.workdir
    basis = args.basis or os.path.join(W, "glitch_basis_L1.npz")
    truth = json.load(open(os.path.join(W, "truth.json")))
    fs = truth["srate"]; dt = 1.0 / fs

    psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0.), 0., 0.125,
                                         lsu.lsu_HertzUnit, 8193)
    psd.data.data = native_inject.analytic_psd(np.arange(8193) * 0.125)
    d_td = lsu.frame_data_to_hoft(os.path.join(W, "frames_glitch.cache"),
                                  "L1:FAKE-STRAIN", verbose=False)
    ts = lal.CreateCOMPLEX16TimeSeries("x", d_td.epoch, 0., d_td.deltaT,
                                       lsu.lsu_DimensionlessUnit, d_td.data.length)
    ts.data.data = np.asarray(d_td.data.data, dtype=complex)
    data_fd = lsu.DataFourier(ts)
    fac = TDInnerProductFactory(solver="auto", glitch_basis_files={"L1": basis},
                                glitch_sigma="profile")
    fac.register_detector_data("L1", psd, data_fd, event_time_geo=truth["tref"])
    ctx = fac.context_for(truth["fmin"], 1000.0, fs / 2.0, data_fd.deltaF, psd,
                          analyticPSD_Q=False)

    # the physical truth template at its TRUE L1 placement (the injection itself, no noise)
    P = native_inject.build_injection_P(
        truth["m1"], truth["m2"], truth["dist"], truth["incl"], truth["phiref"],
        truth["dec"], truth["ra"], truth["psi"], truth["tref"], truth["fmin"],
        truth["fmin"], "IMRPhenomD", fs, truth["seglen"], "L1")
    h = native_inject.native_detector_strain(P, 2)
    h_seg = native_inject._place_in_window(h, truth["start"], fs, truth["seglen"])
    h0 = np.asarray(h_seg.data.data)

    U0 = float(np.real(ctx.ip_normalization * ctx.dot_td(h0, h0)))
    rows = []
    for s in (float(x) for x in args.shifts.split(",")):
        n = int(round(s / dt))
        hs = np.roll(h0, n)
        corr = float(np.real(ctx.glitch_ip_correction(hs, hs)))
        rows.append((s, corr / U0))
        print("[U'] shift %+0.3f s : absorbed-power fraction beta^H M^-1 beta / U0 = %.4e"
              % (s, corr / U0))
    fr = np.array([r[1] for r in rows])
    print("[U'] U0 = %.4f ; max fraction over window = %.4e ; at-arrival = %.4e ; "
          "window variation (max-min) = %.4e"
          % (U0, fr.max(), dict(rows)[0.0] if 0.0 in dict(rows) else float("nan"),
             fr.max() - fr.min()))
    out = dict(U0=U0, shifts=[r[0] for r in rows], absorbed_fraction=[r[1] for r in rows],
               max_fraction=float(fr.max()), variation=float(fr.max() - fr.min()))
    with open(os.path.join(W, "uprime_diag.json"), "w") as fh:
        json.dump(out, fh, indent=2)
    print("[U'] wrote %s" % os.path.join(W, "uprime_diag.json"))


if __name__ == "__main__":
    main()
