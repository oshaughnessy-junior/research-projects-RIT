#!/usr/bin/env python
"""Regenerate the CALIBRATION table in test/test_e2e_analytic_pipeline.py.

The tolerances in that file (Z_TOLERANCE, MAX_SIGMA, MIN_NEFF) are derived from a sweep over
seeds, and the table recording it is a comment.  A comment is a claim nobody can check.  This
script re-derives it:

    python make_e2e_calibration.py --seeds 8          # the table as committed
    python make_e2e_calibration.py --seeds 2 --lane distance-marginalized

It imports the gate's OWN _run_ile, build_event and _exact rather than reimplementing them, so
a lane measured here is the lane the gate runs.  Reimplementing them would produce a table that
calibrates a different thing and agrees with itself.

Cost: about 10 s per arm, so the full 8-seed table is roughly 20 minutes on one core.  That is
why this lives here and not in CI.

If you add a lane to the gate, add it to LANES below, or the table silently stops covering it.
"""
import argparse
import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_TEST_DIR = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _TEST_DIR not in sys.path:
    sys.path.insert(0, _TEST_DIR)

import test_e2e_analytic_pipeline as gate          # noqa: E402

_AV, _PORTFOLIO, _GMM = gate._AV, gate._PORTFOLIO, gate._GMM

# (label, sampler argv, A, B, exact-A, exact-B, extra kwargs for _run_ile)
LANES = [
    ("prior-only,    AV",                        _AV,        None, 0.0, {}),
    ("prior-only,    portfolio",                 _PORTFOLIO, None, 0.0, {}),
    ("prior-only,    GMM",                       _GMM,       None, 0.0, {}),
    ("A=0.75 B=0,    AV",                        _AV,        0.75, 0.0, {}),
    ("A=0.75 B=0,    portfolio",                 _PORTFOLIO, 0.75, 0.0, {}),
    ("A=0.75 B=0,    GMM",                       _GMM,       0.75, 0.0, {}),
    ("A=8    B=0,    AV",                        _AV,        8.0,  0.0, {}),
    ("A=8    B=0,    portfolio",                 _PORTFOLIO, 8.0,  0.0, {}),
    ("A=8    B=0,    GMM",                       _GMM,       8.0,  0.0, {}),
    ("A=0.75 B=3,    AV",                        _AV,        0.75, 3.0, {}),
    ("A=0.75 B=3,    portfolio",                 _PORTFOLIO, 0.75, 3.0, {}),
    ("A=0.75 B=3,    GMM",                       _GMM,       0.75, 3.0, {}),
    ("A=8    B=2,    AV (the survives-swap lanes)", _AV,     8.0,  2.0, {}),
    ("raw inclination contract (cosine sampler)", _AV,       8.0,  2.0,
     dict(incl_is_cosine=True, extra=("--inclination-cosine-sampler",))),
    ("time-marginalized portfolio",              _PORTFOLIO, 8.0,  2.0,
     dict(extra=("--time-marginalization",))),
    ("distance-marginalized",                    _AV,        8.0,  2.0, {"_needs_dmarg": True}),
    ("adaptive_cartesian, --n-max 60000",
     ["--sampler-method", "adaptive_cartesian"], 8.0, 2.0, dict(n_max=60000)),
]


def _dmarg_extra(event, out):
    import subprocess
    table = out / "marg_lookup.npz"
    proc = subprocess.run(
        [sys.executable, gate.MARG_TABLE_TOOL] + gate.DMARG_TABLE_ARGS + ["--out", str(table)],
        cwd=str(out), env=gate._child_env(), stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, timeout=1800)
    if proc.returncode != 0 or not table.exists():
        raise SystemExit("util_InitMargTable failed:\n%s" % proc.stdout.decode()[-1500:])
    return dict(extra=("--distance-marginalization",
                       "--distance-marginalization-lookup-table", str(table),
                       "--d-min", "100", "--d-max", "1000",
                       "--time-marginalization", "--vectorized", "--gpu", "--force-xpy"))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--first-seed", type=int, default=1000)
    ap.add_argument("--lane", default=None, help="substring; measure only matching lanes")
    args = ap.parse_args()

    import pathlib
    out = pathlib.Path(tempfile.mkdtemp(prefix="e2e_calibration_"))
    event = gate.build_event(out)
    if event is None:
        raise SystemExit("could not build the fixture (lal_path2cache missing or failed)")

    seeds = [args.first_seed + i for i in range(args.seeds)]
    selected = [L for L in LANES if not args.lane or args.lane in L[0]]
    if not selected:
        # Exiting 0 having measured nothing, under a summary line that reads like a clean
        # result, is the exact shape this whole branch exists to remove.
        raise SystemExit("--lane %r matched none of:\n  %s"
                         % (args.lane, "\n  ".join(L[0].strip() for L in LANES)))
    print("#   lane                                      max |z|   max sigma   min n_eff")
    worst_z = worst_s = 0.0
    least_n = float("inf")
    for label, sampler, a, b, kw in selected:
        kw0 = kw
        kw = dict(kw)
        if kw.pop("_needs_dmarg", False):
            kw.update(_dmarg_extra(event, out))
        exact = 0.0 if a is None else gate._exact(a, b)
        zs, sigs, neffs = [], [], []
        for seed in seeds:
            tag = "cal_%d_%d" % (LANES.index((label, sampler, a, b, kw0)), seed)
            lnL, sigma, neff = gate._run_ile(event, tag, sampler, a_coeff=a, b_coeff=b,
                                             seed=seed, **kw)
            zs.append(abs((lnL - exact) / sigma))
            sigs.append(sigma)
            neffs.append(neff)
        worst_z = max(worst_z, max(zs))
        worst_s = max(worst_s, max(sigs))
        least_n = min(least_n, min(neffs))
        print("#   %-44s%5.2f      %6.4f      %6.0f"
              % (label, max(zs), max(sigs), min(neffs)))
    print("#")
    print("# across all lanes: worst |z| %.2f, worst sigma %.4f, least n_eff %.0f"
          % (worst_z, worst_s, least_n))
    # Printed, not deleted: a failing lane is worth inspecting.  /tmp is small on the CIT
    # nodes, so clean up when you are done.
    print("# fixture kept at %s   (rm -rf it when done)" % out)


if __name__ == "__main__":
    main()
