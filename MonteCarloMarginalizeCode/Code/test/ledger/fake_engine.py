#!/usr/bin/env python3
"""A stand-in engine for tests of the ledger controller: a likelihood with a known answer.

    fake_engine.py LEASE.dat KEYS.json SHARD.jsonl

ln L = ln N(A; MU, COV) - ln p(A), so the posterior of block A is the Gaussian N(MU, COV)
and the evidence is exactly 1.  Each row gets mean-one lognormal noise fixed by its key,
as the real engine seeds its sampler by the key.  LEDGER_FAKE_SLEEP seconds pass per row
so a test can kill the process inside a lease.  LEDGER_FAKE_UNKEYED=1 draws the noise
from the clock: the mutant a kill test must catch.  LEDGER_FAKE_FAIL=f reports a fraction f
of the keys as FAILED.
"""
import json
import os
import sys
import time

import numpy as np

from RIFT.ledger.proposal import ln_normal
from RIFT.ledger.space import Box, from_grid

MU = np.array([30.0, 0.25, 0.30, -0.30])
SD = np.array([1.0, 0.05, 0.06, 0.06])
RHO = 0.6                      # between mc and s1z
SIGMA = 0.10                   # noise of one evaluation, nats
COV = np.diag(SD ** 2)
COV[0, 2] = COV[2, 0] = RHO * SD[0] * SD[2]
BOX = dict(mc_range=[21.4, 42.0], eta_range=[0.0454, 0.25], chi_max=0.99)


def ln_like(box, grid):
    a = from_grid(grid)[0]
    return ln_normal(a, MU, np.linalg.cholesky(COV)) - box.ln_prior_A(a)


def main():
    lease, keys, shard = sys.argv[1:4]
    meta = json.load(open(keys))
    grid = np.loadtxt(lease, comments="#", ndmin=2)[:, 2:]
    val = ln_like(Box.from_json(BOX), grid)
    pause = float(os.environ.get("LEDGER_FAKE_SLEEP", "0"))
    loose = np.random.default_rng() if os.environ.get("LEDGER_FAKE_UNKEYED") else None
    FAIL = float(os.environ.get("LEDGER_FAKE_FAIL", "0"))        # fraction of keys reported FAILED, fixed by the key
    versions = meta.get("versions") or [meta["version"]] * len(grid)
    if os.environ.get("LEDGER_FAKE_OFF_NOLOOP"):
        print(" this is the old vectorized code path not the xpy path")
    with open(shard, "a") as f:
        for (s, i), ver, g, v in zip(meta["keys"], versions, grid, val):
            if pause:
                time.sleep(pause)
            if FAIL and np.random.default_rng([meta["seed"], 88, ver, s, i]).random() < FAIL:
                f.write(json.dumps(dict(v=ver, s=s, i=i, status="FAILED", lnL=None)) + "\n")
                continue
            z = (loose or np.random.default_rng([meta["seed"], 77, ver, s, i])).standard_normal()
            row = dict(v=ver, s=s, i=i, status="OK", lnL=float(v + SIGMA * z - 0.5 * SIGMA ** 2), sigma=SIGMA,
                       point=[float(x) for x in g])
            f.write(json.dumps(row) + "\n")
            f.flush()


if __name__ == "__main__":
    main()
