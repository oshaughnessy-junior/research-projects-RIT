"""Lightweight worked example for the v2 simulation archive.

Builds a tiny archive at ./hello_archive (created in $TMPDIR by default),
registers three sims with a frozen generator, runs them via
LocalRunQueue, prints the manifest + index, then re-instantiates the
archive in a fresh Archive(...) call to confirm round-trip.

Run:
    python -m RIFT.simulation_manager.examples.sim_database_hello
or directly:
    python sim_database_hello.py [--base /path/to/archive]

This is intentionally minimal — the cluster-aware queues
(CondorRequestQueue / SlurmRunQueue / CondorRunQueue) are stubs in
database.py and should be fleshed out by porting the existing
CondorManager logic onto the new interfaces.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import tempfile
from pathlib import Path

from RIFT.simulation_manager.database import (
    Archive, Manifest, LocalRequestQueue, LocalRunQueue,
)


# ---- frozen generator ------------------------------------------------------
# This function is what the archive captures via inspect.getsource. It must
# be self-contained: standard-library imports only, no closure captures.

def my_generator(params, sim_dir):
    """Trivial generator: writes k * sqrt(2) to <sim_dir>/output.txt."""
    import math
    import os
    val = float(params) * math.sqrt(2.0)
    with open(os.path.join(sim_dir, "output.txt"), "w") as f:
        f.write("{:.17g}\n".format(val))


def my_summarizer(sim_dir, params):
    """Read the output and return a tiny summary dict for the index."""
    import os
    out = os.path.join(sim_dir, "output.txt")
    with open(out) as f:
        val = float(f.read().strip())
    return {"value": val, "params": params}


# ---- frozen similarity hooks ----------------------------------------------
# same_q decides "are these the same simulation"; lookup_key buckets params
# for fast dedup. Both must be self-contained (no closure captures).

def my_same_q(a, b):
    """Treat float params within 1e-9 absolute as the same sim."""
    return abs(float(a) - float(b)) < 1e-9


def my_lookup_key(p):
    """Bucket by float rounded to 9 decimals — coarse enough to put
    same_q-equivalent params in the same bucket, fine enough that
    near-misses don't collide."""
    return round(float(p), 9)


# ---- main ------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=None,
                    help="Archive directory (default: $TMPDIR/hello_archive)")
    ap.add_argument("--keep", action="store_true",
                    help="Don't delete the archive at the end")
    args = ap.parse_args()

    base = Path(args.base or (tempfile.gettempdir() + "/hello_archive"))
    if base.exists():
        shutil.rmtree(base)

    # 1. Build the archive: freeze the generator + summarizer, write the
    #    manifest, and create the on-disk skeleton.
    manifest = Manifest.new(name="hello",
                            request_queue_kind="local",
                            run_queue_kind="local",
                            summarizer_entrypoint="summarizer:my_summarizer",
                            same_q_entrypoint="same_q:my_same_q",
                            lookup_key_entrypoint="lookup_key:my_lookup_key",
                            params_schema={"type": "scalar", "dtype": "float"},
                            summary_schema={"fields": {"value": "float"}})
    archive = Archive(base_location=base,
                      manifest=manifest,
                      generator_spec=my_generator,
                      summarizer_spec=my_summarizer,
                      same_q_spec=my_same_q,
                      lookup_key_spec=my_lookup_key)
    print("Created archive at", base)
    print("Manifest:")
    print(json.dumps(archive.manifest.data, indent=2))

    # 2. Wire up local queues.
    run_q = LocalRunQueue()
    req_q = LocalRequestQueue(run_queue=run_q)
    archive.request_queue = req_q
    archive.run_queue = run_q

    # 3. Register three sims.
    expected = {}
    for k in (0.5, 1.5, 2.5):
        name = archive.register(k)
        expected[name] = k * math.sqrt(2.0)
        print(" Registered sim", name, "with params", k)

    # 3b. Demonstrate dedup. Re-requesting an exact param returns the same
    #     name; requesting a same_q-equivalent param (within 1e-9) also
    #     hits the same sim. Neither path creates a new sim_dir.
    same_exact = archive.register(0.5)
    same_close = archive.register(0.5 + 1e-12)
    different  = archive.register(0.5 + 1e-3)   # outside same_q tolerance
    assert same_exact == archive.index.by_name(same_exact)["name"] == "1"
    assert same_close == "1", "near-identical request should dedupe"
    assert different != "1", "request outside same_q tolerance should NOT dedupe"
    print(" dedup: 0.5 -> {}, 0.5+1e-12 -> {}, 0.5+1e-3 -> {} (new)".format(
        same_exact, same_close, different))

    # 4. Run them. LocalRequestQueue immediately hands all 'ready' sims to
    #    LocalRunQueue, which runs the frozen generator inline and writes
    #    output.txt + summary.json into each sim_dir.
    submitted = req_q.submit_pending(archive)
    print(" Submitted (=> ran) sims:", submitted)

    # 5. Show the index. This is the cheap, canonical view.
    print("\nindex.jsonl:")
    print(archive.index.path.read_text())

    # 6. Verify outputs match expectation.
    bad = []
    for name, ref in expected.items():
        got = archive.index.by_name(name)["summary"]["value"]
        if abs(got - ref) > 1e-12:
            bad.append((name, got, ref))
    if bad:
        print("FAIL:", bad)
        sys.exit(1)

    # 7. Re-instantiate the archive in a fresh Archive(...) call (no
    #    manifest/code passed — pure rehydration) and confirm we recover
    #    everything.
    archive2 = Archive(base_location=base)
    statuses = {r["name"]: r["status"] for r in archive2.index.all()}
    print("\nRehydrated statuses:", statuses)
    assert all(s == "complete" for s in statuses.values()), statuses

    # The frozen generator is callable from the rehydrated archive — no
    # access to the original source needed.
    gen = archive2.load_generator()
    assert callable(gen)
    print("Frozen generator callable from rehydrated archive: OK")

    print("\nPASS: end-to-end archive round trip.")
    if not args.keep:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
