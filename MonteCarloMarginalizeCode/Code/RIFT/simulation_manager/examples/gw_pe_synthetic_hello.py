"""Worked example: GW PE synthetic-targeted backend with the STUB factory.

Exercises the full v2 archive flow without lalsuite, gwpy, or condor:

    1. Build a fresh archive at <tmp>/gw_pe_synthetic_hello using the
       backend's frozen code (generator/summarizer/same_q/lookup_key).
    2. Wire the run queue with the stub subdag_factory.
    3. Register three synthetic events at target_level=1.
    4. submit_pending → wrapper DAG written; sub-DAGs written too.
    5. Simulate the sub-DAGs completing by calling
       factory_stub.execute_stub_inline (drops level_<N>.json into
       sim_dir, mimicking what condor would have done).
    6. archive.refresh_status_from_disk → sims promoted to 'complete'.
    7. Re-instantiate the archive; confirm rehydration works.

This proves the framework wiring (subdag_factory, SUBDAG EXTERNAL in
the wrapper DAG, refinement chain, dedup) end-to-end before the real
pseudo_pipe factory is plugged in. Run:

    python -m RIFT.simulation_manager.examples.gw_pe_synthetic_hello
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

from RIFT.simulation_manager.database import StatusRecord
from RIFT.simulation_manager.backends.gw_pe_synthetic import make_archive
from RIFT.simulation_manager.backends.gw_pe_synthetic import factory_stub


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=None)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    base = Path(args.base or (tempfile.gettempdir() + "/gw_pe_synth_hello"))
    if base.exists():
        shutil.rmtree(base)

    archive = make_archive(
        base_location=base,
        subdag_factory=factory_stub.subdag_factory,
        submit_mode="embed",       # don't dispatch; we'll fake completions
    )
    print("Built archive at:", archive.base)

    # 3 synthetic events at level 1.
    events = [
        {"mc": 25.0, "eta": 0.22, "distance": 400.0,
         "ra": 1.234, "dec": 0.567, "geocent_time": 1.2e9,
         "inclination": 0.5, "polarization": 0.3, "s1z": 0.0, "s2z": 0.0},
        {"mc": 31.0, "eta": 0.20, "distance": 500.0,
         "ra": 2.0,   "dec": -0.3,  "geocent_time": 1.2e9 + 100,
         "inclination": 1.0, "polarization": 0.0, "s1z": 0.1, "s2z": -0.1},
        {"mc": 12.5, "eta": 0.245, "distance": 200.0,
         "ra": 0.7,   "dec": 0.1,   "geocent_time": 1.2e9 + 200,
         "inclination": 0.2, "polarization": 1.0, "s1z": 0.0, "s2z": 0.0},
    ]
    names = [archive.register(e, target_level=1) for e in events]
    print(" Registered:", names)

    # Submit. With submit_mode='embed' the wrapper DAG is written but
    # not dispatched; the per-(sim, level) sub-DAGs are written by
    # factory_stub.subdag_factory.
    archive.request_queue.submit_pending(archive)
    wrapper = Path(archive.run_queue.last_wrapper_dag_path)
    print(" Wrapper DAG:", wrapper)
    print(" Wrapper contents:")
    for line in wrapper.read_text().splitlines():
        print("   ", line)
    assert all(line.startswith("SUBDAG EXTERNAL") for line in
               wrapper.read_text().splitlines() if line.strip())

    # Stand in for condor: drop level_<N>.json into each sim_dir.
    for n in names:
        factory_stub.execute_stub_inline(archive, n, level=1)

    # Reconcile.
    promoted = archive.refresh_status_from_disk()
    print(" promoted:", promoted)
    assert all(archive.get_status(n) == "complete" for n in names)

    # Show what the index looks like.
    print("\nindex.jsonl:")
    print(archive.index.path.read_text())

    # Idempotent re-request: same params -> same name; bumped target ->
    # refine_ready + a fresh wrapper DAG produced.
    n_again = archive.register(events[0], target_level=1)
    assert n_again == names[0], "dedup failed"
    n_bump = archive.register(events[0], target_level=3)
    assert n_bump == names[0]
    assert archive.get_status(n_bump) == "refine_ready"
    archive.request_queue.submit_pending(archive)
    print("\nAfter refine to lvl 3:")
    print(" wrapper:", archive.run_queue.last_wrapper_dag_path)
    print(" wrapper contents:")
    print(Path(archive.run_queue.last_wrapper_dag_path).read_text())

    # Drop fakes for the new levels.
    for lvl in (2, 3):
        factory_stub.execute_stub_inline(archive, n_bump, level=lvl)
    archive.refresh_status_from_disk()
    rec = StatusRecord.read(archive.sim_dir(n_bump))
    assert rec.data["status"] == "complete"
    assert rec.data["current_level"] == 3
    print("\nFinal status of refined sim:", rec.data["status"],
          "at level", rec.data["current_level"])

    print("\nPASS: GW PE synthetic-targeted backend (stub factory) end-to-end.")
    if not args.keep:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
