# Simulation archive design

This document describes the on-disk format and core abstractions for a
RIFT simulation archive. The goals are:

1. **Self-contained.** An archive is a directory you can copy, ship, and
   read months later without access to the originating source tree.
2. **Cheap to query.** Listing what's in the archive, what's complete,
   and basic per-sim summaries should not require opening every output
   file. There is a single `index.jsonl` for that.
3. **Two-tier queueing.** The queue used to *request* simulations is
   independent of the queue used to *run* them. A typical OSG setup is
   "Condor DAG on the submit host requests sims; a Slurm cluster runs
   them." A test setup might be "local request, local run." The archive
   does not care.
4. **Frozen code.** The user-supplied generator is captured at archive
   creation time and stored under `code/`. Workers `exec` the frozen
   copy. The archive is portable without the original repository.

The existing classes in `BaseManager.py` / `CondorManager.py` /
`SlurmManager.py` evolve toward this design; the new schemas live
alongside them in `database.py` (stub, this commit) and can be adopted
incrementally.


## On-disk layout

```
<base_location>/
  manifest.json                 # archive-level metadata (schema, name, code refs, queue config)
  code/                         # frozen interfaces — required to *run* workers, not to read summaries
    generator.py                # entrypoint: generator.run(params, sim_dir) -> writes output
    summarizer.py               # OPTIONAL: summarize(sim_dir) -> dict (used post-completion)
    requirements.txt            # OPTIONAL: package versions known at archive creation
    README                      # human note: what this archive is, how it was built
  index.jsonl                   # one JSON object per line, one line per sim — canonical and cheap
  sims/
    <sim_name>/                 # one subdirectory per sim (sim_name is the archive's internal id)
      params.json               # canonical input parameters
      status.json               # FSM state + per-queue tracking + history
      summary.json              # summarizer output (written when status -> complete)
      output.<ext>              # raw output written by the worker; format up to the generator
      logs/
        worker.stdout
        worker.stderr
        run.log                 # generator-internal logging if any
  request_queue/                # workspace for the queue that *requests* sims
    dags/                       # condor DAGs (when request_queue.kind == 'condor')
    *.sub
  run_queue/                    # workspace for the queue that *runs* sims
    workers/                    # generated per-sim worker scripts
    submit_files/
  state.json                    # cached cluster ids, last-poll timestamps, etc.
```

Anything that's "configuration we may want to overwrite atomically"
lives at the archive root; anything per-sim lives under
`sims/<sim_name>/`. Anything queue-system-specific lives under
`request_queue/` or `run_queue/` — the archive itself stays neutral.


## Manifest

`manifest.json` at the archive root, written once at archive creation
and updated only when archive-level configuration changes (e.g. a queue
swap). Schema:

```jsonc
{
  "schema_version": 1,
  "name": "GW190521_grid_v3",
  "created_at": "2026-04-26T15:00:00Z",
  "rift_version": "0.0.18.0",
  "code": {
    "generator":            "code/generator.py",
    "generator_entrypoint": "generator:run",       // module:callable
    "summarizer":           "code/summarizer.py",
    "summarizer_entrypoint":"summarizer:summarize",
    "requirements":         "code/requirements.txt"
  },
  "request_queue": { "kind": "condor", "extra": { } },
  "run_queue":     { "kind": "slurm",  "extra": { "partition": "compute" } },
  "params_schema": { "type": "scalar", "dtype": "float" },   // hint only
  "summary_schema": { "fields": { "value": "float", "wall_time_s": "float" } }
}
```

The split between `code.generator_entrypoint` ("module:callable")
matches the `setuptools` convention and works equally well for a
single-file generator and a multi-file package frozen under `code/`.


## Index

`index.jsonl` is one record per sim. It is the **fast, canonical view**:
listing the archive, filtering by status, computing aggregate summaries
all use this file and never touch `sims/<sim_name>/` directly. The full
per-sim story lives in `status.json` + `summary.json`; the index is a
projection.

```jsonl
{"name":"1","status":"complete","params":0.5,"summary":{"value":0.7071}}
{"name":"2","status":"running", "params":1.5,"summary":null}
{"name":"3","status":"ready",   "params":2.5,"summary":null}
```

Append-only with periodic compaction. A run that registers many sims
appends one line per sim under a single index lock; status updates
compact (rewrite) the file under the same lock. Compaction is cheap
because the file is JSON-lines and rarely larger than a few MB even
for 100k sims.


## Per-sim status

`sims/<sim_name>/status.json`. Contains the FSM state plus per-queue
tracking and a transition log. The FSM is the same as today's
`QUEUE_STATES`:
`ready -> submit_ready -> running -> complete | stuck`.

```jsonc
{
  "name": "1",
  "params": 0.5,
  "status": "running",
  "history": [
    { "status": "ready",        "ts": "2026-04-26T15:00:00Z" },
    { "status": "submit_ready", "ts": "2026-04-26T15:00:05Z" },
    { "status": "running",      "ts": "2026-04-26T15:00:30Z" }
  ],
  "request_queue": { "kind": "condor", "cluster_id": 12345, "proc_id": 0, "dag_node": "sim_1" },
  "run_queue":     { "kind": "slurm",  "job_id": 98765, "host": "compute-7" },
  "output": { "path": "sims/1/output.npy", "size": null, "sha256": null },
  "started_at":   "2026-04-26T15:00:30Z",
  "completed_at": null
}
```

The `request_queue` and `run_queue` blocks are independent — the same
sim can be tracked in two systems at once. `kind` matches the value in
the manifest's queue spec.


## Frozen code

At archive creation, the user supplies one of:

1. **A Python callable.** The archive captures `inspect.getsource(fn)`
   and writes `code/generator.py` with a `run(params, sim_dir)`
   entrypoint. Closure captures and unusual import scopes are *not*
   supported; the function must be self-contained.
2. **A path to a `.py` file.** Copied verbatim. The user is responsible
   for naming the entrypoint per the manifest.
3. **A spec dict.** `{ "module_path": "...", "entrypoint": "mod:fn",
   "extra_files": ["aux.py", "data.csv"] }`. All listed files are copied
   into `code/`; `module_path` becomes `code/generator.py` (or whatever
   the spec names).

A worker invocation is then approximately:

```bash
python -c "import sys; sys.path.insert(0, 'code'); \
  from generator import run; run(params=..., sim_dir='sims/1')"
```

— so the archive is runnable without RIFT being installed, given a
Python interpreter and whatever the generator's own deps are. The
optional `code/requirements.txt` documents those.

Reading **summaries** does not need `code/` at all — `summary.json` is
plain JSON. Reading **outputs** may need `code/` if the format is
opaque (e.g. pickled objects), so a generator that writes `.npy`/`.h5`/
JSON is preferred.


## Similarity / dedup

Users come back. They request "the same" sim again. Sometimes "the
same" means bit-identical params; sometimes it means "the same up to
floating-point noise"; sometimes it means "the same up to a domain-
specific tolerance" (mass within 1e-6 solar masses, spin within 1e-3,
etc.). The archive should not redo work in any of these cases.

Two user-supplied callables, both optional, both frozen alongside the
generator under `code/`:

```python
def same_q(params_a, params_b) -> bool:
    """Reflexive, symmetric, transitive equality on parameters.
    Defaults to exact equality (params_a == params_b)."""

def lookup_key(params) -> Hashable:
    """Maps params to a coarse hashable bucket for fast dedup.
    Must be consistent with same_q: same_q(a, b) == True implies
    lookup_key(a) == lookup_key(b). Defaults to str(params)."""
```

These together give O(1) average lookup: bucket by `lookup_key`, then
run `same_q` against only the (typically zero or one) entries in that
bucket. The archive keeps an in-memory `{lookup_key: [sim_name, ...]}`
map, rebuilt from `index.jsonl` on rehydration.

Each row in `index.jsonl` carries its `lookup_key` so the dedup index
can be rebuilt without ever calling the user's code (useful when
inspecting an archive on a machine that doesn't have the generator's
runtime deps).

`Archive.register(params)` becomes idempotent under same_q: it returns
the existing sim's name when a match is found, registers a fresh sim
otherwise. `Archive.find_existing(params) -> Optional[str]` is exposed
for callers that want to look without registering.

The manifest grows two optional `code.*` entries:

```jsonc
"code": {
  "generator": "code/generator.py",         "generator_entrypoint": "generator:run",
  "same_q":    "code/same_q.py",            "same_q_entrypoint":    "same_q:same_q",
  "lookup_key":"code/lookup_key.py",        "lookup_key_entrypoint":"lookup_key:lookup_key"
}
```

Typical usage, gravitational-wave style:

```python
def lookup_key(p):
    # Round into a coarse grid; tolerance below same_q's threshold.
    return (round(p["mtotal"], 4),
            round(p["q"],      4),
            round(p["chi1z"],  3),
            round(p["chi2z"],  3))

def same_q(a, b):
    return (abs(a["mtotal"] - b["mtotal"]) < 1e-3 and
            abs(a["q"]      - b["q"])      < 1e-3 and
            abs(a["chi1z"]  - b["chi1z"])  < 1e-2 and
            abs(a["chi2z"]  - b["chi2z"])  < 1e-2)
```


## Two-tier queueing

Two interfaces, composed by the archive. The same archive can mix and
match: `(LocalRequestQueue, LocalRunQueue)` for tests,
`(CondorRequestQueue, SlurmRunQueue)` for an OSG-fed cluster,
`(CondorRequestQueue, CondorRunQueue)` if the request and run queues
are the same condor pool, etc.

```python
class RequestQueue:
    """Decides which sims should next be sent to the run queue, and
    tracks their state in the request system. The request queue does
    NOT execute sims; it forwards them to the run queue."""
    kind: str
    def submit_pending(self, archive) -> list[str]: ...   # 'ready'   -> 'submit_ready'/'running'
    def poll(self, archive)            -> dict[str, str]: ... # name -> request-side status

class RunQueue:
    """Actually runs sims (writes the output file)."""
    kind: str
    def build_worker(self, archive, sim_name) -> str: ...      # path to per-sim worker script
    def submit(self, archive, sim_names)      -> list[tuple[str, str]]: ...   # (name, run_queue_id)
    def poll(self, archive, sim_names)        -> dict[str, str]: ... # name -> run-side status
```

In a production OSG setup the implementations look like:
- `CondorRequestQueue.submit_pending`: builds a DAG over all `ready`
  sims, calls `condor_submit_dag`, stamps `submit_ready`.
- `CondorRequestQueue.poll`: queries the schedd via `_htcondor_module`
  (htcondor or htcondor2), updates `request_queue.cluster_id` etc.
- `SlurmRunQueue.build_worker`: writes `run_queue/workers/<name>.sh`
  that loads `code/generator.py` and calls `run(params, sim_dir)`.
- `SlurmRunQueue.submit`: `sbatch` per worker, capture job ids.
- `SlurmRunQueue.poll`: `squeue --json` parse → status.

The archive composes them and owns the FSM transitions: queue
implementations *report* state, the archive *applies* it.


## Lightweight example

`examples/sim_database_hello.py` registers three sims (params 0.5,
1.5, 2.5), uses `LocalRequestQueue + LocalRunQueue` (which run the
generator inline, no schedd), inspects `manifest.json` and
`index.jsonl`, and reloads the archive in a fresh `Archive(...)` call
to confirm everything round-trips. This is the smallest end-to-end
exercise of the design and should always pass without any cluster.
