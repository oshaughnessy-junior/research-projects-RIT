# Resource profiles: planning ILE matching from site records

Code: `resource_profile.py` (this directory); asimov hook
`RIFT/asimov/rift.py` (`scheduler: resource profile:`); CLI
`bin/util_RIFT_resource_profile.py`. Tests: `test/test_resource_profile.py`,
`test/asimov_integration/test_asimov_rift_resource_profile.py`.

## Problem

An ILE worker runs a fixed number of intrinsic points serially on one GPU. The
number is fixed when the DAG is built (`ile-jobs-per-worker`), and so is the
GPU requirement (`RIFT_REQUIRE_GPUS`). The pool is not uniform. Cards differ
by 8x in memory (4 to 32 GB) and several-fold in draws per second, and the small, slow cards are
the most numerous.

The IR1 review runs showed both failure modes. With a high-memory requirement,
throughput depends on a few large cards. With the requirement relaxed, 4 GB
cards crash near full memory, and a slow card gates the whole iteration.

## Evidence: memory follows draws, not n chunk

Numbers are from the IR1 v0 archive (rc13, Q2, 4096 Hz) and personal probes
(2026-11-02/03). Inputs and scripts are in
`~/data_products/ir1_resource_profile/` on CIT. Device memory is condor
`GpusMemoryUsage`, host memory is `MemoryUsage`, and x is millions of draws in
the worker.

| Card | CUDA build | Workers | Device MB | Host MB |
|---|---|---|---|---|
| A10 | 11.8 | 771 | 292x + 529 (r=0.91) | 424x + 436 (r=0.97) |
| V100 | 11.8 | 204, two GPUs | 270x + 835 (r=0.73) | 664x + 741 (r=0.89) |
| RTX 6000 | 11.8 | 1864, two GPUs | 241x + 1364 (r=0.53) | 590x + 1095 (r=0.88) |
| Blackwell SFF | 12.9 | 60 | 1x + 1211 (r=0.21) | 12x + 924 (r=0.73) |

For the two-GPU rows, x is per GPU. n chunk barely matters: one million draws
on Blackwell peak near 1200 MB at both 10000 and 40000. A 1050 Ti crashes full
at 10000, 20000 and 40000 alike, after 3 to 7 million completed draws. It grew
faster than the A10 fit: 4021 MB at 6.7 million draws, where that fit predicts
2492 MB.

All CUDA 11.8 cards are older than the one CUDA 12.9 card, so image and card
are confounded. The cause is not established.

## What this adds

A *resource profile* is a YAML record kept with the run sequence. It lists the
GPU classes a pool offers and what earlier runs observed on them. At build
time, `plan_resources` turns the profile and the run's workload into four
values:

| Output | Applied as |
|---|---|
| admitted classes | `require_gpus` expression, ANDed with any user requirement |
| points per worker | `ile-jobs-per-worker` in a private copy of the ini |
| host memory | `internal-ile-request-memory` in the same copy |
| n chunk | `internal-ile-n-chunk` in the same copy |
| plan record | `resource-plan.json` in the run directory |

The plan goes into the ini, not onto the command line, because pseudo_pipe
lets the ini override its command line. Iteration 0 gets the same points per
worker. n chunk is written too, since the helper would otherwise scale it with
SNR. The ledger and the source ini are not changed. The copy lives in a
`.rift-resource-plan-*` directory beside the rendered ini and must be kept: the
DAG reads it at run time. The build refuses ILE `condor commands` that set `request_memory` or
`require_gpus`, and a container manifest with per-branch ILE profiles, because
either would replace the planned values in the submit files. The requirement is set only in the builder's environment, so one
asimov process can build several productions.

## Profile

A profile is one file, or a directory with `resources.yaml` and
`observations/*.yaml`. A harvest writes a new file and never edits an old one,
so concurrent monitors do not conflict. The schema is in the module docstring.

A *class* is a capability range, a memory floor (and optional ceiling), and
optional device-name regexes. Memory is the advertised `GlobalMemoryMb`, which
is below the marketed size: a 24 GB Quadro RTX 6000 advertises 22684. A class
also carries `slots` (only the ratios between classes affect the plan), an
optional `relative speed`, and an optional `memory family`. Classes in a family
run the same image and share a memory fit. Order classes from most to least
specific: harvest maps a device to the first class it matches.

An *observation* is one class, one outcome (`success`, `memory-failure`,
`crash`, `failure`) and one workload. It may add draws per point, draws per
second, `memory samples` (per worker: total draws, device MB, host MB) and
`failure draws` (per failed worker: the draws in its completed points).

The workload has two parts. The *signature* (sample rate, Q pregrid factor,
detector count, approximant, l max, time-marginalization resample rate) must
match for any evidence to transfer. n chunk, n eff and a free-form label select
timing data.

## Decision rule

**Memory.** Each class gets a ceiling on draws per worker, the least of:

1. The fit. `base + slope * draws` is fit to the class's memory samples, or to
   its family's when the class has too few (8, spanning 2x in draws). The base
   is raised to the 99th percentile of the residuals, so the line bounds
   workers. The ceiling is where it meets `(1 - vram margin)` of the card.
2. Failures. A `memory-failure` or `crash` in the family, on a card with at
   least this much memory, caps the class at the draws that worker completed,
   unless a worker on a card with no more memory completed as many. Crashes
   count because the IR1 4 GB failures were segfaults near full memory.
3. Host memory. The same fit on host memory, against
   `host memory max mb / (1 + host memory margin)`, because a larger request
   matches fewer slots.
4. Without a fit, the most draws a worker on a card with no more memory has
   completed.

With no evidence at all, the class is excluded unless `admit unprofiled` is set.

**Cost.** Seconds per point are draws per point divided by draws per second.
Draws per point (ILE `ntotal`) depend on the event, the iteration and n eff,
not on the card. They are pooled over all classes. Draws per second depend on
the card and n chunk, not on the point. Splitting the two keeps a fast card
that served hard late-iteration points from looking slow. A class takes the
low quantile (`1 - timing quantile`) of its own rates. If it has none, it takes
the rate measured at a smaller n chunk (assumed slower), or a rate scaled from
another class by `relative speed`.

**Points per worker.** Point costs are heavy-tailed: one IR1 campaign spans
0.36M to 52M draws per point. A worker's cost is the sum over its points, so
the planner bootstraps 1000 sequences of points and takes the `timing quantile`
of the cumulative draws at each N, which is non-decreasing in N. A class's limit
is the largest N whose draws fit both its ceiling and `worker wall target hours`
less `worker setup seconds`. A class whose `timing quantile` point exceeds
`max seconds per point` (default 1800 s) is excluded.

**Which classes.** One N serves the DAG. At a given N, every class whose limit
reaches N is worth admitting, so only each class's own limit needs trying. The
pool throughput is

    sum_c slots_c * N / (N * mean_draws / rate_c + setup)

The planner keeps the N with the highest throughput. Ties go to the smaller N,
which has the larger pool. A numerous small card joins only if the N it can
sustain still beats what the larger cards do without it.

**Requirement.** The default style, `exclude`, writes a memory floor (the
smallest admitted class), then allows a card that an admitted class describes
or that no excluded class describes. Cards no class describes can still match,
so a new site is not locked out. A specific admitted class inside a broader
excluded one still matches. `include` lists
the admitted classes only. Give each class a memory ceiling, or excluding
"Pascal, 4 GB and up" also excludes a 24 GB Pascal card.

**Host memory.** The host fit at the planned worker's draws, largest over the
admitted classes, times `1 + host memory margin`, rounded up to 100 MB. With no
fit, the largest observed value is used. With neither, the run keeps its own
value. RIFT doubles this for `ILE_extr.sub`, so `host memory max mb` does not
bound extrinsic jobs.

## Harvest

`harvest_rundir` reads each `iteration_*_ile/logs/ILE*.out` for the cupy
device banner, its condor log (ClassAd or classic format) for wall time,
memory use and exit status, its `.err` for CUDA out-of-memory messages, and
the worker's `.dat` outputs for `ntotal`. The ClassAd-format log also gives the
card's `GlobalMemoryMb`, used to classify it, and `GpusMemoryUsage`, kept as
device memory for single-GPU workers. A worker that fanned out over K
GPUs (`.gpuK` shards) counts its wall time K times. Out-of-memory text counts
only when the worker failed. Rows whose `ntotal` is not a whole number (the
hyperpipeline row layout) and extrinsic jobs are skipped. Held-then-removed
workers have no termination record and are skipped, so a host-memory hold is
not yet recorded.

With `harvest: true` and a directory profile, `after_completion` writes the
run's observations to the profile. Failures there are logged and do not block
completion. The CLI does the same for a run directory.

## Limits and assumptions

- One N per DAG. A slow card's share is fixed at build time. A match-time N
  needs either per-class ILE nodes or an ILE wall-time budget. ILE already
  processes a worker's points in order, so a truncated worker drops an
  unbiased subset (`--sampler-sequential-warmstart` help); the budget is the
  missing piece. The container manifest's per-branch `request_memory`
  already selects host memory at match time.
- `slots` is an operator estimate. A monitor that counts matched slots per
  class should update it.
- Draws per second are assumed non-decreasing in n chunk. IR1 fixed-budget
  trials agree: Blackwell ran about 6400 draws/s at 10000 and 9600 at 40000.
- A requirement that pins a class (`Capability == 6.1`) is ANDed with the plan
  and can match nothing. Drop such pins when using a profile.
- `require_gpus` does not reach a class that needs a separate opt-in. Those
  attributes stay in the blueprint.
- Multi-GPU fan-out makes workers finish sooner than planned. The plan is
  conservative there.
