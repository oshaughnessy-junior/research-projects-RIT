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

The IR1 review runs (rc13, 4096 Hz, Q2, n chunk 40000, n eff 20) showed both
failure modes. With a high-memory requirement, throughput depends on a few
large cards. With the requirement relaxed, 4 GB cards crash near full memory,
and a slow card gates the whole iteration because it got the same number of
points as a fast one.

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
SNR. The ledger and the source ini are not changed. The copy lives in
`C01_offline/.rift-resource-plan-*/` and must be kept: the DAG reads it at run
time. The build refuses ILE `condor commands` that set `request_memory` or
`require_gpus`, and a container manifest with per-branch ILE profiles, because
either would replace the planned values in the submit files. The requirement is set only in the builder's environment, so one
asimov process can build several productions.

## Profile

A profile is one file, or a directory with `resources.yaml` and
`observations/*.yaml`. A harvest writes a new file and never edits an old one,
so concurrent monitors do not conflict. The schema is in the module docstring.

A *class* is a capability range, a memory floor (and optional ceiling), and
optional device-name regexes. Memory is the advertised `GlobalMemoryMb`, which
is below the marketed size: a 24 GB Quadro RTX 6000 advertises 22684. It also carries `slots`, the number of GPUs a run
can usually hold at once, and an optional `relative speed`. Order classes from
most to least specific: harvest maps a device to the first class it matches.

An *observation* is one class, one outcome (`success`, `memory-failure`,
`crash`, `failure`) and one workload. It may add peak device memory, host
memory, draws per point and draws per second.

The workload has two parts. The *signature* (sample rate, Q pregrid factor,
detector count, approximant, l max, time-marginalization resample rate) fixes
the device footprint at a given n chunk. Evidence never transfers between
signatures. n chunk, n eff and a free-form label select timing data.

## Decision rule

**Device memory.** For each class, in order:

1. A success at an n chunk no smaller than the run's, on a class with no more
   memory, *vouches* for the class. Without one, a `memory-failure` or `crash`
   at an n chunk no larger than the run's, on a class with at least this much
   memory, excludes the class. A crash counts because the IR1 4 GB failures
   were segfaults near full memory, not clean out-of-memory errors. Vouching
   keeps one unrelated crash from barring a class that has run the workload.
2. If successful runs recorded peak memory, the need is scaled linearly in n
   chunk, `fixed + (peak - fixed) * n / n_obs`, and the largest value is
   used. Peaks more than `max chunk scaling` (default 2) away in n chunk are
   not used. The class is admitted if `(1 - vram margin)` of its memory covers
   the need.
3. Otherwise a vouching success admits the class.
4. Otherwise the class is unprofiled and is excluded unless
   `admit unprofiled` is set.

**Cost.** Seconds per point are draws per point divided by draws per second.
Draws per point (ILE `ntotal`) depend on the event, the iteration and n eff,
not on the card. They are pooled over all classes. Draws per second depend on
the card and n chunk, not on the point. Splitting the two keeps a fast card
that served hard late-iteration points from looking slow. A class takes the
low quantile (`1 - timing quantile`) of its own rates. If it has none, it takes
the rate measured at a smaller n chunk (assumed slower), or a rate scaled from
another class by `relative speed`.

**Points per worker.** Point costs are heavy-tailed: one IR1 campaign spans
0.36M to 52M draws per point. A worker's wall time is the sum over its points,
so its quantile is taken from 1000 bootstrap sequences of points, not from the
per-point quantile times N. The sum for N points is the first N entries of each
sequence, so the quantile is non-decreasing in N. N is the largest value whose `timing quantile` wall time
fits `worker wall target hours` less `worker setup seconds`. A class whose
`timing quantile` point exceeds `max seconds per point` (default 1800 s) is
excluded.

**Which classes.** Candidates are sorted by rate. For each k, admitting the
fastest k fixes N from the slowest of them, and the pool throughput is

    sum_c slots_c * N / (N * mean_draws / rate_c + setup)

The planner keeps the k with the highest throughput. Ties go to the larger
pool. Adding many slow cards lowers N and costs the fast cards more setup per
point. Adding a few costs more than they return.

**Requirement.** The default style, `exclude`, writes a memory floor (the
smallest admitted class), then allows a card that an admitted class describes
or that no excluded class describes. Cards no class describes can still match,
so a new site is not locked out. A specific admitted class inside a broader
excluded one still matches. `include` lists
the admitted classes only. Give each class a memory ceiling, or excluding
"Pascal, 4 GB and up" also excludes a 24 GB Pascal card.

**Host memory.** The largest host memory at the signature, any n chunk, times
`1 + host memory margin`, rounded up to 100 MB. pseudo_pipe notes that host
memory is flat in n chunk. With no observation, the run keeps its own value.

## Harvest

`harvest_rundir` reads each `iteration_*_ile/logs/ILE*.out` for the cupy
device banner, its condor log (ClassAd or classic format) for wall time,
memory use and exit status, its `.err` for CUDA out-of-memory messages, and
the worker's `.dat` outputs for `ntotal`. The ClassAd-format log also gives the
card's `GlobalMemoryMb`, used to classify it, and `GpusMemoryUsage`, recorded
as peak device memory for single-GPU workers. A worker that fanned out over K
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
- Device memory is assumed affine in n chunk, and draws per second
  non-decreasing in n chunk. Both should be checked against qualification
  probes before relying on a transfer of more than 2x.
- Multi-GPU fan-out makes workers finish sooner than planned. The plan is
  conservative there.
