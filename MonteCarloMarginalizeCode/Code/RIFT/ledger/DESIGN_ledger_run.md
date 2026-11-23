# Design: ledger runs (`RIFT/ledger`, `bin/rift-run`)

Status 2026-11-19: step M0, one host. The evidence and the wider design are in the
paper repository `RIFT_roboto_paper`, under `analyses/framework_redesign_20261113/`
(`DESIGN_estimator.md`, `DESIGN_execution.md`, gates G2, G3, G5 and G7).

## Purpose

A conventional RIFT analysis alternates likelihood evaluations with a fit, and reports
draws from the last fit. A ledger run keeps the evaluations and drops the reported fit.
It adapts a proposal with an evaluable density, then measures the posterior and the
evidence by importance weighting, with an error it can check. The run stops on that
measured error.

## Statistical design

An intrinsic point has two blocks (`space.py`): block A is chirp mass, mass
difference and the two aligned spin components; block B is the in-plane spins. A
proposal acts on block A. Block B is drawn from the prior conditional on block A, so
its factors cancel in a weight `L p(A) / q(A)`.

| Stage | Rule |
|---|---|
| Version 1 | block A from the prior |
| Learning version | `learn_per_stream` draws on each of `n_streams` streams from the current proposal |
| Next proposal | tempered weights against the mean density of all earlier versions; a Gaussian mixture of at most four components plus a Student-t (`adapt.py`) |
| Freeze | the temper reaches 1 on two consecutive fits and the tail index is below 0.7 |
| Frozen round | draws from the frozen proposal only; no row of it enters a fit |
| Stop | n_eff, ln Z error, tail index and stream agreement all pass (`estimator.py`) |
| Extension | more draws from the measured efficiency, at most `extension_factor` times the round's size per step |

Every draw of the frozen round counts in the estimate. A draw outside the prior
support, or one whose evaluation failed, has weight zero. Four streams give four
independent estimates; their chi-squared about the common mean is one of the checks.

A run that cannot freeze by `max_learn_versions` ends with no certificate. A run that
reaches `max_frozen_per_stream` with a check failing writes a certificate whose
`certified` field is false. Both exit 4, with no looser rule.

The weights are used raw, and a heavy tail makes ln Z read low: n draws from weights of
tail index k typically miss a fraction of the evidence that falls only as n^-(1-k). The
certificate's `lnZ_tail` estimates that fraction from the generalized Pareto fitted to
the largest weights. Read a certificate as ln Z within [-2 err, +2 err + 3 `lnZ_tail`]
of its value; in simulation that interval missed the truth in at most 4% of certified
rounds for tail indices up to 0.6, and in about 5% at 0.65. `lnZ_tail` is reported and is not a check: in simulation, stopping on it selected the
rounds without a heavy draw and made the certified ln Z worse.

## Run directory

`rift-run` keeps the whole state of a run in one directory.

| Path | Content |
|---|---|
| `config.json`, `config.sha256` | seed, sizes, stop rule, prior box, grid header; fixed at the first start |
| `engine.json`, `engine_history.jsonl` | the engine command and its working directory; the hash of each version a start accepted |
| `artifacts/proposal-vNN-<sha16>.json` | one proposal per version, never rewritten |
| `plans/plan-vNN-iA-iB.dat` | draws A to B of every stream: point, support flag, `ln p - ln q` |
| `HEAD.json` | state, current version, draws planned per stream |
| `leases/lease-vNN-nK.{dat,json}` | points for one engine process, and their keys |
| `ledger/shard-vNN-nK*.jsonl` | one line per finished key |
| `certificate.json` | the estimate, checks, proposals, plan hashes and configuration |
| `posterior_weighted.dat`, `posterior_samples.dat` | the frozen round as hyperpipeline grids: every weighted row, and equal-weight draws |

The controller (`controller.py`) is a function of these files. One tick reads them and
does one of three things: fit and publish the next proposal, extend or close the
frozen round, or write leases for the rows still missing. It holds nothing in memory
between ticks, so a killed run resumes from the directory.

## Keys and reproducibility

A key is (version, stream, index). Draw `index` of stream `stream` comes from a
generator seeded with (proposal seed, stream, index), so a plan can be extended without
redrawing it.

A Gaussian or Student-t draw is `mean + L z`, with `L` the Cholesky factor stored in
the proposal artifact. `Generator.multivariate_normal` uses an SVD whose signs depend on the LAPACK build, and
gave different points on two machines (gate G7). Two machines can still differ in a
draw's last bits. The plan file written when a draw is first made is therefore the
record, with its weight, and its hash is recorded. Later ticks read it and never redraw a key.
`rift-run verify` builds the certificate and both posterior tables again from the plan
files and the ledger, and exits 1 if any field or table differs. A certificate of an earlier format is compared
on the fields it holds. `verify` needs the environment that made the run: on another
machine the refits and the last bits of the estimate differ.

The engine reseeds every random stream from (run seed, key) before it evaluates a
point. A row then does not depend on which process produced it or what that
process did before. `rift-run replay` repeats ledger keys in a fresh process and
compares each row's status, ln L, error, sample count and point for equality; it exits
1 on a difference.

## Engine

`engine.py` runs `integrate_likelihood_extrinsic_batchmode` unmodified in one process
per lease. It replaces the per-point call of the AV sampler by a call that reads the
key, reseeds, runs to `n_max_samples` samples with no n_eff target, and appends one
synced row. Data reading, waveforms, precompute and the likelihood are ILE's own code.

The engine follows each point from the entry of ILE's `analyze_event` to its return.
While a point is being worked on, its key and phase (precompute or sampler) are in
`<shard>.inflight`. The row of the sampler call is written when `analyze_event` returns
a finite result.

On an exception in one point ILE either ends the process (a GPU or memory error) or
continues with the next point. The engine writes nothing at the exception. If ILE
continues, the point gets a FAILED line, also when the sampler had already returned.
If the process ends, the key has no line and the transport decides. A lease row ILE
never starts gets no line.

A row whose sampler reported a collapsed live volume is marked `collapsed`. Its ln L is
a lower bound. The certificate gives the count of such rows in the frozen round and
their share of the weight. In the two GW150914 runs they were 1% of rows and held under 1e-7 of
the frozen weight. An OK row carries the point it evaluated, and the
controller checks all eight columns against the key's.

The four NoLoop flags and `--sampler-method AV` are required. Options that call the
sampler twice per point, carry state between points, or report a failure as success are
refused. So are the glitch-basis options, which add a constant to ln L that the run
does not record. The check resolves each argument against the option names in the ILE script,
because optparse accepts abbreviations. A second sampler call that gets through writes
an INVALID line and ends the process; the controller refuses a ledger that holds one.

After each lease the transport reads the engine log, and at every start it reads all of
them, compressed or not. A line that shows the likelihood off the NoLoop path ends the run in state FAILED,
because such rows must not enter a fit.

Each ln L is one Monte Carlo estimate at a fixed sample count. Its noise is unbiased in
L and enters the weights. Its bias at that count does not average out: gate G2 measured
ln Z low by 0.06 at 4.0e5 samples. The certificate says so, and its ln Z error does not
include that term.

## Transport

`inproc.py` is the only transport: a supervisor on one host that alternates a tick
with engine processes, one per lease, side by side when `--engines` exceeds 1. It
stops engines when its time budget ends and exits 3; the next start resumes. It holds a
lock on the run directory, and on start it kills engines that an earlier supervisor
left running on the same host.

| Engines on one A100, public GW150914, 936 rows | Rows per GPU-hour, relative | Rows identical to one engine's |
|---|---|---|
| 2 | 1.28 | all |
| 4 | 1.46 | all |

A failed row has weight zero and lowers ln Z, so a point is marked failed only on
evidence about that point.

| After an engine exits | The transport |
|---|---|
| killed by this supervisor | records nothing; the next lease covers the rows |
| every lease row has a line | does nothing; the engine reported its own failed points |
| died on a key, and the engine is known to work | gives that key a strike; two strikes make it FAILED |
| anything else with rows missing | marks nothing |

The engine is known to work when it wrote a row in the same launch. A key on which a
launch died without that goes to the end of the next lease, and a lease of only such
keys starts with a key that already has a row. One point that kills the engine is then
failed after two strikes and the run continues. A lost device earns the key in flight
one strike at most, so it fails no point. An engine that writes no in-flight file gets
no strikes. A launch is one round of engines; three in a row with no lease finished and
no strike exit 6.

A frozen round with more than `max_failed_fraction` failed rows is not certified. Further draws fail at the
same rate, so the round is closed at once when the count exceeds the limit by three
standard deviations of a count at the limit. A learning version with more than `max_failed_fraction_learn` ends the run.

| Exit status | Meaning |
|---|---|
| 0 | certified |
| 3 | time budget spent; start again to resume |
| 4 | stopped without a certificate |
| 5 | an engine left the NoLoop path |
| 6 | the engine is not working: in three launches in a row no engine finished its lease or gave evidence against a point |

## Hyperpipe stage

`util_RIFT_pseudo_pipe.py --pipeline-builder Hyperpipe --ledger-run` builds a workflow
with no iterations and one `command-v1` terminal stage, `ledger_run`. The stage runs
`rift-run` on `ledger/` in the run directory. `create_eos_posterior_pipeline` accepts
`--n-iterations 0` for this, and only with a terminal stage.

When it first starts, `rift-run` reads three files of the build (`rundir.py`):

| File | Use | Refused |
|---|---|---|
| `args_ile.txt` | the engine's ILE line; `--manual-logarithm-offset` | a missing NoLoop flag, a sampler other than AV, `--auto-logarithm-offset`, calibration marginalization, and the options named in `engine.ILE_REFUSED` |
| last line of `args_cip_list.txt` | the prior box: `--mc-range`, `--eta-range`, `--chi-max` | any option not known to leave the prior alone; a coordinate set other than mc, delta_mc and polar spins; a downselect that cuts the box |
| `proposed-grid.dat` | the grid header for leases | columns other than masses and Cartesian spins |

The product is `ledger/certificate.json` (`RIFT.misc.terminal_stage_products`). Its
`certified` field says whether the stop rule held.

pseudo_pipe refuses `--ledger-run` with `--add-extrinsic`, `--use-osg`,
`--use-jax-ile`, without `--ile-sampler-method AV`, and outside the Hyperpipe builder.

`config.json` is fixed at the first start. A start refuses an `engine.json` that changed
since the last one, because rows do not name the command that made them.
`--accept-engine-change` records the change in the certificate, for a moved run.

## Defaults

`controller.DEFAULTS`; set at the first start with `rift-run --set key=value`.

| Key | Value | Source |
|---|---|---|
| `n_streams`, `learn_per_stream`, `frozen_per_stream` | 4, 375, 375 | gate G2 ran 1500 draws per version |
| `target_n_eff`, `lnz_tol`, `k_max`, `chi2_max` | 1000, 0.05, 0.7, 11.3 | gate G7 |
| `max_learn_versions`, `max_frozen_per_stream` | 12, 2000 | gate G7 |
| `extension_factor`, `extension_margin`, `min_extension` | 1.5, 1.1, 64 | null worlds, `m0_delivery/null_extension.py` |
| `n_max_samples` | 400000 | gate G3: the cost of n_eff 40; gate G2: enough near the peak |
| `max_failed_fraction`, `max_failed_fraction_learn` | 0.002, 0.2 | failed rows at random lower ln Z by -ln(1 - f) on average for a fraction f; rows that fail at high weight cost more |

## Not implemented

- Transports across hosts (shared filesystem, OSPool pilots). The lease and shard
  files are the interface they will use.
- Extrinsic samples. The frozen round uses distance marginalization, so its rows do
  not carry distance draws; `--add-extrinsic` is refused.
- Priors other than the one in `space.py`: aligned spin, tides, eccentricity,
  volumetric or tapered spin priors, and extra waveform parameters.
- Samplers other than AV in the engine.
- A second precision class for points far below the peak. Every point gets the same
  sample count.

## Tests

`test/ledger/test_ledger_run.py` has three kinds of test, none with a real likelihood.

| Kind | Covers |
|---|---|
| Whole runs through `rift-run` with `fake_engine.py` (ln Z is 0, block-A posterior a given Gaussian) | the certified estimate and that it met the stop rule; kills, restarts and a second engine; the recorded plan as the record; exit statuses; a row without its key's point; `verify` after edits to the certificate, a table or a plan, on an older certificate and on a run without one; an engine file or configuration changed in mid-run; collapsed rows in the certificate; engine logs, compressed or not |
| The real engine and AV sampler with `stub_ile.py` in ILE's place, through `rift-run` or alone | keyed rows against an analytic integral; replay; a skipped point; a second sampler call; a point rejected after the sampler; a collapsed integral; one and two points that kill the engine; a memory error on one point and on every point; a device lost in mid-run; an error exit, a status-0 exit, a non-finite result and a caught exception inside a point, on the first point of a lease and a later one |
| Pieces against analytic or independent answers | each proposal's density; the prior; the estimate, its errors and stream estimates from weights worked by hand; each check of the stop rule; the tail index; the extension limit; the refusals in `rundir.py` and `pseudo_pipe` |

`test/hyperpipe/integration/run_pseudo_build_gate.py` also builds the stage. An independent reviewer mutated this code in four rounds; the record is in the paper
repository (`RESULTS_m0_delivery_20261119.md`). CI does not run the build gate.
