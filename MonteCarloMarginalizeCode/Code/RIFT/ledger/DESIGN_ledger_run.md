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

A run that cannot freeze by `max_learn_versions`, or that reaches
`max_frozen_per_stream` with a check failing, ends without a certificate. It does not
fall back to a looser rule.

## Run directory

`rift-run` keeps the whole state of a run in one directory.

| Path | Content |
|---|---|
| `config.json` | seed, sizes, stop rule, prior box, grid header |
| `engine.json` | the engine command and its working directory |
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
the proposal artifact. Drawing through `Generator.multivariate_normal` goes through an
SVD whose vector signs depend on the LAPACK build; the same artifact then gave
different points on two machines (gate G7). Two machines can still differ in the last
bits of a draw. The plan file written when a draw is first made is therefore the
record, with its weight. Later ticks read it and never redraw a planned key.
`rift-run verify` recomputes the certificate from the plan files and the ledger.

The engine reseeds every random stream from (run seed, key) before it evaluates a
point. A row then does not depend on which process produced it, or on what that
process did before. `rift-run replay` repeats ledger keys in a fresh process and
compares the rows bit for bit.

## Engine

`engine.py` runs `integrate_likelihood_extrinsic_batchmode` unmodified in one process
per lease. It replaces the per-point call of the AV sampler by a call that reads the
key, reseeds, runs to `n_max_samples` samples with no n_eff target, and appends one
synced row. Data reading, waveforms, precompute and the likelihood are ILE's own code.

The four NoLoop flags and `--sampler-method AV` are required and checked before ILE
starts. After each lease the transport reads the engine log. A line that shows the
likelihood off the NoLoop path ends the run in state FAILED, because such rows must not
enter a fit.

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

| Exit status | Meaning |
|---|---|
| 0 | certified |
| 3 | time budget spent; start again to resume |
| 4 | stopped without a certificate |
| 5 | an engine left the NoLoop path |

## Hyperpipe stage

`util_RIFT_pseudo_pipe.py --pipeline-builder Hyperpipe --ledger-run` builds a workflow
with no iterations and one `command-v1` terminal stage, `ledger_run`. The stage runs
`rift-run` on `ledger/` in the run directory. `create_eos_posterior_pipeline` accepts
`--n-iterations 0` for this, and only with a terminal stage.

When it first starts, `rift-run` reads three files of the build (`rundir.py`):

| File | Use | Refused |
|---|---|---|
| `args_ile.txt` | the engine's ILE line; `--manual-logarithm-offset` | a missing NoLoop flag, a sampler other than AV, `--auto-logarithm-offset`, calibration marginalization, options that call the sampler twice per point |
| last line of `args_cip_list.txt` | the prior box: `--mc-range`, `--eta-range`, `--chi-max` | any option not known to leave the prior alone; a coordinate set other than mc, delta_mc and polar spins; a downselect that cuts the box |
| `proposed-grid.dat` | the grid header for leases | columns other than masses and Cartesian spins |

The product is `ledger/certificate.json` (`RIFT.misc.terminal_stage_products`). Its
`certified` field says whether the stop rule held.

pseudo_pipe refuses `--ledger-run` with `--add-extrinsic`, `--use-osg`,
`--use-jax-ile` and outside the Hyperpipe builder.

## Defaults

`controller.DEFAULTS`; set at the first start with `rift-run --set key=value`.

| Key | Value | Source |
|---|---|---|
| `n_streams`, `learn_per_stream`, `frozen_per_stream` | 4, 375, 375 | gate G2 ran 1500 draws per version |
| `target_n_eff`, `lnz_tol`, `k_max`, `chi2_max` | 1000, 0.05, 0.7, 11.3 | gate G7 |
| `max_learn_versions`, `max_frozen_per_stream` | 12, 2000 | gate G7 |
| `extension_factor`, `extension_margin`, `min_extension` | 1.5, 1.1, 64 | null worlds, `m0_delivery/null_extension.py` |
| `n_max_samples` | 400000 | gate G3: the cost of n_eff 40; gate G2: enough near the peak |

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

`test/ledger/test_ledger_run.py` drives `rift-run` with an engine whose answer is
known: ln Z is 0 and the block-A posterior is a given Gaussian. It checks the
certified estimate against that answer, that kills, restarts and a second engine
change no byte of the estimate, that the recorded plan is the record, that each
proposal's density is the density of its draws, and the refusals above.
`test/hyperpipe/integration/run_pseudo_build_gate.py` builds the stage.
