# Learned flow-map research on S250114ax

Latest decision: [COMMON_QUADRATURE_PLAN.md](COMMON_QUADRATURE_PLAN.md). The
original valid-point NaN was deliberate certificate refusal. Source-only planning
used no new physical calls; inference remains paused pending a common converged
value/derivative and reconstruction contract.

A genuinely real-reward-fine-tuned periodic model now exists. The stage3f
implementation smoke passed19 mathematical/model tests, two real full
lookahead/control value-gradient checks before optimizer creation, eight batch4
updates and post-validation. The initial base/reference stayed frozen; student
parameters changed by L2 .002805. All scientific capability/cost claims remain
unestablished. See `PERIODIC_WTF_SMOKE_RESULTS.md` for scope, exact pins and limits.

The stage used100 endpoint attempts /92 physical rows,30.31 CPU /39.35 wall
seconds including tests, setup and validation. Its separate predecessor allocations
used31.05 CPU seconds(model build/repairs) and35.83 CPU seconds(periodic parity).
The current checkpoint is71be3106; the pretrained comparator a63d634b is unchanged.
Both learned maps remain implicit proposal-training objects with no normalized logq
or posterior-draw/evidence calibration contract.

No five-arm inference comparison ran. The conditional next request is a one-anchor,
one-seed10000-attempt corrected accounting/coverage pilot after bad-oracle/cache
isolation/reset tests, with20 CPU minutes/8GiB. `PLAN.md`/`STATE.json` record the
current boundary. PR445 and all production/defaults remain unchanged.

Historical progression is retained below: old-coordinate parity passed locally
(stage3a), six old-coordinate real-objective updates stopped at the chart guard
(stage3b), validated replay diagnosed that defect(stage3c), and the first periodic
build failed one-step accuracy(stage3d). The subsequent repair now passes; it does
not relabel any earlier failed checkpoint or recreate the unsaved original endpoint.

## Original chart implementation and algorithm scope (historical)

- `Velocity(dim=6)` learns the stochastic-interpolant flow-matching velocity on
  real posterior training data; it is not a fitted Gaussian or kernel surrogate.
- A second neural network learns `X(s,t,x)=x+(t-s)*v(s,t,x)` from RK4 trajectories
  of the frozen learned drift, for arbitrary two-time jumps. The diagonal is
  independently anchored to the drift. It is a trained approximate solution
  operator, not a normalizing flow relabelled as one.
- `wtf_loss` implements diagonal control regression onto the frozen policy's
  Monte Carlo value gradient plus off-diagonal semigroup self-distillation.
  `value_gradient` retains the exact Jacobian through both lookahead jumps and
  through the base drift's **input**, with sum-square control norm. Frozen
  parameters retain input derivatives. Reward/time shape guards prevent silent
  broadcasting; constant reward/zero control gives a zero gradient.
- These fine-tuning components passed mathematical and real Torch/JAX bridge
  checks. **Six real-likelihood objective updates ran**, but the seventh batch
  stopped at the chart guard; final validation did not run. The six-update
  partial checkpoint is not a scientifically validated WTF-tuned sampler.

Method source: [WTF v2](https://arxiv.org/abs/2609.27033v2), especially section 4
and Appendix A; authors' code inspected at
`058a4f2d9a42f384a0be8d1224784b755986e4ff`. This small implementation is independent
of their image stack. Pretraining by supervised RK4 distillation is our explicit
choice; the paper assumes a pretrained flow map. The objective uses the paper's
mathematical exact-gradient version, not the released image recipe's detached
base input derivative, mean-square control norm, Euclidean reward pullback,
normalization or jitter. Noise is time 0 and data time 1. No released checkpoint
or reported image result was reproduced.

The Euclidean control metric is in standardized chart coordinates, not an
invariant metric on physical angles/distance. Finite data, neural approximation,
local angular chart seams, and hard physical prior bounds violate any easy appeal
to the population theorem. The map supplies **no normalized log q**. Outputs must
remain proposal-training candidates behind the existing corrected final sampler;
reward-optimized transport outputs are not posterior draws.

## Verified real case and split

Case: S250114ax, fixed intrinsic anchor 47536; full six-dimensional extrinsic
inference with time marginalized. The archived accepted reference is AV,
SEOBNRv5PHM, lmax 4, 4096 Hz, sinc interpolation and bandlimited time quadrature,
corrected network-sky inverse mapping, internal phase rotation and full adaptation.
Distance prior is `cosmo_sourceframe` on [1,10000] Mpc. A four-point value and
one held-out gradient parity check now passes; see `STAGE3A.md` for its limits.

- Reference source `276863bfe155b3df40567f2067761dc7d19e3899` plus reviewed PR195
  inverse-map and `e014b355d` per-row-offset patches.
- Four accepted seeds, each ESS >=100 and 100 exported fair draws. Train on
  25011401–03; hold out 25011404. Retain fair-draw duplicates; these are not 400
  independent exact posterior draws and are not raw importance rows.
- Raw result archives are individually hash-verified before XML parsing; four
  shared coordinates match the compact NPZ exactly. `collect.py` appends seeds
  in the manifest order, which defines the group split.
- The NPZ SHA is
  `4581e8b8584922946f5ee01756b2ffb9f336d2a66171014ac95641ff365beb2e`;
  manifest SHA is `4de8a247ffb55d2f007faa1ec82d65c92c165c4aa9f7ebcc66cbfa7f662cb060`.
- RA, coalescence phase and polarization use training-only circular centers
  (periods 2pi, 2pi, pi); declination/inclination use atanh(sin dec)/atanh(cos inc),
  distance uses log distance. Standardization uses training rows only. Exported
  conditional time is omitted from this time-marginalized model.
- `results/case_inventory.json` records the paper checkout, original worker and
  input hashes. Strain/PSDs, source archives, scientific samples and trained
  weights remain on Caltech; only provenance and aggregate diagnostics are in Git.

## Results and honest gates

Two hidden layers of width 64; batch 128; seed 261015. Initial 2,000 flow-matching
and 2,000 distillation steps missed the predeclared one-step solver gate:
RMS .17617 > .15. That result is retained. One bounded repair increased only
solver distillation to 6,000 steps. It was selected using the learned numerical
teacher, not the held-out real posterior. Subsequent identical-configuration
replays verified objective contracts and reproducibility, with no hyperparameter
search or additional physical likelihood calls.

Final solver diagnostics, on noise absent from teacher training trajectories:

| Diagnostic | Measured RMS | Predeclared limit |
|---|---:|---:|
| one-step map versus learned-drift RK4 | .147507 | .15 |
| 64/128-step teacher refinement | 2.88e-7 | .01 |
| semigroup consistency | .020646 | .10 |
| diagonal velocity agreement | .089824 | .20 |
| arbitrary held-out segment versus RK4 | .045011 | reported |

The one-step gate passes narrowly. These check the learned solver, not accuracy
of the posterior or evidence. Held-out positive-cos(inclination) mass is .600;
the distilled map gives .58984 and the drift .60742. One held-out sampler seed
cannot establish mode calibration. Phase-coordinate Wasserstein distance is
.444 standard deviations; its 5th/95th percentile discrepancies are about
-.895/+.336 standard deviations. These linear-chart diagnostics can be affected
by the angular seam and finite teacher ESS; they are not physical coverage tests.
Full joint/circular, tail, evidence and held-out-anchor validation is pending.

Four mathematical-contract tests passed on the existing Caltech Torch runtime:
exact value gradient versus finite differences, frozen-weight/input-gradient
behavior, diagonal identity/zero-drift analytic gradient, reward/time shape
rejection, and constant-reward zero-gradient behavior (some checks share tests).
Earlier 2/3-test executions are overlapping subsets, not additional unique tests.
These research checks are manually run and are not claimed to be CI-covered.

## Measured resources and full-cost limitations

Verified route: mini -> COS-MATH2519 laptop -> ldas-grid.ligo.caltech.edu.
Runtime `/cvmfs/software.igwn.org/conda/envs/igwn/bin/python`, Python 3.11.14,
Torch 2.8.0, NumPy 2.2.6. One CPU thread, no GPU, no scheduler job IDs,
900-CPU-second limit, fixed small model/arrays with 2-GiB resident-memory budget.
No installs or shared runtime changes. Scientific scratch:
`/scratch/richard.oshaughnessy/codex_learned_wtf_20261015`.

| Run | Flow / distillation steps | Wall s | CPU user+system s | Peak RSS KiB |
|---|---|---:|---:|---:|
| initial, failed solver gate | 2000 / 2000 | 5.68 | 4.15 | 372672 |
| bounded repair | 2000 / 6000 | 7.46 | 6.74 | 366572 |
| reward-shape contract replay | 2000 / 6000 | 7.34 | 6.68 | 366544 |
| final constant-reward contract replay | 2000 / 6000 | 7.16 | 6.59 | 368052 |

All four runs total 27.64 measured wall seconds / 24.16 CPU seconds; no new true
likelihood evaluations. Each final-config run counts 256000 flow-matching forward
and backward examples, 65536 ODE-teacher forward examples, 2304000 distillation
forward and 768000 backward examples, and 395520 validation forward examples.
These are neural example counts, not interchangeable FLOP units or physical
likelihood calls. Complete phase records, including failed-gate work, are retained.
Final four-test check: 3.80 s wall, 2.92 CPU s, 322472 KiB peak RSS; earlier check
logs/timing receipts are also preserved. One earlier two-test replay has only
pytest elapsed time, so no exact total development CPU claim is made.

Historical teacher generation recorded **15,984,560 sampler.ntotal rows** across
four accepted integrations. Total true calls, hidden work, setup, device time and
complete failed acquisition cost were not audited. This is a lower bound, not a
free training set and not a measured cold training speedup. Neither a break-even
reuse count nor end-to-end benefit can be established from these records alone.
A future cost comparison must include a cached-resampling/warm-start baseline
using the **same** posterior cache, as well as current strong AV/AV+GMM methods.
The neural model must beat inexpensive cached reuse, not only cold inference.

Final checkpoint (kept on Caltech):

```
/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/final_seed261015/pretrained.pt
SHA256 4ce26989ccdcfcdb1702dd5e270d79a772487adb2b348b8b471bcaf3a8cf3158
```

All three 6,000-step runs produced that identical checkpoint hash. Full source
hashes are in each `result.json`. `pretrained.pt` contains tensor state dictionaries
and transform configuration; load only this own trusted file, using Torch's
`weights_only=True`. It is not a posterior export.

## Reproduce this stage on Caltech

From this research directory, using the existing runtime:

```sh
export OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 CUDA_VISIBLE_DEVICES=''
P=/cvmfs/software.igwn.org/conda/envs/igwn/bin/python
$P -m pytest -q check_model.py
$P train_real.py --npz <pinned-sinc-bandlimited.npz> --manifest <pinned-timing_manifest.json> \
  --worker-root <timing-continuous-stable-ensemble/results> --distill-steps 6000 \
  --output <new-isolated-directory>
```

The exact real paths and source hashes are in `results/final_seed261015/result.json`.
Output directories must be new, so reruns cannot overwrite earlier evidence.

## Stage 3a completed: limited physical-reward parity

Executed bounded parity preflight: **one affinity-pinned Caltech CPU, 20 CPU minutes,
8 GiB resident limit, no GPU, maximum 128 attempted true-likelihood rows**, including
finite-difference and failed attempts. Check current shared-host memory/thread
headroom first; use approved scheduler/accounting policy if batch placement is
needed, never copy a historical accounting group blindly.

Rebuild the same data/PSD/waveform bank from the hash-recorded inputs; require the
maintained NoLoop path (all four flags from AGENTS.md), and a verified differentiable
six-dimensional counterpart with identical physical likelihood stencil/quadrature. Audit phase
and polarization export/inversion, chart seams and distance bounds. Count build,
JIT, value, gradient, finite-difference, and rejection costs. Stop on a source,
coordinate, value/gradient, memory or time mismatch. Dependency presence is verified;
full-mode waveform/bank construction and the local parity gates passed.

The initial adapter inherited a mode-pruning default and was stopped; its 51
attempted rows remain charged. The repaired full-mode run used 65 rows and passed.
Total: 116 attempted rows, 289.56 measured process CPU seconds, 302.15 wall
seconds, peak 2,853,072 KiB RSS, including both setups/JIT and the interruption.
Administrative source audits/transfers were not timed. The old rotated angular
prior has mass 8; unadjusted historical evidence is not a current-code baseline.
See `STAGE3A.md` and `results/stage3a/summary.json` for commands, costs and limits.

That gate led to the bounded **actual real-reward WTF objective smoke** below;
it stopped at the angular-chart guard. After resolving that failure, compare
pretrained-map, tuned-map, and cached-resampling proposals
behind the same corrected broad-support final sampler, at matched resource budgets
and held-out intrinsic anchors. Reuse amortization must be measured and realistic.
No larger training/production campaign starts automatically from a solver gate.

## Stage 3b stop and next decision

See `STAGE3B.md` and `results/stage3b/summary.json`: bridge checks passed, six
updates completed, seventh batch rejected at the strict angular-chart boundary.
Including setup repair: 62 endpoint attempts, 55 physical rows, 38.50 CPU /
44.06 wall seconds, 2,142,916 KiB peak RSS. Previous costs remain separate.
Resolve that representation/domain contract before more training or inference.
A conditional 10,000-row / 20-CPU-minute corrected-sampler accounting pilot is
estimated in STAGE3B.md; no inference pilot or campaign was launched.

## Stage 3c diagnosis

One reconstructed phase endpoint is .196174 rad outside the local chart; all
failed-batch physical distance/pole checks pass. The frozen reference generates
it independently of student updates. The original failed endpoint was not saved;
its reconstructed private artifact is distinguished from an original capture.
A torus-equivariant representation is specified and unit-tested algebraically,
but no replacement neural model has been built or trained. Old checkpoint/guard
behavior is preserved. See `CHART_REPAIR.md` and `results/stage3c/summary.json`.

## Stage 3d periodic rebuild

13 model/geometry tests passed; trained shift/Jacobian discrepancies <=7.05e-15.
RK4 refinement, semigroup and diagonal gates passed, with zero finite/support
failures in1024 drift/map endpoint checks. One-step teacher accuracy failed at
.277704 (> .15), confirmed at .271981 on separate fixed validation noise.
This periodic checkpoint is not accepted for physical reward tuning. Tests,
build and follow-up used13.06 CPU /15.20 wall seconds, zero physical rows.
Old checkpoints and their guards are untouched. `PERIODIC_RESULTS.md` records
the source/data pins and blocked128-attempt physical parity proposal.

## Latest result: bounded five-arm pilot

See [FIVE_ARM_RESULTS.md](FIVE_ARM_RESULTS.md) and results/stage4/summary.json.
Four arms completed but all had ESS about one; the cache arm stopped on a JAX NaN
where maintained NumPy NoLoop is finite. No calibrated inference or cost advantage
was demonstrated. All setup failures, tests and diagnosis remain charged:9,554
physical attempts,242.84 CPU /266.32 summed command wall seconds, one CPU, peak
2.18 GiB. Historical teacher acquisition >=15,984,560 rows remains additional.
No larger campaign was started. This section supersedes earlier prospective
next-stage wording above; prior stage records are preserved as history.

## Stage5: valid-point NaN isolated

[TIME_CERTIFICATE_RESULTS.md](TIME_CERTIFICATE_RESULTS.md) identifies a failed
first doubling certificate, with finite physical primitives. A narrow research
patch permits one additional certified doubling; seven isolated regressions pass.
Two earlier controls have exactly unchanged values and gradients. The recovered
point still differs from NoLoop's coarse Simpson rule by .0073493 nat and has
material derivative differences. The common-target gate remains blocked; no
inference arm was rerun. Stage5 used 38 physical attempts and 358.50 measured CPU
seconds, including setup and failed attempts. Previous costs remain additional.
