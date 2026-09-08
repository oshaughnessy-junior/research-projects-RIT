# The cross-axis direct-marginalization policy

Module `direct_marginalization_policy.py`; driver flag
`--direct-marginalization-policy {off,auto}`, default `off`.
Companion to `DESIGN_direct_marginalization_planner.md` (the generic
error/resource planner, still unwired) and to PR #268, whose controller this
policy runs.

## Status

Opt-in, value-only. Wired on 2026-09-07 for `--mode flowmc-phipsimarg`.
Nothing selects it by default. Gradient parity is not validated; see the
gate below.

## Why not widen `--angle-marg-scheme auto`

That selector chooses between `exact` and `laplace` on one amplitude
crossover, controls angles only, and excludes both peak-local kernels by a
pinned test. The composite method owns four axes at once and decides per
evaluation from diagnostics. It is a different object and gets a different
flag.

## What one evaluation does

For a batch of extrinsic rows `(ra, dec, incl)`:

1. `anglemarg.angle_coefficient_tables(..., guard=G)` builds the exact
   coefficient tables once with `G` primitive-only support samples at each
   end. The norm table is collapsed per row; its deviation over time is
   recorded and a row whose norm moves with time is marked unusable.
2. Per row, under `vmap`: `rank_joint_starts_from_uvq_device` at angular
   oversample 1 (base) and 2 (extra), then
   `make_all_axis_mode_plan_pair_device` refines both in one shared pass and
   freezes two nested plans. The plans are placed under `stop_gradient`.
3. `empirical_enrichment_with_exact_reserve_sequential_batch` applies the
   local gate per row and, on a decline only, executes the reserve.
4. The reserve is the exact-angle coefficient integral on a time rule
   refined `reserve_time_refine` times (default 4) over the native cadence.
   It is warranted by the two-guard comparison (`G` against `G/2`) and by
   the half-refined check rule evaluated in the same declined branch. The
   warrant is a convergence statement about the refined rules. The native
   Simpson rule is not the check rule: on a peak narrower than a sample it
   is the unconverged one, and its error is what the refinement removes.
   The refined rules are trapezoid, not Simpson: Simpson aliases at half the
   node spacing on a sub-sample peak (0.03 to 1.8 nat at refine 4 for peaks
   of 0.05 to 0.2 samples), the trapezoid rule converges exponentially.
   The wiring test measures the native rule's error on its analytic fixture.
5. The selected value and the ledger come back per row.

Acceptance diagnostics, all required for the local branch:

| diagnostic | ledger key |
|---|---|
| norm table time-independent | `norm_time_invariant` |
| no capacity truncation, base and enriched | `base_capacity_ok`, `enriched_capacity_ok` |
| finite stationary modes | `base_and_enriched_values_finite`, `decline_no_modes` |
| valid, nested local geometry | `geometry_nesting_ok`, `decline_geometry` |
| base/enriched mode agreement | `mode_nesting_ok` |
| nested quadrature convergence | `decline_quadrature`, `decline_enrichment` |
| two-guard time agreement | `decline_time_reconstruction` |
| omitted-time mass under budget | `time_omitted_mass_ok` |
| total empirical error score under budget | `value_error_budget_ok` |

Reserve warrant: `reserve_time_guard_validated`,
`reserve_time_resolution_validated`, `reserve_time_error_budget_ok`, combined
in `reserve_time_warranted`. A reserve that fails its warrant is escalated:
the rule is doubled and re-checked against its own half, up to
`reserve_time_refine_max` (default 32). A row still unwarranted, or whose
norm table varies with time, is `usable=False` and its likelihood is `nan`.
The finite diagnostic stays in the ledger under `selected_value` and never
reaches the sampler. The driver raises on the first `nan` it evaluates and
refuses to publish samples or evidence that contain one (external review of
PR #278, P1). A MALA step onto a `nan` target is rejected, so chains do not
carry such rows either.

No SNR threshold appears anywhere. The transitions reported in the paper
(reserve at 40 and 80, local at 160 and 320) emerge from these diagnostics.

## Measures

The local branch integrates `x**-4 dx  dt_sample  dphi  du`. The reserve and
the exact scheme average both angles, weight distance by the normalized
`log_w_grid` (fixed grid) or the normalized volumetric measure
(`JAX_ILE_DISTMARG_GH`), and integrate time in seconds by Simpson.
`policy_log_normalization` derives the conversion (the wiring test checks
it end to end against an independent fine-time reference on both distance
paths):

| term | constant |
|---|---|
| angles | `-2 log(2 pi)` |
| time | `log(deltaT) + log(sum(w_t) / ((npts-1) deltaT))` |
| distance, fixed grid | `3 log(Dref) - log(sum_i d_i^2 dd)` with `dd` read off the grid |
| distance, GH | `log 3 - log(x_min^-3 - x_max^-3)` |

## Refusals

The policy refuses, with a message, any of: a resolved angle scheme other
than `exact`; a time rule other than `simpson`; a distance prior other than
volumetric; a distance grid other than uniform-in-d; a `time_guard` below 2;
a reserve refinement that is not an even integer of at least 2; a request
for `lnL(t)`. The driver refuses at parse time a policy request in any mode
other than `flowmc-phipsimarg`, and any of the three policy knobs when the
policy is off (external review of PR #278, P1). Refusal rather than silence
is the standing rule on this arm.

## Cost

Planning is vectorized. Accepted rows pay fixed local work per retained mode;
declined rows pay three exact reserve evaluations (refined rule at two guards,
plus the half-refined check). The sampler's angle-scheme chunk cap applies.

`PolicyConfig.reserve_batch_rows` sets how many rows run under one `vmap`;
`--direct-marginalization-batch-rows` exposes it. At 1 the rows run one at a
time under `lax.map`, the graph PR #268 measured. Above 1 the tier-escalation
`lax.cond` becomes a `select`, so every reserve tier runs for every row. Nothing else changes:
`test_row_batch_size_changes_cost_not_values_decisions_or_gradients` requires
`lnL` bitwise equal, every ledger key and summary count equal, and the
gradient equal to one ulp, over the full-batch, whole-multiple and remainder
paths.

### Device workspace

XLA buffer assignment, ladder-2 tables, NVIDIA RTX PRO 4000 Blackwell,
jax 0.9.2, `--n-phi 32 --n-psi 8 --distance-grid-points 256`,
`reserve_time_refine_max` 32:

| `reserve_batch_rows` | temp GiB at rho 40.8 | temp GiB at rho 652.3 |
|---|---|---|
| 1  |  0.425 |  0.432 |
| 2  |  0.812 |  0.822 |
| 4  |  1.588 |  1.592 |
| 8  |  3.132 |  3.132 |
| 16 |  6.212 |  6.212 |
| 32 | 12.371 | 12.371 |
| 64 | 24.692 | 24.692 |

The fit is `0.046 + 0.385 B` GiB, maximum residual 6 MiB. The rung does not
enter: the reserve grids take their shape from the three grid options and only
their sizing scalar from the amplitude. Measured device use at B=32 was
23.3 GiB against the 12.4 GiB analysis figure, so buffer assignment understates
the card about twofold. A 24 GiB card holds B=32 and not B=64.

The tier count multiplies it. At B=8, `reserve_time_refine_max` 32 costs
3.114 GiB and 138 s to compile; at 4 (one tier) the same batch costs
0.415 GiB and 29 s. A batched row pays every tier.

### Throughput

Batching does not pay. Idle card, rho 40.8, `reserve_time_refine_max` 4,
`_batched_ledger`, second timed call:

| `reserve_batch_rows` | rows | wall s | s per row | workspace GiB |
|---|---|---|---|---|
| 1 | 2 | 192.6 | 96.3 | 0.103 |
| 8 | 8 | 799.3 | 99.9 | 0.415 |

At `reserve_time_refine_max` 32, B=8 had not finished one pass after 8000 s,
above 980 s per row; that point was stopped, not completed. First calls gave
110.3 and 87.5 s per row, a spread of 13%, wider than the gap between the
batch sizes. One row already fills the card, leaving a batch no occupancy to
recover. The row loop is not why the Section VI.A cells stall. Every row here
declined to the reserve (`accepted_local` 0 of 8), so the per-row cost is one
reserve evaluation. Profile that next. `reserve_batch_rows` defaults to 1.

## Gate before this can be a default

PR #268 warrants scalar values. Differentiating the composite differentiates
a truncated fixed-plan integral, and the JAX sampler's hill climbing and
MALA/HMC steps consume those gradients. Required before any default change:
value and gradient parity through the SNR and higher-mode ladder, on
production tables, recorded in the paper repository
(`development/OPEN_jax_direct_marginalization_policy.md`).

## Known adversarial items

Items 1 and 2 below come from the adversarial review of PR #268 through this
wiring (2026-09-07) and are verified on synthetic tables only. They are the
first questions for the production-table ladder.

- On synthetic 22-only carrier tables the base portfolio at angular
  oversample 1 overflows `max_starts=32`: the 9-point phi lattice's
  max-over-angles time profile ripples with the rotating carrier phase and
  produces spurious time peaks, so every row declines on capacity and the
  local branch never runs. Oversample 2 fits but the same tables then
  decline on nested quadrature (13 vs 19 nodes, 8e-3 nat). PR #268's real
  SNR-160 capture reports 4 base candidates with no overflow, so the
  synthetic result does not transfer directly; the ladder must measure the
  acceptance rate on production tables before the paper's "local at 160 and
  320" is quoted from this code. Suggested fix if it does transfer: rank
  time peaks from the triangle envelope the time-cover step already
  computes, not from the lattice profile.
- Sub-sample peaks: at 150 Hz and 4096 Hz the time peak is about 4.35/rho
  samples wide, so above rho of a few tens the reserve needs refinement well
  beyond 4. The escalation ceiling and the trapezoid rules address the
  warrant; the cost (three dense evaluations per tier) is the ladder's to
  measure. The norm lower bound also loosens with inclination (0.46 of the
  norm edge-on), which can exhaust the 64 retained time nodes.
- Capacity at higher harmonic order: random m_max=4 tables show 5 to 12
  angular lattice maxima per time node against `max_modes=4`, and the u
  lattice does not grow with oversample, so enrichment refines phi only.
- The error score double-charged common-mode terms. Base and enriched plans
  measure the same quadrature, guard, and omitted-time discrepancies, and
  the score summed both. On the analytic fixture at 10x amplitude a value
  correct to 1.5e-4 nat was refused at a score of 1.06e-3 (two copies of one
  5.27e-4 guard discrepancy). Fixed in PR #278: each pair is charged once as
  its maximum; the per-plan terms stay in the ledger.
- A distance peak below the prior's support pins every optimizer lane to the
  boundary with an identical gradient norm growing like rho^2. PR #268
  declines such a row (`decline_no_modes`); the signature matches the one
  the PR #270 ladder reported for its enriched tier, which points at a
  distance-support problem in that harness rather than a refinement defect.
- The local box radius is a common-mode term. Base and enriched plans use
  the same whitened radius, so mass outside the box is invisible to the
  enrichment gate and to the error score. At radius 3 the accepted value on
  the wiring test's analytic fixture was 0.015 nat below an independent
  fine-time reference while every diagnostic passed. The default is the
  library's 6; the wiring test pins the accepted value against the external
  reference at 2e-3 nat, which the gate alone would not have caught.
- PR #270's host controller (`multipeak_planner`) declined on every rung of a
  production ladder because its enriched tier never refined (identical
  gradient norms across starts). This policy does not use that controller.
  The same failure class, degenerate or unrefined enriched starts, must be
  probed on this device pipeline with per-start gradient norms on production
  tables before the ladder result is trusted.
- The audit ledger is evaluated on a subsample of exported rows after
  sampling. It describes the exported cloud, not every evaluation the
  sampler made. Unwarranted rows are not a labelling matter: they are `nan`
  and stop the run.
