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
   The wiring test measures that error on its analytic fixture.
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
`reserve_time_refine_max` (default 16). A row still unwarranted, or whose
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

The batch runs the controller row by row (`lax.map`), so reserve workspace
is one row's. Planning is vectorized. Accepted rows pay fixed local work per
retained mode; declined rows pay three exact reserve evaluations (refined
rule at two guards, plus the half-refined check). The sampler's angle-scheme chunk
cap applies because the resolved scheme is `exact`. The policy has not been
profiled on a GPU under the sampler; PR #268 records single-row device
timings only.

## Gate before this can be a default

PR #268 warrants scalar values. Differentiating the composite differentiates
a truncated fixed-plan integral, and the JAX sampler's hill climbing and
MALA/HMC steps consume those gradients. Required before any default change:
value and gradient parity through the SNR and higher-mode ladder, on
production tables, recorded in the paper repository
(`development/OPEN_jax_direct_marginalization_policy.md`).

## Known adversarial items

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
