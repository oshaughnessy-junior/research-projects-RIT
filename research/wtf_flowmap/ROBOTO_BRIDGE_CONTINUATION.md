# Four-endpoint bridge continuation: passed, no training

The authorized continuation completed exactly the four missing h/2 endpoints
from stage9: inclination and log-distance state coordinates, plus/minus, at
h=5e-5. It reused the20 completed native stencil values after verifying their
physical coordinates and immutable result SHA256. No completed physical stencil,
canonical value/VJP, or historical endpoint was redispatched. The previous raw
partial record remains unchanged; its missing checks are resolved by this
separate record, not retrospectively labeled as passed within the old budget.

**Raw log-reward shifts remain +0.2418003 and +0.4986873 nat.** They accompany
small selected old/new pullback changes, not reward agreement or evidence of
posterior improvement. The first state is a validation preflight; only the
second belongs to the historical training batch. **One of32 historical training
targets is now fully rechecked; the other31 remain unexamined.** No claim about
those31 or the whole old training trajectory follows.

## Completed checks

| Selected state | Full AD/FD error / tolerance | h/h2 refinement / tolerance | Old/new full target relative L2 change |
|---|---:|---:|---:|
| Original preflight0, reused stage9 pass | .00044358 | .00007450 | .04298% |
| Actual first training member0, completed here | .00075981 | .000009080 | 1.58537% |

Every ratio is below1, with unchanged .01+.001*abs(g) full-gradient tolerance.
The four new physical integrals satisfy independent factor32/64/128 and
Simpson/trapezoid convergence, maximum discrepancy1.771e-6nat (limit1e-4).
The complete six-coordinate h/2 gradient now matches the stored Q8 canonical AD
pullback through both frozen-map lookaheads and the physical decoder. Replaying
the cached physical gradient reproduces the stored full AD vector. The separate
control-cost finite-difference check (atol1e-7,rtol1e-5), complete tuned-student
loss backward and finite parameter-gradient checks pass. No optimizer exists;
all student, reference and base weights and both checkpoint files remain equal.

The same explicit float64,21-mode H1/L1 bank, physical parameter order, fixed
time window, source/coordinate checks and normalized-prior conventions apply.
The reward remains raw physical log time integral, with no parameter prior or
decoder Jacobian added. Q8/cubic uses the existing native PR261/274 reflected-Q
builder; no new physical approximation or production default was introduced.
The previous Q8/Q16 and canonical/native checks were reused, not rerun. The
paper/source pins are unchanged from stage8 and the stage9 report. No new
full-frequency oracle or calibrated posterior/evidence validation was performed.

## Source/checkpoint analysis and recommendation

A separate pure-neural analysis loaded the unchanged checkpoint and the two
cached old/new targets, with **zero physical evaluations**. It confirms8 old
updates, lambda=.001,beta=1,Adam lr1e-5, implementation-validation-only metadata,
the original pretrained reference SHA, unchanged base drift and a student L2
weight change of.00280514 from that reference. Tuned SHA remains
71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1;
pretrained SHA remains
a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea.

At the two cached states, the student's diagonal squared-error loss changes
1205.88799→1206.66459 and .39204159→.39234569 when only the old target is replaced
by its Q8 target. These are cached two-point objective diagnostics, not a
converged objective, dataset audit, likelihood-cost saving or posterior metric.
The large preflight loss also gives no basis for claiming a well-optimized map.

**Recommendation: retain the old checkpoint as an explicitly old-recipe baseline
and warm start; prefer a small, explicit Q8 retuning before calling any new arm
Q8-trained.** Selected gradients agree within the declared tolerances, so neither
a full restart nor discarding the checkpoint is supported. But only1/32 actual
training targets was rechecked, and raw reward shifts are .24/.50nat; treating
those shifts as a global constant or certifying the old training trajectory is
unsupported. This is a numerical-consistency recommendation, not a prediction
that retuning improves inference.

For a training-only oracle baseline, old-recipe generated points can remain
heuristic proposal-training inputs if scored under the current true target and
kept out of evidence/cache/posterior output. The implicit map still has no
normalized logq. Any later integration must retain a tractable corrected final
proposal, broad-support component and likelihood+prior−mixture-logq weights.
No checkpoint has been relabeled Q8-trained or calibrated.

## Cost and next decision

This continuation, including all setup/imports and the zero-call checkpoint
analysis, consumed **25.03 measured CPU seconds**, **27.82 summed command wall
seconds**, one CPU, peak **991304KiB (.95GiB)** and **four new physical endpoints**.
There were no failed scientific attempts or extra physical derivative probes.
A further15CPU seconds was conservatively reserved for incompletely timed
coordination/source/reporting, not measured; measured+reserve40.03CPU seconds
is below the separate120CPU-second cap. The actual scientific commands and
receipts are in `results/stage10/`. No scheduler job ID applies: bounded
interactive validation used mini→COS-MATH2519→ldas-grid. No install or runtime,
credential, PR445, production-default or shared-state change occurred.

Stage9+10 together use62WE=56 physical endpoints+6 scalar reverse VJP seeds.
Identified cumulative cost is **1568.92 CPU seconds**, **1741.82 summed command
wall seconds**, **10052 actual physical endpoints** (10075 attempted rows under
the historical guard-inclusive convention). Earlier42 forward JVP lanes and6
scalar reverse seeds remain separate; previous unit definitions are not silently
converted. Historical teacher acquisition remains>=15,984,560 sampler rows with
hidden work/CPU unknown. Untimed work remains additional, so full cold cost,
complete project elapsed time and any ESS-per-total-cost gain remain unknown.
The old ESS~1 inference pilot remains inconclusive.

The proportionate **next proposed stage, not run or authorized by this
continuation**, is a two-update Q8 retuning smoke: batch4, exactly8 physical
value/VJP endpoints plus two fixed diagnostic endpoints before/after (four
value-only endpoints), **12 physical endpoints /20WE**, one CPU,8GiB,
**300CPU seconds including setup/JIT/failures and validation**. Stop at the cap
or any numerical/support/gradient failure; no silent retries. Warm-start the
student from the old tuned checkpoint while retaining the original pretrained
frozen reference and base, metric, lambda,beta and learning rate. Write a new
separately labeled candidate only if checks pass; retain the old artifact.
Do not replay the31 unexamined historical endpoints as an implicit prerequisite
campaign. The two validation endpoints are implementation diagnostics, never a
higher-reward success or model-selection criterion. A negative result remains
useful; no automatic inference pilot follows.

If that consistency smoke succeeds, a separate decision can authorize the
previously specified matched-cost three-arm real comparison. Until then there
is no new posterior, evidence, accuracy or efficiency claim.
