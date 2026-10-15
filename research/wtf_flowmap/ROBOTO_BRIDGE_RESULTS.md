# Complete reward/control bridge recheck (stage9)

Update: the separately budgeted [four-endpoint continuation](ROBOTO_BRIDGE_CONTINUATION.md) now completes both selected bridges. This original partial-stage record and its costs remain unchanged.

This stage audits the existing periodic learned-map objective under the already
verified roboto reflected-Q8 plus cubic recipe. It does not train, replace a
checkpoint, run inference, change a production default, or modify draft PR445.
The pre-dispatch scope and budget are in `results/stage9/journal.json`.

The exact frozen reference map and drift from the original training are replayed
at two states: original seed261040 preflight0, and member0 of the actual first
seed261041 training batch, with its original random draw order. The latter is not
a newly invented validation point. Both learned lookaheads, physical decoder,
reward pullback and control-cost input derivatives remain in the graph. The
tuned student participates in the diagonal and periodic semigroup loss and one
backward pass per state, with no optimizer construction or step.

The reward remains physical raw log time integral, not a posterior density or
evidence. Physical parameter order is RA,dec,psi,incl,phase,D_Mpc. The fixed window
and normalized time-prior offset are recorded in the result. Angular/isotropic
and Planck15 distance-prior contracts are checked against the pinned driver;
parameter prior and decoder Jacobian are not added to the physical reward.
JAX and Torch are explicitly float64. Physical support is checked before every
dispatch. The already reviewed research-only one-extra-certificate-doubling core
is loaded under a separate module name, leaving the shared runtime untouched.

Each state compares Q8 and Q16 canonical value/physical gradients and the exact
old q1/sinc callback. An independent NumPy NoLoop Q8 field at 78,465 time nodes
is reduced on factors32/64/128 with two integration rules. Every native FD row
must satisfy 1e-4-nat integration convergence. All six state coordinates are
checked at h=1e-4 and h/2 through the complete value, with original full-gradient
tolerance .01+.001*abs(g). The purely neural control gradient has a separate 1e-7 absolute / 1e-5
relative FD check; completion is reported per state below. No coarse-rule substitution is
made to force agreement. Existing source-reported full-frequency validation
remains source-reported; Q8/Q16 share a boundary family.

## Cost interpretation

One endpoint primal costs1 work equivalent; one scalar reverse VJP seed adds1.
These reverse-mode units are recorded separately from earlier forward JVP lanes
and are not asserted to cost the same FLOPs. Direct dense FD rows each cost1.
The planned total is62WE (56 endpoint dispatches, six scalar VJP seeds), cap64.
Independent physical rows, derivative work, setup, Q-pregrid transforms and all
integration refinements are included in command CPU/wall receipts. Internal
adaptive FFT/reduction traversal counts are not independently instrumented in
this stage; a WE is a declared accounting token, not a wall-time prediction.
Cached replay of an already measured physical value/gradient performs only
neural/decoder algebra and is separately metered, not charged as another true
physical evaluation. Checkpoint files are read-only inputs.

One import failure used zero physical probes and is included in the receipts.
A local py_compile cache write was rejected by the sandbox; AST parsing instead
validated syntax. No scheduler job was submitted: this is bounded one-CPU
interactive validation on ldas-grid through mini → COS-MATH2519 → Caltech.

## Measured outcome: partial, stopped at CPU reserve

| State | Full bridge | Q8/Q16 physical gradient error / tolerance | Canonical minus independent raw value (nat) | Old/new full-gradient error / tolerance |
|---|---|---:|---:|---:|
| Original preflight0 | Passed | .00992 | +7.223e-6 | .6213 |
| Actual first training member0 | Incomplete | .01690 | -4.968e-6 | .7283 |

For preflight0 the six-coordinate full AD/FD error ratio is .000444 and h/h2
self-convergence ratio .0000745. Its control FD and tuned-student loss backward
pass; student weights remain equal. The training member completed all h stencils
and the first four coordinate pairs at h/2; the final inclination and log-distance
state-coordinate pairs (four physical endpoints) and subsequent pure-neural
control/loss checks were **not run**. The ledger retains its raw `running` case
status when the outer run records the explicit budget stop; it is not a pass.

Historical replay is faithful: old callback physical-gradient norm
5573.794392712092 matches the recorded first-training value5573.794392712065.
The two old/new pulled-back gradients fall within the declared .01+.001*abs(g)
tolerance, but raw rewards shift by +.2418003 and +.4986873 nat. This is evidence
of a numerical recipe change, not a posterior improvement. Neither these two
states nor their gradient agreement certify the other31 original training
endpoints. The original checkpoint remains an unchanged old-recipe trained
artifact; it is **not certified Q8-trained, calibrated, or efficient**. Its tuned
SHA remains71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1.

Cost including the failed import, setup/JIT and every dispatched derivative/FD:
**248.61 CPU seconds**, **252.65 summed command wall seconds**, one CPU, peak
**2616468KiB (2.50GiB)**, **58/64WE =52 physical endpoint dispatches +6 scalar
VJP seeds**. A further35CPU seconds was conservatively reserved for incompletely
timed coordinator/source/reporting work, not claimed as measured. Combined
measured cost plus that reserve is283.61CPU seconds, below300. The scientific
process stopped before another dispatch at its245CPU-second internal reserve;
no budget was silently extended. It exited0 because the script serializes the
failure: **the numerical check status is `stopped`, not passed**. Receipt commands
are preserved verbatim. No scheduler job ID applies.

Identified cumulative cost is1543.89CPU seconds,1714.00 summed command wall
seconds,10048 actual physical endpoints;42 earlier forward JVP lanes and6 new
scalar reverse VJP seeds remain separate. Historical teacher acquisition is
still>=15,984,560 sampler rows with unknown hidden work/CPU. Complete cold cost
and project wall time remain unknown; no speedup or ESS-per-total-cost win is
claimed. The old ESS~1 pilot is still inconclusive.

## Next decision

No inference comparison is justified by this incomplete gate alone. First
finish the four stored-state h/2 probes and pure-neural checks in an explicitly
bounded continuation, then assess the remaining historical training gradients
before deciding whether the old checkpoint can be reused or any retuning is
needed. Do not rerun the already completed physical stencils. No checkpoint
replacement follows automatically.

Once that prerequisite is resolved, the smallest useful real comparison is one
fixed seed and three arms (no oracle, existing cheap oracle, transport guidance),
each with the same2000-total-physical-call cap including that arm's actual setup,
training, precision/optimizer work and final sampling. Use the identical
corrected final sampler with a broad-support component, raw likelihood plus
normalized prior minus mixture logq, and training-cache separation. Reserve
physical calls for final sampling **before** selecting any training allocation;
if preparation exhausts an arm's cap, report that failure rather than grant
extra calls. This would be a feasibility comparison only; a five-seed followup
requires a separate decision. Report evidence, mode/moment/tail errors and ESS
per total calls/wall time, with historical acquisition separately amortized and
cold-cost uncertainty explicit. No automatic pilot is scheduled or launched.

Syntax AST validation and diff whitespace checks passed locally. No additional
regression suite was run in this bounded stage; stage8's five float64 selected
pregrid tests remain prior evidence. Research-head CI status is recorded after
push; draft PR445 remains unchanged at acf8dc1cc6838ee17a7b6246bcfa2b5be30ee510.
