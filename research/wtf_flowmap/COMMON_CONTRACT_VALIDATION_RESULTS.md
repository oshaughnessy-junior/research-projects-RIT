# Common-contract validation stopped: physical reconstruction mismatch remains

**Latest result:** [ROBOTO_REUSE_RESULTS.md](ROBOTO_REUSE_RESULTS.md). The existing
roboto reflected-Q8/cubic recipe passes the declared three-point local contract
with the same converged quadrature and stricter tolerances. Earlier coarse-sinc
failures remain preserved; no inference, production default or checkpoint changed.

**The contract is not resolved.** The authorized array-only gate passed. At the
first physical point, integration and all six derivative components converged on
the shared frozen grids, but independent dense physical gathering disagreed in
value by **0.00134700788 nat**, above the unchanged 0.001-nat threshold. Validation
stopped immediately. The other two points and every mixed-direction FD stencil
were not run. No inference, production-library change or separate PR followed.

**Interpretation correction remains:** the original stage4 NaN was deliberate
integral-certificate refusal with finite physical primitives. This stage does not
resurrect a claim of a nonfinite physical model or a generic arithmetic NaN defect.
The four ESS~1 pilot outcomes remain inconclusive and the incomplete cache
population remains failed; no likelihood row was dropped or replaced.

## Journal and fixed scope

Before execution, `results/stage7/journal.json` fixed the resource limits, case
order, grid, thresholds and work accounting; its hash was verified against the
executed remote copy. The array gate allowed 30 CPU seconds. The physical stage
allowed 40 work equivalents, 180 CPU seconds, one affinity-pinned CPU and 8 GiB,
including setup, JIT, refinement and failed work. The case order began with the
faithfully reconstructed stage4 failure. It remains a reconstruction, not an
original failed-batch capture; stage5 provenance and input/source hashes persist.

One work equivalent conservatively charges one attempted physical assembly at
an endpoint with at most 78465 time nodes, plus one additional equivalent per JVP
direction. Coarse assembly is charged the same as the maximum-grid assembly.
These are budget tokens, not FLOP or runtime equivalence. Actual dispatches,
JVP directions, time-node counts, FFT columns and reduction traversals are recorded
separately. All counters advance **before** dispatch. Coarse-to-fine algebraic
refinement cannot be mistaken for a free physical reassembly.

The adapter is confined to `research/wtf_flowmap/common_contract_adapter.py`.
No production library or shared runtime changed in this stage. It uses a fixed
32x/64x/128x grid and 512/1024 coarse-sample guards, complex primitive interpolation
before reduction, stable maximum-subtracted positive quadrature, and a constant
in-time model norm. SciPy FFT/resample plus Simpson is checked against separately
implemented NumPy FFT padding plus trapezoid. Input derivative arrays use physical
order `(RA,dec,psi,incl,phase,D_Mpc)`. Unsupported time-dependent norm is refused.

The exact represented geocentric interval remains
`[-.074951171875,.07470703125]` seconds, 614 coarse nodes at 1/4096s. Its duration
is 613/4096=.149658203125s. Guards add support, not time-prior mass. Raw log integral
is primary; the optional normalized uniform time average adds 1.8994012307831323.
The gradient is unchanged by that fixed constant. No prior or decoder-Jacobian
term enters the physical reward. The mode, data, waveform, time-grid, precision
and source pins are inherited and rechecked by the existing context loader.

## What passed, and what failed

Array phase: analytic constant with all-six derivative components, analytic ramp
integral/derivative and normalization, and time-dependent-norm refusal passed.
All three saved real primitives also passed the two successive 32->64->128 value
and supplied distance-derivative checks, guard agreement and independent method
agreement. Saved-fixture derivative coverage there is distance only; it is not
six-component physical gradient validation.

Physical phase: one coarse guarded primitive assembly with six JVP directions
provided all six physical derivative arrays. The array adapter then integrated
value and derivative numerators on identical fixed nodes and weights. Its first
point passed both successive doubling comparisons and independent guard/method
checks. Maximum value discrepancy was 8.11e-12 nat; maximum component derivative
error divided by its reference tolerance was 1.584e-6 (<=1 passes). Reference
tolerances stayed 1e-4 nat and .001+.0001*abs(g_ref); cross-implementation tolerances
stayed 1e-3 nat and .01+.001*abs(g_ref). None was relaxed.

The independent physical reference called the maintained NumPy **NoLoop** API
with `return_lnLt=True`, `explicit_time_values=True`, sinc interpolation and the
original Q cadence on 78465 explicit time nodes. This returns the physical time
field without invoking its coarse integration rule. The same maximum-grid field
was decimated to 64x and 32x, so those comparisons required no new physical assembly.
The research adapter supplied all integrations explicitly; no fallback was inserted.

| Construction | Raw log integral |
|---|---:|
| Coarse primitive, converged guarded reconstruction |47.829242649495455|
| Independently gathered physical field,32x Simpson |47.83058999942547|
| Independently gathered physical field,64x Simpson |47.830589682703454|
| Independently gathered physical field,128x Simpson |47.83058965737513|
| Independently gathered physical field,128x trapezoid |47.83058965661620|

The independent dense-grid/method spread is 3.42e-7 nat, below the 1e-4 reference
quality threshold. The physical reconstruction difference is 1.34701e-3, above
the 1e-3 cross threshold. The same time-prior offset leaves this difference intact.

Reporting-only arithmetic on the captured arrays found the two fields agree at
the original 614 coarse nodes to **1.7053e-13**. Thus neither a coarse-node assembly
nor a constant normalization mismatch explains this result. It localizes the gap
to the between-node constructions: directly gather the stored correlations at
fine times versus interpolate the already gathered coarse primitive. This does
not yet isolate the contributions of finite-stencil interpolation, extension or
reconstruction accuracy against a separately converged continuous physical model.
No additional stencil/support study was run after the stop.

The original NoLoop coarse Simpson 47.83659196403751 is a historical diagnostic
from earlier stages, not a new probe or the forced reference here. Neither that
coarse number nor the converged FFT interpolant is promoted to exact physical
truth. The mismatch is not resolved by improving quadrature of either field alone.

## Accounting and regression limits

| Work | Equivalents | Actual dispatches | JVP directions | Physical time nodes |
|---|---:|---:|---:|---:|
| Guarded primitive plus full parameter JVPs |7|1|6|2662|
| Independent maximum-grid physical field |1|1|0|78465|
| Remaining points and FD stencils |0|0|0|0|
| **Consumed** |**8/40**|**2**|**6**|**81127**|

Including direction lanes, the assembly time-node equivalent count is 97099.
The primitive reference made four FFT constructions; every input/output size and
active derivative-column count is retained. Its 12 fixed-grid value/derivative
reductions traversed 549260 nodes, and the independent dense-field reductions
traversed 274630 nodes. These mathematical traversals are separate from physical
endpoint calls, and their CPU is included. There was no hidden optimizer,
likelihood retry, proposal training, neural update or sampler integration.

Measured whole-command costs, including imports/setup/JIT/refinement:

| Command | CPU seconds | Command wall seconds |
|---|---:|---:|
| Array gate |1.23|2.42|
| Physical gate, stopped at mismatch |30.08|40.36|
| Reporting-only reads/arithmetic/hashes |.09|.45|
| **Stage total** |**31.40**|**43.23**|

Peak RSS was 1528444 KiB (1.46 GiB). Both phase budgets and the memory cap were
respected. Caltech interactive execution used affinity [0], no GPU and no batch
job; there is no scheduler job ID. No environment install, credential change or
alternate compute site was used. Transfer/coordinator/admin time is incompletely
measured and remains explicit rather than zero.

Passed evidence is limited to the listed analytic/refusal tests, three saved-array
fixtures, and the first physical point's frozen-grid value/all-six derivative
convergence. **Not run:** mixed-direction physical FD, the other two physical
points, full physical cross-backend derivative convergence, broad exposure sweep,
full production/CI/GPU gates, posterior/evidence comparison. No full common-contract
or production correctness claim follows from the partial pass.

## Production exposure and next decision

This is a preexisting numerical reconstruction/API-contract question exposed by
the research adapter. Unknowns include which deployed paths encounter these
conventions, frequency across intrinsic points/SNR/time bands/stencils, CPU/GPU
behavior, accuracy against an independently converged physical reconstruction,
and any effect on posterior/evidence results. No affected production result has
been established. A low-valued selected test point cannot establish impact or
justify silently excluding it.

Stop the authorized validation here; unused 32 equivalents and CPU are not a new
allocation. Richard may choose a separately scoped numerical reconstruction/
contract investigation or draft change. That should decide the intended continuous
primitive and independently validate its interpolation/support, before changing
shared libraries or spending on another sampler comparison. A separate production
PR remains a recommendation, not an automatic continuation of this blue-sky task.

Identified cumulative command receipts now total **1163.12 CPU seconds (19.39min)**,
**1302.79 summed command wall seconds**, and **9972 actual real endpoint-dispatch
rows**, plus the separately logged six new JVP directions. Earlier stages did not
use this work-equivalent convention, so no exact cumulative equivalent total is
claimed. Identified attempted endpoint rows are 9995, including earlier pre-dispatch
guard rejections. At least 3740858 cheap synthetic sampler-callback rows remain a
separate category. Pre-existing teacher acquisition remains **>=15,984,560
sampler.ntotal rows**, with hidden work and CPU unknown. Earlier untimed checks,
source preparation, transfers and administrative gaps remain additional. This
is an auditable measured subtotal, not full cold cost or a performance claim.
