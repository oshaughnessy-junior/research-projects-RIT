# Common quadrature contract: source-only decision, no new physical runs

**Correction retained:** the stage4 NaN was JAX's deliberate refusal of a failed
integral-doubling certificate. Physical primitives were finite. The research retry
repairs that refusal, but does not make the legacy NoLoop coarse rule a converged
reference. All four ESS~1 outcomes remain inconclusive; no comparison was rerun.

**Next decision:** pause the WTF inference comparison. First build a small,
array-only common reference/adapter and its convergence tests. Treat the shared
likelihood-validation question separately from the sampler experiment. A later
physical validation should be a single bounded contract test, not a new inference
campaign. Neither that test nor a separate draft PR was launched in this stage.

## Scientific target and normalization

For this nonrotating, fixed-distance, non-phase-marginalized case, the reference is

    I(theta) = integral[a,b] exp(Re kappa(theta,t) - rho_squared(theta)/2) dt.

The physical model, retained modes, detector data/PSD, calibration assumptions,
interpolation/reconstruction, precision and geocentric time origin must be pinned
before comparing numerical implementations. In particular, the model norm is
constant in time here. This contract does not silently extend to arrival-time-
dependent norms, phase/distance/calibration marginalization, or pruned mode banks.

The integration window is exactly the represented closed interval
`[-0.074951171875, 0.07470703125]` seconds relative to the fixed event reference.
It has 614 nodes, spacing 1/4096 and duration **T=613/4096=0.149658203125 seconds**.
A guard is reconstruction support, not extra probability mass. Reflection is a
numerical continuation, not a periodic time prior. Include both endpoints as
quadrature nodes with the appropriate endpoint weights; do not replace T with
614/4096 or nominal 0.15 seconds. Endpoint mass is a truncation diagnostic; it
must not select a different rule, drop a sample, or enlarge the physical domain.

Keep the existing API's raw `log I` explicitly named and available. If a normalized
uniform time prior is requested, return **log I - log T**, an additive
**+1.8994012307831323** for this fixed window. Both endpoints must declare the same
choice. Its derivative is unchanged because T is fixed. Normalized time averaging
is appropriate for evidence under that declared prior; the raw seconds-valued
integral remains the historical diagnostic. Do not retrospectively change stage4
numbers or infer posterior draws from a reward map. Angular/distance priors and
decoder Jacobians stay outside this physical log-likelihood definition.

## Two distinct validation layers

1. **Integration of a defined reconstruction.** Start from the identical immutable,
   finite complex kappa and time-independent norm arrays, with explicit crop,
   guard/taper/even-extension convention. Reconstruct the complex primitive before
   any nonlinear phase or distance reduction. Compare independently implemented
   fine-grid integration (for example SciPy FFT plus Simpson versus JAX FFT plus
   stable trapezoid); test analytic fields as well. Maximum subtraction is an
   offset-invariant numerical operation, not a target or normalization change.
2. **Accuracy of that reconstruction for the physical model.** Before accepting a
   common physical target, compare the reconstructed primitive with independently
   densely gathered values from the maintained precomputed correlation series on
   the same interval. Increase time resolution and interpolation support/stencil
   accuracy separately. This can reuse a verified packed bank; it does not require
   changing the waveform model. If no immutable bank snapshot exists, a later
   rebuild is setup work that must be budgeted. No such rebuild occurred now.

Layer 1 alone cannot certify the original physical integral. Two FFT integrators
can agree perfectly while sharing a boundary/reconstruction error. Conversely,
forcing either backend to match coarse Simpson merely preserves an unconverged
approximation. The current source also has distinct reflection conventions:
NoLoop's refined path uses literal `[x,flip(x)]` (length 2N), whereas JAX uses the
standard 2(N-1) even extension with guarded taper. This was not needed to explain
the failed row, which NoLoop never refined, but must be controlled in a general
comparison. Choose a representation only after layer 2 validation; do not simply
rename the JAX interpolant “truth.” Historical DESIGN prose and current source
must be distinguished when reviewing these conventions.

## Value and derivative convergence

Retain the original cross-backend acceptance tolerances: **1e-3 nat** for value
and componentwise **.01+.001*abs(g_ref)** in the declared physical coordinates
`(RA,dec,psi,incl,phase,D_Mpc)`. These are not relaxed to accommodate this row.
A numerical reference should consume at most one tenth of that error budget:
1e-4 nat and .001+.0001*abs(g_ref), respectively. These are proposed reference
quality targets, not newly measured global guarantees.

For an accepted reference, require two successive refinement comparisons
`f->2f` and `2f->4f` to pass **both** value and derivative criteria, plus independent
support/guard checks for both quantities. Width is a starting-cost heuristic,
not an accuracy certificate or a reason to keep coarse Simpson. Keep a finite
resource ceiling; if it is exhausted, return an explicit numerical refusal and
stop the experiment. Do not represent refusal as zero likelihood or treat it as
an invalid physical configuration. Certificates are numerical evidence, not a
rigorous universal error bound, so analytic/adversarial fixtures remain necessary.

For fixed limits, each physical derivative satisfies

    d log I / d theta_i = E_time[ d (Re kappa - rho_squared/2) / d theta_i ].

Integrate value and derivative numerators on the **same nodes and weights**.
Freeze a jointly converged factor and support choice across the base point and
finite-difference stencils; do not differentiate an adaptive selector or compare
finite differences that silently switch quadrature rules. Validate JAX AD against
central differences at h and h/2 on that common grid, with finite-difference
self-convergence inside the reference error budget. Suggested predeclared steps
are 1e-5/5e-6 radians for the five angular coordinates and 1e-3/5e-4 Mpc for distance;
report cancellation/curvature failure rather than tune steps to pass. Periodic
coordinates use continuous representatives and their known physical periods.
Any parameter-dependent time window would need explicit boundary derivatives;
that is outside this fixed-window contract.

Earlier successful pointwise gates are not invalidated: stage5's two old/new
physical controls had exactly unchanged values and gradients. They are also not
a substitute for this derivative-convergence contract over a broader domain.
Full learned-objective revalidation and training are not part of this planning stage.

## Cheap checks performed now

Only saved stage5 primitives were loaded; no RIFT physical callback or waveform
code was imported. One affinity-pinned Caltech CPU performed 42 array quadratures:
30 at the recorded points across factors 4,8,16,32,64 and guards 512/1024, plus 12
algebraically distance-rescaled evaluations. The latter use the already specified
kappa~1/D and rho_squared~1/D^2 laws; they are mathematical sensitivity checks,
not new physical-backend probes. They do not test angular derivatives.

The 32x->64x maximum value difference is recorded in results/stage6/summary.json;
all three distance derivatives also stabilize and match two algebraic central
stencils. For the failed point, the refined value remains 47.82924264949543 and
the distance derivative is -.02201479807120748. The time-prior offset is exactly
the stated constant and has zero derivative. These checks support layer 1 for
these arrays only. Higher-factor double confirmation and layer 2 remain to be run;
there is no claim that every new reference gate has already passed.

This stage used **zero new waveform/physical-likelihood calls**, **.77 measured CPU
seconds / 1.92 command wall seconds**, peak 206460 KiB. Input hashes and full array
results are retained, with no raw scientific primitives committed.

## Minimal isolated implementation and exposure

Add a research-only reference accepting a hash-identified packed primitive and,
when available, its physical derivative arrays. Make its output carry raw log
integral, optional normalized-time average, gradient, factors/support, estimated
successive differences, node counts, precision, assumptions and refusal reason.
Pin one reconstruction contract; share nodes/weights with the JAX AD adapter,
while retaining an independent NumPy/SciPy implementation as the numerical check.
Use it consistently for every comparison arm. Keep existing NoLoop output as a
labeled legacy-rule diagnostic until convergence is demonstrated. Do not patch
the comparison with per-row fallback values or make an implicit map a density.

First expose six portable families: analytic constants/ramps/Gaussian or coherent
sinusoid integrals; odd/even/Nyquist and reflection/crop conventions; boundary
mass and normalization; a coarse-width-resolved counterexample; derivative and
selector-boundary checks; and explicit failure at resolution/guard/resource
ceilings. Keep the recorded real primitive as a separately pinned private fixture.
Register portable cases with the explicit CI manifest and collection floor;
private fixtures must be explicitly excluded from ordinary CI and run manually.
Capture attempted inputs before evaluation and budget every value, derivative
lane, FD row, setup/JIT and failed attempt. Track replay/quadrature-node work
separately from physical assembly and neural/surrogate work.

This is a **preexisting shared numerical-validation/API-contract issue exposed by
the research adapter**, not evidence of a new map-induced NaN or a generally
nonfinite physical likelihood. The two code policies predate the learned experiment,
and the same saved coarse primitive reproduces their disagreement. It is worth a
separate focused draft contract/regression change because those APIs are used
outside this sampler. Production impact, frequency and affected posterior/evidence
results have **not** been established. Do not describe historical production
results as wrong. Do not broaden this into a production sweep or silently alter
production quadrature defaults. A standalone behavior fix needs its own review
and independent reconstruction evidence, even if the WTF work is abandoned.

## Proposed next budget and stop decision — not executed

Start with **zero-call array-only adapter tests**, capped at 30 CPU seconds, one CPU.
Only after agreeing the common contract should one bounded physical validation be
considered: the three fixed points, **40 conservative attempted endpoint/directional
work equivalents**, **one CPU, 180 CPU seconds, 8 GiB**, including setup and failures.
A possible allocation is 21 equivalents for three primitive values with six JVP
lanes each, 3 independent NumPy assemblies, 12 mixed-direction h/h2 stencil rows,
and 4 reserve equivalents. Record actual dispatches and derivative lanes separately;
never call the budget “three evaluations” by hiding AD work. This tests all six AD
components across grids, but only a mixed-direction physical FD check; it is **not**
a full per-coordinate two-step physical FD campaign. That broader validation would
require a new estimate. Reuse a verified bank snapshot if available; otherwise
include the rebuild. Estimated 80–130 CPU seconds is based on stage5 setup/gradient
and regression receipts; 180 is the hard proposed cap, not permission to retry past it.

Stop if either numerical layer or derivative test fails. Even a pass would justify
only deciding whether a better-designed, separately budgeted sampler pilot is
worthwhile. Current ESS~1 outcomes do not justify a powered held-out-anchor study.
No new physical run, inference comparison, production sweep or standalone PR was
started here. Cumulative costs and their substantial missing historical components
are in results/stage6/summary.json; future work should remain separately authorized.

## Identified cumulative cost, without hiding the historical input

| Recorded group | CPU seconds | Summed command wall seconds | New real physical rows |
|---|---:|---:|---:|
| Simplified analytic-target prototype, seven timers |69.47|85.74|0|
| Earlier learned/real stages, published selected subtotal |450.30|493.95|378|
| Three separately timed model checks |9.84|13.81|0|
| Five-arm pilot, including failures and diagnosis |242.84|266.32|9554|
| Certificate investigation, including failures |358.50|397.82|38|
| This saved-array planning check |.77|1.92|0|
| **Identified timed sum** |**1131.72**|**1259.56**|**9970**|

That is **18.86 CPU minutes**, not complete task elapsed time. The selected real
stages record 9993 attempted endpoint rows, including guards that prevented physical
dispatch. The initial prototype separately audited at least 3740858 inexpensive
synthetic sampler-callback rows; those are not real gravitational-wave target
rows or comparable FLOP units. Earlier synthetic validation rows were not all audited.

The teacher acquisition was pre-existing, not newly launched by this investigation:
**>=15,984,560 sampler.ntotal rows**, with hidden likelihood work, setup/device/CPU
cost unknown. Its cost still belongs in any cold oracle comparison. Do not treat
cache availability as free acquisition, or add synthetic and physical counts into
an alleged exact call total. The identified receipts also omit some untimed checks,
coordinator edits/admin, transfer/SSH time, and gaps. The sums are auditable recorded
work, not an unconditional complete-cost claim or an efficiency result.

Saved-array 32x->64x maximum differences are 2.6012e-10 nat in value and 1.3065e-11
per Mpc in the distance derivative. The largest discrepancy from the two algebraic
FD stencils is 1.7504e-9 per Mpc. These small errors say nothing about unsaved angular
primitive derivatives or physical reconstruction error. They do not cure ESS collapse.

Source basis: current `jax_ile/core.py` primitive certificate/reflection functions;
`time_marginalization_quadrature.py` classification/refinement and literal reflection;
maintained `factored_likelihood.py` NoLoop dispatch; JAX README and historical
`DESIGN_time_marginalization_quadrature.md`. Exact stage5 source/input hashes remain
in the retained results; this planning stage changes none of those algorithms.
