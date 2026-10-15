# Negative result: close the current candidate/adapter line

The authorized three-arm information-retention benchmark reached **both
predeclared scientific futility conditions on seed261071**. Stop here. Seed261072
was not run; unused calls are not reassigned. There is no automatic retuning,
extra seed, integration run, fitter sweep or sampler rewrite.

## Observed comparison

The score-band formula was fixed before evaluation: median of100 held-out Q8
log-likelihood values minus10, yielding **2850.038794730212nat**. This is a
geometry-retention diagnostic, not a posterior-mass definition. The300 training
and100 held-out cache coordinates were all rescored using the same explicit
float64 roboto Q8/cubic canonical target; they remain finite-ESS old-recipe
empirical samples, not Q8 posterior truth. No held-out row initialized a fitter.

| Arm | Source hits in score band | Fitted-proposal hits | Mean held-out normalized logq |
|---|---:|---:|---:|
| Strong native AV+GMM, all300 cache rows |300/300|0/100|8.67253|
| Same-cache ResamplingOracle |100/100|0/100|8.72575|
| Frozen Q8 map |0/100|0/100|7.07360|

The map's source maximum logL2834.60 is below the band; its median is2191.50.
The full training cache spans2854.14–2863.19 under the **current Q8 target**, so
these useful cache points did not disappear when changing numerical recipes.
The fitted map proposal's held-out logq is **1.65215nat below resampling** and
1.59893 below full-cache native fitting. Nominal paired SE versus resampling is
.08607; this is descriptive only because teacher dependence is not calibrated.
The required minimum+1nat improvement was absent, rather than narrowly missed.

Both declared stops apply:

- All three fitted clouds had0/100 hits: **common fitter failure**.
- Map source0/100, resampling source100/100 (>=90), and map mean logq more than1nat
  below the full-cache native control: **clear map failure**.

The serialized primary reason is common fitter failure; both flags are true.
The result closes this candidate/adapter combination under the stated budget;
it does not establish a universal limitation of learned transport.

## Geometry, density and calibration

All source/fitted clouds retained both inclination branches. Fitted branch counts
were41/59,50/50,48/52, while the held-out reference was40/60. Branch presence alone
therefore did not establish joint-target coverage. Circular, distance and tail
diagnostics are recorded in `physical.json`; no individual marginal was promoted
to a success criterion.

Independent normalization checks passed: AV density/mass errors0 and grouped
GMM mass errors2.22e-16 in all arms. Each actual final density is the same
normalized .2uniform+.4AV+.4GMM mixture. The map supplies only training points;
no implicit-map logq is assigned. Exact draw/density equality and parameter/support
checks passed, with the normalized Planck15 distance prior retained for weight
concentration diagnostics. There was no fallback target or density approximation.

The20% uniform component intentionally broadens unweighted proposal moments;
whole-cloud distance/tail discrepancies must **not** be read as posterior errors.
A zero-call audit of saved labels separates this effect: each arm drew20uniform,
43AV and37GMM points. **All129 AV and111 GMM draws across the three arms also
missed the band.** Thus the uniform safety component alone does not explain the
failure. GMM mean distances385.53,392.89,388.06Mpc were close to the held-out
385.59Mpc, yet their maximum logL values2572.30,2644.75,2115.74 remained far below
the score band. Marginal agreement did not preserve the thin joint geometry.

Corrected logL+logprior−logq weight concentration was diagnostic only: ESS1,
1.319 and1; max weights1,.859 and1. No evidence estimate, posterior population or
main inference cache was created. Existing calibration requirements were not
weakened; these draws do not establish posterior/evidence accuracy or a cost win.
The earlier+.24/+.50nat old/new bridge shifts and selected gradient agreement
remain local numerical facts, not an inference improvement claim.

## Exact work, resources and provenance

The pre-evaluation journal fixes both seeds, all streams, fitter settings,
branch/tail/circular metrics, score-band formula, minimum utility and early stops.
The uncertainty clause was conservatively operationalized before evaluation as
paired mean logq minus2 nominal SE>=1nat, without calling it a calibrated interval.
The first seed already failed the primary empirical conditions; there was no
threshold adjustment or selective continuation.

| Physical work | New true likelihood rows |
|---|---:|
| Shared training cache Q8 rescore |300|
| Shared held-out cache Q8 rescore |100|
| Frozen map source cloud |100|
| Native, resampling and map fitted clouds |100 each|
| **Total** |**800 of1200 cap**|

The100 resampled source scores were exact lookups of already rescored cache rows,
separately recorded as logical reuse, not new physical calls. There were32
completed25-row dispatches, three native density fits,100 neural forward rows,
zero neural updates, zero physical derivative seeds and zero failed scientific
attempts. Every input batch was persisted before dispatch, and scores/labels and
private arrays remain in Caltech stage13/run1. Nothing was spliced into old pilot
populations. Candidate SHA71dacb2f… remains unchanged.

Whole benchmark cost, including imports/setup, waveform/bank/pregrid, JIT,
normalized prior, all calls/fits and diagnostics: **36.50CPU/45.16 wall seconds**.
Saved-component analysis adds **.11CPU/.44 wall seconds**, with zero new calls or
draws. Total **36.61CPU/45.60 summed command wall seconds**, one CPU, peak
**1921744KiB (1.83GiB)**. A further35CPU seconds was conservatively reserved for
incompletely timed source/coordination/provenance/reporting work, not measured;
measured+reserve71.61CPU seconds is below300. Total timing includes all scientific
work; exclusive per-arm CPU/setup allocations were not measured, so no per-arm
speedup is inferred. No GPU or scheduler job ID applies: bounded interactive work
used mini→COS-MATH2519→ldas-grid. No install/security/shared-runtime/default/PR445
change occurred.

The raw manifest hashes all39 scientific arrays/source/journal/timing artifacts;
local final manifest adds postprocessing source/results. The four native fitting
source hashes exactly match those audited in stage4; ResamplingOracle's hash is
also recorded. The target retains the pinned isolated retry-core SHA70fb605f…,
full21-mode H1/L1 bank and fixed raw time-integral window. All source/data/model
pins descend from the stage11 manifest and the stage13 journal. Commands and
GNU-time receipts are committed; raw private draws remain on Caltech.

Identified cumulative investigation cost is now **1644.15CPU seconds**,
**1831.15 summed command wall seconds**, **10864 actual physical endpoint rows**
(10887 attempted under historical guard-inclusive conventions),42 prior forward
JVP lanes and14 scalar reverse seeds. The candidate's separate38.16CPU-second/
20WE retuning remains a preparation cost: R=1 assigns it once; R=10 assigns
3.816CPU seconds/2WE per reuse. Neither scenario erases prior model preparation,
this investigation, or the **>=15,984,560-row historical teacher acquisition**
with unknown hidden work/CPU. Full cold cost remains unknown; resampling is not
being declared a calibrated inference or total-cost winner.

## Terminal scientific decision

The frozen map failed minimum practical utility versus cheap identical-data
reuse, and the common fitting stage failed to exploit even the full known-good
cache at this budget. The useful result is a **negative method/adapter comparison**,
not another passed smoke or equally unconverged evidence table.

**Close this candidate/adapter line.** A different representation, geometry-
preserving joint proposal or different learned-map training objective would be
a new scientific direction requiring explicit user judgment. No such work is
started, scheduled or implicitly authorized by the remaining budget.
