# AV coverage diagnostic and opt-in O4c guard

## Diagnostic

`unseen_fraction` reports the likelihood weight of retained points alone in
their final-grid bin. `unseen_frac_final` and the `[AV mc diag]` line expose it;
above `COVERAGE_UNSEEN_WARN` the sampler prints a warning. The diagnostic alone
changes no result. It estimates occupancy risk, not the integral's bias.

The retained-density estimator uses all historical proposals, including the
initial full-box proposal. Later grids depend on the same points subsequently
reweighted. Fixed-proposal balance-weight reasoning therefore does not prove
this adaptive estimator unbiased. Final-grid holes are relevant diagnostics,
without being identical to lost posterior mass or integral error.

## Experimental guard

The guard ports `_coarsen_for_coverage` from upstream O4c commit
`1c1fa5b9e7b2cb464ae531489c18e73f9e6d843e`, introduced by upstream PR224.
O4d keeps it disabled by default: `coverage_unseen_tol=0`.

Enable it through the standalone AV `integrate_log` keyword
`coverage_unseen_tol=0.05`, or the sampler attribute of that name. The attribute
persists until explicitly changed, as in O4c. This port adds no ILE CLI option.
It acts only in standalone `integrate_log`, not portfolio-member adaptation.

After the ordinary bin allocation, it checks weighted singleton occupancy.
When one adaptive axis occupies at most two bins and another spans more, it
coarsens the axes with the largest occupied spans by a factor of 0.8 until the
singleton fraction meets the tolerance or the span condition no longer holds.
Bounds enforcement floors the changed bin counts. The existing grid builder
then rebuilds occupied bins at the new resolution. `n_coarsen_cycles` counts
cycles where the guard changed resolution.

The default path skips the helper entirely, consumes no additional random
numbers and preserves the existing estimator, retention and stopping rules.
The enabled path changes proposals. It leaves the #412 density formula intact
and does not establish unbiasedness.

## Evidence and limits

Paper-repository records own numerical results:
`analyses/av412_native_bias_20261116/RESULTS_20261116.md` and
`analyses/av412_matched_20261115/NOTE_math_review_20261116.md`.
They compare O4c guard on/off, demonstrate adaptive-recycling bias, and separate
threshold retention from final-grid coverage. Superseded low-count discovery
measurements remain in their historical records.

An earlier O4d coarsening experiment was withdrawn after it failed to activate
in problematic event configurations and reduced effective counts elsewhere.
That history is recorded in O4d PR458. This opt-in port requires matched event
tests and the enabled shape-recovery gate before any adoption decision.
