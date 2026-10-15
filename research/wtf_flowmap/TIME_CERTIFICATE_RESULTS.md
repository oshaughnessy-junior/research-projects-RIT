# Isolated valid-point NaN: certificate retry and a separate quadrature mismatch

The original valid-point NaN is a deliberate **failed integral-doubling certificate**,
not nonfinite physical primitives, pruning, a coordinate error, a prior mismatch,
or demonstrated exponent overflow. A narrow research-branch patch permits one
additional certified doubling. It makes this point finite and agrees with an
independent dense SciPy reference. It does **not** establish a common numerical
target with maintained NoLoop: NoLoop selects coarse Simpson for this row and
its value and derivatives differ. No larger inference study should start yet.

## Provenance and deterministic reproducer

The point is index 12 of stage4's reconstructed cache failure batch, in physical
order `(RA,dec,psi,incl,phase,D_Mpc)`:

```
[3.3098110759092685, 0.048317966390789106, 1.0625090397890906,
 1.3211940156563102, 6.169022226149153, 1861.1068204841292]
```

It was **reconstructed**, not captured in the original failing call. Stage4's
zero-true-call replay matched 15 preceding saved batches exactly. Stage5 verified
the full reconstructed batch against the saved diagnostic array bit for bit,
then repeated the failing point and two earlier parity controls. Source, input,
coordinate and artifact hashes are in results/stage5/extraction.json and
supporting.json; raw primitives remain on Caltech. The failure primitive hash is
4abebb6f1246f693821f339de328050f12058cf649758ff2122e16b360c84598.

The existing pinned likelihood sources were reused, including core SHA
0f499791e796558b7b2460027ef8459302925dcf792801883664ceaf56336ab8 and NumPy quadrature
SHA 6f21ed72f1612c187df24db5fa666608104621dd150bf68a7b77bcb36b7ca44b.
All 21 modes per H1/L1 remain retained, `ignore_threshold=None`; no phase
marginalization, sinc interpolation, float64/JAX x64, identical 614-node closed
window, dt 1/4096, integration-window grid SHA
58069d49c1da7eab8cddf84193abff20e5a1ba5dda5af42c7b2ff04c08684d63.
Physical storage support is unchanged, with 512/1024 initial/certified guards.
Both APIs return the same raw time-integral convention, without prior or new
normalization terms. Distance and angular bounds and coordinate round trips pass.
No implicit-map density or posterior assumption enters this diagnosis.

A portable synthetic regression requires no astrophysical inputs: 17 integration
nodes, 128 guard nodes each side, lnL on the crop approximately 0.2*t, and dt 1.
The analytic integral is log(expm1(3.2)/.2). Coarse->2x trapezoid change exceeds
1e-3, whereas 2x->4x passes. The original implementation refuses it; the retry
accepts it within 1e-3 of the analytic integral. The real saved primitive adds an
independent SciPy FFT/resample plus Simpson 32x dense reference, avoiding a test
that merely compares the patched routine with itself.

## Root cause and separate rule discrepancy

All extracted complex kappa and rho-squared entries are finite. For the failed row:

| Refinement factor | Guard 1024 log integral | Difference from previous |
|---:|---:|---:|
|1|47.82795959124954|—|
|2|47.82924266156774|0.00128307031820|
|4|47.82924264949543|0.00000001207231|
|8–32|47.82924264949543|roundoff|

The starting-factor curvature heuristic chose 1, and the remeasured width already
passed the 2-samples-per-sigma criterion. Guard 1024 versus 512 differs by only
8.15e-12 nat on the refined row. The **first integral-doubling comparison alone**
failed the fixed 1e-3-nat threshold. The old code returned NaN immediately rather
than try the next certified pair. No physical input or log likelihood was NaN
before this deliberate refusal. Stable maximum-subtracted integration is used.

Maintained NoLoop classifies this coarse row as already resolved and does not
refine it: its report has factor 1 and zero refined rows. Applying that exact
NumPy rule to the **same extracted JAX coarse primitive** returns
47.83659196403750, reproducing the physical NoLoop 47.83659196403751. Thus
assembly/pruning/coordinate differences are not needed to explain this case.
Dense converged JAX/SciPy integration is 47.82924264949544, a difference of
-.00734931454207 nat. The methods use different quadrature acceptance/rule
contracts despite targeting the same physical time integral. A constant
normalization change cannot explain their derivative disagreement.

## Narrow patch and regression scope

Only the research checkout's `_time_marginalize_reflected_primitive` changes.
Previously successful rows return exactly their existing value; failed first
pairs may try one extra factor doubling. The retry must pass remeasured width,
endpoint (when requested), two-guard agreement and successive-integral
agreement. Tolerances, physical domain and maximum fine factor 2048 are unchanged.
Retry exhaustion still returns NaN; no Simpson, NumPy or other fallback is used.
No shared runtime or pinned old source was edited. The patch was loaded from an
isolated source file and also tested in a private copied RIFT tree.

Portable tests cover analytic recovery, retry AD versus finite differences,
already accepted constant value/derivative, endpoint-certificate refusal,
exhaustion and the fine-grid ceiling. The private fixture checks the recorded
failure against the independent dense reference. Six portable tests are added
to the explicit JAX CI manifest (collection floor 1044->1050); the one private
scientific fixture is explicitly deselected there, rather than silently skipped.

The initial old-code run failed the new recovery, retry-gradient and real-fixture
regressions, as expected. It also exposed an unrelated overly exact constant
fixture: tapered support differs from a global constant by 5e-7. That fixture was
changed to exact constant support, without relaxing tolerances. All failed runs
and an old-control 30s timeout remain charged. An audit serialization repair and
a CLI spelling repair consumed zero physical rows and are retained in the ledger.

Two physical control points from earlier parity checks have **exactly zero old/new
value and all-six-gradient deltas**. The recovered point has finite patched AD;
its maximum absolute discrepancy from patched central finite differences is
2.5253e-5, passing atol 1e-5+rtol 1e-5. Its NoLoop central derivatives differ by up
to 2.46080 (declination), consistent with the different coarse numerical rule.
Therefore the earlier successful pointwise gradient gates remain supported and
unchanged; their extrapolation to this broader region does not. Full neural
objective gates and training were not rerun, and no checkpoint was modified.

## Decision and resources

All four ESS~1 pilot outcomes remain inconclusive. The cache arm's partial
population remains failed; no row was dropped, replaced or used to complete an
estimate. The retry removes an avoidable refusal, but the point still fails the
previous 1e-3-nat and derivative **cross-backend** comparison. Before another
inference campaign, agree and regress a common quadrature accuracy/rule contract
for already-resolved coarse rows, using independent dense references. Do not
silently substitute the patched answer into stage4 or label either approximation
as exact truth. No powered held-out-anchor study or performance claim is justified.

Stage5 measured **358.50 CPU seconds** across all commands, including failures,
under the 600-second cap; **397.82 summed command wall seconds**, peak
**2,212,180 KiB RSS** (2.11 GiB), below the 8-GiB cap. Final isolated regression:
**7 passed**.

Stage5 used 38 new physical attempts: 9 extraction/check rows, 4 old/new control
value-gradient rows, 1 patched failure value-gradient row, 12 patched FD rows and
12 NoLoop FD rows. The patched FD calls also compute analytic gradients, so
17 physical value-and-gradient dispatches are included, not only the five
explicit gradient checks. All numerical primitive replays and synthetic tests used zero
new physical rows. One CPU affinity [0], no GPU, no batch job submitted. Complete
command CPU/wall and RSS, including failed attempts and setup, are recorded in
results/stage5/summary.json and supporting.json. Coordinator source edits, transfers
and administrative elapsed time are not fully measured. Prior stage and historical
teacher acquisition costs remain additional, not reset or amortized away.

Commands and outcomes for the final isolated regression and physical validation
are in those records. No full 1044-test JAX suite, GPU gate, production shape gate
or new CI run is claimed. Local macOS roster census could not run its existing
q-window list hook because system Bash lacks `mapfile`; no unrelated shell/runtime
change was made. The JAX gate script passes shell syntax checking.
