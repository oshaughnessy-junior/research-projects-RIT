# Decision: withdraw the six-arm integration repeat

Update: the separately authorized [three-arm benchmark](RETENTION_BENCHMARK_RESULTS.md) fired both first-seed futility conditions and closed this candidate/adapter line. The pre-evaluation design below is preserved.

Do **not** spend12,000 calls repeating the old2,000-attempt arms. There is no
power calculation supporting that budget: each complete arm had only1,000 final
draws, the other calls went to preparation/adaptation, and every final population
collapsed. Six arms do not answer the immediate question. Use one bounded
three-arm scientific comparison of **information retention through generation
and proposal fitting**, with a negative stopping decision. No evidence estimate
or posterior win is available from an unconverged run.

This design used immutable existing samples/source only: .46CPU seconds,
1.52 command wall seconds,95,160KiB RSS; zero physical calls, training, proposal
draws or inference. Hash-checked inputs and calculations are in results/stage12/.

## What the existing evidence supports

Use the old-target cache training median minus10,2849.748nat, as a **diagnostic
score band**, not a posterior-mass definition or an acceptance/calibration gate:

| Old arm | Initial cloud median/max logL | Saved final-proposal maximum logL | Rows in diagnostic band |
|---|---:|---:|---:|
| Fresh AV+GMM |78.23 /1130.66|1862.63|0/1000|
| Same-cache warm start |2859.75 /2862.75|2570.22|0/700 saved|
| Pretrained map |2201.54 /2835.72|2803.02|0/1000|
| Old reward-tuned map |2195.24 /2836.67|2560.38|0/1000|

The cache arm had800 final attempts; the failed eighth batch was not originally
saved, so this table uses only700 saved draws and assigns no completed estimate.
All100 original cache training points were in the band; neither100-point map
cloud put any there. Thus the teacher cache does contain useful points under
the **same old callback** that scored the failed pilot. The map loses substantial
score fidelity before density fitting, and the common fitting/drawing stage loses
it even when supplied good cache points. This is more specific than “not enough
samples.” No lower bound on required integration N can be inferred from it.

**Sharp target and deficient proposal geometry are supported explanations.**
Within the two inclination branches, standardized-cache correlation eigenvalue
ratios min/max are .00571 and .00122. Cross-group correlations omitted by the
three independent2D GMM factors reach .439 and .499. The AV grid is axis-aligned;
the old driver used100-point clouds, isotropic allocation and one-neighbor
dilation, followed by adaptation from highly collapsed importance histories.
These facts support a thin correlated target and loss of geometry in the
particular adapter. They do not identify how much error comes from each choice,
and do not demonstrate a defect in all current native samplers.

**No evidence for a large scalar normalization bug.** Independent recorded GMM
group masses differ from1 by at most8.9e-16; AV masses are similarly accurate;
source draws use their normalized laws and a20% uniform floor. Reconstructed
logq ranges are only3.54–12.03nat, while the log-weight gaps between the top two
contributions are45.81,102.43 and13.50nat for AV+GMM/pretrained/tuned (AV:210.26).
The corresponding likelihood gaps are46.13,104.23,13.60 (AV:213.51). Hence collapse
is already present in target-score dominance, not created chiefly by the q
correction. Correct mass does **not** prove the entire density implementation
correct, nor does it imply useful coverage or calibration.

**A wholesale old target/cache mismatch is disfavored, not globally excluded.**
Data/archive, parameter and coordinate hashes were checked; cached points score
2854.30–2862.75 under the pilot callback. Training/held-out distances and coarse
inclination masses are compatible as finite-cache diagnostics. The acquisition
NPZ has no stored likelihood scores, so a pointwise historical-export likelihood
match cannot be reconstructed from it. The cache is finite-ESS, correlated and
old-recipe; it is not Q8 posterior truth.

**The numerical contract has a measured local effect, not an explained global
rescue.** Old/new full-bridge raw shifts are+.2418/+.4987nat and pulled-back
gradient changes .043%/1.585%; Q8/Q16 values/gradients pass selected checks.
The old cache failure was a deliberate quadrature-certificate refusal with finite
physical primitives. Only1/32 historical training targets was fully rechecked;
there is no global bound on old→Q8 errors at the importance winners. Therefore
we cannot assign the old ESS collapse to that change or claim it irrelevant.
The hundreds-of-nats observed coverage deficits are much larger than the measured
local shifts, but that is a scale comparison, not a global error bound. Constant
time-prior/angular-normalization offsets cannot change normalized weights/ESS.

## One smaller, falsifiable scientific comparison

Ask: **does the frozen Q8 map retain teacher-supported geometry better than cheap
data reuse, and does the same native fitting stage preserve that information?**
This can fail decisively even when none of the proposals supports integration.
It cannot establish calibrated inference or a cost advantage from ESS alone.

Use three arms, all with identical current native AV+GMM density machinery and
20% uniform support, fixed fitting settings, no online retuning and no adaptation
after fitting during this diagnostic:

1. **Strong information-matched native control:** fit/warm-start native AV+GMM
   directly from all300 training-cache rows. This uses the best already available
   data rather than repeating the old100-uniform-point cold start. It is a
   source-supported native baseline, not claimed to be every production mode.
2. **Same-cache resampling:** native ResamplingOracle supplies100 rows drawn
   from that exact300-row cache, then the identical fitter. This tests whether
   thinning/resampling itself is the loss, and is the minimum cheap competitor.
3. **Frozen Q8 map:**100 generated rows, then the identical fitter. All models
   were trained on the same300-row source; the held-out fourth source is excluded
   from fitting. Every generated point is scored under the current Q8 target.

Run at most two fixed seeds261071/261072. Other models/pretrained-vs-old ablations
and a separate fitted-Gaussian arm are **not necessary** to test this utility
claim. Their attribution questions matter only if the current candidate first
survives a cheap data-reuse/native control. This replaces the six-arm proposal.

Measure before and after fitting: two inclination-branch occupancy, circular/
distance/tail discrepancies to the held-out empirical reference, high-score hit
fraction, and normalized fitted-q log predictive density on the100 held-out
coordinates. Define the score band in advance as median held-out Q8 logL minus10;
freeze that formula before scoring. Held-out q score measures empirical support
coverage, not posterior correctness. Track corrected-weight concentration only
as a diagnostic. High reward alone cannot satisfy this test.

Predeclare a practical minimum utility: in **both seeds** the map-derived fitted
proposal must improve mean held-out logq by at least1nat over the better native/
resampling control, without losing either inclination branch or worsening
high-score hit fraction by more than5 percentage points. Report paired fixed-set
differences and sampling uncertainty; teacher correlations prevent treating100
held-out rows as100 independent truths. The1nat floor is a minimum practical
effect, not a calibrated significance threshold or speedup conversion. Failure
or uncertainty means **no demonstrated utility**, not a smaller effect promoted
to success. Even meeting it justifies considering a separately powered,
independently calibrated inference comparison; it is not that comparison.

Economical stopping: after the first seed stop **all arms** if the map has0/100
source-band hits while resampling has>=90/100 and also has a>=1nat fitted-q
log-score deficit to the native control. Separately, if **all three** fitted
proposals have0/100 diagnostic-band hits, stop as a common compression bottleneck;
do not spend more integration rows or train another tiny update. Otherwise
finish the second seed, then stop at the predeclared result; no seed/budget search.
All rows remain proposal diagnostics/training-only and enter no evidence/cache/
posterior population. Thus this futility selection cannot be used to publish
selected-stopping evidence estimates. Later calibration thresholds remain
ESS>=100,max-weight<=.05,relative-SE<=.1 plus split/shape/reference checks;
passing ESS alone is never enough.

## Cost and terminal decision

Shared Q8 rescore300 training+100 held-out coordinates:400 calls, charged once.
Two seeds each score100 new map rows and100 draws from each of three fitted
proposals:800 more calls. Resampling source scores are exact lookups in that
already rescored300-row cache; log actual calls and logical reuse separately.
Total hard cap **1200 new physical attempts**, **300CPU seconds including setup,
fits, failures and analysis**, one CPU,8GiB; no physical gradients or map training.
This is1/10 the six-arm physical budget. The estimate uses the old measured
242.84CPU/9554-row scale plus a conservative .05CPU-second/new-row allowance,
40CPU seconds setup/JIT and30 for native fitting/analysis (~130CPU seconds),
with300 as a stop cap, **not a power estimate**. Q8 throughput is uncertain;
if the cap prevents completion, report a failed bounded comparison without rescue.

Carry38.16CPU seconds/20WE of candidate retuning as preparation, report R=1 and
R=10 reuse scenarios and retain the>=15.98M teacher-acquisition lower bound and
missing cold costs. If the map has no coverage advantage but costs more, its
proposed cost justification fails **in this setting**. A predictive-q gain cannot
be converted to an ESS/speedup or break-even claim without a valid integration.

If the map fails this utility test, stop this candidate/adapter line rather than
run more smoke tests. If all fitters fail, the scientific negative result is
that this representation/adapter cannot exploit even known good samples within
the stated budget; any different representation is a new research decision,
not an automatic repair campaign. No next run is authorized or launched here.
Cumulative identified cost is1607.54CPU seconds/1785.55 summed command wall
seconds; physical endpoints remain10064. Complete cold cost remains unknown.
