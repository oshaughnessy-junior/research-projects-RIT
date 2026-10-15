# Two-update Q8 retuning smoke: passed, implementation only

The authorized stage11 ran two genuine periodic-WTF regression updates under
verified roboto reflected-Q8/cubic and the complete physical reward/control
bridge. The student warm-started from the separately labeled old-recipe
checkpoint; the original pretrained reference map and drift remained frozen.
No old artifact was overwritten. No inference comparison was run.

## Predeclared method and validation

The pre-dispatch journal fixes Adam lr1e-5,betas(.9,.999),eps1e-8,zero weight decay,
new optimizer state, reward coefficient.001 and semigroup coefficient1. There is
no gradient clipping, reward renormalization/smoothing, extra perturbed reward,
coefficient search or checkpoint selection by reward. Training seed261061 gives
two batches of4; independent validation noise seed261062 gives two fixed states
before and after (t=.25,.35;tau=.625,.725). Global Torch seed261060 is recorded.

The original pretrained map supplies both target lookaheads and the semigroup
reference. The base's input derivative remains in the control-cost target even
though its parameters are frozen. The model metric remains unit-radius T3 and
standardized R3 tangent sum-square, with periodic chord semigroup loss. This is
a manifold-adapted genuine learned-model experiment, not a faithful reproduction
of the image implementation or a theorem about posterior transport.

The reward is full21-mode H1/L1 physical **raw log time integral** on
[-.074951171875,.07470703125], duration613/4096s. Normalizing its uniform time
prior adds1.8994012307831323; parameter prior and decoder Jacobian are not added
to the physical reward. The same normalized angular/isotropic and Planck15
distance-prior source contracts apply. Parameter order is RA,dec,psi,incl,phase,
D_Mpc; model state order is RA,phase,2psi,z_dec,z_incl,z_logD. Torch/JAX are
explicit float64. Q8 pregrid and four-tap cubic use the existing PR261/274 native
builder and JAX wiring. The reviewed research-only extra integral-certificate
doubling is loaded in an isolated module; shared runtime/defaults are unchanged.

Predeclared pass criteria were finite supported before/after values, finite
nonzero physical gradients, finite complete loss/parameter gradients, finite
changed student weights, unchanged frozen weights/files, existing periodic
velocity/jump/input-derivative/decoder contracts, and exact endpoint/update/WE
limits. **Reward increase and decreasing losses were not acceptance criteria.**
Existing stage9+10 full FD bridge evidence is reused, not redispatched here.

## Results

| Item | Result |
|---|---:|
| Completed updates / batch |2 /4|
| Physical endpoints / scalar reverse VJP seeds / declaredWE |12 /8 /20|
| Update losses, on different batches |2.362110; .551373|
| Student weight change from warm start, L2 |.00114159|
| Maximum sampled periodic/input-derivative contract error |1.055e-14|
| Fixed mean value before / after |1.99770743 /1.99794613|

All predeclared criteria passed. The mean value change+2.387e-4 is only a
finite two-point diagnostic; it is not a model-quality, posterior, evidence,
mode-recovery, tail or efficiency result. Losses use different random batches,
so their numerical decrease is not a convergence claim. This smoke did not
repeat quantitative teacher-solver RMS gates or examine the other31 historical
training endpoints. The two earlier old/new raw reward shifts remain
**+.2418003 and +.4986873nat**, alongside selected gradient agreement; they are
not evidence of posterior improvement and are not erased by these two updates.

## Separate candidate and exact ledger

Caltech candidate:

`/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage11/run1/q8_smoke_2updates.pt`

SHA256:`71dacb2fb9846a94962799ba8bdd654176e429c89d5c05646e0d645a66e2839a`.
It includes the exact numerical contract, source/input/checkpoint lineage,
reference and base weights, optimizer/generator state, predeclared journal,
validation scope and cost-ledger snapshot. The adjacent `summary.json`,
`retune.time` and `manifest.json` form the complete ledger, including final
serialization/interpreter teardown absent from an in-checkpoint snapshot.
The manifest hashes the candidate, both explicitly unvalidated partial snapshots,
ledger and every staged source. Raw checkpoints stay on Caltech; audited source,
ledger and manifest are committed under `results/stage11/`.

Source pins remain RIFT base1621bb6e12391086edc3bfc8730ad807c8ad638c;
roboto paperd668ff9c79930783d18725f74e8fa2cf96bb852e;
infra-atlasbb8608c695028f7eeeff53e5c4b5eef6537b2a39;
WTF authors inspected-only058a4f2d9a42f384a0be8d1224784b755986e4ff and
arXiv2609.27033v2. The isolated retry core hash is
70fb605f65a3a6cda71e1819b79bcd232f6b6719b0842076cae373bb5e4f1097.
Original pretrained SHAa63d634b… and old tuned SHA71be3106… are unchanged.

Measured whole-command cost: **38.16 CPU seconds**, **42.21 wall seconds**,
one CPU, peak **2372680KiB (2.26GiB)**, **12 true physical endpoints+8 scalar
reverse seeds=20WE**. This includes setup/imports, source/input checks,
waveform/bank/pregrid, JIT, both updates and optimizer work, all validation,
partial/final checkpoint writes and shutdown. No failed scientific attempt
occurred. Source/report/manifest/transfer work is not fully timed;35CPU seconds
was conservatively reserved, not measured (measured+reserve73.16CPU seconds
under300). Bounded interactive work used mini→COS-MATH2519→ldas-grid; no GPU or
scheduler job ID, install, credential change or production/default/PR445 change.

Identified cumulative receipts are now **1607.08 CPU seconds**,
**1784.03 summed command wall seconds**, **10064 actual physical endpoints**
(10087 attempted rows under earlier guard-inclusive accounting),42 earlier
forward JVP lanes and14 scalar reverse VJP seeds. These lane types and older
WE conventions remain separate. Pre-existing teacher acquisition is still
>=15,984,560 sampler rows, with hidden evaluations/CPU unknown; complete cold
cost and project elapsed time remain unknown. No total-cost speedup claim.

## Next scientific comparison, proposed only

**Superseded:** the [existing-artifact scientific design decision](SCIENTIFIC_DESIGN_DECISION.md) withdraws this six-arm repeat in favor of a smaller, falsifiable three-arm information-retention experiment. The original proposal below is preserved as history; it is not the current recommendation.

Do a direct, bounded **one-seed, one-anchor six-arm comparison**, with all models
frozen and the same Q8/cubic target. Reuse current driver/proposal machinery;
no sampler rewrite or further infrastructure campaign is proposed.

| Arm | Training-only initializer |
|---|---|
| Current native AV+GMM |Fresh/selfish initialization, no oracle|
| Same-cache ResamplingOracle |Unweighted native resampling from the exact300-row training cache|
| Same-cache FisherGaussianOracle |Gaussian moments/covariance fitted from that same cache, not supplied true moments|
| Pretrained periodic map |Original accepted map, no reward updates|
| Old-recipe map |Explicitly labeled eight-update old artifact|
| New Q8 map |Separate two-update candidate above|

Same-cache means the same first-three-source,300-row finite-ESS teacher export
used for pretraining; the held-out fourth source is never an initializer. Cache
rows are old-recipe proposal-training inputs, not asserted Q8 posterior draws.
Each oracle's candidate cloud is scored under the current true Q8 likelihood;
resampling and fitted-Gaussian baselines have the same data access as the models.
Do not inject target moments or invent Fisher truth. Use existing stable
parameter ordering/reset contracts. Any rejected or optimized likelihood work
must be counted, with no free optimizer calls.

Give every arm the identical corrected final AV+GMM sampler and20% broad uniform
component, density normalization checks, fixed adaptation schedule and common
random seeds; reserve1000 final draws before allocating at most1000 setup,
validation, oracle-scoring and adaptation calls. Mixture conditioning uses each
draw's actual normalized q, with logL+logprior−logq. Oracle points stay in the
training copy and never enter evidence/cache/posterior output. No implicit-map
logq is assigned. The native no-oracle AV+GMM and same-cache/fitted-Gaussian arms
are the substantive current comparators; pretrained and old maps isolate what
pretraining and the new two updates contribute.

Proposed incremental cap: **2000 new physical calls per arm,12000 total**,
**1800CPU seconds total**, one CPU,8GiB, including setup/failures/analysis.
Stop at a cap or unresolved target/density failure; no repeated-arm rescue or
silent seed expansion. This is a modest feasibility comparison, not a powered
study. Both physical calls and derivative-equivalent work remain separately
reported (no new training/AD is planned). Old/failed pilot estimates remain
inconclusive; do not splice their populations into this one.

Report lnZ agreement, ESS/max weight and ESS per measured CPU/wall cost, mode
proxies, circular moments and tails with convergence/uncertainty diagnostics.
The fourth-source teacher sample is a finite-ESS old-target reference, **not
analytic Q8 truth**; discrepancies against it are labeled proxies, not evidence
or posterior errors. Real evidence accuracy is unestablished without an
independent matched-target reference. An ESS~1 result remains a failed shape/
precision demonstration even if reward or a single mode proxy improves.

Show both measured incremental deployment costs and R=1/R=10 amortized model/
cache reuse accounting. Charge this38.16CPU-second/20WE retuning preparation to
the Q8 arm rather than calling it free. Share common setup once in actual-total
accounting and state each arm's allocation; do not double-count reused work.
List the inherited cache acquisition>=15.98M-call lower bound, measured model
preparation, numerical investigation costs and missing cold costs separately.
Thus the incremental call caps are matched, but **full cold-cost equality or a
net speedup cannot be established**. This proposal is not launched automatically;
no inference/performance conclusion follows from the smoke.
