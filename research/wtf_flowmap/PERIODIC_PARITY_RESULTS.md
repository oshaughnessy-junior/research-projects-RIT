# Repaired periodic-model physical parity: limited probe passed

The exact repaired checkpoint `a63d634b...3ef4ea` passed the newly executed
periodic-coordinate parity gate. Old local-chart checks were not substituted.
**123 endpoint attempts /115 physical-likelihood rows** were charged, including
eight deliberate support rejections. One process, no repair/retry and no
unexpected failed row. No optimizer, reward training, evidence integration or
inference ran. The narrow prior solver pass did not change any tolerance.

## Exact scope and outcomes

| Check | Scope | Result |
|---|---|---|
| full retained-mode source/bank | all21 modes2<=l<=4, H1/L1 | passed |
| decoder | two fixed rows, Torch/JAX/full finite-difference Jacobian | passed |
| cached physical values | rows0/100/200/300, NumPy NoLoop vs JAX | passed |
| held-out bridge | sampler seed04, all six periodic-state derivatives | passed |
| seam/representative tests | both sides of RA/phase/psi zero seams, full-period AD shifts | passed |
| generated values | all16 fixed base draws mapped by repaired network, both backends | passed |
| full map pullback | base noise -> repaired0->1 jump -> decoder -> physical reward | passed |
| physical guard fixtures | dmin/dmax, dec poles, incl poles, NaN/Inf | eight expected rejections |

The maximum observed absolute value difference was **3.0257145e-5 nat**, below
1e-3. Gradient tolerances remained `.01+.001*abs(reference gradient)`, with central
differences at h1e-4 and5e-5 in the new state/base-noise units. Maximum component
errors divided by those tolerances (<=1 passes):

- held-out Torch/JAX bridge vs FD: .0482984; FD step refinement: .144902;
- physical angular representative gradients: <=1.32823e-7;
- repaired-map complete pullback vs **NumPy NoLoop** FD: .000647055;
  its step refinement: .000817616.

The full pullback also agreed with the explicit product of the map Jacobian,
decoder Jacobian and physical JAX gradient. This checks a **single** repaired
0->1 jump and the new decoder, not the future two-lookahead WTF value/control
estimator or a training loss. Cached seed04 remains held out from model training;
it was previously examined in old-coordinate tests, so it is not described as an
unseen scientific validation dataset. Generated base seed261030 was fixed before
any likelihood evaluation; no points were filtered or redrawn.

## Target and normalization contract

The physical parameter order is `(RA,dec,psi,incl,phase,D_Mpc)`. The periodic
state is `(RA,phase,2*psi,z_dec,z_incl,z_logD)`, using training-only nonangular
mean/std. The decoder does not wrap angles; the periodic model defines equivalent
lifts mathematically. The psi half-angle and negative inclination derivative
were checked explicitly. Strict distance (1,10000) Mpc/pole/finite guards remain.

Both backends use the same full-mode SEOBNRv5PHM GWSignal bank,4096 Hz, sinc
interpolation and bandlimited time quadrature, no phase/distance/calibration
marginalization, and `ignore_threshold=None`. The maintained NumPy endpoint is
`DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop`. It is called directly, not
through a CLI fallback. Source hashes were checked before constructing the bank.

Identical shared time grid:614 points, dt1/4096, closed interval
[-.074951171875,.07470703125], SHA256
`58069d49c1da7eab8cddf84193abff20e5a1ba5dda5af42c7b2ff04c08684d63`.
Integration half-window setting remains .075 s. Stored half-window .37890625 s
provides initial/certified guards512/1024; it does not enlarge the integrated
time domain. The compared values are the existing **raw log time-integral API**
on this grid; no new time-prior constant was silently added or removed.

Current driver source was audited for normalized angular sampling-range priors
and named Planck15 distance cosmology. In physical coordinates, angular prior
mass is1 on one fundamental RA/phase/psi domain, not8. The physical reward callback
adds neither prior nor coordinate-Jacobian factors. Prior/integration/evidence
normalization was not numerically estimated in this stage. Historical rotated
angular mass8 and its ln(8) offset remain recorded for future matched baselines.
Reward=log likelihood does not make map outputs posterior samples.

## Accounting and resource use

Every endpoint attempt is journaled/flushed before its guard. Every actual
physical row is separately counted/flushed before dispatch; failure costs would
remain. Eight invalid fixtures consume attempts but zero physical rows. Raw
pre-dispatch context is saved privately before each evaluator call, and the fixed
generated/cached validation point payload is also preserved. No reconstruction
would be needed to inspect this stage's failed endpoint. The earlier stage3b
unsaved original endpoint and stage3c reconstruction remain distinct history.

Physical calls:50 NumPy NoLoop rows,26 JAX value-only rows,39 JAX value+AD rows
=115. An AD call is one forward physical row with backward work counted in
time and gradient-row metadata; it is not counted as zero-cost differentiation.
Two Torch bridge backward rows were also recorded. No true optimizer calls or
hidden training scoring are present. The eight fixtures bring attempts to123.

Complete-process GNU time: **35.83 CPU seconds** (33.24 user +2.59 system),
**43.78 wall seconds**, peak **2056840 KiB RSS** (~1.96 GiB). This includes imports,
input/source hashing, frame/PSD setup(.1624 wall s), full-mode precompute(2.4798 s),
JIT, all values/gradients/stencils, private dispatch capture, validation and shutdown.
Administrative SSH/transfer checks and local syntax-check overhead are not
separately timed. The smaller in-process snapshot excludes final shutdown.

One CPU affinity[0] on ldas-grid, no GPU or scheduler job ID; hard process CPU570 s
inside the authorized600-s stage, hard8-GiB address-space limit. The stage ledger
would charge previous run journals/timing files on repair; none existed. This is
a separate allocation from model training. Earlier model repair31.05 CPU seconds,
earlier physical stages and historical acquisition remain separate, not erased or
silently amortized. Both setup and validation costs are included here.

## Provenance

Research parent `419528e32638c0e4fb289deeaf6f3331e6910e2f`; RIFT base
`1621bb6e12391086edc3bfc8730ad807c8ad638c`; infra-atlas
`bb8608c695028f7eeeff53e5c4b5eef6537b2a39`. Established mini -> laptop -> Caltech
route, unchanged credentials/security/runtime. PyTorch2.8.0, numpy2.2.6,
JAX0.9.0, float64. No production library edits.

Checkpoint, verified unchanged after execution:
`/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage3d/repair1/accepted.pt`
SHA256 `a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea`.
Drift/map widths are read independently from checkpoint metadata (both64).

Executed parity source SHA256
`e28aad15f851b8c02f50a81997527c7a71e1eb6f0b5e3bd48e4ee1fe32dc16a6`;
full-mode bank helper
`f17c6e525580904040a14d75a1141a98de84fce527d89c0afbdcbf9e4a28ed8c`;
periodic model
`878d90069b3c9691a9b87f403660f2f341feff84cc9e481a110d7f55cfae90de`.
Maintained NoLoop source
`7b87a19e7133155cb1bb67a790a58ab7204f605d6d12cd78471edc34e044c974`;
JAX wrapper/core
`0db282efa8db272e404cc2d649485feaba3c0339998a36ddde293b7ac4166005` /
`0f499791e796558b7b2460027ef8459302925dcf792801883664ceaf56336ab8`.
Normalized current driver source
`d88a561e6f838f6066cf543fd93c58b838d96d022b7d8536fa95819e8252cf20`.

Data/frame/PSD/intrinsic hashes were rechecked against case_inventory.json.
The four XMLs were rechecked against data-provenance record SHA256
`e809627592936cd16e2e55fe9abece6ea476d25323d550f47b91162b097166a9`.
Original posterior NPZ/manifest pins remain
`4581e8b8584922946f5ee01756b2ffb9f336d2a66171014ac95641ff365beb2e` /
`4de8a247ffb55d2f007faa1ec82d65c92c165c4aa9f7ebcc66cbfa7f662cb060`.

Private stage3e/run1 artifacts under the same scratch prefix: fixed_points.pt
SHA256 `fd4a702a928411eea4920baf619f1c0439137fd16472c2553eb0698b678691b9`;
last_dispatch.pt `eaf5a6a8ef7604275bce0e6eafe539cb59d4ab812c9a038458dca3620362549d`;
log `14b85b30186fbf2b33ae467ec8d4b79bc8b5c10573faf1a76b87a105d592216e`.
Raw data/model weights stay on Caltech; Git contains aggregate journals only.

Exact command (completed outputs must not be overwritten):

```sh
root=/scratch/richard.oshaughnessy/codex_learned_wtf_20261015
stage="$root/stage3e"
export PYTHONPATH="$root/stage3a/source/MonteCarloMarginalizeCode/Code" PYTHONDONTWRITEBYTECODE=1
/usr/bin/time -v -o "$stage/run1.time" \
 /cvmfs/software.igwn.org/conda/envs/igwn/bin/python "$stage/source/parity_periodic.py" \
 --stage-root "$stage" --timing-file "$stage/run1.time" \
 --inventory "$root/stage3a/source/research/wtf_flowmap/results/case_inventory.json" \
 --checkpoint "$root/stage3d/repair1/accepted.pt" --training "$root/final_seed261015" \
 --output "$stage/run1" > "$stage/run1.log" 2>&1
```

## Limits and next bounded comparison — not launched

This passes **specified local value/first-derivative contracts**, not global
support, all sky/phase configurations, historical waveform-version fidelity,
all intrinsic anchors, higher derivatives, integrator prior normalization or
posterior/evidence calibration. The complete current-NoLoop target was shared
between backends; that is not an independent external waveform truth check.
Model distillation still has a narrow sampled margin, and the underlying drift
is trained on a dependent finite-ESS cache. No simulation-free cost win follows:
neural rollout avoidance does not eliminate physical likelihood work.

The next meaningful WTF comparison needs an actual **periodic-WTF-tuned** model,
which does not yet exist. First propose a separately bounded implementation/smoke:
one CPU, ten CPU minutes,8 GiB, at most256 endpoint attempts, <=8 updates of batch4,
after pure mathematical tests. Adapt the semigroup term to periodic chord error,
retain ordinary tangent control norm and all frozen-map/base **input** derivatives,
and predeclare the new metric's coefficient (not inherit a Bayesian meaning).
Numerically validate the two-lookahead/control value gradient; this stage's single
jump pullback does not certify it. Stop at the first unresolved failure. This is
implementation validation, not a reward-only scientific success criterion.

Then propose the previously bounded **one-anchor, one-seed five-arm comparison**:
fresh matched current AV and AV+GMM, identical-cache resampling/warm-start,
pretrained periodic map and genuinely tuned periodic map. Allocation proposal:
2000 attempted true rows per arm /10000 total, one CPU,20 CPU minutes,8 GiB,
including setup, fitting, rejected candidates, oracle/optimizer/gradient work and
validation. Cache/map points enter **proposal training only**, excluded from
evidence/main sample/posterior output. Cache/map arms share the same corrected
normalized defensive final sampler with broad support; implicit maps never enter
as density members. Do not assume approximate box-truncated flow density is exact.

Use identical seed/target/normalized-prior contracts and comparable cache access.
Report evidence agreement/error where a justified reference exists, mode masses,
moments/tails, ESS per **total** cost, and end-to-end wall time. Preserve the
historical >=15984560-row acquisition lower bound and unknown hidden cost;
report cold versus explicitly amortized reuse separately. A one-seed2000-row
arm may not converge on this sharp target: treat that as inconclusive, never as
a confident method win against an unconverged baseline. This is a coverage/cost
pilot, not a powered adoption study; estimate replication/held-out-anchor work
from actual corrected-sampler measurements, not these cached parity timings.
Neither this comparison nor reward training/inference is authorized or started
by this result. PR445 remains unchanged.
