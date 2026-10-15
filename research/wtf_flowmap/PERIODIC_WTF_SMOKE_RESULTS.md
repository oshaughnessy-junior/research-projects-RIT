# A genuinely real-reward-tuned periodic model now exists

**Passed implementation smoke only.** The new periodic objective passed its
mathematical tests and two real, complete two-lookahead/control value-gradient
checks **before optimizer creation**. Eight batch-four updates then used actual
physical log-likelihood derivatives, with the initial reference and base frozen.
The student changed (parameter L2 .002805143), all losses/gradients were finite,
and post-update support/value/shift/Jacobian checks passed. This is not a
calibrated posterior sampler, an efficiency result or a faithful Euclidean WTF
reproduction. No five-arm inference comparison ran.

## Objective and predeclared choices

Fixed before physical evaluation: lambda=.001, beta=1, Adam lr1e-5, float64,
eight updates of batch4. Initial repaired checkpoint a63d634b is the fixed
reference; the pretrained drift is also fixed. No reference refresh, clipping,
redrawing, reward normalization, gradient smoothing or coefficient selection.
Seeds: real derivative checks261040, update draws261041, fixed validation261042.

For t<=tau, middle=X_ref(t,tau,x), endpoint=X_ref(tau,1,middle), and

    V_hat(x,t) = lambda*logL(endpoint)
                 - (1-t)/2 * ||v_ref(tau,tau,middle)-b(middle,tau)||^2.

The gradient retains input derivatives through **both** lookahead jumps, the
reference diagonal velocity and the frozen base drift. Frozen parameters do
not imply a detached base input. The control norm is an ordinary **sum** of
tangent-coordinate squares; angular controls are never modulo-reduced.
Diagonal student control is regressed onto this detached frozen-policy gradient.

Semigroup distillation compares a student jump with two frozen-reference jumps.
Its angular error is sum `2*(1-cos(delta_theta))` and its nonangular error is sum
`delta_z^2`, then mean over the batch (not an unintended additional division by6).
The declared metric is flat in unit-radius `(RA,phase,2*psi)` and standardized
nonangles. Thus physical psi speed has factor4 in its squared metric. Base noise
is uniform on T3 times a standard Gaussian on the nonangles.

These choices are an explicit periodic/manifold adaptation of the paper's
Euclidean model. No theorem transfer is asserted. They also differ from the
released image recipe's mean-square norm, detached base input derivative,
Euclidean reward approximation, normalization and smoothing. No image stack
was imported and no released image result was reproduced. Lambda is a transport
objective coefficient, not a Bayesian posterior power. The reward has no added
prior or decoder-Jacobian terms; reward=logL does not yield posterior draws.

## Mathematical and real-gradient validation

19 manual Caltech tests passed: six new objective tests plus13 existing model/
geometry contracts. New tests cover chord consistency, nonwrapped2pi tangent
cost, a base-input-derivative counterexample, full two-lookahead finite
differences, value/gradient/loss lattice invariance, finite student gradients,
frozen parameters, an analytic zero-drift linear-coordinate diagnostic, shape
and time-domain guards. There was the existing benign Torch scalar-conversion
warning in a diagnostic comparison. No CI coverage is implied.

Two fixed on-policy states were checked before creating Adam, with full
`estimate_value` NumPy NoLoop central stencils at h1e-4 and5e-5. Tolerances
stayed value1e-3 nat and derivative .01+.001*abs(reference derivative). Errors
divided by tolerance must be <=1. Separate pure-neural control-cost FD checks
used atol1e-7/rtol1e-5, confirming the nonzero cost derivative was retained.

| Real check | Raw logL difference | V-gradient ratio | FD refinement ratio | Control-gradient norm | lambda*reward-pullback norm |
|---|---:|---:|---:|---:|---:|
| fixed point0 | 4.140489e-6 | 1.884867e-5 | 7.439717e-5 | .112545 | 34.782721 |
| fixed point1 | 1.678069e-5 | 1.077425e-5 | 6.960771e-6 | .152567 | .586626 |

All32 update endpoints had nonzero measured physical likelihood gradient norms.
Real scores therefore entered the regression targets; this is not merely a
self-distillation or synthetic-reward checkpoint. Eight finite updates completed,
and base/reference tensors were checked exactly unchanged after each one.
Trained shift/Jacobian error was1.110223e-14, below1e-10.

Four fixed validation states, excluded from updates, gave mean V .978629 before
and .979766 after, and mean logL1049.608 ->1051.200. These four-point summaries
were not used for checkpoint selection or a success threshold. They provide
**no scientific improvement claim**. The fixed eight-update result was retained.
Tuned diagonal-flow/ODE fidelity, global support, posterior modes/tails/evidence
and ESS per full cost have not been established by this smoke.

## Complete current-stage ledger

**100 endpoint attempts /92 physical rows /8 updates**, no unexpected failures or
repairs. Eight deliberately invalid physical fixtures were rejected before
dispatch and still charged. Preflight:52 physical rows (two AD rows, two NoLoop
center values,48 NoLoop stencil sides). Updates:32 AD rows. Before/after validation:
eight AD rows. Total:50 NoLoop and42 JAX value+gradient rows. AD is not free:
all backward/JIT cost is in process time, with34 Torch reward-backward rows logged.
The separate neural ledger includes every forward and64 student-output backward
rows, plus input-Jacobian validation. No hidden optimizer likelihood work exists.

| Process | Complete CPU s | Wall s | Peak RSS KiB |
|---|---:|---:|---:|
|19 mathematical/model tests |3.01|5.57|329404|
| real checks +8updates +post-validation |27.30|33.78|2106788|
| current-stage total |**30.31 /600**|**39.35**|**2106788 peak**|

One CPU affinity[0] on Caltech ldas-grid,8-GiB hard address-space cap, no GPU or
scheduler job ID. GNU time includes imports/shutdown, data/source hashing,
waveform/PSD setup, JIT, all validation/stencils, failures fixtures, optimization
and checkpoint persistence. Administrative SSH/transfer/hash checks and local
syntax-check process overhead were not separately metered. The in-process
snapshot is smaller because shutdown is outside it.

The program charges all existing stage timing/journal records before any retry;
the prior3.01 test CPU seconds were included, not reset. Total updates across
retries cannot exceed8. Any partial-updates retry requires an explicit resume,
not silently restarting the initial model. No retry occurred here. Pre-dispatch
endpoint/noise/time/RNG/model context and a partial optimizer checkpoint are
saved privately, so future failure diagnosis does not depend on reconstruction.

Known measured scientific-process groups, **not an exhaustive historical total**:

| Group | CPU s | Wall s | Physical rows |
|---|---:|---:|---:|
| original four model builds |24.16|27.64|0|
| old-chart parity, including stopped attempt |289.56|302.15|116|
| old-chart objective smoke/setup failure |38.50|44.06|55|
| forward-only phase diagnosis |.89|1.33|0|
| periodic model build +both repair candidates |31.05|35.64|0|
| periodic physical parity |35.83|43.78|115|
| this objective smoke/tests |30.31|39.35|92|
| listed measured subtotal |450.30|493.95|378|

Earlier extra mathematical checks are not all timed (a separate final check was
2.92 CPU/3.80 wall s); administrative and historical acquisition costs are also
incomplete. Do not turn this subtotal into an unconditional speedup. Historical
teacher acquisition remains >=15984560 target rows, with hidden costs unknown.
All original stage records and failures remain, including the unsaved stage3b
endpoint versus its explicitly labelled stage3c reconstruction.

## Model/source/data provenance

Reward-tuned periodic checkpoint:
`/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage3f/run1/periodic_wtf_tuned.pt`
SHA256 `71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1`.
It contains both model states, separate drift/map widths64, transforms,
optimizer/RNG state, coefficients, source hashes and `real_objective_smoke_passed`.
It is labelled implementation-validation-only. It does **not** claim that the
new policy is the old teacher or that original teacher-accuracy gates certify a
changed controlled flow. Loaders must distinguish this schema from the earlier
pretrained checkpoint's `gates_passed` flag.

Starting pretrained checkpoint remains unchanged:
`a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea`.
The reference and base are unchanged; only student map parameters were updated.
Older nonperiodic/failed checkpoints remain forensic artifacts.

Research parent `06a6c67ef68a76bafd070dacb0c51215c4c5aca0`; RIFT base
`1621bb6e12391086edc3bfc8730ad807c8ad638c`; infra-atlas
`bb8608c695028f7eeeff53e5c4b5eef6537b2a39`. Established mini -> laptop -> Caltech
SSH route, unchanged credentials/security/runtime. Float64 with the same pinned
full21-mode NoLoop/JAX bank and shared614-point time grid as stage3e. Current
normalized-prior source conventions remain; raw time-integral API values are
used consistently and no hidden prior/time-normalization constant is introduced.

Executed objective/smoke/context SHA256:

- periodic_wtf.py `440b52051b3b0213833aba1b607f11a775a43490607474b777aa4c31e3766a48`
- smoke_periodic_wtf.py `018c2b387e1c4f75eabdf872a895e5ba191afccb03339f9cdcf916273d7d42ca`
- periodic_real_context.py `037eff5c2430bc0f884d005705569e170dc1dbb43b637a51005c61dd046d0714`
- test_periodic_wtf.py `9100419ae10b3b0482b35128c035f021571685d2d13b09b133cc5aadb1d5ec7f`

Periodic model remains `878d9006...fae90de`, bank helper `f17c6e52...28ed8c`;
all full hashes and maintained likelihood source pins are in run1/result.json.
Case/frame/PSD/intrinsic/XML hashes were checked again; data provenance record
`e809627592936cd16e2e55fe9abece6ea476d25323d550f47b91162b097166a9`.
Original posterior NPZ/manifest SHA256 remain
`4581e8b8584922946f5ee01756b2ffb9f336d2a66171014ac95641ff365beb2e` /
`4de8a247ffb55d2f007faa1ec82d65c92c165c4aa9f7ebcc66cbfa7f662cb060`.
Raw model/data artifacts stay on Caltech. Private log SHA256
`8a714310e83f789a83c4595eddc3a2c83ef386665b73bc7ad7685762420fbfbe`;
last dispatch `a088ffd8afb8d758bdd0920eac0f7a83cc9a32bbcdc08fefc029b4a89fa7f424`.

Exact remote smoke command (completed directories must not be overwritten):

```sh
root=/scratch/richard.oshaughnessy/codex_learned_wtf_20261015
stage="$root/stage3f"
export PYTHONPATH="$root/stage3a/source/MonteCarloMarginalizeCode/Code" PYTHONDONTWRITEBYTECODE=1
/usr/bin/time -v -o "$stage/run1.time" \
 /cvmfs/software.igwn.org/conda/envs/igwn/bin/python "$stage/source/smoke_periodic_wtf.py" \
 --stage-root "$stage" --timing-file "$stage/run1.time" \
 --inventory "$root/stage3a/source/research/wtf_flowmap/results/case_inventory.json" \
 --checkpoint "$root/stage3d/repair1/accepted.pt" --training "$root/final_seed261015" \
 --output "$stage/run1" > "$stage/run1.log" 2>&1
```

19 manual tests ran with pytest under CPU20s/AS2GiB; their exact wrapper is in
tests.time. The smoke deducted that cost before its CPU560s cap inside the
600-s stage. Research files are not CI-covered. PR445 remains unchanged.

## Ready next comparison budget — NOT launched

Both required learned objects now exist: pretrained periodic map a63d634b and
genuinely reward-tuned periodic map71be3106. The next bounded request is an
instrumented, exploratory comparison at this one anchor and one fixed seed:

- current strong AV and AV+GMM;
- identical-cache resampling/warm-start;
- pretrained periodic-map training oracle;
- reward-tuned periodic-map training oracle.

Allocation proposal: **one Caltech CPU,20 CPU minutes,8 GiB,2000 attempted target
rows per arm /10000 total**, counting setup/fitting, failed/bounds candidates,
oracle scoring, hidden optimization/gradients and validation. Stop at any cap;
do not silently complete missing arms with extra budget. This is a coverage and
cost pilot, not a powered adoption study. It is not sized by cached parity-row
timings. Require fresh current normalized-prior/likelihood baselines and treat
any unconverged2000-row arm as inconclusive, not a model victory.

Before physical pilot work, validate parameter order, periodic conversion,
source/setup/reset behavior and the training-only/cache-isolation contract,
including a deliberately wrong/collapsed oracle. The cache/map arms must share
the same validated density-corrected defensive final sampler, with a broad
support component; implicit maps are never density members. Count out-of-support
proposal work and preserve the sampler's correct normalization/zero-prior handling.
Do not claim exact logq for empirical box-truncation normalization.

Evidence/shape/ESS metrics must come from corrected final samples, not oracle
points or reward averages. Report evidence agreement/error only with justified
reference uncertainty, mode-mass/moment/tail discrepancies and ESS per total
physical and end-to-end cost. Shared precompute/acquisition/model costs are not
free: label cold versus explicitly amortized reuse, retain historical mass8/
ln(8) and unknown acquisition costs. Estimate multi-seed/held-out-anchor work
from actual corrected-sampler measurements before any larger campaign or cost-win
claim. No merging, production/default change or five-arm comparison occurred here.
