# Learned WTF flow-map investigation — staged plan

Status: periodic model-only long-jump repair passed unchanged selection and
reserved-final gates, one-step RMS .144216. Frozen teacher unchanged; original
300-CPU-second allocation used31.05 seconds including all failed work. New-periodic-coordinate
physical parity, actual reward adaptation and inference are not launched. See
DISTILL_REPAIR_RESULTS.md. No scientific capability/cost win claimed.
Production defaults remain fixed.
PR445 and its negative heuristic pilot are preserved as a separate historical result.

## Real use case and baseline

Use S250114ax extrinsic inference at the saved intrinsic anchor 47536, then test
reuse at held-out nearby intrinsic anchors in the same event. Start from the
accepted continuous sinc/bandlimited fixed-anchor ensemble, not an incomplete full-run
posterior or an intrinsic-posterior proxy. Inventory and verify hashes, priors,
coordinates, likelihood flags, raw likelihood-call cost, and validation records.
The existing AV reference is a strong baseline; also compare recommended corrected
AV+GMM with adaptation/rescue options where applicable. Historical settings are not
a current-code matched baseline until replayed and checked.

The learned object will model the full six-dimensional time-marginalized extrinsic
posterior (RA, declination, inclination, distance, coalescence phase, polarization),
using the accepted raw XML exports after archive-hash validation. The compact NPZ
is a cross-check for its four shared coordinates. No implicit density or posterior
estimation contract is supplied by the learned map.

## Stages and gates

1. **Inventory:** pin the accepted data/worker/source; identify a callable maintained
   NoLoop or verified JAX equivalent and a reproducible strong baseline. Stop before
   expensive replay if the target contract or data access is unresolved.
2. **Genuine model:** learn a neural drift by flow matching on real posterior
   training samples; numerically integrate that frozen drift and distill a neural
   two-time map `X(s,t,x)=x+(t-s)*v(s,t,x)`. Validate on held-out seeds/noise using
   trajectory error, semigroup error, diagonal agreement, and real marginal
   mode/tail diagnostics. Preserve checkpoint and exact input hashes. Development
   cap: one CPU thread, 15 CPU minutes, 2 GiB resident memory, no GPU/installation.
3. **WTF fine-tuning:** diagonal control regression onto a frozen reference map's
   exact autodifferentiated MC value gradient, plus off-diagonal semigroup
   distillation. Use sum-square control norm and gradients through frozen base
   drift inputs for the mathematical objective. Record deviations from released
   code (detached base derivative, mean-square norm, reward normalization,
   Euclidean pullback, smoothing) explicitly. Analytic diagnostic tests are gates,
   not evidence of real-case value. Connect the actual differentiable likelihood
   only after source/parity and coordinate checks. A surrogate reward, if used
   during development, must be labelled and independently validated against true
   likelihood; it cannot supply the scientific acceptance result.
4. **Real-case pilot:** preserve posterior/evidence via the existing training-only
   boundary and corrected broad-support final sampler. Do not add the implicit
   map as a density member. Compare drift-only map, WTF-tuned map, and strong
   nonlearned baseline with matched total resources/seeds and held-out anchors.
   Include raw cached posterior resampling / warm-start using the identical
   teacher cache, so the neural method must beat inexpensive existing reuse.
   Predeclare pilot request after measuring stage-2 cost; no large campaign starts
   automatically. Run small physical-likelihood/parity probes first.
5. **Decision:** continue only if evidence/mode/tail calibration is maintained and
   the model gives either >=20% lower measured total cost at a declared realistic
   reuse count, or reliably recovers a scientifically relevant missed mode at a
   fixed budget. Require replication across held-out anchors/seeds; no reward-only
   or teacher-fit-only success. If map approximation or target fidelity fails,
   repair that stage under a new bounded budget rather than interpreting it as a
   general failure of WTF. No indefinite hyperparameter search.

## Full cost and reuse contract

Count attempted true likelihood rows, optimizer/gradient calls, rejected/out-of-box
candidates, surrogate rows, neural forward/backward rows, ODE teacher generation,
density evaluation, setup/JIT, fitting, validation, failures, and CPU/GPU wall time.
Teacher posterior acquisition is not free: preserve historical `ntotal` as a lower
bound where hidden work was not recorded, and report unavailable costs explicitly.
Cold cost includes acquisition; conditional-on-existing-data cost is a separate
scenario. Report `C_total(R)=C_acquire+C_pretrain+C_distill+C_finetune+
R*(C_adapt+C_true_final+C_density+C_validation)` alongside baseline cost and observed
break-even R. A historical sample cache cannot justify an unconditional speedup.
Keep generated scientific data/checkpoints on Caltech; repository provenance holds
paths, hashes, configurations and aggregate diagnostics, not proprietary data.

## Persistent record

Research branch `codex/learned-flowmap-realcase`, based on current `rift_O4d`
`1621bb6e12391086edc3bfc8730ad807c8ad638c`, carrying PR445 as dependency commit
`e5c7b1fc`. WTF v2: arXiv:2609.27033v2; inspected authors' source
`058a4f2d9a42f384a0be8d1224784b755986e4ff`. Route and policy use infra-atlas
`bb8608c695028f7eeeff53e5c4b5eef6537b2a39`, mini → laptop → Caltech.
Every completed stage updates `STATE.json` and `results/` with commands, source/data
hashes, resources, results, blockers and the precise next stage. Before a larger
training/production campaign, report the concrete resource request to Richard.

## Stage-2 pilot fixed before training

Select the single accepted `sinc-bandlimited` arm: NPZ SHA256
`4581e8b8584922946f5ee01756b2ffb9f336d2a66171014ac95641ff365beb2e`,
400 fair draws, 100 per seed in the verified producer's concatenation order.
Train seeds 25011401–03 (300 rows), hold out seed 25011404 (100 rows). Retain
duplicates. These are finite-ESS conditional exports, not 400 independent truth
rows. Historical acquisition `ntotal` lower bound is 15,984,560; hidden evaluation
counts and total acquisition CPU cost are unavailable. No free-teacher speedup claim.

Features: RA, coalescence phase and polarization unwrapped around training
circular means, atanh(sin declination), atanh(cos inclination), log distance; standardized using training rows only.
The atanh/tanh pairs enforce physical angular bounds; RA is periodic on inversion.
Before training, inventory found all six physical extrinsic coordinates in the
accepted raw exports. Verify each archive SHA and exact agreement with the NPZ
for shared columns. Unwrap coalescence phase (2pi) and polarization (pi) about
training circular means. The exported conditional time is omitted because this
model targets the time-marginalized six-dimensional inference problem. These
local angular charts have seams; full periodic reward/coordinate validation is
a stage-3 gate, not assumed from this pretraining run.

Two hidden layers of width 64, 2,000 flow-matching and 2,000 distillation Adam steps,
batch 128, seed 261015. Cached RK4 teacher trajectories have 32 intervals. Hold-out
noise and a 64/128-step refinement diagnostic are distinct from training trajectories.
Model-only continuation gates: finite outputs, teacher refinement RMS <=.01,
one-step teacher RMS <=.15, semigroup RMS <=.1, diagonal RMS <=.2 (standardized
coordinates), and report all held-out real marginal/mode/tail errors. These gates
only check the learned solver; no evidence or inference-efficiency success follows.
No model-based tuning against the held-out seed is authorized in this fixed pilot.

## Single bounded stage-2 solver repair

The first 2,000/2,000-step real-data run produced a trained drift/map, but
one-step RMS .17617 exceeded .15. RK4 refinement RMS 4.3e-7, semigroup .03077,
and diagonal .15257 passed their gates. Preserve it as a failed solver-gate pilot.
A dtype-preserving frozen-parameter copy fix is separately regression-tested.
Allow one 2,000-flow/6,000-distillation-step pilot (same data, seed, architecture),
chosen for teacher-solver accuracy, not for real hold-out posterior scores.
Estimate <15 CPU seconds from the measured 4.15 CPU-second initial run; retain
the hard 900 CPU-second/2-GiB development cap. No physical likelihood or GPU work.
If it fails, report the model bottleneck and a concrete next-stage plan rather
than run an open-ended search or start physical fine-tuning anyway.
