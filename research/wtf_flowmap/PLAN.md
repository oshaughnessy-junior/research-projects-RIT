# Learned WTF flow-map investigation — staged plan

Status: stage 1 inventory in progress; no cost/capability win claimed. Owner: this
Codex task on the mini, low-effort blue-sky work. Production defaults remain fixed.
PR445 and its negative heuristic pilot are preserved as a separate historical result.

## Real use case and baseline

Use S250114ax extrinsic inference at the saved intrinsic anchor 47536, then test
reuse at held-out nearby intrinsic anchors in the same event. Start from the
accepted continuous cubic/Simpson fixed-anchor ensemble, not an incomplete full-run
posterior or an intrinsic-posterior proxy. Inventory and verify hashes, priors,
coordinates, likelihood flags, raw likelihood-call cost, and validation records.
The existing AV reference is a strong baseline; also compare recommended corrected
AV+GMM with adaptation/rescue options where applicable. Historical settings are not
a current-code matched baseline until replayed and checked.

The first learned object will model the saved sky/distance/inclination marginal.
This is a development substrate for an extrinsic proposal, not a new posterior
estimator. Missing nuisance coordinates must be handled explicitly in the later
training-only adapter; no implicit density or posterior claims are permitted.

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
