# Transport training oracle: bounded prototype, negative adoption result

This draft preserves useful evaluator accounting. It does **not** recommend enabling
the new oracle in ILE. Five synthetic seeds show worse mode recovery and lower ESS
per total likelihood cost than no oracle. No production defaults or ILE wiring change.

## Source and scope

- RIFT base / assessed source: `1621bb6e12391086edc3bfc8730ad807c8ad638c`,
  current `rift_O4d` verified before implementation on 2026-10-15.
- [WTF paper v2](https://arxiv.org/abs/2609.27033v2), revised 2026-10-12.
- [Authors' implementation](https://github.com/wasserstein-tilted-flow-maps/WTF/tree/058a4f2d9a42f384a0be8d1224784b755986e4ff)
  pinned at `058a4f2d9a42f384a0be8d1224784b755986e4ff`.
- infra-atlas policy/runtime/routes: `bb8608c695028f7eeeff53e5c4b5eef6537b2a39`.
- Exact Caltech-tested source hashes: `transport_results/source_sha256.txt`.

WTF optimizes drift-induced transport-regularized reward. Setting reward to log
likelihood does **not** make its outputs posterior draws. Simulation-free refers
to avoiding neural rollouts; it does not remove physical likelihood calls. Faithful
WTF needs a differentiable reward and a pretrained **two-time flow map**; a RIFT
normalizing flow is not automatically such a map.

Here `TransportOracle` fits a small Gaussian-kernel surrogate to already scored
history and moves anchors by projected ascent of
`r_hat(y) - ||y-z||^2/(2*transport_scale)` in unit-box coordinates. It uses 96 fit
points, 32 anchors and five ascent steps per update. This is a **simplified
transport-inspired algorithm**, with no pretrained drift, flow map, neural training,
or WTF implementation imported. Gradient rows are surrogate work, separately counted.
Periodic coordinates use shortest displacement and wrap at the seam; other dimensions
clip to the box. The oracle returns no density and is training-only.

Source-reported implementation caveats, not reproduced here: the authors' released
text-to-image checkpoint/report uses `cost_at_tau=false`, while the shipped recipe
uses true (see their `REPRODUCE.md` and config). Their Euclidean reward gradients,
rescaling/smoothing, detached base velocity, and mean-square control norm need a
separate mathematical audit for a faithful reproduction. Ordinary plus two perturbed
reward backwards can require three reward calls; zero drift needs
`no_w_rew_norm:true`. No image stack, checkpoint, or authors' GPU result was run here.

## Accounting and contracts

`EvaluationLedger.wrap` belongs at **every actual evaluator boundary**, including
callbacks retained by optimizers. Rows are attempted inputs, counted before dispatch;
exceptions/retries count, and failures/calls/host elapsed time remain visible. Scalar
optimizer requests count one row; arrays use an explicit columns or matrix layout.
For grouped coordinates, wrap a flattened evaluator boundary. Unwrapped callbacks
cannot be counted automatically. Timings do not synchronize asynchronous GPU work.

`benchmark_integrators.run` now starts wall timing before seed/setup, gives its warm
hook a shallow target view with the same counted likelihood, reports `n_sampler`
separately, and uses actual true rows for `n_eval` and efficiency. No reuse is assumed
(`amortized_reuses=1`). Portfolio phases distinguish sampling, oracle scoring,
oracle training (including optimizer/gradient callbacks), and member training.
Phase wall timers include surrogate fitting and proposal adaptation, not just evaluator
execution. Training time is a subset of integration/warm time, not additive to them.

Oracles keep their separate proposal-training copy. Raw oracle `lnL` weights remain
**heuristic training scores**, not posterior importance weights. Final estimator weights
remain `lnL + lnprior - lnq_mix`. An implicit map supplies no normalized `q` and must
not be installed as a density member. Existing NFlow box-truncation normalization is
untouched and is not claimed exact.

`setup(oracle_args=[...])` supplies independent per-oracle options, validated by count;
parameter order is set before setup and options are saved/replayed on reset. Passing
`oracle_realizations=[]` explicitly clears the list. Supply new oracles at construction
before `add_parameter`; this prototype does not introduce dynamic parameter registration
or auto-wire ILE's deliberately disabled oracle path.

## Experiment and measured outcome

Two-dimensional box `[-5,5]^2`; normalized independent truncated-Gaussian components
centered at `(-2,-1)` and `(2,1)`, widths `(0.45,0.6)`, masses `(0.8,0.2)`.
Nonuniform normalized prior factors are `(1+0.12*x)/10` and `(1-0.1*y)/10`.
Likelihood is `3 * normalized_target / prior`, so exact evidence is 3. Truth for
moments, positive-x mass and `|x|>3` tails comes from truncated-normal formulas and
is never supplied to either learned oracle.

All arms use the same corrected portfolio, 25% frozen normalized uniform box and
75% adaptive two-component GMM, fixed allocation, 1,900 final estimator draws.
The total budget is exactly 2,000 true rows per arm:

- None: 100 initial uniform training rows + 1,900 final draws.
- Cheap existing `PuffballOracle`: 40 initial training + 60 oracle scores + 1,900 draws.
- Transport: same 40 + 60 + 1,900, plus 960 surrogate gradient rows.

Initial rows train the GMM and never enter evidence. Six online oracle rounds each
score ten candidates; subsequent final draws use the learned proposal. This compares
ways to spend a fixed 100-row training budget, not an identical-history ablation.
Seeds are `101,202,303,404,505`; all backends are seeded, and the diagnostic refuses
a GPU backend. Every arm runs in a fresh process, with no amortized model reuse.

Means across five seeds (errors are mean **absolute** errors):

| Arm | lnZ error | positive-x mass error | tail mass error | Kish ESS / 2,000 true rows | process wall s |
|---|---:|---:|---:|---:|---:|
| None | 0.03309 | 0.01140 | 0.00187 | 0.30295 | 2.569 |
| Cheap puffball | 0.06735 | 0.01584 | 0.00204 | 0.09112 | 2.464 |
| Transport | 0.02237 | 0.02996 | 0.00104 | 0.21886 | 2.421 |

| Arm | mean error x,y | second-moment error x,y | setup s | warm s | training s | integration s |
|---|---|---|---:|---:|---:|---:|
| None | .04304, .02742 | .05456, .02582 | 1.997 | .00394 | .04487 | .06426 |
| Cheap | .09031, .04722 | .11184, .03931 | 1.908 | .00213 | .04578 | .06960 |
| Transport | .12405, .07412 | .04152, .05436 | 1.857 | .00213 | .04382 | .06634 |

Process time includes imports; setup measures sampler construction/setup after the
initial common module imports. Small wall differences are not a speedup claim.
Transport improves evidence/tail error here but worsens mode mass and means, and
loses about 28% ESS per true row against none. Five seeds and one toy target cannot
establish a general ranking. Preserve accounting; do not enable production use.

## Execution, validation and total development costs

Verified route: mini -> atlas laptop Tailscale endpoint (existing host-key identity)
-> `ldas-grid.ligo.caltech.edu`, BatchMode, no credential/security changes. Laptop
local DNS failed, but SSH worked through the atlas endpoint. Its desktop executor
status was not used as SSH evidence. No alternative compute site was used.

Runtime: `/cvmfs/software.igwn.org/conda/envs/igwn/bin/python`, Python 3.11.14,
NumPy 2.2.6, SciPy 1.13.1, pytest 9.1.1, source-first `PYTHONPATH`. CuPy is
installed but the sampler device probe selected numpy on ldas-grid. All thread
limits below were one. Isolated scratch:
`/scratch/richard.oshaughnessy/codex_transport_20261015/source`.
Small interactive CPU runs on the atlas general-use host; **no HTCondor job IDs**,
GPU allocation, scheduler/accounting change, shared-env install, or physical likelihood
campaign. Initial estimate was <10 CPU minutes including validation.

- First failed pilot: 200 true rows, 1.61 s wall / 1.20 CPU s; harness passed explicit
  list portfolio weights into a preexisting `.astype` path. Corrected harness state.
- Successful one-seed pilot: 6,000 true rows; 2.55 s wall / 1.97 CPU s.
- Initial shared-process five-seed study: 30,000 rows; 6.10 s wall / 4.05 CPU s.
  Scientific values valid; per-arm startup timing was order-biased.
- Final fresh-process study: 30,000 rows; **46.54 s wall / 37.15 CPU s** including
  subprocesses; max RSS 393,332 KiB. Same scientific values. Full records in
  `transport_results/cold_five_seeds.json` and `.time`.
- Focused initial six tests: passed (2.43 s pytest time).
- Expanded validation first attempt: 66 passed, one new mock-fixture failure;
  8.92 s wall / 8.04 CPU s. Fixed missing mock sample-record keys.
- Expanded validation: **67 passed**, 9.04 s wall / 8.04 CPU s.
- Metered validation replay: **67 passed**, 10.98 s wall / 9.02 CPU s; exact
  **3,674,658 attempted synthetic sampler-callback rows**, including **3,600,000**
  hidden member-training rows, 40 oracle-score rows and 18 optimizer rows.
  These existing tests use inexpensive analytic/constant likelihoods. See
  `validation_cost.json`; mocked counter tests and analytic truth diagnostics are
  distinct from inference callbacks.
- Historical validation attempts before the metered runner were not row-audited.
  Their wall costs are retained; do not interpret 3,740,858 (=66,200 experiment +
  final audited validation) as an exact total development row count. It is a lower
  bound excluding those earlier validations. Future runs can use the metered runner.
- Final focused regression after moving the generic benchmark timer before seeding:
  **8 passed** (2.41 s pytest time); see `focused_final.log`. The final file hashes identify this source adjustment.
- Cache-consumer audit: passed, all 159 registered post-rebind reads accounted for.
- Local CI roster failed on macOS's old Bash (`mapfile` unavailable); Linux result
  **passed** (349 files; 298 CI-reachable) in `roster.log`. This is a tooling portability observation, not a sampler failure.
- Independent read-only review completed; backend seeding issue corrected. No merge.
- **Not run:** production shape-recovery base/candidate merge gate, GPU lanes, real
  event likelihoods, faithful WTF reproduction. Shape gate remains mandatory before
  production merge; this is a draft prototype, not merge-ready evidence.

Commands (from the checkout root on Caltech):

```sh
export PYTHONPATH="$PWD/MonteCarloMarginalizeCode/Code"
export OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
export CUDA_VISIBLE_DEVICES='' GW_SURROGATE=''
P=/cvmfs/software.igwn.org/conda/envs/igwn/bin/python
$P MonteCarloMarginalizeCode/Code/test/integrators/transport_oracle_benchmark.py --json results.json
$P MonteCarloMarginalizeCode/Code/test/integrators/run_transport_validation.py --json validation_cost.json
$P MonteCarloMarginalizeCode/Code/test/expensive_before_merging/integrators/audit_rvs_fairdraw.py --check
```

A zero-drift analytic diagnostic is also tested: base N(0,1), quadratic reward
`-(y-a)^2/(2*s^2)` gives transport mean `lambda*a/(s^2+lambda)` and variance
`(1+lambda/s^2)^(-2)`, versus Bayesian variance `(1+lambda/s^2)^(-1)`.
For `lambda/s^2 >= 1`, direct importance correction without defensive tails has
infinite second moment. This is an analytic check, not an executed physical experiment.

Next decision: review the accounting API and retain it if useful. Any new oracle
study should predeclare a stronger target suite and matched-budget training protocol;
the present result supplies no reason for a larger campaign or sampler rewrite.
