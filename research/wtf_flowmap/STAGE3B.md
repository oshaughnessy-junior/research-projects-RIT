# Real-objective smoke: bridge passed, angular-chart guard stopped training

This is implementation validation only. Six actual real-log-likelihood WTF
objective updates completed, but the requested eight-update smoke test **did
not pass**: the seventh batch violated the strict local angular-chart guard.
No clipping, redrawing, reward change or larger campaign followed. There is no
posterior/evidence result, scientific improvement or performance claim.

## Configuration fixed before evaluation

Caltech only, one CPU, 600 total CPU seconds, 8 GiB, 256 endpoint/physical
attempts including fixtures, failures, validation and repairs. Each process has
570 CPU seconds and 8 GiB virtual memory as hard limits; the first failed setup
used only 5.36 CPU seconds, so the retry still fit the original total cap.
CPU affinity [0], no GPU, scheduler submission or job ID, install or runtime edit.

Seed 261016; float64; lambda=.001; Adam learning rate 1e-5; beta=1; at most
eight updates of batch four. The base drift and initial pretrained two-time
reference are frozen throughout, with exact weight equality asserted after
every completed update. The reference is never refreshed. The reward is physical
log L, without prior or chart-Jacobian terms. Lambda is a transport objective
coefficient, not a Bayesian posterior temperature. No normalized log q is given
by these maps; generated points may only become proposal-training candidates.

Support is strict distance (1,10000) Mpc, finite interior angular coordinates,
local centered RA/phase/psi charts inside their half-periods by 1e-6, and no
canonical angle seam within 1e-6. These are diagnostic stop guards, not silent
truncation of the training distribution. The predeclared guard is retained.

## Passed and stopped checks

- Full six-by-six Torch chart Jacobian agreed with JAX AD and independent
  central finite differences, including parameter permutation and negative
  inclination derivative.
- Torch/JAX reward value agreed with NoLoop to 2.012643e-5 nat (<1e-3).
- Standardized bridge gradient AD/FD error divided by tolerance was .001759;
  h/h2 refinement ratio .005293 (both <=1). Tolerance was .01+.001*abs(gradient),
  h=1e-4 and 5e-5. Direct JAX/bridge derivatives also agreed.
- Three deliberately invalid distance/chart/seam fixtures were rejected before
  evaluator dispatch. Their attempts are counted, despite zero physical calls.
- Six finite-loss/finite-gradient optimizer updates completed, with unchanged
  base/reference weights. Per-update losses are in the aggregate journal; they
  use different random batches and do not demonstrate an improvement trend.
- The seventh batch of four endpoints failed the local angular-chart guard
  before any of its four physical calls. The error does not identify which of
  RA/phase/psi failed or how far it was outside the permitted chart. This remains
  a representation/domain blocker. Because the reference stays frozen, this
  endpoint failure is not evidence that the six student updates caused it.
- Post-update validation and the final accepted checkpoint were **not run**.
  Only the explicitly labelled six-update partial checkpoint is preserved.
  Torch/JAX higher derivatives are not supported; the current frozen-policy
  regression target requires first derivatives and is detached before updates.

## Measured cost, including failures and setup

| Attempt | Outcome | Endpoint attempts | Physical rows | CPU s | Wall s | Peak RSS KiB |
|---|---|---:|---:|---:|---:|---:|
| run1 | missing builder import, repaired | 0 | 0 | 5.36 | 7.50 | 858672 |
| run2 | bridge pass; stopped at update seven | 62 | 55 | 33.14 | 36.56 | 2142916 |
| total | smoke incomplete | 62 | 55 | 38.50 | 44.06 | 2142916 peak |

The 55 physical rows comprise 27 bridge checks (including 24 finite-difference
sides), four pre-update validation rows and 24 rows across six training updates.
There were 54 JAX gradient rows and 25 Torch reward-backward rows; the other
physical call was NoLoop value parity. The remaining seven attempts comprise
three deliberate rejection fixtures and the four rejected training endpoints.
No failed point enters inference, evidence, a posterior cache or sample output.

GNU time includes imports/shutdown, frames/PSD setup, waveform precompute, JIT,
validation, optimizer work and failures. Administrative source audits/transfers
were not timed. The last neural snapshot logs 268 forward rows and 24 student
backward rows through update six. The rejected batch additionally performs five
batch-four frozen forward calls before its guard: **20 extra forward rows** are
derived from the audited control flow, not measured by that last snapshot.
Thus total forward rows are 288 with this explicit derivation. Their time is
already included. Do not describe the stale neural snapshot as all attempted work.

Previous stages remain separate: model builds 24.16 CPU / 27.64 wall seconds;
stage3a 116 physical rows, 289.56 CPU / 302.15 wall seconds. Historical teacher
acquisition >=15,984,560 rows; hidden acquisition costs remain unavailable.
Both historical/current drivers use Planck15. Historical rotated angular prior
mass is 8; retain the ln(8) evidence-normalization correction for fresh matched
current-code baselines. No unconditional amortization or speedup claim follows.

A transient mini executor/control outage delayed the retry. After recovery,
read-only inspection confirmed no run2 directory or smoke process; only then
was run2 launched. Caltech SSH itself remained reachable. This is resolved.

## Exact provenance and reproduction

Research parent 4865d36e5af47808024a23e2bb40830499d8e57e; base rift_O4d
1621bb6e12391086edc3bfc8730ad807c8ad638c; infra-atlas
bb8608c695028f7eeeff53e5c4b5eef6537b2a39. Mini -> atlas laptop Tailscale route ->
ldas-grid.ligo.caltech.edu. No library source changes. Builder copies the verified
stage3a bank settings, retaining all 21 modes per detector and explicitly setting
ignore_threshold=None. Input hashes: results/case_inventory.json. Training XML
hashes also rechecked against the pretrained result manifest.

Starting pretrained checkpoint SHA256:
4ce26989ccdcfcdb1702dd5e270d79a772487adb2b348b8b471bcaf3a8cf3158.
Partial checkpoint:
/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage3b/run2/partial_smoke.pt
SHA256 479eac9c0c5e412755ce6a55de4a248eda60c8e3d8c47452e0efb4a4591673c9.
It contains six updates, no completed-smoke acceptance, and is not an inference model.

Executed smoke_real.py / repaired real_target.py / unchanged model.py SHA256:
607e364cd7de267989daadc2a7867ffc15add990c1ea021cdc4abf116ccbf7ea /
ba06d69217d9a7fabe840483907af6326e8d5d1f6f53b9c19a5fc940cdf63d7b /
5510640a980b8b2bba10d36eb8079ab0a6d44f83a1d7f330fda620387b4075da.
The initial builder SHA was
831f7240c3c4fbd978155e246c4bae7835316b4f696f725fbfdefe79969f2c87;
the sole repair adds import os. Original source and both runs remain on Caltech.
Logs SHA256 run1/run2:
e98e0d5a09993fa6041415a73c82690af39af3d7a6a245bb5a007108da0f2d18 /
661603f5eb8a9e972a0bc678d1994c6f9c86de8976e2aa53b49c9462c8b4f5d8.
Raw endpoint samples and weights were not copied into Git.

Exact run2 command on Caltech (do not rerun automatically):

```sh
root=/scratch/richard.oshaughnessy/codex_learned_wtf_20261015
export PYTHONPATH="$root/stage3a/source/MonteCarloMarginalizeCode/Code" PYTHONDONTWRITEBYTECODE=1
/usr/bin/time -v -o "$root/stage3b/run2.time" \
 /cvmfs/software.igwn.org/conda/envs/igwn/bin/python "$root/stage3b/source2/smoke_real.py" \
 --inventory "$root/stage3a/source/research/wtf_flowmap/results/case_inventory.json" \
 --checkpoint "$root/final_seed261015/pretrained.pt" --training "$root/final_seed261015" \
 --output "$root/stage3b/run2" > "$root/stage3b/run2.log" 2>&1
```

Local syntax validation of smoke_real.py and real_target.py passed. Actual
execution was manual on Caltech; these research files have no CI coverage.
PR445 stays unchanged at acf8dc1cc6838ee17a7b6246bcfa2b5be30ee510.

## Next decision and conditional sampler allocation

**First resolve the local angular-chart/domain contract**, then complete the
smoke's final validation in a separately bounded, reviewed diagnostic. Do not
hide this failure by wrapping an out-of-chart neural state, widening the chart,
clipping, filtering failed endpoints, selecting favorable noise, or redrawing.
No inference pilot should start with the current unresolved guard failure.

Conditional corrected-sampler allocation proposal, NOT launched: one fixed
anchor and seed, five arms (current AV, current AV+GMM, identical-cache
resampling, pretrained map, tuned map), at most 2000 attempted physical rows
per arm / 10000 total, one CPU, 20 CPU minutes, 8 GiB. Count setup/fitting,
oracle scoring, failed attempts and validation. Last three arms share the same
corrected defensive final sampler; map points enter proposal training only.
This is an accounting/configuration pilot, not a powered efficiency study.
Its cap is chosen for diagnostic coverage, NOT extrapolated from cached-row
rate; runtime feasibility is unknown until the actual sampler is measured.
Stop at the allocation even with incomplete arms and report them as incomplete.
Fresh normalized priors, held-out intrinsic anchors and replicated seeds are
required before evidence/mode/tail/ESS-per-total-cost scientific claims. A larger
campaign needs a separate estimate from the actual corrected sampler pilot.
