# Bounded five-arm real-case pilot: inconclusive, with a backend blocker

The pilot does **not** demonstrate calibrated inference or a transport advantage.
Four arms completed their fixed 1,000 evidence draws, all with Kish ESS about one.
The cache arm failed before completing that population. A bounded diagnostic
then found a valid point where the differentiable JAX backend returns NaN while
the maintained NumPy NoLoop backend returns finite log likelihood
47.83659196403751. Stop here; repair and regress that backend discrepancy before
considering a powered held-out-anchor study. No larger study is proposed or run.

## Scope and exact contracts

This is one seed (261051), one training anchor (S250114ax/47536), and five research
arms: fresh native AV; native AV+GMM; same-cache warm start; pretrained periodic
map; actually reward-tuned periodic map. It is an external bounded driver using
current native proposal/adaptation APIs, **not a full production ILE benchmark**.
No production integrator, ILE auto-wiring, security setting, or shared environment
was changed in this stage. No checkpoint was retrained in this pilot.

All arms use the same six-dimensional unit-cube coordinates, normalized Planck15
cosmo_sourceframe distance prior on [1,10000] Mpc, isotropic orientation/sky
priors, full retained modes, and the previously verified raw time-integral API.
The distance-prior mass check was 0.99999969436. This API's log integral is not
normalized over a uniform time prior; that alternative adds -log(613/4096).
Historical angular-prior mass eight is not silently treated as a current baseline.

The first five cube coordinates are RA/(2pi), (sin(dec)+1)/2, psi/pi,
(cos(incl)+1)/2, and phase/(2pi); the sixth is (D-1)/9999. Only the already
periodic model's angular representatives are reduced modulo their periods.
The final proposal has a fixed 20% uniform defensive component. AV-only receives
80%; the other arms receive 40% AV and 40% GMM. GMM groups are sky, phase/psi,
and distance/inclination, with 4,4,2 components. All groups adapt in this research
driver; transformed coordinates and these settings limit direct production
comparability. Native portfolio ESS-based mixture-weight adaptation is not used.

AV density is its exact live-bin sampling_density(), not draw_simplified()'s
legacy scale. Each fitted two-dimensional GMM box mass is checked independently
by conditional-normal quadrature. Oracle rows only bootstrap proposals; no
implicit-map density is assigned. Map/cache clouds are fitted unweighted as a
heuristic proposal-training distribution, not asserted to have posterior weights.
Fresh AV/GMM initialization uses its own uniform initial target evaluations.

Each arm reserves 1,000 final draws in advance, in ten batches. Correction uses
that batch's conditional mixture density, with lnL + lnprior - lnq. Adaptation
has a separate bounded allowance; unused allowance is not converted into extra
evidence or transferred between arms. All true work is metered before dispatch.
Training, validation, adaptation and failure-diagnostic rows never enter the
final estimator. No sampler main cache or posterior export is populated by the
oracle. The cache arm's incomplete population yields **no evidence result**.

## Results (diagnostics, not accepted evidence)

| Arm | Total attempts, including diagnosis | Fixed population | Raw computed log integral | Kish ESS | Max weight |
|---|---:|---|---:|---:|---:|
| AV | 1,941 | complete | 1777.7782 | 1.0000 | 1.0000 |
| AV+GMM | 1,934 | complete | 1854.0825 | 1.0000 | 1.0000 |
| Same cache | 1,809 | failed; 800 evidence attempts | not reported | not reported | not reported |
| Pretrained map | 1,928 | complete | 2786.3007 | 1.0000 | 1.0000 |
| Tuned map | 1,942 | complete | 2540.5566 | 1.000003 | 0.999999 |

Every completed arm fails the declared ESS>=100, max-weight<=.05 and relative
linear-integral SE<=.1 criteria; nominal relative SE is about one. First/second
half diagnostics are in diagnostics.json. These raw numbers are neither reliable
absolute evidence estimates nor evidence-error measurements. There is no exact
real-case evidence truth, and the new backend failure further blocks a common
validated-target claim. Higher encountered likelihood is not a success criterion.

The held-out 100-row teacher fair-draw cache is a finite-ESS, potentially correlated
diagnostic reference, not exact posterior truth. Relative to it, positive-cos(incl)
mass discrepancies are -.60, -.60, +.40, +.40 for the four completed arms; distance
mean discrepancies are -141.46, +401.90, -59.11, +21.08 Mpc. Distance-tail mass
discrepancies are +.86, +.86, -.14, -.14, using training-cache 5/95% thresholds
323.3218035 and 447.6315955 Mpc. Weight collapse makes apparently small individual
moment discrepancies or zero nominal SNIS error unpersuasive. Full circular
moments, joint phase/psi bins, distance second moment, tails, nominal uncertainty,
ESS/call and explicitly shared-setup ESS/CPU and ESS/wall are in diagnostics.json.
Those uncertainty calculations do not calibrate adaptive/teacher dependence.
No true mode-mass error or absolute evidence error is claimed.

## Failures retained and charged

Initial entry-point setup omitted JAX float64 and stopped at the strict Jacobian
gate, with zero true calls. The next used a mistakenly tighter 1e-6-nat value
threshold instead of the existing stage3e 0.001-nat threshold, spending ten calls.
Both are preserved. The accepted restart restored the existing precision and
threshold, and debited those calls from each arm's allowance. Actual parity
errors on its two fixed points were <=2.013e-5 nat. These gates sample points;
they do not prove global backend validity.

The cache arm's eighth evidence batch contained a nonfinite target. That original
failed batch was not saved: a limitation of the runner's failure capture. A
subsequent zero-true-call replay matched all 15 preceding saved training/adaptation/
evidence batches exactly and reconstructed the missing batch. This is explicitly
a reconstruction, not an original capture. Its coordinates are inside bounds,
none is at a cube boundary, and all 100 prior values are positive and finite.
A separately charged 100-row JAX evaluation found one NaN; one NumPy NoLoop
reference evaluation at that index gave 47.83659196403751. No replacement values
were inserted, no incomplete estimate was reported, and no arm was restarted.
A future runner should persist attempted inputs before dispatch; this single-pilot
runner is preserved as executed, including its manual prior-attempt debit.

## Full measured cost and reuse

Across tests, failed setups, final run, analysis, replay and diagnosis:
**9,554 attempted/physical target rows, 242.84 process CPU seconds, 266.32 summed
command wall seconds, 2,287,632 KiB peak RSS**, one CPU affinity [0], no GPU. Counts:
validation including failures 30; initial training 500; AV adaptation 4,123;
evidence attempts 4,800; failure diagnosis 101. No gradient/optimizer likelihood
work occurred in this stage. Two map forwards used 100 rows each. Mathematical
normalization tests used 20,000 synthetic proposal draws across their two runs;
these are not physical likelihood evaluations, and their CPU is included.

The caps were one CPU, 1,200 aggregate CPU seconds, 8 GiB and 10,000 attempts, no
arm above 2,000. Bounded interactive validation ran on ldas-grid via the established
mini -> laptop -> Caltech SSH route; no HTCondor job was submitted, so no batch job
ID exists. Host/PIDs and command timing records are retained. No alternate compute
site, install, credential change or external contact was used.

The summed command wall duration includes each command's import, setup, JIT,
validation, training and integration. It excludes coordinator gaps and unmeasured
source preparation, SSH/admin and transfer overhead; it is not complete project
elapsed time. Earlier selected measured stages add 450.30 CPU / 493.95 wall seconds
and 378 physical rows, with additional historical checks/admin costs not fully timed.
Teacher acquisition remains **>=15,984,560 rows plus unknown overhead and CPU**.
Cold cost cannot be replaced by the measured reuse cost. Equal five-way allocation
of shared setup/failed-gate costs is explicitly an amortization scenario, not five
independently measured cold jobs. Cached/model cold ESS/true-row values in the
JSON are only upper bounds using the acquisition lower bound. There is no
measured break-even reuse count or demonstrated cost advantage.

## Reproduction, sources and next decision

RIFT development base remains 1621bb6e12391086edc3bfc8730ad807c8ad638c;
infra-atlas pin bb8608c695028f7eeeff53e5c4b5eef6537b2a39;
WTF source inspected 058a4f2d9a42f384a0be8d1224784b755986e4ff,
arXiv 2609.27033v2. This learned periodic research implementation and its
training-only use are distinct from a faithful reproduction of the authors'
image recipe and from the original simplified transport-inspired draft prototype.
Reward=log likelihood still does not make implicit-map outputs posterior draws.
See PERIODIC_WTF_SMOKE_RESULTS.md for the eight-update mathematical/real-objective
smoke, frozen-reference choices, and its separate costs.

Pretrained checkpoint SHA a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea;
tuned checkpoint SHA 71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1.
Exact executable/source, input and private-artifact hashes are in
results/stage4/provenance.json and the existing case inventory. Private raw draws
and reconstructed failing batch remain under
/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage4; only aggregates,
commands and hashes are committed. Re-execution spends a new budget: the scripts
are the recorded single-pilot commands, not permission for another campaign.

On Caltech, with the pinned CVMFS Python and pinned RIFT source first on PYTHONPATH:

```
CUDA_VISIBLE_DEVICES= OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
  taskset -c 0 /cvmfs/software.igwn.org/conda/envs/igwn/bin/python \
  -m pytest -q test_five_arm_sampling.py
```

Final result: 7 passed. Earlier gate: 5 passed. Native AV/GMM wrong-oracle
normalization, collapsed cloud rejection, zero-weight denominator, exact adaptive
conditional-weight enumeration, bounds/alignment, fixed count, reset isolation,
and independent correlated/diagonal box masses are covered. Local mini pytest
was unavailable; no packages were installed. Read-only replay matched 15 batches.
The target diagnostic **failed** global backend consistency. No production shape
recovery gate, held-out intrinsic anchor study or new CI run was executed.

Retain the useful accounting, normalization tests, negative result and backend
reproducer. First resolve the finite-input JAX/NoLoop discrepancy and capture
failed inputs before dispatch. This single-seed, severely underconverged pilot
neither establishes a win nor rejects the general method. It does not justify
pricing or launching a powered held-out-anchor campaign yet.
