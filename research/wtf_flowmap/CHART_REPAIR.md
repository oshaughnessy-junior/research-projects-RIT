# Angular failure diagnosis and proposed minimal repair

**Diagnosis: local phase-chart representation failure in the frozen pretrained
reference.** A forward-only reconstruction matches the recorded first-step
distillation loss exactly (absolute difference zero). It reproduces six passing
batches and then one of four phase endpoints **0.1961739505 rad beyond +/-pi**
in the seventh batch. RA and psi remain within their charts; all four rows are
finite, inside distance (1,10000) Mpc, and away from angular poles. This is not
a physical-prior support violation in the reconstructed failed batch. No
physical likelihood was called to make this diagnosis.

The endpoint depends only on the frozen initial map and random draws, not the
student updates. Thus this specific failure cannot have been caused by unstable
student training. The six-update checkpoint is finite, with parameter change
L2=.00160553084 and fixed-validation endpoint RMS change .00125667488. These
small changes do not establish general training stability or posterior fidelity.

## What is saved, reconstructed, and measured

The original smoke did **not** save its failed endpoint or RNG state. It saved
the reason and six-update checkpoint. Do not call the reconstructed endpoint an
original capture. `diagnose_chart.py` replays the exact constructor/random-draw
order, without an optimizer, backwards pass or likelihood import. Frozen copies
also consume initialization draws; their constructors must not be skipped.
The first-step distillation value is an independent recorded RNG fingerprint.
Its exact match plus the six-pass/seventh-fail pattern supports this reconstruction;
it is not a byte-for-byte comparison to an absent original endpoint artifact.

The raw reconstructed batch and a diagnostic RNG snapshot are now preserved
privately on Caltech (the RNG snapshot is taken after partial-checkpoint inspection,
not at the original failure point):
`/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage3c/forward_replay/replayed_endpoint.pt`,
SHA256 `0ba0c682e5068811b16893a90d7728a8be1d94be2a04ce5f50faf0c3b525c9b6`.
Input checkpoints remain `4ce26989...cf3158` (pretrained) and
`479eac9c...1673c9` (partial), with full hashes in the aggregate result.

The bounded Caltech inspection used one CPU, **.89 complete-process CPU seconds,
1.33 wall seconds, 238548 KiB peak RSS**. Zero new physical rows, zero optimizer
steps, no training. Full imports/shutdown are included. Administrative SSH/hash/
transfer time is not separately measured. Previous stage costs remain separate.
Nine standard-library mathematical/unit tests passed locally on the mini using
Python 3.9.6 (test-body time .001 s; full local process cost not metered).
Tests use synthetic coordinates and a toy periodic velocity, not a newly trained net.

## Minimal recommended representation: three circle coordinates

Keep a six-dimensional learned state, but make the three angular factors
genuinely periodic. Use

    theta = (RA, phase, 2*psi), each modulo 2*pi
    u = (atanh(sin dec), atanh(cos incl), log D)

Retain training-only mean/std standardization of the three nonangular coordinates.
Use unit-radius angular radians instead of an empirical linear angular chart.
Keep the existing strict physical-distance and pole guards. Distance was not the
observed defect, so a logit-distance change is **not part of the minimal repair**.
`periodic_geometry.py` also tests a separately labelled optional bounded-distance
variant; it is an alternative specification, not an adopted or trained model.

Feed `[sin(theta_i),cos(theta_i)]` and the three nonangular coordinates to both
neural velocity networks. They have nine state features, six tangent outputs and
the existing time inputs. Do not feed raw angular lifts through any other path.
Use a lifted residual jump

    X(s,t,x) = x + (t-s)*v(s,t,features(x)).

Because the features are periodic, v(x+2*pi*k)=v(x), and therefore
X(x+2*pi*k)=X(x)+2*pi*k for angular lattice shifts k. The jump descends to a
well-defined map on the torus. Intermediate states retain continuous angular
lifts; there is no output clipping or wrapping of the old nonperiodic network.
This property is architectural, not an approximate data-augmentation promise.
Large lifts still require a numerical argument-reduction/finite-value gate.

Use uniform angular base noise on the torus and Gaussian noise for standardized
nonangular coordinates. Flow-matching interpolation follows the shortest angular
displacement `atan2(sin(theta1-theta0),cos(theta1-theta0))`; nonangular paths stay
linear. Specify a deterministic convention for exactly antipodal pairs, a
measure-zero ambiguity, and test both sides. Teacher ODE integration can retain
lifts because the drift is periodic. Retrain/distill the map from that drift.

For semigroup distillation, use angular chord loss `2*(1-cos(delta))` plus
nonangular squared error; comparing raw angular lifts would penalize equivalent
representatives. The diagonal target and control difference are tangent velocities,
so their control cost remains a sum of squares, **without** reducing velocities
modulo 2*pi. All frozen drift/map input derivatives remain in the reward pullback.
The declared metric is flat in these torus angles and standardized nonangles.
In particular theta_psi=2*psi weights physical psi velocity by a factor four in
the squared norm. This changes the old standardized-chart transport objective;
lambda=.001 from the old smoke is not an equivalent regularization strength.

This is a proposed manifold/periodic adaptation of the mathematical WTF objective,
not a reproduced image recipe or an assertion that the paper's Euclidean theorem
automatically covers this parameterization. The physical coordinate product also
excludes angular poles as measure-zero coordinate singularities; no global unique
parameterization of all physical rotations is claimed.

## Decoder, Jacobian and likelihood boundary

Physical callback order stays `[RA,dec,psi,incl,phase,D]`:

    (theta0, asin(tanh u0), theta2/2, acos(tanh u1), theta1, exp u2).

Its nonzero Jacobian entries, before optional nonangular standardization, are
`J[0,0]=1`, `J[4,1]=1`, `J[2,2]=1/2`, `J[1,3]=sech(u0)`,
`J[3,4]=-sech(u1)`, `J[5,5]=D`. Absolute determinant is
`D*sech(u0)*sech(u1)/2`; multiply by all three nonangular standard deviations
when decoding standardized variables. The negative inclination sign and psi
factor 1/2 matter. Uniform physical psi density 1/pi pulls back to 1/(2*pi),
not another mass factor two. The tests verify the full unstandardized Jacobian
by independent finite differences; standardized AD still needs the next parity gate.

Pass lifted angles to a verified periodic physical backend rather than inserting
a remainder discontinuity into AD. Existing one-point period tests are not enough
to accept the new model: test values/gradients on both sides of each seam, with
integer representative changes, against NumPy NoLoop. The reward remains physical
log L alone. Prior and decoder Jacobian enter only their explicitly declared
integration/coordinate contracts; do not add them silently to the reward.
Physical priors normalize on one fundamental angular domain, not on all lifts.

These maps still have no tractable normalized log q. They remain training-only
oracles behind a corrected defensive sampler. A pushforward of a real-valued net
through modulo can define a distribution, but it does not make its drift/jumps
equivariant or preserve the intended quotient-space transport objective. That is
why simply weakening the current guard or wrapping its outputs is not this repair.

## Alternatives and checkpoint invalidation

A two-chart-per-angle atlas could avoid seams with transition maps, overlap
consistency, metric transformation and chart gating/partition weights. It adds
up to eight chart combinations and more consistency contracts. A tanh-bounded
single open chart is an almost-everywhere reparameterization, but creates a
cut-dependent transport barrier and large inverse coordinates near the cut.
Neither is smaller than periodic inputs/residual jumps for this observed phase
problem. Neither can be bolted onto old weights while claiming the same objective.

**Retrain both drift and two-time map.** Their input dimensions, angular noise
law, interpolation targets, losses and metric change. The old pretrained and
six-update checkpoints remain forensic artifacts, not warm starts accepted under
the new representation. Old ODE/map accuracy gates do not certify the periodic
replacement. Reuse the same hash-verified 300/100 seed split and acquisition record;
do not select a new favorable split. Old physical NoLoop/JAX value tests remain
historical evidence for that backend, but the new decoder and complete pullback
need fresh parity. Prior mass8/ln(8) correction remains relevant for historical
evidence; no prior normalization or production default changes are proposed.

Future failures must persist the pre-dispatch endpoint, base noise, times, RNG,
all relevant model hashes and neural ledger in a finally block on Caltech. This
is a diagnostic design requirement, not a training rerun performed here.

## Proposed next bounded gates — none executed

1. **Periodic pretraining/distillation:** one Caltech CPU, five CPU minutes,
   2 GiB resident cap with monitoring, zero physical rows; one fixed seed 261017,
   width64/batch128, 2000 flow-matching + 6000 distillation steps, the existing
   300/100 split. Estimate tens of CPU seconds for one build from the previously
   measured model-only builds; the five-minute cap is a limit, not a guarantee.
   No hyperparameter sweep or automatic repair run.
2. Before any real reward, require exact architecture lattice/derivative
   equivariance (float64 <=1e-10), seam/antipodal/psi-period tests, no nonfinite
   outputs, and report all endpoint distance/pole failures without filtering.
   Require RK4 64/128 refinement RMS <=.01, one-step teacher RMS <=.15,
   semigroup RMS <=.1 and diagonal tangent RMS <=.2. RMS uses shortest angular
   displacement in unit-radius radians and standardized nonangles. These are
   new declared units, not a numerical comparison to the old chart gates.
   Report held-out circular mode/marginal/tail errors without retuning the split.
3. **New-coordinate physical parity only:** one CPU, ten CPU minutes, 8 GiB,
   at most128 endpoint attempts including all failures, zero optimizer updates.
   Plan 111 attempts: eight cached-point values, 26 bridge checks, twelve
   two-sided seam/backend values, 32 generated-point/backend values, 25 generated
   gradient checks and eight deliberate guard fixtures. This is diagnostic
   coverage budgeting, not cached-row-rate extrapolation to an inference campaign.
   Keep value tolerance1e-3 nat, derivative tolerance .01+.001*abs(AD), two finite
   difference step sizes, full chart scaling/Jacobian, and periodic derivative
   equality. Stop on any failed certificate/parity/support test or resource cap.
4. Only after both gates pass, separately budget another <=8-update actual-objective
   smoke under the new metric, with a newly declared coefficient and full failure
   capture. The five-arm corrected-sampler pilot remains **not launched** and on
   hold. This document proposes future execution; this stage performed no training
   or likelihood evaluations and did not change the old guard.

Validation commands/results and exact source/checkpoint pins are in
`results/stage3c/summary.json`, the replay result, and `local_unit_tests.txt`.
