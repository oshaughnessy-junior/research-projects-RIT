**Follow-up:** the separately recorded model-only continuation passed the unchanged
solver gates. See `DISTILL_REPAIR_RESULTS.md`. This file preserves the original
failed build and its complete costs; it has not been relabelled as a pass.

# Periodic model rebuild: representation contracts pass, solver gate fails

**This stage did not pass.** Both learned models were rebuilt from scratch on
the pinned real data, and periodic representation/derivative contracts pass.
The distilled map's one-step agreement with its learned drift teacher is
insufficient: RMS **.277704 > .15**. A separately seeded read-only check gives
.271981. No physical likelihood, reward tuning, parity gate or inference pilot
ran. No wider training search or automatic repair followed this single build.

## Implemented model and training consistency

`PeriodicVelocity` retains six state/tangent dimensions but uses nine state
features: sin/cos of `(RA,phase,2*psi)` and the three standardized nonangles
`atanh(sin dec), atanh(cos incl), log distance`. Time features are additional.
Angular lifts are never fed linearly into the velocity network. Therefore drift
and map velocities are periodic, and `X=x+(t-s)*v` is equivariant under all angular
lattice shifts. There is no output wrapping of an old nonperiodic network.
Both neural nets have two tanh hidden layers of width64 and new weights.

Base noise is uniform on the three circles times a standard Gaussian in the
nonangles. Flow matching uses shortest angular displacements, linear nonangular
interpolation and exact antipodal tie +pi; no exact ties occurred in training.
Nonangular standardization uses only the 300 training rows. The 100-row fourth
sampler seed remains held out, duplicates retained. These are finite-ESS fair
draws, not independent exact posterior truth. The same frozen learned drift
generates RK4 trajectories for two-time map distillation. Chord loss handles
angular equivalence, nonangles use squared error, and diagonal tangent velocities
are anchored to the frozen drift. Velocity/control differences are never modulo.

Fixed seed261017, float64, batch128, Adam lr1e-3, 2000 flow-matching and6000
distillation updates, 512 cached32-interval teacher paths. No earlier model
checkpoint weights were loaded. `model.py` contributes only the existing generic
meter/RK4 helpers. This is a proposed periodic/manifold adaptation, not an image
recipe reproduction or a claim that the Euclidean theorem transfers automatically.
No normalized log q is supplied and no output is certified as a posterior draw.

The decoder preserves lifted angles, includes psi's half-angle and the negative
inclination derivative, and retains the strict distance/pole support checks.
The old chart model and guard are unchanged. The optional logit-distance geometry
was not used. Angular metric units are unit-radius radians; nonangles use training
standard deviations. Accuracy numbers are not directly comparable to the old chart.

## Validation

13 manual Caltech tests passed: four explicit neural-model tests plus nine geometry
tests. They check base-law moments/support, quotient-consistent interpolation,
antipodal convention, neural shift/jump/Jacobian equivariance, seam derivatives,
full standardized decoder FD/Jacobian and scalar-spec agreement, frozen input
derivatives, finite training gradients and exact diagonal jump identity.
The same contract helper was run on the actual trained drift/map. Maximum trained
velocity/Jacobian shift discrepancy was **7.049917e-15**, below1e-10.
There was one benign Torch scalar-conversion warning in diagnostic comparisons.

Independent validation used1024 fresh base draws. RK4 refinement uses128 of them;
semigroup/diagonal/segment checks use256 independently sampled time triples.
There is no exclusion of nonfinite or out-of-support rows.

| Gate | Measured | Required | Result |
|---|---:|---:|---|
| one-step teacher RMS | .277704264 | <=.15 | **failed** |
| RK4 64/128 refinement RMS | 4.035601e-8 | <=.01 | passed |
| semigroup RMS | .031819917 | <=.1 | passed |
| diagonal tangent RMS | .135636221 | <=.2 | passed |
| drift finite/support failures | 0/1024 | 0 | passed |
| map finite/support failures | 0/1024 | 0 | passed |

Held-out segment RMS was .076371. Passing local/semigroup gates does not establish
the required long-jump teacher accuracy. The primary blocker is solver distillation,
not the old phase seam. Finite/support tests on1024 draws do not prove global support.

A read-only checkpoint validation (independent seed261020, zero optimizer steps)
confirmed one-step RMS .271981. Per-coordinate RMS was RA .307741, phase .334647,
twice-psi .301295, standardized declination .240351, inclination .252255, logD
.158016. Mixed signed shifts of (-2pi,+4pi,-6pi) gave drift/map/jump errors <=
3.552714e-15. Diagonal time derivatives `d_t X=v`, `d_s X=-v` passed and were finite.
Its complete noise/teacher/map/residual payload is private on Caltech, SHA256
`cb2313d8d91253d5451e9f20f660d49418623bcbcca0a010a72c1f0668211a65`.
This is a new validation sample, not the first build's unsaved validation noise.

Full marginal/tail summaries are preserved without treating them as pass criteria:
held-out cos-inclination positive mass .600 versus drift .617188 / map .632813;
phase16-bin L1 discrepancy .473281 / .951797. They are conditional finite-cache
diagnostics, not a calibrated inference comparison or evidence of a cost win.

## Cost and limits

One affinity-pinned Caltech CPU [0], no GPU, batch job, install or shared runtime
change. Stage cap300 CPU seconds, hard process limits20s(tests),260s(build),20s
(read-only validation); actual total remained far below the cap. Each process
had a hard2-GiB virtual-memory limit, stricter than the proposed resident cap.

| Process | CPU seconds | Wall seconds | Peak RSS KiB |
|---|---:|---:|---:|
| 13 model/geometry tests | 2.83 | 3.82 | 327436 |
| one fixed-seed build and all gates | 9.10 | 9.75 | 374852 |
| read-only checkpoint validation | 1.13 | 1.63 | 241888 |
| total | **13.06** | **15.20** | **374852 peak** |

GNU time includes imports/shutdown, data/hash setup, all training, teacher rollout,
AD/FD contract tests, failed gate work and read-only validation. Administrative
SSH/source transfer/hash checks are not separately timed. There were **zero new
physical rows**. Build exit0 means execution completed, not that science/model
gates passed; the journal and checkpoint explicitly set `gates_passed=false`.
Earlier acquisition/model/stage3a/3b/3c costs remain separate and unamortized.

## Exact pins and preservation

Research parent `12ad1573de6091a41b0aa97a79c3e438d1f7df23`; RIFT base
`1621bb6e12391086edc3bfc8730ad807c8ad638c`; infra-atlas
`bb8608c695028f7eeeff53e5c4b5eef6537b2a39`. Route remains mini -> atlas laptop ->
ldas-grid.ligo.caltech.edu. CPU PyTorch2.8.0/numpy2.2.6, float64.

Failed-gate checkpoint (both new models):
`/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage3d/seed261017/periodic_pretrained.pt`
SHA256 `394ad4a1a56e2f648c11c4d14ff5a0a4a7b9a16cf39dfab3d6b29787fb580a6d`.
Raw scientific data and model weights remain on Caltech; Git holds aggregates.

Executed source SHA256:

- periodic_model.py: `878d90069b3c9691a9b87f403660f2f341feff84cc9e481a110d7f55cfae90de`
- test_periodic_model.py: `a75a0282eeea806cfccdd859c7fd28332175b721fd2208afb0d663f06fcea439`
- train_periodic.py: `6762d0c4849a78cc8a63aa6b5bd1022e0adc96fb19f52ae24845ea769b118f6a`
- read-only validator: `b3d903a845afe0026f265abab9eb8e79bbb86d4160db003cd6a48a73bfe31867`

Data NPZ SHA256 `4581e8b8584922946f5ee01756b2ffb9f336d2a66171014ac95641ff365beb2e`;
manifest `4de8a247ffb55d2f007faa1ec82d65c92c165c4aa9f7ebcc66cbfa7f662cb060`.
Source XML/archive hashes were reverified and shared columns matched the NPZ
exactly. Data-provenance record SHA256
`e809627592936cd16e2e55fe9abece6ea476d25323d550f47b91162b097166a9`.
Full archive/source pins and neural counts are in `results/stage3d/seed261017/result.json`.

The old pretrained `4ce26989...cf3158` and six-update partial `479eac9c...1673c9`
checkpoint hashes were rechecked unchanged. They remain forensic artifacts.
The earlier smoke's original failed endpoint was never captured; stage3c's
validated forward reconstruction remains explicitly distinguished from it.
No checkpoint in this stage retroactively repairs that earlier evidence.

Commands (remote root stage3d under the scratch prefix above):

```sh
# Tests: taskset -c0, CPU20s/AS2GiB wrapper, then pytest with no cache:
python -m pytest -q -p no:cacheprovider source/test_periodic_model.py source/test_periodic_geometry.py
# Exact external wrapper command is preserved in tests.time.
/usr/bin/time -v -o train.time /cvmfs/software.igwn.org/conda/envs/igwn/bin/python source/train_periodic.py \
 --data-record ../final_seed261015/result.json --training ../final_seed261015 --output seed261017
/usr/bin/time -v -o checkpoint_validation.time /cvmfs/software.igwn.org/conda/envs/igwn/bin/python \
 source/validate_periodic_checkpoint.py --checkpoint seed261017/periodic_pretrained.pt --output checkpoint_validation
```

These completed output directories must not be overwritten. Manual Caltech tests
are not CI; the research directory has no CI coverage. PR445 remains unchanged.

## Next decision and blocked parity estimate

First improve **teacher-to-map long-jump distillation** in a separately bounded
model-only repair. Do not change the .15 gate, select favorable validation noise,
or reinterpret semigroup consistency as teacher accuracy. The learned drift's
refinement and periodic representation passed, so the next candidate can keep
that drift frozen and target the map's endpoint accuracy; no such repair ran here.

After the solver gate passes, the proposed physical-parity allocation remains
one CPU, ten CPU minutes, 8 GiB and at most128 attempted endpoint/physical rows,
with no optimizer updates. A revised123-attempt plan includes the earlier111
coverage attempts plus12 seam/representative AD checks. Count all failed endpoints
and gradients, preserve pre-dispatch inputs/RNG/model hashes, and stop at any
support/certificate/parity/resource failure. Ten minutes is a conservative bound
from complete prior setup/parity processes, not a cached-row-rate prediction.

Revalidate the complete **new** standardized decoder/Jacobian (psi1/2 and negative
inclination sign), Torch/JAX bridge, NoLoop/JAX values and gradients, both sides
of each angular seam and integer representatives, and fixed generated endpoints.
Retain full-mode waveform precompute, exact time grid and1e-3-nat value tolerance;
gradient tolerance .01+.001*abs(AD), two finite-difference steps. The physical
target did not change, but old chart-pullback parity cannot certify new coordinates.
Historical angular prior mass8 still needs its ln(8) normalization correction for
fresh matched evidence baselines. The metric changed, so old lambda=.001 is not
an equivalent physical regularization strength. No physical parity, reward smoke,
corrected inference or larger pilot starts from this failed-gate checkpoint.

Before a later real-objective smoke, adapt and recheck the actual WTF loss too:
the old `model.wtf_loss` uses Euclidean lift differences in its semigroup term
and must not be reused unchanged. The periodic version needs chordal angular
semigroup error, ordinary tangent control differences with the declared metric,
and the exact frozen-drift/map reward pullback. The old mathematical-objective
tests do not certify that future adaptation. None was executed in this model-only
stage.
