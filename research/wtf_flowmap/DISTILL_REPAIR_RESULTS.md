# Long-jump repair passed the unchanged sampled model gates

The original failed periodic build is preserved. This continuation keeps its
learned drift **exactly frozen** and repairs only the two-time map. A duration
control failed at RMS .223041; half-endpoint coverage passed at .147620 on the
fixed selection set and **.144216 on a once-only reserved final set**, both below
the original .15 gate. All other original gates passed. Training stopped at that
point; later planned duration/capacity candidates were never run.

This is a model-only solver result, not physical reward tuning, posterior/evidence
calibration or an efficiency claim. No physical-likelihood calls, parity or
inference work ran.

## Why local/self-consistency gates could pass

The original discrete pair schedule draws i uniformly from0..31, then j uniformly
from i+1..32. Its exact full-interval probability is1/1024: only750 expected
0->1 rows in6000 batches of128. Mean span is .2734375; only .0380972 of rows span
at least25/32. Also, endpoint error for short jumps scales with (t-s), so local
fits do not constrain the complete map as tightly as the one-step teacher check.

Semigroup consistency compares the map with itself, not with the teacher. A
constant translation .4 in one of six axes is an exact semigroup and differs
from an identity teacher by diagonal RMS .1633 (<.2), yet one-step RMS .1633
fails .15. Two local mathematical tests verify the coverage calculation and this
counterexample. They do not themselves diagnose the trained network causally.

The matched repair comparison supports insufficient long-jump coverage as a
practical contributor: extra training alone improved accuracy but did not pass;
endpoint coverage did. This is one comparison, not proof that coverage was the
only cause or that the original512-row teacher cache had no effect.

## Fixed data, recipes and validation separation

Both recipes restart from the same periodic checkpoint, use a **new shared4096-row
teacher cache**, identical Adam lr1e-3, width64, batch128 and6000 extra steps.
The drift, physical transforms and metric are unchanged. The original cache was
not saved, so it is not falsely claimed to be reused. New cache trajectories are
32-step RK4 from the same frozen drift. Independent64/128-step refinement passes.

Control A uses the original pair schedule. Candidate B resets to the original
map/optimizer state and paired training draws, then assigns the first64 rows of
every batch to (s,t)=(0,1); the other64 retain their random pairs. Diagonal weight
stays .1. This changes the coverage of both map and diagonal training inputs;
it is not an isolated change to one loss coefficient. No clipping or seed search.

| Candidate | Extra steps | One-step RMS, selection | Semigroup | Diagonal | Result |
|---|---:|---:|---:|---:|---|
| original checkpoint | 0 | .271981 | .030688 | .139099 | failed |
| A: original coverage, extra duration | 6000 | .223041 | .025849 | .106232 | failed |
| B: half full-interval coverage | 6000 | .147620 | .024324 | .165716 | passed selection |
| B: reserved final confirmation | 0 | **.144216** | **.031625** | **.166000** | **passed all** |

Seed roles were fixed and recorded before training: cache261021, paired training
draws261024, reused selection validation261020, reserved final validation261022.
The first selection set was already examined in stage3d, so it is explicitly a
selection set, not an untouched confirmation. Training/selection/final base rows
have no exact overlap. Final results were inspected once, after B passed selection;
the program would stop without retuning if that reserved set failed. It instead
passed and returned immediately. No widening or additional duration iteration ran.

The original real-data300/100 seed split also remains unchanged: the frozen drift
was trained on the first300 finite-ESS fairdraw rows, not the held-out100. This
repair uses neural teacher trajectories, not additional physical truth labels.

Final confirmation used1024 fresh base samples,256 time triples and128 refinement
rows. Results: teacher refinement RMS4.415511e-8 <=.01; shift/Jacobian discrepancy
1.132427e-14 <=1e-10; zero map/teacher support failures and zero nonfinite states.
Semigroup .031625 <=.1 and diagonal .166000 <=.2. The one-step margin is narrow;
this is acceptance under the declared sampled gates, not uniform/global accuracy.
The teacher's finite-data posterior approximation and held-out marginal errors
remain limitations even with better distillation.

## Original CPU allocation, never reset

Before launching, all existing stage3d timing files were inventoried and parsed:
tests.time2.83 CPU s, train.time9.10, checkpoint_validation.time1.13 = **13.06**.
No other training attempts or running repair process were found. The continuation
itself reads every prior .time file and refuses unresolved timing records.
It subtracts those costs from300 before setting its process limit. Runtime
soft/hard limits and a shutdown reserve prevent exceeding the original cap.

| Work | Complete process CPU s | Wall s | Peak RSS KiB |
|---|---:|---:|---:|
| previous stage3d tests/build/check | 13.06 | 15.20 | 374852 |
| teacher cache, both candidates, selection/final checks | 17.99 | 20.44 | 321424 |
| original-allocation cumulative | **31.05 /300** | **35.64** | **374852 peak** |

Remaining allocation: **268.95 CPU seconds**, not a new budget. Candidate training
wall times were7.7383 s(A) and6.7172 s(B). Separate per-candidate CPU times were
not recorded; full process CPU includes both attempts, setup/hash/import/shutdown,
teacher rollout, all AD checks and final validation. Do not interpret the warm
second candidate's lower wall time as a performance comparison. Administrative
SSH/transfer/hash checks and local mathematical test process overhead were not
separately metered. Two local math tests passed (test-body time0.000s).

The aggregate neural ledger includes4,608,000 candidate forward rows and3,072,000
output-backward rows, plus separately recorded teacher/setup/validation and input
Jacobian work. There are **zero physical rows**. One CPU affinity[0],2-GiB hard
address-space cap, no GPU, scheduler job, installation or shared runtime changes.

## Source, checkpoint and cache pins

Research parent `34a39f4f46d9b337f965f8ed8f9ddf8e8a094fb4`; RIFT base
`1621bb6e12391086edc3bfc8730ad807c8ad638c`; infra-atlas
`bb8608c695028f7eeeff53e5c4b5eef6537b2a39`. Established mini -> laptop -> Caltech
route. Executed repair source SHA256
`8ed86b375aeb7124831d313ca1891833c834d4917d881b9d4c5424da3154d2b2`.
Periodic model source remains
`878d90069b3c9691a9b87f403660f2f341feff84cc9e481a110d7f55cfae90de`.
All helper/source hashes and prior timing paths are in the raw aggregate result.

Accepted **model-gate-only** checkpoint:
`/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage3d/repair1/accepted.pt`
SHA256 `a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea`.
It contains drift_width64 and map_width64 explicitly; future loaders must read
these separate metadata fields. The state/decoder/base remain T3_R3_v1.

Original periodic checkpoint `394ad4a1...580a6d` remains unchanged; old nonperiodic
pretrained `4ce26989...cf3158` and partial smoke `479eac9c...1673c9` remain forensic.
The earlier unsaved failed endpoint is not retroactively created by this result;
its stage3c reconstruction remains distinguished from an original capture.

Frozen teacher tensor SHA256
`fd2e2e6e4d1ce1d8ad8dbe10613606ccf939293f00d38b67958c709d893ae217`;
all drift tensors were checked equal after optimization and at acceptance.
New teacher cache SHA256
`80aa74d157cb968419a19f80e324abaa4f28ec1e298c58ab7a333ba212f844ae`;
reserved final payload SHA256
`7c05bb519391f5d514a9505a6a6379b2bf9c3f973e2004642cb5201baf846a5a`.
Both raw caches/payloads remain on Caltech. The original NPZ/manifest pins remain
`4581e8b8584922946f5ee01756b2ffb9f336d2a66171014ac95641ff365beb2e` /
`4de8a247ffb55d2f007faa1ec82d65c92c165c4aa9f7ebcc66cbfa7f662cb060`.

Exact remote command, with new output directory required for any future execution:

```sh
root=/scratch/richard.oshaughnessy/codex_learned_wtf_20261015
allocation="$root/stage3d"
export PYTHONDONTWRITEBYTECODE=1
/usr/bin/time -v -o "$allocation/repair1.time" \
 /cvmfs/software.igwn.org/conda/envs/igwn/bin/python "$allocation/repair_source/repair_periodic_distillation.py" \
 --allocation-root "$allocation" --timing-file "$allocation/repair1.time" \
 --checkpoint "$allocation/seed261017/periodic_pretrained.pt" \
 --selection-payload "$allocation/checkpoint_validation/solver_validation.pt" \
 --output "$allocation/repair1" > "$allocation/repair1.log" 2>&1
```

## Next boundary

Stop here as requested. The separately proposed physical parity gate (one CPU,
ten CPU minutes,8 GiB,128 attempts maximum,123 planned) and all inference remain
**not launched**. It must verify the new standardized decoder/psi half-angle,
Torch/JAX pullback, full-mode NoLoop/JAX values/gradients, angular seam/lattice
representatives, physical support and time-integration certificates on fixed
cached/generated points, while counting every failed attempt and gradient.
Model solver acceptance does not establish any of those physical contracts.

The periodic actual-WTF loss still needs its semigroup term adapted and tested;
the old Euclidean-lift `model.wtf_loss` cannot be reused unchanged. The new metric
also prevents treating old lambda=.001 as equivalent regularization. Historical
angular prior mass8 requires the retained ln(8) normalization correction for fresh
matched evidence baselines. No calibrated posterior, speedup, reward-tuning result
or production adoption is claimed. PR445 and production code remain unchanged.
