# OPEN: LISA likelihood passes host scalars to a GPU-defaulting helper

Recorded 2026-08-28 against `origin/rift_O4d` @ 1333a37a. Verified in a clean
worktree off that ref, not from the reporting session's memory.

Kept as a file because the chip carrying this task was lost twice by the chip
queue; see `session-teams` mechanic 2 (a pending chip is not durable storage).

## Symptom

    MonteCarloMarginalizeCode/Code/test/test_lisa_operational_synthetic.py::\
      test_synthetic_lisa_tdi_precompute_and_likelihood

fails on any host where `cupy` imports (observed on `ldas-pcdev13`,
`CUDA_VISIBLE_DEVICES=3`):

    test_lisa_operational_synthetic.py:131 -> _evaluate_lisa_lnL
    RIFT/likelihood/factored_likelihood_LISA.py:331
      SphericalHarmonicsVectorized(modes, inclination, -phi_ref)
    RIFT/likelihood/SphericalHarmonics_gpu.py  ->  cos_theta = xpy.cos(theta)
    cupy._core._kernel._preprocess_args raises

Confirmed **pre-existing**: reproduces on the unmodified committed file, and is
unrelated to any time-quadrature work.

## Cause

`SphericalHarmonicsVectorized` defaults `xpy` to cupy whenever cupy is
importable, while `factored_likelihood_LISA` hands it host (numpy/python)
`inclination` and `phi_ref`.

All four call sites in the tree, which is the part the original report got
slightly wrong -- there are **two** unguarded LISA lines, not one:

| site | passes `xpy`? |
|---|---|
| `factored_likelihood_LISA.py:331` | **no** |
| `factored_likelihood_LISA.py:332` | **no** |
| `factored_likelihood.py:2574` | yes, `xpy=xpy` threaded |
| `jax_ile/spherical.py:224` | yes, `xpy=np` explicit |

`jax_ile/spherical.py:224` is a direct precedent for the one-line fix. Separately,
`factored_likelihood_freqresponse.py:388` and `factored_likelihood_with_rotation.py:830`
pass `xpy=np` to `TimeDelayFromEarthCenter` for exactly this class of reason.

## Why CI never caught it

Two independent gaps, both real:

- **GitHub `lisa-check` does not run this file.** `.travis/test-lisa.sh` names 17
  LISA test files explicitly; `test_lisa_operational_synthetic.py` is not one of
  them. An unlisted test never runs.
- **GitLab does run it** (`.gitlab-ci.yml:92`) but its runner has no cupy, so the
  GPU branch is never taken.

A test that silently exercises only the CPU backend is how this stayed hidden.

## Decision needed

1. Does `factored_likelihood_LISA` pass `xpy=np` at both 331 and 332, or should
   `SphericalHarmonicsVectorized` infer its backend from its arguments? The
   second is the broader fix and affects every caller.
2. Make the LISA suite actually exercise a GPU host, or document why it cannot.
   Adding the file to `.travis/test-lisa.sh` fixes the listing gap but not the
   no-cupy gap.

## Environment

Use `/cvmfs/software.igwn.org/conda/envs/igwn/bin/python` (NOT `/usr/bin/python3`,
which is 3.6). `export PYTHONPATH=<worktree>/MonteCarloMarginalizeCode/Code` and
verify `RIFT.__file__` resolves into your own worktree; `export OMP_NUM_THREADS=1`.
Work in your own worktree off `origin/rift_O4d`; never run writing git commands in
a shared checkout.
