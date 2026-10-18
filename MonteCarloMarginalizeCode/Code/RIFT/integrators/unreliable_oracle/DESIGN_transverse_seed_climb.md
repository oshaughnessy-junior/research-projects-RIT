# Transverse-spin seed climb

Status: prototype (branch `transverse_seed_climb`, 2026-10-17). Off by default.

## Problem

The default initial grid is aligned-spin. `helper_LDG_Events.py` seeds only `s1x`,
at about 1e-5 (`RIFT/physics/teobresums_compat.py`, `initial_transverse_spin_range`).
For events with large in-plane spin the high-likelihood region is narrow in
(Mc, q, chi_eff) and shifted from the aligned-spin optimum. Points with random
in-plane spin rarely land on it. IR1 needed external Bilby bootstraps to place
points there (S250830bp, `~/LVK/IR1/s250830bp-high-transverse-bootstrap-20261010`).

## Method

`bin/util_TransverseSeedClimb.py` hillclimbs a fast likelihood and writes the
points it visits near the peak as a sim_inspiral grid.

**Chart.** x = (mu1, mu2, q, s2z, chi1_perp, phi1), optionally with
(chi2_perp, phi2). The (mu1, mu2) pair (`RIFT/misc/tools.py`) removes most of the
Mc-q-chi_eff correlation, so a climb in this chart moves along the narrow
direction at the right scale. A point outside the Kerr bound scores -inf.

**Objective** (`RIFT/likelihood/jax_ile/intrinsic_objective.py`). For each
intrinsic point:

1. the production precompute (`build_data_from_precompute`) builds Q, U, V from
   the lalsimulation modes of `--climb-approx` (default: the analysis `--approx`);
2. the JAX ILE kernel `fused_log_likelihood_distphipsimarg` marginalizes over
   time, distance, orbital phase and polarization;
3. the result is averaged over a fixed batch of (ra, dec, incl) drawn from an
   importance proposal: a BIC-selected Gaussian mixture in
   (ra, sin dec, cos incl), plus 10% prior.

The batch is fixed during a climb (common random numbers), so the objective is a
deterministic function of x. It is re-adapted before each climb. The per-point
arrays are jit arguments substituted into a template data object, so the kernel
compiles once per run. The stock wrappers close over one data object and would
recompile for every point.

**Climber** (`LadderClimbingOracle`, this directory). CMA-ES, with reflection at
box edges and wrapping of angles. It runs:

- free climbs from the best few points of the input grid, after scoring a subset
  of it;
- a ladder of climbs with chi1_perp pinned at fixed values (default
  0.2, 0.4, 0.6, 0.8). Rungs run outward from the free optimum. Each starts from
  its neighbour's best point, projected inside the Kerr bound at fixed (Mc, q).

The output is every climb point within `--delta-lnL` (default 10) of the best,
topped up with draws from each climb's final Gaussian. The ladder supplies
near-peak points at large chi1_perp with the in-plane orientation the data
prefer. Random seeding does not.

The seeds are proposals. ILE re-scores them, and the climb objective never enters
an evidence or posterior.

## Pipeline wiring

| Scheme | Option | Where it runs | Effect |
|---|---|---|---|
| (a) setup | `pseudo_pipe --transverse-seed-climb setup` | submit host, during setup, CPU unless a GPU is visible | appends seeds to `proposed-grid.xml.gz` |
| (b) DAG | `pseudo_pipe --transverse-seed-climb dag [--transverse-seed-climb-request-gpu]` | node `seed_climb.sub`, parent of every iteration-0 ILE job | appends seeds to `overlap-grid-0.xml.gz` |

Both take extra climber arguments through `--transverse-seed-climb-args`. The
climber reads data, PSD and waveform settings from `args_ile.txt`.

Scheme (b) uses the CEPP options `--seed-climb-exe`, `--seed-climb-args`,
`--seed-climb-n-max`, `--seed-climb-request-gpu` and `--seed-climb-request-memory`
(BasicIteration only). The iteration-0 ILE batches are sized at build time for
(input grid + `--seed-climb-n-max`) points. ILE exits cleanly on indices past
the end of the grid. The node is skipped under `--first-iteration-jumpstart`.
The sub file has no file transfer, so it needs the shared run directory
(CIT local pool on `/scratch`, as for the other non-worker jobs).

## Limits

- The climb model must have the same precession as the analysis model. On IR1
  PP event 005 (XPHM injection, q = 0.09, chi1 = 0.94), IMRPhenomPv2 scores the
  true parameters 38 nats below XPHM. A Pv2 climb would follow the wrong ridge.
- `--device-precompute gpu` reduces Q/U/V on the GPU with
  `gpu_precompute.compound_precompute_arrays` (A = 1), the reduction written for
  the rotating finite-response likelihood. The first point is also built on CPU
  and compared. lalsim mode generation stays on CPU and is the floor: about
  1.6 s (l_max 2) and 3.3 s (l_max 4) per point on S250830bp (32 s at 4096 Hz).
- `--mode-backend ripple` takes IMRPhenomXPHM modes from ripple 0.4.1
  (`RIFT/likelihood/jax_ile/ripple_modes.py`), so the objective runs on the
  accelerator. ripple's h+, hx match lalsim XPHM to overlap 0.999999. The modes
  are J-frame and unconditioned, which is valid only because the objective
  marginalizes orientation. On PP 005 (pcdev11) the objective agrees with the
  lalsim XPHM path to within 1.4 nats over a 7 to 46 nat range, with the same
  ranking of six test points. ripple is an optional dependency, for targeting
  and development; the dependency-free cupy port is PR #453.
- Default kernel settings (S = 256 sky points, 8 x 4 phase/polarization grid,
  24 distance nodes) are coarse on purpose: on PP 005 with ripple they cost
  0.33 s per point against 1.29 s at (512, 16 x 8, 48), move lnL by up to
  3 nats, and keep the ranking.
- Cost per point is the precompute plus the kernel. See the RESULTS note in
  RIFT_roboto_paper `analyses/transverse_seed_climb/` for timings.
- JAX on a CIT head node starts an XLA thread pool sized by the CPU count. With
  the 500-thread user cap, a second JAX process on a busy node fails with
  `pthread_create() failed`. Condor execute nodes have no such cap.
- The sky batch is small (256 to 512 points). For targeting this is enough; the
  objective's lnL can sit a few nats from the ILE value.
- Only BasicIteration has scheme (b). Other builders can use scheme (a).
- Open: on CPU (jax 0.9.0), `IntrinsicMarginalObjective.__call__` followed by
  `lnL_sky` on the same point once returned all-NaN rows; the same call with the
  precomputed data passed in never did (12 of 12 calls, both jax versions).
  Not yet isolated. Non-finite rows score as zero likelihood and are counted in
  `stats()['n_nonfinite']`.

## Tests

- `test/integrators/test_ladder_climber.py`: toy ridge, box reflection, draws.
- Real-data runs and their numbers are in the paper-repo RESULTS note.
