# T7 -- ringdown / inspiral segment-likelihood science demonstration

Task T7 of `RIFT_TD_INTEGRATION_PLAN.md` (Sec. 6): first science-shaped run of the RIFT
time-domain **segment** likelihood in three template-frame configurations (full IMR /
inspiral-only / ringdown-only), through the real ILE Precompute + factored-likelihood path.
Full write-up + figures: `~/rift_td/analyses/t7_ringdown_demo/REPORT.md`.

Branch: `rift_O4d_tdlike_t7` (off `rift_O4d_tdlike`, LOCAL ONLY). No RIFT modules modified.
Env: `~/.conda/envs/junior_rift/bin/python`, `PYTHONPATH=<worktree>/MonteCarloMarginalizeCode/Code`.

## Injection (fiducial single event -- "GW250114-like")
m1=m2=50 Msun, IMRPhenomD, fmin 25 Hz, srate 2048, dist 700 Mpc, H1+L1, 8 s segment (N=2^14),
smooth analytic PSD (reused from ../td_equivalence). mchirp_true=43.53, eta_true=0.25.
peak+10M = 10*G*Mtot/c^3 = 10*4.925491e-6*100 = **4.9255e-3 s** (ringdown-only start offset).

## Scripts
| file | purpose |
|---|---|
| `build_grid.py` | build an intrinsic (mchirp x eta) sim-xml grid |
| `make_noise_frames.py` | add PSD-colored Gaussian noise to a zero-noise frame cache |
| `run_config.sh` | one batched-ILE NoLoop MC evaluation of the grid for one config |
| `run_deterministic_surfaces.py` | zero-MC-noise 3-config surfaces (T4 harness + distance profiling) |
| `pp_run.py` | reduced-scope ringdown-only pp-check (deterministic, per-injection noise) |
| `make_figures.py` | consistency figures + tables + pp-plot |

## Exact commands
```bash
PY=~/.conda/envs/junior_rift/bin/python
export PYTHONPATH=$(cd ../.. && pwd)

# 1. injection + zero-noise 32 s frames (H1,L1) + local cache
$PY ../../bin/util_WriteInjectionFile.py --parameter m1 --parameter-value 50 \
    --parameter m2 --parameter-value 50 --parameter dist --parameter-value 700 \
    --parameter fmin --parameter-value 25 --parameter incl --parameter-value 0.7 \
    --parameter phiref --parameter-value 1.2 --parameter theta --parameter-value 0.3 \
    --parameter phi --parameter-value 2.1 --parameter psi --parameter-value 0.8 \
    --parameter tref --parameter-value 1000000000 --approximant IMRPhenomD --fname inj_bbh50
mkdir -p frames32 && cd frames32
for ifo in H1 L1; do $PY ../../../bin/util_LALWriteFrame.py --inj ../inj_bbh50.xml.gz \
    --event 0 --instrument $ifo --srate 2048 --seglen 32 --start 999999984 --approx IMRPhenomD; done
cd ..
# build frames.cache with ABSOLUTE file:// paths pointing at frames32/*.gwf (durations vary by mass)

# 2. intrinsic grid (35 pts) + pp posterior grid
$PY build_grid.py --out grid.xml.gz --nmc 7 --neta 5 --mc-lo 41 --mc-hi 46 --eta-lo 0.20 --eta-hi 0.25
$PY build_grid.py --out ppgrid.xml.gz --nmc 9 --neta 3 --mc-lo 42 --mc-hi 45 --eta-lo 0.225 --eta-hi 0.25

# 3. MC full-extrinsic NoLoop surfaces, one batched ILE call per config (~200-300 s each)
./run_config.sh full     frames.cache grid.xml.gz 35 zn/full     1234
./run_config.sh ringdown frames.cache grid.xml.gz 35 zn/ringdown 1234 --td-analysis-start 0.0049255
./run_config.sh inspiral frames.cache grid.xml.gz 35 zn/inspiral 1234 --td-analysis-end -0.05

# 4. deterministic (zero-MC-noise) distance-profiled 3-config surfaces
$PY run_deterministic_surfaces.py --cache frames.cache --grid grid.xml.gz --dist 700 --out zn_det.json

# 5. reduced-scope ringdown-only pp-check
$PY pp_run.py --n-inj 6 --grid ppgrid.xml.gz --out pp --seed0 7000

# 6. figures + tables
$PY make_figures.py --zn-det zn_det.json --pp pp/pp_results.json
```

## .dat columns (run_config.sh output, per grid point)
`event_id m1 m2 s1x s1y s1z s2x s2y s2z  lnL_marg sigma ntotal neff`  (lnL_marg = col 10).

## Key numbers / caveats
- **Width ordering (deterministic, robust):** sigma(mchirp) full 0.011 < inspiral 0.178 <
  ringdown 0.480 (ringdown broadest -- as expected).
- **T6:** rel_var ~ 1e-16 (full), ~4-9e-20 (ringdown template-frame cut); nothing tripped.
- **guess_snr (truth grid pt):** full 11.6, inspiral 8.25, ringdown 4.15.
- **Absolute recovery is offset** (full/inspiral rail to opposite grid edges): an
  injection(`util_LALWriteFrame`)-vs-template(`hlmoft`) waveform-conditioning mismatch for
  these few-cycle signals -- a DEMO DATA artifact, not a TD-segment bug (TD==FD to ~1e-13 in
  T4). Follow-on: inject a RIFT-native waveform for a clean absolute/pp result. See REPORT.md.
- **MC full-extrinsic** runs end-to-end but neff~1-4 (pre-existing GMM limitation on this cheap
  PSD); the deterministic surfaces are the quantitative backbone.
- **Efficiency note:** N=2^14 == module `_MAX_DENSE_N`, so a fresh `TDInnerProductFactory` per
  grid point eagerly builds a 2 GB dense matrix. Reuse one factory per config, or `solver=pcg`.

Bulky/regenerable products (frames*, pt_*.dat, ILE logs) are gitignored.
