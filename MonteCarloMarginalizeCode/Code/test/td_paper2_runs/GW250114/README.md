# GW250114_082203 — paper-2 glitch-marg workspace (SMOKE-TESTED, go/no-go PASSED)

The loudest demonstration event (netSNR 78.6) and the bilby-antiglitch head-to-head. Reuses the
paper-1 (g4_poc) real-blip infrastructure. Configs **U / MF / MP** (no public deglitched frames,
so no D). See `event.env` for all parameters and `../README.md` for the shared workflow.

## Glitch character

Per bilby-antiglitch (arXiv:2606.31304) GW250114 has **no strong non-Gaussian artifact** — a weak
low-frequency excess-power / small-vibration feature in L1. Its paper-2 job is **robustness**:
marginalization must not distort a clean, loud event. We attach a real L1-O3a **Blip** basis
(the g4_poc corpus, re-colored to the GW250114 L1 PSD near merger) — this reuses paper-1 infra and
exercises the full U/MF/MP path. If the antiglitch low-frequency model is preferred, swap
`GLITCH_CLASS`/band/window in `event.env` and rebuild.

## Data / basis provenance

- `fetch_data.sh`: H1+L1 GWOSC open strain, 16 s frames @ 2048 Hz (`GWOSC-STRAIN`), off-source
  median-Welch PSDs (df=0.125 Hz, ends 40 s before the event), combined into one multi-IFO
  `psd/combined-psd.xml.gz`. Coinc synthesized at the fiducial 35.95+35.95 Msun (Mc 31.3 / eta 0.25).
- `build_basis.sh`: reuses `~/rift_td/analyses/g4_poc/work/{blip_corpus,blip_pca_white}.npz`
  (43-blip real L1 O3a corpus, r=6 @ 90.5% energy), colors the whitened PCA to the **real GW250114
  L1 PSD**, and fits the population prior. The sign-alignment audit re-adopts **FULL** mode
  (ratio 3.05, 100th percentile of the arbitrary-sign null). `basis/glitch_basis_L1.npz` (flat,
  MF) and `basis/glitch_basis_L1_pop.npz` (FULL mu+Sigma_shape, MP) are committed (~50 KB each).

## Smoke-test result (2026-07-09, this workspace, go/no-go for P2-runs)

`make fetch && make basis && make build && make validate` end-to-end on REAL GW250114 data. The
generated MP `ILE.sub` carries `--internal-ile-inner-product td-toeplitz`, `--td-solver auto`,
`--glitch-basis-file L1:.../glitch_basis_L1_pop.npz`, `--glitch-sigma profile`, `--window-shape 0.1`
(inherited from `--data-tukey-window-time 0.4` / 8 s), `--approx IMRPhenomXPHM --l-max 4`, and
`transfer_input_files = .../glitch_basis_L1_pop.npz`.

One ILE point extracted from that `ILE.sub` (CPU `--force-xpy`, n-max 30000), banners:

```
[TD] Using time-domain Toeplitz inner product (solver=auto, ... glitch=['L1(file)']/sigma=profile)
[TD] inner-product factory: inherited data window_shape=0.1
[T6] [H1] rel_var=4.402e-16   [T6] [L1] rel_var=2.955e-16
[G3] L1 glitch basis attached: r=6, support=[5.7498,6.2498]s, file:glitch_basis_L1_pop.npz
     [FULL pop-prior: mu+Sigma_shape], sigma_g=9.991e-21, Delta_lnZ=145.7225
[G3] adding glitch Delta-lnZ = 145.723 to lnL_marg
[G3] [L1] glitch support vs template support: overlap=True; U' evaluated at reference time
     (time-indexed U'(t) deferred)   <-- the known arrival-frame follow-on (V2 item)
```

**Finite lnL = 2491.0** (real XPHM waveform, td-toeplitz kernel, FULL population glitch prior);
runtime ~18 s for one point on CPU. This is the go/no-go PASS for the whole P2-runs machinery.

## Notes for cluster submission
- `make validate` runs the BRANCH `bin/integrate_likelihood_extrinsic_batchmode` (the conda-installed
  RIFT predates the td/glitch flags). On the cluster the container's RIFT carries them — no override.
- `--internal-waveform-fd-L-frame` + XPHM is auto-added by the pipeline; the glitch path is
  waveform-agnostic (kernel-level), so HM/precession are fine.
