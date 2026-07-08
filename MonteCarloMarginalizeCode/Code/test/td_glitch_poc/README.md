# td_glitch_poc — glitch-marginalization proof of concept (G4/V2)

Real-blip glitch marginalization for the RIFT time-domain (Toeplitz) segment likelihood:
a **real** LIGO L1 O3a blip glitch, a **data-informed PCA basis** built from real blips
(a la glitschen, Merritt et al. arXiv:2108.12044 — basis modes 3/4 of
`GLITCH_MARG_DESIGN.md` Sec. 3), and a synthetic 35+35 BBH whose merger track **overlaps**
the blip in time-frequency in one detector.  Three-way analysis through the genuine ILE
NoLoop path: glitch-free reference / unmitigated / analytically glitch-marginalized
(`--glitch-basis-file`).

Results, report, and figures live in the companion notes repo under `analyses/g4_poc/`.

## Pipeline (all steps deterministic given the seeds baked into the defaults)

1. **`fetch_blips.py`** — corpus acquisition.  Gravity Spy O3a machine-learning
   classifications (Glanzer et al., Zenodo record 5649212, `L1_O3a.csv` — download
   separately, ~140 MB) supply GPS times of high-confidence L1 blips; open strain is
   fetched from GWOSC with gwpy (grouped by 4096 s file to minimize transfer).
   Conditioning per instance: 32 s @ 4096 Hz → resample 2048 Hz → whiten (median PSD,
   4 s FFT) → band-pass 25–900 Hz → crop ±0.125 s around the peak → sign-align →
   unit-norm.  Output `blip_corpus.npz` (whitened domain, the glitschen convention).
2. **`build_pca.py`** — uncentered SVD of the training snippets (central 64-sample
   window; the window sweep is documented in the CLI help), `Lambda_0 = s^2/max(s^2)`,
   rank by ≥90 % captured energy, and held-out reconstruction mismatches **with the
   per-instance whitened-noise-floor expectation** (real snippets contain real detector
   noise; a perfect basis still leaves the noise share in the residual).
3. **`make_scene.py`** — reference frames (native_inject 35+35, fixed noise seed) +
   glitchy frames (same realization + a REAL held-out blip in L1, colored to strain by
   `sqrt(S_demo(f))`, scaled to the target whitened SNR, centred at merger+5 ms) +
   `glitch_basis_L1.npz` (`--glitch-basis-file` contract: basis/lambda0/t0/fs; the basis
   columns are colored by the SAME operation, so basis↔glitch consistency is exact and
   `Lambda_0` carries over unchanged — the coloring is linear).
4. **`run_three_way.sh`** — one batched-ILE grid evaluation per configuration with the
   production conventions (window-shape 0.1, AV sampler, analytic distance+phase
   marginalization, NoLoop `--gpu --vectorized --force-xpy`, `td-toeplitz`).
5. **`diag_uprime.py`** — the scalar-U' honesty diagnostic: fraction of template power
   absorbed by the glitch subspace as the template slides across the ±0.15 s
   marginalization window (the exact time dependence the G3 scalar-U' path neglects).
6. **`make_figures.py`** — money numbers (`results_summary.json`) + the three
   publication figures (Q-scan strip, posterior contours, g_hat vs injected blip).

## Example (paths relative to a scratch workdir)

```sh
python fetch_blips.py --catalog L1_O3a.csv --outdir work
python build_pca.py   --corpus work/blip_corpus.npz --outdir work
python make_scene.py  --pca work/blip_pca_white.npz --outdir work --dist 600 --blip-snr 25
# PSD + grid (once):
python ../td_equivalence/make_psd_ascii.py --out work/psd_smooth_ascii.txt
(cd work && convert_psd_ascii2xml --fname-psd-ascii psd_smooth_ascii.txt --ifo H1 --conventional-postfix
           convert_psd_ascii2xml --fname-psd-ascii psd_smooth_ascii.txt --ifo L1 --conventional-postfix)
python ../td_ringdown_demo/build_grid.py --out work/grid.xml.gz --nmc 7 --neta 5 \
    --mc-lo 28.5 --mc-hi 32.5 --eta-lo 0.20 --eta-hi 0.25
# three-way:
bash run_three_way.sh ref   work work/frames_ref.cache    work/run_ref
bash run_three_way.sh unmit work work/frames_glitch.cache work/run_unmit
bash run_three_way.sh marg  work work/frames_glitch.cache work/run_marg \
     --glitch-basis-file L1:work/glitch_basis_L1.npz
python diag_uprime.py  --workdir work
python make_figures.py --workdir work --outdir <report dir>
```

## What is committed here

Scripts only.  The corpus, frames, PSD/grid xml, ILE outputs and figures are regenerable
and live outside the repo (see `.gitignore`).  The `--glitch-basis-file` ILE option and
the (N, r) matrix support in the FFT Toeplitz matvec added for this PoC are regression-
tested in `../test_td_glitch.py::test_basis_file_matches_direct_attach` (defaults
bit-identical).
