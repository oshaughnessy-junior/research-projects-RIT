# GW191109_010717 — paper-2 glitch-marg workspace (templated; scattering basis)

GPBilby head-to-head + the spin/precession claim. Configs **U / MF / MP / D**. See `event.env`
and `../README.md`.

## Glitch character (arXiv:2409.03912; GWTC-3)
Slow **scattered-light** glitches in **L1 (and H1)** overlapping the inspiral; L1 footprint
~35–40 Hz, ~0.1–0.04 s pre-merger. Scattering arches are long (~2–2.5 s) so the corpus/basis use
a 1.5 s crop, 15–60 Hz band, 4 s strain support (`GLITCH_NOUT=8192`), 1 s PCA window — set in
`event.env`. Detector-frame Mc~59 / Mtot~140 / eta~0.24 (fiducial coinc only); netSNR ~17.3.

## Status
Templated + validated to generate. `make fetch` and `make build` use the same proven path as
GW250114. `make basis` fetches a fresh **Scattered_Light** corpus from Gravity Spy L1 O3b
(Zenodo 5649212, `L1_O3b.csv` ~216 MB) via `common/fetch_class.py`, then PPCA + population fit.
Download the CSV first:
```sh
curl -L -o L1_O3b.csv 'https://zenodo.org/records/5649212/files/L1_O3b.csv?download=1'
# then set CORPUS_CATALOG=<abs path to L1_O3b.csv> in event.env (or place it in this dir)
```
The scattering-corpus fetch is heavier than a blip corpus (long snippets, low band); expect the
audit to more often pick **COVARIANCE-ONLY** than blips did (scattering morphology is less
sign-coherent) — `population_fit.json` records the verdict.

## D config (available)
Public **BayesWave-deglitched** frames exist (GWTC-3 glitch-modelling release, **Zenodo 5546679**;
both H1 and L1 for this event). Download them, set `DEGLITCH_CACHE` (file:// cache) and
`DEGLITCH_CHANNEL` in `event.env`, then `make build-D` (classic fd-diagonal on the cleaned frames).
Published posterior samples for the h2h live in the GWTC-3 PE release (**Zenodo 5546663**) and the
GPBilby re-analysis (arXiv:2604.02018).
