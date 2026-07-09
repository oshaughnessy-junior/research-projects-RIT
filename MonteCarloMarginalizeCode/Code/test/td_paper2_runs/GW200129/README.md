# GW200129_065458 — paper-2 glitch-marg workspace (templated; scattering basis)

The "subtraction schools" head-to-head; the precession claim turns on how the overlapping glitch
is subtracted. Configs **U / MF / MP / D**. See `event.env` and `../README.md`.

## Glitch character (Payne et al. arXiv:2206.11932; GWTC-3)
A loud **scattered-light** glitch in **L1**, ~20–50 Hz, in the ~0.5 s before merger, overlapping
the signal. Long-duration arch → 1.5 s corpus crop, 15–55 Hz band, 4 s strain support, 1 s PCA
window (`event.env`). Detector-frame Mc~32.1 / Mtot~74.7 / eta~0.24 (fiducial coinc only);
netSNR ~26.8.

## Status
Templated + validated to generate (same path as GW250114). `make basis` fetches a fresh
**Scattered_Light** corpus from Gravity Spy L1 O3b (Zenodo 5649212, `L1_O3b.csv`):
```sh
curl -L -o L1_O3b.csv 'https://zenodo.org/records/5649212/files/L1_O3b.csv?download=1'
# set CORPUS_CATALOG=<abs path> in event.env
```
See GW191109/README for the scattering-corpus notes (heavier fetch; COVARIANCE-ONLY more likely).

## D config (available — this is the headline comparison for GW200129)
Public **BayesWave-deglitched** L1 frames exist in the GWTC-3 glitch-modelling release
(**Zenodo 5546679**) and the dedicated Payne et al. release (**Zenodo 7259655**, "The curious case
of GW200129"). Download, set `DEGLITCH_CACHE`/`DEGLITCH_CHANNEL` in `event.env`, then
`make build-D`. Published PE samples: GWTC-3 release (**Zenodo 5546663**) + Payne 7259655.
