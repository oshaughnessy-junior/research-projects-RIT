# EV-4 = GW230731_215307 — paper-2 WEAK/non-obvious O4a glitch (templated)

The robustness objective: a real but sub-dominant glitch that no competing analysis leads with.
Configs **U / MF / MP** (no public deglitched frames). See `event.env` and `../README.md`.

## Identity + rationale
Chosen from the GWTC-4.0 DQ Table 8 (arXiv:2508.18082, Appendix B). Of the eight glitch-mitigation
candidates, GW230731_215307 is the sweet spot: mitigation was **only an f_low raise 20→40 Hz in H1
alone** (no BayesWave subtraction) — the collaboration judged the glitch **sub-dominant**.
Single-detector, band-restriction-only mitigation = exactly "glitch present but not dominant".
Open O4a strain confirmed on GWOSC (both H1 and L1). GPS 1374875605.9; netSNR 12.2; detector-frame
Mc~9.5 / Mtot~22.3 / eta~0.24 (m1~13.4, m2~8.9). Low total mass → longer inspiral → 16 s segment.

**The demo raises NO f_low** (`FMIN=20`): the whole point is to marginalize the glitch analytically
rather than excise the 20–40 Hz band the collaboration cut.

## Residual uncertainty (flagged)
Table 8 gives f_low-raise rows only the cutoff — **no Gravity Spy class and no explicit TF window**.
The 20–40 Hz near-trigger H1 footprint is *consistent with* low-frequency scattering / low-freq blip
but is unconfirmed; the glitch GPS/window in `event.env` is **inferred** (trigger GPS + cutoff band).
The Gravity Spy **O4** catalog is not in Zenodo 5649212 (O1–O3 only), so the H1 basis uses an **O3b
Scattered_Light morphology proxy** — a documented approximation, `GLITCH_CLASS=Scattered_Light`.
Even-more-modest alternatives if a nearly-absent glitch is wanted: GW230803_033412 (f_low→30, H1) or
GW230601_224134 (f_low→20.93, H+L, near-trivial). Swap `TAG`/`GPS`/masses in `event.env` to switch.

## Status
Templated + validated to generate (same path as GW250114). `make basis` fetches the H1 O3b proxy
corpus (`H1_O3b.csv`, Zenodo 5649212); see GW191109/README for scattering-corpus notes.
