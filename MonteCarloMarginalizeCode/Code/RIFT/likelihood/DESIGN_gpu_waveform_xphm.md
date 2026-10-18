# DESIGN: batched CuPy IMRPhenomXPHM, stage 1

Module: `gpu_waveform_xphm.py`. Test: `test/waveforms/test_gpu_waveform_xphm.py`.
Benchmark: `test/waveforms/benchmark_gpu_waveform_xphm.py`.

## Scope

Stage 1 ports IMRPhenomXAS (2,2) and the IMRPhenomXP NNLO twisting-up
(PrecVersion 102) from LALSimulation 6.2.0. It returns the five inertial (2,m)
modes for many intrinsic points at once, shape `(n, 5, N)`. The array module is
`cupy` or `numpy`. There is no JAX or Ripple dependency, so the module can be
backported to `rift_O4c`.

Only LAL defaults are implemented:

| Setting | Value |
|---|---|
| InsPhase / IntPhase / RDPhase | 104 / 105 / 105 |
| InsAmp / IntAmp / RDAmp | 103 / 104 / 103 |
| PrecVersion | 102 (NNLO angles, 3PN spinning L) |
| Convention | 1 |
| FinalSpinMod | 4, which LAL maps to 0 (chi_p final spin) for PrecVersion 102 |
| Tides, PNR, non-GR terms | off |

## Structure

The per-point setup runs on the host in NumPy. It covers the fits, the three
collocation solves (batched 4x4 and 5x5), the connection coefficients, the time
shift, and the Euler-angle offsets at f_ref. On a GPU this work was 74 ms of
kernel launches for any n. On the host it takes 4-6 ms. The coefficient arrays
are then copied to the device. The frequency-grid evaluation runs on `xp` with
shapes `(n, n_freq)`.

All three phase and amplitude regions are evaluated on the whole grid and
selected with `where`. This triples the arithmetic but avoids ragged arrays.

## Conventions

- `imrphenomxp_nnlo_modes` equals `SimInspiralChooseFDModes(IMRPhenomXPHM)` with
  PrecVersion 102, co-precessing ModeArray (2,+-2), ModeArrayJframe l=2 and
  `ThresholdMband=0`. These are J-frame modes on LAL's ascending two-sided grid
  `f = (k - n_freq + 1) df`, the ordering `compound_precompute_arrays` expects.
- `imrphenomxp_nnlo_polarizations` equals `SimInspiralChooseFDWaveform(IMRPhenomXP)`
  with PrecVersion 102, including the 2 zeta polarization rotation.
- Masses are in Msun and distance in metres. Spins are in the L0 source frame.
  Points with m1 <= m2 are relabelled as LAL does.
- `jframe_to_rift_lframe` applies the static rotation that `lalsimutils.hlmoft`
  applies with `fd_L_frame=True`. The Euler angles come from
  `lalsimutils.extract_JL_angles`, computed on the host by `rift_jl_angles`.

## Convention gaps

1. Conditioning is not applied. RIFT's ChooseFDModes path applies an FD
   high-pass taper, an inverse FFT, a sign flip, a centring roll, and TD tapers.
   These operations are linear and identical for every mode, so they commute
   with the L-frame rotation. A device version would reuse the helpers in
   `gpu_waveform.py`.
2. RIFT has no ChooseFDModes path for the `IMRPhenomXP` approximant. It builds XP
   modes from polarizations evaluated at five inclinations
   (`hlmoft_IMRPv2_dict`). The production equivalent of these modes is XPHM with
   PrecVersion 102 and only the (2,+-2) co-precessing modes.
3. Default XPHM multibands the (2,2) mode (`ThresholdMband=1e-3`). The reference
   here has multibanding off.
4. `extract_JL_angles` uses RIFT's own PN L. It is not ported to the device.
5. `lalsimutils.rotate_hlm_static` fails for FrequencySeries and ndarray input,
   because it references `lal.Complex16FrequencySeries`. The test wraps arrays
   in TimeSeries.

## Validation (2026-10-18)

50 random points: Mtot 10-150 Msun, q 0.1-1, |chi| <= 0.95 isotropic,
fmin = fref = 20 Hz, df = 1/16 Hz, fmax = 2048 Hz. Mismatch is maximized over
time and phase with the aLIGO zero-detuned high-power PSD.

| Comparison | max mismatch | max relative error |
|---|---|---|
| J-frame (2,m) modes vs ChooseFDModes | 1.2e-15 | 6.4e-12 |
| h+, hx vs ChooseFDWaveform | 7.8e-16 | 6.4e-12 |
| aligned-spin h22 vs SimIMRPhenomXASGenerateFD (20 points) | 8.9e-16 | 2.1e-12 |
| CuPy vs NumPy modes (pcdev11) | | 6.6e-12 of max mode |

A mismatch of 1e-15 is the round-off floor of 1 - match. CuPy and NumPy differ
at 1e-11 because their exp and sin differ at phases of order 1e4 rad.

Throughput, modes only, f_min = 20 Hz, ldas-pcdev11 (RTX PRO 4000 Blackwell;
Xeon E5-2630 v4, one thread):

| Grid | n | CuPy pts/s | NumPy pts/s | LAL pts/s |
|---|---|---|---|---|
| 8 s, 2048 Hz | 1 | 67 | 76 | 66 |
| 8 s, 2048 Hz | 10 | 642 | 98 | 66 |
| 8 s, 2048 Hz | 100 | 5160 | 113 | 66 |
| 32 s, 4096 Hz | 1 | 63 | 13 | 14 |
| 32 s, 4096 Hz | 10 | 607 | 13 | 13 |
| 32 s, 4096 Hz | 100 | 805 | 11 | 12 |

The CuPy floor of about 15 ms per call is the host setup plus kernel launches.

## Next stages

2. MSA angles (PrecVersion 223, the XPHM default) and FinalSpinMod 3.
3. XHM higher modes (21, 33, 32 with mode mixing, 44) twisted with the same
   Wigner-d machinery, with the mprime-dependent angle offsets.

Later: device-side RIFT conditioning and `extract_JL_angles`, and fusing the
grid evaluation into fewer kernels.

## Licence

The module is a translation of GPL-2.0-or-later LALSuite code, so it carries
that licence. The rest of RIFT is MIT. The maintainer should confirm that this
is acceptable before merge.
