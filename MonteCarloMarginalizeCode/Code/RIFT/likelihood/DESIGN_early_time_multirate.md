# DESIGN: early-time multirate precompute (data-side weighting, DSWc)

**Status:** design only, 2026-10-08. No code until session A's combined DSWc plus
multibanded U, V run passes G1-G4 (Richard O'Shaughnessy, 2026-10-08). This file is a
record; it will be superseded by the implementation and its tests.
**Evidence:** RIFT_roboto_paper `analyses/early_time_compression/`: session A's
`RESULTS_2026-10-07b_review.md` (PR #216, `64573f9a`) and session B's `rift_interface/`
notes (PR #215). Base: `rift_O4d` at `615dc1bfa`. No default changes.

## Variant

| Item | Choice | Evidence |
|---|---|---|
| Q, data side | `dbar = C^-1 d` once per job; early segment low-passed (200 dB FIR) and decimated to fs_e; final 100 s (400 s for CE at rho ~ 1000) at full rate | A: Verdict; Table E |
| C^-1 | RIFT's own inner-product weights, `InnerProduct.weights2side` (`lalsimutils.py:2354`). With `--inv-spec-trunc-time 0` these are 1/S; otherwise they are the truncated weights. Either way Q matches today's | |
| Data cut at f_max | RIFT's hard cut. Combined DSWc + multibanded U, V at 160 dB with the hard cut, rho 1000: bns_ce D1 1.55e-3, D3 4.4e-3; bns_o4 D1 1.73e-4, D3 7.1e-3 (32 s) / 3.4e-3 (64 s); hmedge_o4 D1 1.7e-5, D3 2.4e-4; peak-time shift <= 3e-9 sigma_t. Equal to the roll-off rows (A, 2026-10-08). 200 dB rows pending | A: combined runs 569269890 (`451e2115`) |
| Early template | RIFT's own call at `deltaT = 1/fs_e`, then cos^2 taper over the top quarter below fs_e/2 | A: Table C (emulated with IMRPhenomTHM); B: XHM pair agrees after a 30-38 Hz low-pass |
| Late template | full rate, short buffer, per-mode f_min (below) | B: `wf_pair_hm.py` |
| U, V | FD modes on a multibanded grid, 32 s margin | A: Table D |
| Template window | sigma_h >= 1 s | A: Verdict |

The full-rate lnL is unchanged. It is not converged in f_max either way; that is a separate
open item (A's `development/OPEN_fmax_convergence_full_rate.md`).

## Opt-in interface

- ILE flag `--early-time-multirate SCHEDULE.json`, default off. The schedule carries, per
  detector and segment: epoch, duration, rate, filter, valid interior, guard, owner rule,
  noise-weight convention, plus the frozen chirp-mass and t_c range and a hash.
- Module `RIFT/likelihood/factored_likelihood_multirate.py`, function
  `PrecomputeLikelihoodTermsMultirate(...)`. It takes the arguments of
  `PrecomputeLikelihoodTerms` (`factored_likelihood.py:540`) plus the schedule and the
  per-job data products, and returns the same tuple. Everything downstream, NoLoop
  included, is untouched.
- Dispatch at the precompute call (`integrate_likelihood_extrinsic_batchmode:3821`). Build
  the data products once, after the data and PSD load (`:1503`, `:1525`).
- Refuse, never fall through: calibration marginalization, `--freqresponse`, ROM basis, a
  TD-only approximant, an intrinsic point outside the frozen range, a waveform whose
  generator rejects the low rate, or in-plane spins (precession; see below).
- Log one line per job naming the schedule, its hash and the segments.

## Time convention

RIFT's FD branch (`lalsimutils.py:3846-3870`) rolls by `int(0.1 N)` samples but sets the
epoch from `0.9 N dt`. Sample k therefore sits at `epoch + (k - frac(0.1 N)) dt`, and pieces
of different N disagree by up to one sample (0.42 rad in (2,2) between 128 Hz and 4096 Hz).

The prototype puts every piece on the time convention of the equivalent full-rate
template: true sample times, then the full-rate offset `frac(0.1 N_full) dt_full` added
back. Then a one-segment schedule reproduces today's `rholms` exactly, and recovered t_c
behaves as today. Switching to true times would move t_c by 49 us at N = 2^25, 4096 Hz.
That needs Richard's decision after the separate t_c measurement reports.

## Per-mode start frequency for late templates

**Aligned spins only.** For a non-precessing source, mode (l, m) at time tau before the
peak has frequency about (m/2) f22(tau). RIFT's call starts every mode at the same f_min in
the mode's own frequency. For |m| > 2 that is earlier than a short buffer holds, so it wraps.
For |m| = 1 it starts too late.

For a precessing source this rule does not hold (R. O'Shaughnessy, 2026-10-08). Each
inertial-frame (l, m) mode mixes co-precessing components m' = -l..l, so it carries content
near (m'/2) f22 for several m' at once. No single f_min per inertial mode both covers the
m' = 1 part and keeps the m' = l part inside the buffer. One call covering both needs a
buffer about l^(8/3) times longer than tau_start (about 40x for l = 4). Candidates, none
tested:
- generate co-precessing-frame modes per m' with f_min = (m'/2) f22(tau_start), then rotate
  to the inertial frame with the precession angles over the late segment;
- generate the late template from a low f_min on a long buffer, then window it in time
  (no saving on the late piece).
Until one is designed and measured, the prototype refuses in-plane spins. The early-rate
bound uses m_max = l_max, which covers mode mixing. The precession-frequency spread needs
its own margin there.

- One call per |m| group: `f_min,lm = (m/2) f22(tau_start)`, inside RIFT's call with
  `fd_standoff_factor = 0.9` (the driver's value, `factored_likelihood.py:422`). tau_start
  is the late-segment start plus guard plus 100 s.
- Buffer: a power of two at least tau_start plus 2 s.
- Measured (B, 3G BNS, zero spin, XHM l <= 4, tau_start 600 s, 1024 s buffer): all modes
  agree with the early template across the guard to 1.1e-4 rad and 7e-5 in amplitude. With
  one f_min, |m| = 1, 3, 4 fail at order unity. Precessing sources were not tested.
- Cost: four late calls instead of one, all on the short buffer. The cost row's 0.064-0.107
  late fraction assumed one call; per-|m| calls must be measured.

## Slow rotation (path to G9)

The rotation bank (`factored_likelihood_with_rotation.py:380-463`) builds chi_a by applying
a time-derivative weight (order p) and a sidereal modulation exp(i n Omega t) to each mode.
Both are linear and local in time, so they commute with the segment partition. Each segment
builds its own chi_a on its own grid, with the same time reference. Q_a is computed per
segment as for a = 0. The 2|a|^2 U, V terms per detector go on the multibanded grid; the
modulation shifts frequency by n f_sidereal (~1e-5 Hz), well inside one coarse bin. Memory
and time then scale with |a| on the low-rate arrays, not on N. Not in the first prototype:
it refuses `--rotation-slow` until the static path passes.

## Stages measured

Per stage: elapsed time, peak RSS (`VmHWM`) and read bytes, using the existing profiling
wrapper extended to the new functions.

| Stage | When |
|---|---|
| dbar, low-pass and decimate, late segment cut | once per job |
| early modes at fs_e plus taper | per point |
| late modes, per-|m| f_min, short buffer | per point |
| U, V on the multibanded grid | per point |
| Q early (low-rate sub-phase inverse FFTs, lag window only) and Q late | per point |
| integration | per point, unchanged |

## Acceptance

1. A one-segment full-rate schedule reproduces today's `rholms`, `crossTerms` and
   `crossTermsV` to floating-point precision on identical inputs.
2. The two-rate schedule meets A's pre-registered gates (`TOLERANCES.json`): D1 <= 0.1 nats,
   D3 <= 0.01, D2 per the amended rule, at rho = 20, 100, 300 and 1000, three noise seeds.
   G4 also holds: the peak-time shift is at most 0.1 sigma_t. The expected scale is A's
   end-to-end D1 at rho = 1000 (160 dB): 1.55e-3 for CE BNS, 1.7e-4 for O4 BNS. The smaller
   DSW-only rows are not end-to-end figures.
3. Measured per-point time and peak RSS are compared with B's prediction (5.0-7.3x for the
   3G CE BNS). A gap of more than 2x is a finding, not a tuning target.
4. One end-to-end ILE run on a known injection, with the multirate path in force (log line
   present). A run that falls back to the full-rate path does not test it.
