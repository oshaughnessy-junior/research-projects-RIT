PR 436 adversarial CuPy precompute review
======================================

Reviewed head: 8a157ff8d2441d9152b29bcb0144c516c19acd1c.
PR: https://github.com/oshaughnessy-junior/research-projects-RIT/pull/436

Repairs
-------

* Cached weighted data, multiband plans and device FFT/interpolation plans could
  survive changes in frequency limits, mode count, PSD/data contents, truncation,
  timing and CUDA device. Cache scope now fingerprints all data/PSD contents and
  metadata and includes the schedule and calculation parameters. Only one input
  context is retained, with keys independent of recycled Python object ids.
* XPHM's restricted co-precessing ModeArray failed when its default J-frame output
  requested an absent l. Restricting both arrays removed that exception but an
  independent oracle revealed a 3.0% change in (2,+/-1), even at zero spin. The final
  implementation retains XPHM's co-precessing inputs and restricts J-frame output
  only. It is checked against unrestricted waveform generation and the full-rate
  one-segment control, in addition to backend comparisons.
* An early/full rate ratio of one designed an interpolation filter at Nyquist,
  which scipy rejects. It now uses an identity interpolation filter.
* ILE precompute now follows its selected GPU/CPU backend. The new
  --early-time-multirate-backend numpy|cupy|reference option makes comparisons
  explicit. Explicit CuPy fails without a usable device; the reference path does
  not probe CuPy. A backend option without a schedule is rejected.
* Invalid schedules, incompatible grids and unsupported mode counts are rejected
  before waveform generation. More unsupported direct-API waveform options are
  refused instead of silently ignored.

Evidence and reproducibility
----------------------------

The new cache/backend regression subset against the original module produced six
failures: fMax, Lmax, in-place PSD, in-place data, inverse-spectrum truncation and
invalid backend. The other two cache cases (storage window and sky position)
passed on the original; they remain regression coverage, not claimed defects.

Local logs and source hashes are stored beside this report. The final local suite
passed 47 tests with one GPU-only skip (local-tests-final.txt). The final CIT source,
job descriptions, hardware records, pytest logs, ILE commands and outputs live at::

  /scratch/richard.oshaughnessy/pr436_adversarial_20261018/

The final GPU suites require a usable CuPy device through RIFT_CI_REQUIRE_GPU=1.
They cover 2048/4096/8192 Hz data, early rates 256/512 Hz, Lmax 2/3/4, XHM/XPHM,
aligned spins, chirp-mass range edges, nonzero lags and differing detectors/PSDs,
inverse-spectrum truncation, cubic interpolants, in-place input changes, and
complex correlation offsets at rate ratios 1/2/8. A mocked CUDA device switch
checks cache isolation; actual multi-GPU switching in one worker is not claimed.

The end-to-end harness uses the paper acceptance-4 H1/L1 zero-noise BNS frames,
PSD and two intrinsic grid points per process. Each architecture runs eight CLI
configurations: XHM l<=4 (CuPy, NumPy, reference, full-rate one-segment), XPHM l<=3
(CuPy, reference), and XHM l<=2 with 2 s inverse-spectrum truncation (CuPy, NumPy).
All integrations use the GPU NoLoop likelihood and AV sampling, seed 19, 4096
requested samples per point in chunks of 2048 (AV may round the actual count up), with saved sample XML and marginal results.
Matched extrinsic samples are checked for finite-status agreement and lnL parity.
Backend gates are 1e-10 on Q/U/V; saved XML lnL uses real_4, so CLI parity is gated
at 1e-4 nats. The full-rate approximation gate is 0.01 nats within 50 of the saved
maximum. These short integrations test wiring and numerical agreement; they do
not establish converged posterior or evidence estimates.

Limits
------

This remains an opt-in aligned-spin XHM/XPHM prototype. No precessing, calibration,
slow-rotation, frequency-response, NR or ROM support is implied. Hashing input
contents adds a linear host-memory read per intrinsic point; no new speedup claim
is made. XPHM's retained co-precessing inputs can cost more than XHM's restricted
calls. Production end-taper/model/frequency-cutoff questions remain separate from
CuPy arithmetic parity.

The complete final architecture targets are A100-SXM4-80GB (Ampere, CUDA 11.8
container) and RTX PRO 4000 Blackwell (CUDA 12.8 container). Pascal passed the
initial backend-parity check, but its final full round was retired because the
available slots were reserved for online CBC work. A full 4 GB GPU memory gate
is therefore not claimed. The A100 run selects one device explicitly using
CUDA_VISIBLE_DEVICES and APPTAINERENV_CUDA_VISIBLE_DEVICES; the Blackwell run
uses the GPU allocated by HTCondor.

Completed independent convergence check
---------------------------------------

A separate A100 ILE comparison pins sky, inclination, polarization and orbital
phase, samples distance and marginalizes arrival time. Both intrinsic points
reached n_eff >= 50 in the first 2048 draws: n_eff = 533.14 and 344.74. The
multirate/full-rate integrated log-likelihood differences were 9.43e-8 and
1.59e-7 nats. This validates a converged conditional integral, not convergence
of the unrestricted extrinsic posterior. Commands and outputs are under
final_ampere/pinned on CIT; the summary is cit-ampere/pinned-results.json.

On the local host, fingerprinting 41,943,048 bytes of H1 data and PSD for the
512 s / 4096 Hz fixture took 0.0264 s per warm call (median of six calls). This
is a host-specific overhead measurement, not an end-to-end speedup claim.

Completed GPU suites and test strengthening
------------------------------------------

A100: 48 passed, no skips (280.02 s). Blackwell: 48 passed, no skips
(567.58 s). The only warning was an upstream qnm/scipy deprecation. After those
full runs, the synthetic complex-correlation fixture was strengthened to place
its early template window inside its short random array. Previously that
fixture's early term was identically zero, although the physical BNS cases
already exercised the early path. All three changed cases were rerun on CPU,
A100 and a Blackwell development GPU and passed. The current test file's bytes
are recorded as final_edge_tests.py in cit-source.sha256; the earlier full-suite
test file is retained in the immutable CIT final_code snapshot. The physics
source and ILE executable were unchanged during this test-only strengthening.

A100's eight CLI cases passed. Each of the two intrinsic points had 204 matching
saved extrinsic samples per comparison. Backend differences in serialized lnL
were zero; full-rate versus multirate differed by at most 1.0e-6 nats. Backend
differences in integrated log likelihood were at most 1.14e-13 nats. These XML
samples are filtered retained samples, not all proposed draws and not a claim
of an unrestricted posterior fair draw. Use a fresh output directory when
reproducing the harness.

Final disposition
-----------------

Both GPU runners completed successfully. Blackwell's eight CLI cases also
passed, with 204 matching retained samples per intrinsic point in each
comparison. Its maximum backend difference in integrated log likelihood was
1.14e-13 nats; its maximum saved-sample full-rate/multirate difference
was 1e-06 nats. Serialized backend lnL differences were zero.

Totals: 48 full-suite tests per GPU architecture, followed by three strengthened
correlation rechecks per architecture; 16 broad-prior ILE processes covering
32 intrinsic-point evaluations; and two converged conditional ILE processes
covering four more evaluations. Local full-suite results: 47 passed, one GPU
skip, followed by three strengthened CPU correlation rechecks. No numerical
failures remain in this tested matrix. No unrestricted-posterior convergence
or full 4 GB memory certification is claimed.

Final Blackwell Condor cluster: 570156205. A100 ran on ldas-pcdev12 with an
explicit GPU UUID; the strengthened Blackwell edge recheck ran on ldas-pcdev13.
Earlier exploratory jobs were retired when their source snapshot was superseded.
All final physics source hashes match the local checkout.
