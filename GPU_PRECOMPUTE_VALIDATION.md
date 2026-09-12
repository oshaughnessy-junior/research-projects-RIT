# GPU compound precompute: implementation and validation

Status: OPEN, 2026-09-12. Internal implementation requested by Richard;
no PR merge authorized. Base: c14c1fbc3c2c05ef1f4b228404443a8bb9841769.

## Question and failure criteria

Can a GPU construct the compound response bank with the same numerical
likelihood as the LAL reference, while reusing detector inputs across intrinsic
points? Per Richard's correction, all executable tests use tiny synthetic
inputs or short BBH waveforms. No full BNS benchmark is authorized for this
validation stage; the long-grid memory bound is analytic only.

Fail if frequency ordering, Fourier normalization, complex conjugation, epoch,
retained-time window, detector weights, or waveform conditioning changes the
likelihood; if mutated inputs reuse stale cache entries; if unsupported physics
silently falls back; or if the long case exceeds device memory.

## Fixed diagnostics

- D1: Q, U, V maximum absolute and scale-relative differences against LAL.
- D2: pointwise downstream log-likelihood differences, including shifted times
  and nontrivial phases; both near the signal and across the prior.
- D3: sequential intrinsic points and changed detector/PSD/grid inputs, compared
  to fresh contexts; count data uploads and record retained bytes.
- D4: finite inputs/output and explicit rejection of unsupported configurations.
  Review (do not benchmark) the memory bound for the eventual long grid.
- D5: synchronized process runtime for initial setup, waveform, basis, Q, U/V,
  transfer of compact results, and subsequent intrinsic points. Container
  transfer and queue turnaround are excluded.
- D6: if AV integration is run, log weights, n-eff=300, n-max=800000,
  n-chunk=20000 initially; output only bounded fairdraw (200). Report achieved
  ESS, collapse state, prior-edge contact, and seed variation. A failed
  convergence check is not a posterior result.

## Method

Independent adversarial tests use small synthetic arrays and LAL as oracle;
short physical-waveform runs use frozen snapshots outside the repository. First validate overlap
operations using identical waveform modes. Then validate the Ripple waveform
adapter including the conditioning needed by the production RIFT convention.
Use multiple intrinsic evaluations in one worker. Every measured snapshot is
identified by a source hash. CPU reference and GPU use matching physical inputs.

## Results and verdict

- Clean container, user-site disabled: 19 CPU tests passed, 18 GPU cases
  deselected. Includes independent LAL FFT normalization, full maintained
  CPU precompute/packing/downstream likelihood, and streamed V reduction.
- Short physical harness (30+25 Msun, df=0.5 Hz, N=2048, H1/L1/V1,
  A=40, K=2), NumPy backend, two nearby intrinsic points: pointwise maximum
  absolute lnL differences 1.2333e-9 and 7.8353e-10. Nine cached input arrays
  copied initially; second point added zero copies (nine cache hits).
  Reference times 9.2616/2.1810 s; candidate NumPy times 0.2374/0.2250 s.
  These are harness observations, not replicated performance estimates and
  emphatically not GPU speedups. First reference includes lazy startup.
- GPU snapshot01 c1a6cd17db7fda9459187d3565e5aa597f6e86888fdfcff80f768e4b75cc49a5:
  infrastructure failure before tests (worker lacked pytest).
- GPU snapshot02 c00aac0cd29bc0ed8d96e2df33d7de5ee240d705573db979e31e21407799dcf5:
  35 tests passed, two failed writing the default CuPy kernel cache (ENOSPC).
  The benchmark failed at the same cache-write boundary, not an array mismatch.
  Dependencies archived separately, SHA256
  83e8c50e5f683380d953af38ed40456aa31e0247ab4124040e31072a84932248.
- Snapshot03 AV submission failed before ILE because /scratch was not bound
  inside the container. Corrected launch uses an explicit /results bind and
  independent writable per-job CuPy/Numba/CUDA/temp directories.
- GPU snapshot04 49a3382f54bc0b9cf41099d034e8114be15bd277d9f8bfcd913d8926cb523326:
  42 tests passed on a real GPU (60769855.0, exit 0). Five-point short BBH
  benchmark also exited 0 (60769855.1), NVIDIA RTX PRO 4000 Blackwell SFF,
  3 detectors, A40/K2/N2048. Warm reference CPU times were
  [4.91756, 5.02197, 4.96536, 4.86751] s; warm GPU times were
  [0.57125, 1.03418, 0.57905, 0.58016] s. Medians 4.94146/0.57960 s.
  These are old-reference-vs-new-algorithm timings, not an isolated GPU
  hardware gain. First-use reference/GPU calls were 84.61696/45.28720 s.
  All five downstream lnL comparisons had max absolute difference <=1.188e-9.
  Nine input-array uploads initially (442368 bytes); none on later points,
  36 total cache hits, nine retained input arrays. This is NOT a BNS scaling
  measurement. Timers cover the precompute calls and synchronize device work;
  queue and container transfer are excluded.
- Final short CPU suite: 29 passed, 18 CUDA cases deselected, using explicit
  JAX_PLATFORM_NAME=cpu/JAX_PLATFORMS=cpu in the GPU-oriented image.
- Snapshot05 42d883faf574cc763f3e4492820df8417a6a12de50c2dcc2f5550f4d1c0bf7d0:
  adds finer FFT/Gram timing, final native-provider guards, same-worker batched
  NumPy comparison, and hardened AV output parsing; final GPU run 60769857
  completed with five matched intrinsic points and 47 passing tests (60769857).
  Same-worker warm medians were 4.60593 s reference CPU, 0.47379 s batched
  NumPy, 0.53046 s CuPy. Warm CuPy range 0.52167--0.93523 s. Thus the
  short-grid speedup over the original loop is predominantly algorithmic;
  there is no demonstrated incremental GPU gain at this N. Representative
  last-point per-detector device stages: primary basis 0.037 s, Q FFT 0.022 s,
  U Gram 0.0008 s, conjugate basis 0.072 s, V Gram 0.012 s. Fine-grained
  timings are nested inside the coarse stages; do not add both sets.
- Snapshot04 bounded AV run 60769856 FAILED convergence, correctly rejected
  by the runner: neff=1.2349/1.0055, collapsed=true at both intrinsic points.
  This failure is retained and is not a posterior result. The fixture was at
  200 Mpc with broad inherited bounds. Its software path executed, but that
  does not validate its integral. Snapshot06 uses 400 Mpc, sky truth +/-0.1
  rad, inclination/psi +/-0.2 rad, distance [300,500] Mpc and cubic time
  interpolation; convergence requirements unchanged.
- Snapshot06 d66150cafff35c584adfac571e04bf9cd1c98175ef94c7919e91cd41f3d15f43:
  explicit legacy-generator option forwarding, both mode-bank transfers,
  per-mode grid/epoch rejection and real TaylorF2 compatibility tests added;
  GPU test 60769858 passed all 52 tests. Corrected short AV 60769859
  returned neff=22.637 and 12.640, both finite and collapse=false. The old
  runner exited on its 300 threshold. The user subsequently set the test-run
  target to 20: event 0 meets it, event 1 remains below it. This is a smoke
  test, not a production posterior certification.
- Native Ripple adapter remains excluded from the production environment
  switch pending conditioning/epoch validation against the actual PhenomD
  LAL path, not the distinct ChooseFDModes conditioning path.

No full-BNS run or converged posterior claim has been made.

## Response-order budget and generic-mode validity

Read `paper/research_notes_paper1.tex` in full and the modulation, FD-precompute,
and generalized-response validation appendices of `paper/paper1_scaling3g.tex`
in the paper repository before selecting further response-order tests.
The exact compound response count is
`A = (P+1)*[(Q+2)*(P+5) + Q*(Q+1)]`, from the sum over `(b,p,n)`
with `w_0=2`, `w_(1+q)=q+2`, and `|n| <= w_b+p`.
For `(P,Q)=(0,0),(1,1),(2,2),(3,6)`, A is 10,40,102,424.
With K waveform modes and N frequency samples, the primary complex128 bank
alone uses `16*A*K*N` bytes; Q FFT work scales as `A*K*N*log(N)` and
U/V Gram work as `(A*K)^2*N`. Scratch space and other resident arrays are
additional. Streaming the conjugate bank does not remove the quadratic Gram
work. Thus increasing both response orders and Lmax blindly is prohibited in
the validation plan; budget using actual A, K and N first.

Response-order selection for production must use coherent omitted-waveform
U/V norms over the intended extrinsic support and a declared error budget,
not SNR alone, the highest frequency, or data-overlap Q arrays. The notes'
finite angular scans are estimates, not rigorous prior-wide certificates.
Any optional higher-reference-order selector also incurs that reference bank's
precompute cost, even when it selects a cheap production truncation.

Snapshot07 SHA256 `5412158f150081dec5127dbbf299b0349011456c3265da25953703820bcba373`
adds mode-label alignment for reordered conjugate dictionaries and a real short
precessing IMRPhenomXPHM test: 30+25 solar masses, fmin40 Hz, dt1/512 s,
df0.25 Hz, all 21 modes through l=4, p=Q=0, one H1 detector. The NumPy
comparison passes Q/U/V, epochs and downstream likelihood checks near and
away from the source. GPU correctness job 60769860 passed all 55 tests. This is not a
performance test or a certification of response truncation for long BNS.

Raw logs and snapshots: /scratch/richard.oshaughnessy/rift_gpu_precompute_20260912.

## Internal use (not merged)

Set `RIFT_GPU_PRECOMPUTE=1` in the worker to replace compound
rotation-plus-frequency-response precompute in conventional ILE or ILE-JAX.
CuPy is mandatory on this opt-in path; failure does not silently fall back.
The default still generates the conditioned base modes with LAL, then performs
compound basis FFTs and Q/U/V reduction on the GPU. The ordinary ILE driver
also computes its pre-existing ordinary bank; that separate overhead has not
been removed in this change.

Compatibility is load-bearing: the default calls the existing
`factored_likelihood.internal_hlm_generator(P, Lmax, **hlm_kwargs)` for every
intrinsic point and uploads BOTH its ordinary and conjugate mode dictionaries.
It does not restrict the default path to IMRPhenomD and does not require JAX or
Ripple. Existing waveform configuration is passed through. The numerical GPU
bank requires modes to share a frequency grid and epoch; it explicitly rejects
a mismatched legacy bank rather than silently shifting its modes. Native
generation is an optional, separate provider hook, not an automatic replacement.

`GPUPrecomputeContext` reuses detector data, response weights, and inverse PSD
arrays across intrinsic points. Content hashes invalidate modified input arrays;
old versions of a cache role are replaced. An optional timing callback reports
synchronized waveform, input preparation, basis, Q/U, V, and export durations.
The legacy return structure copies only compact Q windows and U/V to the host.
The direct API also offers device returns and a native-provider hook, but the
production environment switch refuses unvalidated native-waveform selection.

For integration tests use AV with internal log weights, multiple intrinsic
points, bounded n-max/n-eff, and save-samples only with fairdraw capped at 200.
All generated frames, grids, outputs, containers, dependency bundles, and logs
remain outside this source repository. The user subsequently authorized a draft
PR; no merge is authorized. End-to-end device residency is still in progress:
the current legacy wrapper exports Q/U/V to the host, while the direct device
return API avoids that export but is not yet connected to the ILE driver.
