# Optional source angular components in NoLoop

Status: opt-in source-component implementation with generic algebra tests and the
bounded native XPNR checks below. No waveform-model default is changed. Production
GPU validation and benchmarking remain separate merge requirements.

Some waveform models need more than one intrinsic component for each physical
`(ell,m)`. Relabeling those components with artificial `m` values changes their
inclination dependence and is incorrect. Instead the optional path retains
arbitrary immutable labels, for example `(ell,m,(mprime,sector))`, and supplies a
source-only angular coefficient for each label.

`PrecomputeLikelihoodTerms(..., source_basis_generator=generator,
ignore_threshold=None)` invokes `generator(P,Lmax)` after setting the ordinary
reference distance and frequency resolution. The generator returns FD component
dictionaries `(h, h_conj)`, where `h_conj[key]` is the Fourier transform of the
actual complex-conjugate time series. It must not infer conjugation from `-m`
parity. Source components must match the data's frequency origin as well as its
resolution and length. Native ChooseFDModes arrays may carry f0=0 metadata despite
ascending centered data. The XPNR source callback normalizes its owned native
component metadata to -Nyquist before shared conditioning, so the conditioned TD
series has f0=0 and both standard and true-conjugate re-FFTs have RIFT f0=-Nyquist.
This changes no waveform bytes or epoch and does not modify the legacy conditioner.
The existing Q/U/V precomputation then retains every component pair,
including mixed sectors and mixed co-precessing harmonics. Reflection-based mode
pruning is refused for this path.
`force_22_mode=True` is also refused: component selection must be explicit in
the supplied generator and its matching coefficient metadata.

`PackLikelihoodDataStructuresAsArrays(...,source_basis=True)` retains the ordered
labels as an immutable tuple. Its legacy conjugation-index return is `None`:
the component set need not close under a reflection relation. NoLoop consumes
the supplied U/V matrices and does not use that index return.

`DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(...,
source_angular_basis=coefficients)` calls
`coefficients(labels,incl,phiref,xpy=xpy,l_max=Lmax,P_vec=P_vec)`. The callback returns a
complex array of shape `(n_extrinsic,n_components)` on the requested numpy/cupy
backend. The full source parameter vector permits explicit PE-frame bookkeeping,
including broadcast spin coordinates when the PE conversion requires them.
A callback may close over fixed intrinsic metadata; it must not generate
a new waveform for each extrinsic sample. Coefficients are evaluated once for
each ordered label set per batch and shared across detectors. The ordinary
contractions apply without dropping S/A or A/A terms. Analytic phase
marginalization is refused because arbitrary component coefficients need not
have the supported dominant-mode phase dependence.
Coefficient outputs must already use the selected array backend and have real
or complex floating dtype, the required shape and finite entries. The finite
check transfers one explicit scalar boolean from a GPU per unique label set
per batch; it never transfers a coefficient array. Benchmark this additional
synchronization before a production merge. The absent-hook path pays none of
this validation or synchronization cost.

The present numerical domain is an ordinary uncalibrated likelihood (`n_cal=1`),
with cubic interpolation and no analytic phase marginalization. Calibration,
distance marginalization, rotating detector response, source-specific PE
coordinates and native waveform agreement are not established by the synthetic
suite. Required production GPU checks remain separate merge gates.

With no generator or angular callback, all legacy waveform and likelihood
arithmetic remains unchanged. The default mode arrays retain their historical
shape and conjugation lookup. Expanded labels without their explicit packing
or angular callback are refused.

## Verification

`test/test_source_angular_basis.py` projects overlapping general complex signals
in time, evaluates independent numpy rFFT bin likelihoods, and compares them
with cubic NoLoop over two detectors and varied inclination, orbital phase,
polarization, distance and non-grid times. These signals retain unequal positive
and negative frequency content. Dropping mixed U or V sectors separately causes
resolved likelihood errors; the controls do not pass accidentally on orthogonal
component spectra. CPU/GPU parity is checked when a CUDA device is available.

The historical-body replay is separate from the callback equality control:

```
python MonteCarloMarginalizeCode/Code/test/waveforms/check_source_basis_legacy.py --baseline-ref 9cb3057e8
```

Run from the repository root with its Code directory first on PYTHONPATH.
It AST-loads the independent baseline
NoLoop body from Git and asserts bitwise equality on identical legacy inputs.
The local replay over two detectors, four extrinsic samples and three time
points has maximum difference exactly zero. This replay is not a substitute
for the native model's PE-frame validation or required production GPU gates.

## Native XPNR scientific gate

`test/waveforms/check_xpnr_source_basis_likelihood.py` checks three generic-spin
sources and 76 components per source, with fixed Cartesian spins and independently
PE-reconverted thetaJN/orbital-phase coordinates. Each source exercises four
inclination/phase pairs, polarization and distance changes, and three fractional
time shifts: 216 likelihood points across H1 and L1. The shared conditioner splits
the frequency-side components before its time-domain taper, and retains every
mixed U/V term.

The independent physical oracle is native LAL FD polarization generation with
requested minimum frequency 20 Hz and analysis band 20--512 Hz; PE polarizations
are additionally checked against Bilby. Native generation at 18 Hz is recorded
separately to expose conditioning effects. It is not used to satisfy the physical
gate. The script requires waveform residual <=0.02, mismatch <=1e-4 and both
NoLoop/native20 and NoLoop/component-direct likelihood differences <=0.1 nat.
It saves all scientific failures before returning a failing exit status. No lag,
phase or amplitude is fitted and no antisymmetric component is removed.

At LALSuite 7.26.16 / native commit 548bc5710bea021239da56e9beff1feaae163956,
all 216 points pass with 4096 Hz sampling and cubic interpolation. The maximum
NoLoop/native20 difference is 0.089254 nat; the maximum NoLoop/component-direct
difference is 0.043602 nat. The largest polarization residual is 0.000714 and
mismatch 2.54e-7. The small remaining margin in the likelihood gate must remain
visible. Raw basis-component norm fractions verify nonzero antisymmetric
components; they are not detector-strain asymmetry fractions.

For reproducible validation, the receipt records package versions, the native
commit, fixture parameters, explicit mode/model settings, production module and
harness hashes, conditioning settings and use of the existing optional batched
cross-term precompute. The mathematical complete-pair computation is retained
on either cross-term path. This CPU result does not claim the production GPU,
calibration or analytic-marginalization gates have been met.
