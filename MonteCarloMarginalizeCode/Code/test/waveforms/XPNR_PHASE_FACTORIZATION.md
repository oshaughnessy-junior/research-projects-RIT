# Native XPNR source coordinates and intrinsic likelihood components

Candidate opt-in implementation: `RIFT/likelihood/xpnr_source_basis.py`.
The ordinary waveform path remains separate; NoLoop integration and its full
mixed-U/V likelihood regression are owned by the corresponding integration change.
No per-extrinsic waveform generation, fitted phase/time correction, removal of
antisymmetric modes, or disabled (3,2) mixing is part of this source basis.

## Exact setup and two essential controls

Inspected native LALSuite7.26.16 / LALSimulation6.2.1, source commit
548bc5710bea021239da56e9beff1feaae163956. RIFT diagnostic base is
e0b5f1a9383d10bd02afd511dc8f8e68ae1929fa, with the new opt-in source module.
The generic branch uses PrecVersion330, FinalSpinMod7, tuned angles and tuned
coprecessing strain, tunedCoprec33 off, ForceXHMAlignment off, antisymmetric
waveform on, and HMReleaseVersion122019. Reference20Hz, generation18Hz for
requested20Hz/standoff.9, deltaF1/16Hz, Nyquist1024Hz.

Declare the complete native coprecessing ModeArray explicitly:
(2,+/-2),(2,+/-1),(3,+/-3),(3,+/-2),(4,+/-4). ModeArrayJframe is distinct and selects
inertial modes. Native OneMode otherwise initializes integration bounds from its
inertial ell/|m|, while the polarization route populates the coprecessing array
first. Precession.c297-315 overwrites those bounds from an explicit ModeArray,
forcing the same M_MIN1/M_MAX4 for every inertial mode and the public PNR helper.
This removes the previous approximately1e-4 source-gauge covariance residual.
Wire a fresh LALValue per call; its SWIG object is not deepcopy/JSON compatible.
Store plain mode pairs in receipts.

Set **both** PhenomXHMThresholdMband=0 and PhenomXPHMThresholdMband=0.
PhenomXPHMMBandVersion=0 alone does not disable angle multibanding:
precession.c1226-1234 overwrites the version with1 when the PHM threshold is
nonzero. With explicit modes and both zero thresholds, actual-observer native
J-mode projection matches raw ChooseFDWaveform plus/cross at3.2e-16–4.3e-16
across the three generic fixtures. With the omitted PHM threshold, the residual
was approximately.0007–.0027 even in the symmetric component. The precision
basis explicitly uses the zero-threshold setup; native production defaults are
a separate declared control, not silently equivalent options.

## Native source-gauge derivation

Let old kappa use the physical native330 reference PN angular momentum and new
kappa use the PNR reference opening angle beta, the maximal-emission direction.
Compute old J with the native3PN L polynomial including spin-dependent L3/L5,
not an approximate nonspinning expression. Native rotations are
Rz(-kappa) Ry(-beta) Rz(-phiJ). The source observer is
N=(sin(i)cos(pi/2-phi),sin(i)sin(pi/2-phi),cos(i)); the LAL waveframe is
X=(-cos(i)sin(phi),-cos(i)cos(phi),sin(i)). Zeta is the angle of this rotated X in
the native convention1 P/Q basis. `MAX_TOL_ATAN=1e-15` and the native atan2tol
zero branch are retained at observer poles.

The native mode route initializes all analytic PN alpha coefficients to zero
(precession.c1120+). Get_alphaepsilon_atfref1192+,1597-1606 therefore initializes
alpha_offset=-alpha0_old=kappa_old-pi. Its SpinTaylor spline has gauge
alpha_intrinsic-kappa_old (4430-4432). The initial offset cancels the observer
term before building the PNR alpha spline. This differs from the separate public
angle wrappers at4188/5068, which must not be used as a mode-gauge oracle.
After PNR remapping, Delta(alpha_offset)=Delta(kappa_new).

AntisymmetricWaveform.c409-421,622-624 uses raw alpha plus the remapped offset
twice and explicitly adds zeta_polarization. With default convention1 phi0=0,
its analytic source coefficient is

    g_positive = exp(i [Delta zeta_new + 2 Delta kappa_new - Delta kappa_old])
    g_negative = conjugate(g_positive).

Deltas compare the target observer with generator inclination0,phase0. The
ordinary J-mode factor is exp(i m Delta kappa_new). Project with
Y_lm(thetaJN_new,0) and rotate polarization by exp(2i zeta_new). These are derived
geometrical coefficients; no fitted complex scalar or lag enters the basis.
At generator inclination0, kappa is phase independent and Delta zeta=-phase.
The original18 phase-covariance assertions are recovered to precision. With the
complete explicit setup, the36 generic observer points (both frequency sides)
now agree within5.7e-13 rather than the earlier approximate1e-4 helper-grid result.

## Physical PE-coordinate conversion

At fixed masses, spin magnitudes, tilts, phi12 and phiJL, converting another
PE thetaJN/phase to native Cartesian spins changes both in-plane spins by a
common Rz(delta); their z components and magnitudes remain fixed. Infer delta
from a well-resolved reference in-plane spin and verify the **other** spin and
all supplied intrinsic invariants. Reuse with changed masses/spin magnitudes,
z components, spin reference frequency, or non-common rotations is refused.
Native fref0 is resolved to the requested fmin before applying generation standoff. For ordinary RIFT's fixed
Cartesian intrinsic spins, delta=0, including when P_vec supplies scalar spins.

Native epsilon0=phiJ-pi shifts by delta. Each coprecessing mprime therefore needs
exp(-i mprime delta) at positive frequency and its conjugate at negative
frequency, in addition to the observer factors above. A single summed symmetric
inertial mode cannot carry this physical-PE dependence for several mprime.

Three intrinsic calls separate the required components while preserving the
full mode array and retaining native antisymmetric physics:

1. S0: convention1, phase0, antisymmetric contribution off.
2. Spi: convention6, phasepi, antisymmetric contribution off. Convention6 keeps
   the same alpha0/epsilon0 but preserves phi0=phiRef; it flips odd mprime.
3. H0: convention1, phase0, full antisymmetric waveform on.

S_even=(S0+Spi)/2; S_odd=S0-S_even. For ell2 these are S22/S21; for ell3 they are
S32/S33; ell4 is S44=S0. A22=H0-S0. Splitting each component's native frequency
support gives76 labels (ell,m,(kind,mprime,side)), kind S/A, side +/-1.
The decomposition sums to S0 exactly, avoiding antisymmetric leakage from
numerically recomputed spin-rotation Fourier samples. All remaining extrinsic
evaluation uses these fixed arrays and analytic coefficients.

## Conditioning and NoLoop API

`XPNRSourceBasis.generate_raw(P,Lmax)` returns native ascending two-sided LAL FD
series. ChooseFDModes labels this grid with f0=0 despite data ordering from
-Nyquist to+Nyquist; the physical frequency axis is reconstructed explicitly and
validated. Before invoking the shared conditioner, the source callback normalizes
its owned component metadata to f0=-Nyquist. Native f0=0 on the same array would
produce heterodyned TD metadata f0=+Nyquist and re-FFT metadata f0=0. The normalized
source instead has TD f0=0 and standard/conjugate RIFT FD f0=-Nyquist, matching the
data grid. This changes no array bytes, ordering, phase, epoch or normalization;
the legacy helper and ordinary waveform path are unchanged. Split positive/negative support **before** applying TD tapers, which
mix frequencies. Apply the shared `condition_fd_mode_components` identically to
all components with global_sign=+1. Existing hlmoft retains its historical sign.

The callable basis returns conditioned FD components and Fourier transforms of
their literal time conjugates. Conjugate series retain the SAME labels, with an
identity `basis_conjugation` map; no +/-m reflection or symmetry is assumed.
`angular_coefficients(labels,incl,phiref,xpy,l_max,P_vec)` is vectorized and does
no generation. Optional full Cartesian P_vec supports physical PE reconversion;
ordinary fixed-Cartesian batches require no coordinate conversion. Metadata
records reference distance, resolved fref, beta/phiJ, options and plain mode pairs.
Precompute must use its ordinary100Mpc normalization before source generation.

The once-per-intrinsic public SpinTaylor-angle preflight explicitly refuses
native silent fallback from requested330 to MSA223. It shares the declared full
ModeArray/integration bounds but is used only to detect fallback, not to supply
the mode-route alpha offset. Aligned/weak-precession branches are rejected by
this generic source basis and remain on their separately validated paths.

## Validation and the remaining native numerical envelope

`check_xpnr_source_basis.py` validates three generic spin configurations against
raw native LAL plus/cross and Bilby's lal_binary_black_hole. Four generic
inclination/phase points per case have fixed-Cartesian maximum residuals
5.72e-14,4.56e-14,1.22e-13. Four physical PE thetaJN/phase reconversions per case
have maximum residuals2.43e-5,1.45e-5,8.39e-6; Bilby/native are exactly equal in
all12 physical-PE controls. Both-spin rigid rotation holds within2.22e-16.
The PE residual is reported separately; no threshold is loosened to label it
precision agreement. CoarseFactor1 does not remove it.

Independent native `PhenomXPHMPrecModes=1` controls expose untwisted coprecessing
arrays before basis projection. For case0 delta=-.9, S21/S22/S33/S44 change only
approximately2e-10–1e-9 under a rigid spin rotation; native S32 changes9.13e-5
with exact constructed rotation, or4.34e-4 with PE-converted last-bit changes.
Thus the remaining PE envelope exists in native co-precessing(3,2), independently
of this basis or NoLoop. HM_internals.c2188-2213 reconstructs its mixed-mode phase
second derivative using step1e-7 and multiplier1e14, a concrete source of numerical
sensitivity; this is not yet established as the sole mechanism. Mode32 mixing
remains enabled. No empirical phase/amplitude correction is applied.

Exact old/new J observer poles and1e-12/1e-8 offsets are finite for fixed
Cartesian spins; near-pole discrepancies reach6.9e-7 from floating geometry.
Physical PE thetaJN0/pi remains finite within the measured native numerical
envelope. Pole behavior mirrors the native1e-15 atan2 guard.

Independent review additionally found two ill-conditioned frame domains which
this opt-in basis explicitly refuses, with no legacy fallback:

- Initial physical J nearly parallel/antiparallel to L: |sin(beta_old)|<1e-6,
  mass-weighted transverse J within the native atan2 zero tolerance, or |J|<1e-10.
  Large individual transverse spins can cancel, so an individual-spin test is
  insufficient. The m60/30,s1(.1,.1,.2),s2(-.4,-.4+epsilon,-.1) family triggered
  large native/basis errors for epsilon0,1e-15,1e-12 before the guard. Near the
  supported boundary beta_old1.2e-6,3.6e-6,1.2e-5, fixed-Cartesian controls stayed
  within7.4e-14 and ordinary PE controls within3.6e-5.
- A nonzero physical PE spin rotation with |sin(thetaJN_new)|<1e-6. Native beta_new
  changes about1.1e-9 under a rigid spin rotation in the near-cancellation cases;
  at the observer pole this moves kappa/zeta across a singular gauge branch and
  produced approximately3.36% normalized errors. Ninety independent offset
  controls (three old tilts, two rotations, five directions, three radii) gave
  maximum joint residuals1.88e-4 at radius1e-7,2.26e-5 at1e-6,1.26e-5 at1e-5.
  The1e-6 PE observer exclusion therefore has a measured boundary. Fixed-Cartesian
  delta0 poles remain supported; no per-point native regeneration is introduced.

A separate pure-LAL continuity control makes the upstream AS issue explicit:
at fixed Cartesian spins and inclinations beta_PNR+/-1e-7 on a common continuous
LAL source waveframe, symmetric-only polarizations change6.92e-8,3.82e-8,8.45e-8
across the three generic fixtures, whereas full antisymmetric polarizations
change7.24%,9.93%,8.57%. This discontinuity predates RIFT and NoLoop. The input
regression must remain visible; symmetrizing away the antisymmetric strain is
not a correction. The guarded intrinsic bank reproduces the declared native
model on its validated frame domain rather than hiding this upstream defect.

Projection and shared conditioning commute to6.57e-16; literal time-conjugate
components round-trip to4.89e-16, with all76 epochs equal to-14s in the exercised
16s buffer/postevent2s setup.

Fast contracts in `test/test_xpnr_source_basis.py` also verify source-only frequency
metadata normalization against byte-exact standard/conjugate pre-normalization
replays, with and without conditioning. They cover initialization, scalar
spin broadcasting, complete common-spin rotation, intrinsic-reuse refusals and
pole handling (25 tests passed). `check_xpnr_phase_covariance.py` retains18 native
phase controls. Final direct-bin/cubic NoLoop likelihood tests must include all
mixed S/A and mprime U,V terms, true conjugates, varied time/distance/polarization,
detector caching, and reject unsupported analytic phase marginalization.

The raw reflected-mode parity issue is separate: Linux/macOS native XPNR(3,2)
receipts previously differed, while default aligned XPHM(2,1) asymmetry appeared
on both platforms. Those raw input-model tests must remain visible; these
source-coordinate corrections do not justify masking their failures.

The independent raw-native reproducer
`check_native_xpnr_coprec_rotation.py --assert-precision --output /tmp/native.json`
imports only NumPy/LAL/LALSimulation. Its deterministic Rz(-.9) controls retain all
mode32 mixing and expose coS32 residuals3.13944e-4,5.64428e-5,5.41910e-5 while
coS21/22/33/44 remain below9.78e-10. The1e-7 precision contract intentionally fails
on the tested native build; this belongs to the visible input-model checks, not
to a masked or xfailed production RIFT regression.

### Exact equal-mass native interface ordering

The precision basis compensates a native interface inconsistency only when
`m1 == m2`, by exchanging the two **complete** Cartesian spin argument triples
at `SimInspiralChooseFDModes`. Stored physical parameters, reference-frame
geometry, public angle queries, preflight, and PE spin-rotation validation retain
the original labels. No mass perturbation, fitted phase, or symmetry projection
is applied. The receipt records `equal_mass_mode_spin_swap`.

This follows the pinned LALSuite `548bc5710bea021239da56e9beff1feaae163956`
call graph. `LALSimIMRPhenomXUtilities.c:562` uses strict `m1 > m2` in
`XLALIMRPhenomXPCheckMassesAndSpins`, exchanging spins at equality.
`LALSimIMRPhenomX_internals.c:341` instead uses `m1 >= m2`, preserving input
aligned spins at equality. The polarization entry in
`LALSimIMRPhenomXPHM.c:196–203,277` saves the original aligned spins before the
checker and initializes the waveform struct from those originals. The mode
entry `OneMode` at `2324,2403` initializes that struct from the already checked
spins; `XPHMModes:3487` calls this entry for each mode. Thus the two interfaces
assign opposite aligned-spin labels at exact equality. The full-spin exchange
at the mode interface restores the polarization convention; unequal masses use
the original argument order.

The regression compares both symmetric and full asymmetric polarization sectors
against independent native `ChooseFDWaveform`, at exact equal mass with reference
frequencies 20/30 Hz, and a nearby unequal-mass control. It includes changed
inclinations/phases and common physical PE spin rotations. Fixed-Cartesian
controls require relative residual below `1e-9`; rotated controls require
`1e-4`, retaining the previously documented native numerical covariance envelope.
Independent review also checked 28 mode-only compensation controls: fixed-input
maximum residual `3.90e-13`, rotated maximum `4.685e-5`.

Reference observer angles are cached as Python floats, avoiding host NumPy
zero-dimensional arrays in the CuPy callback. An optional CUDA regression uses
all actual native components and PE-rotated device spin fields. For supplied
`P_vec.fref == 0`, the effective reference frequency is the supplied `P_vec.fmin`
when present; changing that effective frequency refuses reuse of the intrinsic
bank. These checks preserve the precomputed reference spin frequency.
