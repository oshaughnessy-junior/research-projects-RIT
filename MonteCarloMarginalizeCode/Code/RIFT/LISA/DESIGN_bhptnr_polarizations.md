# BHPTNR native polarization synthesis

`bhptnr_polarizations` is an optional mathematical layer above
`BHPTNRAdapter`. It consumes `BHPTNRModes`, checks its supported source/data
identity, physical parameters, units, calibration flag, mode roster, sample
count and endpoints, then copies its numerical inputs. It imports NumPy and
the adapter definitions without loading the external surrogate or its dataset.
The receipt checks establish consistency; they do not authenticate an arbitrary
caller-created object or reproduce the adapter's runtime qualification.

`source_polarizations(native, inclination=..., observer_azimuth=...)` sums all
25 positive modes and their fixed-plane reflection partners,
`h(l,-m)=(-1)^l conj(h(l,m))`. It uses spin-weight -2 spherical harmonics from
`exp(-i theta Jy)`, with ell from 2 through 10. The complex convention is
`h = hp - i hc`; the returned array has rows hp and hc. The native modes
already carry physical strain scaling. Synthesis applies no further mass or
distance scaling. The source is nonspinning and quasicircular; this reflection
rule does not define a generic precessing-waveform interface.

`ssb_polarizations(native, qS=..., phiS=..., qL=..., phiL=...,
orbital_phase_offset=...)` uses the explicit BHPWave source-angle convention.
The sky and orbital angular momentum have spherical polar angles qS and qL
in [0, pi] and azimuths phiS and phiL in [0, 2pi). The line-of-sight inclination
is the angle to `-n`. In the sky basis `(e_theta,e_phi)`, let
`z = L dot e_theta + i L dot e_phi`. Evaluate the inclination as
`atan2(abs(z), -n dot L)` to retain small offsets near alignment.
The spin rotation is `z/conj(z)`;
the source observer azimuth is `-pi/2 - orbital_phase_offset`. The phase offset
is periodic and does not align the intrinsic phases of independently generated
models. At a sky pole the supplied phiS fixes the basis. An orbital axis aligned
with the sky within 64 machine epsilons has no defined projected polarization
angle and raises; a future explicit-gauge interface would need separate tests.

Both functions preserve every native time sample. They assign no observation
epoch, interpolate no values and compute no detector response or likelihood.
`BHPTNRPolarizations` contains copied read-only float64 time and 2-by-N
polarization arrays and a deep-copied metadata receipt. The receipt preserves
native warnings and calibration information, records frame/angle conventions,
and hashes the normalized float64 times and complex128 input modes with their
byte order. Finite, strictly increasing times and finite modes are checked
again after conversion to output precision. Concurrent mutation of inputs while
synthesis executes is unsupported. Read-only NumPy flags discourage accidental
mutation; they are not an immutable storage boundary.

The implementation reuses the spectral harmonic construction and explicit
frame convention from the archived Paper 5 synthesis controller. Unit tests
use a separate finite Wigner sum for all ell/m values, quadrupole closed forms,
and a vector-basis SSB calculation. A small fixture retains selected historical
upstream `mode_sum=True` samples, with source artifact hashes and the original
full-array normalization peaks. Its builder accepts only those exact archives
and performs no waveform evaluation. This finite regression does not qualify
another runtime or establish physical waveform accuracy. Dated runtime and
response evidence remains in the paper analysis records. The native adapter,
FEW integration, shared response, CLI and waveform defaults are unchanged.
