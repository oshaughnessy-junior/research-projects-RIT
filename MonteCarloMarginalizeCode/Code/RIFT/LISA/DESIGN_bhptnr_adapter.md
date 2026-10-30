# BHPTNR native-mode adapter

This design record describes the optional finite native-mode interface. Dated
numerical results live in the Paper 5 analysis records and may be superseded.
The implementation and its runtime lock define what executes.

`BHPTNRAdapter` supports the pinned nonspinning, quasicircular
`BHPTNRSur1dq1e4` source. Its parameters are observed primary and secondary
solar masses and luminosity distance in Gpc. The mass ratio must lie within
[2.5, 10000]. Calibration is an explicit boolean constructor argument.
No spin, eccentricity, epoch or requested duration is accepted.

The caller supplies a source root, a prefetched data directory and a JSON
runtime lock. Construction checks the complete Python source roster and hashes,
the dataset size and SHA256, platform, Python ABI and dependency versions before
importing the model. Imports use that source root and data directory. Loaded
module paths and dependency distribution paths are checked. Native Python
namespace packages are permitted only when every namespace directory contains
files from the recorded dependency distribution. Originless execution modules
remain refused. Empty source namespace packages must resolve to their canonical
module directory inside the verified source root. This includes the uninitialized
optional GPR submodule; it supplies no code to this spline model. The constructor
restores its temporary Python path and environment settings. It does not load
the HDF5 model; the first evaluation does. Both import and evaluation refuse
upstream's implicit download path.

Use a dedicated process with one adapter. Preloaded surrogate modules are
refused because upstream stores the HDF5 fit data in a process-global cache.
Concurrent adapter initialization and external mutation of model globals are
unsupported. Source, data and dependency artifacts must remain immutable for
the process lifetime. The lock pins dependency versions and records loaded
package hashes; a numerical qualification remains specific to its tested native
runtime and cannot transfer to another binary with the same version string.

A runtime lock has exactly these fields:

```json
{
  "schema_version": 1,
  "source_revision": "403875df67ef4d18fad1da785d7436e7b933ccd1",
  "python_abi": "cpython-311",
  "system": "Linux",
  "architecture": "x86_64",
  "versions": {
    "numpy": "2.2.6", "scipy": "1.13.1",
    "h5py": "3.16.0", "gwtools": "1.2.0"
  }
}
```

The version values describe one historical runtime. Read the acceptance record
for the runtime qualified by a particular job. Missing or mismatched artifacts
raise before a model import. There is no optional dependency installation at
module import and no fallback to another surrogate.

`generate(BHPTNRParameters(...))` calls the native model with total mass in
solar masses, distance in Mpc, `mass_scale='M'`, `lmax=10`, `neg_modes=False`
and `mode_sum=False`. It requests all 25 available positive modes. The result
contains their complex strain arrays and the native times in seconds, without
an assigned observation epoch. Reflected partners remain the synthesis stage's
responsibility. Arrays are copied and marked read-only; the mode mapping is
read-only. Metadata records source/data/runtime identity, parameters, calibration,
mode roster, sample count, endpoints and emitted warnings.

Finite strictly increasing times, finite equal-shaped modes, the full mode roster
and a bounded sample count are required. The cap is checked on returned output;
it does not bound the upstream HDF5 loader's workspace. No interpolation,
extrapolation, padding, harmonic synthesis or response occurs in this adapter.
The finite surrogate does not provide arbitrary-duration EMRI observations.
Upstream calibration rescales amplitudes only through ell=5; higher modes retain
their raw amplitudes. Its warning is preserved. Physical waveform accuracy and
posterior accuracy require separate evidence.
