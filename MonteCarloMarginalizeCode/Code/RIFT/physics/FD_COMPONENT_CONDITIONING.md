# Conditioning a native FD angular basis

`lalsimutils.condition_fd_mode_components` applies the existing ChooseFDModes
conditioning operator to a dictionary of independently labelled FD components.
It performs no waveform generation or angular reconstruction. The caller owns
the physical spin/frame mapping and must resolve a default reference frequency
before generating at the lower `P.fmin * fd_standoff_factor` start.

The input is LAL's ascending, center-DC two-sided frequency grid. It is distinct
from RIFT's reversed `DataFourier` frequency-value convention. Every component
must have the same length, deltaF, f0 metadata and epoch. The length is `N` or
`N+1`, with `N = int(1 / (P.deltaT * P.deltaF))`; native LAL f0 metadata may be
zero even though the stored spectrum is centered on DC. Labels are arbitrary,
including `(ell, m, (kind, mprime, side))` for an extended angular basis.

```python
time_components = lalsimutils.condition_fd_mode_components(
    P, native_J_frequency_components,
    global_sign=+1,
    fd_standoff_factor=0.9,
    fd_alignment_postevent_time=2.0,
)
```

`global_sign` is required. `+1` preserves the native-J normalization used by a
direct angular reconstruction; `-1` reproduces RIFT's historical ChooseFDModes
global sign. The caller's angular coefficients must match that explicit choice.
The helper never applies a mode-dependent phase, static frame rotation,
polarization angle, equatorial-symmetry reconstruction or antisymmetric-mode
suppression.

The common operator follows the existing operation order: a symmetric frequency
turn-on from the standoff start to `P.fmin`, the N+1-to-N resize and paired
Nyquist endpoint removal, inverse FFT, explicit global sign, fixed buffer roll
and epoch, and the start/end time tapers. The end taper retains the existing
duration cap. Centering depends only on caller controls and grid dimensions;
it never fits a component peak. `no_condition=True` disables the frequency and
time tapers while retaining endpoint handling, transform, sign and fixed
centering/epoch. All output components share the same time grid.

When positive and negative frequency pieces have different angular
coefficients, split them in the **raw frequency basis before calling** this
operator. Multiplication by a time taper mixes frequencies, so splitting the
conditioned spectrum afterwards does not recover the separately conditioned
pieces. Keep each label through conditioning and reconstruct by a linear
combination afterwards. Compute a conjugate partner from the literal conjugate
of that same conditioned time component, retaining its label; do not replace it
by an assumed opposite-m symmetry relation.

Inputs and `P` are unchanged by default. `copy_inputs=False` explicitly transfers
ownership of the FD series for in-place conditioning and resizing. Existing
`hlmoft` uses that ownership policy and `global_sign=-1`, then performs its
existing frame conversion and output handling. Its production defaults and
normalizations are unchanged.

The regression suite covers complex linearity, order-independent labels,
source-state preservation, inconsistent grids, explicit sign and Nyquist
handling, and frequency leakage demonstrating why the support split must come
first. Branch-pinned bitwise waveform comparisons additionally verify the
existing Phenom FD paths against the pre-refactor source.
