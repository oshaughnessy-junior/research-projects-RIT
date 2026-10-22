# Experimental finite-covariance TD likelihood

Select `--internal-ile-inner-product td-gs` with all four maintained likelihood flags:
`--time-marginalization --vectorized --gpu --force-xpy`.
Install the optional public dependencies with `pip install 'RIFT[td-gs]'` (Python 3.11+).
tdanalysis has its own noncommercial license and additional terms; see
https://pypi.org/project/tdanalysis/ . It is an optional external package.
The fast prepared-kernel adapter relies on the internal kernel layout rather
than a stable public prepared-state API. It is pinned to tdanalysis 1.0.0 and checks its public
apply result before use. Base RIFT and the existing FD/TD-Toeplitz routes are unchanged.

This route reads unwindowed time-domain strain and generates untapered native TD
waveform modes. It inverts a finite Toeplitz covariance on each fixed data piece,
and evaluates Q(t), U(t), V(t) on one common grid. Use explicit `--interpolate-time nearest`. Cubic/sinc interpolation is refused
because interpolating self terms can produce negative norms. Optional `--td-u-svd-tol` compresses
the self-term series; dropped components introduce an explicit approximation.
`--td-u-stride` must be one; interpolating self terms does not preserve positivity.

`--td-piece START,END` uses units of `--td-mref` solar masses relative to the event;
`--td-ifo-time-offset IFO:SECONDS` moves that reference in each detector.
Multiple disjoint pieces use independent covariance blocks: correlations between
pieces are omitted. This is a modeling assumption, not the full retained-sample
covariance. Omit pieces to use the full data segment. `--td-acf-file IFO:PATH.npy`
loads an ACF with rows [lag seconds, covariance]; spacing must match the sample rate.
Without files, the PSD is converted with public tdanalysis/waveformtools and the
explicit conditioning target `--td-acf-cond`. Dense pieces use Cholesky; longer
pieces use GS after positive-innovation covariance validation.

Calibration, Gaussian glitches, analytic phase marginalization, FD-native/NR/ROM
waveform routes, rotation/frequency-response variants, refined time quadrature,
independent Q pregrids, and continuous time draws are refused. Orbital phase must
be sampled because a finite covariance generally gives a phase-dependent norm.
GS calibration and fractional-template placement are reserved for later work.
The data-only optional high-pass is a conditioning approximation: it does not also
transform the templates or covariance, and needs analysis-specific validation.

Finite covariance inversion and the existing band-limited precision-kernel
TD-Toeplitz route describe different noise models and need not give the same
posterior. Dense-reference algebra tests do not establish real-data coverage or
FD posterior equivalence. These likelihood ratios use a fixed noise model and
data selection; cross-selection absolute evidence requires matched normalization.
