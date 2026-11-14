"""
RIFT.likelihood.hlm_transform
=============================

The extra-parameter waveform-transform seam (``--supplementary-waveform-transform-*``).

A grid row carries intrinsic parameters theta plus extra parameters lambda (first use:
lensing).  The template is

    h'_lm(f) = F(f; lambda, P) h_lm(f)

and because F changes the template norms it must act on the mode banks BEFORE the
inner products U = <h_lm|h_l'm'>, V = <h_lm^*|h_l'm'> and Q_lm(t) = <h_lm(t)|d> are built.
:func:`RIFT.likelihood.factored_likelihood.PrecomputeLikelihoodTerms` therefore calls the
seam immediately after ``internal_hlm_generator``.  Everything downstream (mode pruning,
U/V/Q, calibration self terms) sees the transformed banks; NoLoop's extrinsic algebra is
untouched, so F may depend on lambda and the intrinsic P and on NOTHING else (no sky, time,
psi, inclination, orbital phase or distance; detector- and mode-independent).

Plugin contract (a module named by file path or dotted name)::

    NAME = "point_lens"                         # optional, for logs
    EXTRA_PARAMETERS = ["lens_mass_z", "lens_y"] # optional; names of the grid columns used
    def prepare(config=None, extra_parameters=None, opts=None, **kw): ...   # optional, once
    def transfer_function(f, extra_params, P=None, xpy=numpy, **kw):
        # f: 1-D array of NON-NEGATIVE physical frequencies [Hz], standard sign convention
        #    H(f) = int h(t) exp(-2 pi i f t) dt, so a pure delay Delta is exp(-2 pi i f Delta).
        # extra_params: {name: float}.  P: a copy of the intrinsic ChooseWaveformParams.
        # returns a finite complex array of the same shape.
    def is_identity(extra_params): ...           # optional; True -> the seam skips the multiply

THE SEAM, NOT THE PLUGIN, BUILDS THE TWO-SIDED RESPONSE.  It evaluates the plugin on f >= 0
only, fills F(-f) = conj F(f), and takes the real part at DC and at the even-length Nyquist
bin.  F is therefore Hermitian (a real time-domain kernel), which is exactly the condition
under which the SAME array multiplies both ``hlms`` (FFT of h_lm(t)) and ``hlms_conj`` (FFT of
conj h_lm(t)): FFT[conj(K*h)](f) = conj(F(-f)) FFT[conj h](f) = F(f) FFT[conj h](f).

PHASE-SIGN TRAP.  The banks are LAL two-sided series from ``COMPLEX16TimeFreqFFT``: ascending,
physical frequency of bin k = ``deltaF (k - N//2)``.  ``lalsimutils.evaluate_fvals``' default
labelling (``deltaF (N/2 - k)``) is the NEGATIVE of that; using it would conjugate F, which
leaves every norm unchanged and moves a delay the wrong way.  This module builds f itself
from N and deltaF; the sign is pinned against a numpy time-domain reference in
``test/test_hlm_transform.py``.

The series' ``f0`` metadata is NOT used: it is the FFT-shift origin ``-floor(N/2) deltaF``
on the IMRPhenomD/SEOB routes, but 0 on the FD-model (IMRPhenomXHM/XPHM) route, whose TD
modes carry ``f0 = +fNyq`` into ``DataFourier``.  The bin layout is the same on both routes
(checked against the time-domain reference for IMRPhenomXHM in the test file).

The even-length Nyquist bin (``F[0]``) is ``Re F(fNyq)``: the plugin is evaluated there too,
and the bin is self-conjugate, so only its real part is Hermitian.

WRAP-AROUND.  Multiplying by F on the analysis grid is a CYCLIC convolution of h_lm(t) with
the kernel K(t).  A kernel with a long delay (a lensed second image, say) pushes template
energy past the end of the buffer and back to its start, with no error from the algebra.  For
each grid the seam therefore convolves the dominant mode with K linearly (zero padded) and
measures the fraction of the result's energy that falls outside the buffer.  Above
``WRAP_WARN_FRACTION`` it warns; above the tolerance (``WRAP_ERROR_FRACTION``, or the plugin's
``WRAP_TOLERANCE``) it raises :class:`WaveformTransformError`.  Lengthen the segment (smaller
deltaF) rather than raising the tolerance.

ERRORS.  Anything raised while binding or applying a transform (a plugin exception, a
non-finite or wrongly shaped response) is re-raised as :class:`WaveformTransformError`, which
ILE treats as fatal (nonzero exit) rather than as a per-point ``FAILED ANALYSIS``.
"""

import copy
import functools
import inspect
import warnings

import numpy

from RIFT.misc import plugin_loader

FLAG_CODE = "--supplementary-waveform-transform-code"
FLAG_INI = "--supplementary-waveform-transform-ini"
DEFAULT_FUNCTION = "transfer_function"


class WaveformTransformError(RuntimeError):
    """A failure inside the seam or the plugin.  ILE aborts the job on it."""


class WaveformTransform(object):
    """A loaded transfer-function plugin plus its declared extra-parameter names."""

    def __init__(self, transfer_function, extra_parameters=(), name=None, is_identity=None,
                 module=None, config=None):
        if not callable(transfer_function):
            raise TypeError("waveform transform: transfer function is not callable")
        self.transfer_function = transfer_function
        self.extra_parameters = tuple(extra_parameters)
        self.name = name or getattr(transfer_function, "__name__", "transfer_function")
        self.is_identity = is_identity
        self.module = module
        self.config = config
        try:
            self._accepts_xpy = "xpy" in inspect.signature(transfer_function).parameters or any(
                p.kind == p.VAR_KEYWORD for p in inspect.signature(transfer_function).parameters.values())
        except (TypeError, ValueError):
            self._accepts_xpy = True

    def identity_for(self, extra_params):
        return bool(self.is_identity is not None and self.is_identity(dict(extra_params)))

    def evaluate(self, f_nonneg, extra_params, P=None, xpy=numpy):
        # The plugin gets a COPY of P, so a plugin that writes to it cannot alter the template.
        if P is not None and hasattr(P, "manual_copy"):
            P = P.manual_copy()
        kw = {"P": P}
        if self._accepts_xpy:
            kw["xpy"] = xpy
        return self.transfer_function(f_nonneg, dict(extra_params), **kw)

    def bind(self, extra_params, xpy=numpy):
        """Callable ``(hlms, hlms_conj, P) -> (hlms, hlms_conj)`` for one grid point, for
        ``PrecomputeLikelihoodTerms(hlm_transform=...)``.  ``None`` if F is the identity at
        *extra_params* (declared by the plugin's ``is_identity``)."""
        try:
            identity = self.identity_for(extra_params)
        except Exception as exc:
            raise WaveformTransformError("waveform transform {}: is_identity failed at lambda={}: {!r}"
                                         .format(self.name, dict(extra_params), exc)) from exc
        if identity:
            return None
        return functools.partial(apply_hlm_transform, transform=self,
                                 extra_params=dict(extra_params), xpy=xpy)


def load_waveform_transform(code, function=None, ini=None, parameters=None, opts=None):
    """Load a plugin per the contract above and return a :class:`WaveformTransform`.

    The extra-parameter names are the union, in order, of the module's ``EXTRA_PARAMETERS``
    and *parameters* (``--supplementary-waveform-transform-parameter``).  ``prepare`` is called
    once, after the names are final."""
    module = plugin_loader.load_module_from_spec(code, flag=FLAG_CODE,
                                                 module_prefix="_rift_wf_transform_")
    function = function or DEFAULT_FUNCTION
    if not hasattr(module, function):
        raise AttributeError("{}: module {!r} has no function {!r}".format(FLAG_CODE, code, function))
    config = plugin_loader.parse_ini(ini, flag=FLAG_INI)
    names = []
    for n in list(getattr(module, "EXTRA_PARAMETERS", ()) or ()) + list(parameters or ()):
        if n not in names:
            names.append(n)
    prep = getattr(module, "prepare", None)
    if callable(prep):
        # a copy: the job's refusals are already checked and the plugin must not undo them
        prep(config=config, extra_parameters=list(names), opts=copy.copy(opts))
    return WaveformTransform(getattr(module, function), names,
                             name=getattr(module, "NAME", None) or function,
                             is_identity=getattr(module, "is_identity", None),
                             module=module, config=config)


def packed_frequencies(series):
    """Physical frequencies of a LAL two-sided (FFT-shifted) COMPLEX16FrequencySeries:
    ``deltaF (k - N//2)``, from N and deltaF only (``series.f0`` is not reliable; see the
    module docstring)."""
    n = int(series.data.length)
    df = float(series.deltaF)
    return df * (numpy.arange(n) - n // 2)


def two_sided_response(transform, n, deltaF, extra_params, P=None, xpy=numpy):
    """Hermitian length-*n* response on the LAL packed grid, from the plugin's f >= 0 values."""
    m = n // 2
    even = n % 2 == 0
    # bins k = m .. n-1  ->  f = 0 .. (n-1-m) dF; for even n also f = m dF (the Nyquist bin k = 0)
    f_nonneg = deltaF * numpy.arange(n - m + (1 if even else 0))
    Fpos = transform.evaluate(f_nonneg, extra_params, P=P, xpy=xpy)
    if hasattr(Fpos, "get") and not isinstance(Fpos, numpy.ndarray):
        Fpos = Fpos.get()                             # cupy -> host; the banks are LAL (host)
    Fpos = numpy.array(Fpos, dtype=complex).reshape(-1)
    if Fpos.shape != f_nonneg.shape:
        raise ValueError("waveform transform {}: returned shape {} for {} frequencies".format(
            transform.name, Fpos.shape, f_nonneg.shape[0]))
    if not numpy.all(numpy.isfinite(Fpos)):
        raise ValueError("waveform transform {}: non-finite response at lambda={}".format(
            transform.name, dict(extra_params)))
    Fpos[0] = Fpos[0].real                            # DC of a real kernel is real
    if even:
        F_nyq = Fpos[-1].real                         # self-conjugate bin: only Re is Hermitian
        Fpos = Fpos[:-1]
    F = numpy.empty(n, dtype=complex)
    F[m:] = Fpos
    # negative bins k = 1 .. m-1 (even n) / 0 .. m-1 (odd n): f = -(m-k) dF  <-  conj F(+(m-k) dF)
    k0 = 1 if n % 2 == 0 else 0
    ks = numpy.arange(k0, m)
    F[ks] = numpy.conj(Fpos[m - ks])
    if even:
        F[0] = F_nyq
    return F


WRAP_WARN_FRACTION = 1e-3
WRAP_ERROR_FRACTION = 1e-2


def wrapped_energy_fraction(packed_data, F):
    """Fraction of the energy of the LINEAR convolution of a packed series with the kernel of
    *F* that falls outside the length-N buffer, i.e. what the cyclic multiply wraps around.

    Both are mapped to the time domain with the same ``ifft(ifftshift(.))``, so the result does
    not depend on the packing's sign convention (a time reversal maps outside to outside)."""
    data = numpy.asarray(packed_data, dtype=complex)
    n = data.shape[0]
    x = numpy.fft.ifft(numpy.fft.ifftshift(data))
    k = numpy.fft.ifft(numpy.fft.ifftshift(numpy.asarray(F, dtype=complex)))
    L = 2 * n
    j = numpy.arange(n)
    lag = numpy.where(j < (n + 1) // 2, j, j - n)        # signed kernel lags
    xp = numpy.zeros(L, dtype=complex)
    xp[:n] = x
    kp = numpy.zeros(L, dtype=complex)
    kp[lag % L] = k
    y = numpy.fft.ifft(numpy.fft.fft(xp) * numpy.fft.fft(kp))
    total = float(numpy.sum(numpy.abs(y) ** 2))
    if total <= 0:
        return 0.0
    return float(numpy.sum(numpy.abs(y[n:]) ** 2)) / total


def _check_wrap(transform, series, F, extra_params):
    frac = wrapped_energy_fraction(series.data.data, F)
    tol = float(getattr(getattr(transform, "module", None), "WRAP_TOLERANCE", WRAP_ERROR_FRACTION))
    if frac > tol:
        raise WaveformTransformError(
            "waveform transform {}: {:.3g} of the template energy wraps around the {:.4g} s "
            "analysis buffer at lambda={} (tolerance {:.3g}); the kernel's delay is too long for "
            "this segment.  Use a longer segment (smaller deltaF).".format(
                transform.name, frac, 1.0 / float(series.deltaF), dict(extra_params), tol))
    if frac > WRAP_WARN_FRACTION:
        warnings.warn("waveform transform {}: {:.3g} of the template energy wraps around the "
                      "analysis buffer at lambda={}".format(transform.name, frac, dict(extra_params)))
    return frac


def apply_hlm_transform(hlms, hlms_conj, P=None, transform=None, extra_params=None, xpy=numpy):
    """Multiply every mode of BOTH banks by the same Hermitian F, in place.  Returns the banks.
    Any failure is raised as :class:`WaveformTransformError`."""
    if transform is None:
        return hlms, hlms_conj
    try:
        return _apply_hlm_transform(hlms, hlms_conj, P, transform, extra_params, xpy)
    except WaveformTransformError:
        raise
    except Exception as exc:
        raise WaveformTransformError("waveform transform {}: failed at lambda={}: {}: {}".format(
            getattr(transform, "name", transform), extra_params, type(exc).__name__, exc)) from exc


def _apply_hlm_transform(hlms, hlms_conj, P, transform, extra_params, xpy):
    cache = {}
    dominant = {}                       # grid -> (norm, series) of the strongest mode in hlms
    for key in list(hlms.keys()):
        series = hlms[key]
        if hasattr(series, "data") and hasattr(series.data, "data"):
            grid = (int(series.data.length), float(series.deltaF))
            nrm = float(numpy.sum(numpy.abs(series.data.data) ** 2))
            if grid not in dominant or nrm > dominant[grid][0]:
                dominant[grid] = (nrm, series)
    for grid, (_, series) in dominant.items():
        cache[grid] = two_sided_response(transform, grid[0], grid[1], extra_params, P=P, xpy=xpy)
        _check_wrap(transform, series, cache[grid], extra_params)
    seen = set()
    for bank in (hlms, hlms_conj):
        for key in list(bank.keys()):
            series = bank[key]
            if id(series) in seen:      # never apply twice to a series shared between banks
                continue
            seen.add(id(series))
            if not hasattr(series, "data") or not hasattr(series.data, "data"):
                raise TypeError("waveform transform: mode {} is not a LAL frequency series "
                                "({}); this precompute route is not supported".format(key, type(series)))
            grid = (int(series.data.length), float(series.deltaF))
            if grid not in cache:
                cache[grid] = two_sided_response(transform, grid[0], grid[1], extra_params, P=P, xpy=xpy)
            series.data.data = series.data.data * cache[grid]
    return hlms, hlms_conj
