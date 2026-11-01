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
        # extra_params: {name: float}.  P: intrinsic ChooseWaveformParams (read-only).
        # returns a finite complex array of the same shape.
    def is_identity(extra_params): ...           # optional; True -> the seam skips the multiply

THE SEAM, NOT THE PLUGIN, BUILDS THE TWO-SIDED RESPONSE.  It evaluates the plugin on f >= 0
only, fills F(-f) = conj F(f), takes the real part at DC and sets the even-length Nyquist bin
to 1.  F is therefore Hermitian (a real time-domain kernel), which is exactly the condition
under which the SAME array multiplies both ``hlms`` (FFT of h_lm(t)) and ``hlms_conj`` (FFT of
conj h_lm(t)): FFT[conj(K*h)](f) = conj(F(-f)) FFT[conj h](f) = F(f) FFT[conj h](f).

PHASE-SIGN TRAP.  The banks are LAL two-sided series from ``COMPLEX16TimeFreqFFT``: ascending,
``f0 = -floor(N/2) deltaF``, physical frequency of bin k = ``deltaF (k - N//2)``.
``lalsimutils.evaluate_fvals``' default labelling (``deltaF (N/2 - k)``) is the NEGATIVE of
that; using it would conjugate F, which leaves every norm unchanged and moves a delay the
wrong way.  This module asserts ``f0`` and builds f itself; the sign is pinned against a
numpy time-domain reference in ``test/test_hlm_transform.py``.
"""

import functools
import inspect

import numpy

from RIFT.misc import plugin_loader

FLAG_CODE = "--supplementary-waveform-transform-code"
FLAG_INI = "--supplementary-waveform-transform-ini"
DEFAULT_FUNCTION = "transfer_function"


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
        kw = {"P": P}
        if self._accepts_xpy:
            kw["xpy"] = xpy
        return self.transfer_function(f_nonneg, dict(extra_params), **kw)

    def bind(self, extra_params, xpy=numpy):
        """Callable ``(hlms, hlms_conj, P) -> (hlms, hlms_conj)`` for one grid point, for
        ``PrecomputeLikelihoodTerms(hlm_transform=...)``.  ``None`` if F is the identity at
        *extra_params* (declared by the plugin's ``is_identity``)."""
        if self.identity_for(extra_params):
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
        prep(config=config, extra_parameters=list(names), opts=opts)
    return WaveformTransform(getattr(module, function), names,
                             name=getattr(module, "NAME", None) or function,
                             is_identity=getattr(module, "is_identity", None),
                             module=module, config=config)


def packed_frequencies(series):
    """Physical frequencies of a LAL two-sided (FFT-shifted) COMPLEX16FrequencySeries.

    Asserts the LAL origin ``f0 = -floor(N/2) deltaF``; returns ``deltaF (k - N//2)``."""
    n = int(series.data.length)
    df = float(series.deltaF)
    if not numpy.isclose(float(series.f0), -df * (n // 2), rtol=1e-12, atol=1e-12 * max(df, 1.0)):
        raise ValueError("waveform transform: mode series origin f0={} is not the LAL FFT-shift "
                         "origin -floor(N/2)*deltaF={} (N={}, deltaF={}); refusing to guess the "
                         "packing".format(float(series.f0), -df * (n // 2), n, df))
    return df * (numpy.arange(n) - n // 2)


def two_sided_response(transform, n, deltaF, extra_params, P=None, xpy=numpy):
    """Hermitian length-*n* response on the LAL packed grid, from the plugin's f >= 0 values."""
    m = n // 2
    f_nonneg = deltaF * numpy.arange(n - m)          # bins k = m .. n-1  ->  f = 0 .. (n-1-m) dF
    Fpos = numpy.asarray(transform.evaluate(f_nonneg, extra_params, P=P, xpy=xpy))
    if hasattr(Fpos, "get"):                          # cupy -> host; the banks are LAL (host)
        Fpos = Fpos.get()
    Fpos = numpy.array(Fpos, dtype=complex).reshape(-1)
    if Fpos.shape != f_nonneg.shape:
        raise ValueError("waveform transform {}: returned shape {} for {} frequencies".format(
            transform.name, Fpos.shape, f_nonneg.shape[0]))
    if not numpy.all(numpy.isfinite(Fpos)):
        raise ValueError("waveform transform {}: non-finite response at lambda={}".format(
            transform.name, dict(extra_params)))
    Fpos[0] = Fpos[0].real                            # DC of a real kernel is real
    F = numpy.empty(n, dtype=complex)
    F[m:] = Fpos
    # negative bins k = 1 .. m-1 (even n) / 0 .. m-1 (odd n): f = -(m-k) dF  <-  conj F(+(m-k) dF)
    k0 = 1 if n % 2 == 0 else 0
    ks = numpy.arange(k0, m)
    F[ks] = numpy.conj(Fpos[m - ks])
    if n % 2 == 0:
        F[0] = 1.0                                    # self-conjugate Nyquist bin: identity
    return F


def apply_hlm_transform(hlms, hlms_conj, P=None, transform=None, extra_params=None, xpy=numpy):
    """Multiply every mode of BOTH banks by the same Hermitian F, in place.  Returns the banks."""
    if transform is None:
        return hlms, hlms_conj
    cache = {}
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
            packed_frequencies(series)  # asserts the origin
            grid = (int(series.data.length), float(series.deltaF))
            if grid not in cache:
                cache[grid] = two_sided_response(transform, grid[0], grid[1], extra_params, P=P, xpy=xpy)
            series.data.data = series.data.data * cache[grid]
    return hlms, hlms_conj
