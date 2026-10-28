"""Explicit valid-support BHPTNR FIR filtering and cadence conversion.

See DESIGN_bhptnr_cadence.md for the sampling contract and validation scope.
"""
from copy import deepcopy
from dataclasses import dataclass
from fractions import Fraction
from numbers import Integral

import numpy as np

from .bhptnr_adapter import _real
from .bhptnr_window import (BHPTNREpochMapping, TIMING_LIMIT_SECONDS,
                            _array_hash, select_native_window)


@dataclass(frozen=True)
class BHPTNRCadence:
    polarizations: np.ndarray
    dt_seconds: float
    epoch_start_seconds: float
    epoch_reference: str
    metadata: dict

    @property
    def n_samples(self):
        return self.polarizations.shape[1]


def convert_cadence(native, *, first_native_seconds, n_samples, mapping,
                    dt_seconds, cutoff_hz, numtaps, beta, spline_degree):
    """Apply an explicit symmetric FIR and spline only on genuine support.

    All filter/interpolation choices are mandatory. This changes the waveform;
    the caller must qualify its transfer function and response separately.
    """
    if not isinstance(mapping, BHPTNREpochMapping):
        raise ValueError('require an explicit BHPTNREpochMapping')
    for name, value in [('n_samples', n_samples), ('numtaps', numtaps),
                        ('spline_degree', spline_degree)]:
        if isinstance(value, (bool, np.bool_)) or not isinstance(value, Integral):
            raise ValueError(name+' must be an integer')
    n_samples, numtaps, spline_degree = map(int, (n_samples, numtaps, spline_degree))
    if (not 2 <= n_samples <= 32768 or not 9 <= numtaps <= 8193
            or numtaps % 2 != 1 or spline_degree not in (5, 7)):
        raise ValueError('require bounded output count, odd 9..8193 taps and spline degree 5 or 7')
    start = _real(first_native_seconds, 'first_native_seconds')
    dt = _real(dt_seconds, 'dt_seconds')
    cutoff = _real(cutoff_hz, 'cutoff_hz')
    beta = _real(beta, 'beta')
    if dt <= 0 or not 0 < cutoff < .5/dt or not 0 <= beta <= 30:
        raise ValueError('require positive cadence, cutoff below output Nyquist and beta in [0,30]')
    # Validate the complete native support and retain its fixed precision guard.
    if not hasattr(native, 'time_seconds'):
        raise ValueError('require native BHPTNR polarizations')
    times = np.asarray(native.time_seconds)
    if times.ndim != 1 or times.size < 2:
        raise ValueError('require genuine native time support')
    raw = select_native_window(native, first_index=0, n_samples=times.size,
        mapping=BHPTNREpochMapping(float(times[0]), 0., 'native seconds', 'relative native seconds'))
    if dt < raw.dt_seconds or cutoff >= .5/raw.dt_seconds:
        raise ValueError('require downsampling and cutoff below native Nyquist')
    if raw.n_samples < numtaps+spline_degree:
        raise ValueError('insufficient genuine support for filter and interpolation')
    # Relative coordinates avoid subtracting nearly equal large epochs in the spline.
    start_exact = Fraction(start)-Fraction(float(raw.native_times_seconds[0]))
    start_relative = float(start_exact)
    half = numtaps//2
    valid_times = np.arange(half, raw.n_samples-half, dtype=float)*raw.dt_seconds
    targets = start_relative+np.arange(n_samples, dtype=float)*dt
    if (not np.all(np.isfinite(targets)) or np.any(np.diff(targets) <= 0)
            or targets[0] < valid_times[0] or targets[-1] > valid_times[-1]):
        raise ValueError('requested output needs unavailable genuine filter support')
    epoch_exact = Fraction(mapping.observation_anchor_seconds)+Fraction(start)-Fraction(mapping.native_anchor_seconds)
    try:
        epoch = float(epoch_exact)
    except OverflowError as exc:
        raise ValueError('observation epoch is unrepresentable') from exc
    if not np.isfinite(epoch):
        raise ValueError('observation epoch is unrepresentable')
    # Bound both the relative spline queries and the consumer absolute grid.
    def upward(value):
        result = float(value)
        return float(np.nextafter(result, np.inf)) if Fraction(result) < value else result
    span = (n_samples-1)*dt
    with np.errstate(over='ignore', invalid='ignore'):
        bound = (raw.metadata['combined_timing_bound_seconds']
                 + upward(abs(Fraction(start_relative)-start_exact))
                 + upward(abs(Fraction(epoch)-epoch_exact))
                 + abs(float(np.spacing(abs(start_relative)+span)))
                 + abs(float(np.spacing(abs(epoch)+span)))
                 + abs(float(np.spacing(span))))
    bound = float(np.nextafter(bound, np.inf))
    if not np.isfinite(bound) or bound > TIMING_LIMIT_SECONDS or epoch+dt == epoch:
        raise ValueError('converted time grid exceeds fixed timing budget')
    from scipy.signal import firwin, fftconvolve
    from scipy.interpolate import make_interp_spline
    taps = firwin(numtaps, cutoff, window=('kaiser', beta), fs=1/raw.dt_seconds, scale=True)
    filtered = fftconvolve(raw.polarizations, taps[None, :], mode='valid', axes=-1)
    output = np.array(make_interp_spline(valid_times, filtered, k=spline_degree, axis=1)(
        targets, extrapolate=False), dtype=float, copy=True)
    if output.shape != (2, n_samples) or not np.all(np.isfinite(output)):
        raise ValueError('invalid filtered/interpolated output')
    output.setflags(write=False)
    receipt = dict(source_metadata=deepcopy(raw.metadata), frame=native.metadata.get('frame'),
        complex_convention='hp - 1j*hc', native_times_preserved=False,
        operation='symmetric zero-phase FIR valid convolution, then explicit spline interpolation',
        first_native_seconds=start, native_dt_seconds=raw.dt_seconds, output_dt_seconds=dt,
        n_samples=n_samples, cutoff_hz=cutoff, numtaps=numtaps, kaiser_beta=beta,
        spline_degree=spline_degree, fir_float64_le_sha256=_array_hash(taps),
        polarizations_float64_le_sha256=_array_hash(output),
        valid_filtered_support_relative_seconds=[float(valid_times[0]), float(valid_times[-1])],
        native_anchor_seconds=mapping.native_anchor_seconds,
        observation_anchor_seconds=mapping.observation_anchor_seconds,
        native_reference=mapping.native_reference, observation_reference=mapping.observation_reference,
        combined_timing_bound_seconds=bound, timing_limit_seconds=TIMING_LIMIT_SECONDS,
        boundary_rule='valid support only; no physical extension, padding or extrapolation',
        scope='numerically filtered waveform; no response or physical-fidelity qualification')
    return BHPTNRCadence(output, dt, epoch, mapping.observation_reference, receipt)
