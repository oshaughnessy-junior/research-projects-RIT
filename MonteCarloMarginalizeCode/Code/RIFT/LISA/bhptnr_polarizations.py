"""Optional fixed-plane BHPTNR mode synthesis, preserving native time samples.

The complex convention is hp - i hc. This module neither generates waveforms
nor assigns epochs, resamples, computes a detector response or aligns models.
"""
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
import hashlib
from numbers import Integral

import numpy as np

from .bhptnr_adapter import (BHPTNRModes, BHPTNRParameters, DATA_SHA256,
                             MAX_SAMPLES, POSITIVE_MODES, SOURCE_REVISION, _real)


@dataclass(frozen=True)
class BHPTNRPolarizations:
    time_seconds: np.ndarray
    polarizations: np.ndarray
    metadata: dict


def _digest(array):
    return hashlib.sha256(np.ascontiguousarray(array).tobytes()).hexdigest()


def _validated(native):
    if not isinstance(native, BHPTNRModes):
        raise ValueError('require BHPTNRModes from the native adapter')
    raw = np.asarray(native.time_seconds)
    if raw.ndim != 1 or raw.dtype.kind not in 'fi' or not 2 <= raw.size <= MAX_SAMPLES:
        raise ValueError('require bounded real native time samples')
    with np.errstate(over='ignore', invalid='ignore'):
        times = np.array(raw, dtype=np.float64, copy=True)
    if not np.all(np.isfinite(times)) or not np.all(np.diff(times) > 0):
        raise ValueError('native times must remain finite and increasing at output precision')
    if set(native.positive_modes) != set(POSITIVE_MODES):
        raise ValueError('require all 25 positive modes, without reflected partners')
    modes = {}
    for key in POSITIVE_MODES:
        raw = np.asarray(native.positive_modes[key])
        if raw.shape != times.shape or raw.dtype.kind not in 'fci':
            raise ValueError('invalid native mode shape or type')
        with np.errstate(over='ignore', invalid='ignore'):
            value = np.array(raw, dtype=np.complex128, copy=True)
        if not np.all(np.isfinite(value)):
            raise ValueError('native modes must remain finite at output precision')
        modes[key] = value
    receipt = deepcopy(native.metadata)
    expected = dict(model='BHPTNRSur1dq1e4', source_revision=SOURCE_REVISION,
                    data_sha256=DATA_SHA256, mass_scale='M', time_units='seconds',
                    mode_units='strain', positive_modes=[list(k) for k in POSITIVE_MODES],
                    time_origin='native surrogate time origin; no observation epoch assigned',
                    n_samples=len(times), first_time_seconds=float(times[0]),
                    last_time_seconds=float(times[-1]))
    if any(receipt.get(k) != v for k, v in expected.items()):
        raise ValueError('native metadata is inconsistent with the supported source or samples')
    if (receipt.get('neg_modes') is not False or receipt.get('mode_sum') is not False
            or type(receipt.get('calibrated')) is not bool):
        raise ValueError('native metadata must declare calibration and positive-mode output')
    try:
        BHPTNRParameters(**receipt['parameters'])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('native metadata requires supported physical parameters') from exc
    receipt['synthesis_inputs'] = {
        'time_float64_sha256': _digest(times),
        'mode_complex128_sha256': {f'{l},{m}': _digest(v) for (l,m),v in modes.items()},
        'byte_order': 'little' if np.little_endian else 'big',
    }
    return times, modes, receipt


@lru_cache(maxsize=9)
def _angular_momentum_spectrum(ell):
    m = np.arange(-ell,ell+1,dtype=float)
    raising = np.diag(np.sqrt(ell*(ell+1)-m[:-1]*(m[:-1]+1)),k=-1)
    jy = (raising-raising.T)/(2j)
    values,vectors=np.linalg.eigh(jy)
    values.setflags(write=False);vectors.setflags(write=False)
    return values,vectors


@lru_cache(maxsize=128)
def _rotation_matrix(ell, theta):
    eigenvalues,eigenvectors = _angular_momentum_spectrum(ell)
    rotation = (eigenvectors*np.exp(-1j*theta*eigenvalues))@eigenvectors.conjugate().T
    if np.max(np.abs(rotation.imag))>2e-13:
        raise ValueError('angular-momentum rotation has a nonphysical imaginary part')
    result = rotation.real.copy()
    result.setflags(write=False)
    return result


def spin_weighted_harmonic(ell, m, theta, phi):
    """Spin -2 harmonic via exp(-i*theta*J_y), covering ell=2..10."""
    if (isinstance(ell,bool) or isinstance(m,bool)
            or not isinstance(ell,Integral) or not isinstance(m,Integral)
            or not 2<=ell<=10 or abs(m)>ell):
        raise ValueError('require integer ell=2..10 and |m|<=ell')
    theta,phi=_real(theta,'theta'),_real(phi,'phi')
    if not 0<=theta<=np.pi:
        raise ValueError('theta must be in [0,pi]')
    d=_rotation_matrix(int(ell),theta)[int(m)+int(ell),2+int(ell)]
    return np.sqrt((2*ell+1)/(4*np.pi))*d*(np.cos(phi)+1j*np.sin(phi))**int(m)



def _finish(times, strain, receipt, **metadata):
    polarizations = np.array([strain.real, -strain.imag], dtype=np.float64)
    if not np.all(np.isfinite(polarizations)):
        raise ValueError('synthesis produced nonfinite polarizations')
    times.setflags(write=False)
    polarizations.setflags(write=False)
    receipt.update(metadata)
    receipt.update(complex_convention='hp - 1j*hc', native_times_preserved=True,
                   negative_modes='h(l,-m)=(-1)^l*conj(h(l,m)); fixed-plane reflection',
                   amplitude_rescaling=False,
                   scope='finite fixed-plane synthesis; no response, fidelity or cross-model alignment qualification')
    return BHPTNRPolarizations(times, polarizations, receipt)


def _synthesize(modes, theta, phi):
    total = np.zeros_like(next(iter(modes.values())))
    with np.errstate(over='ignore', invalid='ignore'):
        for (ell, m), values in sorted(modes.items()):
            total += (values*spin_weighted_harmonic(ell,m,theta,phi)
                      + (-1)**ell*values.conjugate()*spin_weighted_harmonic(ell,-m,theta,phi))
    return total


def source_polarizations(native, *, inclination, observer_azimuth):
    """Synthesize source-frame hp,hc from already scaled positive-mode strain."""
    theta = _real(inclination, 'inclination')
    phi = _real(observer_azimuth, 'observer_azimuth')
    if not 0 <= theta <= np.pi:
        raise ValueError('inclination must be in [0,pi]')
    times, modes, receipt = _validated(native)
    return _finish(times, _synthesize(modes,theta,phi), receipt,
                   frame='native inertial source-frame spherical basis',
                   inclination=theta, observer_azimuth=phi)


def ssb_polarizations(native, *, qS, phiS, qL, phiL, orbital_phase_offset):
    """Use the explicit BHPWave sky/orbital-axis SSB polarization convention.

    Orbital phase is an observer-azimuth offset; intrinsic cross-model phase
    origins remain unaligned. An aligned sky/orbital axis needs a separate gauge.
    """
    geometry = {k:_real(v,k) for k,v in dict(qS=qS, phiS=phiS, qL=qL,
                phiL=phiL, orbital_phase_offset=orbital_phase_offset).items()}
    qS,phiS,qL,phiL,phase = geometry.values()
    if not (0 <= qS <= np.pi and 0 <= qL <= np.pi
            and 0 <= phiS < 2*np.pi and 0 <= phiL < 2*np.pi):
        raise ValueError('require polar angles in [0,pi] and azimuths in [0,2pi)')
    n = np.array([np.sin(qS)*np.cos(phiS), np.sin(qS)*np.sin(phiS), np.cos(qS)])
    axis = np.array([np.sin(qL)*np.cos(phiL), np.sin(qL)*np.sin(phiL), np.cos(qL)])
    e_theta = np.array([np.cos(qS)*np.cos(phiS), np.cos(qS)*np.sin(phiS), -np.sin(qS)])
    e_phi = np.array([-np.sin(phiS), np.cos(phiS), 0.])
    projected = axis@e_theta + 1j*(axis@e_phi)
    if abs(projected) <= 64*np.finfo(float).eps:
        raise ValueError('aligned sky/orbital axis needs an explicit polarization gauge')
    theta = float(np.arctan2(abs(projected), -n@axis))
    phi = -np.pi/2 - float(np.arctan2(np.sin(phase),np.cos(phase)))
    rotation = projected/projected.conjugate()
    times, modes, receipt = _validated(native)
    return _finish(times, _synthesize(modes,theta,phi)*rotation, receipt,
                   frame='SSB, explicit BHPWave source-angle/polarization convention',
                   geometry=geometry, inclination=theta, observer_azimuth=phi,
                   spin_rotation=[float(rotation.real),float(rotation.imag)])
