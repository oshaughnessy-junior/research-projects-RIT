"""Contiguous native BHPTNR samples with an explicit, precision-checked epoch."""
from copy import deepcopy
from dataclasses import dataclass
from fractions import Fraction
import hashlib
from numbers import Integral

import numpy as np

from .bhptnr_adapter import MAX_SAMPLES, _real
from .bhptnr_polarizations import BHPTNRPolarizations

TIMING_LIMIT_SECONDS = 1e-9


@dataclass(frozen=True)
class BHPTNREpochMapping:
    native_anchor_seconds: float
    observation_anchor_seconds: float
    native_reference: str
    observation_reference: str

    def __post_init__(self):
        for name in ('native_anchor_seconds','observation_anchor_seconds'):
            object.__setattr__(self,name,_real(getattr(self,name),name))
        for name in ('native_reference','observation_reference'):
            value=getattr(self,name)
            if not isinstance(value,str) or not value.strip():
                raise ValueError('both references must name their time origin and scale')


@dataclass(frozen=True)
class BHPTNRWindow:
    polarizations: np.ndarray
    native_times_seconds: np.ndarray
    dt_seconds: float
    epoch_start_seconds: float
    epoch_reference: str
    metadata: dict

    @property
    def n_samples(self):
        return self.polarizations.shape[1]


def _array_hash(values):
    return hashlib.sha256(np.asarray(values,dtype='<f8',order='C').tobytes()).hexdigest()


def select_native_window(native, *, first_index, n_samples, mapping):
    """Copy consecutive native samples; do not interpolate, decimate or pad.

    Endpoint cadence and explicit anchor mapping must satisfy a fixed 1ns bound,
    including conservative roundoff for a consumer's absolute float64 time grid.
    This is a representation check, not response or waveform qualification.
    """
    if not isinstance(native,BHPTNRPolarizations) or not isinstance(mapping,BHPTNREpochMapping):
        raise ValueError('require BHPTNRPolarizations and BHPTNREpochMapping')
    for name,value in [('first_index',first_index),('n_samples',n_samples)]:
        if isinstance(value,(bool,np.bool_)) or not isinstance(value,Integral):
            raise ValueError(name+' must be an integer')
    first_index,n_samples=int(first_index),int(n_samples)
    if first_index<0 or not 2<=n_samples<=MAX_SAMPLES:
        raise ValueError('require a nonnegative index and bounded native sample count')
    raw_times=np.asarray(native.time_seconds);raw_values=np.asarray(native.polarizations)
    if (raw_times.ndim!=1 or not 2<=raw_times.size<=MAX_SAMPLES or raw_times.dtype.kind not in 'fi'
            or raw_values.shape!=(2,raw_times.size) or raw_values.dtype.kind not in 'fi'
            or first_index+n_samples>raw_times.size):
        raise ValueError('requested window needs genuine finite native support')
    with np.errstate(over='ignore',invalid='ignore'):
        times=np.array(raw_times[first_index:first_index+n_samples],dtype=float,copy=True)
        values=np.array(raw_values[:,first_index:first_index+n_samples],dtype=float,copy=True)
    if not np.all(np.isfinite(times)) or np.any(np.diff(times)<=0) or not np.all(np.isfinite(values)):
        raise ValueError('native window must remain finite and increasing at output precision')
    metadata=deepcopy(native.metadata)
    if (metadata.get('complex_convention')!='hp - 1j*hc' or metadata.get('native_times_preserved') is not True
            or metadata.get('n_samples')!=raw_times.size
            or metadata.get('first_time_seconds')!=float(raw_times[0])
            or metadata.get('last_time_seconds')!=float(raw_times[-1])):
        raise ValueError('native synthesis receipt disagrees with the input contract')
    dt=float((Fraction(float(times[-1]))-Fraction(float(times[0])))/(n_samples-1))
    # Subtract native epochs exactly: reconstructing a grid at a large native
    # origin can round away the very irregularity this check must measure.
    first_exact=Fraction(float(times[0]));dt_exact=Fraction(dt)
    residual_exact=max(abs(Fraction(float(t))-first_exact-i*dt_exact) for i,t in enumerate(times))
    residual=float(residual_exact)
    if Fraction(residual)<residual_exact:residual=float(np.nextafter(residual,np.inf))
    if not np.isfinite(dt) or dt<=0 or not np.isfinite(residual) or residual>TIMING_LIMIT_SECONDS:
        raise ValueError('native window is not uniform within the fixed timing budget')
    epoch_exact=(Fraction(mapping.observation_anchor_seconds)+Fraction(float(times[0]))
                 -Fraction(mapping.native_anchor_seconds))
    try:epoch=float(epoch_exact)
    except OverflowError as exc:raise ValueError('observation epoch is unrepresentable') from exc
    if not np.isfinite(epoch):raise ValueError('observation epoch is nonfinite')
    epoch_roundoff_exact=abs(Fraction(epoch)-epoch_exact)
    epoch_roundoff=float(epoch_roundoff_exact)
    if Fraction(epoch_roundoff)<epoch_roundoff_exact:epoch_roundoff=float(np.nextafter(epoch_roundoff,np.inf))
    span=(n_samples-1)*dt
    with np.errstate(over='ignore',invalid='ignore'):
        absolute_bound=(abs(float(np.spacing(abs(epoch)+span)))+abs(float(np.spacing(span))))/2
    absolute_bound=float(np.nextafter(absolute_bound,np.inf))
    combined=float(np.nextafter(float(np.nextafter(residual+epoch_roundoff,np.inf))+absolute_bound,np.inf))
    if epoch+dt==epoch or not np.isfinite(combined) or combined>TIMING_LIMIT_SECONDS:
        raise ValueError('absolute time grid exceeds the fixed timing budget')
    times.setflags(write=False);values.setflags(write=False)
    receipt=dict(source_metadata=metadata,operation='contiguous native samples; no interpolation, decimation, padding or taper',
        first_native_index=first_index,n_samples=n_samples,native_anchor_seconds=mapping.native_anchor_seconds,
        observation_anchor_seconds=mapping.observation_anchor_seconds,native_reference=mapping.native_reference,
        observation_reference=mapping.observation_reference,grid_residual_seconds=residual,
        epoch_roundoff_seconds=epoch_roundoff,absolute_grid_roundoff_bound_seconds=absolute_bound,
        combined_timing_bound_seconds=combined,timing_limit_seconds=TIMING_LIMIT_SECONDS,
        native_time_float64_le_sha256=_array_hash(times),polarizations_float64_le_sha256=_array_hash(values),
        frame=metadata.get('frame'),complex_convention='hp - 1j*hc',
        scope='native window and explicit time-anchor mapping; no phase alignment, response or fidelity qualification')
    return BHPTNRWindow(values,times,dt,epoch,mapping.observation_reference,receipt)
