"""Independent filter phase/transfer, genuine support and precision checks."""
from dataclasses import replace
from fractions import Fraction

import numpy as np
import pytest

from RIFT.LISA.bhptnr_polarizations import BHPTNRPolarizations
from RIFT.LISA.bhptnr_window import BHPTNREpochMapping
from RIFT.LISA.bhptnr_cadence import convert_cadence


def native(n=16001, origin=-135000., frequency=.02):
    t=np.arange(n)*.5
    values=np.array([np.cos(2*np.pi*frequency*t+.37),np.sin(2*np.pi*frequency*t+.37)])
    times=origin+t
    return BHPTNRPolarizations(times, values, dict(frame='SSB',warnings=['preserved'],
        complex_convention='hp - 1j*hc',native_times_preserved=True,n_samples=n,
        first_time_seconds=float(times[0]),last_time_seconds=float(times[-1])))


def run(n, **changes):
    options=dict(first_native_seconds=float(n.time_seconds[0])+1000.,n_samples=1000,
        mapping=BHPTNREpochMapping(-135000.,0.,'native seconds','orbit t=0 seconds'),
        dt_seconds=5.,cutoff_hz=.04,numtaps=1025,beta=12.,spline_degree=5)
    options.update(changes)
    return convert_cadence(n,**options)


def independent_taps(count=1025,dt=.5,cutoff=.04,beta=12.):
    k=np.arange(count)-count//2
    window=np.i0(beta*np.sqrt(1-(2*k/(count-1))**2))/np.i0(beta)
    taps=2*cutoff*dt*np.sinc(2*cutoff*dt*k)*window
    return taps/taps.sum()


@pytest.mark.parametrize('frequency',[.001,.02,.03,.05,.12,.4])
def test_tone_gain_and_phase_against_explicit_transfer(frequency):
    n=native(frequency=frequency);out=run(n)
    taps=independent_taps();k=(np.arange(1025)-512)*.5
    gain=np.sum(taps*np.cos(2*np.pi*frequency*k))
    t=1000.+np.arange(1000)*5.
    expected=gain*np.exp(1j*(2*np.pi*frequency*t+.37))
    actual=out.polarizations[0]+1j*out.polarizations[1]
    np.testing.assert_allclose(actual,expected,rtol=0,atol=2e-12)
    if frequency<=.03:assert abs(gain-1)<2e-6
    if frequency>=.05:assert abs(gain)<2e-6
    assert out.metadata['combined_timing_bound_seconds']<=1e-9


def test_direct_convolution_at_native_samples_and_receipt_copy():
    n=native();out=run(n,dt_seconds=.5,n_samples=1000)
    taps=independent_taps()
    # Centers 2000..2999 correspond to valid-convolution positions 1488..2487.
    expected=np.array([np.convolve(row,taps,mode='valid')[1488:2488] for row in n.polarizations])
    np.testing.assert_allclose(out.polarizations,expected,rtol=0,atol=2e-15)
    assert out.epoch_start_seconds==1000. and out.dt_seconds==.5
    assert out.metadata['native_times_preserved'] is False
    n.metadata['warnings'].append('later');n.polarizations[:]=0
    assert out.metadata['source_metadata']['source_metadata']['warnings']==['preserved']
    assert np.max(np.abs(out.polarizations))>.9 and not out.polarizations.flags.writeable


@pytest.mark.parametrize('field,value',[
    ('n_samples',True),('numtaps',np.bool_(True)),('spline_degree',5.),
    ('n_samples',32769),('n_samples',1),('numtaps',1024),('numtaps',8195),
    ('spline_degree',3),('beta',31.),('cutoff_hz',.1),('cutoff_hz',0.),
    ('dt_seconds',0.),('dt_seconds',.25),('dt_seconds',np.nan),
    ('first_native_seconds',-135000.),('first_native_seconds',-128000.),
    ('mapping',None)])
def test_invalid_configuration_refused(field,value):
    with pytest.raises(ValueError):run(native(),**{field:value})


def test_nonuniform_and_insufficient_support_refused():
    n=native();n.time_seconds[100]+=1e-6
    with pytest.raises(ValueError,match='timing budget'):run(n)
    with pytest.raises(ValueError,match='insufficient'):run(native(n=1028))


def test_large_absolute_epoch_refused():
    with pytest.raises(ValueError,match='timing budget'):
        run(native(),mapping=BHPTNREpochMapping(-135000.,1e12,'native seconds','absolute seconds'))


@pytest.mark.parametrize('degree',[5,7])
def test_fractional_output_grid_matches_analytic_tone(degree):
    n=native(frequency=.02)
    out=run(n,first_native_seconds=-133999.875,dt_seconds=4.9,n_samples=1000,spline_degree=degree)
    taps=independent_taps();gain=np.sum(taps*np.cos(2*np.pi*.02*(np.arange(1025)-512)*.5))
    times=1000.125+np.arange(1000)*4.9
    expected=gain*np.exp(1j*(2*np.pi*.02*times+.37))
    np.testing.assert_allclose(out.polarizations[0]+1j*out.polarizations[1],expected,rtol=0,atol=1e-11)
    assert out.epoch_start_seconds==1000.125
