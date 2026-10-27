"""Native time-grid, explicit epoch and copying invariants without a backend."""
from dataclasses import replace
from fractions import Fraction
import hashlib

import numpy as np
import pytest

from RIFT.LISA.bhptnr_polarizations import BHPTNRPolarizations
from RIFT.LISA.bhptnr_window import BHPTNREpochMapping,select_native_window


def native():
    times=-135000.+np.arange(100)*.975
    values=np.vstack([np.arange(100),np.arange(100)**2])*1e-22
    return BHPTNRPolarizations(times,values,dict(frame='SSB',complex_convention='hp - 1j*hc',native_times_preserved=True,
        n_samples=100,first_time_seconds=float(times[0]),last_time_seconds=float(times[-1]),warnings=['native warning']))


def mapping(**overrides):
    return BHPTNREpochMapping(**dict(native_anchor_seconds=-135000.,observation_anchor_seconds=0.,
        native_reference='native surrogate seconds',observation_reference='orbit t=0 seconds',**overrides))


def test_exact_contiguous_samples_and_anchor_mapping():
    n=native();m=mapping()
    out=select_native_window(n,first_index=7,n_samples=43,mapping=m)
    np.testing.assert_array_equal(out.polarizations,n.polarizations[:,7:50])
    np.testing.assert_array_equal(out.native_times_seconds,n.time_seconds[7:50])
    expected=Fraction(float(n.time_seconds[7]))-Fraction(m.native_anchor_seconds)+Fraction(m.observation_anchor_seconds)
    assert out.epoch_start_seconds==float(expected)
    assert out.dt_seconds==float((Fraction(float(n.time_seconds[49]))-Fraction(float(n.time_seconds[7])))/42)
    assert out.epoch_reference==m.observation_reference and out.n_samples==43
    modeled=out.epoch_start_seconds+np.arange(43)*out.dt_seconds
    exact=np.array([float(Fraction(float(t))-Fraction(m.native_anchor_seconds)) for t in out.native_times_seconds])
    assert np.max(np.abs(modeled-exact))<=out.metadata['combined_timing_bound_seconds']
    assert out.metadata['combined_timing_bound_seconds']<=1e-9
    assert out.metadata['native_time_float64_le_sha256']==hashlib.sha256(np.asarray(out.native_times_seconds,dtype='<f8').tobytes()).hexdigest()
    assert out.metadata['polarizations_float64_le_sha256']==hashlib.sha256(np.asarray(out.polarizations,dtype='<f8').tobytes()).hexdigest()


def test_copy_and_deepcopy():
    n=native();out=select_native_window(n,first_index=0,n_samples=100,mapping=mapping())
    n.polarizations[:]=0;n.time_seconds[:]=0;n.metadata['warnings'].append('later')
    assert out.polarizations[1,-1]>0 and out.native_times_seconds[0]==-135000.
    assert out.metadata['source_metadata']['warnings']==['native warning']
    assert not out.polarizations.flags.writeable and not out.native_times_seconds.flags.writeable


@pytest.mark.parametrize('field,value',[('native_anchor_seconds',True),('observation_anchor_seconds',np.inf),
    ('native_reference',''),('observation_reference',' '),('observation_reference',4)])
def test_invalid_anchor_mapping(field,value):
    kwargs=dict(native_anchor_seconds=0.,observation_anchor_seconds=0.,native_reference='native seconds',observation_reference='orbit seconds')
    kwargs[field]=value
    with pytest.raises(ValueError):BHPTNREpochMapping(**kwargs)


@pytest.mark.parametrize('start,count',[(-1,10),(True,10),(0,True),(0,1),(0,262145),(90,11),(0,10.),(1.5,10)])
def test_invalid_window_bounds(start,count):
    with pytest.raises(ValueError):select_native_window(native(),first_index=start,n_samples=count,mapping=mapping())


@pytest.mark.parametrize('kind',['nonuniform','decreasing','nan_time','infinite_value','complex_value','collapsed','metadata_count','metadata_epoch','metadata_convention'])
def test_invalid_native_input(kind):
    n=native()
    if kind=='nonuniform':n.time_seconds[40]+=1e-6
    if kind=='decreasing':n.time_seconds[40]=n.time_seconds[39]
    if kind=='nan_time':n.time_seconds[40]=np.nan
    if kind=='infinite_value':n.polarizations[0,40]=np.inf
    if kind=='complex_value':n=replace(n,polarizations=n.polarizations.astype(complex))
    if kind=='collapsed':
        times=np.array([2**60+i for i in range(100)],dtype=np.int64)
        n=replace(n,time_seconds=times);n.metadata.update(first_time_seconds=float(times[0]),last_time_seconds=float(times[-1]))
    if kind=='metadata_count':n.metadata['n_samples']=99
    if kind=='metadata_epoch':n.metadata['first_time_seconds']=0.
    if kind=='metadata_convention':n.metadata['complex_convention']='hp+i*hc'
    with pytest.raises(ValueError):select_native_window(n,first_index=0,n_samples=100,mapping=mapping())


@pytest.mark.parametrize('epoch',[1e12,1e100,-1e100])
def test_absolute_epoch_precision_refused(epoch):
    m=BHPTNREpochMapping(-135000.,epoch,'native seconds','large absolute seconds')
    with pytest.raises(ValueError,match='timing budget'):
        select_native_window(native(),first_index=0,n_samples=100,mapping=m)


def test_epoch_overflow_refused():
    m=BHPTNREpochMapping(-1.7e308,1.7e308,'native seconds','absolute seconds')
    with pytest.raises(ValueError,match='unrepresentable'):
        select_native_window(native(),first_index=0,n_samples=100,mapping=m)


def test_small_representable_negative_epoch():
    m=BHPTNREpochMapping(-135000.,-1e6,'native seconds','orbit seconds')
    out=select_native_window(native(),first_index=0,n_samples=100,mapping=m)
    assert out.epoch_start_seconds==-1e6 and out.metadata['combined_timing_bound_seconds']<=1e-9


@pytest.mark.parametrize('origin,accepted',[(-135000.,True),(-1e9,False)])
def test_native_epoch_cancellation_bound(origin,accepted):
    times=origin+np.arange(3)*.2
    n=BHPTNRPolarizations(times,np.array([[1.,2.,3.],[3.,1.,2.]])*1e-21,
        dict(frame='SSB',complex_convention='hp - 1j*hc',native_times_preserved=True,n_samples=3,
             first_time_seconds=float(times[0]),last_time_seconds=float(times[-1])))
    m=BHPTNREpochMapping(origin,0.,'native seconds','orbit seconds')
    if not accepted:
        with pytest.raises(ValueError,match='timing budget'):
            select_native_window(n,first_index=0,n_samples=3,mapping=m)
        return
    out=select_native_window(n,first_index=0,n_samples=3,mapping=m)
    actual_grid=out.epoch_start_seconds+np.arange(3)*out.dt_seconds
    errors=[abs(Fraction(float(actual_grid[i]))-(Fraction(float(t))-Fraction(origin))) for i,t in enumerate(times)]
    assert max(errors)<=Fraction(out.metadata['combined_timing_bound_seconds'])
    assert out.metadata['grid_residual_seconds']>=float(max(errors))
