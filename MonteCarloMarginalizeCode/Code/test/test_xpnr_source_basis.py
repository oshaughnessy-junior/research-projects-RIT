"""Fast contracts for explicit XPNR component labels and observer geometry."""
from types import SimpleNamespace
import numpy as np
import pytest
from RIFT.likelihood.xpnr_source_basis import XPNRSourceBasis,_geometry,_native_old_frame

LABELS=((2,2,('S',2,1)),(2,-2,('A',2,-1)))

def basis(general=True):
    b=XPNRSourceBasis(general_pe=general)
    b.parameters=SimpleNamespace(s1x=.3,s1y=.2,s1z=.4,s2x=-.2,s2y=.1,s2z=.6,m1=50.,m2=30.,fmin=20.,fref=20.)
    b.reference_spin='s1';b.beta_old=.35;b.beta_new=.3;b.phiJ=.2
    b.kappa_old0,_,_=_geometry(b.beta_old,b.phiJ,0.,0.)
    b.kappa_new0,b.zeta0,_=_geometry(b.beta_new,b.phiJ,0.,0.)
    b._ready=True
    return b

def fields(b):
    return {s:getattr(b.parameters,s) for s in ('s1x','s1y','s1z','s2x','s2y','s2z','m1','m2')}

def test_uninitialized_callback_refuses():
    with pytest.raises(RuntimeError,match='generate_raw'):
        XPNRSourceBasis().angular_coefficients(LABELS,.8,1.1)

@pytest.mark.parametrize('general',[False,True])
def test_scalar_spin_fields_broadcast_to_batch(general):
    b=basis(general);angles=np.array([.4,.8,1.4])
    actual=b.angular_coefficients(LABELS,angles,.7,P_vec=fields(b))
    expected=b.angular_coefficients(LABELS,angles,.7)
    assert actual.shape==(3,2)
    np.testing.assert_allclose(actual,expected,rtol=0,atol=1e-14)

@pytest.mark.parametrize('name,change',[('s1z',.01),('s2z',.01),('m1',1.),('s2x',.01)])
def test_changed_intrinsics_refused(name,change):
    b=basis();p=fields(b);p[name]+=change
    with pytest.raises(ValueError):b.angular_coefficients(LABELS,.8,.7,P_vec=p)


def test_both_spins_must_share_rotation():
    b=basis();p=fields(b);z=(p['s1x']+1j*p['s1y'])*np.exp(.2j)
    p.update(s1x=z.real,s1y=z.imag)
    with pytest.raises(ValueError,match='both spins'):
        b.angular_coefficients(LABELS,.8,.7,P_vec=p)


def test_incomplete_nonzero_rotation_refused():
    b=basis();z=(b.parameters.s1x+1j*b.parameters.s1y)*np.exp(.2j)
    with pytest.raises(ValueError,match='all Cartesian'):
        b.angular_coefficients(LABELS,.8,.7,P_vec=dict(s1x=z.real,s1y=z.imag))


def test_vectorized_rigid_rotation_agrees_with_scalar_batches():
    b=basis();p=fields(b);delta=np.array([.2,-.7,1.1]);inc=np.array([.4,.8,1.4]);phase=np.array([.1,1.,2.])
    for pre in ('s1','s2'):
        z=(p[pre+'x']+1j*p[pre+'y'])*np.exp(1j*delta)
        p[pre+'x'],p[pre+'y']=z.real,z.imag
    actual=b.angular_coefficients(LABELS,inc,phase,P_vec=p)
    for i in range(3):
        one={key:(value[i] if np.ndim(value) else value) for key,value in p.items()}
        expected=b.angular_coefficients(LABELS,inc[i],phase[i],P_vec=one)[0]
        np.testing.assert_allclose(actual[i],expected,rtol=0,atol=1e-14)


def test_fixed_cartesian_basis_refuses_nonzero_rotation():
    b=basis(False);p=fields(b)
    for pre in ('s1','s2'):
        z=(p[pre+'x']+1j*p[pre+'y'])*np.exp(.2j);p[pre+'x'],p[pre+'y']=z.real,z.imag
    with pytest.raises(ValueError,match='general_pe'):
        b.angular_coefficients(LABELS,.8,.7,P_vec=p)

@pytest.mark.parametrize('beta',[.3,.35])
def test_native_atan2_tolerance_at_observer_pole(beta):
    kappa,zeta,theta=_geometry(beta,.2,beta,np.pi/2-.2)
    assert kappa==0.
    assert np.isfinite(zeta) and np.isfinite(theta)


def test_changed_spin_reference_frequency_refused():
    b=basis();p=fields(b);p['fref']=30.
    with pytest.raises(ValueError,match='reference frequency'):
        b.angular_coefficients(LABELS,.8,.7,P_vec=p)


def test_native_zero_reference_resolves_before_generation_standoff():
    b=basis();p=fields(b);p['fref']=0.
    actual=b.angular_coefficients(LABELS,.8,.7,P_vec=p)
    expected=b.angular_coefficients(LABELS,.8,.7)
    np.testing.assert_allclose(actual,expected,rtol=0,atol=1e-14)


@pytest.mark.parametrize('epsilon',[0.,1e-15,1e-12])
def test_large_individual_spins_cancelling_native_j_frame_refused(epsilon):
    import lal
    p=SimpleNamespace(m1=60*lal.MSUN_SI,m2=30*lal.MSUN_SI,
        s1x=.1,s1y=.1,s1z=.2,s2x=-.4,s2y=-.4+epsilon,s2z=-.1,fref=20.)
    assert np.hypot(p.s1x,p.s1y)>.1 and np.hypot(p.s2x,p.s2y)>.5
    with pytest.raises(ValueError,match='degenerate native reference J'):
        _native_old_frame(p)


def test_nonzero_pe_rotation_at_new_j_observer_pole_refused():
    b=basis();p=fields(b);delta=.4
    for pre in ('s1','s2'):
        z=(p[pre+'x']+1j*p[pre+'y'])*np.exp(1j*delta)
        p[pre+'x'],p[pre+'y']=z.real,z.imag
    with pytest.raises(ValueError,match='PE observer.*pole'):
        b.angular_coefficients(LABELS,b.beta_new,np.pi/2-b.phiJ-delta,P_vec=p)


def test_fixed_cartesian_new_j_observer_pole_is_supported():
    b=basis()
    coefficients=b.angular_coefficients(LABELS,b.beta_new,np.pi/2-b.phiJ,P_vec=fields(b))
    assert np.all(np.isfinite(coefficients))


def test_old_j_tilt_resolution_guard_excludes_near_cancellation():
    import lal
    p=SimpleNamespace(m1=60*lal.MSUN_SI,m2=30*lal.MSUN_SI,
        s1x=.1,s1y=.1,s1z=.2,s2x=-.4,s2y=-.4+1e-6,s2z=-.1,fref=20.)
    with pytest.raises(ValueError,match='degenerate native reference J'):
        _native_old_frame(p)


@pytest.mark.parametrize('native_f0',[0.,-128.])
@pytest.mark.parametrize('no_condition',[False,True])
def test_source_frequency_metadata_normalization_preserves_waveform_bytes(monkeypatch,native_f0,no_condition):
    import lal
    from RIFT import lalsimutils as lsu
    p=lsu.ChooseWaveformParams();p.deltaT=1/256;p.deltaF=1/8;p.fmin=p.fref=20.
    n=int(1/(p.deltaT*p.deltaF));frequency=(np.arange(n+1)-n//2)*p.deltaF
    values=np.exp(-((frequency-50.)/9.)**2)*(1+.3j)+.2j*np.exp(-((frequency+70.)/7.)**2)
    def raw_components():
        h=lal.CreateCOMPLEX16FrequencySeries('native component',lal.LIGOTimeGPS(0),
            native_f0,p.deltaF,lal.DimensionlessUnit,n+1)
        h.data.data[:]=values
        return {LABELS[0]:h}
    options=dict(fd_standoff_factor=.9,fd_alignment_postevent_time=2.,no_condition=no_condition)
    # Independent pre-normalization replay uses the real shared conditioner.
    previous_time=lsu.condition_fd_mode_components(p,raw_components(),global_sign=1,**options)[LABELS[0]]
    previous_standard=lsu.DataFourier(previous_time)
    previous_conjugate_time=lal.CreateCOMPLEX16TimeSeries('conjugate',previous_time.epoch,
        previous_time.f0,previous_time.deltaT,previous_time.sampleUnits,n)
    previous_conjugate_time.data.data[:]=np.conj(previous_time.data.data)
    previous_conjugate=lsu.DataFourier(previous_conjugate_time)
    b=XPNRSourceBasis(conditioning_options={'no_condition':no_condition});b.parameters=p
    supplied=raw_components()
    monkeypatch.setattr(b,'generate_raw',lambda *args,**kwargs:supplied)
    standard,conjugate=b(p,4)
    for actual,previous in [(standard[LABELS[0]],previous_standard),(conjugate[LABELS[0]],previous_conjugate)]:
        assert actual.f0==-.5/p.deltaT
        assert lsu.DataInverseFourier(actual).f0==0.
        assert actual.data.data.tobytes()==previous.data.data.tobytes()
        assert actual.epoch==previous.epoch and actual.deltaF==previous.deltaF
    assert supplied[LABELS[0]].data.data.tobytes()==values.tobytes()
    assert supplied[LABELS[0]].f0==-.5/p.deltaT
