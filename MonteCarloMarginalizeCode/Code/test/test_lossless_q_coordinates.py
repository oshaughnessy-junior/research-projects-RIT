"""Lossless transverse chart invariants, using actual LAL object/vector paths."""
from pathlib import Path
import sys
import numpy as np
import pytest
lal=pytest.importorskip('lal')
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from RIFT import lalsimutils as m

PHYSICAL=['m1','m2','s1x','s1y','s1z','s2x','s2y','s2z']
CHART=list(m.PRECESSION_Q_COORDINATES)

@pytest.mark.parametrize('frequency',[20.,35.])
def test_roundtrip_over_mass_ratio_and_spin_domain(frequency):
    rng=np.random.default_rng(271828);n=10000
    m1=rng.uniform(5,200,n);m2=m1*rng.uniform(.05,1,n)
    spins=[]
    for _ in range(2):
        s=rng.normal(size=(n,3))
        s*=rng.uniform(0,.99,n)[:,None]/np.linalg.norm(s,axis=1)[:,None]
        spins.append(s)
    f=m.precession_q_forward(m1,m2,*spins,reference_frequency=frequency)
    a,b=m.precession_q_inverse(m1,m2,spins[0][:,2],spins[1][:,2],f,frequency)
    np.testing.assert_allclose(a,spins[0],atol=1e-10,rtol=0)
    np.testing.assert_allclose(b,spins[1],atol=1e-10,rtol=0)


def test_equal_mass_cancellation_retains_residual():
    a=np.array([.4,.2,.1]);b=np.array([-.4,-.2,-.2])
    f=m.precession_q_forward(30,30,a,b)
    assert f[0]==f[1]==0
    x,y=m.precession_q_inverse(30,30,a[2],b[2],f)
    np.testing.assert_allclose(x,a,atol=1e-14)
    np.testing.assert_allclose(y,b,atol=1e-14)


def test_negative_D_and_rejected_domain():
    a=np.array([.01,.02,-.99]);b=np.array([.01,-.02,-.99])
    f=m.precession_q_forward(1000,100,a,b)
    x,y=m.precession_q_inverse(1000,100,a[2],b[2],f)
    np.testing.assert_allclose(x,a,atol=1e-10)
    np.testing.assert_allclose(y,b,atol=1e-10)
    with pytest.raises(ValueError,match='physical chart domain'):
        m.precession_q_inverse(1000,100,-.99,-.99,[0,0,0,0])


@pytest.mark.parametrize('redshift',[0,.4])
def test_object_vector_frequency_and_native_parity(redshift):
    low=np.array([[10,.2,.5,.7,-.2,.4,1,2],[15,.1,0,0,1,-1,0,0]])
    names=['mc','delta_mc','chi1','chi2','cos_theta1','cos_theta2','phi1','phi2']
    physical=m.convert_waveform_coordinates(low,coord_names=PHYSICAL,
        low_level_coord_names=names,source_redshift=redshift)
    native=['delta_mc','mu1','mu2','chiMinus']
    expected=m.convert_waveform_coordinates(low,coord_names=native,
        low_level_coord_names=names,source_redshift=redshift)
    out=m.convert_waveform_coordinates(low,coord_names=native+CHART,
        low_level_coord_names=names,source_redshift=redshift,reference_frequency=35)
    np.testing.assert_array_equal(out[:,:4],expected)
    for i,row in enumerate(physical):
        p=m.ChooseWaveformParams(m1=row[0]*lal.MSUN_SI,m2=row[1]*lal.MSUN_SI,
            s1x=row[2],s1y=row[3],s1z=row[4],s2x=row[5],s2y=row[6],s2z=row[7],fref=35)
        np.testing.assert_allclose(out[i,4:],[p.extract_param(k) for k in CHART],atol=1e-14)
    other=m.convert_waveform_coordinates(low[:1],coord_names=CHART,
        low_level_coord_names=names,source_redshift=redshift,reference_frequency=20)
    assert not np.allclose(other[0],out[0,4:])


def test_mixed_kerr_batch_and_zero_spin_undefined_angles():
    low=np.array([[10,.2,.5,.7,-.2,.4,1,2],[10,.2,1.1,.7,-.2,.4,1,2],
                  [10,.2,0,0,np.nan,np.nan,np.nan,np.nan]])
    names=['mc','delta_mc','chi1','chi2','cos_theta1','cos_theta2','phi1','phi2']
    out=m.convert_waveform_coordinates(low,coord_names=CHART,low_level_coord_names=names,enforce_kerr=True)
    assert np.isfinite(out[0]).all() and np.isneginf(out[1]).all()
    np.testing.assert_array_equal(out[2],0)


def test_actual_cip_parser_switches_fit_basis_only(monkeypatch,tmp_path):
    import contextlib,io,runpy,os
    from test_rf_transverse_spin import CIP_BASE
    script=Path(__file__).resolve().parents[1]/'bin/util_ConstructIntrinsicPosterior_GenericCoordinates.py'
    args=[s if s!='physics3' else 'lossless-q' for s in CIP_BASE]
    for name in ['mc','chi1','chi2','cos_theta1','cos_theta2','phi1','phi2']:
        args+=['--parameter-nofit',name]
    grid=tmp_path/'g.dat';grid.write_text('0 20 10 0 0 0 0 0 0 10 0.1 100 1000\n')
    monkeypatch.chdir(tmp_path);monkeypatch.setenv('GW_SURROGATE','')
    monkeypatch.setattr(sys,'argv',[str(script),'--fname',str(grid),'--no-plots']+args)
    stop=next(i for i,line in enumerate(script.read_text().splitlines(),1)
        if line=="if 'q' in low_level_coord_names and 'mc' in low_level_coord_names:")
    class BasisReady(Exception):pass
    result={}
    def trace(frame,event,arg):
        if event=='line' and frame.f_code.co_filename==str(script) and frame.f_lineno==stop:
            result.update(fit=list(frame.f_globals['coord_names']),
                          sample=list(frame.f_globals['low_level_coord_names']))
            raise BasisReady
        return trace
    previous=sys.gettrace();sys.settrace(trace)
    try:
        with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
            with pytest.raises(BasisReady):runpy.run_path(str(script),run_name='__main__')
    finally:sys.settrace(previous)
    assert result['fit']==['delta_mc','mu1','mu2','chiMinus']+CHART
    assert set(result['sample'])=={'mc','delta_mc','chi1','chi2','cos_theta1','cos_theta2','phi1','phi2'}
    assert not set(CHART).intersection(result['sample'])
