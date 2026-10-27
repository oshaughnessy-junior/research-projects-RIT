"""Independent harmonic, quadrupole, geometry and native contract checks."""
from dataclasses import replace
import math
from types import MappingProxyType

import numpy as np
import pytest

from RIFT.LISA import bhptnr_adapter as ba
from RIFT.LISA import bhptnr_polarizations as bp


def finite_harmonic(ell,m,theta,phi):
    """Independent finite Wigner sum, with column index -spin=2."""
    n=2
    norm=math.sqrt(math.factorial(ell+n)*math.factorial(ell-n)*
                   math.factorial(ell+m)*math.factorial(ell-m))
    c,s=math.cos(theta/2),math.sin(theta/2)
    terms=[]
    for k in range(max(0,n-m),min(ell+n,ell-m)+1):
        denominator=(math.factorial(ell+n-k)*math.factorial(k)*
                     math.factorial(m-n+k)*math.factorial(ell-m-k))
        terms.append((-1)**(m-n+k)*norm/denominator*c**(2*ell+n-m-2*k)*s**(m-n+2*k))
    return math.sqrt((2*ell+1)/(4*math.pi))*math.fsum(terms)*np.exp(1j*m*phi)



def native():
    times = np.array([-3., -1., 0., 2.])
    modes = {k:np.zeros(4,dtype=complex) for k in ba.POSITIVE_MODES}
    modes[2,2] = np.array([1+2j, 2-3j, -1+.5j, .25-.3j])*1e-20
    metadata = dict(model='BHPTNRSur1dq1e4',source_revision=ba.SOURCE_REVISION,
        data_sha256=ba.DATA_SHA256,mass_scale='M',time_units='seconds',mode_units='strain',
        positive_modes=[list(k) for k in ba.POSITIVE_MODES],
        time_origin='native surrogate time origin; no observation epoch assigned',
        n_samples=4,first_time_seconds=-3.,last_time_seconds=2.,neg_modes=False,
        mode_sum=False,calibrated=False,parameters=dict(primary_msun=1e6,secondary_msun=200.,distance_gpc=1.),
        warnings=['preserved warning'])
    return ba.BHPTNRModes(times,MappingProxyType(modes),metadata)


@pytest.mark.parametrize('theta',[0.,.2,1.3,np.pi/2,np.pi])
def test_all_harmonics_against_finite_wigner_sum(theta):
    for ell in range(2,11):
        values = []
        for m in range(-ell,ell+1):
            actual = bp.spin_weighted_harmonic(ell,m,theta,.73)
            expected = finite_harmonic(ell,m,theta,.73)
            assert abs(actual-expected) < 3e-13
            values.append(abs(actual)**2)
        assert sum(values) == pytest.approx((2*ell+1)/(4*np.pi),abs=3e-14)


@pytest.mark.parametrize('theta',[0.,.8,np.pi/2,np.pi])
def test_quadrupole_closed_form(theta):
    n = native(); phi=.37
    actual = bp.source_polarizations(n,inclination=theta,observer_azimuth=phi)
    h = n.positive_modes[2,2]*np.exp(2j*phi)
    a = np.sqrt(5/(64*np.pi)); c=np.cos(theta)
    expected = np.array([2*a*(1+c*c)*h.real,-4*a*c*h.imag])
    np.testing.assert_allclose(actual.polarizations,expected,rtol=0,atol=2e-35)
    np.testing.assert_array_equal(actual.time_seconds,n.time_seconds)


def test_all_mode_synthesis_independent_and_copy():
    n = native()
    for i,k in enumerate(ba.POSITIVE_MODES):
        n.positive_modes[k][:] = (i+1)*np.array([1+2j,3-1j,2+.1j,-4+2j])*1e-22
    theta,phi=.9,1.2
    expected = sum(v*finite_harmonic(l,m,theta,phi)+(-1)**l*v.conjugate()*finite_harmonic(l,-m,theta,phi)
                   for (l,m),v in n.positive_modes.items())
    out = bp.source_polarizations(n,inclination=theta,observer_azimuth=phi)
    np.testing.assert_allclose(out.polarizations[0]-1j*out.polarizations[1],expected,rtol=0,atol=1e-33)
    n.positive_modes[2,2][:]=0; n.time_seconds[:]=1; n.metadata['warnings'].append('later')
    assert out.metadata['warnings'] == ['preserved warning']
    assert out.time_seconds[0] == -3
    assert not out.time_seconds.flags.writeable and not out.polarizations.flags.writeable
    assert len(out.metadata['synthesis_inputs']['mode_complex128_sha256']) == 25


def test_ssb_geometry_against_independent_vector_basis():
    n=native(); qS,phiS,qL,phiL,phase=1.,2.,.7,.3,.2
    # Cross products construct the observer basis independently of its component formula.
    sky=np.array([np.sin(qS)*np.cos(phiS),np.sin(qS)*np.sin(phiS),np.cos(qS)])
    axis=np.array([np.sin(qL)*np.cos(phiL),np.sin(qL)*np.sin(phiL),np.cos(qL)])
    east=np.cross([0.,0.,1.],sky); east/=np.linalg.norm(east)
    south=np.cross(east,sky)
    psi=np.arctan2(axis@east,axis@south)
    theta=np.arccos(-axis@sky); phi=-np.pi/2-phase
    expected=sum(v*finite_harmonic(l,m,theta,phi)+(-1)**l*v.conjugate()*finite_harmonic(l,-m,theta,phi)
                 for (l,m),v in n.positive_modes.items())*np.exp(2j*psi)
    out=bp.ssb_polarizations(n,qS=qS,phiS=phiS,qL=qL,phiL=phiL,orbital_phase_offset=phase)
    np.testing.assert_allclose(out.polarizations[0]-1j*out.polarizations[1],expected,rtol=0,atol=3e-35)
    equivalent=bp.ssb_polarizations(n,qS=qS,phiS=phiS,qL=qL,phiL=phiL,orbital_phase_offset=phase+2*np.pi)
    np.testing.assert_allclose(equivalent.polarizations,out.polarizations,rtol=0,atol=3e-35)


@pytest.mark.parametrize('field,value',[('model','other'),('mode_units','geometric'),
    ('source_revision','bad'),('data_sha256','bad'),('n_samples',5),('mode_sum',True),
    ('neg_modes',True),('calibrated',1),('last_time_seconds',3.)])
def test_metadata_mismatch_refused(field,value):
    n=native(); n.metadata[field]=value
    with pytest.raises(ValueError):bp.source_polarizations(n,inclination=1.,observer_azimuth=0.)


@pytest.mark.parametrize('kind',['collapsed','nonfinite','missing','negative','bad_mode','mode_overflow'])
def test_malformed_arrays_refused(kind):
    n=native()
    if kind=='collapsed': n=replace(n,time_seconds=np.array([2**60+i for i in range(4)],dtype=np.int64))
    if kind=='nonfinite': n.time_seconds[1]=np.nan
    if kind=='missing': n=replace(n,positive_modes={k:v for k,v in n.positive_modes.items() if k!=(2,1)})
    if kind=='negative': n=replace(n,positive_modes={**n.positive_modes,(2,-2):np.zeros(4)})
    if kind=='bad_mode': n.positive_modes[2,2][0]=np.inf
    if kind=='mode_overflow':
        extended = np.longdouble(np.finfo(float).max)
        if np.finfo(np.longdouble).max > np.finfo(float).max: extended *= 2
        else: extended = np.longdouble(np.inf)
        n=replace(n,positive_modes={**n.positive_modes,(2,2):np.full(4,extended)})
    with pytest.raises(ValueError):bp.source_polarizations(n,inclination=1.,observer_azimuth=0.)


@pytest.mark.parametrize('angle,value',[('qS',True),('phiS',-1.),('phiL',2*np.pi),('qL',4.),('orbital_phase_offset',np.inf)])
def test_bad_geometry_refused(angle,value):
    geometry=dict(qS=1.,phiS=2.,qL=.7,phiL=.3,orbital_phase_offset=.2);geometry[angle]=value
    with pytest.raises(ValueError):bp.ssb_polarizations(native(),**geometry)


def test_aligned_geometry_refused():
    with pytest.raises(ValueError,match='gauge'):
        bp.ssb_polarizations(native(),qS=1.,phiS=2.,qL=1.,phiL=2.,orbital_phase_offset=0.)


@pytest.mark.parametrize('theta,phi',[(True,0.),(1.,np.nan),(-1.,0.)])
def test_bad_source_angles_refused(theta,phi):
    with pytest.raises(ValueError):bp.source_polarizations(native(),inclination=theta,observer_azimuth=phi)


def test_historical_upstream_mode_sum_checkpoints():
    import json
    from pathlib import Path
    fixture=json.loads((Path(__file__).parent/'fixtures/bhptnr_source_mode_sum.json').read_text())
    def decode(values):
        a=np.array(values);return a[:,0]+1j*a[:,1]
    n=native();times=np.array(fixture['times_seconds'])
    modes={tuple(map(int,k.split(','))):decode(v) for k,v in fixture['positive_modes'].items()}
    n.metadata.update(parameters=fixture['binary'],n_samples=len(times),
                      first_time_seconds=times[0],last_time_seconds=times[-1])
    n=replace(n,time_seconds=times,positive_modes=modes)
    assert fixture['source_revision']==ba.SOURCE_REVISION
    assert fixture['data_sha256']==ba.DATA_SHA256
    for (theta,phi),values,peak in zip(fixture['source_views'],fixture['upstream_mode_sum_references'],
                                     fixture['reference_full_array_peaks'],strict=True):
        out=bp.source_polarizations(n,inclination=theta,observer_azimuth=phi)
        actual=out.polarizations[0]-1j*out.polarizations[1]
        assert np.max(np.abs(actual-decode(values)))/peak <= fixture['relative_error_max']


@pytest.mark.parametrize('antialigned',[False,True])
def test_near_alignment_preserves_nonquadrupole_inclination(antialigned):
    n=native()
    for v in n.positive_modes.values():v[:]=0
    n.positive_modes[2,1][:]=1e-20*(1+2j)
    qS,phiS,delta=1.,2.,1e-9
    qL=(np.pi-qS if antialigned else qS)+delta
    phiL=phiS+np.pi if antialigned else phiS
    expected_theta=abs(qL-(np.pi-qS)) if antialigned else np.pi-abs(qL-qS)
    out=bp.ssb_polarizations(n,qS=qS,phiS=phiS,qL=qL,phiL=phiL,orbital_phase_offset=0.)
    assert out.metadata['inclination']==pytest.approx(expected_theta,abs=6e-16,rel=0)
    # Inclination, independently known from these meridians, controls the small21 signal.
    source=bp.source_polarizations(n,inclination=expected_theta,observer_azimuth=-np.pi/2)
    actual=np.hypot(*out.polarizations)
    expected=np.hypot(*source.polarizations)
    np.testing.assert_allclose(actual,expected,rtol=1e-6,atol=0)
