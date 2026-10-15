"""Primitive certificate regressions; no waveform or physical likelihood calls."""
import os
import numpy as np
import pytest
jax=pytest.importorskip('jax');jax.config.update('jax_enable_x64',True)
import jax.numpy as jnp
from RIFT.likelihood.jax_ile import core



def linear(slope):
    guard=128;n=17
    k=slope*(jnp.arange(n+2*guard,dtype=jnp.float64)-guard)
    return k[None].astype(jnp.complex128),jnp.zeros((1,n+2*guard)),guard


def value(slope,endpoint=None):
    k,r,g=linear(slope)
    return core._time_marginalize_reflected_primitive(k,r,1.,guard=g,endpoint_log_gap=endpoint)[0]


def test_additional_doubling_recovers_convergent_linear_field():
    # Analytic exp(linear) integral. Trapezoid1->2 differs ~.0025 nat,
    # so the first certificate fails; 2->4 differs ~.000625 and passes.
    actual=float(value(.2));truth=np.log(np.expm1(.2*16)/.2)
    assert np.isfinite(actual)
    assert abs(actual-truth)<.001


def test_retry_gradient_matches_finite_difference():
    f=jax.jit(value);g=float(jax.grad(value)(.2));h=1e-5
    fd=(float(f(.2+h))-float(f(.2-h)))/(2*h)
    assert np.isfinite(g)
    assert g==pytest.approx(fd,rel=1e-6,abs=1e-7)


def test_already_accepted_constant_value_and_gradient():
    def constant(a):
        return core._time_marginalize_reflected_primitive(jnp.full((1,17),a,dtype=jnp.complex128),jnp.zeros((1,17)),1.,guard=0)[0]
    v,g=jax.value_and_grad(constant)(2.)
    assert float(v)==pytest.approx(2+np.log(16),abs=1e-10)
    assert float(g)==pytest.approx(1.,abs=1e-10)


def test_failed_endpoint_certificate_is_not_bypassed():
    assert np.isnan(float(value(0.,endpoint=10.)))


def test_retry_exhaustion_still_refuses():
    # A steep boundary field is not converged after either allowed pair.
    assert np.isnan(float(value(5.)))


def test_recorded_private_primitive_when_available():
    path=os.environ.get('RESEARCH_PRIMITIVE_FIXTURE')
    if not path:pytest.skip('private scientific fixture stays on Caltech')
    d=np.load(path);k=jnp.asarray(d['kappa']);r=jnp.asarray(d['rho'])
    v=core._time_marginalize_reflected_primitive(k,r,float(d['deltaT']),guard=int(d['guard']))
    # Independent SciPy FFT plus Simpson reference on a 32x fine grid.
    from scipy.signal import resample
    from scipy.integrate import simpson
    guard=int(d['guard']);n=k.shape[1]-2*guard;row=np.asarray(k[0]);ramp=.5*(1-np.cos(np.pi*np.arange(guard+1)/guard))
    tapered=row*np.r_[ramp[:-1],np.ones(n),ramp[:-1][::-1]]
    reflected=np.r_[tapered,tapered[1:-1][::-1]]
    dense=resample(reflected,len(reflected)*32)
    field=dense[guard*32:guard*32+(n-1)*32+1].real-.5*float(r[0,0]);m=field.max()
    reference=m+np.log(simpson(np.exp(field-m),dx=float(d['deltaT'])/32))
    assert reference==pytest.approx(47.829242649495434,abs=1e-9)
    assert float(v[0])==pytest.approx(reference,abs=1e-9)


def test_retry_respects_fine_grid_ceiling(monkeypatch):
    # Initial factor1 is permitted, but its 4x retry exceeds fine ceiling2.
    monkeypatch.setattr(core,'_TIME_ADAPTIVE_FACTOR_MAX',1)
    assert np.isnan(float(value(.2)))
