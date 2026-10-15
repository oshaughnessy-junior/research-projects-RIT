import math
import pytest
import torch
from periodic_model import PeriodicVelocity,frozen,base_noise
from periodic_wtf import periodic_error_sum,estimate_value,value_gradient,wtf_loss


class Identity:
    def jump(self,x,s,t):return x
    def __call__(self,x,s,t=None):return torch.zeros_like(x)


def test_chord_consistency_and_ordinary_tangent_cost():
    x=torch.zeros(2,6,dtype=torch.float64);y=x.clone();y[:,0]=2*math.pi
    torch.testing.assert_close(periodic_error_sum(x,y),torch.zeros(2,dtype=x.dtype),atol=1e-14,rtol=0)
    class Constant(Identity):
        def __call__(self,x,s,t=None):
            v=torch.zeros_like(x);v[:,0]=2*math.pi;return v
        def jump(self,x,s,t):return x+(t-s)[:,None]*self(x,s,t)
    t=torch.full((2,),.25,dtype=x.dtype);tau=t+.25
    value=estimate_value(Constant(),Identity(),x,t,tau,lambda y:y[:,0]*0,1.)
    torch.testing.assert_close(value,torch.full((2,),-.5*.75*(2*math.pi)**2,dtype=x.dtype))


def test_base_input_derivative_is_not_detached():
    class LinearBase:
        def __call__(self,x,t):return x
    x=torch.ones(2,6,dtype=torch.float64);t=torch.full((2,),.25,dtype=x.dtype);tau=t+.25
    grad,_=value_gradient(Identity(),LinearBase(),x,t,tau,lambda y:y[:,0]*0,1.)
    torch.testing.assert_close(grad,-.75*x,atol=1e-14,rtol=0)


def test_two_lookahead_gradient_matches_full_value_finite_difference():
    torch.manual_seed(261043)
    ref=frozen(PeriodicVelocity(two_time=True).double());base=frozen(PeriodicVelocity().double())
    x=base_noise(3);t=torch.full((3,),.3,dtype=x.dtype);tau=t+.35
    reward=lambda y:torch.cos(y[:,:3]).sum(-1)-y[:,3:].square().sum(-1)/2
    grad,_=value_gradient(ref,base,x,t,tau,reward,.001)
    for d in range(6):
        dx=torch.zeros_like(x);dx[:,d]=1e-5
        fd=(estimate_value(ref,base,x+dx,t,tau,reward,.001)-estimate_value(ref,base,x-dx,t,tau,reward,.001))/(2e-5)
        torch.testing.assert_close(grad[:,d],fd,atol=1e-8,rtol=1e-6)


def test_value_gradient_and_loss_are_lattice_invariant():
    torch.manual_seed(261044)
    model=PeriodicVelocity(two_time=True).double();ref=frozen(model);base=frozen(PeriodicVelocity().double())
    z=base_noise(4);t=torch.full((4,),.25,dtype=z.dtype);tau=t+.4
    shift=torch.tensor([2*math.pi,-4*math.pi,6*math.pi,0,0,0],dtype=z.dtype)
    reward=lambda y:torch.cos(y[:,:3]).sum(-1)-y[:,3:].square().sum(-1)/2
    a,av=value_gradient(ref,base,z,t,tau,reward,.001)
    b,bv=value_gradient(ref,base,z+shift,t,tau,reward,.001)
    torch.testing.assert_close(a,b,atol=1e-12,rtol=1e-12);torch.testing.assert_close(av,bv,atol=1e-12,rtol=1e-12)
    loss,parts=wtf_loss(model,ref,base,z,t,tau,reward,generator=torch.Generator().manual_seed(17))
    other,_=wtf_loss(model,ref,base,z+shift,t,tau,reward,generator=torch.Generator().manual_seed(17))
    torch.testing.assert_close(loss,other,atol=1e-12,rtol=1e-12)
    loss.backward();assert all(p.grad is not None and torch.isfinite(p.grad).all() for p in model.parameters())
    assert all(p.grad is None for p in ref.parameters()) and all(p.grad is None for p in base.parameters())


def test_linear_subspace_zero_drift_analytic_gradient():
    x=torch.ones(2,6,dtype=torch.float64);t=torch.full((2,),.25,dtype=x.dtype);tau=t+.25
    grad,_=value_gradient(Identity(),Identity(),x,t,tau,lambda y:-(y[:,3]-2).square()/2,.001)
    expected=torch.zeros_like(x);expected[:,3]=.001
    torch.testing.assert_close(grad,expected,atol=1e-14,rtol=0)


def test_reward_shapes_and_time_domains_are_explicit():
    x=torch.ones(2,6,dtype=torch.float64);t=torch.full((2,),.25,dtype=x.dtype)
    with pytest.raises(ValueError,match='shape'):estimate_value(Identity(),Identity(),x,t,t+.1,lambda y:y[:,:1],1.)
    with pytest.raises(ValueError,match='tau'):estimate_value(Identity(),Identity(),x,t,t-.1,lambda y:y[:,0],1.)
