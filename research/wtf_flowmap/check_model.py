"""Cheap mathematical-contract tests; none establish real inference capability."""
import torch
import pytest
from model import Velocity, frozen, value_gradient, wtf_loss


class ZeroDrift(torch.nn.Module):
    def forward(self, x, t):
        return torch.zeros_like(x)


class IdentityMap(torch.nn.Module):
    def jump(self, x, s, t):
        return x
    def forward(self, x, s, t):
        return torch.zeros_like(x)


def test_exact_value_gradient_and_frozen_weights():
    torch.manual_seed(3)
    model = Velocity(2, two_time=True).double()
    ref = frozen(model).double()
    for original, copied in zip(model.parameters(), ref.parameters()):
        torch.testing.assert_close(original, copied, rtol=0, atol=0)
    base = frozen(Velocity(2).double()).double()
    x = torch.randn(4, 2, dtype=torch.float64)
    t = torch.full((4,), .25, dtype=torch.float64)
    tau = torch.full((4,), .7, dtype=torch.float64)
    reward = lambda y: -(y-1.).square().sum(-1)/2
    g, _ = value_gradient(ref, base, x, t, tau, reward, .4)
    # Independent finite difference of the complete mathematical value.
    def value(v):
        a = ref.jump(v, t, tau)
        b = ref.jump(a, tau, torch.ones_like(t))
        c = ref(a, tau, tau)-base(a, tau)
        return .4*reward(b)-.5*(1-t)*c.square().sum(-1)
    for d in range(2):
        dx = torch.zeros_like(x); dx[:, d] = 1e-5
        numerical = (value(x+dx)-value(x-dx))/(2e-5)
        torch.testing.assert_close(g[:, d], numerical, atol=1e-7, rtol=1e-6)
    assert all(p.grad is None for p in ref.parameters())
    loss, _ = wtf_loss(model, ref, base, x, t, tau, reward, .4)
    loss.backward()
    assert all(p.grad is not None and torch.isfinite(p.grad).all() for p in model.parameters())


def test_zero_drift_reward_gradient_and_identity_jump():
    x = torch.tensor([[.2], [1.4]], dtype=torch.float64)
    t = torch.tensor([.1, .7], dtype=torch.float64)
    tau = (1+t)/2
    g, _ = value_gradient(IdentityMap(), ZeroDrift(), x, t, tau,
                           lambda y: -(y[:, 0]-2.).square()/1., .3)
    torch.testing.assert_close(g, .6*(2.-x))
    m = Velocity(1, two_time=True).double()
    torch.testing.assert_close(m.jump(x, t, t), x, rtol=0, atol=0)


def test_reward_and_time_contracts_reject_silent_broadcasting():
    x=torch.ones(3,2); t=torch.full((3,),.2); tau=torch.full((3,),.7)
    with pytest.raises(ValueError,match='one scalar'):
        value_gradient(IdentityMap(),ZeroDrift(),x,t,tau,lambda y:y[:,0:1],1.)
    with pytest.raises(ValueError,match='0 <= t'):
        value_gradient(IdentityMap(),ZeroDrift(),x,t,torch.zeros(3),lambda y:y[:,0],1.)


def test_constant_reward_zero_control_has_zero_value_gradient():
    x=torch.ones(3,2); t=torch.full((3,),.2); tau=torch.full((3,),.7)
    g,_=value_gradient(IdentityMap(),ZeroDrift(),x,t,tau,lambda y:torch.zeros(len(y)),1.)
    torch.testing.assert_close(g,torch.zeros_like(x),rtol=0,atol=0)
