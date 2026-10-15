"""Periodic adaptation of the WTF frozen-policy regression objective.

Flat metric on unit-radius T^3 and standardized R^3. The semigroup term uses
periodic chord error; controls are ordinary tangent vectors with sum-square
norm. This is not a claim that the paper's Euclidean theorem extends here.
"""
import torch


def periodic_error_sum(predicted,target):
    d=predicted-target
    return 2*(1-torch.cos(d[...,:3])).sum(-1)+d[...,3:].square().sum(-1)


def control_cost(reference,base,x,t,tau):
    middle=reference.jump(x,t,tau)
    control=reference(middle,tau,tau)-base(middle,tau)
    return .5*(1-t)*control.square().sum(-1)


def estimate_value(reference,base,x,t,tau,reward,reward_scale):
    if x.ndim!=2 or t.shape!=(len(x),) or tau.shape!=t.shape:raise ValueError('x=(N,D),t=tau=(N,) required')
    if torch.any(t<0) or torch.any(t>1) or torch.any(tau<t) or torch.any(tau>1):raise ValueError('0 <= t <= tau <= 1 required')
    middle=reference.jump(x,t,tau)
    endpoint=reference.jump(middle,tau,torch.ones_like(t))
    control=reference(middle,tau,tau)-base(middle,tau)
    r=reward(endpoint)
    if r.shape!=t.shape:raise ValueError('reward must have shape (N,)')
    return reward_scale*r-.5*(1-t)*control.square().sum(-1)


def value_gradient(reference,base,x,t,tau,reward,reward_scale):
    leaf=x.detach().requires_grad_(True)
    values=estimate_value(reference,base,leaf,t,tau,reward,reward_scale)
    gradient=torch.autograd.grad(values.sum(),leaf,allow_unused=True)[0] if values.requires_grad else None
    if gradient is None:gradient=torch.zeros_like(leaf)
    return gradient.detach(),values.detach()


def wtf_loss(student,reference,base,z,t,tau,reward,reward_scale=.001,beta=1.,generator=None):
    with torch.no_grad():xt=reference.jump(z,torch.zeros_like(t),t)
    target,values=value_gradient(reference,base,xt,t,tau,reward,reward_scale)
    control=student(xt.detach(),t,t)-base(xt.detach(),t).detach()
    diagonal=(control-target).square().sum(-1).mean()
    times=torch.rand((len(z),3),dtype=z.dtype,generator=generator).sort(-1).values
    s,u,v=times.unbind(-1)
    with torch.no_grad():
        xs=reference.jump(z,torch.zeros_like(s),s)
        stopped=reference.jump(reference.jump(xs,s,u),u,v)
    predicted=student.jump(xs,s,v)
    semigroup=periodic_error_sum(predicted,stopped).mean()
    return diagonal+beta*semigroup,dict(diagonal=diagonal.detach(),semigroup=semigroup.detach(),value=values.mean())
