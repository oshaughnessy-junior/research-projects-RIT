"""Learned velocities and two-time jumps on T^3 x R^3.

State = (RA, phase, 2*psi, standardized atanh(sin dec),
standardized atanh(cos incl), standardized log distance).
Angles are continuous lifts; there is no remainder at the network output.
No normalized density, physical likelihood, or posterior-draw contract.
"""
from copy import deepcopy
import math
import torch
from torch import nn


def features(x):
    angular=torch.stack((torch.sin(x[...,:3]),torch.cos(x[...,:3])),dim=-1).flatten(-2)
    return torch.cat((angular,x[...,3:]),dim=-1)


class PeriodicVelocity(nn.Module):
    def __init__(self,two_time=False,width=64,meter=None):
        super().__init__();self.two_time=two_time;self.width=width;self.meter=meter
        self.net=nn.Sequential(nn.Linear(9+(2 if two_time else 1),width),nn.Tanh(),
                               nn.Linear(width,width),nn.Tanh(),nn.Linear(width,6))

    def forward(self,x,s,t=None):
        if x.shape[-1]!=6 or self.two_time!=(t is not None):raise ValueError('wrong state/time contract')
        if self.meter:self.meter.add('neural_forward_rows',len(x))
        times=[s.reshape(-1,1)]+([] if t is None else [t.reshape(-1,1)])
        return self.net(torch.cat([features(x)]+times,dim=-1))

    def jump(self,x,s,t):
        if not self.two_time:raise ValueError('drift is not a two-time map')
        return x+(t-s)[:,None]*self(x,s,t)


def frozen(model):
    result=deepcopy(model);result.meter=model.meter
    result.requires_grad_(False)
    return result


def base_noise(n,dtype=torch.float64,generator=None):
    angles=(torch.rand(n,3,dtype=dtype,generator=generator)*2-1)*math.pi
    linear=torch.randn(n,3,dtype=dtype,generator=generator)
    return torch.cat((angles,linear),dim=-1)


def angular_delta(end,start):
    delta=torch.atan2(torch.sin(end-start),torch.cos(end-start))
    # Exact antipodal tie convention +pi; a.e. derivative checks exclude ties.
    return torch.where(torch.abs(delta)==math.pi,torch.full_like(delta,math.pi),delta)


def displacement(end,start):
    return torch.cat((angular_delta(end[...,:3],start[...,:3]),end[...,3:]-start[...,3:]),dim=-1)


def interpolant(start,end,t):
    delta=displacement(end,start)
    return start+t[:,None]*delta,delta


def chord_loss(predicted,target):
    error=predicted-target
    return torch.cat((2*(1-torch.cos(error[...,:3])),error[...,3:].square()),dim=-1).mean()


def rms(predicted,target):
    return displacement(predicted,target).square().mean().sqrt()


def decode(state,center,scale):
    """Differentiable lifted-angle decoder; guards are a separate diagnostic."""
    raw=state[...,3:]*scale+center
    return torch.stack((state[...,0],torch.asin(torch.tanh(raw[...,0])),state[...,2]/2,
                        torch.acos(torch.tanh(raw[...,1])),state[...,1],torch.exp(raw[...,2])),dim=-1)


def support_mask(physical):
    return (torch.isfinite(physical).all(-1)&(physical[...,5]>1)&(physical[...,5]<10000)&
            (physical[...,1].abs()<math.pi/2-1e-8)&(physical[...,3]>1e-8)&
            (physical[...,3]<math.pi-1e-8))
