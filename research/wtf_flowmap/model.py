"""Small learned flow maps and the mathematical WTF-v2 objective.

Independent low-dimensional implementation of arXiv:2609.27033v2 Eq. 18/19,
with exact reward pullback and sum-square control norm. No image dependencies.
These implicit maps have NO normalized density or posterior-sampling contract.
"""
from copy import deepcopy
from collections import Counter
import torch
from torch import nn


class Meter:
    def __init__(self):
        self.counts = Counter()
        self.phase = 'unclassified'

    def add(self, kind, rows):
        self.counts[self.phase + ':' + kind] += int(rows)

    def snapshot(self):
        return dict(self.counts)


class Velocity(nn.Module):
    def __init__(self, dim, two_time=False, width=64, meter=None):
        super().__init__()
        self.dim, self.two_time, self.width = dim, two_time, width
        self.meter = meter
        self.net = nn.Sequential(nn.Linear(dim + (2 if two_time else 1), width),
                                 nn.Tanh(), nn.Linear(width, width), nn.Tanh(),
                                 nn.Linear(width, dim))

    def forward(self, x, s, t=None):
        if self.two_time != (t is not None):
            raise ValueError('drift takes one time; map velocity takes two')
        if self.meter is not None:
            self.meter.add('neural_forward_rows', len(x))
        times = [s.reshape(-1, 1)] + ([] if t is None else [t.reshape(-1, 1)])
        return self.net(torch.cat([x] + times, dim=-1))

    def jump(self, x, s, t):
        if not self.two_time:
            raise ValueError('a drift is not a two-time map')
        return x + (t-s).reshape(-1, 1)*self(x, s, t)


def frozen(model):
    # Share the ledger, not mutable parameters. Frozen weights retain input AD.
    result = Velocity(model.dim, model.two_time, model.width, model.meter)
    parameter = next(model.parameters())
    result = result.to(device=parameter.device, dtype=parameter.dtype)
    result.load_state_dict(deepcopy(model.state_dict()))
    result.requires_grad_(False)
    return result


def rk4(drift, x, s, t, steps=32):
    """Teacher generation only; this rollout cost must be accounted separately."""
    h = (t-s)/steps
    for k in range(steps):
        time = s + k*h
        k1 = drift(x, time)
        k2 = drift(x + .5*h[:, None]*k1, time+.5*h)
        k3 = drift(x + .5*h[:, None]*k2, time+.5*h)
        k4 = drift(x + h[:, None]*k3, time+h)
        x = x + h[:, None]*(k1+2*k2+2*k3+k4)/6
    return x


def value_gradient(reference, base, x, t, tau, reward, reward_scale):
    """Frozen-policy MC value gradient, with exact input Jacobians.

    Frozen base PARAMETERS do not mean a detached base INPUT derivative.
    reward must return one differentiable scalar per row and meter every actual
    true/surrogate evaluator call itself. No reward normalization or jitter.
    """
    if x.ndim != 2 or t.shape != (len(x),) or tau.shape != t.shape:
        raise ValueError('expected x=(N,D), t=tau=(N,)')
    if torch.any(t < 0) or torch.any(t > 1) or torch.any(tau < t) or torch.any(tau > 1):
        raise ValueError('WTF value estimator requires 0 <= t <= tau <= 1')
    leaf = x.detach().requires_grad_(True)
    xtau = reference.jump(leaf, t, tau)
    endpoint = reference.jump(xtau, tau, torch.ones_like(t))
    control = reference(xtau, tau, tau) - base(xtau, tau)
    rewards = reward(endpoint)
    if rewards.shape != t.shape:
        raise ValueError('reward must return exactly one scalar per row, shape (N,)')
    values = reward_scale*rewards - .5*(1-t)*control.square().sum(-1)
    if values.requires_grad:
        gradient = torch.autograd.grad(values.sum(), leaf, allow_unused=True)[0]
        gradient = torch.zeros_like(leaf) if gradient is None else gradient.detach()
    else:
        gradient = torch.zeros_like(leaf)
    return gradient, values.detach()


def wtf_loss(student, reference, base, z, t, tau, reward, reward_scale=1., beta=1.):
    """WTF diagonal regression plus Appendix-A semigroup self-distillation.

    Time convention: noise at 0, data at 1 (opposite the released image recipe).
    On-policy states and frozen regression targets never backprop into parameters.
    """
    with torch.no_grad():
        xt = reference.jump(z, torch.zeros_like(t), t)
    target, values = value_gradient(reference, base, xt, t, tau, reward, reward_scale)
    control = student(xt.detach(), t, t) - base(xt.detach(), t).detach()
    diagonal = (control-target).square().sum(-1).mean()
    # Independent off-diagonal jump, sampling s<=u<=v and on-policy x_s.
    times = torch.rand((len(z), 3), dtype=z.dtype, device=z.device).sort(-1).values
    s, u, v = times.unbind(-1)
    with torch.no_grad():
        xs = reference.jump(z, torch.zeros_like(s), s)
        stopped = reference.jump(reference.jump(xs, s, u), u, v)
    predicted = student.jump(xs, s, v)
    distillation = (predicted-stopped).square().sum(-1).mean()
    return diagonal + beta*distillation, dict(diagonal=diagonal.detach(),
                                            distillation=distillation.detach(),
                                            value=values.mean())
