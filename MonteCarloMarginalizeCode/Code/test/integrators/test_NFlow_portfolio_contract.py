#!/usr/bin/env python
"""
test_NFlow_portfolio_contract.py

Pins the two things mcsamplerNFlow has to provide before mcsamplerPortfolio can
carry it as a member.  Both were missing, and both are reachable from production
CLI (``--sampler-portfolio NFlow`` on util_ConstructIntrinsicPosterior_GenericCoordinates
and util_ConstructEOSPosterior).

1. RETURN ORDER.  ``draw_simplified`` used to end with ``return rv, p_s, p_prior``,
   while MCSamplerGeneric and every other implementation (mcsamplerGPU,
   mcsamplerAdaptiveVolume, mcsamplerEnsemble, the unreliable_oracle members)
   return ``(p_s, p_prior, rv)``.  mcsamplerPortfolio.draw() unpacks
   ``joint_p_s_here, joint_p_prior_here, rv_here = member.draw_simplified(...)``,
   so an NFlow member had the SAMPLES assigned to joint_p_s.  Measured on
   junior/rift_O4d @94f352ad8 with a real nflows install, [AV, NFlow] portfolio,
   unit Gaussian in a [-5,5]^d box:
     * d=2: ValueError "could not broadcast input array from shape (2,100) into
       shape (100,)" out of mcsamplerPortfolio.draw.
     * d=1: NO error.  The (1,n) sample array broadcasts into the (n,) density
       slot, and the run COMPLETES with ln Z = -4.458 against a true -1.384 --
       3.07 nats low, silently.  (Control: the same portfolio with the NFlow
       member replaced by a second AV returns -1.273, 0.11 nats.)  So the
       failure is not reliably loud, which is why this is a pinned test and not
       a comment.

2. sampling_density.  mcsamplerPortfolio builds its balance-heuristic mixture
   denominator q_mix = sum_m frac_m q_m from ``member.sampling_density(X)``.
   NFlow had no such method, so an NFlow member forced the portfolio onto the
   legacy stratified per-member density -- which is only valid when every member
   shares one support, and which PR #356 ("portfolio: make sampling_density the
   member p_s contract") turns into a hard refusal.

NO nflows/torch REQUIRED.  mcsamplerNFlow imports torch and nflows at module
scope, which is why test_NF_reuse.py is rostered OPTDEP.  The code paths pinned
here -- the uniform ``self.nf_flow is None`` draw and its matching
sampling_density branch -- are pure numpy, so this file imports the module
behind import-time stubs when the real packages are absent.  It uses the REAL
packages when they are installed, so a dev box exercises the genuine import.

Usage:
  python -m pytest -q MonteCarloMarginalizeCode/Code/test/integrators/test_NFlow_portfolio_contract.py
"""
from __future__ import print_function

import ast
import inspect
import io
import sys
import types

import numpy as np
import pytest


# ---------------------------------------------------------------- import stubs

_SUBMODULES = [
    "torch", "torch.optim", "torch.optim.lr_scheduler", "torch.utils", "torch.utils.data",
    "nflows", "nflows.flows", "nflows.flows.base", "nflows.utils",
    "nflows.distributions", "nflows.distributions.normal",
    "nflows.transforms", "nflows.transforms.normalization", "nflows.transforms.base",
    "nflows.transforms.autoregressive", "nflows.transforms.permutations",
    "nflows.transforms.standard", "nflows.transforms.lu",
    "nflows.nn", "nflows.nn.nets",
]


def _make_stub(name):
    """A module whose every attribute is a fresh permissive CLASS.

    A class (not a function) is required: mcsamplerNFlow does
    ``class TanhTransform(Transform)`` at module scope, so the stand-in has to be
    usable as a base.  This stub only has to survive IMPORT -- every path this
    file exercises is numpy-only -- so it deliberately does not try to imitate
    torch or nflows behaviour.  Anything that actually needs them belongs in
    test_NF_reuse.py, which is rostered OPTDEP.
    """
    mod = types.ModuleType(name)

    def __getattr__(attr):
        return type(str(attr), (object,), {
            "__init__": lambda self, *a, **k: None,
            "__call__": lambda self, *a, **k: None,
        })

    mod.__getattr__ = __getattr__
    return mod


@pytest.fixture(scope="module")
def NF():
    """mcsamplerNFlow, imported with real torch/nflows if present, else stubbed.

    Restores sys.modules afterwards so a stub cannot leak into another test file
    sharing the interpreter.
    """
    try:
        import torch  # noqa: F401
        import nflows  # noqa: F401
        real = True
    except Exception:
        real = False

    saved = {}
    if not real:
        for name in _SUBMODULES:
            saved[name] = sys.modules.get(name)
            sys.modules[name] = _make_stub(name)
    saved["RIFT.integrators.mcsamplerNFlow"] = sys.modules.pop(
        "RIFT.integrators.mcsamplerNFlow", None)
    try:
        import RIFT.integrators.mcsamplerNFlow as mod
        mod._test_used_real_deps = real
        yield mod
    finally:
        for name, prev in saved.items():
            if prev is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prev


# ------------------------------------------------------------------- fixtures

# Asymmetric box, and a prior that is NOT the sampling density.  Both matter: with
# a uniform prior on a symmetric box, p_s and p_prior are the same number and a
# swapped return order is undetectable.
_BOX = [(-5.0, 5.0), (0.0, 4.0), (-1.0, 3.0)]
_PARAMS = ["x0", "x1", "x2"]
_VOL = float(np.prod([hi - lo for lo, hi in _BOX]))


def _ramp(lo, hi):
    """Normalized linear ramp on [lo,hi]: integrates to 1, varies point to point."""
    w = hi - lo
    return np.vectorize(lambda x, lo=lo, w=w: 2.0 * (x - lo) / (w * w))


def _build(NF):
    s = NF.MCSampler(n_chunk=64)
    for p, (lo, hi) in zip(_PARAMS, _BOX):
        s.add_parameter(p, np.vectorize(lambda x, w=(hi - lo): 1.0 / w),
                        prior_pdf=_ramp(lo, hi),
                        left_limit=lo, right_limit=hi,
                        adaptive_sampling=True)
    assert s.nf_flow is None, "fixture must stay on the untrained (uniform) branch"
    return s


# ----------------------------------------------------------------- the contract

def test_draw_simplified_returns_ps_prior_rv(NF):
    """(p_s, p_prior, rv), positionally -- the order mcsamplerPortfolio unpacks.

    Each slot is identified by a property only it has, so EVERY permutation of the
    three fails rather than just the one that shipped:
      slot 0  constant 1/V   (the uniform sampling density)
      slot 1  varies, and equals prod(prior_pdf) at the returned samples
      slot 2  shape (ndim, n), inside the box
    """
    np.random.seed(20260916)
    s = _build(NF)
    n = 37
    out = s.draw_simplified(n, save_no_samples=True)

    assert isinstance(out, tuple) and len(out) == 3, \
        "draw_simplified must return a 3-tuple, got {!r}".format(type(out))
    p_s, p_prior, rv = out

    # slot 2: the samples
    rv = np.asarray(rv)
    assert rv.shape == (len(_PARAMS), n), \
        ("slot 2 must be the SAMPLES with shape (ndim, n)=({}, {}), got {}."
         "  A (n,) array here means the return order is (rv, p_s, p_prior)."
         .format(len(_PARAMS), n, rv.shape))
    for indx, (lo, hi) in enumerate(_BOX):
        assert np.all(rv[indx] >= lo) and np.all(rv[indx] <= hi)

    # slot 0: the sampling density, constant 1/V on the untrained branch
    p_s = np.asarray(p_s)
    assert p_s.shape == (n,), "slot 0 must be p_s with shape (n,), got {}".format(p_s.shape)
    assert np.allclose(p_s, 1.0 / _VOL), \
        "slot 0 must be the uniform sampling density 1/V={:g}, got {!r}".format(1.0 / _VOL, p_s[:4])

    # slot 1: the prior at those samples -- varies, so it cannot be confused with slot 0
    p_prior = np.asarray(p_prior)
    assert p_prior.shape == (n,), "slot 1 must be p_prior with shape (n,), got {}".format(p_prior.shape)
    expect = np.ones(n)
    for indx, (lo, hi) in enumerate(_BOX):
        expect *= _ramp(lo, hi)(rv[indx])
    assert np.allclose(p_prior, expect), "slot 1 must be prod(prior_pdf) at the returned samples"
    assert p_prior.std() > 0, "the ramp prior must vary, else this test cannot tell slot 1 from slot 0"


class _Tensorish(object):
    """The two methods the draw path calls on a flow's output: .detach().numpy()."""

    def __init__(self, arr):
        self._arr = arr

    def detach(self):
        return self

    def numpy(self):
        return self._arr


class _StrictIntFlow(object):
    """A flow stand-in that is STRICT about the one thing being tested.

    It reproduces nflows.distributions.base.Distribution.sample's check verbatim
    (``check.is_positive_int`` -> ``isinstance(n, int) and n > 0``), because a
    permissive stand-in here would accept the numpy int and pass a defect that
    the real nflows rejects.  Everything else it does is the minimum the draw
    path consumes.
    """

    def __init__(self, ndim, seed=0):
        self.ndim = ndim
        self.rng = np.random.RandomState(seed)
        self.saw = []

    def sample_and_log_prob(self, num_samples):
        self.saw.append(type(num_samples))
        if not isinstance(num_samples, int) or isinstance(num_samples, bool) or num_samples <= 0:
            raise TypeError("Number of samples must be a positive integer.")
        x = np.column_stack([self.rng.uniform(lo, hi, size=num_samples) for lo, hi in _BOX])
        return _Tensorish(x), _Tensorish(self._logq(x))

    def log_prob(self, t):
        return _Tensorish(self._logq(t.numpy()))

    def _logq(self, x):
        return np.full(x.shape[0], -np.log(_VOL))


def test_draw_simplified_accepts_a_numpy_int_on_the_flow_branch(NF):
    """mcsamplerPortfolio hands members n_samples_per_member[i], a numpy int64.

    nflows type-checks its sample count with isinstance(n, int), which a numpy
    integer FAILS ("Number of samples must be a positive integer"), so NFlow was
    the one member with a stricter signature than the portfolio's own call.  The
    untrained branch cannot see this -- numpy sizes accept a numpy int happily --
    so drive the flow branch through a stand-in that keeps nflows' check.

    This also pins the return order on the TRAINED branch, independently of the
    uniform-branch test above.
    """
    np.random.seed(11)
    s = _build(NF)
    n = np.array([23], dtype=np.int64)[0]
    assert not isinstance(n, int), "vacuous unless a numpy int is not a python int"

    s.nf_flow = _StrictIntFlow(len(_PARAMS), seed=4)
    p_s, p_prior, rv = s.draw_simplified(n, save_no_samples=True, enforce_bounds=True)

    assert s.nf_flow.saw, "the flow branch was not taken -- this test checked nothing"
    assert s.nf_flow.saw[0] is int, \
        ("draw_simplified passed {} to the flow; nflows requires a python int."
         .format(s.nf_flow.saw[0]))

    rv = np.asarray(rv)
    assert rv.shape == (len(_PARAMS), 23), \
        "slot 2 must be the (ndim, n) samples on the trained branch too, got {}".format(rv.shape)
    assert np.allclose(np.asarray(p_s), 1.0 / _VOL), "slot 0 must be the flow density exp(log_prob)"
    assert np.asarray(p_prior).shape == (23,) and np.asarray(p_prior).std() > 0, \
        "slot 1 must be the (varying) prior at those samples"


def test_sampling_density_exists_and_is_the_uniform_box_density(NF):
    """The method mcsamplerPortfolio needs for q_mix = sum_m frac_m q_m."""
    s = _build(NF)
    assert hasattr(s, "sampling_density"), \
        ("mcsamplerNFlow needs sampling_density(X): mcsamplerPortfolio builds its "
         "balance-heuristic mixture denominator from it, and PR #356 makes a member "
         "without one a hard error.")

    rng = np.random.RandomState(3)
    X = np.column_stack([rng.uniform(lo, hi, size=200) for lo, hi in _BOX])
    q = s.sampling_density(X)
    assert q is not None, "sampling_density must not be None once the box is registered"
    q = np.asarray(q)
    assert q.shape == (200,)
    assert np.allclose(q, 1.0 / _VOL)

    # it must agree with the p_s draw_simplified reports for its OWN draws -- that
    # agreement IS the member p_s contract the portfolio relies on
    np.random.seed(5)
    p_s, _, rv = s.draw_simplified(50, save_no_samples=True)
    assert np.allclose(np.asarray(s.sampling_density(np.asarray(rv).T)), np.asarray(p_s))

    # (ndim, N) is tolerated, as for mcsamplerAdaptiveVolume/mcsamplerEnsemble
    assert np.allclose(np.asarray(s.sampling_density(X.T)), q)

    # zero outside the box: enforce_bounds means no accepted draw lands there, so a
    # nonzero density out there would overstate this member's share of q_mix
    X_out = X.copy()
    X_out[:7, 0] = _BOX[0][1] + 1.0
    q_out = np.asarray(s.sampling_density(X_out))
    assert np.all(q_out[:7] == 0.0)
    assert np.allclose(q_out[7:], q[7:])


def test_internal_call_sites_unpack_in_contract_order(NF):
    """Source check: nothing inside mcsamplerNFlow may keep the old order.

    The behavioural tests above pin the single ``return`` statement, which both
    the trained and untrained branches share.  They cannot see the module's OWN
    call site in integrate_log -- reaching it needs a real trained flow -- and
    that call used to carry a "Beware reversed order of rv" comment precisely
    because it compensated for the defect.  Flipping the return without flipping
    the call site would leave NFlow's own integrator broken, so pin it here.
    """
    path = inspect.getsourcefile(NF)
    assert path and path.endswith(".py"), \
        "could not locate mcsamplerNFlow source (got {!r})".format(path)
    tree = ast.parse(io.open(path, encoding="utf-8").read())

    sites = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target, value = node.targets[0], node.value
        if not isinstance(target, ast.Tuple) or not isinstance(value, ast.Call):
            continue
        fn = value.func
        if not (isinstance(fn, ast.Attribute) and fn.attr == "draw_simplified"):
            continue
        names = [e.id if isinstance(e, ast.Name) else "<expr>" for e in target.elts]
        sites.append((node.lineno, names))

    assert sites, \
        ("found no tuple-unpacking call to draw_simplified in {}; this test has stopped "
         "checking anything -- re-point it at the real call site.".format(path))

    for lineno, names in sites:
        assert len(names) == 3, "line {}: expected a 3-tuple, got {}".format(lineno, names)
        # the samples must be LAST; they are the only slot whose name carries 'rv'
        assert "rv" in names[2], \
            ("{}:{} unpacks draw_simplified as {} -- the samples must be the THIRD "
             "element, matching (p_s, p_prior, rv)."
             .format(path, lineno, tuple(names)))
        for indx in (0, 1):
            assert "rv" not in names[indx], \
                ("{}:{} unpacks draw_simplified as {} -- slot {} holds the samples, but "
                 "the contract is (p_s, p_prior, rv)."
                 .format(path, lineno, tuple(names), indx))
