"""
mcsamplerGPU.MCSampler.add_parameter(..., cdf_inv=None) with a vectorized pdf.

cdf_inverse() builds the CDF by integrating the pdf with scipy's
odeint, whose callback receives a python FLOAT.  Every pdf helper in mcsamplerGPU is
vectorized -- ret_uniform_samp_vector_alt returns ones(len(x))/(b-a) since 2022-04 --
so len(float) raised TypeError on any add_parameter call that omitted cdf_inv.  The
failure is scipy-independent (odeint has always probed with a float; reproduced on
scipy 1.10.1 and 1.13.1) and happened before any likelihood evaluation.

In ILE the only add_parameter call without an analytic cdf_inv was t_ref, reached with
--time-marginalization off under the default --sampler-method adaptive_cartesian_gpu.
That call now passes the analytic inverse it already constructed; the driver test at
the bottom pins the wiring.

Passes with xpy_default = numpy (CI, CVMFS igwn python) and with xpy_default = cupy
(ldas-pcdev13, cupy 10.6 and 12.0, 2026-09-08).
"""
import ast
import functools
from pathlib import Path

import numpy as np
import pytest

import RIFT.integrators.mcsamplerGPU as mcsamplerGPU

# (label, pdf, lo, hi, x at cdf=0.25, x at cdf=0.5)
CASES = [
    ("uniform_alt", mcsamplerGPU.ret_uniform_samp_vector_alt(-0.002, 0.002),
     -0.002, 0.002, -0.001, 0.0),
    ("uniform_phase", mcsamplerGPU.uniform_samp_phase,
     0.0, 2 * np.pi, np.pi / 2, np.pi),
    ("uniform_psi", mcsamplerGPU.uniform_samp_psi,
     0.0, np.pi, np.pi / 4, np.pi / 2),
    # pdf sin(x)/2 on [0,pi]: cdf = (1-cos x)/2, so cdf=0.25 at pi/3, 0.5 at pi/2
    ("theta", mcsamplerGPU.uniform_samp_theta,
     0.0, np.pi, np.pi / 3, np.pi / 2),
    # scalar-style pdf (if x>a and x<b) must keep working through the length-1 probe
    ("uniform_scalar", functools.partial(mcsamplerGPU.uniform_samp, -1.0, 3.0),
     -1.0, 3.0, 0.0, 1.0),
]


@pytest.mark.parametrize("label,pdf,lo,hi,q25,q50", CASES, ids=[c[0] for c in CASES])
def test_add_parameter_without_cdf_inv(label, pdf, lo, hi, q25, q50):
    s = mcsamplerGPU.MCSampler()
    s.add_parameter(label, pdf=pdf, cdf_inv=None, left_limit=lo, right_limit=hi,
                    prior_pdf=pdf)
    inv = s.cdf_inv[label]
    x = inv(np.array([0.0, 0.25, 0.5, 1.0]))
    tol = 2e-3 * (hi - lo)   # 1000-point grid, linear interpolation
    assert x[0] == pytest.approx(lo, abs=tol)
    assert x[1] == pytest.approx(q25, abs=tol)
    assert x[2] == pytest.approx(q50, abs=tol)
    assert x[3] == pytest.approx(hi, abs=tol)
    # the draw path hands cdf_inv a vector of uniforms
    draws = inv(np.random.default_rng(0).uniform(size=2000))
    assert draws.min() >= lo and draws.max() <= hi


def test_ile_tref_passes_analytic_cdf_inv():
    """The t_ref add_parameter call must pass the analytic inverse, not None.

    None routes through cdf_inverse -> odeint -> interp1d; on a cupy host interp1d
    then rejects the device array draw_simplified hands it, so the odeint fix alone
    would not make the default sampler run there.
    """
    ile = Path(__file__).parents[1] / "bin" / "integrate_likelihood_extrinsic_batchmode"
    tree = ast.parse(ile.read_text())
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute) and node.func.attr == "add_parameter"
        and node.args and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "t_ref"
    ]
    assert len(calls) == 1
    kw = {k.arg: k.value for k in calls[0].keywords}
    assert isinstance(kw["cdf_inv"], ast.Name)
    assert kw["cdf_inv"].id == "tref_sampler_cdf_inv"
    assigned = {
        t.id for node in ast.walk(tree) if isinstance(node, ast.Assign)
        for t in node.targets if isinstance(t, ast.Name)
    }
    assert "tref_sampler_cdf_inv" in assigned
    # and that inverse is exact on the window ends, in the units the sampler draws in
    inv = functools.partial(mcsamplerGPU.uniform_samp_cdf_inv_vector, -0.002, 0.002)
    assert np.allclose(inv(np.array([0.0, 0.5, 1.0])), [-0.002, 0.0, 0.002])
