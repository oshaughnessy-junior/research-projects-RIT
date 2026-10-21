"""Scientific refusals found by adversarial review of the public GS port."""
import numpy as np
import pytest
from RIFT.likelihood import td_gs_precompute as tdg
import test_td_time_indexed_U as indexed


def test_indefinite_covariance_refused_before_gs():
    with pytest.raises(ValueError, match="positive-definite"):
        tdg.gs_generators(np.array([1., 2., 0., 0.]))


@pytest.mark.parametrize("rho", [0., 0.5, 0.999])
def test_positive_covariance_innovations(rho):
    tdg.validate_covariance_acf(rho ** np.arange(128))


@pytest.mark.parametrize("stencil", ["cubic", "sinc"])
def test_time_indexed_interpolation_refused(stencil):
    case = indexed._make_case()
    with pytest.raises(NotImplementedError, match="nearest"):
        indexed._call(case, case['U_t'], case['V_t'], time_interp=stencil)


def test_time_indexed_phase_refused():
    case = indexed._make_case()
    with pytest.raises(NotImplementedError, match="phase"):
        indexed._call(case, case['U_t'], case['V_t'], phase_marginalization=True)


@pytest.mark.parametrize("keyword,value", [("time_quadrature", "bandlimited"),
                                           ("explicit_time_values", True),
                                           ("q_deltaT", 1./8192)])
def test_current_api_unsafe_grid_combinations_refused(keyword, value):
    case = indexed._make_case()
    with pytest.raises(NotImplementedError):
        indexed._call(case, case['U_t'], case['V_t'], **{keyword:value})


def test_negative_norm_refused_on_ordinary_likelihood():
    case = indexed._make_case(dets=('H1',), modes=((2, 2),))
    U = {'H1': -np.ones((256, 1, 1), dtype=complex)}
    V = {'H1': np.zeros((256, 1, 1), dtype=complex)}
    with pytest.raises(ValueError, match='negative template norm'):
        indexed._call(case, U, V)
