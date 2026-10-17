"""IntrinsicMarginalObjective: one compile across intrinsic points, same values as the stock wrapper."""
import numpy as np
import lal
import lalsimulation as lalsim

import RIFT.lalsimutils as lalsimutils
from RIFT.likelihood.jax_ile import build_data_from_precompute
from RIFT.likelihood.jax_ile.wrapper import JAXDistPhiPsiMargLikelihood
from RIFT.likelihood.jax_ile.intrinsic_objective import IntrinsicMarginalObjective

MSUN, PC = lal.MSUN_SI, lal.PC_SI
FID = 1126259462.0


def _P(m1, m2, s1x=0.0):
    P = lalsimutils.ChooseWaveformParams()
    P.m1, P.m2 = m1 * MSUN, m2 * MSUN
    P.s1x, P.s1z, P.s2z = s1x, 0.1, -0.2
    P.fmin = P.fref = 30.0
    P.deltaT, P.deltaF = 1.0 / 2048, 0.25
    P.approx = lalsim.IMRPhenomPv2
    P.radec = True
    P.tref = FID
    return P


def _data():
    P = _P(35, 30, 0.3)
    P.phi, P.theta, P.psi, P.incl, P.phiref = 1.2, -0.4, 0.7, 0.9, 2.1
    P.dist = 600e6 * PC
    dd, pp = {}, {}
    for det in ("H1", "L1", "V1"):
        Pd = P.copy(); Pd.detector = det
        dd[det] = lalsimutils.non_herm_hoff(Pd)
        pp[det] = lalsim.SimNoisePSDaLIGOZeroDetHighPower
    return dd, pp


def test_objective_matches_wrapper_and_compiles_once():
    dd, pp = _data()
    obj = IntrinsicMarginalObjective(
        dd, pp, _P(35, 30), FID, 1.0, 5000.0, l_max=2, fmax=1000.0, n_sky=64,
        nphi=8, npsi=4, n_dist=64, waveform_kwargs=dict(ignore_threshold=None),
        analyticPSD_Q=True)
    rng = np.random.default_rng(3)
    ra, dec, incl = rng.uniform(0, 2 * np.pi, 64), rng.uniform(-1, 1, 64), rng.uniform(0, np.pi, 64)
    for P in (_P(35, 30, 0.3), _P(36, 29, 0.0)):
        mine, _ = obj.lnL_sky(P, ra, dec, incl)
        data, _ = build_data_from_precompute(
            obj._prepare(P), dd, pp, FID, 0.15, 0.075, 2, 1000.0, analyticPSD_Q=True,
            ignore_threshold=None)
        ref = JAXDistPhiPsiMargLikelihood(data, 1.0, 5000.0, nphi=8, npsi=4, n_grid=64,
                                          angle_marg="grid")
        ref_v = np.asarray(ref.log_likelihood(ra, dec, incl))
        assert np.allclose(mine, ref_v, rtol=0, atol=1e-8), np.max(np.abs(mine - ref_v))
    assert obj.n_compiles == 1
