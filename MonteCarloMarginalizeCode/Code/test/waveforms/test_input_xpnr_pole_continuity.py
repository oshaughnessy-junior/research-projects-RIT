"""Raw XPNR antisymmetric polarization continuity: ordinary failing contracts.

The physical waveframe is smooth here. An internal J-frame azimuth singularity
must not create a finite jump in the polarizations. No RIFT, conditioning,
fitted phase, or mode removal is involved. Both native thresholds and the full
coprecessing mode list are explicit to isolate the antisymmetric contribution.
"""
from importlib.metadata import version

import lal
import lalsimulation as ls
import numpy as np
import pytest

CASES = ((70., 20., .45, .3, .8, 1.7, .7, 1.2, 1.4, 1.1),
         (55., 35., .65, .45, 1.1, .6, 1.3, 2.2, .8, .25),
         (80., 15., .6, .35, .6, 2.1, 2., .3, 2.2, 2.))
OPTIONS = dict(PhenomXPrecVersion=330, PhenomXPFinalSpinMod=7,
    PhenomXPNRUseTunedAngles=1, PhenomXPNRUseTunedCoprec=1,
    PhenomXPNRForceXHMAlignment=0, PhenomXPNRUseTunedCoprec33=0,
    PhenomXHMReleaseVersion=122019, PhenomXPHMMBandVersion=0,
    PhenomXHMThresholdMband=0., PhenomXPHMThresholdMband=0.,
    PhenomXPConvention=1)


def dictionary(anti):
    d = lal.CreateDict()
    for key, value in dict(OPTIONS, PhenomXAntisymmetricWaveform=anti).items():
        getattr(ls, 'SimInspiralWaveformParamsInsert' + key)(d, value)
    modes = ls.SimInspiralCreateModeArray()
    for ell, m in ((2, 1), (2, 2), (3, 2), (3, 3), (4, 4)):
        for sign in (-1, 1):
            ls.SimInspiralModeArrayActivateMode(modes, ell, sign*m)
    ls.SimInspiralWaveformParamsInsertModeArray(d, modes)
    return d


def joint_norm(polarizations):
    return np.sqrt(sum(np.linalg.norm(h)**2 for h in polarizations))


@pytest.mark.parametrize('case', CASES, ids=['generic', 'two_spin', 'high_q'])
def test_input_xpnr_polarizations_are_continuous_across_native_j_pole(case, record_property):
    m1, m2, a1, a2, t1, t2, p12, pjl, theta, phase = case
    m1 *= lal.MSUN_SI
    m2 *= lal.MSUN_SI
    spin = ls.SimInspiralTransformPrecessingNewInitialConditions(
        theta, pjl, t1, t2, p12, a1, a2, m1, m2, 20., phase)
    spins = tuple(float(x) for x in spin[1:])
    _, beta, _, frequencies, _, _ = ls.SimIMRPhenomX_PNR_GeneratePNRAnglesHM(
        m1, m2, *spins, 0., 1/16, 18., 2048., 20., 2, 2, dictionary(1))
    beta_ref = float(np.interp(20., frequencies.data, beta.data))
    phi_j = np.arctan2(m1*m1*spins[1] + m2*m2*spins[4],
                       m1*m1*spins[0] + m2*m2*spins[3])
    phi_ref = np.pi/2 - phi_j
    step = 1e-7
    assert 1e-3 < beta_ref < np.pi - 1e-3, 'Use a genuinely precessing source'
    residuals = {}
    banks = {}
    for anti in (0, 1):
        waveforms = []
        for sign in (-1, 1):
            hp, hc = ls.SimInspiralChooseFDWaveform(
                m1, m2, *spins, 400e6*lal.PC_SI, beta_ref + sign*step,
                phi_ref, 0., 0., 0., 1/16, 18., 2048., 20., dictionary(anti),
                ls.IMRPhenomXPNR)
            f = np.arange(hp.data.length)*hp.deltaF
            mask = (f >= 20.) & (f <= 512.)
            waveforms.append([hp.data.data[mask].copy(), hc.data.data[mask].copy()])
        banks[anti] = waveforms
        scale = joint_norm(waveforms[0])
        assert np.isfinite(scale) and scale > 0
        residuals[anti] = float(joint_norm([a-b for a, b in zip(*waveforms)]) / scale)
        assert np.isfinite(residuals[anti])
    for side in (0, 1):
        fraction = float(joint_norm([a-b for a,b in zip(banks[1][side], banks[0][side])]) / joint_norm(banks[1][side]))
        record_property('asymmetry_fraction_' + str(side), fraction)
        assert np.isfinite(fraction) and fraction > 1e-3, 'Native asymmetry flag must retain a resolved contribution'
    record_property('inclination_step_radians', 2*step)
    record_property('symmetric_control_residual', residuals[0])
    record_property('retained_asymmetry_residual', residuals[1])
    assert residuals[0] < 1e-5, 'The symmetric native polarization control must be smooth'
    assert residuals[1] < 1e-5, (
        f'LALSuite {version("lalsuite")} raw XPNR with antisymmetry retained: '
        f'polarization jump {residuals[1]:.9g} across {2*step:g} radians; '
        'upstream J-pole asymmetry-gauge discontinuity remains')
