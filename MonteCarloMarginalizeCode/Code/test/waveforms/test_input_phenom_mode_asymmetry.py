"""Upstream LAL mode parity contracts, deliberately NOT xfailed or skipped.

These tests call raw SimInspiralChooseFDModes, without RIFT conditioning,
rotation, FFTs or likelihood code. LALSuite 7.26.16 fails the XPHM contract;
the XPNR failure is platform dependent, so every residual is retained in JUnit.
They should turn green when the input implementation is fixed; do not change
thresholds or symmetrize the modes to silence them.

For generic spins we reflect BOTH binaries' in-plane spins and compare mode
amplitudes across that reflected pair. We do not impose equatorial symmetry
on a single precessing waveform: its physical +/-m asymmetry must be retained.
Amplitudes avoid the known pi azimuth difference between the two J frames.
"""
from functools import lru_cache
from importlib.metadata import version

import lal
import lalsimulation as ls
import numpy as np

GENERIC = (48., 32., (.5, .2, .3), (-.1, .4, -.2))
ALIGNED = (44., 36., (0., 0., .5), (0., 0., -.3))
SPINTAYLOR = {"PhenomXPrecVersion": 320, "PhenomXPFinalSpinMod": 2,
             "PhenomXHMReleaseVersion": 122022}
AMPLITUDE_TOLERANCE = 1e-6


@lru_cache(maxsize=None)
def raw_modes(model, case, reflect=False, spintaylor=False):
    m1, m2, s1, s2 = case
    if reflect:
        s1 = (-s1[0], -s1[1], s1[2])
        s2 = (-s2[0], -s2[1], s2[2])
    d = lal.CreateDict()
    options = SPINTAYLOR if spintaylor else {}
    if model == "IMRPhenomXPNR":
        # Preserve the production model's antisymmetric contribution explicitly.
        options = dict(options, PhenomXAntisymmetricWaveform=1)
    for key, value in options.items():
        getattr(ls, "SimInspiralWaveformParamsInsert" + key)(d, value)
    linked = ls.SimInspiralChooseFDModes(
        m1 * lal.MSUN_SI, m2 * lal.MSUN_SI, *s1, *s2,
        1 / 16, 20., 2048., 20., 0., 400e6 * lal.PC_SI, 0., d,
        ls.GetApproximantFromString(model))
    result = {}
    for ell in range(2, 5):
        for m in range(-ell, ell + 1):
            h = ls.SphHarmFrequencySeriesGetMode(linked, ell, m)
            if h is not None:
                # Native ChooseFDModes grid is ascending -fNyq ... +fNyq,
                # including both endpoints. Reversing is exact f -> -f here.
                assert h.deltaF == 1 / 16
                assert len(h.data.data) == 2 * 2048 * 16 + 1
                result[ell, m] = np.array(h.data.data, copy=True)
    assert (2, 2) in result and (2, -2) in result
    return result


def reflected_amplitude_residual(model, case, mode, spintaylor=False):
    a = raw_modes(model, case, spintaylor=spintaylor)
    b = raw_modes(model, case, reflect=True, spintaylor=spintaylor)
    ell, m = mode
    target = a[ell, -m][::-1]
    candidate = b[ell, m]
    scale = max(np.linalg.norm(target), np.linalg.norm(candidate))
    # Never diagnose a nominally absent/numerically zero mode.
    assert scale / np.linalg.norm(a[2, 2]) > 1e-3
    return float(np.linalg.norm(abs(candidate) - abs(target)) / scale)


def test_input_generic_xpnr_reflected_mode_amplitude(record_property):
    residual = reflected_amplitude_residual("IMRPhenomXPNR", GENERIC, (3, 2))
    record_property("reflected_amplitude_residual", residual)
    assert residual <= AMPLITUDE_TOLERANCE, (
        f"LALSuite {version('lalsuite')} raw XPNR (3,2) reflected-pair amplitude "
        f"residual={residual:.9g}; upstream mode-parity defect remains")


def test_input_generic_spintaylor_reflection_control(record_property):
    residual = reflected_amplitude_residual(
        "IMRPhenomXPHM", GENERIC, (3, 2), spintaylor=True)
    record_property("reflected_amplitude_residual", residual)
    assert residual <= AMPLITUDE_TOLERANCE


def test_input_generic_xpnr_keeps_physical_mode_asymmetry(record_property):
    modes = raw_modes("IMRPhenomXPNR", GENERIC)
    same_binary_reflection = np.conj(modes[2, -2][::-1])
    asymmetry = np.linalg.norm(modes[2, 2] - same_binary_reflection) / np.linalg.norm(modes[2, 2])
    record_property("physical_mode_asymmetry", float(asymmetry))
    assert asymmetry > .01, "This precessing control must retain physical mode asymmetry"


def test_input_aligned_default_xphm_mode_pair_amplitude(record_property):
    residual = reflected_amplitude_residual("IMRPhenomXPHM", ALIGNED, (2, 1))
    record_property("reflected_amplitude_residual", residual)
    assert residual <= AMPLITUDE_TOLERANCE, (
        f"LALSuite {version('lalsuite')} raw aligned XPHM (2,1) amplitude "
        f"residual={residual:.9g}; upstream mode-parity defect remains")
