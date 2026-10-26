"""assign_param('chieff_aligned', x) must set xi to x and hold chiminus = (m2 s1z - m1 s2z)/M fixed.

Covers unequal and exactly equal masses, the no-change identity, repeated assignment, and a
near-zero-factor (1e-8; 0 is singular) util_ParameterPuffball run in (mc, delta_mc, chieff_aligned), which must return its input.
"""
import os
import subprocess
import sys

import numpy as np
import pytest

from RIFT import lalsimutils as lsu

N = 50


def _chiminus(P):
    return (P.m2*P.s1z - P.m1*P.s2z)/(P.m1 + P.m2)


def _truth(seed, equal_mass=False):
    rng = np.random.default_rng(seed)
    Ps = []
    for _ in range(N):
        P = lsu.ChooseWaveformParams()
        m1, m2 = np.sort(rng.uniform(2, 60, 2))[::-1]
        if equal_mass:
            m2 = m1
        P.m1, P.m2 = m1*lsu.lsu_MSUN, m2*lsu.lsu_MSUN
        P.s1z, P.s2z = rng.uniform(-0.9, 0.9, 2)
        Ps.append(P)
    return Ps


@pytest.mark.parametrize("equal_mass", [False, True])
def test_identity(equal_mass):
    for P in _truth(1, equal_mass):
        s1z, s2z = P.s1z, P.s2z
        P.assign_param('chieff_aligned', P.extract_param('xi'))
        assert np.allclose([P.s1z, P.s2z], [s1z, s2z], rtol=0, atol=1e-12)


@pytest.mark.parametrize("equal_mass", [False, True])
def test_requested_value_and_held_chiminus(equal_mass):
    rng = np.random.default_rng(2)
    for P in _truth(3, equal_mass):
        dm = _chiminus(P)
        x = rng.uniform(-0.8, 0.8)
        P.assign_param('chieff_aligned', x)
        assert P.extract_param('xi') == pytest.approx(x, abs=1e-12)
        assert _chiminus(P) == pytest.approx(dm, abs=1e-12)


def test_repeated_assignment_is_idempotent():
    for P in _truth(4):
        P.assign_param('chieff_aligned', 0.3)
        once = (P.s1z, P.s2z)
        P.assign_param('chieff_aligned', 0.3)
        assert np.allclose([P.s1z, P.s2z], once, rtol=0, atol=1e-12)


def test_precessing_spins_keep_inplane_components():
    rng = np.random.default_rng(6)
    for P in _truth(7):
        P.s1x, P.s1y, P.s2x, P.s2y = rng.uniform(-0.3, 0.3, 4)
        inplane = (P.s1x, P.s1y, P.s2x, P.s2y)
        dm = _chiminus(P)
        P.assign_param('chieff_aligned', 0.1)
        assert P.extract_param('xi') == pytest.approx(0.1, abs=1e-12)
        assert _chiminus(P) == pytest.approx(dm, abs=1e-12)
        assert (P.s1x, P.s1y, P.s2x, P.s2y) == inplane


def _puff(tmp_path, factor, extra=(), seed=5):
    rng = np.random.default_rng(seed)
    Ps = []
    for _ in range(N*4):
        P = lsu.ChooseWaveformParams()
        P.m1, P.m2 = rng.uniform(34, 38)*lsu.lsu_MSUN, rng.uniform(20, 30)*lsu.lsu_MSUN
        P.s1z, P.s2z = rng.uniform(-0.9, 0.9, 2)
        P.fmin = P.fref = 20.
        Ps.append(P)
    lsu.ChooseWaveformParams_array_to_xml(Ps, fname=str(tmp_path/"in"))
    exe = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, "bin", "util_ParameterPuffball.py")
    cmd = [sys.executable, exe, "--inj-file", str(tmp_path/"in.xml.gz"), "--inj-file-out", str(tmp_path/"out"),
           "--puff-factor", str(factor), "--fmin", "20", "--fref", "20", "--random-seed", "7",
           "--parameter", "mc", "--parameter", "delta_mc", "--parameter", "chieff_aligned"] + list(extra)
    proc = subprocess.run(cmd, cwd=tmp_path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          universal_newlines=True, timeout=300)
    assert proc.returncode == 0, proc.stdout[-3000:]
    return Ps, lsu.xml_to_ChooseWaveformParams_array(str(tmp_path/"out.xml.gz")), proc.stdout


def test_puffball_tiny_factor_identity(tmp_path):
    Ps, Pout, log = _puff(tmp_path, 1e-8)   # 0 makes the covariance singular
    assert len(Pout) == len(Ps), log[-3000:]
    got = np.array([[p.s1z, p.s2z] for p in Pout])
    want = np.array([[p.s1z, p.s2z] for p in Ps])
    assert np.allclose(got, want, rtol=0, atol=1e-6)


def test_puffball_spin_downselect_uses_written_spins(tmp_path):
    cap = 0.5
    Ps, Pout, log = _puff(tmp_path, 1, ["--downselect-parameter", "chi1", "--downselect-parameter-range", "[0,%g]" % cap,
                                        "--downselect-parameter", "chi2", "--downselect-parameter-range", "[0,%g]" % cap])
    assert len(Pout) > 0, log[-3000:]
    chi = np.array([[p.extract_param('chi1'), p.extract_param('chi2')] for p in Pout])
    assert (chi <= cap + 1e-12).all()
