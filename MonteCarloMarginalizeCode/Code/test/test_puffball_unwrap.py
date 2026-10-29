"""PR249 seam-crossing proposals through the real CLI, preserving #245 spin cuts."""
import os
from pathlib import Path
import subprocess
import sys
import numpy as np
import pytest
from RIFT import lalsimutils as lsu

CODE = Path(__file__).resolve().parents[1]
NAMES = ['mc', 'delta_mc', 'eccentricity', 'meanPerAno', 'chieff_aligned']


def seeds(tmp_path, uniform=False):
    rng = np.random.default_rng(249)
    pts = []
    for _ in range(180):
        u = rng.normal()
        p = lsu.ChooseWaveformParams()
        p.assign_param('mc', (16 + rng.normal(0, .15))*lsu.lsu_MSUN)
        p.assign_param('delta_mc', .2 + rng.normal(0, .015))
        p.s1z, p.s2z = -.3 + rng.normal(0, .01, 2)
        p.eccentricity = .12 + .012*u + rng.normal(0, .0008)
        p.meanPerAno = (rng.uniform(0, 2*np.pi) if uniform else .22*u + rng.normal(0, .02)) % (2*np.pi)
        p.fmin = p.fref = 10
        pts.append(p)
    lsu.ChooseWaveformParams_array_to_xml(pts, fname=str(tmp_path/'input'), fref=10)
    return pts


def run(tmp_path, extra=(), script=None):
    command = [sys.executable, str(script or CODE/'bin/util_ParameterPuffball.py'),
               '--inj-file', str(tmp_path/'input.xml.gz'), '--inj-file-out', str(tmp_path/'output'),
               '--puff-factor', '.5', '--random-seed', '249', '--force-away', '.01']
    for name in NAMES:
        command += ['--parameter', name]
    proc = subprocess.run(command + list(extra), cwd=tmp_path, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
    return proc


@pytest.mark.parametrize('center', ['gap', 'mean'])
def test_unwrap_retains_seam_ridge_and_negative_spins(tmp_path, center):
    seeds(tmp_path)
    proc = run(tmp_path, ['--unwrap-parameter', 'meanPerAno', '--unwrap-center', center,
                         '--downselect-parameter', 'meanPerAno', '--downselect-parameter-range', '[0,6.283185307179586]'])
    assert proc.returncode == 0, proc.stdout
    pts = lsu.xml_to_ChooseWaveformParams_array(str(tmp_path/'output.xml.gz'))
    assert len(pts) > 160
    angle = np.angle(np.exp(1j*np.array([p.meanPerAno for p in pts])))
    ecc = np.array([p.eccentricity for p in pts])
    assert np.std(angle) < .4
    assert np.corrcoef(ecc, angle)[0, 1] > .95
    assert all(0 <= p.meanPerAno < 2*np.pi and p.s1z < 0 and p.s2z < 0 for p in pts)
    assert 'Unwrapped meanPerAno' in proc.stdout


@pytest.mark.parametrize('extra,message', [
    (['--unwrap-parameter','mc'], 'is not periodic'),
    (['--unwrap-parameter','phi1'], 'must also be specified'),
    (['--unwrap-parameter','meanPerAno','--eccentric-phase-coordinates'], 'cannot be combined'),
])
def test_invalid_unwrap_requests_fail_before_reading_input(tmp_path, extra, message):
    proc = run(tmp_path, extra)
    assert proc.returncode != 0
    assert message in proc.stdout


def test_full_circle_warns_instead_of_claiming_ridge_improvement(tmp_path):
    seeds(tmp_path, uniform=True)
    proc = run(tmp_path, ['--unwrap-parameter', 'meanPerAno'])
    assert proc.returncode == 0, proc.stdout
    assert 'no large empty arc' in proc.stdout


def test_unwrap_keeps_native_reflection_bounds(tmp_path):
    seeds(tmp_path)
    proc = run(tmp_path, ['--unwrap-parameter','meanPerAno','--reflect-parameter','eccentricity',
                         '--downselect-parameter','eccentricity','--downselect-parameter-range','[0.1,0.14]'])
    assert proc.returncode == 0, proc.stdout
    pts = lsu.xml_to_ChooseWaveformParams_array(str(tmp_path/'output.xml.gz'))
    assert len(pts) > 160
    assert all(.1 <= p.eccentricity <= .14 for p in pts)
