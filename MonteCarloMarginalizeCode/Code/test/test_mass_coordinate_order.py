"""Mass coordinates are assigned in list order, and each assign_param holds a different quantity fixed.

assign_param('mtot') and ('mc') keep the mass ratio; ('q') and ('delta') keep mtot; ('eta') and
('delta_mc') keep mc.  So ['mtot', 'delta_mc'] does not return the requested mtot, while
['delta_mc', 'mtot'] does.  The inconsistent orders must raise, and the tools that emit
parameter lists must not emit them.
"""
import os
import re
import subprocess
import sys

import lal
import numpy as np
import pytest

from RIFT import lalsimutils

CODE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MASS_NAMES = ('mtot', 'mc', 'mc_ecc', 'log_mc', 'q', 'delta', 'eta', 'delta_mc')


def _truth(n=50, seed=3):
    rng = np.random.default_rng(seed)
    m1 = rng.uniform(20, 80, n)
    m2 = m1 * rng.uniform(0.2, 0.95, n)
    return m1, m2, rng.uniform(-0.5, 0.5, n), rng.uniform(-0.5, 0.5, n)


def _rows(names, m1, m2, s1z, s2z):
    x = np.zeros((len(m1), len(names)))
    for i in range(len(m1)):
        P = lalsimutils.ChooseWaveformParams()
        P.m1, P.m2, P.s1z, P.s2z = m1[i], m2[i], s1z[i], s2z[i]
        x[i] = [P.extract_param(p) for p in names]
    return x


@pytest.mark.parametrize("pair", [['mtot', 'delta_mc'], ['mtot', 'eta'], ['mc', 'q'], ['mc', 'delta']])
def test_inconsistent_order_raises(pair):
    names = pair + ['s1z', 's2z']
    x = _rows(names, *_truth())
    with pytest.raises(ValueError, match=r"inconsistent mass coordinates.*{}".format(pair[1])):
        lalsimutils.convert_waveform_coordinates(x, coord_names=['m1', 'm2', 'xi'], low_level_coord_names=names)


@pytest.mark.parametrize("pair", [['delta_mc', 'mtot'], ['eta', 'mtot'], ['q', 'mc'],
                                  ['mtot', 'q'], ['mtot', 'delta'], ['mc', 'delta_mc']])
def test_consistent_order_roundtrips(pair):
    m1, m2, s1z, s2z = _truth()
    names = pair + ['s1z', 's2z']
    out = lalsimutils.convert_waveform_coordinates(_rows(names, m1, m2, s1z, s2z), coord_names=['m1', 'm2', 'xi'],
                                                   low_level_coord_names=names)
    assert np.allclose(out[:, 0], m1, rtol=1e-12, atol=0) and np.allclose(out[:, 1], m2, rtol=1e-12, atol=0)


def test_puffball_rejects_inconsistent_order(tmp_path):
    P_list = []
    for m1, m2 in [(50., 30.), (52., 28.), (48., 31.), (55., 25.)]:
        P = lalsimutils.ChooseWaveformParams()
        P.m1, P.m2, P.fmin = m1 * lal.MSUN_SI, m2 * lal.MSUN_SI, 20.
        P_list.append(P)
    lalsimutils.ChooseWaveformParams_array_to_xml(P_list, str(tmp_path / 'inj'))
    _assert_parser_rejects(tmp_path, 'util_ParameterPuffball.py', '--inj-file', str(tmp_path / 'inj.xml.gz'),
                           '--inj-file-out', str(tmp_path / 'out'), '--parameter', 'mtot', '--parameter', 'delta_mc',
                           '--fmin', '20', '--fref', '20')


def test_cip_rejects_inconsistent_order(tmp_path):
    _assert_parser_rejects(tmp_path, 'util_ConstructIntrinsicPosterior_GenericCoordinates.py', '--fname', 'none.composite',
                           '--parameter', 'mtot', '--parameter', 's1z', '--parameter-nofit', 'eta', '--no-plots')


def test_grid_rejects_inconsistent_order(tmp_path):
    _assert_parser_rejects(tmp_path, 'util_ManualOverlapGrid.py', '--fname', 'grid', '--skip-overlap', '--fmin', '20',
                           '--grid-cartesian-npts', '20', '--random-parameter', 'mtot', '--random-parameter-range', '[40.,80.]',
                           '--random-parameter', 'delta_mc', '--random-parameter-range', '[0.1,0.6]')


def _assert_parser_rejects(tmp_path, script, *args):
    env = dict(os.environ, PYTHONPATH=CODE + os.pathsep + os.environ.get('PYTHONPATH', ''))
    res = subprocess.run([sys.executable, os.path.join(CODE, 'bin', script)] + list(args),
                         cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=300)
    assert res.returncode == 2 and 'inconsistent mass coordinates' in res.stderr, res.stderr[-2000:]


def _assigned_masses(names):
    """Assign the truth's values for `names`, in order, to a fresh P; return its (m1, m2)."""
    truth = lalsimutils.ChooseWaveformParams()
    truth.m1, truth.m2 = 61.0, 23.0
    P = lalsimutils.ChooseWaveformParams()
    for p in names:
        P.assign_param(p, truth.extract_param(p))
    return P.m1 / truth.m1 - 1, P.m2 / truth.m2 - 1


def test_emitted_parameter_lists_reproduce_the_masses():
    # every literal sampling-coordinate list in the files that write puffball, grid and CIP arguments
    # (a .replace() line maps one name to another; it is not a list)
    pattern = re.compile(r"--(parameter|parameter-nofit|random-parameter)[ =]+([A-Za-z_0-9]+)")
    bad = []
    for name in ['helper_LDG_Events.py', 'util_RIFT_pseudo_pipe.py', 'util_RIFT_pseudo_pipe_lowlatency.py']:
        with open(os.path.join(CODE, 'bin', name)) as f:
            for lineno, line in enumerate(f, 1):
                if '.replace(' in line:
                    continue
                found = [m.groups() for m in pattern.finditer(line)]
                names = [p for kind, p in found if kind != 'parameter-nofit'] + [p for kind, p in found if kind == 'parameter-nofit']
                names = [p for p in names if p in MASS_NAMES]
                if len(names) > 1 and np.max(np.abs(_assigned_masses(names))) > 1e-12:
                    bad.append('{}:{} {}'.format(name, lineno, names))
    assert not bad, bad
