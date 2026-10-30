"""Execute each shipped driver's mapping/export blocks without an ILE campaign."""
import ast
import functools
from pathlib import Path
from types import SimpleNamespace

import lal
import lalsimulation as lalsim
import numpy as np
import pytest

from RIFT import lalsimutils
from RIFT.misc import sky_rotations

CODE = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('driver', ['integrate_likelihood_extrinsic_batchmode', 'integrate_likelihood_extrinsic'])
@pytest.mark.parametrize('enabled', [False, True])
def test_driver_exports_the_same_physical_sky_used_by_likelihood(driver, enabled):
    tree = ast.parse((CODE/'bin'/driver).read_text())
    # Execute the real frame setup, including its direction and backend binding.
    setup = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and
                 any(isinstance(c, ast.Assign) and any(isinstance(t, ast.Name) and
                     t.id == 'frm_inverse' for t in c.targets) for c in n.orelse))
    epoch = lal.LIGOTimeGPS(1389783918.)
    env = dict(np=np, numpy=np, functools=functools, lalsimutils=lalsimutils,
               sky_rotations=sky_rotations, identity_convert_togpu=lambda x:x,
               xpy_default=np, ifo_list=['H1','L1'], fiducial_epoch=epoch)
    exec(compile(ast.Module(body=setup.orelse, type_ignores=[]), driver, 'exec'), env)
    export = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and
                  any(isinstance(c, ast.Assign) and isinstance(c.value, ast.Call) and
                      isinstance(c.value.func, ast.Name) and c.value.func.id == 'my_rotation_cpu'
                      for c in n.body))
    ra = np.linspace(.01, 2*np.pi-.01, 200)
    dec = np.full(200, .4)
    samples = dict(right_ascension=ra.copy(), declination=dec.copy())
    env.update(samples=samples, opts=SimpleNamespace(internal_sky_network_coordinates=enabled))
    exec(compile(ast.Module(body=[export], type_ignores=[]), driver, 'exec'), env)
    if not enabled:
        np.testing.assert_array_equal(samples['right_ascension'], ra)
        np.testing.assert_array_equal(samples['declination'], dec)
        return
    th_expected, ph_expected = env['my_rotation'](np.pi/2-dec, ra)
    np.testing.assert_allclose(samples['declination'], np.pi/2-th_expected, atol=1e-14)
    np.testing.assert_allclose(samples['right_ascension'], ph_expected%(2*np.pi), atol=1e-14)
    assert np.max(np.abs(samples['declination']-dec)) > .1
    loc_h = lalsim.DetectorPrefixToLALDetector('H1').location
    loc_l = lalsim.DetectorPrefixToLALDetector('L1').location
    delays = [lal.ArrivalTimeDiff(loc_h, loc_l, float(r), float(d), epoch)
              for r,d in zip(samples['right_ascension'], samples['declination'])]
    assert np.ptp(delays) < 1e-11
