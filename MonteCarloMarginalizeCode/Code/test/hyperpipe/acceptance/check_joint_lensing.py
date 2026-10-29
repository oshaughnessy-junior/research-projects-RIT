"""Independent deterministic seam controls; see ../JOINT_LENSING_ACCEPTANCE.md.

No LAL, scipy or pytest needed. This tests production driver source through its
public run API, while bypassing RIFT's heavyweight package initialization only.
It does not exercise the adaptive native posterior/evidence executable.
"""
from __future__ import annotations
import argparse
import importlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
import numpy as np

CODE = Path(__file__).resolve().parents[3]
DRIVERS = Path(os.environ.get('RIFT_JOINT_DRIVER_DIR', CODE / 'RIFT' / 'hyperpipe' / 'drivers'))


def load_driver():
    for name, path in [('RIFT', CODE / 'RIFT'),
                       ('RIFT.hyperpipe', CODE / 'RIFT' / 'hyperpipe'),
                       ('RIFT.hyperpipe.drivers', DRIVERS)]:
        pkg = types.ModuleType(name)
        pkg.__path__ = [str(path)]
        sys.modules[name] = pkg
    return importlib.import_module('RIFT.hyperpipe.drivers.joint_lensing')


def constant_factory(config):
    def evaluate(parameters, model):
        return 0.0
    return evaluate


def gaussian_factory(config):
    """Source Gaussian x and lens Gaussian log(y), with unit peak likelihood."""
    def evaluate(parameters, model):
        x = parameters['source_x']
        z = math.log(parameters['lens_y'])
        return -0.5 * (((x - 5.0) / .65)**2 + ((z - math.log(2.0)) / .45)**2)
    return evaluate


def truncated_normal(mu, sigma, lo, hi):
    """Exact unnormalized integral, mean, variance; independent of driver."""
    a, b = (lo - mu) / sigma, (hi - mu) / sigma
    phi = lambda t: math.exp(-.5 * t*t) / math.sqrt(2*math.pi)
    p = .5 * (math.erf(b / math.sqrt(2)) - math.erf(a / math.sqrt(2)))
    d = (phi(a) - phi(b)) / p
    return sigma * math.sqrt(2*math.pi) * p, mu + sigma*d, sigma*sigma * (1 + (a*phi(a)-b*phi(b))/p - d*d)


class Acceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver = load_driver()

    def evaluate(self, coordinates, points, factory):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            manifest = {'factory': f'check_joint_lensing:{factory}',
                        'factory_config': {}, 'fixed': {},
                        'coordinates': coordinates, 'model': 'lensed'}
            (tmp / 'manifest.json').write_text(json.dumps(manifest))
            names = list(coordinates)
            grid = np.column_stack([np.zeros((len(points), 2)), points])
            np.savetxt(tmp / 'grid.dat', grid,
                       header='lnL sigma_lnL ' + ' '.join('u_' + n for n in names))
            path = self.driver.JointLensingDriver().run([
                '--joint-config', str(tmp / 'manifest.json'),
                '--using-eos', 'file:' + str(tmp / 'grid.dat'),
                '--eos_start_index', '0', '--eos_end_index', str(len(points)),
                '--outdir', str(tmp), '--fname-output-integral', 'out',
                '--conforming-output-name'])
            result = np.loadtxt(path, ndmin=2)
            np.testing.assert_allclose(result[:, 2:], points, rtol=1e-14, atol=1e-14)
            self.assertTrue(np.all(result[:, 1] == 0))
            return result[:, 0]

    def test_constant_likelihood_normalized_nonunit_priors(self):
        coordinates = {
            'source_x': {'min': 2., 'max': 8., 'prior': 'uniform'},
            'lens_y': {'min': .2, 'max': 20., 'prior': 'log_uniform'}}
        nodes, weights = np.polynomial.legendre.leggauss(12)
        nodes, weights = (nodes + 1)/2, weights/2
        u, v = np.meshgrid(nodes, nodes, indexing='ij')
        lnL = self.evaluate(coordinates, np.column_stack([u.ravel(), v.ravel()]), 'constant_factory')
        evidence = np.sum(np.outer(weights, weights).ravel() * np.exp(lnL))
        self.assertAlmostEqual(evidence, 1., places=13)

    def test_isotropic_measure_and_invalid_quantiles(self):
        manifest = {'model': 'lensed', 'factory': 'check_joint_lensing:constant_factory',
                    'coordinates': {'inclination': {'min': .2, 'max': 2.7, 'prior': 'isotropic'}}}
        self.driver.validate_manifest(manifest)
        nodes, weights = np.polynomial.legendre.leggauss(24)
        us, weights = (nodes+1)/2, weights/2
        cosines = np.array([math.cos(self.driver.physical_parameters(
            {'u_inclination': u}, manifest)['inclination']) for u in us])
        lo, hi = math.cos(.2), math.cos(2.7)
        self.assertAlmostEqual(weights @ cosines, (lo+hi)/2, places=13)
        self.assertAlmostEqual(weights @ cosines**2, (lo*lo+lo*hi+hi*hi)/3, places=13)
        for u in [-.01, 1.01, float('nan'), float('inf')]:
            with self.assertRaises(ValueError):
                self.driver.physical_parameters({'u_inclination': u}, manifest)
        with self.assertRaises(ValueError):
            self.driver.physical_parameters({'inclination': .5}, manifest)

    def test_workflow_uses_explicit_normalized_sampling_box(self):
        manifest = {'model': 'lensed', 'factory': 'check_joint_lensing:constant_factory',
                    'coordinates': {'mass': {'min': 2., 'max': 8., 'prior': 'uniform'},
                                    'lens_y': {'min': .2, 'max': 20., 'prior': 'log_uniform'}}}
        with tempfile.TemporaryDirectory() as tmp:
            path = self.driver.build_workflow(manifest, Path(tmp)/'new', n_seed=32, seed=99)
            config = json.loads(path.read_text())
            self.assertEqual(config['post']['coords-sample'], 'u_mass:[0,1] u_lens_y:[0,1]')
            self.assertEqual(config['post']['settings']['n-eff'], 2000)
            grid = np.loadtxt(path.parent/'seed.dat', ndmin=2)
            self.assertEqual(grid.shape, (32, 4))
            self.assertTrue(np.all((grid[:, 2:] >= 0) & (grid[:, 2:] <= 1)))
            self.assertIn('--joint-config', config['marg-list'][0]['args'])
            with self.assertRaises(FileExistsError):
                self.driver.build_workflow(manifest, path.parent)

    def test_joint_gaussian_evidence_and_posterior_shape(self):
        coordinates = {
            'source_x': {'min': 2., 'max': 8., 'prior': 'uniform'},
            'lens_y': {'min': .2, 'max': 20., 'prior': 'log_uniform'}}
        nodes, weights = np.polynomial.legendre.leggauss(64)
        nodes, weights = (nodes + 1)/2, weights/2
        u, v = np.meshgrid(nodes, nodes, indexing='ij')
        points = np.column_stack([u.ravel(), v.ravel()])
        lnL = self.evaluate(coordinates, points, 'gaussian_factory')
        raw = np.outer(weights, weights).ravel() * np.exp(lnL)
        evidence = raw.sum()
        ix, mx, vx = truncated_normal(5., .65, 2., 8.)
        iz, mz, vz = truncated_normal(math.log(2.), .45, math.log(.2), math.log(20.))
        self.assertAlmostEqual(evidence, ix*iz/(6*math.log(100)), places=11)
        w = raw/evidence
        physical = np.column_stack([2+6*points[:, 0], math.log(.2)+math.log(100)*points[:, 1]])
        means = w @ physical
        cov = (physical-means).T @ (w[:, None]*(physical-means))
        np.testing.assert_allclose(means, [mx, mz], atol=1e-10, rtol=0)
        np.testing.assert_allclose(cov, [[vx, 0], [0, vz]], atol=1e-10, rtol=0)
        print(json.dumps({'control': 'joint_gaussian', 'Z': float(evidence),
                          'expected_Z': ix*iz/(6*math.log(100)),
                          'mean_x_logy': means.tolist(), 'cov_x_logy': cov.tolist()}))


def check_native_gaussian(integral_path, posterior_path):
    """Gate actual EOS_POST plain lnZ file and equally weighted .dat draws.

    Use >=2000 effective samples and repeat independent seeds. Tolerances allow
    Monte Carlo fluctuations; evidence alone cannot pass the shape checks.
    """
    lnz = float(np.loadtxt(integral_path))
    draws = np.genfromtxt(posterior_path, names=True)
    if draws.size < 2000:
        raise AssertionError('at least 2000 posterior draws required (also inspect sampler ESS)')
    x = 2 + 6 * draws['u_source_x']
    z = math.log(.2) + math.log(100) * draws['u_lens_y']
    ix, mx, vx = truncated_normal(5., .65, 2., 8.)
    iz, mz, vz = truncated_normal(math.log(2.), .45, math.log(.2), math.log(20.))
    expected_lnz = math.log(ix*iz/(6*math.log(100)))
    means = np.array([x.mean(), z.mean()])
    cov = np.cov(np.array([x, z]), bias=True)
    if abs(lnz-expected_lnz) > .03:
        raise AssertionError(f'lnZ {lnz} differs from exact {expected_lnz} by > .03 nat')
    np.testing.assert_allclose(means, [mx, mz], atol=.1*min(math.sqrt(vx), math.sqrt(vz)), rtol=0)
    np.testing.assert_allclose(np.diag(cov), [vx, vz], rtol=.1, atol=0)
    if abs(cov[0, 1]) > .1*math.sqrt(vx*vz):
        raise AssertionError('posterior source/lens covariance gate failed')
    print(json.dumps({'native_lnZ': lnz, 'expected_lnZ': expected_lnz,
                      'posterior_draws': int(draws.size), 'mean_x_logy': means.tolist(),
                      'cov_x_logy': cov.tolist()}))


if __name__ == '__main__':
    # Factory import in the manifest must refer to the same module as __main__.
    sys.modules['check_joint_lensing'] = sys.modules[__name__]
    if '--native-integral' in sys.argv:
        parser = argparse.ArgumentParser()
        parser.add_argument('--native-integral', required=True)
        parser.add_argument('--native-posterior', required=True)
        args = parser.parse_args()
        check_native_gaussian(args.native_integral, args.native_posterior)
    else:
        unittest.main()
