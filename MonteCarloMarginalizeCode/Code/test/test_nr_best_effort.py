"""Analytic mixture checks; no waveform library, frames, GPU, or cluster needed."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import numpy as np

SCRIPT = Path(__file__).resolve().parents[1] / 'bin' / 'util_NRBestEffortPosterior.py'
if not SCRIPT.exists():  # also allow the development staging directory
    SCRIPT = Path(__file__).with_name('util_NRBestEffortPosterior.py')
spec = importlib.util.spec_from_file_location('nr_best_effort', SCRIPT)
nr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nr)


def rows(masses=(10., 11., 14.), ratio=1., log_l=1000.):
    return np.array([[-1, m/(1+ratio), m*ratio/(1+ratio), 0,0,0,0,0,0,
                      log_l, .1, 10000, 100] for m in masses])


class NRBestEffortTests(unittest.TestCase):
    def test_nonuniform_grid_and_constant_evidence(self):
        plan = nr.make_plan(np.vstack([rows(), rows(ratio=.5, log_l=1000+np.log(3))]), (10,14))
        np.testing.assert_allclose([s['posterior_probability'] for s in plan['simulations']], [.75,.25])
        for s in plan['simulations']:
            components = [c for c in plan['components'] if c['simulation'] == s['simulation']]
            np.testing.assert_allclose(nr.probabilities([c['log_weight'] for c in components]), [.125,.5,.375])
        self.assertAlmostEqual(plan['simulations'][1]['log_evidence'], 1000)

    def test_linear_likelihood_integral(self):
        data = rows(log_l=0)
        data[:,9] = np.log([1,2,5])  # L(M)=M-9; integral / range = 3
        plan = nr.make_plan(data, (10,14))
        self.assertAlmostEqual(plan['simulations'][0]['log_evidence'], np.log(3))

    def test_linear_mass_prior(self):
        plan = nr.make_plan(rows(), (10,14), power=1)
        self.assertAlmostEqual(plan['simulations'][0]['log_evidence'],1000)
        np.testing.assert_allclose(nr.probabilities([c['log_weight'] for c in plan['components']]),
                                   np.array([.5,2,1.5])*[10,11,14]/48)

    def test_log_mass_prior(self):
        plan = nr.make_plan(rows(), (10,14), power=-1)
        self.assertTrue(np.isfinite(plan['simulations'][0]['log_evidence']))

    def test_simulation_prior(self):
        data = np.vstack([rows(),rows(ratio=.5)])
        plan = nr.make_plan(data,(10,14))
        priors = {s['simulation']: float(i+1) for i,s in enumerate(plan['simulations'])}
        plan = nr.make_plan(data,(10,14), simulation_priors=priors)
        np.testing.assert_allclose([s['posterior_probability'] for s in plan['simulations']], [1/3,2/3])

    def test_reject_duplicate_and_missing_mass_support(self):
        for data in [np.vstack([rows(), rows()[:1]]), rows((11,12,14)), rows((10,11,13))]:
            with self.assertRaises(ValueError):
                nr.make_plan(data,(10,14))

    def test_reject_bad_input(self):
        for data in [rows()[:,:12], rows()*np.nan, rows()[:1]]:
            with self.assertRaises(ValueError):
                nr.make_plan(data,(10,14))
        for bounds in [(0,14),(14,10),(10,float('inf'))]:
            with self.assertRaises(ValueError):
                nr.make_plan(rows(),bounds)

    def test_partial_mass_support_keeps_common_prior_normalization(self):
        data=np.vstack([rows(),rows((11,12,14),ratio=.5)])
        plan=nr.make_plan(data,(10,14),allow_partial_mass_range=True)
        np.testing.assert_allclose([s['posterior_probability'] for s in plan['simulations']],[3/7,4/7])
        self.assertFalse(plan['simulations'][0]['complete_mass_range'])

    def test_eccentric_and_anomaly_are_distinct_spokes(self):
        data = np.insert(rows(), 9, .2, axis=1)
        data = np.insert(data, 10, .1, axis=1)
        other = data.copy()
        other[:,10] = .7
        plan = nr.make_plan(np.vstack([data,other]),(10,14), eccentric=True, mean_per_ano=True)
        self.assertEqual(len(plan['simulations']),2)
        self.assertEqual(plan['components'][0]['intrinsic']['eccentricity'], .2)

    def test_sigma_does_not_change_catalog_prior(self):
        data = rows()
        plan = nr.make_plan(data,(10,14))
        data[:,10] = [.01,.3,.8]
        other = nr.make_plan(data,(10,14))
        self.assertEqual([c['log_weight'] for c in plan['components']],
                         [c['log_weight'] for c in other['components']])

    def test_ratio_rounding_boundary_does_not_split_spoke(self):
        data=rows(ratio=.50004998)
        data[1]=rows(ratio=.50005002)[1]
        plan=nr.make_plan(data,(10,14))
        self.assertEqual(len(plan['simulations']),1)
        other=nr.make_plan(data[::-1],(10,14))
        self.assertEqual(plan,other)

    def test_repeat_coalescence_matches_known_variance_weighted_mean(self):
        data=rows()
        repeat=data[:1].copy()
        repeat[:,9]+=np.log(2)
        repeat[:,10]=.05  # same absolute sigma as original -> equal precision
        plan=nr.make_plan(np.vstack([data,repeat]),(10,14),coalesce_repeats=True)
        first=plan['components'][0]
        self.assertAlmostEqual(first['log_marginal_likelihood'],1000+np.log(1.5))
        self.assertAlmostEqual(first['relative_error'],.1/np.sqrt(2)/1.5)
        self.assertEqual(plan['simulations'][0]['combined_repeat_rows'],1)

    def test_log_weight_validation(self):
        for logs in [[],[np.nan],[np.inf],[-np.inf]]:
            with self.assertRaises(ValueError):
                nr.probabilities(logs)
        np.testing.assert_allclose(nr.probabilities([1000,1000-np.log(3)]),[.75,.25])

    def setup_pools(self, directory):
        # Unequal conditional sample counts must NOT bias the mixture.
        np.savetxt(directory/'a.dat', np.ones((10,1)),header='distance')
        np.savetxt(directory/'b.dat', np.zeros((1000,1)),header='distance')
        return dict(schema_version=1,sample_kind='fairdraw',components=[
            dict(id=0,log_weight=1000+np.log(3),samples=['a.dat']),
            dict(id=1,log_weight=1000,samples=['b.dat'])])

    def test_combination_ignores_pool_size_and_is_reproducible(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            plan = self.setup_pools(directory)
            result,ids,stats = nr.combine(plan,directory,30000,71)
            self.assertAlmostEqual(result['distance'].mean(),.75,delta=.01)
            other,other_ids,_ = nr.combine(plan,directory,30000,71)
            np.testing.assert_equal(result,other)
            np.testing.assert_equal(ids,other_ids)
            self.assertEqual(stats['0']['available'],10)

    def test_missing_pool_is_never_silently_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)
            plan=self.setup_pools(directory)
            plan['components'][1]['log_weight']=-100
            plan['components'][1]['samples']=[]
            with self.assertRaisesRegex(ValueError,'missing fairdraw'):
                nr.combine(plan,directory,1,0)

    def test_reject_reused_file_raw_samples_schema_and_intrinsic_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)
            plan=self.setup_pools(directory)
            plan['sample_kind']='importance'
            with self.assertRaises(ValueError): nr.combine(plan,directory,10,0)
            plan['sample_kind']='fairdraw'
            plan['components'][1]['samples']=['a.dat']
            with self.assertRaisesRegex(ValueError,'more than once'): nr.combine(plan,directory,10,0)
            plan['components'][1]['samples']=['b.dat']
            np.savetxt(directory/'b.dat', [[0]],header='ra')
            with self.assertRaisesRegex(ValueError,'schemas differ'): nr.combine(plan,directory,10,0)
            np.savetxt(directory/'b.dat', [[0]],header='distance')
            plan['components'][1]['intrinsic']={'m1':10}
            with self.assertRaisesRegex(ValueError,'lacks intrinsic'): nr.combine(plan,directory,10,0)
            np.savetxt(directory/'a.dat', [[10,10]],header='distance m1')
            np.savetxt(directory/'b.dat', [[0,12]],header='distance m1')
            with self.assertRaisesRegex(ValueError,'intrinsic mismatch'): nr.combine(plan,directory,10,0)

    def test_cli_bootstrap_and_combine(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)
            np.savetxt(directory/'all.net',np.vstack([rows(),rows(ratio=.5,log_l=1000+np.log(3))]))
            command=[sys.executable,str(SCRIPT)]
            subprocess.run(command+['plan','--composite',str(directory/'all.net'),
                '--mass-range','10','14','--output-dir',str(directory/'planned')],check=True,capture_output=True)
            manifest=directory/'planned'/'manifest.json'
            plan=json.loads(manifest.read_text())
            for c in plan['components']:
                path=manifest.parent/('cell-{}.dat'.format(c['id']))
                intrinsic=c['intrinsic']
                np.savetxt(path, [list(intrinsic.values())+[c['mass']]],header=' '.join(list(intrinsic)+['distance']))
                c['samples']=[path.name]
            manifest.write_text(json.dumps(plan))
            output=directory/'posterior.dat'
            subprocess.run(command+['combine','--manifest',str(manifest),'--output',str(output)],check=True,capture_output=True)
            samples=np.genfromtxt(output,names=True)
            self.assertEqual(len(samples),3000)
            self.assertTrue(Path(str(output)+'.provenance.json').exists())
            self.assertEqual(len(np.loadtxt(str(output)+'.component_ids')),3000)

    def test_xml_roundtrip_when_lalsuite_available(self):
        try:
            import lal
            from RIFT import lalsimutils
        except ImportError:
            self.skipTest('XML integration requires installed RIFT and lalsuite')
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)
            templates=[]
            for ratio in [1.,.5]:
                point=lalsimutils.ChooseWaveformParams()
                point.m1=10/(1+ratio)*lal.MSUN_SI
                point.m2=10*ratio/(1+ratio)*lal.MSUN_SI
                point.fref=20.
                point.fmin=15.
                point.tref=lal.LIGOTimeGPS(1242442967.4)
                templates.append(point)
            lalsimutils.ChooseWaveformParams_array_to_xml(templates,str(directory/'template'))
            plan=nr.make_plan(np.vstack([rows(),rows(ratio=.5)]),(10,14))
            nr.export_xml(plan,[0,3,2],directory/'template.xml.gz',directory)
            grid=lalsimutils.xml_to_ChooseWaveformParams_array(str(directory/'quadrature-grid.xml.gz'))
            posterior=lalsimutils.xml_to_ChooseWaveformParams_array(str(directory/'intrinsic-posterior.xml.gz'))
            self.assertEqual(len(grid),6)
            self.assertEqual(len(posterior),3)
            for point,component in zip(grid,plan['components']):
                self.assertAlmostEqual(point.m1/lal.MSUN_SI,component['intrinsic']['m1'],places=5)
                self.assertAlmostEqual(point.m2/lal.MSUN_SI,component['intrinsic']['m2'],places=5)
                self.assertAlmostEqual(float(point.tref),1242442967.4,places=5)


if __name__ == '__main__':
    unittest.main()
