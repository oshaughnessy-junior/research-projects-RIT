"""Independent negative controls for production single-worker result collector."""
from __future__ import annotations
import json
from pathlib import Path
import tempfile
import unittest
from check_joint_lensing import load_driver


class CollectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver = load_driver()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        post = self.root / 'iteration_1_post'
        post.mkdir()
        self.workflow = self.root/'hyperpipe.yaml'
        self.config = {'arch': {'n-iterations': 2, 'explode-marg-jobs': 1},
                       'post': {'coords-fit': 'u_x', 'coords-sample': 'u_x:[0,1]'},
                       'general': {'rundir': str(self.root)}}
        self.save_config()
        self.integral = post / 'output-2-7'
        self.integral.write_text('-2.7\n')
        self.posterior = Path(str(self.integral)+'.dat')
        for path in [self.posterior, self.root/'grid-2.dat', self.root/'all.marg_net']:
            path.write_text('# lnL sigma_lnL u_x\n-1 0 .4\n-2 0 .7\n')

    def save_config(self):
        self.workflow.write_text(json.dumps(self.config))

    def collect(self):
        return self.driver.collect_result(self.workflow)

    def test_valid_and_wrong_iteration(self):
        self.assertEqual(self.collect()['log_evidence_ratio'], -2.7)
        self.config['arch']['n-iterations'] = 3
        self.save_config()
        with self.assertRaises((ValueError, FileNotFoundError)):
            self.collect()

    def test_missing_and_ambiguous_workers(self):
        self.posterior.unlink()
        with self.assertRaises((ValueError, FileNotFoundError)):
            self.collect()
        (self.integral.parent/'output-2-8').write_text('-2.8\n')
        with self.assertRaises(ValueError):
            self.collect()

    def test_multiworker_refused(self):
        self.config['arch']['explode-marg-jobs'] = 2
        self.save_config()
        with self.assertRaises(ValueError):
            self.collect()

    def test_nonfinite_integral(self):
        self.integral.write_text('nan\n')
        with self.assertRaises(ValueError):
            self.collect()

    def test_out_of_prior_or_wrong_schema_posterior(self):
        for text in ['# lnL sigma_lnL u_x\n-1 0 1.1\n',
                     '# other\n.4\n',
                     '# lnL sigma_lnL u_x\nnan 0 .4\n']:
            self.posterior.write_text(text)
            with self.assertRaises(ValueError):
                self.collect()

    def test_missing_cumulative_likelihood(self):
        (self.root/'all.marg_net').unlink()
        with self.assertRaises((ValueError, FileNotFoundError)):
            self.collect()


if __name__ == '__main__':
    unittest.main()
