"""Independent negative controls for production single-worker result collector."""
from __future__ import annotations
import ast
import json
import os
import shutil
import types
from pathlib import Path
import tempfile
import unittest
from check_joint_lensing import DRIVERS, load_driver


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

    def test_native_event_staging_alias_guard(self):
        source = DRIVERS.parents[2]/'bin'/'create_eos_posterior_pipeline'
        tree = ast.parse(source.read_text())
        loops = [node for node in tree.body if isinstance(node, ast.For)
                 and isinstance(node.target, ast.Name) and node.target.id == 'event'
                 and ast.unparse(node.iter) == 'opts.event_file']
        self.assertEqual(len(loops), 1)
        module = ast.Module(body=[loops[0]], type_ignores=[])
        code = compile(ast.fix_missing_locations(module), str(source), 'exec')
        dest = self.root/'event-0.net'
        dest.write_text('preserve same inode')
        def stage(path):
            exec(code, {'os': os, 'shutil': shutil, 'indx': 0,
                        'opts': types.SimpleNamespace(event_file=[str(path)], working_directory=str(self.root))})
        stage(dest)
        self.assertEqual(dest.read_text(), 'preserve same inode')
        alias = self.root/'alias.net'
        alias.symlink_to(dest)
        stage(alias)
        self.assertEqual(dest.read_text(), 'preserve same inode')
        other = self.root/'other.net'
        other.write_text('legacy external event')
        stage(other)
        self.assertEqual(dest.read_text(), 'legacy external event')
        with self.assertRaises(FileNotFoundError):
            stage(self.root/'missing.net')

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
