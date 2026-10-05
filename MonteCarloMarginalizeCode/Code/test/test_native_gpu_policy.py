"""Execute actual native routing/worker guard AST without likelihood work."""
import ast,contextlib,io,sys,types,unittest
from pathlib import Path
CODE=Path(__file__).resolve().parents[1]
class Policy(unittest.TestCase):
 def route(self,cpu=False,hybrid=False):
  tree=ast.parse((CODE/'bin/util_RIFT_pseudo_pipe.py').read_text());opts=types.SimpleNamespace(ile_no_gpu=cpu,ile_xpu=hybrid,ile_force_gpu=False)
  [node]=[n for n in tree.body if isinstance(n,ast.If) and ast.unparse(n.test)=='opts.ile_no_gpu or opts.ile_xpu']
  exec(compile(ast.Module(body=[node],type_ignores=[]),'native policy','exec'),{'opts':opts});return opts
 def worker(self,strict):
  tree=ast.parse((CODE/'bin/integrate_likelihood_extrinsic_batchmode').read_text())
  [node]=[n for n in tree.body if isinstance(n,ast.If) and ast.unparse(n.test)=='opts.gpu and opts.force_gpu_only and (not cupy_success)']
  opts=types.SimpleNamespace(gpu=True,force_gpu_only=strict,force_xpy=True)
  with contextlib.redirect_stdout(io.StringIO()):exec(compile(ast.Module(body=[node],type_ignores=[]),'native worker guard','exec'),{'opts':opts,'cupy_success':False,'sys':sys})
 def test_default_gpu_missing_device_exits35_even_with_force_xpy(self):
  self.assertTrue(self.route().ile_force_gpu)
  with self.assertRaises(SystemExit) as e:self.worker(True)
  self.assertEqual(e.exception.code,35)
 def test_explicit_cpu_does_not_require_cuda(self):
  self.assertFalse(self.route(cpu=True).ile_force_gpu);self.worker(False)
 def test_explicit_cross_platform_opt_in(self):
  self.assertFalse(self.route(hybrid=True).ile_force_gpu);self.worker(False)
if __name__=='__main__':unittest.main()
