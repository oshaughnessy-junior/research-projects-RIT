"""Bounded array-only contract gate; no physical likelihood imports."""
import os,resource,json,time,traceback,hashlib
from pathlib import Path
os.sched_setaffinity(0,{0});resource.setrlimit(resource.RLIMIT_CPU,(25,28))
import numpy as np
from common_contract_adapter import evaluate,reduce_field
root=Path('/scratch/richard.oshaughnessy/codex_learned_wtf_20261015');out=root/'stage7/array';out.mkdir(exist_ok=False,parents=True)
r=dict(status='running',physical_calls=0,waveform_calls=0,tests=[],saved_cases=[]);start=time.perf_counter();cpu=time.process_time()
def test(name,fn):
 fn();r['tests'].append(dict(name=name,status='passed'))
def constant():
 field=np.full(17,2.);der=np.ones((17,6));v,g=reduce_field(field,der,.1,'simpson_scipy')
 np.testing.assert_allclose(v,2+np.log(1.6),atol=1e-12);np.testing.assert_allclose(g,1,atol=1e-12)
def ramp():
 guard=128;n=17;x=np.arange(n+2*guard)-guard;k=(.2*x).astype(complex)
 dk=np.zeros((len(k),6),complex);dk[:,0]=x
 q=evaluate(k,np.zeros(len(k)),dk,np.zeros(6),n,1.,guard)
 assert q['passed'],q['refusal'];assert abs(q['raw_log_integral']-np.log(np.expm1(3.2)/.2))<1e-4
 truth=16*np.exp(3.2)/np.expm1(3.2)-5
 assert abs(q['gradient'][0]-truth)<1e-3
 assert abs(q['uniform_time_log_average']-q['raw_log_integral']+np.log(16))<1e-12
 r['ramp_result']=q
def invalid():
 try:evaluate(np.zeros(273),np.arange(273),np.zeros((273,6)),np.zeros(6),17,1.,128)
 except ValueError:return
 raise AssertionError('time-dependent norm accepted')
try:
 test('analytic_constant_all_six_derivatives',constant);test('analytic_ramp_and_normalization',ramp);test('time_dependent_norm_refusal',invalid)
 for i in range(3):
  p=root/'stage5/extract'/f'primitive_{i}.npz';d=np.load(p);D=float(d['physical'][5]);k=d['kappa'][0];rho=d['rho'][0];dk=np.zeros((len(k),6),complex);dk[:,5]=-k/D;dr=np.zeros(6);dr[5]=-2*rho[0]/D
  result=evaluate(k,rho,dk,dr,len(k)-2*int(d['guard']),float(d['deltaT']),int(d['guard']))
  r['saved_cases'].append(dict(index=i,source_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),**result));assert result['passed'],f'saved case{i} failed reference convergence'
 r['status']='array_gate_passed';r['physical_phase_eligible']=True
except BaseException as e:
 r['status']='array_gate_failed';r['error']=repr(e);r['traceback']=traceback.format_exc();r['physical_phase_eligible']=False
finally:
 r['cpu_seconds']=time.process_time()-cpu;r['wall_seconds']=time.perf_counter()-start;r['peak_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False));print(json.dumps({k:v for k,v in r.items() if k not in ['ramp_result','saved_cases']},indent=2))
 if r['status']!='array_gate_passed':raise SystemExit(1)
