"""Bounded complete periodic WTF pullback audit; no optimizer or checkpoint writes."""
import os,time,resource,json,argparse,hashlib,traceback,importlib.util
from pathlib import Path
os.environ.update(JAX_ENABLE_X64='1',JAX_PLATFORMS='cpu',CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',RIFT_TIME_FFT_WORKERS='1')
START=time.perf_counter();CPU=time.process_time()
from run_five_arm import load_context
resource.setrlimit(resource.RLIMIT_CPU,(260,265))
import numpy as np
import torch
import jax,jax.numpy as jnp
from copy import deepcopy
from model import Meter
from periodic_model import PeriodicVelocity,base_noise,support_mask
from periodic_wtf import value_gradient,estimate_value,control_cost,wtf_loss
from roboto_pregrid_adapter import RobotoPregrid
from common_contract_adapter import reduce_field
p=argparse.ArgumentParser()
for k in ['output','checkpoint','tuned','training','inventory','patched-core','journal']:p.add_argument('--'+k,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
r=dict(status='setup',journal=json.loads(Path(a.journal).read_text()),work_equivalents=0,endpoint_dispatches=0,vjp_seeds=0,dispatches=[],cases=[],optimizer_steps=0,checkpoint_writes=0)
def save():
 r.update(cpu_seconds=time.process_time()-CPU,wall_seconds=time.perf_counter()-START,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
 if 'meter' in globals():r['neural_counts']=meter.snapshot()
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
def probe(label,q,reverse,fn):
 we=2 if reverse else 1
 if r['work_equivalents']+we>64 or time.process_time()-CPU>245:raise RuntimeError('stage budget reserve reached')
 q=np.asarray(q,dtype=np.float64);assert q.shape==(6,) and support_mask(torch.tensor(q)[None]).all()
 r['work_equivalents']+=we;r['endpoint_dispatches']+=1;r['vjp_seeds']+=int(reverse)
 item=dict(label=label,physical=q.tolist(),we=we,vjp_seeds=int(reverse),max_time_nodes=78465,status='attempted');r['dispatches'].append(item);save()
 result=fn();item['status']='completed';save();return result
try:
 assert jax.config.jax_enable_x64
 ctx=load_context(a,r,save);ref=ctx['model'];decode=ctx['decode'];ck=ctx['checkpoint'];meter=Meter();ref.meter=meter
 base=PeriodicVelocity(width=ck['drift_width'],meter=meter).double();base.load_state_dict(ck['drift']);base.requires_grad_(False)
 assert sha(a.tuned)=='71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1'
 tuned=torch.load(a.tuned,map_location='cpu',weights_only=True)
 for k in ['geometry','center','scale','drift_width','map_width']:assert str(tuned[k])==str(ck[k])
 for k,v in ck['drift'].items():assert torch.equal(v,tuned['drift'][k])
 student=deepcopy(ref);student.load_state_dict(tuned['flowmap']);student.requires_grad_(True);initial=deepcopy(student.state_dict())
 adapters={f:RobotoPregrid(ctx,f) for f in [8,16]};r['q_pregrid_reports']={str(f):v.reports for f,v in adapters.items()}
 spec=importlib.util.spec_from_file_location('RIFT.likelihood.jax_ile.core_retry_stage9',a.patched_core);fixed=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixed)
 r['patched_core_sha256']=sha(a.patched_core)
 vgs={}
 for factor in [8,16]:
  def fun(q,factor=factor):return fixed.fused_log_likelihood(adapters[factor].data,*[q[i:i+1] for i in range(6)],interp='cubic',phase_marginalization=False,time_quadrature='bandlimited')[0]
  vgs[factor]=jax.jit(jax.value_and_grad(fun))
 data=adapters[8].data;dt=data.deltaT;times=np.linspace(float(data.tvals[0]),float(data.tvals[-1]),(data.npts-1)*128+1)
 r['target_contract']=dict(window=[float(times[0]),float(times[-1])],nodes=len(times),duration=float(times[-1]-times[0]),uniform_time_log_offset=-float(np.log(times[-1]-times[0])),reward='raw physical log integral; no parameter prior/decoder Jacobian',physical_order=['RA','dec','psi','incl','phase','D_Mpc'],dtype='float64')
 def direct(q,label):
  field=probe(label,q,False,lambda:adapters[8].independent_field(q,times));assert np.isfinite(field).all()
  vals=[float(reduce_field(field[::128//f],np.zeros((len(field[::128//f]),6)),dt/f,m)[0]) for f in [32,64,128] for m in ['simpson_scipy','trapezoid_numpy_fft']]
  error=max(abs(v-vals[-2]) for v in vals)
  r['dispatches'][-1].update(quadrature_values=vals,convergence_error=error);save()
  assert error<1e-4,'independent physical quadrature unresolved'
  return vals[-2]
 states=[]
 gen=torch.Generator().manual_seed(261040);z=base_noise(2,generator=gen)[:1];t=torch.tensor([.25],dtype=torch.float64);tau=torch.tensor([.625],dtype=torch.float64);states.append(('original_preflight0',z,t,tau))
 gen=torch.Generator().manual_seed(261041);z=base_noise(4,generator=gen);t=torch.rand(4,dtype=torch.float64,generator=gen);tau=t+(1-t)*torch.rand(4,dtype=torch.float64,generator=gen);states.append(('actual_first_training_member0',z[:1],t[:1],tau[:1]))
 for name,z,t,tau in states:
  meter.phase=name
  with torch.no_grad():x=ref.jump(z,torch.zeros_like(t),t);mid=ref.jump(x,t,tau);endpoint=ref.jump(mid,tau,torch.ones_like(t));q=decode(endpoint).numpy()[0]
  torch.save(dict(z=z,x=x,t=t,tau=tau,endpoint=endpoint,physical=q),out/(name+'_capture.pt'))
  case=dict(name=name,x=x.tolist(),t=t.tolist(),tau=tau.tolist(),physical=q.tolist(),status='running');r['cases'].append(case);save()
  cache={}
  for label in ['q8','old','q16']:
   fn=(lambda:ctx['bank']['like'].value_and_grad(q)) if label=='old' else (lambda label=label:vgs[int(label[1:])](jnp.asarray(q,dtype=jnp.float64)))
   v,g=probe(name+'_'+label+'_value_vjp',q,True,fn);v=float(np.asarray(v));g=np.asarray(g);assert g.dtype==np.float64 and np.isfinite(v) and np.isfinite(g).all()
   cache[label]=(v,g);case[label]=dict(value=v,physical_gradient=g.tolist());save()
  tol=.01+.001*abs(cache['q16'][1]);case['q8_q16_gradient_error_ratio']=float(max(abs(cache['q8'][1]-cache['q16'][1])/tol));case['q8_q16_value_difference']=cache['q8'][0]-cache['q16'][0]
  assert abs(case['q8_q16_value_difference'])<=.001 and case['q8_q16_gradient_error_ratio']<=1,'q8/q16 physical derivative mismatch'
  native=direct(q,name+'_native_baseline');case['native_value_difference']=native-cache['q8'][0];save();assert abs(case['native_value_difference'])<=.001,'canonical/native physical value mismatch'
  class Bridge(torch.autograd.Function):
   @staticmethod
   def forward(c,physical,label):
    np.testing.assert_allclose(physical.detach().numpy()[0],q,atol=1e-12,rtol=1e-12)
    c.save_for_backward(torch.tensor(cache[label][1][None],dtype=torch.float64));return torch.tensor([cache[label][0]],dtype=torch.float64)
   @staticmethod
   def backward(c,upstream):
    meter.add('cached_reward_backward_rows',1);return upstream[:,None]*c.saved_tensors[0],None
  def reward(label):return lambda e:Bridge.apply(decode(e),label)
  grad,value=value_gradient(ref,base,x,t,tau,reward('q8'),.001);oldg,oldv=value_gradient(ref,base,x,t,tau,reward('old'),.001)
  case.update(full_gradient=grad.tolist(),old_full_gradient=oldg.tolist(),old_to_new_full_gradient_max_abs=float(abs(grad-oldg).max()),old_to_new_raw_value=cache['q8'][0]-cache['old'][0],physical_gradient_norm_old=float(np.linalg.norm(cache['old'][1])))
  def numpy_reward(e):return torch.tensor([direct(decode(e).detach().numpy()[0],name+'_full_fd')],dtype=torch.float64)
  estimates=[]
  for h in [1e-4,5e-5]:
   fd=[]
   for axis in range(6):
    e=torch.eye(6,dtype=torch.float64)[axis:axis+1]*h
    with torch.no_grad():plus=estimate_value(ref,base,x+e,t,tau,numpy_reward,.001);minus=estimate_value(ref,base,x-e,t,tau,numpy_reward,.001)
    fd.append(float((plus-minus)/(2*h)))
   estimates.append(np.array(fd));case['fd_completed']=estimates[-1].tolist();save()
  g=grad.numpy()[0];case['full_fd']=[v.tolist() for v in estimates];case['full_gradient_error_ratio']=float(max(abs(estimates[1]-g)/(.01+.001*abs(g))));case['fd_refinement_ratio']=float(max(abs(estimates[1]-estimates[0])/(.01+.001*abs(estimates[0]))));save()
  assert max(case['full_gradient_error_ratio'],case['fd_refinement_ratio'])<=1,'full pullback finite-difference mismatch'
  leaf=x.clone().requires_grad_(True);cg=torch.autograd.grad(control_cost(ref,base,leaf,t,tau).sum(),leaf)[0];fd=[]
  for axis in range(6):
   e=torch.eye(6,dtype=x.dtype)[axis:axis+1]*1e-5
   fd.append(float(((control_cost(ref,base,x+e,t,tau)-control_cost(ref,base,x-e,t,tau))/(2e-5)).detach()))
  torch.testing.assert_close(cg,torch.tensor([fd],dtype=x.dtype),atol=1e-7,rtol=1e-5)
  case['control_gradient_fd_passed']=True
  loss,parts=wtf_loss(student,ref,base,z,t,tau,reward('q8'),.001,1.,torch.Generator().manual_seed(261049));loss.backward()
  assert torch.isfinite(loss) and all(p.grad is None or torch.isfinite(p.grad).all() for p in student.parameters())
  assert all(torch.equal(v,initial[k]) for k,v in student.state_dict().items());student.zero_grad()
  case.update(status='passed',tuned_loss_backward_finite=True,tuned_loss=float(loss.detach()),loss_parts={k:float(v) for k,v in parts.items()});save()
 assert r['work_equivalents']==62
 r.update(status='two_state_complete_bridge_passed_no_training',checkpoint_unchanged=sha(a.tuned)=='71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1',checkpoint_new_contract_validity='geometry unchanged; two historical states rechecked; all 32 historical training targets NOT replayed; not certified Q8-trained or calibrated')
except BaseException as exc:r.update(status='stopped',reason=repr(exc),traceback=traceback.format_exc())
finally:save();print(json.dumps({k:r[k] for k in ['status','work_equivalents','endpoint_dispatches','vjp_seeds','cpu_seconds','wall_seconds']},indent=2))
