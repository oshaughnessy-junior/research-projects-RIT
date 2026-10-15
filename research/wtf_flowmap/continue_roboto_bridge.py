"""Resume exactly four missing physical stencils; no completed dispatch repeats."""
import os,time,resource,argparse,json,hashlib,traceback
from pathlib import Path
os.environ.update(JAX_ENABLE_X64='1',JAX_PLATFORMS='cpu',CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',RIFT_TIME_FFT_WORKERS='1')
START=time.perf_counter();CPU=time.process_time()
from run_five_arm import load_context
resource.setrlimit(resource.RLIMIT_CPU,(100,105))
import numpy as np
import torch
import jax
from copy import deepcopy
from model import Meter
from periodic_model import PeriodicVelocity,base_noise,support_mask
from periodic_wtf import estimate_value,value_gradient,control_cost,wtf_loss
from roboto_pregrid_adapter import RobotoPregrid
from common_contract_adapter import reduce_field
p=argparse.ArgumentParser()
for k in ['output','checkpoint','tuned','training','inventory','previous','journal']:p.add_argument('--'+k,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
r=dict(status='setup',journal=json.loads(Path(a.journal).read_text()),endpoint_dispatches=0,work_equivalents=0,vjp_seeds=0,new_dispatches=[],reused_stencils=[],optimizer_steps=0,checkpoint_writes=0)
def save():
 r.update(cpu_seconds=time.process_time()-CPU,wall_seconds=time.perf_counter()-START,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
 if 'meter' in globals():r['neural_counts']=meter.snapshot()
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
try:
 assert jax.config.jax_enable_x64
 assert sha(a.previous)==r['journal']['previous_raw_sha256'],'previous scientific result changed'
 previous=json.loads(Path(a.previous).read_text());case=previous['cases'][1]
 assert previous['work_equivalents']==58 and previous['cases'][0]['status']=='passed'
 assert case['name']=='actual_first_training_member0'
 cached=[d for d in previous['dispatches'] if d['label']=='actual_first_training_member0_full_fd']
 assert len(cached)==20 and all(d['status']=='completed' for d in cached)
 assert sha(a.tuned)=='71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1'
 ctx=load_context(a,r,save);ref=ctx['model'];decode=ctx['decode'];ck=ctx['checkpoint'];meter=Meter();meter.phase='four_endpoint_continuation';ref.meter=meter
 base=PeriodicVelocity(width=ck['drift_width'],meter=meter).double();base.load_state_dict(ck['drift']);base.requires_grad_(False)
 tuned=torch.load(a.tuned,map_location='cpu',weights_only=True)
 for k in ['geometry','center','scale','drift_width','map_width']:assert str(tuned[k])==str(ck[k])
 for k,v in ck['drift'].items():assert torch.equal(v,tuned['drift'][k])
 student=deepcopy(ref);student.load_state_dict(tuned['flowmap']);student.requires_grad_(True)
 initial=deepcopy(student.state_dict());initial_ref=deepcopy(ref.state_dict());initial_base=deepcopy(base.state_dict())
 gen=torch.Generator().manual_seed(261041);z=base_noise(4,generator=gen);t=torch.rand(4,dtype=torch.float64,generator=gen);tau=t+(1-t)*torch.rand(4,dtype=torch.float64,generator=gen);z=z[:1];t=t[:1];tau=tau[:1]
 with torch.no_grad():x=ref.jump(z,torch.zeros_like(t),t);middle=ref.jump(x,t,tau);endpoint=ref.jump(middle,tau,torch.ones_like(t));q=decode(endpoint).numpy()[0]
 np.testing.assert_allclose(x.numpy(),case['x'],atol=0,rtol=0);np.testing.assert_array_equal(t.numpy(),case['t']);np.testing.assert_array_equal(tau.numpy(),case['tau']);np.testing.assert_allclose(q,case['physical'],atol=1e-12,rtol=1e-12)
 adapter=RobotoPregrid(ctx,8);r['q_pregrid_reports']=adapter.reports
 data=adapter.data;dt=data.deltaT;times=np.linspace(float(data.tvals[0]),float(data.tvals[-1]),(data.npts-1)*128+1)
 np.testing.assert_array_equal([times[0],times[-1]],previous['target_contract']['window']);assert len(times)==78465 and len(times)==previous['target_contract']['nodes']
 r['target_contract']=previous['target_contract'];r['historical_training_endpoints']=dict(total=32,fully_rechecked_before=0,current_index=0,other31='unexamined; no dispatch authorized')
 def endpoint_q(state):
  with torch.no_grad():middle=ref.jump(state,t,tau);end=ref.jump(middle,tau,torch.ones_like(t));return decode(end).numpy()[0]
 def physical_value(point,index,h,axis,sign):
  assert support_mask(torch.tensor(point)[None]).all()
  if index<20:
   row=cached[index];np.testing.assert_allclose(point,row['physical'],atol=1e-12,rtol=1e-12)
   assert row['convergence_error']<1e-4
   r['reused_stencils'].append(dict(previous_dispatch_index=previous['dispatches'].index(row),h=h,axis=axis,sign=sign,value=row['quadrature_values'][-2]));save()
   return row['quadrature_values'][-2]
  if r['endpoint_dispatches']>=4 or time.process_time()-CPU>90:raise RuntimeError('continuation budget reserve reached; no full-suite retry')
  item=dict(h=h,axis=axis,sign=sign,physical=point.tolist(),status='attempted',max_time_nodes=len(times));r['new_dispatches'].append(item);r['endpoint_dispatches']+=1;r['work_equivalents']+=1;save()
  field=adapter.independent_field(point,times);assert np.isfinite(field).all()
  vals=[float(reduce_field(field[::128//f],np.zeros((len(field[::128//f]),6)),dt/f,m)[0]) for f in [32,64,128] for m in ['simpson_scipy','trapezoid_numpy_fft']]
  error=max(abs(v-vals[-2]) for v in vals);item.update(quadrature_values=vals,convergence_error=error,status='completed');save();assert error<1e-4,'new stencil quadrature unresolved'
  return vals[-2]
 estimates=[];index=0
 for h in [1e-4,5e-5]:
  fd=[]
  for axis in range(6):
   pair=[]
   for sign in [1,-1]:
    state=x+sign*torch.eye(6,dtype=x.dtype)[axis:axis+1]*h;point=endpoint_q(state);v=physical_value(point,index,h,axis,sign);index+=1
    with torch.no_grad():val=.001*v-float(control_cost(ref,base,state,t,tau)[0])
    pair.append(val)
   fd.append((pair[0]-pair[1])/(2*h))
  estimates.append(np.array(fd));r['full_fd']=[v.tolist() for v in estimates];save()
 np.testing.assert_allclose(estimates[0],case['fd_completed'],atol=1e-9,rtol=1e-9)
 gradient=np.asarray(case['full_gradient'])[0];r['full_gradient_error_ratio']=float(max(abs(estimates[1]-gradient)/(.01+.001*abs(gradient))));r['fd_refinement_ratio']=float(max(abs(estimates[1]-estimates[0])/(.01+.001*abs(estimates[0]))));save()
 assert max(r['full_gradient_error_ratio'],r['fd_refinement_ratio'])<=1,'full bridge AD/FD mismatch'
 class Bridge(torch.autograd.Function):
  @staticmethod
  def forward(c,physical):
   np.testing.assert_allclose(physical.detach().numpy()[0],q,atol=1e-12,rtol=1e-12)
   c.save_for_backward(torch.tensor([case['q8']['physical_gradient']],dtype=torch.float64));return torch.tensor([case['q8']['value']],dtype=torch.float64)
  @staticmethod
  def backward(c,upstream):meter.add('cached_reward_backward_rows',1);return upstream[:,None]*c.saved_tensors[0]
 reward=lambda e:Bridge.apply(decode(e))
 replay_gradient,replay_value=value_gradient(ref,base,x,t,tau,reward,.001)
 np.testing.assert_allclose(replay_gradient.numpy()[0],gradient,atol=1e-12,rtol=1e-12)
 r['cached_full_ad_replay_passed']=True
 leaf=x.clone().requires_grad_(True);cg=torch.autograd.grad(control_cost(ref,base,leaf,t,tau).sum(),leaf)[0];fd=[]
 for axis in range(6):
  e=torch.eye(6,dtype=x.dtype)[axis:axis+1]*1e-5
  fd.append(float(((control_cost(ref,base,x+e,t,tau)-control_cost(ref,base,x-e,t,tau))/(2e-5)).detach()))
 torch.testing.assert_close(cg,torch.tensor([fd],dtype=x.dtype),atol=1e-7,rtol=1e-5)
 r['control_gradient_fd_passed']=True
 loss,parts=wtf_loss(student,ref,base,z,t,tau,reward,.001,1.,torch.Generator().manual_seed(261049));loss.backward()
 assert torch.isfinite(loss) and all(p.grad is None or torch.isfinite(p.grad).all() for p in student.parameters())
 for model,initial_state in [(student,initial),(ref,initial_ref),(base,initial_base)]:assert all(torch.equal(v,initial_state[k]) for k,v in model.state_dict().items())
 assert sha(a.tuned)=='71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1' and sha(a.checkpoint)=='a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea'
 assert r['endpoint_dispatches']==4 and len(r['reused_stencils'])==20
 r.update(status='two_selected_full_bridges_passed_no_training',tuned_loss_backward_finite=True,tuned_loss=float(loss.detach()),loss_parts={k:float(v) for k,v in parts.items()},all_model_weights_unchanged=True,checkpoint_files_unchanged=True,raw_reward_shifts_nat=[c['old_to_new_raw_value'] for c in previous['cases']],original_training_endpoints_fully_rechecked=1,original_training_endpoints_unexamined=31,preflight_states_fully_rechecked=1,scope='two selected full bridges only; remaining31 training endpoints not evaluated')
except BaseException as exc:r.update(status='stopped',reason=repr(exc),traceback=traceback.format_exc())
finally:save();print(json.dumps({k:r[k] for k in ['status','endpoint_dispatches','cpu_seconds','wall_seconds']},indent=2))
