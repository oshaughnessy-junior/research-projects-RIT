"""Predeclared two-update Q8/cubic implementation smoke, never inference."""
import os,time,resource,argparse,json,hashlib,traceback,importlib.util
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
from periodic_wtf import estimate_value,wtf_loss
from test_periodic_model import contract_checks
from roboto_pregrid_adapter import RobotoPregrid
p=argparse.ArgumentParser()
for k in ['output','checkpoint','warm-start','training','inventory','patched-core','journal']:p.add_argument('--'+k,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
journal=json.loads(Path(a.journal).read_text())
r=dict(status='setup',journal=journal,physical_endpoints=0,work_equivalents=0,scalar_vjp_seeds=0,updates_completed=0,dispatches=[],updates=[],optimizer_steps=0,checkpoint_files=[],inference_run=False)
def save():
 r.update(cpu_seconds=time.process_time()-CPU,wall_seconds=time.perf_counter()-START,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
 if 'meter' in globals():r['neural_counts']=meter.snapshot()
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
def check_time():
 if time.process_time()-CPU>245:raise RuntimeError('scientific CPU reserve reached; never extend or restart')
try:
 assert jax.config.jax_enable_x64
 torch.manual_seed(261060)
 for filename,digest in journal['helper_sha256'].items():assert sha(Path(__file__).parent/filename)==digest,'helper source changed: '+filename
 assert sha(a.warm_start)==journal['warm_start_sha256']
 assert sha(a.patched_core)=='70fb605f65a3a6cda71e1819b79bcd232f6b6719b0842076cae373bb5e4f1097'
 ctx=load_context(a,r,save);ref=ctx['model'];decode=ctx['decode'];ck=ctx['checkpoint'];meter=Meter();ref.meter=meter
 base=PeriodicVelocity(width=ck['drift_width'],meter=meter).double();base.load_state_dict(ck['drift']);base.requires_grad_(False)
 warm=torch.load(a.warm_start,map_location='cpu',weights_only=True)
 for k in ['geometry','center','scale','drift_width','map_width']:assert str(warm[k])==str(ck[k])
 assert all(torch.equal(v,ck['drift'][k]) for k,v in warm['drift'].items())
 student=deepcopy(ref);student.load_state_dict(warm['flowmap']);student.requires_grad_(True)
 initial=deepcopy(student.state_dict());frozen_ref=deepcopy(ref.state_dict());frozen_base=deepcopy(base.state_dict())
 adapter=RobotoPregrid(ctx,8);r['q_pregrid_reports']=adapter.reports
 spec=importlib.util.spec_from_file_location('RIFT.likelihood.jax_ile.core_retry_q8_retune',a.patched_core);fixed=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixed)
 data=adapter.data
 contract=dict(q_pregrid_factor=8,arrival_interpolator='cubic',terminal_quadrature='bandlimited canonical adaptive integral with research-only one-extra-doubling certificate retry',patched_core_sha256=sha(a.patched_core),physical_order=['RA','dec','psi','incl','phase','D_Mpc'],window=[float(data.tvals[0]),float(data.tvals[-1])],base_time_nodes=data.npts,deltaT=data.deltaT,duration=float(data.tvals[-1]-data.tvals[0]),reward='raw log physical time integral; no parameter prior or decoder Jacobian',uniform_time_prior_log_offset=-float(np.log(data.tvals[-1]-data.tvals[0])),dtype='float64',geometry=ck['geometry'],reference_pretrained_sha256=sha(a.checkpoint),warm_start_old_recipe_sha256=sha(a.warm_start),source_pins=journal['source_pins'],likelihood_sources=r['likelihood_sources'],prior_contract=r['normalization_convention'],q_pregrid_reports=adapter.reports)
 np.testing.assert_array_equal(contract['window'],[-.074951171875,.07470703125]);assert data.npts==614
 r['numerical_contract']=contract
 def fun(q):return fixed.fused_log_likelihood(data,*[q[i:i+1] for i in range(6)],interp='cubic',phase_marginalization=False,time_quadrature='bandlimited')[0]
 vf=jax.jit(fun);vg=jax.jit(jax.value_and_grad(fun))
 def dispatch(point,reverse,endpoint):
  check_time();we=2 if reverse else 1
  if r['physical_endpoints']>=12 or r['work_equivalents']+we>20:raise RuntimeError('physical endpoint/WE cap reached')
  point=np.asarray(point,dtype=np.float64)
  assert point.shape==(6,) and support_mask(torch.tensor(point)[None]).all(),'physical support failure before dispatch'
  item=dict(ordinal=r['physical_endpoints'],phase=r['phase'],physical=point.tolist(),endpoint_state=endpoint.detach().tolist(),reverse_vjp=reverse,we=we,status='attempted')
  r['physical_endpoints']+=1;r['work_equivalents']+=we;r['scalar_vjp_seeds']+=int(reverse);r['dispatches'].append(item);save()
  result=(vg if reverse else vf)(jnp.asarray(point,dtype=jnp.float64))
  if reverse:v,g=result;g=np.asarray(g);assert g.dtype==np.float64 and np.isfinite(g).all() and np.linalg.norm(g)>0;item['physical_gradient']=g.tolist()
  else:v=result;g=None
  v=float(np.asarray(v));assert np.isfinite(v),'nonfinite reward / certificate refusal'
  item.update(status='completed',raw_log_reward=v);save();return v,g
 class Bridge(torch.autograd.Function):
  @staticmethod
  def forward(c,q,endpoints):
   values=[];gradients=[]
   for point,endpoint in zip(q.detach().numpy(),endpoints):v,g=dispatch(point,True,endpoint);values.append(v);gradients.append(g)
   c.save_for_backward(torch.tensor(np.array(gradients),dtype=torch.float64));return torch.tensor(values,dtype=torch.float64)
  @staticmethod
  def backward(c,upstream):
   meter.add('physical_reward_backward_rows',len(upstream));return upstream[:,None]*c.saved_tensors[0],None
 def reward_gradient(endpoints):return Bridge.apply(decode(endpoints),endpoints.detach())
 def reward_value(endpoints):
  q=decode(endpoints).detach().numpy();return torch.tensor([dispatch(point,False,end)[0] for point,end in zip(q,endpoints)],dtype=torch.float64)
 # Fixed before/after states independent of the training generator; no selection.
 validation_gen=torch.Generator().manual_seed(261062);validation_z=base_noise(2,generator=validation_gen)
 vt=torch.tensor([.25,.35],dtype=torch.float64);vtau=torch.tensor([.625,.725],dtype=torch.float64)
 with torch.no_grad():vx=ref.jump(validation_z,torch.zeros_like(vt),vt)
 r['validation_states']=dict(base_noise=validation_z.tolist(),x=vx.tolist(),t=vt.tolist(),tau=vtau.tolist())
 r['phase']='before_fixed_validation';meter.phase=r['phase']
 with torch.no_grad():before=estimate_value(student,base,vx,vt,vtau,reward_value,.001)
 r['validation_before']=dict(values=before.tolist(),mean_value=float(before.mean()),raw_rewards=[d['raw_log_reward'] for d in r['dispatches']]);save()
 optimizer=torch.optim.Adam(student.parameters(),lr=1e-5,betas=(.9,.999),eps=1e-8,weight_decay=0)
 train_gen=torch.Generator().manual_seed(261061)
 def payload(validated):
  return dict(geometry=ck['geometry'],flowmap=student.state_dict(),drift=base.state_dict(),reference_flowmap=ref.state_dict(),drift_width=ck['drift_width'],map_width=ck['map_width'],center=ck['center'],scale=ck['scale'],base=ck['base'],reward_scale=.001,beta=1.,adam_lr=1e-5,updates_completed=r['updates_completed'],prior_old_recipe_updates=warm['updates_completed'],optimizer_state=optimizer.state_dict(),training_generator_state=train_gen.get_state(),global_rng=torch.random.get_rng_state(),numerical_contract=contract,predeclared_journal=journal,source_checkpoint_sha256=sha(a.checkpoint),warm_start_sha256=sha(a.warm_start),implementation_validation_only=True,q8_retuning_smoke_passed=validated,validation_scope='finite/support/periodic/input-gradient/loss contracts only; no posterior or efficiency claim',cost_ledger_snapshot=deepcopy(r),complete_cost_ledger_files=['summary.json','retune.time','manifest.json'],complete_cost_ledger_note='snapshot through validation; external GNU time receipt additionally includes final serialization/interpreter teardown, authoritative complete command cost in manifest',historical_teacher_acquisition=dict(sampler_rows_lower_bound=15984560,cpu_and_hidden_work_unknown=True))
 for step in range(2):
  check_time();r['phase']='q8_training_update_'+str(step);meter.phase=r['phase']
  z=base_noise(4,generator=train_gen);t=torch.rand(4,dtype=torch.float64,generator=train_gen);tau=t+(1-t)*torch.rand(4,dtype=torch.float64,generator=train_gen)
  r['current_training_state']=dict(step=step,noise=z.tolist(),t=t.tolist(),tau=tau.tolist());save()
  loss,parts=wtf_loss(student,ref,base,z,t,tau,reward_gradient,.001,1.,train_gen)
  assert torch.isfinite(loss)
  optimizer.zero_grad();loss.backward();meter.add('student_output_backward_rows',8)
  assert all(p.grad is not None and torch.isfinite(p.grad).all() for p in student.parameters())
  optimizer.step();r['updates_completed']+=1;r['optimizer_steps']+=1
  assert all(torch.isfinite(v).all() for v in student.state_dict().values())
  for model,old in [(ref,frozen_ref),(base,frozen_base)]:assert all(torch.equal(v,old[k]) for k,v in model.state_dict().items())
  r['updates'].append(dict(step=step,loss=float(loss.detach()),**{k:float(v) for k,v in parts.items()}));save()
  partial=out/f'partial_after_update{step+1}_not_validated.pt';torch.save(payload(False),partial);r['checkpoint_files'].append(dict(path=partial.name,sha256=sha(partial),validated=False));save()
 r['phase']='after_fixed_validation';meter.phase=r['phase']
 with torch.no_grad():after=estimate_value(student,base,vx,vt,vtau,reward_value,.001)
 r['validation_after']=dict(values=after.tolist(),mean_value=float(after.mean()),raw_rewards=[d['raw_log_reward'] for d in r['dispatches'] if d['phase']=='after_fixed_validation'])
 r['validation_mean_value_change']=float(after.mean()-before.mean())
 meter.phase='pure_neural_contract_validation';r['periodic_contract_max_error']=max(contract_checks(base,student,ctx['center'],ctx['scale']).values())
 r['student_weight_change_l2']=float(sum((v-initial[k]).square().sum() for k,v in student.state_dict().items()).sqrt());assert r['student_weight_change_l2']>0
 assert r['physical_endpoints']==12 and r['work_equivalents']==20 and r['scalar_vjp_seeds']==8 and r['optimizer_steps']==2
 assert all(d['status']=='completed' for d in r['dispatches'])
 assert sha(a.warm_start)==journal['warm_start_sha256'] and sha(a.checkpoint)==contract['reference_pretrained_sha256']
 r.update(status='passed_two_update_q8_smoke_implementation_only',old_checkpoints_unchanged=True,frozen_reference_and_base_unchanged=True,validation_criteria_passed=True,performance_claim=False,posterior_claim=False)
 save();candidate=out/'q8_smoke_2updates.pt';torch.save(payload(True),candidate);r['checkpoint_files'].append(dict(path=candidate.name,sha256=sha(candidate),validated=True));save()
except BaseException as exc:r.update(status='stopped',reason=repr(exc),traceback=traceback.format_exc())
finally:save();print(json.dumps({k:r[k] for k in ['status','updates_completed','physical_endpoints','work_equivalents','cpu_seconds','wall_seconds']},indent=2))
