"""Bounded actual periodic-WTF objective smoke, not an inference experiment.

One stage ledger covers all test/setup/failure/repair CPU, attempted endpoints
and completed updates. Coefficients/seed roles are fixed before target dispatch.
"""
import os,time,resource,argparse,json,hashlib,math,re,traceback
from pathlib import Path
os.environ.update(CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',
    NUMEXPR_NUM_THREADS='1',JAX_PLATFORMS='cpu',JAX_ENABLE_X64='1',GW_SURROGATE='')
START=time.perf_counter();os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})


def main(args,r,save):
    import numpy as np
    import torch
    from copy import deepcopy
    from model import Meter
    from periodic_model import PeriodicVelocity,base_noise,frozen,support_mask
    from periodic_wtf import estimate_value,value_gradient,control_cost,wtf_loss
    from periodic_real_context import load_context
    from test_periodic_model import contract_checks
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    context=load_context(args,r,save);ck=context['checkpoint'];decode=context['decode']
    bank=context['bank'];like=bank['like'];ref_like=bank['reference']
    meter=Meter();r['_meter']=meter
    reference=context['model'];reference.meter=meter
    base=PeriodicVelocity(width=ck['drift_width'],meter=meter).double();base.load_state_dict(ck['drift']);base.requires_grad_(False)
    student=deepcopy(reference);student.meter=meter;student.requires_grad_(True)
    initial={k:v.clone() for k,v in reference.state_dict().items()}
    initial_base={k:v.clone() for k,v in base.state_dict().items()}
    r['_phase']='guard_contracts';r['_context']={}
    def begin(q,z,kind):
        rows=len(q)
        if r['prior_attempted_rows']+r['attempted_rows']+rows>256:raise RuntimeError('stage endpoint budget exhausted')
        r['attempted_rows']+=rows
        item=dict(phase=r['_phase'],kind=kind,attempted_rows=rows,physical_rows=0,status='attempted')
        r['calls'].append(item);save()
        torch.save(dict(physical=q.detach().clone(),endpoint_state=z.detach().clone() if z is not None else None,
            context=r['_context'],global_rng=torch.random.get_rng_state(),source_checkpoint_sha256=sha(args.checkpoint),
            phase=r['_phase'],ordinal=r['attempted_rows'],last_partial_checkpoint=r.get('partial_checkpoint')),
            Path(args.output)/'last_dispatch.pt')
        return item
    def guard(q):
        if not support_mask(q).all():raise ValueError('nonfinite or physical distance/pole support failure')
    def dispatch(q,item,backend):
        r['physical_rows']+=1;item['physical_rows']+=1;save();t0=time.perf_counter()
        try:
            if backend=='jax':
                r['jax_gradient_rows']+=1;save();v,g=like.value_and_grad(q.detach().numpy())
            else:v=ref_like(q.detach().numpy());g=None
            if not np.isfinite(v) or (g is not None and not np.isfinite(g).all()):
                raise ValueError('nonfinite likelihood/gradient or bandlimited certificate failure')
            return v,g
        finally:item['wall_seconds']=item.get('wall_seconds',0)+time.perf_counter()-t0;save()
    class PhysicalBridge(torch.autograd.Function):
        @staticmethod
        def forward(ctx,q,item):
            values=[];gradients=[]
            for row in q:
                value,gradient=dispatch(row,item,'jax');values.append(value);gradients.append(gradient)
            ctx.save_for_backward(torch.tensor(np.asarray(gradients),dtype=q.dtype))
            item['physical_gradient_norms']=[float(np.linalg.norm(g)) for g in gradients]
            r['_last_physical_gradient_norms']=item['physical_gradient_norms']
            r['_last_values']=np.asarray(values);item['status']='passed';save()
            return torch.tensor(values,dtype=q.dtype)
        @staticmethod
        def backward(ctx,upstream):
            (g,)=ctx.saved_tensors;r['torch_reward_backward_rows']+=len(g);save()
            return upstream[:,None]*g,None
    def reward(z,backend='jax'):
        q=decode(z);item=begin(q,z,'value_gradient' if backend=='jax' else 'numpy_value')
        try:guard(q)
        except Exception as exc:item.update(status='rejected_support',reason=str(exc));save();raise
        try:
            if backend=='jax':return PhysicalBridge.apply(q,item)
            values=[dispatch(row,item,'numpy')[0] for row in q]
            r['_last_values']=np.asarray(values);item['status']='passed';save()
            return torch.tensor(values,dtype=z.dtype)
        except BaseException as exc:item.update(status='failed',reason=type(exc).__name__+': '+str(exc));save();raise
    numpy_reward=lambda z:reward(z,'numpy')
    # Expected invalid physical fixtures are counted even without dispatch.
    q0=torch.tensor(context['theta'][3],dtype=torch.float64)
    for axis,value in [(5,1.),(5,10000.),(1,-math.pi/2),(1,math.pi/2),(3,0.),(3,math.pi),(0,float('nan')),(0,float('inf'))]:
        q=q0[None].clone();q[0,axis]=value;item=begin(q,None,'deliberate_guard_fixture')
        try:guard(q)
        except ValueError:item['status']='expected_support_rejection'
        else:raise AssertionError('invalid support fixture accepted')
        save()
    r['support_guard_fixtures_passed']=8
    # Two fixed validation states independent of the update generator. Verify
    # the ACTUAL implementation before any optimizer step, using NoLoop FD.
    validation_generator=torch.Generator().manual_seed(261040)
    check_noise=base_noise(2,generator=validation_generator)
    for index in range(2):
        r['_phase']='pre_update_full_value_gradient';meter.phase=r['_phase']
        t=torch.tensor([.25+.1*index],dtype=torch.float64);tau=torch.tensor([.625+.1*index],dtype=torch.float64)
        with torch.no_grad():x=reference.jump(check_noise[index:index+1],torch.zeros_like(t),t)
        r['_context']=dict(index=index,base_noise=check_noise[index:index+1],x=x,t=t,tau=tau)
        grad,value=value_gradient(reference,base,x,t,tau,reward,.001);raw_jax=r['_last_values'].copy()
        with torch.no_grad():nv=estimate_value(reference,base,x,t,tau,numpy_reward,.001)
        raw_error=float(np.max(np.abs(raw_jax-r['_last_values'])));assert raw_error<=.001,'preflight raw physical value parity'
        estimates=[]
        for h in [1e-4,5e-5]:
            fd=[]
            for axis in range(6):
                e=torch.eye(6,dtype=x.dtype)[axis:axis+1]*h
                with torch.no_grad():
                    plus=estimate_value(reference,base,x+e,t,tau,numpy_reward,.001)
                    minus=estimate_value(reference,base,x-e,t,tau,numpy_reward,.001)
                fd.append(float((plus-minus)/(2*h)))
            estimates.append(torch.tensor(fd,dtype=x.dtype)[None])
        tol=.01+.001*grad.abs()
        error=float(((estimates[1]-grad).abs()/tol).max())
        refinement=float(((estimates[1]-estimates[0]).abs()/(.01+.001*estimates[0].abs())).max())
        assert max(error,refinement)<=1,'real two-lookahead/control value-gradient parity failed'
        leaf=x.clone().requires_grad_(True);cost=control_cost(reference,base,leaf,t,tau)
        cg=torch.autograd.grad(cost.sum(),leaf)[0];cost_fd=[]
        for axis in range(6):
            e=torch.eye(6,dtype=x.dtype)[axis:axis+1]*1e-5
            cost_fd.append(float((control_cost(reference,base,x+e,t,tau)-control_cost(reference,base,x-e,t,tau))/(2e-5)))
        torch.testing.assert_close(cg,torch.tensor(cost_fd,dtype=x.dtype)[None],atol=1e-7,rtol=1e-5)
        r['gradient_checks'].append(dict(index=index,raw_value_difference_nat=raw_error,
            value_gradient_error_over_tolerance=error,fd_refinement_over_tolerance=refinement,
            control_gradient_norm=float(cg.norm()),scaled_reward_pullback_norm=float((grad+cg).norm()),
            control_gradient_fd_passed=True))
        assert float((grad+cg).norm())>0,'real-reward pullback unexpectedly zero'
        save()
    r['full_value_gradient_checks_passed_before_updates']=True;save()
    # Fixed before/after policy-value diagnostic. It is not used to pick
    # coefficients, batches, number of updates or a successful checkpoint.
    vg=torch.Generator().manual_seed(261042);validation_noise=base_noise(4,generator=vg)
    vt=torch.full((4,),.25,dtype=torch.float64);vtau=torch.full((4,),.625,dtype=torch.float64)
    with torch.no_grad():validation_x=reference.jump(validation_noise,torch.zeros_like(vt),vt)
    r['_phase']='pre_update_policy_validation';meter.phase=r['_phase'];r['_context']=dict(x=validation_x,t=vt,tau=vtau)
    with torch.no_grad():before=estimate_value(reference,base,validation_x,vt,vtau,reward,.001)
    r['validation_before']=dict(mean_value=float(before.mean()),mean_logL=float(r['_last_values'].mean()));save()
    optimizer=torch.optim.Adam(student.parameters(),lr=1e-5)
    train_generator=torch.Generator().manual_seed(261041)
    for step in range(8):
        assert r['prior_updates_completed']+r['updates_completed']<8
        r['_phase']='actual_periodic_wtf_updates';meter.phase=r['_phase']
        noise=base_noise(4,generator=train_generator);t=torch.rand(4,dtype=torch.float64,generator=train_generator)
        tau=t+(1-t)*torch.rand(4,dtype=torch.float64,generator=train_generator)
        r['_context']=dict(step=step,base_noise=noise,t=t,tau=tau,training_generator_state=train_generator.get_state())
        loss,parts=wtf_loss(student,reference,base,noise,t,tau,reward,.001,1.,train_generator)
        assert torch.isfinite(loss),'nonfinite periodic WTF loss'
        optimizer.zero_grad();loss.backward();meter.add('student_output_backward_rows',8)
        assert all(p.grad is None or torch.isfinite(p.grad).all() for p in student.parameters()),'nonfinite student gradient'
        optimizer.step();r['updates_completed']=step+1
        assert all(torch.equal(v,initial[k]) for k,v in reference.state_dict().items()),'reference changed'
        assert all(torch.equal(v,initial_base[k]) for k,v in base.state_dict().items()),'base changed'
        r['updates'].append(dict(step=step,loss=float(loss.detach()),
            physical_gradient_norms=r['_last_physical_gradient_norms'],**{k:float(v) for k,v in parts.items()}))
        partial=Path(args.output)/'partial_tuned.pt'
        torch.save(dict(geometry=ck['geometry'],flowmap=student.state_dict(),drift=base.state_dict(),
            drift_width=base.width,map_width=student.width,center=ck['center'],scale=ck['scale'],base=ck['base'],
            updates_completed=step+1,reward_scale=.001,beta=1.,adam_lr=1e-5,seed_roles=r['seed_roles'],
            source_checkpoint_sha256=sha(args.checkpoint),source_sha256=r['source_sha256'],
            optimizer_state=optimizer.state_dict(),training_generator_state=train_generator.get_state(),
            implementation_validation_only=True),partial)
        r['partial_checkpoint']=dict(path=str(partial),sha256=sha(partial));save()
    r['_phase']='post_update_policy_validation';meter.phase=r['_phase'];r['_context']=dict(x=validation_x,t=vt,tau=vtau)
    with torch.no_grad():after=estimate_value(student,base,validation_x,vt,vtau,reward,.001)
    r['validation_after']=dict(mean_value=float(after.mean()),mean_logL=float(r['_last_values'].mean()))
    r['trained_contract_max_error']=max(contract_checks(base,student,context['center'],context['scale']).values())
    change=float(sum((v-initial[k]).square().sum() for k,v in student.state_dict().items()).sqrt());assert change>0
    r.update(student_parameter_change_l2=change,frozen_reference_and_base_unchanged=True,
        status='passed_periodic_real_objective_smoke_implementation_only',genuinely_reward_finetuned_periodic_model_exists=True,
        final_checkpoint=r['partial_checkpoint'])
    assert r['attempted_rows']==100 and r['physical_rows']==92 and r['updates_completed']==8
    final_payload=torch.load(r['partial_checkpoint']['path'],map_location='cpu',weights_only=True)
    final_payload.update(real_objective_smoke_passed=True,validation_scope='implementation only, no calibrated inference',
        full_value_gradient_checks=r['gradient_checks'],validation_before=r['validation_before'],validation_after=r['validation_after'])
    final=Path(args.output)/'periodic_wtf_tuned.pt';torch.save(final_payload,final)
    r['final_checkpoint']=dict(path=str(final),sha256=sha(final))
    save()


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ['stage-root','timing-file','inventory','checkpoint','training','output']:p.add_argument('--'+key,required=True)
    args=p.parse_args();root=Path(args.stage_root);out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    previous=[];old_rows=old_updates=0
    for path in root.rglob('*.time'):
        if path.resolve()==Path(args.timing_file).resolve():continue
        text=path.read_text();u=re.search(r'User time \(seconds\): ([0-9.]+)',text);s=re.search(r'System time \(seconds\): ([0-9.]+)',text)
        if not u or not s:raise RuntimeError('unresolved prior timing file '+str(path))
        previous.append(dict(path=str(path),cpu_seconds=float(u[1])+float(s[1])))
    for path in root.rglob('result.json'):
        if path.parent==out:continue
        record=json.loads(path.read_text())
        if record.get('stage')=='periodic_wtf_real_objective_smoke_v1':
            old_rows+=record['attempted_rows'];old_updates+=record['updates_completed']
    used=sum(a['cpu_seconds'] for a in previous);hard=min(560,math.floor(600-used-5))
    if hard<10 or old_rows>=256 or old_updates:raise RuntimeError('insufficient budget or partial updates require explicit resume, never restart silently')
    resource.setrlimit(resource.RLIMIT_CPU,(hard,hard));resource.setrlimit(resource.RLIMIT_AS,(8*1024**3,8*1024**3))
    r=dict(stage='periodic_wtf_real_objective_smoke_v1',status='running',attempted_rows=0,physical_rows=0,jax_gradient_rows=0,
        torch_reward_backward_rows=0,prior_attempted_rows=old_rows,prior_updates_completed=old_updates,
        prior_timing_records=previous,prior_cpu_seconds=used,calls=[],updates=[],updates_completed=0,gradient_checks=[],
        seed_roles=dict(real_gradient_checks=261040,training=261041,fixed_validation=261042),reward_scale=.001,beta=1.,adam_lr=1e-5,
        metric='unit-radius T3 radians and standardized R3, ordinary tangent sum-square',
        reference='initial repaired map frozen for all updates; base frozen',
        source_parent_commit='06a6c67ef68a76bafd070dacb0c51215c4c5aca0',
        caps=dict(stage_cpu_seconds=600,process_cpu_seconds=hard,memory_gib=8,attempted_rows=256,max_updates=8,batch=4),
        source_sha256={f:hashlib.sha256((Path(__file__).parent/f).read_bytes()).hexdigest()
            for f in ['smoke_periodic_wtf.py','periodic_wtf.py','periodic_real_context.py','periodic_target_bank.py','periodic_model.py','test_periodic_wtf.py']},
        scientific_capability_or_cost_win_claimed=False,inference_run=False)
    def save():
        snap={k:v for k,v in r.items() if not k.startswith('_')}
        if '_meter' in r:snap['neural_counts']=r['_meter'].snapshot()
        usage=resource.getrusage(resource.RUSAGE_SELF);snap['cost']=dict(process_wall_seconds=time.perf_counter()-START,
            cpu_user_seconds=usage.ru_utime,cpu_system_seconds=usage.ru_stime,max_rss_kib=usage.ru_maxrss)
        temp=out/'result.tmp';temp.write_text(json.dumps(snap,indent=2)+'\n');temp.replace(out/'result.json')
    save()
    try:main(args,r,save)
    except BaseException as exc:
        r.update(status='stopped',blocker=type(exc).__name__+': '+str(exc),traceback=traceback.format_exc());raise
    finally:save()
