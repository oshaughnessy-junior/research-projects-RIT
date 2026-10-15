"""Bounded implementation check of actual real-likelihood WTF updates.

Caltech only. Stops on any support/seam/certificate/parity failure. Endpoints
and failed attempts count against the cap even if no physical row is dispatched.
No evidence estimate, inference sample output, or efficiency claim.
"""
import os
os.environ.update(CUDA_VISIBLE_DEVICES='', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
                  OPENBLAS_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1',
                  JAX_PLATFORMS='cpu', JAX_ENABLE_X64='1', GW_SURROGATE='')
import time
START=time.perf_counter()
import resource
resource.setrlimit(resource.RLIMIT_CPU,(570,570))
resource.setrlimit(resource.RLIMIT_AS,(8*1024**3,8*1024**3))
os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})
import argparse
import hashlib
import json
from pathlib import Path
import traceback


def run(args, record, save):
    import numpy as np
    import torch
    import jax
    import jax.numpy as jnp
    from real_target import build_real_target
    from model import Velocity, Meter, frozen, wtf_loss
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    torch.manual_seed(261016);np.random.seed(261016)
    target=build_real_target(args,record,save)
    ck=target['checkpoint'];like=target['like']
    for entry in json.loads((Path(args.training)/'result.json').read_text())['data']['source_archives']:
        path=Path(args.training)/f"input-{entry['seed']}.xml.gz"
        assert hashlib.sha256(path.read_bytes()).hexdigest()==entry['xml_sha256']
    center=torch.tensor(ck['center'],dtype=torch.float64)
    scale=torch.tensor(ck['scale'],dtype=torch.float64)
    angles=torch.tensor(ck['angle_centers'],dtype=torch.float64)
    phase='bridge'
    def decode(z):
        x=z*scale+center
        return torch.stack([torch.remainder(x[:,0]+angles[0],2*np.pi),
            torch.asin(torch.tanh(x[:,1])),torch.remainder(x[:,5]+angles[2],np.pi),
            torch.acos(torch.tanh(x[:,2])),torch.remainder(x[:,4]+angles[1],2*np.pi),
            torch.exp(x[:,3])],dim=-1)
    def guard(z,theta):
        # Stop rather than clip, wrap across a local chart boundary, or redraw.
        if not torch.isfinite(z).all() or not torch.isfinite(theta).all():
            raise ValueError('nonfinite chart or physical endpoint')
        raw=z*scale+center
        periods=torch.tensor([2*np.pi,2*np.pi,np.pi],dtype=torch.float64)
        if torch.any(torch.abs(raw[:,[0,4,5]])>=periods/2-1e-6):
            raise ValueError('endpoint outside local angular chart or at its seam')
        canonical=theta[:,[0,4,2]]
        if torch.any(torch.minimum(canonical,periods-canonical)<=1e-6):
            raise ValueError('endpoint at canonical angle seam')
        if torch.any((theta[:,5]<=1)|(theta[:,5]>=10000)):
            raise ValueError('endpoint outside strict distance support (1,10000) Mpc')
        if torch.any(torch.abs(theta[:,1])>=np.pi/2-1e-8) or torch.any((theta[:,3]<=1e-8)|(theta[:,3]>=np.pi-1e-8)):
            raise ValueError('endpoint at angular coordinate pole')
    def begin(rows,kind):
        if record['attempted_endpoint_rows']+rows>256:
            raise RuntimeError('stage attempted endpoint/physical-row budget exhausted')
        record['attempted_endpoint_rows']+=rows
        item=dict(phase=phase,kind=kind,attempted_rows=rows,physical_rows=0,status='attempted')
        record['calls'].append(item);save();return item
    def physical(q,item,gradient=True,reference=False):
        record['dispatched_physical_rows']+=1;item['physical_rows']+=1;save()
        t=time.perf_counter()
        try:
            out=target['reference'](q) if reference else (like.value_and_grad(q) if gradient else like.value(q))
            v,g=out if gradient and not reference else (out,np.zeros(6))
            if not np.isfinite(v) or not np.isfinite(g).all():
                raise ValueError('nonfinite physical likelihood/gradient or failed bandlimited certificate')
            if gradient and not reference:record['jax_gradient_rows']+=1
            return float(v),np.asarray(g)
        finally:item['wall_seconds']=item.get('wall_seconds',0)+time.perf_counter()-t;save()

    class RewardBridge(torch.autograd.Function):
        @staticmethod
        def forward(ctx,theta,item):
            values=[];gradients=[]
            for q in theta.detach().cpu().numpy():
                v,g=physical(q,item);values.append(v);gradients.append(g)
            g=torch.tensor(np.array(gradients),dtype=theta.dtype)
            ctx.save_for_backward(g)
            item['status']='passed';save()
            return torch.tensor(values,dtype=theta.dtype)
        @staticmethod
        def backward(ctx,upstream):
            (g,)=ctx.saved_tensors
            record['torch_reward_backward_rows']+=len(g);save()
            return upstream[:,None]*g,None
    def reward(z):
        item=begin(len(z),'torch_jax_value_gradient')
        theta=decode(z)
        try:guard(z.detach(),theta.detach())
        except Exception as exc:
            item['status']='rejected_domain';item['reason']=str(exc);save();raise
        try:return RewardBridge.apply(theta,item)
        except BaseException:
            item['status']='failed';save();raise

    # Expected rejection fixtures exercise the same guard, with zero evaluator
    # dispatch. They still count against the attempted-endpoint allocation.
    phase='guard_contract'
    fixtures=[]
    for d,value in [(3,np.log(10001.)),(4,np.pi),(0,-float(angles[0]))]:
        bad=torch.tensor(target['zrows'][3:4],dtype=torch.float64)
        bad[0,d]=(value-center[d])/scale[d];fixtures.append(bad)
    for bad in fixtures:
        item=begin(1,'deliberate_domain_guard_fixture')
        try:guard(bad,decode(bad))
        except ValueError as exc:item.update(status='expected_domain_rejection',reason=str(exc))
        else:raise AssertionError('invalid endpoint passed domain/seam guard')
        save()
    record['domain_guard_contract_tests']=3
    phase='bridge'
    z=torch.tensor(target['zrows'][3:4],dtype=torch.float64,requires_grad=True)
    q=target['theta'][3]
    jac=torch.autograd.functional.jacobian(lambda x:decode(x[None])[0],z[0]).numpy()
    jac_jax=np.asarray(jax.jacfwd(lambda x:target['decode'](x,jnp))(z.detach().numpy()[0]))
    np.testing.assert_allclose(jac,jac_jax,atol=1e-10,rtol=1e-10)
    # Full Jacobian is also finite-differenced without physical evaluations.
    h=1e-6;fdjac=[]
    for d in range(6):
        e=torch.eye(6,dtype=torch.float64)[d]*h
        fdjac.append(((decode(z.detach()+e)-decode(z.detach()-e))/(2*h)).numpy()[0])
    np.testing.assert_allclose(jac,np.array(fdjac).T,atol=1e-5,rtol=1e-6)
    v=reward(z);g=torch.autograd.grad(v.sum(),z)[0].numpy()[0]
    item=begin(1,'independent_jax_value_gradient');direct,dg=physical(q,item);item['status']='passed'
    np.testing.assert_allclose(float(v.detach()[0]),direct,atol=1e-8,rtol=0)
    np.testing.assert_allclose(g,jac.T@dg,atol=1e-8,rtol=1e-9)
    item=begin(1,'numpy_noloop_value');ref,_=physical(q,item,gradient=False,reference=True);item['status']='passed'
    assert abs(ref-direct)<=1e-3
    estimates=[]
    for h in [1e-4,5e-5]:
        fd=[]
        for d in range(6):
            e=torch.eye(6,dtype=torch.float64)[d]*h
            hi=float(reward(z.detach()+e)[0]);lo=float(reward(z.detach()-e)[0]);fd.append((hi-lo)/(2*h))
        estimates.append(np.array(fd))
    tolerance=.01+.001*np.abs(g)
    err=float(np.max(np.abs(estimates[1]-g)/tolerance))
    conv=float(np.max(np.abs(estimates[1]-estimates[0])/tolerance))
    assert max(err,conv)<=1,'Torch/JAX bridge gradient finite-difference parity failed'
    record['bridge']=dict(status='passed',jacobian='torch_ad_vs_jax_ad_and_central_fd',
                          value_vs_noloop_absolute_error=abs(ref-direct),
                          gradient_error_over_tolerance=err,fd_refinement_over_tolerance=conv,
                          gradient_order='standardized six-dimensional model chart',higher_derivatives_supported=False)
    save()
    meter=Meter()
    base=Velocity(6,meter=meter).double();base.load_state_dict(ck['drift']);base=frozen(base)
    student=Velocity(6,two_time=True,meter=meter).double();student.load_state_dict(ck['flowmap'])
    reference=frozen(student)  # fixed initial reference throughout this smoke test
    frozen_before={k:v.detach().clone() for k,v in reference.state_dict().items()}
    base_before={k:v.detach().clone() for k,v in base.state_dict().items()}
    initial={k:v.detach().clone() for k,v in student.state_dict().items()}
    optimizer=torch.optim.Adam(student.parameters(),lr=1e-5)
    # Independent fixed validation noise, not used for optimizer updates.
    validation_noise=torch.randn(4,6,dtype=torch.float64)
    phase='pre_update_validation';meter.phase=phase
    with torch.no_grad():before=reward(student.jump(validation_noise,torch.zeros(4),torch.ones(4)))
    record['validation_before_mean_logL']=float(before.mean());save()
    phase='real_objective_updates';meter.phase=phase
    for step in range(8):
        noise=torch.randn(4,6,dtype=torch.float64)
        t=torch.rand(4,dtype=torch.float64);tau=t+(1-t)*torch.rand(4,dtype=torch.float64)
        loss,parts=wtf_loss(student,reference,base,noise,t,tau,reward,reward_scale=1e-3,beta=1.)
        assert torch.isfinite(loss),'nonfinite WTF loss'
        optimizer.zero_grad();loss.backward();meter.add('neural_backward_rows',4)
        assert all(p.grad is None or torch.isfinite(p.grad).all() for p in student.parameters()),'nonfinite student gradients'
        optimizer.step()
        record['updates'].append(dict(step=step,loss=float(loss.detach()),
            **{k:float(v) for k,v in parts.items()}));record['updates_completed']=step+1
        assert all(torch.equal(v,frozen_before[k]) for k,v in reference.state_dict().items())
        assert all(torch.equal(v,base_before[k]) for k,v in base.state_dict().items())
        partial=Path(args.output)/'partial_smoke.pt'
        torch.save(dict(flowmap=student.state_dict(),updates_completed=step+1,
            implementation_validation_only=True,source_checkpoint_sha256=
            hashlib.sha256(Path(args.checkpoint).read_bytes()).hexdigest()),partial)
        record['partial_checkpoint']=dict(path=str(partial),sha256=hashlib.sha256(partial.read_bytes()).hexdigest())
        record['neural_counts']=meter.snapshot();save()
    phase='post_update_validation';meter.phase=phase
    with torch.no_grad():after=reward(student.jump(validation_noise,torch.zeros(4),torch.ones(4)))
    assert torch.isfinite(after).all()
    change=float(sum((v-initial[k]).square().sum() for k,v in student.state_dict().items()).sqrt())
    assert change>0,'optimizer did not update student'
    output=Path(args.output)/'smoke_tuned.pt'
    torch.save(dict(flowmap=student.state_dict(),source_checkpoint_sha256=hashlib.sha256(Path(args.checkpoint).read_bytes()).hexdigest(),
        center=ck['center'],scale=ck['scale'],angle_centers=ck['angle_centers'],seed=261016,
        reward_scale=1e-3,updates=8,implementation_validation_only=True),output)
    record.update(status='passed_implementation_smoke_only',validation_after_mean_logL=float(after.mean()),
        student_parameter_change_l2=change,frozen_reference_and_base_unchanged=True,
        neural_counts=meter.snapshot(),checkpoint=dict(path=str(output),sha256=hashlib.sha256(output.read_bytes()).hexdigest()))


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ['inventory','checkpoint','training','output']:parser.add_argument('--'+key,required=True)
    args=parser.parse_args();out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    record=dict(status='running',attempted_endpoint_rows=0,dispatched_physical_rows=0,jax_gradient_rows=0,
        torch_reward_backward_rows=0,calls=[],updates=[],updates_completed=0,
        seed=261016,reward_scale=.001,adam_lr=.00001,beta=1.,dtype='float64',
        max_updates=8,batch_size=4,reference='frozen initial pretrained map, never refreshed',
        caps=dict(cpu_seconds=570,reserve_cpu_seconds=30,virtual_memory_bytes=8*1024**3,attempted_rows=256),
        source_parent_commit='4865d36e5af47808024a23e2bb40830499d8e57e',
        source_sha256={f:hashlib.sha256((Path(__file__).parent/f).read_bytes()).hexdigest() for f in ['smoke_real.py','real_target.py','model.py']},
        previous_stage_costs_charged_separately=True,scientific_performance_claim=False)
    def save():
        r=resource.getrusage(resource.RUSAGE_SELF)
        record['cost']=dict(process_wall_seconds=time.perf_counter()-START,cpu_user_seconds=r.ru_utime,
            cpu_system_seconds=r.ru_stime,max_rss_kib=r.ru_maxrss)
        tmp=out/'result.tmp';tmp.write_text(json.dumps(record,indent=2)+'\n');tmp.replace(out/'result.json')
    save()
    try:run(args,record,save)
    except BaseException as exc:
        record['status']='stopped';record['blocker']=type(exc).__name__+': '+str(exc);record['traceback']=traceback.format_exc()
        raise
    finally:save()
