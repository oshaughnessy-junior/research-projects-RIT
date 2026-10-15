"""Budget-continuing map-only distillation repair; no physical likelihood.

The existing stage3d timing files are charged before the process starts. Fixed
training/control recipes and seed roles are recorded before optimization. A
reserved final set is evaluated once, never used to train/select another model.
"""
import os,time,resource,math,argparse,json,hashlib,re
from pathlib import Path
START=time.perf_counter()
os.environ.update(CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})


class BudgetExhausted(RuntimeError):pass


def prior_cost(root,own):
    records=[]
    for path in sorted(Path(root).rglob('*.time')):
        if path.resolve()==Path(own).resolve():continue
        text=path.read_text()
        if not text.strip():raise RuntimeError('unresolved empty timing file: '+str(path))
        user=re.search(r'User time \(seconds\): ([0-9.]+)',text)
        system=re.search(r'System time \(seconds\): ([0-9.]+)',text)
        if not user or not system:raise RuntimeError('unresolved timing file: '+str(path))
        records.append(dict(path=str(path),cpu_seconds=float(user[1])+float(system[1])))
    return records


def main(args,r,save):
    import torch
    from model import Meter,rk4
    from periodic_model import (PeriodicVelocity,base_noise,displacement,chord_loss,rms,decode,support_mask)
    from test_periodic_model import contract_checks
    from distill_diagnostics import original_pair_coverage,semigroup_counterexample
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    assert sha(args.checkpoint)=='394ad4a1a56e2f648c11c4d14ff5a0a4a7b9a16cf39dfab3d6b29787fb580a6d'
    assert sha(args.selection_payload)=='cb2313d8d91253d5451e9f20f660d49418623bcbcca0a010a72c1f0668211a65'
    ck=torch.load(args.checkpoint,map_location='cpu',weights_only=True)
    selected=torch.load(args.selection_payload,map_location='cpu',weights_only=True)
    meter=Meter();r['_meter']=meter
    teacher=PeriodicVelocity(width=64,meter=meter).double();teacher.load_state_dict(ck['drift']);teacher.requires_grad_(False)
    teacher_before={k:v.clone() for k,v in teacher.state_dict().items()}
    center=torch.tensor(ck['center'],dtype=torch.float64);scale=torch.tensor(ck['scale'],dtype=torch.float64)
    r.update(original_coverage=original_pair_coverage(),semigroup_counterexample=semigroup_counterexample(),
        runtime=dict(host=os.uname().nodename,affinity=sorted(os.sched_getaffinity(0)),pid=os.getpid(),torch=torch.__version__),
        frozen_teacher_tensor_sha256=hashlib.sha256(b''.join(v.numpy().tobytes() for v in teacher_before.values())).hexdigest())
    def budget():
        used=resource.getrusage(resource.RUSAGE_SELF)
        if used.ru_utime+used.ru_stime>r['process_cpu_soft_seconds']:raise BudgetExhausted('remaining original five-minute allocation reached')
    meter.phase='teacher_cache_setup'
    with torch.no_grad():
        generator=torch.Generator().manual_seed(261021)
        noise=base_noise(4096,generator=generator);times=torch.linspace(0,1,33,dtype=torch.float64)
        path=[noise]
        for i in range(32):path.append(rk4(teacher,path[-1],times[i].expand(4096),times[i+1].expand(4096),steps=1))
        paths=torch.stack(path,dim=1)
        val_noise=selected['base_noise'];val_teacher=selected['teacher']
        refined=rk4(teacher,val_noise[:128],torch.zeros(128,dtype=torch.float64),torch.ones(128,dtype=torch.float64),steps=128)
        refinement=float(rms(refined,val_teacher[:128]))
    assert refinement<=.01,'frozen teacher unresolved'
    assert support_mask(decode(val_teacher,center,scale)).all(),'frozen teacher support failure'
    # No exact shared base rows. This verifies sample disjointness, not statistical
    # independence of finite teacher draws. Generator roles are fixed in advance.
    train_rows={row.numpy().tobytes() for row in noise}
    selection_rows={row.numpy().tobytes() for row in val_noise}
    assert not any(row.numpy().tobytes() in train_rows for row in val_noise)
    bank=Path(args.output)/'teacher_cache.pt'
    torch.save(dict(base_noise=noise,paths=paths,times=times,seed=261021,teacher_source_sha256=sha(args.checkpoint)),bank)
    r['teacher_cache']=dict(path=str(bank),sha256=sha(bank),rows=4096,steps=32)
    r['selection_teacher_refinement_rms']=refinement;save()

    def evaluate(model,z,target,seed):
        meter.phase='selection_validation' if seed==261020 else 'reserved_final_validation'
        g=torch.Generator().manual_seed(seed+100)
        with torch.no_grad():
            zero=torch.zeros(len(z),dtype=z.dtype);one=zero+1;mapped=model.jump(z,zero,one)
            n=256;s=torch.rand(n,dtype=z.dtype,generator=g);u=s+(1-s)*torch.rand(n,dtype=z.dtype,generator=g)
            t=u+(1-u)*torch.rand(n,dtype=z.dtype,generator=g)
            xs=rk4(teacher,z[:n],torch.zeros_like(s),s,steps=32)
            direct=model.jump(xs,s,t);composed=model.jump(model.jump(xs,s,u),u,t)
            m=dict(one_step_rms=float(rms(mapped,target)),semigroup_rms=float(rms(direct,composed)),
                diagonal_rms=float((model(xs,s,s)-teacher(xs,s)).square().mean().sqrt()),
                coordinate_rms=displacement(mapped,target).square().mean(0).sqrt().tolist(),
                map_support_failures=int((~support_mask(decode(mapped,center,scale))).sum()),
                teacher_support_failures=int((~support_mask(decode(target,center,scale))).sum()),
                nonfinite_state_rows=int((~torch.isfinite(mapped).all(-1)).sum()))
        m['contract_max_error']=max(contract_checks(teacher,model,center,scale).values())
        gates=dict(one_step=m['one_step_rms']<=.15,semigroup=m['semigroup_rms']<=.1,
            diagonal=m['diagonal_rms']<=.2,teacher_refinement=refinement<=.01,
            support=m['map_support_failures']==m['teacher_support_failures']==m['nonfinite_state_rows']==0,
            contracts=m['contract_max_error']<=1e-10)
        return m,gates,mapped

    def checkpoint(model,label,passed=False,extra=None):
        path=Path(args.output)/(label+'.pt')
        torch.save(dict(geometry='T3_R3_v1',drift=teacher.state_dict(),flowmap=model.state_dict(),
            drift_width=64,map_width=model.width,center=ck['center'],scale=ck['scale'],dtype='float64',
            base=ck['base'],source_checkpoint_sha256=sha(args.checkpoint),gates_passed=passed,
            source_sha256=r['source_sha256'],training_generator_state=train_generator.get_state(),extra=extra),path)
        return dict(path=str(path),sha256=sha(path))

    def new_model():
        model=PeriodicVelocity(two_time=True,width=64,meter=meter).double()
        model.load_state_dict(ck['flowmap']);return model
    train_generator=torch.Generator().manual_seed(261024)
    model=new_model();optimizer=torch.optim.Adam(model.parameters(),lr=1e-3)
    mixed=None;mixed_optimizer=None;wide=False;attempt=0
    baseline,bg,_=evaluate(model,val_noise,val_teacher,261020)
    r['baseline_selection']=dict(metrics=baseline,gates=bg);save()
    while True:
        budget();attempt+=1
        if attempt==1:
            label='duration_control_6000';steps=6000;endpoint=False
        elif attempt==2:
            label='endpoint_half_6000';steps=6000;endpoint=True
            model=new_model();optimizer=torch.optim.Adam(model.parameters(),lr=1e-3)
            train_generator.manual_seed(261024)  # paired stochastic draws with duration control
        elif attempt==3:label='endpoint_half_plus12000';steps=12000;endpoint=True
        elif attempt==4:label='endpoint_half_plus24000';steps=24000;endpoint=True
        else:
            endpoint=True;steps=12000;label='wide128_endpoint_chunk_'+str(attempt-4)
            if not wide:
                # Function-preserving widening: new units initially have zero
                # output contribution; no favorable initialization selection.
                old=model;torch.manual_seed(261025)
                model=PeriodicVelocity(two_time=True,width=128,meter=meter).double()
                with torch.no_grad():
                    model.net[0].weight[:64].copy_(old.net[0].weight);model.net[0].bias[:64].copy_(old.net[0].bias)
                    model.net[2].weight[:64].zero_();model.net[2].weight[:64,:64].copy_(old.net[2].weight)
                    model.net[2].bias[:64].copy_(old.net[2].bias)
                    model.net[4].weight.zero_();model.net[4].weight[:,:64].copy_(old.net[4].weight)
                    model.net[4].bias.copy_(old.net[4].bias)
                    p=noise[:32];s=torch.zeros(32,dtype=p.dtype);t=s+1
                    error=float((old.jump(p,s,t)-model.jump(p,s,t)).abs().max())
                assert error<=1e-12,'widening changed initial function';r['widening_initial_error']=error
                optimizer=torch.optim.Adam(model.parameters(),lr=1e-3);wide=True
        item=dict(label=label,steps_planned=steps,steps_completed=0,width=model.width,endpoint_half=endpoint,
            optimizer='Adam lr1e-3',status='training')
        r['attempts'].append(item);meter.phase=label;save();t0=time.perf_counter()
        try:
            for step in range(steps):
                if step%100==0:budget()
                row=torch.randint(4096,(128,),generator=train_generator)
                i=torch.randint(32,(128,),generator=train_generator)
                j=i+1+(torch.rand(128,dtype=torch.float64,generator=train_generator)*(32-i)).long()
                if endpoint:i[:64]=0;j[:64]=32
                s,t=times[i],times[j];x=paths[row,i];target=paths[row,j]
                predicted=model.jump(x,s,t)
                with torch.no_grad():diagonal=teacher(x,s)
                loss=chord_loss(predicted,target)+.1*(model(x,s,s)-diagonal).square().mean()
                assert torch.isfinite(loss),'nonfinite training loss'
                optimizer.zero_grad();loss.backward();meter.add('neural_output_backward_rows',256)
                assert all(p.grad is None or torch.isfinite(p.grad).all() for p in model.parameters()),'nonfinite map gradient'
                optimizer.step();item['steps_completed']=step+1
                if step%1000==0:item['last_loss']=float(loss.detach());save()
        except BaseException:
            item['status']='interrupted';item['checkpoint']=checkpoint(model,label+'_partial')
            item['training_wall_seconds']=time.perf_counter()-t0;save();raise
        item['training_wall_seconds']=time.perf_counter()-t0
        assert all(torch.equal(v,teacher_before[k]) for k,v in teacher.state_dict().items()),'teacher changed'
        metrics,gates,mapped=evaluate(model,val_noise,val_teacher,261020)
        item.update(status='selection_passed' if all(gates.values()) else 'selection_failed',metrics=metrics,gates=gates,
                    checkpoint=checkpoint(model,label,extra=dict(selection_gates=gates)))
        save()
        if not all(gates.values()):continue
        # One untouched confirmation; if it fails, stop without tuning to it.
        meter.phase='reserved_final_teacher';fg=torch.Generator().manual_seed(261022)
        final_noise=base_noise(1024,generator=fg)
        assert not any(row.numpy().tobytes() in train_rows for row in final_noise)
        assert not any(row.numpy().tobytes() in selection_rows for row in final_noise)
        with torch.no_grad():
            zero=torch.zeros(1024,dtype=final_noise.dtype);one=zero+1
            final_teacher=rk4(teacher,final_noise,zero,one,steps=64)
            final_refined=rk4(teacher,final_noise[:128],zero[:128],one[:128],steps=128)
            final_refinement=float(rms(final_refined,final_teacher[:128]))
        fm,fgates,final_mapped=evaluate(model,final_noise,final_teacher,261022)
        fm['teacher_refinement_rms']=final_refinement;fgates['teacher_refinement']=final_refinement<=.01
        payload=Path(args.output)/'reserved_final_validation.pt'
        torch.save(dict(noise=final_noise,teacher=final_teacher,mapped=final_mapped,seed=261022),payload)
        r['final_validation']=dict(metrics=fm,gates=fgates,payload=dict(path=str(payload),sha256=sha(payload)))
        r['status']='passed_selection_and_untouched_final_gates' if all(fgates.values()) else 'stopped_reserved_final_failed_no_retuning'
        r['final_checkpoint']=checkpoint(model,'accepted' if all(fgates.values()) else 'final_failed',
            all(fgates.values()),extra=dict(selection_gates=gates,final_gates=fgates))
        if all(fgates.values()):r['accepted_checkpoint']=r['final_checkpoint']
        r['teacher_unchanged']=all(torch.equal(v,teacher_before[k]) for k,v in teacher.state_dict().items())
        save();return


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ['allocation-root','timing-file','checkpoint','selection-payload','output']:parser.add_argument('--'+key,required=True)
    args=parser.parse_args();out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    previous=prior_cost(args.allocation_root,args.timing_file);used=sum(x['cpu_seconds'] for x in previous)
    hard=math.floor(300-used-3)
    if hard<10:raise RuntimeError('insufficient remaining original allocation')
    resource.setrlimit(resource.RLIMIT_CPU,(hard,hard));resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
    r=dict(status='running',prior_process_costs=previous,prior_cpu_seconds=used,original_allocation_cpu_seconds=300,
        remaining_at_start_seconds=300-used,process_cpu_hard_seconds=hard,process_cpu_soft_seconds=hard-2,
        source_parent_commit='34a39f4f46d9b337f965f8ed8f9ddf8e8a094fb4',new_physical_rows=0,attempts=[],
        seed_roles=dict(teacher_cache=261021,training_draws=261024,capacity_initialization=261025,
            reused_selection_validation=261020,reserved_final_validation=261022),
        schedule='A extra6000 original weights; B restart original+6000 half endpoints; C +12000; D +24000; then function-preserving width128 and fixed12000-step chunks until pass/budget',
        final_validation_policy='one reserved set, stop on failure, no retuning',
        source_sha256={f:hashlib.sha256((Path(__file__).parent/f).read_bytes()).hexdigest()
            for f in ['repair_periodic_distillation.py','periodic_model.py','test_periodic_model.py','distill_diagnostics.py','model.py']})
    def save():
        snapshot={k:v for k,v in r.items() if k!='_meter'}
        if '_meter' in r:snapshot['neural_counts']=r['_meter'].snapshot()
        usage=resource.getrusage(resource.RUSAGE_SELF)
        snapshot['cost']=dict(process_wall_seconds=time.perf_counter()-START,cpu_user_seconds=usage.ru_utime,
            cpu_system_seconds=usage.ru_stime,max_rss_kib=usage.ru_maxrss,
            allocation_cpu_seconds_in_process=used+usage.ru_utime+usage.ru_stime)
        tmp=out/'result.tmp';tmp.write_text(json.dumps(snapshot,indent=2)+'\n');tmp.replace(out/'result.json')
    save()
    try:main(args,r,save)
    except BaseException as exc:
        import traceback
        r.update(status='budget_exhausted' if isinstance(exc,BudgetExhausted) else 'stopped',
            blocker=type(exc).__name__+': '+str(exc),traceback=traceback.format_exc());raise
    finally:save()
