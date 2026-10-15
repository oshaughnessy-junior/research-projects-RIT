"""One fixed-seed periodic drift/map rebuild: NO physical-likelihood calls.

260-second process CPU limit reserves time within the authorized 300-second
stage for separate model tests and shutdown. One CPU, hard 2-GiB address space.
Old checkpoints are not loaded; only their hash-verified data provenance is used.
"""
import os
os.environ.update(CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
import time
START=time.perf_counter()
import resource
resource.setrlimit(resource.RLIMIT_CPU,(260,260))
resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})
import argparse
import hashlib
import json
from pathlib import Path
import traceback
METER=None


def main(args,record,save):
    global METER
    import math
    import numpy as np
    import torch
    from scipy.stats import wasserstein_distance
    from igwn_ligolw import utils,ligolw,lsctables
    from model import Meter,rk4
    from periodic_model import (PeriodicVelocity,frozen,base_noise,interpolant,
                               chord_loss,rms,decode,support_mask,angular_delta)
    from test_periodic_model import contract_checks
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    torch.manual_seed(261017);np.random.seed(261017)
    dtype=torch.float64
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    assert sha(args.data_record)=='e809627592936cd16e2e55fe9abece6ea476d25323d550f47b91162b097166a9'
    provenance=json.loads(Path(args.data_record).read_text())
    meta=provenance['data'];assert meta['train_seeds']==[25011401,25011402,25011403]
    assert meta['validation_seeds']==[25011404]
    assert sha(meta['path'])==meta['sha256']
    assert sha(meta['manifest_path'])==meta['manifest_sha256']
    rows=[]
    for source in meta['source_archives']:
        assert sha(source['archive_path'])==source['archive_sha256']
        path=Path(args.training)/f"input-{source['seed']}.xml.gz"
        assert sha(path)==source['xml_sha256']
        doc=utils.load_filename(str(path),contenthandler=ligolw.LIGOLWContentHandler)
        table=lsctables.SimInspiralTable.get_table(doc)
        arr=np.array([[r.longitude,r.latitude,r.inclination,r.distance,r.coa_phase,r.polarization] for r in table])
        assert arr.shape==(100,6) and np.isfinite(arr).all()
        rows.append(arr)
    full=np.vstack(rows);archive=np.load(meta['path'],allow_pickle=False)
    for k,key in enumerate(['ra','dec','inclination','distance']):np.testing.assert_array_equal(full[:,k],archive[key])
    angles=full[:,[0,4,5]]*np.array([1.,1.,2.])
    raw=np.column_stack((np.arctanh(np.sin(full[:,1])),np.arctanh(np.cos(full[:,2])),np.log(full[:,3])))
    center=torch.tensor(raw[:300].mean(0),dtype=dtype);scale=torch.tensor(raw[:300].std(0),dtype=dtype)
    assert torch.all(scale>1e-8)
    data=torch.cat((torch.tensor(angles,dtype=dtype),(torch.tensor(raw,dtype=dtype)-center)/scale),dim=-1)
    train=data[:300];held=data[300:]
    record['data']=meta;record['data']['provenance_record_sha256']=sha(args.data_record)
    record['transform']=dict(state_order=['ra','phase','twice_psi','standardized_atanh_sin_dec',
        'standardized_atanh_cos_incl','standardized_log_distance'],center=center.tolist(),scale=scale.tolist(),
        angular_periods=[2*math.pi]*3,base='uniform T3 times standard Gaussian R3',metric='unit-radius angular radians and standardized nonangles')
    record['runtime']=dict(host=os.uname().nodename,affinity=sorted(os.sched_getaffinity(0)),
        torch=torch.__version__,numpy=np.__version__,dtype='float64',pid=os.getpid())
    meter=Meter();METER=meter;record['neural_counts']={}
    def snapshot():
        record['neural_counts']=meter.snapshot()
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss>2*1024**2:raise RuntimeError('2 GiB resident cap')
        save()
    drift=PeriodicVelocity(meter=meter).double();student=PeriodicVelocity(two_time=True,meter=meter).double()
    meter.phase='initial_model_contracts'
    record['initial_contract_errors']=contract_checks(drift,student,center,scale);snapshot()
    meter.phase='periodic_flow_matching';opt=torch.optim.Adam(drift.parameters(),lr=1e-3)
    history=[];t0=time.perf_counter();antipodal=0
    for step in range(2000):
        y=train[torch.randint(len(train),(128,))];z=base_noise(128);t=torch.rand(128,dtype=dtype)
        x,target=interpolant(z,y,t);antipodal+=int((target[:,:3].abs()==math.pi).sum())
        loss=(drift(x,t)-target).square().mean();assert torch.isfinite(loss)
        opt.zero_grad();loss.backward();meter.add('neural_output_backward_rows',128);opt.step()
        if step%200==0:history.append(dict(stage='flow_matching',step=step,loss=float(loss.detach())));snapshot()
    record['flow_matching_seconds']=time.perf_counter()-t0
    base=frozen(drift);meter.phase='periodic_ode_teacher';t0=time.perf_counter()
    with torch.no_grad():
        z=base_noise(512);paths=[z];times=torch.linspace(0,1,33,dtype=dtype)
        for i in range(32):paths.append(rk4(base,paths[-1],times[i].expand(512),times[i+1].expand(512),steps=1))
        paths=torch.stack(paths,dim=1)
    record['teacher_seconds']=time.perf_counter()-t0;snapshot()
    meter.phase='periodic_two_time_distillation';opt=torch.optim.Adam(student.parameters(),lr=1e-3);t0=time.perf_counter()
    for step in range(6000):
        row=torch.randint(512,(128,));i=torch.randint(32,(128,));j=i+1+(torch.rand(128,dtype=dtype)*(32-i)).long()
        s,t=times[i],times[j];x=paths[row,i];target=paths[row,j]
        predicted=student.jump(x,s,t)
        with torch.no_grad():diagonal_target=base(x,s)
        loss=chord_loss(predicted,target)+.1*(student(x,s,s)-diagonal_target).square().mean();assert torch.isfinite(loss)
        opt.zero_grad();loss.backward();meter.add('neural_output_backward_rows',256);opt.step()
        if step%200==0:history.append(dict(stage='distillation',step=step,loss=float(loss.detach())));snapshot()
    record['distillation_seconds']=time.perf_counter()-t0
    # Preserve both completed models even if a validation contract raises.
    checkpoint=Path(args.output)/'periodic_pretrained.pt'
    torch.save(dict(geometry='T3_R3_v1',drift=drift.state_dict(),flowmap=student.state_dict(),center=center.tolist(),
        scale=scale.tolist(),width=64,seed=261017,base='uniform_T3_standard_gaussian_R3',dtype='float64',
        gates_passed=False,validation_pending=True,source_sha256=record['source_sha256']),checkpoint)
    record['checkpoint']=dict(path=str(checkpoint),sha256=sha(checkpoint));snapshot()
    meter.phase='trained_model_contracts'
    record['trained_contract_errors']=contract_checks(base,student,center,scale);snapshot()
    meter.phase='independent_solver_validation';t0=time.perf_counter()
    with torch.no_grad():
        z=base_noise(1024);zero=torch.zeros(1024,dtype=dtype);one=torch.ones(1024,dtype=dtype)
        teacher=rk4(base,z,zero,one,steps=64);mapped=student.jump(z,zero,one)
        refined=rk4(base,z[:128],zero[:128],one[:128],steps=128)
        s=torch.rand(256,dtype=dtype);u=s+(1-s)*torch.rand(256,dtype=dtype);t=u+(1-u)*torch.rand(256,dtype=dtype)
        xs=rk4(base,z[:256],torch.zeros_like(s),s,steps=32)
        direct=student.jump(xs,s,t);composed=student.jump(student.jump(xs,s,u),u,t)
        segment=rk4(base,xs,s,t,steps=32)
        metrics=dict(one_step_rms=float(rms(mapped,teacher)),teacher_refinement_rms=float(rms(refined,teacher[:128])),
            semigroup_rms=float(rms(direct,composed)),heldout_segment_rms=float(rms(direct,segment)),
            diagonal_rms=float((student(xs,s,s)-base(xs,s)).square().mean().sqrt()))
        for name,x in [('map',mapped),('drift',teacher)]:
            physical=decode(x,center,scale)
            metrics[name+'_physical_support_failures']=int((~support_mask(physical)).sum())
            metrics[name+'_nonfinite_state_rows']=int((~torch.isfinite(x).all(-1)).sum())
            metrics[name+'_circular_moment_error_real']=(torch.cos(x[:,:3]).mean(0)-torch.cos(held[:,:3]).mean(0)).tolist()
            metrics[name+'_circular_moment_error_imag']=(torch.sin(x[:,:3]).mean(0)-torch.sin(held[:,:3]).mean(0)).tolist()
            metrics[name+'_positive_cosi_mass']=float((physical[:,3]<math.pi/2).double().mean())
            metrics[name+'_linear_wasserstein_standardized']=[float(wasserstein_distance(x[:,k].numpy(),held[:,k].numpy())) for k in range(3,6)]
            metrics[name+'_linear_tail_error_standardized']=(torch.quantile(x[:,3:],torch.tensor([.05,.95],dtype=dtype),dim=0)-
                torch.quantile(held[:,3:],torch.tensor([.05,.95],dtype=dtype),dim=0)).tolist()
        metrics['heldout_positive_cosi_mass']=float((held[:,4]*scale[1]+center[1]>0).double().mean())
        # Fixed 16-bin angular L1 difference, plus four circular quadrants each;
        # chart-independent summaries, not tuned success criteria.
        for name,x in [('map',mapped),('drift',teacher)]:
            canonical=torch.remainder(x[:,:3],2*math.pi);hc=torch.remainder(held[:,:3],2*math.pi)
            metrics[name+'_angular_16bin_L1']=[float((torch.histc(canonical[:,k],bins=16,min=0,max=2*math.pi)/len(x)-
                torch.histc(hc[:,k],bins=16,min=0,max=2*math.pi)/len(held)).abs().sum()) for k in range(3)]
    record['validation_seconds']=time.perf_counter()-t0
    gates={k:metrics[k]<=limit for k,limit in [('one_step_rms',.15),('teacher_refinement_rms',.01),('semigroup_rms',.1),('diagonal_rms',.2)]}
    for k in ['map','drift']:
        gates[k+'_finite_and_support']=metrics[k+'_physical_support_failures']==metrics[k+'_nonfinite_state_rows']==0
    record.update(metrics=metrics,gates=gates,gates_passed=all(gates.values()),loss_history=history,
        antipodal_training_pairs=antipodal,neural_counts=meter.snapshot())
    checkpoint=Path(args.output)/'periodic_pretrained.pt'
    torch.save(dict(geometry='T3_R3_v1',drift=drift.state_dict(),flowmap=student.state_dict(),center=center.tolist(),
        scale=scale.tolist(),width=64,seed=261017,base='uniform_T3_standard_gaussian_R3',dtype='float64',
        gates_passed=all(gates.values()),source_sha256=record['source_sha256']),checkpoint)
    record['checkpoint']=dict(path=str(checkpoint),sha256=sha(checkpoint))
    record['status']='passed_model_only_gates' if all(gates.values()) else 'failed_model_only_gates'
    snapshot()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ['data-record','training','output']:parser.add_argument('--'+key,required=True)
    args=parser.parse_args();out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    record=dict(status='running',seed=261017,flow_matching_steps=2000,distillation_steps=6000,batch_size=128,width=64,
        new_physical_likelihood_rows=0,old_checkpoint_weights_loaded=False,physical_parity_run=False,
        source_parent_commit='12ad1573de6091a41b0aa97a79c3e438d1f7df23',
        source_sha256={f:hashlib.sha256((Path(__file__).parent/f).read_bytes()).hexdigest()
            for f in ['periodic_model.py','test_periodic_model.py','train_periodic.py','model.py','periodic_geometry.py']},
        caps=dict(process_cpu_seconds=260,stage_cpu_seconds=300,virtual_memory_bytes=2*1024**3),scientific_capability_claim=False)
    def save():
        if METER is not None:record['neural_counts']=METER.snapshot()
        r=resource.getrusage(resource.RUSAGE_SELF)
        record['cost']=dict(process_wall_seconds=time.perf_counter()-START,cpu_user_seconds=r.ru_utime,
            cpu_system_seconds=r.ru_stime,max_rss_kib=r.ru_maxrss)
        temp=out/'result.tmp';temp.write_text(json.dumps(record,indent=2)+'\n');temp.replace(out/'result.json')
    save()
    try:main(args,record,save)
    except BaseException as exc:
        record.update(status='stopped',blocker=type(exc).__name__+': '+str(exc),traceback=traceback.format_exc());raise
    finally:save()
