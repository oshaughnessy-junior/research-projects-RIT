"""Bounded stage-2 neural drift pretraining and two-time map distillation.

Uses one hash-verified real posterior arm. No physical likelihood, reward
fine-tuning, evidence estimate, or density correction is claimed by this stage.
"""
import os
os.environ['CUDA_VISIBLE_DEVICES'] = ''
os.environ['OMP_NUM_THREADS'] = os.environ['MKL_NUM_THREADS'] = os.environ['OPENBLAS_NUM_THREADS'] = '1'
import time
START = time.perf_counter()
import resource
resource.setrlimit(resource.RLIMIT_CPU, (900, 900))
import argparse
import hashlib
import json
import tarfile
from pathlib import Path
import numpy as np
import torch
from scipy.stats import wasserstein_distance
from model import Velocity, Meter, frozen, rk4


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--npz', required=True)
    p.add_argument('--manifest', required=True)
    p.add_argument('--worker-root', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--steps', type=int, default=2000)
    p.add_argument('--seed', type=int, default=261015)
    p.add_argument('--distill-steps', type=int, default=None)
    args = p.parse_args()
    torch.set_num_threads(1); torch.set_num_interop_threads(1)
    torch.manual_seed(args.seed); np.random.seed(args.seed)
    output = Path(args.output); output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(Path(args.manifest).read_text())
    arm = next(a for a in manifest['arms'] if a['name']=='sinc-bandlimited')
    expected = '4581e8b8584922946f5ee01756b2ffb9f336d2a66171014ac95641ff365beb2e'
    assert sha(args.npz) == arm['samples_sha256'] == expected
    assert manifest['anchor_index'] == 47536
    assert [r['seed'] for r in arm['runs']] == [25011401,25011402,25011403,25011404]
    assert all(r['accepted'] and r['draws']==100 and r['neff']>=100 for r in arm['runs'])
    archive = np.load(args.npz, allow_pickle=False)
    assert all(archive[k].shape==(400,) for k in ['ra','dec','distance','inclination'])
    # Verify each complete archive before parsing its full six-dimensional rows.
    from igwn_ligolw import utils, ligolw, lsctables
    full=[]; source_records=[]
    for run in arm['runs']:
        path=Path(args.worker_root)/f"sinc-bandlimited-anchor-{run['seed']}"/'pilot-artifacts.tar.gz'
        assert sha(path)==run['raw_sha256']
        with tarfile.open(path) as tar:
            members=[m for m in tar.getmembers() if m.name=='result_0_.xml.gz' and m.isfile()]
            assert len(members)==1
            payload=tar.extractfile(members[0]).read()
        xml_path=output/f"input-{run['seed']}.xml.gz"
        xml_path.write_bytes(payload)
        document=utils.load_filename(str(xml_path),contenthandler=ligolw.LIGOLWContentHandler)
        rows=lsctables.SimInspiralTable.get_table(document)
        values=np.array([[r.longitude,r.latitude,r.inclination,r.distance,r.coa_phase,r.polarization] for r in rows])
        assert values.shape==(100,6) and np.isfinite(values).all()
        full.append(values)
        source_records.append(dict(seed=run['seed'],archive_path=str(path.resolve()),
                                   archive_sha256=sha(path),xml_sha256=sha(xml_path)))
    full=np.vstack(full)
    for index,key in enumerate(['ra','dec','inclination','distance']):
        np.testing.assert_allclose(full[:,index],archive[key],atol=0,rtol=0)
    # Train split is by original sampler seed, never by correlated fairdraw row.
    periods=np.array([2*np.pi,2*np.pi,np.pi])
    angle_values=full[:,[0,4,5]]
    angle_centers=np.angle(np.exp(2j*np.pi*angle_values[:300]/periods).mean(0))*periods/(2*np.pi)
    unwrapped=np.angle(np.exp(2j*np.pi*(angle_values-angle_centers)/periods))*periods/(2*np.pi)
    raw=np.column_stack([unwrapped[:,0],np.arctanh(np.sin(full[:,1])),
                         np.arctanh(np.cos(full[:,2])),np.log(full[:,3]),
                         unwrapped[:,1],unwrapped[:,2]])
    ra_center=angle_centers[0]
    center, scale = raw[:300].mean(0), raw[:300].std(0)
    assert np.all(scale>1e-8) and np.isfinite(raw).all()
    data = torch.tensor((raw[:300]-center)/scale, dtype=torch.float32)
    heldout = (raw[300:]-center)/scale
    meter = Meter()
    drift = Velocity(6, meter=meter)
    opt = torch.optim.Adam(drift.parameters(), lr=1e-3)
    meter.phase='flow_matching'
    t0=time.perf_counter(); history=[]
    for step in range(args.steps):
        y=data[torch.randint(len(data),(128,))]
        z=torch.randn_like(y); t=torch.rand(len(y))
        x=(1-t[:,None])*z+t[:,None]*y
        loss=(drift(x,t)-(y-z)).square().mean()
        opt.zero_grad(); loss.backward(); meter.add('neural_backward_rows',len(y)); opt.step()
        if step%200==0: history.append(dict(stage='flow_matching',step=step,loss=float(loss.detach())))
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss>2*1024*1024:
            raise RuntimeError('2 GiB resident-memory development cap exceeded')
    flow_seconds=time.perf_counter()-t0
    base=frozen(drift)
    meter.phase='ode_teacher_generation'; t0=time.perf_counter()
    with torch.no_grad():
        noise=torch.randn(512,6); path=[noise]; times=torch.linspace(0,1,33)
        for i in range(32):
            path.append(rk4(base,path[-1],times[i].expand(512),times[i+1].expand(512),steps=1))
        paths=torch.stack(path,dim=1)
    teacher_seconds=time.perf_counter()-t0
    student=Velocity(6,two_time=True,meter=meter)
    opt=torch.optim.Adam(student.parameters(),lr=1e-3)
    meter.phase='two_time_distillation'; t0=time.perf_counter()
    for step in range(args.distill_steps or args.steps):
        row=torch.randint(512,(128,)); i=torch.randint(32,(128,))
        j=i+1+(torch.rand(128)*(32-i)).long()
        s,t=times[i],times[j]; x=paths[row,i]; target=paths[row,j]
        predicted=student.jump(x,s,t)
        with torch.no_grad(): diagonal_target=base(x,s)
        loss=(predicted-target).square().mean()+.1*(student(x,s,s)-diagonal_target).square().mean()
        opt.zero_grad(); loss.backward(); meter.add('neural_backward_rows',128); opt.step()
        if step%200==0: history.append(dict(stage='two_time_distillation',step=step,loss=float(loss.detach())))
    distill_seconds=time.perf_counter()-t0
    meter.phase='heldout_validation'; t0=time.perf_counter()
    with torch.no_grad():
        # Independent base noises, absent from distilled teacher trajectories.
        z=torch.randn(1024,6); zero=torch.zeros(1024); one=torch.ones(1024)
        teacher=rk4(base,z,zero,one,steps=64)
        one_step=student.jump(z,zero,one)
        refined=rk4(base,z[:128],zero[:128],one[:128],steps=128)
        a=torch.rand(256); b=a+(1-a)*torch.rand(256); c=b+(1-b)*torch.rand(256)
        xs=rk4(base,z[:256],torch.zeros(256),a,steps=32)
        direct=student.jump(xs,a,c)
        composed=student.jump(student.jump(xs,a,b),b,c)
        teacher_segment=rk4(base,xs,a,c,steps=32)
        metrics=dict(one_step_rms=float((one_step-teacher).square().mean().sqrt()),
                     teacher_refinement_rms=float((refined-teacher[:128]).square().mean().sqrt()),
                     semigroup_rms=float((direct-composed).square().mean().sqrt()),
                     heldout_segment_rms=float((direct-teacher_segment).square().mean().sqrt()),
                     diagonal_rms=float((student(xs,a,a)-base(xs,a)).square().mean().sqrt()))
    gates={name:metrics[name]<=limit for name,limit in [('one_step_rms',.15),('teacher_refinement_rms',.01),('semigroup_rms',.1),('diagonal_rms',.2)]}
    samples=one_step.numpy(); teacher_samples=teacher.numpy()
    for label, sample in [('map',samples),('drift',teacher_samples)]:
        metrics[label+'_heldout_wasserstein_standardized']=[float(wasserstein_distance(sample[:,d],heldout[:,d])) for d in range(6)]
        metrics[label+'_mean_error_standardized']=(sample.mean(0)-heldout.mean(0)).tolist()
        metrics[label+'_tail_quantile_error_standardized']=(np.quantile(sample,[.05,.95],axis=0)-np.quantile(heldout,[.05,.95],axis=0)).tolist()
        physical=sample*scale+center
        physical[:,1:3]=np.tanh(physical[:,1:3])
        metrics[label+'_invalid_cosine_fraction']=float(np.mean((np.abs(physical[:,1])>1)|(np.abs(physical[:,2])>1)))
        metrics[label+'_positive_cosi_mass']=float(np.mean(physical[:,2]>0))
    metrics['heldout_positive_cosi_mass']=float(np.mean(raw[300:,2]>0))
    validation_seconds=time.perf_counter()-t0
    checkpoint=output/'pretrained.pt'
    torch.save(dict(drift=drift.state_dict(),flowmap=student.state_dict(),
                    center=center.tolist(),scale=scale.tolist(),ra_center=float(ra_center),angle_centers=angle_centers.tolist(),
                    dim=6,width=64,seed=args.seed),checkpoint)
    result=dict(stage='pretrained_real_drift_and_distilled_map', reward_finetuning_run=False,
                inference_capability_demonstrated=False, seed=args.seed,steps_per_stage=dict(flow_matching=args.steps,distillation=args.distill_steps or args.steps),
                model_gates=gates,model_gates_passed=all(gates.values()),
                features=['unwrapped_ra','atanh_sin_dec','atanh_cos_inclination','log_distance','unwrapped_coa_phase','unwrapped_polarization'],
                data=dict(path=str(Path(args.npz).resolve()),sha256=sha(args.npz),
                          manifest_path=str(Path(args.manifest).resolve()),manifest_sha256=sha(args.manifest),
                          source_archives=source_records,
                          arm='sinc-bandlimited',anchor=47536,train_seeds=[25011401,25011402,25011403],
                          validation_seeds=[25011404],train_rows=300,validation_rows=100,
                          fairdraw_not_raw_importance_samples=True,
                          acquisition_ntotal_lower_bound=sum(r['ntotal'] for r in arm['runs']),
                          acquisition_total_true_calls=None,acquisition_cpu_seconds=None),
                source={f:sha(Path(__file__).parent/f) for f in ['model.py','train_real.py','check_model.py']},
                runtime=dict(torch=torch.__version__,numpy=np.__version__,host=os.uname().nodename,
                             threads=1,device='cpu',cpu_limit_seconds=900,resident_cap_kib=2097152),
                cost=dict(new_true_likelihood_rows=0,neural=meter.snapshot(),
                          flow_matching_seconds=flow_seconds,teacher_generation_seconds=teacher_seconds,
                          distillation_seconds=distill_seconds,validation_seconds=validation_seconds,
                          process_seconds=time.perf_counter()-START,
                          cpu_user_seconds=resource.getrusage(resource.RUSAGE_SELF).ru_utime,
                          cpu_system_seconds=resource.getrusage(resource.RUSAGE_SELF).ru_stime,
                          max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
                metrics=metrics,loss_history=history,
                checkpoint=dict(path=str(checkpoint.resolve()),sha256=sha(checkpoint)),
                next_stage='inspect model gates, then connect exact real reward/parity adapter before any inference comparison')
    (output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(metrics,indent=2))


if __name__=='__main__': main()
