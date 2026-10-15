"""Forward-only forensic replay. No likelihood imports, calls or optimizer.

Original failed endpoints/RNG were NOT saved. A replay is validated by the
first recorded distillation loss, then classified without physical likelihoods.
Raw replayed endpoints remain on Caltech; only aggregates belong in Git.
"""
import os
os.environ.update(CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
import time
START=time.perf_counter()
import resource
resource.setrlimit(resource.RLIMIT_CPU,(60,60))
resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})
import argparse
import hashlib
import json
import math
from pathlib import Path


def main(args):
    import torch
    from model import Velocity,frozen
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    assert sha(args.checkpoint)=='4ce26989ccdcfcdb1702dd5e270d79a772487adb2b348b8b471bcaf3a8cf3158'
    assert sha(args.partial)=='479eac9c0c5e412755ce6a55de4a248eda60c8e3d8c47452e0efb4a4591673c9'
    ck=torch.load(args.checkpoint,map_location='cpu',weights_only=True)
    partial=torch.load(args.partial,map_location='cpu',weights_only=True)
    recorded=json.loads(Path(args.record).read_text())
    torch.manual_seed(261016)
    # Exactly the original constructor order. Frozen copies initialize a fresh
    # network before loading; omitting them would change subsequent RNG draws.
    base=Velocity(6).double();base.load_state_dict(ck['drift']);base=frozen(base)
    student=Velocity(6,two_time=True).double();student.load_state_dict(ck['flowmap'])
    reference=frozen(student)
    center=torch.tensor(ck['center'],dtype=torch.float64)
    scale=torch.tensor(ck['scale'],dtype=torch.float64)
    angles=torch.tensor(ck['angle_centers'],dtype=torch.float64)
    validation_noise=torch.randn(4,6,dtype=torch.float64)
    batches=[]
    with torch.no_grad():
        for step in range(7):
            noise=torch.randn(4,6,dtype=torch.float64)
            t=torch.rand(4,dtype=torch.float64);tau=t+(1-t)*torch.rand(4,dtype=torch.float64)
            xt=reference.jump(noise,torch.zeros_like(t),t)
            middle=reference.jump(xt,t,tau)
            endpoint=reference.jump(middle,tau,torch.ones_like(t))
            raw=endpoint*scale+center
            angular=raw[:,[0,4,5]]
            half=torch.tensor([math.pi,math.pi,math.pi/2],dtype=torch.float64)
            chart_failure=(torch.abs(angular)>=half-1e-6)
            dist=torch.exp(raw[:,3])
            physical_finite=bool(torch.isfinite(raw).all() and torch.isfinite(dist).all())
            distance_failure=(dist<=1)|(dist>=10000)
            dec=torch.asin(torch.tanh(raw[:,1]));incl=torch.acos(torch.tanh(raw[:,2]))
            pole_failure=(torch.abs(dec)>=math.pi/2-1e-8)|(incl<=1e-8)|(incl>=math.pi-1e-8)
            batches.append(dict(step=step,failed_chart_coordinates=chart_failure.sum(0).tolist(),
                chart_failed_rows=int(chart_failure.any(1).sum()),distance_failed_rows=int(distance_failure.sum()),
                pole_failed_rows=int(pole_failure.sum()),physical_finite=physical_finite,
                max_half_period_excess_radians=(torch.abs(angular)-half).max(0).values.tolist()))
            if step<6:
                times=torch.rand((4,3),dtype=noise.dtype).sort(-1).values
                if step==0:
                    s,u,v=times.unbind(-1)
                    xs=reference.jump(noise,torch.zeros_like(s),s)
                    stopped=reference.jump(reference.jump(xs,s,u),u,v)
                    prediction=student.jump(xs,s,v)
                    fingerprint=float((prediction-stopped).square().sum(-1).mean())
    error=abs(fingerprint-recorded['updates'][0]['distillation'])
    assert error<1e-12,'RNG replay fingerprint did not match; do not classify original failure'
    assert all(b['chart_failed_rows']==0 for b in batches[:6])
    assert batches[6]['chart_failed_rows']>0
    # No training replay: the original reference cannot depend on optimizer
    # updates. Compare saved partial parameters and predictions read-only.
    saved=Velocity(6,two_time=True).double();saved.load_state_dict(partial['flowmap'])
    change=float(sum((saved.state_dict()[k]-v).square().sum() for k,v in student.state_dict().items()).sqrt())
    with torch.no_grad():
        diff=float((saved.jump(validation_noise,torch.zeros(4),torch.ones(4))-
                    reference.jump(validation_noise,torch.zeros(4),torch.ones(4))).square().mean().sqrt())
    out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    payload=out/'replayed_endpoint.pt'
    torch.save(dict(endpoint=endpoint,raw=raw,noise=noise,t=t,tau=tau,
                    rng_state=torch.random.get_rng_state(),source='validated forward-only reconstruction, not original capture'),payload)
    r=resource.getrusage(resource.RUSAGE_SELF)
    result=dict(status='forward_replay_fingerprint_passed',original_failed_endpoint_saved=False,
        original_rng_state_saved=False,replay_first_loss_absolute_error=error,
        replay_batches=batches,diagnosis='local angular chart representation failure in frozen reference',
        replay_private_artifact=dict(path=str(payload),sha256=sha(payload)),
        source_checkpoint_sha256=sha(args.checkpoint),partial_checkpoint_sha256=sha(args.partial),
        partial_updates=partial['updates_completed'],partial_parameters_finite=all(bool(torch.isfinite(v).all()) for v in partial['flowmap'].values()),
        partial_parameter_change_l2=change,partial_vs_initial_validation_endpoint_rms=diff,
        new_physical_likelihood_rows=0,optimizer_steps=0,neural_training_run=False,
        host=os.uname().nodename,affinity=sorted(os.sched_getaffinity(0)),torch=torch.__version__,
        source_sha256={p:sha(Path(__file__).parent/p) for p in ['diagnose_chart.py','model.py']},
        cost=dict(process_wall_seconds=time.perf_counter()-START,cpu_user_seconds=r.ru_utime,
                  cpu_system_seconds=r.ru_stime,max_rss_kib=r.ru_maxrss))
    (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ['checkpoint','partial','record','output']:p.add_argument('--'+key,required=True)
    main(p.parse_args())
