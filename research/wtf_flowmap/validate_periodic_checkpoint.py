"""Read-only follow-up to a failed solver gate. No optimizer or likelihood.

Independent fixed generator; this is not the training run's absent noise capture.
The follow-up saves its complete solver-validation payload privately on Caltech.
"""
import os
os.environ.update(CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
import resource,time
START=time.perf_counter()
resource.setrlimit(resource.RLIMIT_CPU,(20,20));resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})
import argparse,hashlib,json,math
from pathlib import Path


def main(args):
    import torch
    from model import rk4
    from periodic_model import PeriodicVelocity,base_noise,displacement
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    ck=torch.load(args.checkpoint,map_location='cpu',weights_only=True)
    assert ck['geometry']=='T3_R3_v1'
    drift=PeriodicVelocity().double();drift.load_state_dict(ck['drift']);drift.requires_grad_(False)
    model=PeriodicVelocity(two_time=True).double();model.load_state_dict(ck['flowmap']);model.requires_grad_(False)
    generator=torch.Generator().manual_seed(261020)
    z=base_noise(1024,generator=generator);s=torch.zeros(1024,dtype=z.dtype);t=s+1
    with torch.no_grad():
        teacher=rk4(drift,z,s,t,steps=64);mapped=model.jump(z,s,t)
        residual=displacement(mapped,teacher)
    shifts=torch.tensor([-2*math.pi,4*math.pi,-6*math.pi,0,0,0],dtype=z.dtype)
    x=z[:4];ss=torch.ones(4,dtype=z.dtype)*.2;tt=ss+.5
    errors={}
    for label,fn in [('drift',lambda q:drift(q,ss)),('map_velocity',lambda q:model(q,ss,tt)),
                      ('map_jump',lambda q:model.jump(q,ss,tt))]:
        expected=fn(x)+(shifts if label=='map_jump' else 0)
        errors[label]=float((fn(x+shifts)-expected).abs().max());assert errors[label]<=1e-10
    point=x[0]
    fun=lambda st:model.jump(point[None],st[0:1],st[1:2])[0]
    diagonal_time=torch.tensor([.4,.4],dtype=z.dtype)
    jac=torch.autograd.functional.jacobian(fun,diagonal_time)
    v=model(point[None],diagonal_time[:1],diagonal_time[1:])[0]
    torch.testing.assert_close(jac[:,1],v,atol=1e-12,rtol=1e-12)
    torch.testing.assert_close(jac[:,0],-v,atol=1e-12,rtol=1e-12)
    assert torch.isfinite(jac).all()
    out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    payload=out/'solver_validation.pt'
    torch.save(dict(base_noise=z,teacher=teacher,mapped=mapped,residual=residual,seed=261020,
                    checkpoint_sha256=sha(args.checkpoint)),payload)
    r=resource.getrusage(resource.RUSAGE_SELF)
    result=dict(status='read_only_validation_complete_not_training_acceptance',seed=261020,
        checkpoint_sha256=sha(args.checkpoint),source_sha256={f:sha(Path(__file__).parent/f)
            for f in ['validate_periodic_checkpoint.py','periodic_model.py','model.py']},
        one_step_rms=float(residual.square().mean().sqrt()),
        coordinate_rms=residual.square().mean(0).sqrt().tolist(),
        coordinate_order=['ra_radians','phase_radians','twice_psi_radians','standardized_atanh_sin_dec',
                          'standardized_atanh_cos_incl','standardized_log_distance'],
        mixed_signed_shift_errors=errors,diagonal_time_derivative_contract='passed',
        payload=dict(path=str(payload),sha256=sha(payload)),new_physical_rows=0,optimizer_steps=0,
        cost=dict(process_wall_seconds=time.perf_counter()-START,cpu_user_seconds=r.ru_utime,
            cpu_system_seconds=r.ru_stime,max_rss_kib=r.ru_maxrss))
    (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--checkpoint',required=True);p.add_argument('--output',required=True)
    main(p.parse_args())
