"""Stage5 bounded physical extraction; save primitives for zero-call diagnosis."""
import argparse,json,time,resource,hashlib
from pathlib import Path
import numpy as np
import jax,jax.numpy as jnp
from run_five_arm import physical,load_context
resource.setrlimit(resource.RLIMIT_CPU,(570,575))
p=argparse.ArgumentParser()
for name in ['output','checkpoint','training','inventory']:p.add_argument('--'+name,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False)
start=time.perf_counter();cpu=time.process_time();r=dict(status='setup',attempts=0,cap=64,physical_rows=0,probes=[])
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save():
 r['cpu_seconds']=time.process_time()-cpu;r['wall_seconds']=time.perf_counter()-start
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
def probe(label,fn):
 if r['attempts']>=64:raise RuntimeError('64-row cap')
 r['attempts']+=1;r['physical_rows']+=1;r['probes'].append(label);save();return fn()
ctx=load_context(a,r,save)
from RIFT.likelihood.jax_ile import core
from RIFT.likelihood import time_marginalization_quadrature as tq
root=out.parents[1]
source=root/'stage4/run3/cache_failed_batch_reconstructed.npz';original=root/'stage4/diagnose/diagnostic_jax.npz'
x=np.load(source)['x'];old=np.load(original);np.testing.assert_array_equal(x,old['x'])
bad=np.flatnonzero(~np.isfinite(old['ll']));assert bad.tolist()==[12]
q=physical(x)[12];r['coordinate_order']=['ra','dec','psi','incl','phiref','distMpc'];r['physical_coordinate']=q.tolist()
r['origin']=dict(kind='reconstructed, not original failing-batch capture',reconstructed_batch_sha256=sha(source),
 diagnostic_array_sha256=sha(original),batch_index=12,prefix_replay_batches=15,
 replay_summary_sha256=sha(root/'stage4/run3/cache-replay.json'))
r['additional_sources']={str(Path(tq.__file__).name):sha(tq.__file__),str(Path(core.__file__).name):sha(core.__file__)}
data=ctx['bank']['like'].data;guard=core.bandlimited_time_guard(data.npts)[1]
r['precision']=dict(jax_enable_x64=bool(jax.config.x64_enabled),guard=guard,npts=data.npts,deltaT=float(data.deltaT))
r['points']=[]
for i,point in enumerate([q,ctx['theta'][0],ctx['theta'][3]]):
 vals=[np.array([v],dtype=np.float64) for v in point]
 value=probe(f'jax_full_{i}',lambda:np.asarray(ctx['bank']['like'].log_likelihood(*vals))[0])
 reference=probe(f'numpy_noloop_{i}',lambda:ctx['bank']['reference'](point))
 k,rho=probe(f'accumulate_primitive_{i}',lambda:core._accumulate_unit(data,*vals[:5],interp='sinc',phase_marginalization=False,guard=guard))
 inv=float(data.distMpcRef)/point[5];k=np.asarray(k)*inv;rho=np.asarray(rho)*inv**2
 np.savez_compressed(out/f'primitive_{i}.npz',kappa=k,rho=rho,physical=point,deltaT=float(data.deltaT),guard=guard)
 r['points'].append(dict(index=i,label=['failure','earlier_parity_0','earlier_parity_3'][i],
   original_value=float(value) if np.isfinite(value) else None,original_kind='finite' if np.isfinite(value) else 'nonfinite',reference=float(reference),
   primitive_finite=bool(np.isfinite(k).all() and np.isfinite(rho).all()),primitive_sha256=sha(out/f'primitive_{i}.npz')))
 save()
r['status']='extraction_complete';save();print(json.dumps(r,indent=2))
