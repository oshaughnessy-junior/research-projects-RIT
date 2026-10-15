"""Bounded failure diagnosis only: <=108 calls, charged to cache arm."""
import argparse,json,time,resource,hashlib
from pathlib import Path
import numpy as np
from run_five_arm import physical,load_context
start=time.perf_counter();cpu=time.process_time()
p=argparse.ArgumentParser()
for key in ['output','checkpoint','training','inventory']:p.add_argument('--'+key,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False)
r=dict(status='setup',attempts=0,prior_cache_attempts=1708,prior_global_attempts=9453,
       diagnostic_cap=108,phase='failure_diagnosis_only_no_evidence')
def save():
    r['cpu_seconds']=time.process_time()-cpu;r['wall_seconds']=time.perf_counter()-start
    (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
ctx=load_context(a,r,save)
x=np.load(out.parent/'run3/cache_failed_batch_reconstructed.npz')['x'];q=physical(x)
r['attempts']+=len(q);save()
ll=np.asarray(ctx['bank']['like'].log_likelihood(*[q[:,i] for i in range(6)]))
np.savez_compressed(out/'diagnostic_jax.npz',x=x,ll=ll)
r.update(nan_rows=int(np.isnan(ll).sum()),positive_infinity_rows=int(np.isposinf(ll).sum()),
         negative_infinity_rows=int(np.isneginf(ll).sum()),finite_rows=int(np.isfinite(ll).sum()))
r['reference_checks']=[]
for index in np.flatnonzero(~np.isfinite(ll))[:8]:
    r['attempts']+=1;save();value=ctx['bank']['reference'](q[index])
    r['reference_checks'].append(dict(index=int(index),jax_kind='nan' if np.isnan(ll[index]) else ('negative_infinity' if ll[index]<0 else 'positive_infinity'),
        reference=float(value) if np.isfinite(value) else None,reference_finite=bool(np.isfinite(value))))
r['status']='diagnostic_complete';save();print(json.dumps(r,indent=2))
