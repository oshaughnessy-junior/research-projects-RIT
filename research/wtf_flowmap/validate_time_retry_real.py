"""Stage5 isolated patched AD checks; <=29 physical attempts, no inference."""
import json,resource,time,importlib.util,argparse
from pathlib import Path
import numpy as np
import jax,jax.numpy as jnp
from run_five_arm import load_context
resource.setrlimit(resource.RLIMIT_CPU,(180,185))
p=argparse.ArgumentParser()
for k in ['output','checkpoint','training','inventory','patched_core']:p.add_argument('--'+k.replace('_','-'),dest=k,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False);cpu=time.process_time();start=time.perf_counter()
r=dict(status='setup',attempts=0,prior_stage5_attempts=9,cap=64,checks=[])
def save():
 r['cpu_seconds']=time.process_time()-cpu;r['wall_seconds']=time.perf_counter()-start
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
def probe(label,fn):
 if r['attempts']+9>=64:raise RuntimeError('stage5 physical row cap')
 r['attempts']+=1;r.setdefault('probes',[]).append(label);save();return fn()
ctx=load_context(a,r,save);data=ctx['bank']['like'].data
spec=importlib.util.spec_from_file_location('RIFT.likelihood.jax_ile.core_retry_real',a.patched_core)
fixed=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixed)
def f(q):
 return fixed.fused_log_likelihood(data,*[q[i:i+1] for i in range(6)],interp='sinc',phase_marginalization=False,time_quadrature='bandlimited')[0]
vg=jax.jit(jax.value_and_grad(f))
for i,q in enumerate([ctx['theta'][0],ctx['theta'][3]]):
 old,og=probe('old_control_gradient_'+str(i),lambda:ctx['bank']['like'].value_and_grad(q))
 nv,ng=probe('new_control_gradient_'+str(i),lambda:vg(jnp.asarray(q)))
 nv=float(nv);ng=np.asarray(ng)
 np.testing.assert_allclose(nv,old,atol=1e-10,rtol=0);np.testing.assert_allclose(ng,og,atol=1e-8,rtol=1e-10)
 r['checks'].append(dict(label='earlier_parity_control_'+str(i),value_delta=nv-old,max_abs_gradient_delta=float(abs(ng-og).max())));save()
root=out.parents[1];d=np.load(root/'stage5/extract/primitive_0.npz');q=d['physical']
v,g=probe('new_failure_value_gradient',lambda:vg(jnp.asarray(q)));v=float(v);g=np.asarray(g)
assert np.isfinite(v) and np.isfinite(g).all()
newfd=[];referencefd=[]
for i in range(6):
 h=1e-5 if i<5 else 1e-3;e=np.eye(6)[i]*h
 vp=probe('new_fd_plus_'+str(i),lambda:vg(jnp.asarray(q+e))[0]);vm=probe('new_fd_minus_'+str(i),lambda:vg(jnp.asarray(q-e))[0]);newfd.append(float((vp-vm)/(2*h)))
 rp=probe('noloop_fd_plus_'+str(i),lambda:ctx['bank']['reference'](q+e));rm=probe('noloop_fd_minus_'+str(i),lambda:ctx['bank']['reference'](q-e));referencefd.append(float((rp-rm)/(2*h)))
newfd=np.asarray(newfd);referencefd=np.asarray(referencefd)
np.testing.assert_allclose(g,newfd,atol=1e-5,rtol=1e-5)
r['failure_point']=dict(patched_value=v,patched_gradient=g.tolist(),patched_central_fd=newfd.tolist(),noloop_central_fd=referencefd.tolist(),
 maximum_patched_ad_fd_error=float(abs(g-newfd).max()),noloop_gradient_discrepancy=(g-referencefd).tolist(),
 reference_value=47.83659196403751,value_difference_from_reference=v-47.83659196403751)
r['status']='isolated_retry_gradient_checks_passed_common_target_still_mismatched';save();print(json.dumps(r,indent=2))
