"""Zero-physical-call replay of saved time primitives, with each gate exposed."""
import os,json,resource,time,argparse
os.sched_setaffinity(0,{0});resource.setrlimit(resource.RLIMIT_CPU,(120,125))
import numpy as np
import jax
jax.config.update('jax_enable_x64',True)
import jax.numpy as jnp
from RIFT.likelihood.jax_ile import core
from RIFT.likelihood import time_marginalization_quadrature as tq
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--directory',required=True);a=p.parse_args();root=Path(a.directory)
result=dict(true_likelihood_attempts=0,points=[])
for i in range(3):
 d=np.load(root/f'primitive_{i}.npz');k=jnp.asarray(d['kappa'][0]);rho=np.asarray(d['rho'][0]);g=int(d['guard']);dt=float(d['deltaT']);n=len(k)-2*g
 def taper(v,guard):
  ramp=.5*(1-jnp.cos(jnp.pi*jnp.arange(guard+1)/guard))
  return v*jnp.concatenate((ramp[:-1],jnp.ones(n),ramp[:-1][::-1]))
 def at(f,sg):
  trim=g-sg;v=k[trim:-trim] if trim else k
  dense=core._reflected_fft_upsample(taper(v,sg),f)[sg*f:sg*f+(n-1)*f+1].real-.5*rho[0]
  value=float(core._log_trapezoid(dense,dt/f));width,measured=core._peak_width_from_lnL_jax(dense,dt/f)
  return dict(value=value,width=float(width),measurable=bool(measured),resolved=bool((~measured)|(~jnp.isfinite(width))|(dt/f<=width/2)))
 probe=core._reflected_fft_upsample(taper(k,g),2)[2*g:2*g+(n-1)*2+1].real-.5*rho[0]
 sigma,meas=core._peak_width_from_lnL_jax(probe,dt/2)
 need=max(1,2*dt/float(sigma)) if bool(meas) and np.isfinite(float(sigma)) else 1
 base=2**int(np.ceil(np.log2(need)))
 item=dict(index=i,initial_base=base,finite_primitive=bool(np.isfinite(k).all() and np.isfinite(rho).all()),factors=[])
 for f in sorted(set([1,2,4,8,16,32,base,2*base])):
  full=at(f,g);half=at(f,g//2);item['factors'].append(dict(factor=f,**full,half_guard_value=half['value'],guard_difference=abs(full['value']-half['value'])))
 coarse=np.asarray(k[g:g+n])[None];rr=rho[g:g+n][None]
 value=tq.time_marginalize_bandlimited(coarse,rr,dt,lambda v,r:v-.5*r,xpy=np)
 item['numpy_same_coarse_primitive_value']=float(value[0]);item['numpy_rule_report']=tq.last_report()
 result['points'].append(item)
def safe(v):
 if isinstance(v,dict):return {k:safe(x) for k,x in v.items()}
 if isinstance(v,list):return [safe(x) for x in v]
 if isinstance(v,float) and not np.isfinite(v):return str(v)
 return v
result=safe(result)
(root/'certificate-audit.json').write_text(json.dumps(result,indent=2,allow_nan=False))
print(json.dumps(result,indent=2))
