"""Planning-stage mathematical replay only; no RIFT/physical callbacks imported."""
import os,resource,json,time,hashlib
from pathlib import Path
os.sched_setaffinity(0,{0});resource.setrlimit(resource.RLIMIT_CPU,(20,25))
import numpy as np
from scipy.signal import resample
from scipy.integrate import simpson
root=Path('/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage5/extract')
out=root.parents[1]/'stage6';out.mkdir(exist_ok=False)
start=time.perf_counter();cpu=time.process_time()
r=dict(physical_calls=0,waveform_calls=0,runtime='pure NumPy/SciPy on saved primitives; one CPU',points=[])
def calc(k,rho,guard,n,dt,f,D0,D):
 scale=D0/D;k=k*scale;rho=rho*scale**2
 ramp=.5*(1-np.cos(np.pi*np.arange(guard+1)/guard))
 tapered=k*np.r_[ramp[:-1],np.ones(n),ramp[:-1][::-1]]
 reflected=np.r_[tapered,tapered[1:-1][::-1]]
 dense=resample(reflected,len(reflected)*f)
 kr=dense[guard*f:guard*f+(n-1)*f+1].real
 field=kr-.5*rho[0];m=field.max();w=np.exp(field-m)
 integral=simpson(w,dx=dt/f)
 grad=simpson(w*(-kr+rho[0])/D,dx=dt/f)/integral
 return float(m+np.log(integral)),float(grad)
for i in range(3):
 path=root/f'primitive_{i}.npz';d=np.load(path);g=int(d['guard']);n=d['kappa'].shape[1]-2*g;dt=float(d['deltaT']);D=float(d['physical'][5]);T=(n-1)*dt
 item=dict(index=i,source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),closed_interval_duration=T,log_time_prior_offset=-float(np.log(T)),grids=[])
 for f in [4,8,16,32,64]:
  pair=[]
  for sg in [g//2,g]:
   trim=g-sg;k=d['kappa'][0,trim:-trim] if trim else d['kappa'][0];rho=d['rho'][0,trim:-trim] if trim else d['rho'][0]
   v,grad=calc(k,rho,sg,n,dt,f,D,D)
   pair.append(dict(guard=sg,log_integral=v,distance_gradient=grad))
  item['grids'].append(dict(factor=f,values=pair,value_guard_difference=abs(pair[0]['log_integral']-pair[1]['log_integral']),gradient_guard_difference=abs(pair[0]['distance_gradient']-pair[1]['distance_gradient'])))
 k=d['kappa'][0];rho=d['rho'][0]
 item['distance_finite_differences']=[]
 for h in [.001,.0005]:
  vp,_=calc(k,rho,g,n,dt,64,D,D+h);vm,_=calc(k,rho,g,n,dt,64,D,D-h)
  item['distance_finite_differences'].append(dict(step_mpc=h,gradient=(vp-vm)/(2*h)))
 last=item['grids'][-1]['values'][-1];item['normalized_time_log_integral']=last['log_integral']-np.log(T)
 item['gradient_time_prior_difference']=0.0
 r['points'].append(item)
r['cpu_seconds']=time.process_time()-cpu;r['wall_seconds']=time.perf_counter()-start
r['peak_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
(out/'saved-check.json').write_text(json.dumps(r,indent=2));print(json.dumps(r,indent=2))
