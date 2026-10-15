"""Bounded reuse validation: Q8/cubic candidate, Q16 convergence reference."""
import argparse,json,time,resource,hashlib,traceback
from pathlib import Path
import numpy as np
import jax,jax.numpy as jnp
from run_five_arm import load_context
from common_contract_adapter import evaluate,reduce_field
from roboto_pregrid_adapter import RobotoPregrid
resource.setrlimit(resource.RLIMIT_CPU,(380,385))
p=argparse.ArgumentParser()
for name in ['output','checkpoint','training','inventory']:p.add_argument('--'+name,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False);root=out.parents[1];cpu=time.process_time();start=time.perf_counter()
journal=json.loads((out.parent/'source/journal.json').read_text());r=dict(status='setup',journal=journal,work_equivalents=0,assembly_dispatches=0,jvp_lanes=0,dispatches=[],cases=[])
def save():
 r['cpu_seconds']=time.process_time()-cpu;r['wall_seconds']=time.perf_counter()-start;r['peak_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
def dispatch(label,we,nodes,lanes,fn):
 if r['work_equivalents']+we>64:raise RuntimeError('64WE cap')
 if time.process_time()-cpu>365:raise RuntimeError('CPU reserve')
 r['work_equivalents']+=we;r['assembly_dispatches']+=1;r['jvp_lanes']+=lanes
 r['dispatches'].append(dict(label=label,work_equivalents=we,time_nodes=nodes,jvp_lanes=lanes,directional_time_node_equivalents=nodes*(1+lanes),attempted=True));save()
 value=fn();r['dispatches'][-1]['completed']=True;save();return value
class Mismatch(Exception):pass
try:
 ctx=load_context(a,r,save);adapters={f:RobotoPregrid(ctx,f) for f in [8,16]}
 r['q_pregrid_build_reports']={str(f):x.reports for f,x in adapters.items()};save()
 n=adapters[8].data.npts;dt=adapters[8].data.deltaT;g=1024;times=np.linspace(float(adapters[8].data.tvals[0]),float(adapters[8].data.tvals[-1]),(n-1)*128+1)
 with_jac={}
 for f,adapter in adapters.items():
  def aux(q,adapter=adapter):
   v=adapter.primitive(q);return v,v
  with_jac[f]=jax.jit(jax.jacfwd(aux,has_aux=True))
 def reference(v,jac):
  v=np.asarray(v);jac=np.asarray(jac)
  return evaluate(v[0]+1j*v[1],v[2],jac[0]+1j*jac[1],jac[2,0],n,dt,g)
 def integrate_independent(field):
  vals=[]
  for f in [32,64,128]:
   for method in ['simpson_scipy','trapezoid_numpy_fft']:
    row=field[::128//f];value,_=reduce_field(row,np.zeros((len(row),6)),dt/f,method)
    vals.append(dict(factor=f,method=method,value=value,nodes=len(row)))
  value=next(x['value'] for x in vals if x['factor']==128 and x['method']=='simpson_scipy')
  return dict(value=value,convergence_error=max(abs(x['value']-value) for x in vals),grids=vals)
 points=[np.load(root/'stage5/extract/primitive_0.npz')['physical'],ctx['theta'][0],ctx['theta'][3]]
 for index,q in enumerate(points):
  case=dict(index=index,physical=q.tolist(),origin=['reconstructed_failure','earlier_parity0','earlier_parity3'][index],status='running',q_factors={});r['cases'].append(case);save()
  for factor in [8,16]:
   jac,v=dispatch(f'case{index}_q{factor}_primal_six_jvps',7,n+2048,6,lambda factor=factor:with_jac[factor](jnp.asarray(q)))
   jac=np.asarray(jac);v=np.asarray(v);np.savez_compressed(out/f'case{index}_q{factor}_primitive_jac.npz',physical=q,primitive=v,jacobian=jac)
   ref=reference(v,jac);result=dict(reference=ref);case['q_factors'][str(factor)]=result;save()
   if not ref['passed']:raise Mismatch(f'q{factor} frozen-grid value/gradient reference failed')
   field=dispatch(f'case{index}_q{factor}_independent_dense',1,len(times),0,lambda factor=factor:adapters[factor].independent_field(q,times))
   np.savez_compressed(out/f'case{index}_q{factor}_independent.npz',physical=q,times=times,field=field)
   direct=integrate_independent(field);result['independent']=direct;result['physical_reconstruction_difference']=direct['value']-ref['raw_log_integral'];result['coarse_field_max_error']=float(abs(field[::128]-(v[0,g:-g]-.5*v[2,g:-g])).max());save()
   if direct['convergence_error']>1e-4:raise Mismatch(f'q{factor} independent quadrature not converged')
   if abs(result['physical_reconstruction_difference'])>.001:raise Mismatch(f'q{factor} physical reconstruction mismatch')
  c=case['q_factors']['8']['reference'];fine=case['q_factors']['16']['reference'];tol=.01+.001*np.abs(fine['gradient'])
  case['q8_vs_q16']=dict(value_difference=c['raw_log_integral']-fine['raw_log_integral'],gradient_error_over_tolerance=float(np.max(np.abs(np.array(c['gradient'])-fine['gradient'])/tol)));save()
  if abs(case['q8_vs_q16']['value_difference'])>.001 or case['q8_vs_q16']['gradient_error_over_tolerance']>1:raise Mismatch('q8 versus q16 value/derivative convergence failed')
  direction=np.array([1,-1,1,-1,1,100])*1e-5;fd=[];case['mixed_direction_stencils']=[]
  for scale in [1.,.5]:
   pair=[]
   for sign in [1,-1]:
    point=q+sign*scale*direction
    field=dispatch(f'case{index}_q8_fd_{scale}_{sign}',1,len(times),0,lambda point=point:adapters[8].independent_field(point,times))
    np.savez_compressed(out/f'case{index}_q8_fd_{scale}_{sign}.npz',physical=point,times=times,field=field)
    val=integrate_independent(field);case['mixed_direction_stencils'].append(dict(scale=scale,sign=sign,result=val));save()
    if val['convergence_error']>1e-4:raise Mismatch('FD independent quadrature failed')
    pair.append(val['value'])
   fd.append((pair[0]-pair[1])/(2*scale))
  grad=np.asarray(c['gradient']);ad=float(grad@direction);limit=float(np.abs(direction)@(.01+.001*np.abs(grad)))
  case['mixed_direction']=dict(ad=ad,fd=fd,tolerance=limit,self_convergence_tolerance=.1*limit)
  if max(abs(ad-x) for x in fd)>limit or abs(fd[0]-fd[1])>.1*limit:raise Mismatch('cross-implementation mixed derivative mismatch')
  case['status']='passed';save()
 r['status']='local_three_point_roboto_recipe_gate_passed'
except Mismatch as e:
 r['status']='unresolved_stopped';r['reason']=str(e)
 if r['cases']:r['cases'][-1]['status']='unresolved'
except BaseException as e:
 r['status']='execution_failed_stopped';r['reason']=repr(e);r['traceback']=traceback.format_exc()
finally:
 r['cases_not_run']=3-len(r['cases']);save();print(json.dumps({k:v for k,v in r.items() if k not in ['journal','cases','modes','q_pregrid_build_reports']},indent=2))
