"""Research-only common-contract validation with journaled conservative WE."""
import argparse,json,time,resource,hashlib,traceback,types
from pathlib import Path
import numpy as np
import jax,jax.numpy as jnp
from run_five_arm import load_context
from common_contract_adapter import evaluate,reduce_field
resource.setrlimit(resource.RLIMIT_CPU,(170,175))
p=argparse.ArgumentParser()
for name in ['output','checkpoint','training','inventory']:p.add_argument('--'+name,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False);cpu=time.process_time();start=time.perf_counter()
root=out.parents[1];journal=json.loads((out.parent/'source/journal.json').read_text())
assert json.loads((out.parent/'array/summary.json').read_text())['physical_phase_eligible']
r=dict(status='setup',work_equivalents=0,assembly_dispatches=0,jvp_lanes=0,dispatches=[],cases=[],journal=journal)
def save():
 r['cpu_seconds']=time.process_time()-cpu;r['wall_seconds']=time.perf_counter()-start;r['peak_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
def dispatch(label,we,nodes,lanes,fn):
 if r['work_equivalents']+we>40:raise RuntimeError('work-equivalent cap')
 if time.process_time()-cpu>160:raise RuntimeError('CPU reserve exhausted')
 r['work_equivalents']+=we;r['assembly_dispatches']+=1;r['jvp_lanes']+=lanes
 r['dispatches'].append(dict(label=label,attempted=True,work_equivalents=we,time_nodes=nodes,jvp_lanes=lanes,directional_time_node_equivalents=nodes*(1+lanes)));save()
 value=fn();r['dispatches'][-1]['completed']=True;save();return value
class Mismatch(Exception):pass
try:
 ctx=load_context(a,r,save);data=ctx['bank']['like'].data
 from RIFT.likelihood.jax_ile import core
 from RIFT.likelihood import factored_likelihood as fl
 import lal
 closure=dict(zip(ctx['bank']['reference'].__code__.co_freevars,[c.cell_contents for c in ctx['bank']['reference'].__closure__]))
 assert set(closure)=={'banks','p','packed'},'unreviewed reference closure contract'
 banks=closure['banks'];template=closure['p'];g=1024;n=data.npts;dt=float(data.deltaT);nodes=(n-1)*128+1
 t=np.linspace(float(data.tvals[0]),float(data.tvals[-1]),nodes);assert nodes==78465
 def primitive(q):
  vals=[q[i:i+1] for i in range(6)]
  k,rho=core._accumulate_unit(data,*vals[:5],interp='sinc',phase_marginalization=False,guard=g)
  inv=data.distMpcRef/q[5]
  return jnp.stack((k[0].real*inv,k[0].imag*inv,rho[0]*inv**2))
 def aux(q):
  v=primitive(q);return v,v
 with_jac=jax.jit(jax.jacfwd(aux,has_aux=True));primal=jax.jit(primitive)
 def unpack(v,jac=None):
  v=np.asarray(v);k=v[0]+1j*v[1];rho=v[2]
  dk=np.zeros((len(k),6),complex) if jac is None else np.asarray(jac[0])+1j*np.asarray(jac[1])
  dr=np.zeros(6) if jac is None else np.asarray(jac[2,0])
  return evaluate(k,rho,dk,dr,n,dt,g)
 failure=np.load(root/'stage5/extract/primitive_0.npz')['physical'];points=[failure,ctx['theta'][0],ctx['theta'][3]]
 for index,q in enumerate(points):
  case=dict(index=index,origin=['reconstructed_failure','earlier_parity_0','earlier_parity_3'][index],physical=q.tolist(),status='running');r['cases'].append(case);save()
  jac,v=dispatch(f'case{index}_primitive_and_six_jvps',7,n+2*g,6,lambda:with_jac(jnp.asarray(q)))
  jac=np.asarray(jac);v=np.asarray(v);np.savez_compressed(out/f'case{index}_primitive_jac.npz',physical=q,primitive=v,jacobian=jac)
  ref=unpack(v,jac);case['reference']=ref
  if not ref['passed']:raise Mismatch('shared frozen-grid value/gradient reference did not converge')
  def independent():
   pv=types.SimpleNamespace(phi=q[0:1],theta=q[1:2],psi=q[2:3],incl=q[3:4],phiref=q[4:5],dist=q[5:6]*1e6*lal.PC_SI,tref=template.tref,deltaT=template.deltaT,radec=True)
   return np.asarray(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(t,pv,*banks,Lmax=4,xpy=np,time_interp='sinc',return_lnLt=True,explicit_time_values=True,n_cal=1))[0]
  field=dispatch(f'case{index}_numpy_independent_dense_gather',1,nodes,0,independent)
  np.savez_compressed(out/f'case{index}_independent_dense.npz',physical=q,tvals=t,log_time_field=field)
  results=[]
  for factor in [32,64,128]:
   for method in ['simpson_scipy','trapezoid_numpy_fft']:
    row=field[::128//factor];value,_=reduce_field(row,np.zeros((len(row),6)),dt/factor,method)
    results.append(dict(factor=factor,method=method,value=value,nodes=len(row)))
  case['independent_dense_integrals']=results
  base=next(x['value'] for x in results if x['factor']==128 and x['method']=='simpson_scipy')
  error=max(abs(x['value']-base) for x in results)
  case['independent_dense_convergence_error']=error;case['physical_reconstruction_difference']=base-ref['raw_log_integral'];save()
  if error>1e-4:raise Mismatch('independent physical dense field not converged on frozen grids')
  if abs(case['physical_reconstruction_difference'])>.001:raise Mismatch('physical dense-gather versus reconstructed-primitive value mismatch')
  d=np.array([1,-1,1,-1,1,100])*1e-5;fd=[];case['mixed_direction_stencils']=[]
  for scale in [1.,.5]:
   vals=[]
   for sign in [1,-1]:
    point=q+sign*scale*d
    vfd=dispatch(f'case{index}_fd_scale{scale}_sign{sign}',1,n+2*g,0,lambda point=point:primal(jnp.asarray(point)))
    np.savez_compressed(out/f'case{index}_fd_{scale}_{sign}.npz',physical=point,primitive=np.asarray(vfd))
    ff=unpack(vfd)
    case['mixed_direction_stencils'].append(dict(scale=scale,sign=sign,reference=ff))
    if not ff['passed']:raise Mismatch('FD stencil reference failed frozen-grid convergence')
    vals.append(ff['raw_log_integral'])
   fd.append((vals[0]-vals[1])/(2*scale))
  grad=np.asarray(ref['gradient']);ad=float(grad@d);tol=float(np.abs(d)@(.01+.001*np.abs(grad)))
  case['directional_derivative']=dict(ad=ad,fd=fd,tolerance=tol,self_convergence_tolerance=.1*tol)
  if max(abs(ad-x) for x in fd)>tol or abs(fd[0]-fd[1])>.1*tol:raise Mismatch('mixed-direction derivative convergence mismatch')
  case['status']='passed';save()
 r['status']='contract_passed_at_three_points_only'
except Mismatch as e:
 r['status']='contract_unresolved_stopped';r['reason']=str(e)
 if r['cases']:r['cases'][-1]['status']='unresolved'
except BaseException as e:
 r['status']='execution_failed_stopped';r['reason']=repr(e);r['traceback']=traceback.format_exc()
finally:
 r['cases_not_run']=3-len(r['cases']);save();print(json.dumps({k:v for k,v in r.items() if k not in ['cases','journal','modes']},indent=2))
