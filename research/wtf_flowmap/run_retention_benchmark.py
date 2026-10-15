"""One predeclared information-retention comparison; never an evidence estimator."""
import os,time,resource,argparse,json,hashlib,traceback,importlib.util
from pathlib import Path
os.environ.update(JAX_ENABLE_X64='1',JAX_PLATFORMS='cpu',CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',RIFT_TIME_FFT_WORKERS='1')
START=time.perf_counter();CPU=time.process_time()
from run_five_arm import load_context,factory,cube,physical
resource.setrlimit(resource.RLIMIT_CPU,(260,265))
import numpy as np
import torch
import jax,jax.numpy as jnp
from scipy.special import logsumexp
from periodic_model import PeriodicVelocity,base_noise,decode,support_mask
from roboto_pregrid_adapter import RobotoPregrid
from five_arm_sampling import validate_cloud,av_mass_check,gmm_mass_check,mixture_draw,PARAMS,GROUPS
from RIFT.integrators.unreliable_oracle.resampling import ResamplingOracle
from igwn_ligolw import utils,ligolw,lsctables
p=argparse.ArgumentParser()
for k in ['output','checkpoint','candidate','training','inventory','patched-core','journal']:p.add_argument('--'+k,required=True)
a=p.parse_args();out=Path(a.output);out.mkdir(exist_ok=False);sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
journal=json.loads(Path(a.journal).read_text())
r=dict(status='setup',journal=journal,physical_calls=0,attempted_rows=0,logical_cached_score_rows=0,dispatches=[],seeds=[],training_updates=0,scalar_vjp_seeds=0,evidence_rows=0,posterior_rows=0,inference_estimates=False)
def save():
 r.update(cpu_seconds=time.process_time()-CPU,wall_seconds=time.perf_counter()-START,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
 (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
def check_time():
 if time.process_time()-CPU>245:raise RuntimeError('CPU reserve reached; no retry or expansion')
class StructuralFailure(Exception):pass
try:
 assert jax.config.jax_enable_x64
 assert sha(a.candidate)==journal['candidate_sha256']
 assert sha(a.patched_core)=='70fb605f65a3a6cda71e1819b79bcd232f6b6719b0842076cae373bb5e4f1097'
 ctx=load_context(a,r,save);adapter=RobotoPregrid(ctx,8);data=adapter.data
 spec=importlib.util.spec_from_file_location('RIFT.likelihood.jax_ile.core_retention',a.patched_core);fixed=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixed)
 r['target_contract']=dict(q_factor=8,arrival='cubic',terminal='bandlimited canonical adaptive with isolated reviewed extra certificate doubling',dtype='float64',window=[float(data.tvals[0]),float(data.tvals[-1])],time_nodes=data.npts,physical_order=['RA','dec','psi','incl','phase','D_Mpc'],reward='raw physical log time integral, no parameter prior/Jacobian',core_sha256=sha(a.patched_core),q_pregrid_reports=adapter.reports)
 def fun(q):return fixed.fused_log_likelihood(data,*[q[:,i] for i in range(6)],interp='cubic',phase_marginalization=False,time_quadrature='bandlimited')
 vf=jax.jit(fun)
 def score(x,phase):
  x=np.asarray(x,dtype=np.float64);check_time()
  if x.ndim!=2 or x.shape[1]!=6 or len(x)%25:raise ValueError('predeclared25-row batch alignment')
  if not np.isfinite(x).all() or np.any(x<0) or np.any(x>1):raise StructuralFailure('source coordinates outside cube')
  q=physical(x)
  if not support_mask(torch.tensor(q,dtype=torch.float64)).all():raise StructuralFailure('physical support failure; no clipping/redraw')
  values=[]
  for start in range(0,len(x),25):
   check_time()
   if r['attempted_rows']+25>1200:raise RuntimeError('1200 attempted physical rows cap')
   ordinal=len(r['dispatches']);file=f'dispatch_{ordinal:03d}.npz'
   np.savez_compressed(out/file,x=x[start:start+25],physical=q[start:start+25],phase=phase)
   item=dict(phase=phase,artifact=file,attempted_rows=25,status='attempted',input_sha256=sha(out/file));r['dispatches'].append(item);r['attempted_rows']+=25;r['physical_calls']+=25;save()
   ll=np.asarray(vf(jnp.asarray(q[start:start+25],dtype=jnp.float64)))
   if ll.shape!=(25,) or ll.dtype!=np.float64 or not np.isfinite(ll).all():raise StructuralFailure('nonfinite Q8 target or integral certificate refusal')
   np.savez_compressed(out/file,x=x[start:start+25],physical=q[start:start+25],ll=ll,phase=phase)
   item.update(status='completed',artifact_sha256=sha(out/file));values.append(ll);save()
  return np.concatenate(values)
 meta=json.loads((Path(a.training)/'result.json').read_text())['data'];clouds=[]
 for entry in meta['source_archives']:
  path=Path(a.training)/f"input-{entry['seed']}.xml.gz";assert sha(path)==entry['xml_sha256']
  doc=utils.load_filename(str(path),contenthandler=ligolw.LIGOLWContentHandler)
  clouds.append(np.array([[q.longitude,q.latitude,q.polarization,q.inclination,q.coa_phase,q.distance] for q in lsctables.SimInspiralTable.get_table(doc)]))
 train=cube(np.vstack(clouds[:3]));held=cube(clouds[3]);assert train.shape==(300,6) and held.shape==(100,6)
 # Identical normalized Planck15 source-frame distance prior, diagnostic weights only.
 from RIFT.likelihood.priors_utils import get_astropy_cosmology,norm_and_inverse_via_grid_interp
 from astropy.cosmology import z_at_value
 from astropy import units as u
 from scipy.integrate import quad
 cosmo=get_astropy_cosmology('Planck15');zlo=float(z_at_value(cosmo.luminosity_distance,1*u.Mpc));zhi=float(z_at_value(cosmo.luminosity_distance,10000*u.Mpc))
 pdf,_,_=norm_and_inverse_via_grid_interp(lambda z:cosmo.differential_comoving_volume(z).value/(1+z),[zlo,zhi],y_of_x=lambda z:cosmo.luminosity_distance(z).value,vectorized=True)
 mass,error=quad(lambda d:float(pdf(d)),1,10000,epsabs=1e-7,limit=500);assert abs(mass-1)<1e-5
 r['normalized_distance_prior']=dict(mass=mass,quadrature_error=error)
 train_ll=score(train,'shared_training_cache_q8');held_ll=score(held,'shared_heldout_cache_q8');threshold=float(np.median(held_ll)-10)
 r['shared_reference']=dict(threshold=threshold,formula='median heldout Q8 logL minus10',training_q8_ll_quantiles=np.quantile(train_ll,[0,.05,.5,.95,1]).tolist(),heldout_q8_ll_quantiles=np.quantile(held_ll,[0,.05,.5,.95,1]).tolist(),heldout_is_finite_old_recipe_empirical_reference_not_posterior_truth=True)
 lookup={row.tobytes():i for i,row in enumerate(train)}
 ck=torch.load(a.candidate,map_location='cpu',weights_only=False);assert ck['q8_retuning_smoke_passed'] and ck['numerical_contract']['q_pregrid_factor']==8
 model=PeriodicVelocity(two_time=True,width=ck['map_width']).double();model.load_state_dict(ck['flowmap']);model.requires_grad_(False)
 center=torch.tensor(ck['center'],dtype=torch.float64);scale=torch.tensor(ck['scale'],dtype=torch.float64)
 tail=np.quantile(physical(train)[:,5],[.05,.95]);held_phys=physical(held)
 def geometry(x,ll):
  q=physical(x);d=q[:,5];ci=np.cos(q[:,3]);sindec=np.sin(q[:,1]);refci=np.cos(held_phys[:,3]);refd=held_phys[:,5]
  result=dict(n=len(x),high_score_hits=int(np.sum(ll>=threshold)),high_score_fraction=float(np.mean(ll>=threshold)),inclination_branch_counts=[int(np.sum(ci<=0)),int(np.sum(ci>0))],positive_cosi_mass=float(np.mean(ci>0)),distance_mean=float(d.mean()),distance_tail_mass=float(np.mean((d<tail[0])|(d>tail[1]))),polar_tail_mass=float(np.mean(abs(ci)>.9)),sindec_mean=float(sindec.mean()),raw_ll_quantiles=np.quantile(ll,[0,.05,.5,.95,1]).tolist())
  result['empirical_reference_discrepancies']=dict(positive_cosi_mass=result['positive_cosi_mass']-float(np.mean(refci>0)),distance_mean=result['distance_mean']-float(refd.mean()),distance_tail_mass=result['distance_tail_mass']-float(np.mean((refd<tail[0])|(refd>tail[1]))),polar_tail_mass=result['polar_tail_mass']-float(np.mean(abs(refci)>.9)),sindec_mean=result['sindec_mean']-float(np.sin(held_phys[:,1]).mean()))
  for axis,name,period in [(0,'RA',2*np.pi),(2,'psi',np.pi),(4,'phase',2*np.pi)]:
   angles=q[:,axis]*2*np.pi/period;refangles=held_phys[:,axis]*2*np.pi/period
   result[name+'_circular']={f:float(getattr(np,f)(angles).mean()-getattr(np,f)(refangles).mean()) for f in ['sin','cos']}
  return result
 for seed in [261071,261072]:
  check_time();s=dict(seed=seed,status='running',arms={});r['seeds'].append(s);save()
  oracle=ResamplingOracle()
  for param in PARAMS:oracle.add_parameter(param,pdf=lambda x:np.ones_like(x),prior_pdf=lambda x:np.ones_like(x),left_limit=0.,right_limit=1.,adaptive_sampling=True)
  oracle.setup(reference_samples=train,reference_params=PARAMS);assert oracle.params_ordered==PARAMS
  np.random.seed(seed+20);resampled=np.asarray(oracle.draw_simplified(100)[2]);assert resampled.shape==(100,6)
  indices=np.array([lookup[row.tobytes()] for row in resampled]);resampled_ll=train_ll[indices];r['logical_cached_score_rows']+=100
  gen=torch.Generator().manual_seed(seed)
  with torch.no_grad():z=base_noise(100,generator=gen);zero=torch.zeros(100,dtype=torch.float64);endpoint=model.jump(z,zero,torch.ones_like(zero));q=decode(endpoint,center,scale)
  if not support_mask(q).all():raise StructuralFailure('frozen map generated unsupported point; no redraw')
  mapped=cube(q.numpy());mapped_ll=score(mapped,f'seed{seed}_map_source')
  sources={'native_full_cache':(train,train_ll),'same_cache_resampling':(resampled,resampled_ll),'q8_map':(mapped,mapped_ll)}
  for arm,(x,ll) in sources.items():
   check_time();ar=dict(status='fitting',source=geometry(x,ll));s['arms'][arm]=ar;save();validate_cloud(x)
   np.random.seed(seed+10);av,gm=factory();av.bootstrap_from_samples(x,loglkl=None,cover_frac=0,dilate=1,inflate=1,seed=seed+10)
   gm.update_sampling_prior(np.zeros(len(x)),n_history=len(x),external_rvs={p:x[:,i] for i,p in enumerate(PARAMS)},tempering_exp=.1,log_scale_weights=True)
   ar['normalization']=dict(av=av_mass_check(av),gmm=gmm_mass_check(gm));save()
   def density(y):return .2+.4*np.asarray(av.sampling_density(y))+.4*np.asarray(gm.sampling_density(y))
   hq=density(held);assert np.isfinite(hq).all() and (hq>=.2-1e-12).all();loghq=np.log(hq)
   np.random.seed(seed+30);draw,qmix,labels=mixture_draw(av,gm,100,np.random.RandomState(seed+30),[.2,.4,.4]);np.testing.assert_allclose(qmix,density(draw),atol=1e-12,rtol=1e-12)
   final_ll=score(draw,f'seed{seed}_{arm}_postfit');ar['postfit']=geometry(draw,final_ll)
   ar['heldout_logq']=loghq.tolist();ar['mean_heldout_logq']=float(loghq.mean())
   lp=np.log(9999*np.asarray(pdf(physical(draw)[:,5])));assert np.isfinite(lp).all()
   lw=final_ll+lp-np.log(qmix);w=np.exp(lw-logsumexp(lw));ordered=np.sort(lw)
   ar['corrected_weight_concentration_diagnostic_only']=dict(ess=float(1/(w@w)),max_weight=float(w.max()),top_two_log_gap=float(ordered[-1]-ordered[-2]),formula='logL+normalized_logprior-logq; no evidence or posterior estimate')
   np.savez_compressed(out/f'seed{seed}_{arm}_diagnostic.npz',source_x=x,source_ll=ll,postfit_x=draw,postfit_ll=final_ll,qmix=qmix,labels=labels,heldout_q=hq)
   ar.update(status='completed',no_online_adaptation=True,evidence_rows=0);save()
  arms=s['arms'];maparm=arms['q8_map'];native=arms['native_full_cache'];cache=arms['same_cache_resampling'];best=max(['native_full_cache','same_cache_resampling'],key=lambda x:arms[x]['mean_heldout_logq'])
  paired=np.array(maparm['heldout_logq'])-arms[best]['heldout_logq'];delta=float(paired.mean());se=float(paired.std(ddof=1)/10)
  branch_present=all(count>0 for count in maparm['source']['inclination_branch_counts']+maparm['postfit']['inclination_branch_counts'])
  hit_deficit=max(native['postfit']['high_score_fraction'],cache['postfit']['high_score_fraction'])-maparm['postfit']['high_score_fraction']
  empirical_utility=delta>=1 and branch_present and hit_deficit<=.05+1e-12
  s['utility']=dict(best_control=best,paired_mean_logq_advantage=delta,nominal_paired_se_not_calibrated=se,conservative_nominal_lower_diagnostic=delta-2*se,branch_present=branch_present,postfit_hit_fraction_deficit=hit_deficit,empirical_minimum_utility=bool(empirical_utility),uncertainty_clause_pass=bool(delta-2*se>=1),interpretation='fixed finite empirical reference; teacher dependence prevents calibrated inference')
  common_failure=all(ar['postfit']['high_score_hits']==0 for ar in arms.values())
  clear_map_failure=maparm['source']['high_score_hits']==0 and cache['source']['high_score_hits']>=90 and maparm['mean_heldout_logq']<=native['mean_heldout_logq']-1
  s.update(status='completed',early_common_fitter_failure=bool(common_failure),early_clear_map_failure=bool(clear_map_failure));save()
  if common_failure or clear_map_failure:
   r.update(status='stopped_predeclared_scientific_futility',scientific_decision='close_current_candidate_adapter_line',reason='common_fitter_failure' if common_failure else 'clear_map_failure',second_seed_not_run=True);break
 else:
  passed=all(s['utility']['empirical_minimum_utility'] and s['utility']['uncertainty_clause_pass'] for s in r['seeds'])
  r.update(status='completed_minimum_utility_survived' if passed else 'completed_minimum_utility_absent',scientific_decision='separate_user_judgment_for_calibrated_study' if passed else 'close_current_candidate_adapter_line',second_seed_not_run=False)
 assert r['physical_calls']<=1200 and r['attempted_rows']<=1200
 assert sha(a.candidate)==journal['candidate_sha256'];r['candidate_unchanged']=True
 r['different_research_direction_requires_explicit_user_judgment']=True
except BaseException as exc:r.update(status='stopped_execution_or_contract_failure',reason=repr(exc),traceback=traceback.format_exc(),scientific_decision='no_utility_or_calibration_claim_from_execution_failure; no_auto_repair')
finally:save();print(json.dumps({k:r.get(k) for k in ['status','scientific_decision','reason','physical_calls','logical_cached_score_rows','cpu_seconds','wall_seconds']},indent=2))
