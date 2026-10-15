"""Bounded, research-only conditional importance pilot; never a map log-q."""
import os,time,resource,json,hashlib,traceback,argparse
from pathlib import Path
START=time.perf_counter();CPU=time.process_time()
os.sched_setaffinity(0,{0})
resource.setrlimit(resource.RLIMIT_CPU,(1150,1155))
# Virtual address space is not RSS: monitor resident memory at each dispatch.
import threading
def _rss_guard():
    while True:
        status=Path('/proc/self/status').read_text()
        rss=int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:')))
        if rss>8*1024**2:
            os.write(2,b'RSS hard guard exceeded 8 GiB; stop entire pilot\n');os._exit(137)
        time.sleep(.1)
threading.Thread(target=_rss_guard,daemon=True).start()
import numpy as np
import torch
import jax
jax.config.update("jax_enable_x64",True)
from scipy.integrate import quad
from five_arm_sampling import *
from periodic_real_context import load_context
from periodic_model import PeriodicVelocity,base_noise,decode,support_mask


def cube(q):
    q=np.asarray(q);return np.column_stack(((q[:,0]%(2*np.pi))/(2*np.pi),(np.sin(q[:,1])+1)/2,
        (q[:,2]%np.pi)/np.pi,(np.cos(q[:,3])+1)/2,(q[:,4]%(2*np.pi))/(2*np.pi),(q[:,5]-1)/9999))
def physical(x):
    x=np.asarray(x);return np.column_stack((2*np.pi*x[:,0],np.arcsin(2*x[:,1]-1),np.pi*x[:,2],
        np.arccos(2*x[:,3]-1),2*np.pi*x[:,4],1+9999*x[:,5]))

def factory():
    from RIFT.integrators import mcsamplerAdaptiveVolume as A,mcsamplerEnsemble as E
    av=A.MCSampler(n_chunk=100);gm=E.MCSampler()
    for obj in [av,gm]:
        for p in PARAMS:
            obj.add_parameter(p,pdf=lambda x:np.ones_like(x),prior_pdf=lambda x:np.ones_like(x),
                left_limit=0.,right_limit=1.,adaptive_sampling=True)
    av.setup();gm.setup(n_comp={GROUPS[0]:4,GROUPS[1]:4,GROUPS[2]:2},gmm_dict={g:None for g in GROUPS},gmm_defensive_frac=0)
    return av,gm

def main(a):
    out=Path(a.output);out.mkdir(exist_ok=False,parents=True)
    r=dict(status='setup',arms={},global_attempts=10,physical_rows=10,caps=dict(cpu_seconds=1200,
      prior_gate_cpu_seconds=3.30,prior_failed_setups_cpu_seconds=20.41,rss_gib=8,attempts=10000,per_arm=2000),seed=261051,training_seed=261050,
      estimator='fixed 1000 conditional importance draws; normalized six-dimensional prior; raw time-integral API',
      historical_acquisition_rows_lower_bound=15984560,historical_acquisition_cpu_seconds=None)
    def save():
        r['process_cpu_seconds']=time.process_time()-CPU;r['wall_seconds']=time.perf_counter()-START
        r['peak_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        (out/'summary.json').write_text(json.dumps(r,indent=2,allow_nan=False))
    try:
        ctx=load_context(a,r,save);like=ctx['bank']['like'];ref=ctx['bank']['reference']
        from RIFT.likelihood.priors_utils import get_astropy_cosmology,norm_and_inverse_via_grid_interp
        from astropy.cosmology import z_at_value
        from astropy import units as u
        cosmo=get_astropy_cosmology('Planck15')
        zlo=float(z_at_value(cosmo.luminosity_distance,1*u.Mpc));zhi=float(z_at_value(cosmo.luminosity_distance,10000*u.Mpc))
        pdf,cdf,inv=norm_and_inverse_via_grid_interp(lambda z:cosmo.differential_comoving_volume(z).value/(1+z),
            [zlo,zhi],y_of_x=lambda z:cosmo.luminosity_distance(z).value,vectorized=True)
        mass,err=quad(lambda d:float(pdf(d)),1,10000,epsabs=1e-7,limit=500)
        if abs(mass-1)>1e-5:raise ValueError('distance prior not normalized')
        r['prior_mass']=mass;r['prior_quadrature_error']=err
        from igwn_ligolw import utils,ligolw,lsctables
        meta=json.loads((Path(a.training)/'result.json').read_text())['data'];clouds=[]
        for entry in meta['source_archives']:
            path=Path(a.training)/f"input-{entry['seed']}.xml.gz"
            assert hashlib.sha256(path.read_bytes()).hexdigest()==entry['xml_sha256']
            doc=utils.load_filename(str(path),contenthandler=ligolw.LIGOLWContentHandler)
            clouds.append(np.array([[q.longitude,q.latitude,q.polarization,q.inclination,q.coa_phase,q.distance]
                                   for q in lsctables.SimInspiralTable.get_table(doc)]))
        train=np.vstack(clouds[:3]);held=clouds[3];tail=np.quantile(train[:,5],[.05,.95])
        r['tail_thresholds_mpc']=tail.tolist();r['shared_setup_cpu_seconds']=time.process_time()-CPU
        r['shared_setup_wall_seconds']=time.perf_counter()-START;save()
        for arm in ['av','av_gmm','cache','pretrained','tuned']:
            t=time.perf_counter();c=time.process_time();ar=dict(status='running',attempts=2,physical_rows=2,phases={'prior_failed_validation':2})
            r['arms'][arm]=ar;np.random.seed(261051);rng=np.random.RandomState(261051)
            records=[];est=FixedNEstimator(1000)
            def target(x,phase):
                x=np.asarray(x);n=len(x)
                if ar['attempts']+n>2000 or r['global_attempts']+n>10000:raise RuntimeError('attempt cap')
                ar['attempts']+=n;r['global_attempts']+=n;ar['phases'][phase]=ar['phases'].get(phase,0)+n;save()
                if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss>8*1024**2:raise RuntimeError('RSS cap')
                if time.process_time()-CPU>1140:raise RuntimeError('CPU reserve reached')
                if x.shape!=(n,6) or not np.isfinite(x).all() or np.any((x<0)|(x>1)):raise ValueError('target input bounds')
                q=physical(x);ar['physical_rows']+=n;r['physical_rows']+=n;save()
                ll=np.asarray(like.log_likelihood(*[q[:,i] for i in range(6)]),dtype=float)
                lp=np.log(9999*np.asarray(pdf(q[:,5])))
                if not np.isfinite(ll).all() or not np.isfinite(lp).all():raise ValueError('nonfinite physical target')
                records.append((phase,x.copy(),ll.copy(),lp.copy()))
                return ll+lp
            try:
                # Two NumPy/JAX parity points, four charged attempts each arm.
                for q in held[:2]:
                    x=cube(q[None]);val=target(x,'validation')[0]-np.log(9999*float(pdf(q[5])))
                    ar['attempts']+=1;r['global_attempts']+=1
                    ar['phases']['reference_validation']=ar['phases'].get('reference_validation',0)+1
                    ar['physical_rows']+=1;r['physical_rows']+=1;save()
                    gold=ref(physical(x)[0]);ar.setdefault('value_parity',[]).append(dict(jax=float(val),noloop=float(gold),absolute_nat=float(abs(val-gold))))
                    assert abs(val-gold)<.001,'NoLoop/JAX mismatch; unchanged stage3e tolerance'
                av,gm=factory()
                if arm in ['av','av_gmm']:
                    av.update_sampling_prior_selfish(lambda *cols:target(np.column_stack(cols),'training'),*PARAMS,xpy=np)
                else:
                    trng=np.random.RandomState(261050)
                    if arm=='cache':x=cube(train[trng.choice(len(train),100,replace=True)])
                    else:
                        path=a.checkpoint if arm=='pretrained' else a.tuned
                        digest=hashlib.sha256(Path(path).read_bytes()).hexdigest()
                        assert digest==({'pretrained':'a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea',
                                         'tuned':'71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1'}[arm])
                        ck=torch.load(path,map_location='cpu',weights_only=True)
                        model=PeriodicVelocity(two_time=True,width=ck['map_width']).double();model.load_state_dict(ck['flowmap'])
                        gen=torch.Generator().manual_seed(261050)
                        with torch.no_grad():
                            z=base_noise(100,generator=gen);s=torch.zeros(100,dtype=torch.float64)
                            endpoint=model.jump(z,s,torch.ones_like(s))
                            q=decode(endpoint,torch.tensor(ck['center'],dtype=torch.float64),torch.tensor(ck['scale'],dtype=torch.float64))
                        if not support_mask(q).all():raise ValueError('map endpoint support')
                        x=cube(q.numpy());ar['checkpoint_sha256']=digest
                    validate_cloud(x);target(x,'training')
                    av.bootstrap_from_samples(x,loglkl=None,cover_frac=0,dilate=1,inflate=1,seed=261050)
                training=[z for z in records if z[0]=='training'];tx=np.vstack([z[1] for z in training])
                if arm!='av':
                    tw=np.concatenate([z[2]+z[3] for z in training]) if arm=='av_gmm' else np.zeros(len(tx))
                    gm.update_sampling_prior(tw.copy(),n_history=len(tx),external_rvs={p:tx[:,i] for i,p in enumerate(PARAMS)},tempering_exp=.1,log_scale_weights=True)
                else:gm=None
                ar['normalization_checks']=[dict(av=av_mass_check(av),gmm=gmm_mass_check(gm) if gm else None)]
                ar['training_only_rows']=len(tx);ar['training_cpu_seconds']=time.process_time()-c
                ar['training_wall_seconds']=time.perf_counter()-t
                weights=[.2,.8] if gm is None else [.2,.4,.4]
                for chunk in range(10):
                    x,q,labels=mixture_draw(av,gm,100,rng,weights);lt=target(x,'evidence')
                    est.append(x,lt,q,labels,'evidence')
                    if chunk<9:
                        used=ar['phases'].get('adaptation',0);needed=int(np.sum(av.ninbin))
                        if needed<=894-used:
                            av.update_sampling_prior_selfish(lambda *cols:target(np.column_stack(cols),'adaptation'),*PARAMS,xpy=np)
                        if gm:
                            ex=np.vstack(est.points);lw=np.concatenate(est.logweights)
                            gm.update_sampling_prior(lw.copy(),n_history=len(ex),external_rvs={p:ex[:,i] for i,p in enumerate(PARAMS)},tempering_exp=.1,log_scale_weights=True)
                        ar['normalization_checks'].append(dict(av=av_mass_check(av),gmm=gmm_mass_check(gm) if gm else None))
                result=est.result();q=physical(result['x']);w=result['weights'];D=q[:,5]
                metrics=dict(mean_distance=float(w@D),positive_cosi_mass=float(w@(np.cos(q[:,3])>0)),
                  distance_tail_mass=float(w@((D<tail[0])|(D>tail[1]))),polar_tail_mass=float(w@(np.abs(np.cos(q[:,3]))>.9)))
                for i,name,period in [(0,'ra',2*np.pi),(2,'psi',np.pi),(4,'phase',2*np.pi)]:
                    metrics[name+'_cos']=float(w@np.cos(2*np.pi*q[:,i]/period));metrics[name+'_sin']=float(w@np.sin(2*np.pi*q[:,i]/period))
                ar['metrics']=metrics
                for key in ['lnZ','relative_se','ess','max_weight','n']:ar[key]=result[key]
                ar['converged']=bool(result['ess']>=100 and result['max_weight']<=.05 and result['relative_se']<=.1)
                ar['status']='complete';np.savez_compressed(out/(arm+'_evidence.npz'),**{k:v for k,v in result.items() if isinstance(v,np.ndarray)})
            except Exception as e:
                ar['status']='failed';ar['error']=repr(e);ar['traceback']=traceback.format_exc()
            finally:
                # Every dispatched batch is a private audit artifact, including training.
                for i,(phase,x,ll,lp) in enumerate(records):np.savez_compressed(out/f'{arm}_{i:02d}_{phase}.npz',x=x,ll=ll,lp=lp)
                ar['cpu_seconds']=time.process_time()-c;ar['wall_seconds']=time.perf_counter()-t;save()
        r['status']='pilot_complete_not_calibrated';save()
    except Exception as e:r['status']='setup_failed';r['error']=repr(e);r['traceback']=traceback.format_exc();save();raise

if __name__=='__main__':
    p=argparse.ArgumentParser()
    for k in ['output','training','inventory','checkpoint','tuned']:p.add_argument('--'+k,required=True)
    main(p.parse_args())
