"""Periodic checkpoint parity only: no optimizer, evidence or inference.

123 planned endpoint attempts, including eight deliberate no-dispatch fixtures.
Every attempt and every physical dispatch is flushed before evaluator execution.
Raw failure context and fixed generated noise stay privately on Caltech.
"""
import os,time,resource,argparse,json,hashlib,math,re,traceback
from pathlib import Path
os.environ.update(CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',
                  NUMEXPR_NUM_THREADS='1',JAX_PLATFORMS='cpu',JAX_ENABLE_X64='1',GW_SURROGATE='')
START=time.perf_counter();os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})


def main(args,r,save):
    import numpy as np
    import torch
    import jax
    import jax.numpy as jnp
    from igwn_ligolw import utils,ligolw,lsctables
    from periodic_model import PeriodicVelocity,base_noise,decode,support_mask
    from periodic_target_bank import build_bank
    import RIFT.likelihood.factored_likelihood as fl
    from RIFT.likelihood.jax_ile import wrapper,core
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    assert sha(args.checkpoint)=='a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea'
    expected=[(fl.__file__,'7b87a19e7133155cb1bb67a790a58ab7204f605d6d12cd78471edc34e044c974'),
        (wrapper.__file__,'0db282efa8db272e404cc2d649485feaba3c0339998a36ddde293b7ac4166005'),
        (core.__file__,'0f499791e796558b7b2460027ef8459302925dcf792801883664ceaf56336ab8')]
    for path,digest in expected:assert sha(path)==digest,'unreviewed likelihood source change'
    r['likelihood_sources']={path:sha(path) for path,_ in expected}
    driver=Path(fl.__file__).parents[2]/'bin/integrate_likelihood_extrinsic_batchmode'
    driver_text=driver.read_text()
    assert 'get_astropy_cosmology("Planck15")' in driver_text
    assert 'psi_prior_pdf = mcsampler.ret_uniform_samp_vector_alt(psi_prior_range[0], psi_prior_range[1])' in driver_text
    assert 'prior_pdf = mcsampler.ret_uniform_samp_vector_alt(phi_orb_prior_range[0], phi_orb_prior_range[1])' in driver_text
    r['current_normalized_driver_sha256']=sha(driver)
    inventory=json.loads(Path(args.inventory).read_text())
    for entry in inventory['inputs']:
        if entry['path'].endswith(('.gwf','-psd.xml.gz','single-intrinsic.xml.gz')):
            assert sha(entry['path'])==entry['sha256']
    data_record=Path(args.training)/'result.json'
    assert sha(data_record)=='e809627592936cd16e2e55fe9abece6ea476d25323d550f47b91162b097166a9'
    provenance=json.loads(data_record.read_text())['data']
    theta=[]
    for entry in provenance['source_archives']:
        path=Path(args.training)/f"input-{entry['seed']}.xml.gz"
        assert sha(path)==entry['xml_sha256']
        doc=utils.load_filename(str(path),contenthandler=ligolw.LIGOLWContentHandler)
        q=lsctables.SimInspiralTable.get_table(doc)[0]
        theta.append([q.longitude,q.latitude,q.polarization%math.pi,q.inclination,q.coa_phase,q.distance])
    theta=np.asarray(theta);ck=torch.load(args.checkpoint,map_location='cpu',weights_only=True)
    assert ck['geometry']=='T3_R3_v1' and ck['gates_passed']
    model=PeriodicVelocity(two_time=True,width=ck['map_width']).double();model.load_state_dict(ck['flowmap']);model.requires_grad_(False)
    center=torch.tensor(ck['center'],dtype=torch.float64);scale=torch.tensor(ck['scale'],dtype=torch.float64)
    def encode(q):
        raw=np.array([np.arctanh(np.sin(q[1])),np.arctanh(np.cos(q[3])),np.log(q[5])])
        return np.r_[q[0],q[4],2*q[2],(raw-center.numpy())/scale.numpy()]
    states=np.asarray([encode(q) for q in theta])
    def torch_decode(x):return decode(x,center,scale)
    def jax_decode(x):
        raw=x[3:]*jnp.asarray(scale.numpy())+jnp.asarray(center.numpy())
        return jnp.stack([x[0],jnp.arcsin(jnp.tanh(raw[0])),x[2]/2,
            jnp.arccos(jnp.tanh(raw[1])),x[1],jnp.exp(raw[2])])
    for i in [0,3]:
        x=torch.tensor(states[i],dtype=torch.float64)
        np.testing.assert_allclose(torch_decode(x).numpy(),theta[i],rtol=1e-12,atol=1e-10)
        jt=torch.autograd.functional.jacobian(torch_decode,x).numpy()
        jj=np.asarray(jax.jacfwd(jax_decode)(states[i]));np.testing.assert_allclose(jt,jj,atol=1e-10,rtol=1e-10)
        h=1e-6
        for d in range(6):
            e=np.eye(6)[d]*h
            fd=(torch_decode(x+torch.tensor(e)).numpy()-torch_decode(x-torch.tensor(e)).numpy())/(2*h)
            np.testing.assert_allclose(fd,jt[:,d],atol=1e-5,rtol=1e-6)
        assert jt[2,2]==.5 and jt[3,4]<0 and np.isfinite(jt).all()
    r['coordinate_contracts']='two-row roundtrip, Torch/JAX/full FD Jacobian passed'
    r['normalization_convention']=dict(reward='physical log time-integral API only, no prior or Jacobian added',
        angular_prior='unit mass on RA[0,2pi),phase[0,2pi),psi[0,pi)',distance_prior='current normalized Planck15 cosmo_sourceframe on [1,10000]',
        historical_angular_prior_mass=8,historical_lnZ_offset=math.log(8),likelihood_is_not_evidence=True)
    r['runtime']=dict(host=os.uname().nodename,affinity=sorted(os.sched_getaffinity(0)),pid=os.getpid(),
        numpy=np.__version__,torch=torch.__version__,jax=jax.__version__)
    bank=build_bank(args,r,save);like=bank['like'];reference=bank['reference'];save()
    generator=torch.Generator().manual_seed(261030);noise=base_noise(16,generator=generator)
    with torch.no_grad():generated=model.jump(noise,torch.zeros(16,dtype=noise.dtype),torch.ones(16,dtype=noise.dtype))
    payload=Path(args.output)/'fixed_points.pt'
    torch.save(dict(noise=noise,generated=generated,cached_physical=theta,cached_states=states,
        seed=261030,checkpoint_sha256=sha(args.checkpoint)),payload)
    r['fixed_points']=dict(path=str(payload),sha256=sha(payload),generated_rows=16);save()
    def guard(q):
        if not bool(support_mask(torch.tensor(q,dtype=torch.float64))):raise ValueError('nonfinite or physical distance/pole support failure')
    def begin(kind,q):
        if r['prior_attempted_rows']+r['attempted_rows']>=128:raise RuntimeError('stage row budget exhausted')
        r['attempted_rows']+=1
        item=dict(ordinal=r['attempted_rows'],kind=kind,status='attempted',physical_rows=0)
        r['calls'].append(item);save()
        # This artifact exists before guard/evaluator dispatch. A failure never
        # depends on reconstructing an unsaved endpoint from optimizer RNG.
        torch.save(dict(physical=np.asarray(q),kind=kind,ordinal=r['attempted_rows'],
            rng=torch.random.get_rng_state(),checkpoint_sha256=sha(args.checkpoint)),Path(args.output)/'last_dispatch.pt')
        return item
    def evaluate(q,backend,kind,gradient=False):
        q=np.asarray(q,dtype=float);item=begin(kind+':'+backend,q)
        try:guard(q)
        except Exception as exc:item.update(status='rejected_support',reason=str(exc));save();raise
        r['dispatched_physical_rows']+=1;item['physical_rows']=1;item['gradient']=gradient;save();t0=time.perf_counter()
        try:
            if backend=='numpy_noloop':v=reference(q);g=None
            elif gradient:
                r['jax_gradient_rows']+=1;save();v,g=like.value_and_grad(q)
            else:
                v=float(np.asarray(like.log_likelihood(*[np.array([a]) for a in q]))[0]);g=None
            if not np.isfinite(v) or (g is not None and not np.isfinite(g).all()):
                raise ValueError('nonfinite likelihood/gradient or bandlimited certificate failure')
            item['status']='passed';return (float(v),np.asarray(g)) if gradient else float(v)
        except BaseException as exc:item.update(status='failed',reason=type(exc).__name__+': '+str(exc));raise
        finally:item['wall_seconds']=time.perf_counter()-t0;save()
    def close_value(a,b,label):
        error=abs(a-b);r['value_errors'].append(dict(label=label,absolute_nat=error));save()
        assert error<=.001,label+' value parity failed: '+str(error)
    def close_grad(a,b,label):
        ratio=float(np.max(np.abs(a-b)/(.01+.001*np.abs(b))))
        r['gradient_errors'].append(dict(label=label,error_over_tolerance=ratio));save()
        assert ratio<=1,label+' gradient parity failed: '+str(ratio)
    class Bridge(torch.autograd.Function):
        @staticmethod
        def forward(ctx,q):
            value,g=evaluate(q.detach().numpy(),'jax',r['_phase'],True)
            ctx.save_for_backward(torch.tensor(g,dtype=q.dtype));r['_last_gradient']=g
            return torch.tensor(value,dtype=q.dtype)
        @staticmethod
        def backward(ctx,upstream):
            r['torch_reward_backward_rows']+=1;save()
            return upstream*ctx.saved_tensors[0]
    def reward(x):return Bridge.apply(torch_decode(x))
    # Eight deliberate no-dispatch fixtures, charged as attempts.
    for label,axis,value in [('dmin',5,1.),('dmax',5,10000.),('south',1,-math.pi/2),
        ('north',1,math.pi/2),('incl0',3,0.),('inclpi',3,math.pi),('nan',0,float('nan')),('inf',0,float('inf'))]:
        q=theta[3].copy();q[axis]=value;item=begin('expected_guard_fixture:'+label,q)
        try:guard(q)
        except ValueError:item['status']='expected_support_rejection'
        else:raise AssertionError('support fixture not rejected')
        save()
    r['guard_fixtures_passed']=8
    for i,q in enumerate(theta):
        a=evaluate(q,'numpy_noloop','cached_value_'+str(i));b=evaluate(q,'jax','cached_value_'+str(i))
        close_value(a,b,'cached_'+str(i))
    # Held-out sampler seed04: endpoint-state bridge and complete chart FD.
    r['_phase']='heldout_bridge';x=torch.tensor(states[3],dtype=torch.float64,requires_grad=True)
    value=reward(x);gradient=torch.autograd.grad(value,x)[0].numpy()
    direct,dg=evaluate(theta[3],'jax','heldout_direct_gradient',True)
    close_value(float(value.detach()),direct,'heldout_bridge_direct')
    analytic=np.asarray(jax.jacfwd(jax_decode)(states[3])).T@dg
    np.testing.assert_allclose(gradient,analytic,atol=1e-8,rtol=1e-9)
    estimates=[]
    for h in [1e-4,5e-5]:
        fd=[]
        for axis in range(6):
            e=torch.eye(6,dtype=x.dtype)[axis]*h
            fd.append((float(reward(x.detach()+e))-float(reward(x.detach()-e)))/(2*h))
        estimates.append(np.array(fd))
    close_grad(estimates[1],gradient,'heldout_bridge_fd');close_grad(estimates[1],estimates[0],'heldout_fd_refinement')
    r['heldout_bridge_passed']=True;save()
    # Both sides of every physical angle seam, plus shifted representative AD.
    for axis,period,label in [(0,2*math.pi,'ra'),(2,math.pi,'psi'),(4,2*math.pi,'phase')]:
        for sign in [-1,1]:
            q=theta[3].copy();q[axis]=sign*1e-5
            a=evaluate(q,'numpy_noloop',label+'_seam');b=evaluate(q,'jax',label+'_seam')
            close_value(a,b,label+'_seam_'+str(sign))
            v,g=evaluate(q,'jax',label+'_seam_ad',True);close_value(v,b,label+'_seam_ad_value')
            shifted=q.copy();shifted[axis]+=period
            w,h=evaluate(shifted,'jax',label+'_shift_ad',True)
            close_value(v,w,label+'_period_value');close_grad(h,g,label+'_period_gradient')
    r['physical_seams_and_periods_passed']=True;save()
    for i,point in enumerate(generated):
        q=torch_decode(point).numpy()
        a=evaluate(q,'numpy_noloop','generated_value_'+str(i));b=evaluate(q,'jax','generated_value_'+str(i))
        close_value(a,b,'generated_'+str(i))
    # Full repaired jump -> new decoder -> true reward pullback, with
    # independent NumPy NoLoop finite differences in the base-noise coordinates.
    r['_phase']='generated_full_pullback';z=noise[0].clone().requires_grad_(True)
    def jump(x):return model.jump(x[None],torch.zeros(1,dtype=x.dtype),torch.ones(1,dtype=x.dtype))[0]
    value=reward(jump(z));full_grad=torch.autograd.grad(value,z)[0].numpy()
    gp=r['_last_gradient'].copy()
    jm=torch.autograd.functional.jacobian(jump,z.detach()).numpy()
    jd=torch.autograd.functional.jacobian(torch_decode,jump(z.detach())).numpy()
    np.testing.assert_allclose(full_grad,jm.T@jd.T@gp,atol=1e-8,rtol=1e-9)
    estimates=[]
    for h in [1e-4,5e-5]:
        fd=[]
        for axis in range(6):
            e=torch.eye(6,dtype=z.dtype)[axis]*h
            plus=torch_decode(jump(z.detach()+e)).numpy();minus=torch_decode(jump(z.detach()-e)).numpy()
            hi=evaluate(plus,'numpy_noloop','generated_pullback_fd');lo=evaluate(minus,'numpy_noloop','generated_pullback_fd')
            fd.append((hi-lo)/(2*h))
        estimates.append(np.array(fd))
    close_grad(estimates[1],full_grad,'generated_full_pullback_fd')
    close_grad(estimates[1],estimates[0],'generated_full_pullback_refinement')
    r.update(status='passed_periodic_physical_parity_limited_probe',generated_full_pullback_passed=True)
    assert r['attempted_rows']==123 and r['dispatched_physical_rows']==115
    save()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ['stage-root','timing-file','inventory','checkpoint','training','output']:parser.add_argument('--'+key,required=True)
    args=parser.parse_args();root=Path(args.stage_root);out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    old_times=[];old_rows=0
    for path in root.rglob('*.time'):
        if path.resolve()==Path(args.timing_file).resolve():continue
        text=path.read_text();u=re.search(r'User time \(seconds\): ([0-9.]+)',text);s=re.search(r'System time \(seconds\): ([0-9.]+)',text)
        if not u or not s:raise RuntimeError('unresolved prior timing '+str(path))
        old_times.append(dict(path=str(path),cpu_seconds=float(u[1])+float(s[1])))
    for path in root.rglob('result.json'):
        if path.parent==out:continue
        old=json.loads(path.read_text())
        if old.get('stage')=='periodic_model_physical_parity_v1':old_rows+=old['attempted_rows']
    prior_cpu=sum(x['cpu_seconds'] for x in old_times);hard=min(570,math.floor(600-prior_cpu-5))
    if hard<10 or old_rows>=128:raise RuntimeError('prior attempts exhausted stage allocation')
    resource.setrlimit(resource.RLIMIT_CPU,(hard,hard));resource.setrlimit(resource.RLIMIT_AS,(8*1024**3,8*1024**3))
    r=dict(stage='periodic_model_physical_parity_v1',status='running',prior_timing_records=old_times,prior_cpu_seconds=prior_cpu,
        prior_attempted_rows=old_rows,attempted_rows=0,dispatched_physical_rows=0,jax_gradient_rows=0,
        torch_reward_backward_rows=0,calls=[],value_errors=[],gradient_errors=[],optimizer_steps=0,
        source_parent_commit='419528e32638c0e4fb289deeaf6f3331e6910e2f',seed=261030,
        caps=dict(stage_cpu_seconds=600,process_cpu_seconds=hard,virtual_memory_bytes=8*1024**3,attempted_rows=128),
        tolerances=dict(value_absolute_nat=.001,gradient_absolute=.01,gradient_relative=.001,fd_steps=[.0001,.00005]),
        source_sha256={f:hashlib.sha256((Path(__file__).parent/f).read_bytes()).hexdigest()
            for f in ['parity_periodic.py','periodic_target_bank.py','periodic_model.py']})
    def save():
        snap={k:v for k,v in r.items() if not k.startswith('_')};usage=resource.getrusage(resource.RUSAGE_SELF)
        snap['cost']=dict(process_wall_seconds=time.perf_counter()-START,cpu_user_seconds=usage.ru_utime,
            cpu_system_seconds=usage.ru_stime,max_rss_kib=usage.ru_maxrss)
        temp=out/'result.tmp';temp.write_text(json.dumps(snap,indent=2)+'\n');temp.replace(out/'result.json')
    save()
    try:main(args,r,save)
    except BaseException as exc:
        r.update(status='stopped',blocker=type(exc).__name__+': '+str(exc),traceback=traceback.format_exc());raise
    finally:save()
