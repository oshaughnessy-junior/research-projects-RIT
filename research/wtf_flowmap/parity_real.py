"""Bounded real-case NoLoop/JAX value and coordinate-gradient diagnostic.

Run only on Caltech with the pinned source first on PYTHONPATH. No integration,
training or proposal construction. Every attempted target row is journaled before
dispatch, including AD and failed calls. Output contains aggregates, not samples.
"""
import os
os.environ.update(CUDA_VISIBLE_DEVICES='', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
                  OPENBLAS_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1',
                  JAX_PLATFORMS='cpu', JAX_ENABLE_X64='1', GW_SURROGATE='')
import argparse
import json
import resource
import time
from pathlib import Path
import traceback

START = time.perf_counter()
resource.setrlimit(resource.RLIMIT_CPU, (950, 950))
resource.setrlimit(resource.RLIMIT_AS, (8*1024**3, 8*1024**3))
os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})


def run(args, record, save):
    import hashlib
    import types
    import numpy as np
    import torch
    import jax
    import jax.numpy as jnp
    import lal
    import lalsimulation as ls
    import RIFT.lalsimutils as lu
    import RIFT.likelihood.factored_likelihood as fl
    from RIFT.likelihood.jax_ile.wrapper import (
        build_data_from_precompute, JAXExtrinsicLikelihood,
        bandlimited_storage_requirement, EXTRINSIC_PARAM_ORDER)
    from igwn_ligolw import utils, ligolw, lsctables

    sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
    inventory = json.loads(Path(args.inventory).read_text())
    for entry in inventory['inputs']:
        if entry['path'].endswith(('.gwf', '-psd.xml.gz', 'single-intrinsic.xml.gz')):
            assert sha(entry['path']) == entry['sha256'], entry['path']
    assert sha(args.checkpoint) == '4ce26989ccdcfcdb1702dd5e270d79a772487adb2b348b8b471bcaf3a8cf3158'
    ck = torch.load(args.checkpoint, map_location='cpu', weights_only=True)
    center, scale = np.array(ck['center']), np.array(ck['scale'])
    ac = np.array(ck['angle_centers'])
    record['parameter_order'] = list(EXTRINSIC_PARAM_ORDER)
    assert tuple(EXTRINSIC_PARAM_ORDER) == ('ra','dec','psi','incl','phiref','distMpc')
    rows = []
    for seed in range(25011401, 25011405):
        doc = utils.load_filename(str(Path(args.training)/f'input-{seed}.xml.gz'),
                                 contenthandler=ligolw.LIGOLWContentHandler)
        r = lsctables.SimInspiralTable.get_table(doc)[0]
        rows.append([r.longitude,r.latitude,r.polarization%np.pi,r.inclination,r.coa_phase,r.distance])
    theta = np.array(rows)
    def encode(p):
        angles = p[[0,4,2]]
        period = np.array([2*np.pi,2*np.pi,np.pi])
        a = np.angle(np.exp(2j*np.pi*(angles-ac)/period))*period/(2*np.pi)
        raw = np.array([a[0],np.arctanh(np.sin(p[1])),np.arctanh(np.cos(p[3])),np.log(p[5]),a[1],a[2]])
        return (raw-center)/scale
    def decode(z, xp=np):
        x = z*xp.asarray(scale)+xp.asarray(center)
        return xp.stack([xp.mod(x[0]+ac[0],2*np.pi),xp.arcsin(xp.tanh(x[1])),
                         xp.mod(x[5]+ac[2],np.pi),xp.arccos(xp.tanh(x[2])),
                         xp.mod(x[4]+ac[1],2*np.pi),xp.exp(x[3])])
    zrows = np.array([encode(p) for p in theta])
    np.testing.assert_allclose(np.array([decode(z) for z in zrows]),theta,atol=1e-10,rtol=1e-12)
    jac = np.asarray(jax.jacfwd(lambda z: decode(z,jnp))(zrows[0]))
    z = zrows[0]; x = z*scale+center
    expected = np.zeros((6,6))
    expected[0,0]=scale[0]; expected[1,1]=scale[1]/np.cosh(x[1])
    expected[2,5]=scale[5]; expected[3,2]=-scale[2]/np.cosh(x[2])
    expected[4,4]=scale[4]; expected[5,3]=scale[3]*np.exp(x[3])
    np.testing.assert_allclose(jac,expected,atol=1e-10,rtol=1e-10)
    assert np.all((theta[:,5]>1)&(theta[:,5]<10000))
    record['coordinate_roundtrip_and_jacobian']='passed'
    record['runtime'] = dict(host=os.uname().nodename, affinity=sorted(os.sched_getaffinity(0)),
                            numpy=np.__version__,torch=torch.__version__,jax=jax.__version__,
                            source_file=fl.__file__,source_sha256=sha(fl.__file__))
    save()
    t = time.perf_counter()
    base = Path('/home/richard.oshaughnessy/LVK/IR1/high-snr/investigations/S250114ax-full-sky-cubic-20260930/run')
    cache = Path(args.output)/'frames.cache'
    cache.write_text(''.join(f'{d} S250114ax_EXACT_ENCLOSING_9S 1420878135 9 file://localhost{base}/frames_dir/{d}-S250114ax_EXACT_ENCLOSING_9S-1420878135-9.gwf\n' for d in ['H1','L1']))
    data, psd = {}, {}
    for d in ['H1','L1']:
        data[d] = lu.frame_data_to_non_herm_hoff(str(cache),d+':DCS-CALIB_STRAIN_CLEAN_AR01',
                    start=1420878135.222656,stop=1420878143.222656,window_shape=.25,
                    deltaT=1/4096,deltaT_internal=1/4096,use_gwpy=False)
        psd[d] = lu.resample_psd_series(lu.get_psd_series_from_xmldoc(str(base/f'{d}-psd.xml.gz'),d),data[d].deltaF)
        psd[d].data.data *= lu.psd_windowing_factor(.25,len(data[d].data.data))/lu.psd_windowing_factor(.25,len(psd[d].data.data))
        freq = psd[d].f0+psd[d].deltaF*np.arange(len(psd[d].data.data))
        psd[d].data.data[freq<20] = 0
    record['frame_psd_setup_wall_seconds'] = time.perf_counter()-t
    p = lu.xml_to_ChooseWaveformParams_array('/home/richard.oshaughnessy/LVK/IR1/high-snr/investigations/S250114ax-time-discretization-20260917/single-intrinsic.xml.gz')[0]
    p.radec=False; p.fref=20.; p.fmin=13.33; p.tref=1420878141.22266
    p.dist=fl.distMpcRef*1e6*lal.PC_SI; p.phiref=0.; p.psi=0.; p.incl=0.
    p.approx=ls.GetApproximantFromString('SEOBNRv5PHM'); p.deltaT=1/4096; p.deltaF=data['H1'].deltaF
    storage,g0,gcert = bandlimited_storage_requirement(p.deltaT,.075)
    record['precompute'] = dict(attempted=1,integration_half=.075,storage_half=max(.15,storage),
                               guard_initial=g0,guard_certified=gcert,lmax=4,ignore_threshold=None)
    save(); t = time.perf_counter()
    packed, extras = build_data_from_precompute(p,data,psd,p.tref,max(.15,storage),.075,4,1792.,
        analyticPSD_Q=False,inv_spec_trunc_Q=False,T_spec=0.,ignore_threshold=None,use_gwsignal=True,
        use_gwsignal_approx='SEOBNRv5PHM',extra_waveform_kwargs=dict(fd_alignment_postevent_time=2,
        fd_L_frame=True,e_freq=1,gwsignal_extra_kwargs=dict(antisymmetric_modes_hm=True,
        enable_antisymmetric_modes=True,lmax_nyquist=1)))
    record['precompute_wall_seconds']=time.perf_counter()-t
    record['modes']={d:[list(pair) for pair in extras['rholms'][d]] for d in data}
    like = JAXExtrinsicLikelihood(packed,interp='sinc',phase_marginalization=False,time_quadrature='bandlimited')
    banks = [{ } for _ in range(5)]
    for d in data:
        nk,_,_,u,v,r,_,ep=fl.PackLikelihoodDataStructuresAsArrays(list(extras['rholms'][d]),
            extras['rholms_intp'][d],extras['rholms'][d],extras['cross_terms'][d],extras['cross_terms_V'][d])
        for bank, val in zip(banks,[nk,r,u,v,ep]): bank[d]=val
    def ref(q):
        v=types.SimpleNamespace(phi=q[0:1],theta=q[1:2],psi=q[2:3],incl=q[3:4],
              phiref=q[4:5],dist=q[5:6]*1e6*lal.PC_SI,tref=p.tref,deltaT=p.deltaT,radec=True)
        return float(np.asarray(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(
            np.asarray(packed.tvals),v,*banks,Lmax=4,xpy=np,time_interp='sinc',
            time_quadrature='bandlimited',phase_marginalization=False,n_cal=1))[0])
    def call(backend,phase,fn,gradient=False):
        assert record['previous_attempted_true_rows']+record['attempted_true_rows']<128
        record['attempted_true_rows']+=1
        item=dict(backend=backend,phase=phase,rows=1,gradient=gradient,status='attempted')
        record['calls'].append(item); save(); t=time.perf_counter()
        try:
            out=fn()
            arr=np.asarray(out[1] if gradient else out)
            assert np.isfinite(arr).all() and (not gradient or np.isfinite(out[0])), 'nonfinite likelihood/gradient'
            item['status']='passed'
            return out
        finally:
            item['wall_seconds']=time.perf_counter()-t; save()
    for i,q in enumerate(theta):
        a=call('numpy_noloop','values',lambda:ref(q))
        b=call('jax','values',lambda:like.value(q))
        error=abs(a-b); record.setdefault('value_absolute_errors',[]).append(error); save()
        assert error<=1e-3, f'value parity failed at row {i}: {error}'
    # One held-out-seed gradient point: reduced scope after the preserved
    # pruned-bank attempt, to keep TOTAL stage attempts within 128 rows.
    for i in [3]:
        q,z=theta[i],zrows[i]
        val,g=call('jax','autodiff',lambda:like.value_and_grad(q),True)
        grad=np.asarray(jax.jacfwd(lambda zz:decode(zz,jnp))(z)).T@g
        for backend,fun in [('numpy_noloop',ref),('jax',like.value)]:
            estimates=[]
            for h in [1e-4,5e-5]:
                fd=[]
                for d in range(6):
                    e=np.eye(6)[d]*h
                    hi=call(backend,'finite_difference',lambda:fun(decode(z+e)))
                    lo=call(backend,'finite_difference',lambda:fun(decode(z-e)))
                    fd.append((hi-lo)/(2*h))
                estimates.append(np.array(fd))
            err=float(np.max(np.abs(estimates[1]-grad)/(.01+1e-3*np.abs(grad))))
            conv=float(np.max(np.abs(estimates[1]-estimates[0])/(.01+1e-3*np.abs(grad))))
            record.setdefault('gradient_errors_over_tolerance',[]).append(dict(row=i,backend=backend,ad_fd=err,fd_refinement=conv));save()
            assert max(err,conv)<=1, 'gradient parity/refinement failed'
    for backend,fun in [('numpy_noloop',ref),('jax',like.value)]:
        baseline=call(backend,'periodicity_baseline',lambda:fun(theta[0]))
        for d,period in [(0,2*np.pi),(2,np.pi),(4,2*np.pi)]:
            q=theta[0].copy();q[d]+=period
            val=call(backend,'periodicity',lambda:fun(q))
            assert abs(val-baseline)<=1e-3,'physical periodicity failed'
    record['status']='passed'


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ['inventory','checkpoint','training','output']:parser.add_argument('--'+key,required=True)
    parser.add_argument('--previous-result')
    args=parser.parse_args()
    out=Path(args.output);out.mkdir(parents=True,exist_ok=False)
    previous=json.loads(Path(args.previous_result).read_text()) if args.previous_result else {}
    record=dict(status='running',attempted_true_rows=0,calls=[],
                previous_attempted_true_rows=previous.get('attempted_true_rows',0),
                tolerances=dict(value_absolute_nat=.001,gradient_absolute=.01,gradient_relative=.001,
                                fd_steps_standardized=[.0001,.00005]),
                caps=dict(cpu_seconds=950,virtual_memory_bytes=8*1024**3,total_stage_true_rows=128),
                selected_cache_indices=[0,100,200,300],fine_tuning_run=False)
    def save():
        r=resource.getrusage(resource.RUSAGE_SELF)
        record['cost']=dict(process_wall_seconds=time.perf_counter()-START,cpu_user_seconds=r.ru_utime,
                            cpu_system_seconds=r.ru_stime,max_rss_kib=r.ru_maxrss)
        (out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
    try:run(args,record,save)
    except BaseException as exc:
        record['status']='failed';record['blocker']=type(exc).__name__+': '+str(exc);record['traceback']=traceback.format_exc()
        raise
    finally:save()
