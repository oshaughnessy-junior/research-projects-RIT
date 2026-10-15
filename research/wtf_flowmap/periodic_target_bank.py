"""Full retained-mode bank; physical log-integral API, no prior tilting.

Extracted from the verified real_target builder. No old chart checkpoint is
loaded. ignore_threshold=None and identical NoLoop/JAX time grids are explicit.
"""
from pathlib import Path
import time
import hashlib
import types
import numpy as np
import lal
import lalsimulation as ls
import RIFT.lalsimutils as lu
import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood.jax_ile.wrapper import (build_data_from_precompute,
    JAXExtrinsicLikelihood,bandlimited_storage_requirement)


def build_bank(args,record,save):
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
    expected={(l,m) for l in range(2,5) for m in range(-l,l+1)}
    assert all(set(extras['rholms'][d])==expected for d in data),'retained-mode mismatch'
    tvals=np.asarray(packed.tvals)
    record['time_grid']=dict(count=len(tvals),first=float(tvals[0]),last=float(tvals[-1]),
        spacing=float(p.deltaT),sha256=hashlib.sha256(tvals.tobytes()).hexdigest(),
        definition='shared raw log integral over closed NoLoop grid; no new time normalization')
    return dict(like=like,reference=ref)
