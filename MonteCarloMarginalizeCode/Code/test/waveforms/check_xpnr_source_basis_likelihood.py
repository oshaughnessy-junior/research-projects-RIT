#!/usr/bin/env python3
"""Native XPNR PE/Cartesian controls and expanded-basis cubic NoLoop validation.

No fitted lag, phase or amplitude. Both native requested-start (20Hz) and
generation-start (18Hz) polarizations are retained to expose conditioning.
The factorization oracle applies the declared common linear conditioner to
independent native18 polarizations. Raw native20 remains an explicit diagnostic;
--require-requested-start also enforces its original likelihood criterion.
"""
import argparse
import hashlib
from importlib.metadata import version, PackageNotFoundError
import json
import os
from pathlib import Path
import platform
import subprocess
import time

import lal
import lalsimulation as ls
import numpy as np

from RIFT import lalsimutils as lsu
from RIFT.likelihood import factored_likelihood as fl
from RIFT.likelihood import xpnr_source_basis as basis_module
from RIFT.likelihood import SphericalHarmonics_gpu as harmonics_module
from RIFT.likelihood.xpnr_source_basis import XPNRSourceBasis, _dictionary, PRECISION_OPTIONS

CASES = [(70.,20.,.45,.3,.8,1.7,.7,1.2,1.4,1.1),
         (55.,35.,.65,.45,1.1,.6,1.3,2.2,.8,.25),
         (80.,15.,.6,.35,.6,2.1,2.,.3,2.2,2.),
         # Equal-mass adversarial source: at theta/phase below, the PE
         # conversion round-trips to s1=(.7,.1,.1), s2=(-.4,.3,-.4), incl=1.5.
         (50.,50.,.714142842854285,.6403124237432849,
          1.4303066250413763,2.2455372690184494,2.356194490192345,
          5.279420722791896,1.375245667553374,1.2)]
RATE, DURATION, EVENT = 4096, 16., 1e9
FREQ = np.fft.rfftfreq(int(RATE*DURATION), 1/RATE)
PSD = 1e-46*((30/np.maximum(FREQ,1))**4+1+(FREQ/300)**2)
WEIGHT = ((FREQ>=20)&(FREQ<=512))/PSD
START = EVENT-DURATION+2


def parameters(case):
    m1,m2,a1,a2,t1,t2,p12,pjl,theta,phase=case
    p=lsu.ChooseWaveformParams()
    p.m1,p.m2=m1*lal.MSUN_SI,m2*lal.MSUN_SI
    values=ls.SimInspiralTransformPrecessingNewInitialConditions(
        theta,pjl,t1,t2,p12,a1,a2,p.m1,p.m2,20.,phase)
    p.incl,p.s1x,p.s1y,p.s1z,p.s2x,p.s2y,p.s2z=map(float,values)
    p.phiref,p.psi,p.phi,p.theta=phase,1.,2.1,-.4
    p.dist=400e6*lal.PC_SI;p.tref=lal.LIGOTimeGPS(EVENT);p.radec=True
    p.deltaT,p.deltaF,p.fmin,p.fref=1/RATE,1/DURATION,20.,20.
    p.approx=ls.IMRPhenomXPNR
    return p


def native(p,fmin):
    hp,hc=ls.SimInspiralChooseFDWaveform(p.m1,p.m2,p.s1x,p.s1y,p.s1z,
        p.s2x,p.s2y,p.s2z,p.dist,p.incl,p.phiref,0.,0.,0.,p.deltaF,
        fmin,RATE/2,p.fref,_dictionary(p,True),p.approx)
    def pad(h):
        return np.pad(h.data.data,(0,len(FREQ)-len(h.data.data)))
    return {'plus':pad(hp),'cross':pad(hc)}


def bilby_reference(case,theta,phase,p,fmin=20.):
    import bilby
    m1,m2,a1,a2,t1,t2,p12,pjl,_,_=case
    return bilby.gw.source.lal_binary_black_hole(FREQ,mass_1=m1,mass_2=m2,
        luminosity_distance=p.dist/(1e6*lal.PC_SI),a_1=a1,tilt_1=t1,phi_12=p12,
        a_2=a2,tilt_2=t2,phi_jl=pjl,theta_jn=theta,phase=phase,
        waveform_approximant='IMRPhenomXPNR',reference_frequency=20.,minimum_frequency=fmin,
        maximum_frequency=RATE/2,lal_waveform_dictionary=_dictionary(p,True),
        catch_waveform_errors=False)


def condition_native_polarizations(p,polarizations):
    """Independent native polarizations on the same declared conditioned object.

    No source-basis components or angular coefficients enter this oracle. Real
    plus/cross polarizations define both Fourier sides of hplus-i*hcross.
    """
    positive=polarizations['plus']-1j*polarizations['cross']
    negative=polarizations['plus'].conj()-1j*polarizations['cross'].conj()
    values=np.concatenate([negative[:0:-1],positive])
    spectrum=lal.CreateCOMPLEX16FrequencySeries('native polarization oracle',
        lal.LIGOTimeGPS(0),-RATE/2,p.deltaF,lal.DimensionlessUnit,len(values))
    spectrum.data.data[:]=values
    time_series=lsu.condition_fd_mode_components(p,{'native':spectrum},
        global_sign=1,fd_standoff_factor=.9,fd_alignment_postevent_time=2.)['native']
    epoch_phase=np.exp(-2j*np.pi*FREQ*float(time_series.epoch))
    return {'plus':np.fft.rfft(time_series.data.data.real)/RATE*epoch_phase,
            'cross':-np.fft.rfft(time_series.data.data.imag)/RATE*epoch_phase}


def detector_response(polarizations,p,det,offset=0.):
    d=ls.DetectorPrefixToLALDetector(det)
    fp,fc=lal.ComputeDetAMResponse(d.response,p.phi,p.theta,p.psi,
                                 lal.GreenwichMeanSiderealTime(p.tref))
    delay=lal.TimeDelayFromEarthCenter(d.location,p.phi,p.theta,p.tref)
    return (fp*polarizations['plus']+fc*polarizations['cross'])*np.exp(
        -2j*np.pi*FREQ*(EVENT-START+delay+offset))


def logl(data,template):
    return float(4/DURATION*np.sum((data.conj()*template-.5*abs(template)**2).real*WEIGHT))


def metrics(a,b):
    aa=np.sum(abs(a)**2*WEIGHT);bb=np.sum(abs(b)**2*WEIGHT)
    return dict(relative_residual=float(np.sqrt(np.sum(abs(a-b)**2*WEIGHT)/aa)),
                mismatch=float(1-np.sum(a.conj()*b*WEIGHT).real/np.sqrt(aa*bb)))


def vector(samples):
    p=samples[0].manual_copy()
    for name in ('m1','m2','s1x','s1y','s1z','s2x','s2y','s2z',
                 'phi','theta','psi','incl','phiref','dist'):
        setattr(p,name,np.asarray([getattr(q,name) for q in samples]))
    return p


def execute(output,selected,require_requested_start=False):
    rows=[];failures=[];intrinsic_checks=[];requested_start_exceedances=[]
    packages={}
    for package in ('numpy','scipy','lalsuite','bilby','numba','cupy'):
        try:packages[package]=version(package)
        except PackageNotFoundError:packages[package]=None
    for ci in selected:
        begun=time.monotonic();base=parameters(CASES[ci]);injection=native(base,20.)
        data_fd={det:detector_response(injection,base,det) for det in ('H1','L1')}
        data={};psds={}
        for det,fd in data_fd.items():
            ts=lal.CreateCOMPLEX16TimeSeries(det,lal.LIGOTimeGPS(START),0.,1/RATE,
                                            lal.DimensionlessUnit,int(RATE*DURATION))
            ts.data.data[:]=np.fft.irfft(fd,n=int(RATE*DURATION))*RATE
            data[det]=lsu.DataFourier(ts)
            ps=lal.CreateREAL8FrequencySeries(det,lal.LIGOTimeGPS(0),0.,1/DURATION,
                                             lal.SecondUnit,len(PSD))
            ps.data.data[:]=PSD;psds[det]=ps
        basis=XPNRSourceBasis()
        generated={}
        def generator(p,lmax):
            h,hc=basis(p,lmax);generated.update(h);return h,hc
        intrinsic=base.manual_copy();intrinsic.incl=intrinsic.phiref=intrinsic.psi=0.
        _,u,v,rho,_,_=fl.PrecomputeLikelihoodTerms(EVENT,.1,intrinsic,data,psds,4,512.,
            ignore_threshold=None,source_basis_generator=generator,
            skip_interpolation=True,quiet=True,verbose=False)
        packed={det:fl.PackLikelihoodDataStructuresAsArrays(list(rho[det]),None,rho[det],
                     u[det],v[det],source_basis=True) for det in rho}
        arrays=tuple({det:packed[det][i] for det in packed} for i in (0,5,3,4,7))
        print('case',ci,'precompute complete; components',len(generated),'seconds',time.monotonic()-begun,flush=True)
        labels=tuple(generated)
        all_power=sum(np.linalg.norm(h.data.data)**2 for h in generated.values())
        anti_power=sum(np.linalg.norm(h.data.data)**2 for label,h in generated.items()
                       if label[2][0]=='A')
        anti_fraction=float(np.sqrt(anti_power/all_power))
        intrinsic_checks.append(dict(case=ci,component_count=len(labels),
            antisymmetric_component_norm_fraction=anti_fraction,basis_metadata=basis.metadata))
        if anti_fraction<=1e-4:
            failures.append(dict(case=ci,gate='retained antisymmetric components',value=anti_fraction))
        td={label:lsu.DataInverseFourier(h) for label,h in generated.items()}
        epoch=float(next(iter(td.values())).epoch)
        matrix=np.stack([h.data.data for h in td.values()])
        coords=[];samples=[];references=[];bilby_errors=[]
        for pe in (False,True):
            for theta,phase in ((.4,.4),(.8,1.1),(1.4,2.),(2.2,.25)):
                if pe:
                    changed=list(CASES[ci]);changed[-2:]=theta,phase;q=parameters(changed)
                else:
                    q=base.manual_copy();q.incl,q.phiref=theta,phase
                raw20,raw18=native(q,20.),native(q,18.)
                matched18=condition_native_polarizations(q,raw18)
                agreement=None
                if pe:
                    agreement={}
                    for name,start,raw in [('requested20',20.,raw20),('generation18',18.,raw18)]:
                        independent=bilby_reference(CASES[ci],theta,phase,q,start)
                        agreement[name]={k:float(np.linalg.norm(independent[k]-raw[k])/np.linalg.norm(raw[k]))
                                         for k in raw}
                        assert max(agreement[name].values())<1e-12,('Bilby/native PE coordinate mismatch',name,agreement[name])
                for variation in ('base','psi','distance'):
                    p=q.manual_copy()
                    if variation=='psi':p.psi+=.2
                    if variation=='distance':p.dist*=1.1
                    samples.append(p);coords.append((pe,theta,phase,variation))
                    factor=q.dist/p.dist
                    references.append(({k:x*factor for k,x in raw20.items()},
                                       {k:x*factor for k,x in raw18.items()},
                                       {k:x*factor for k,x in matched18.items()}))
                    bilby_errors.append(agreement)
        pvec=vector(samples)
        coefficients=basis.angular_coefficients(labels,pvec.incl,pvec.phiref,P_vec=pvec)
        z=coefficients@matrix
        fourier_epoch=np.exp(-2j*np.pi*FREQ*epoch)
        candidate=[]
        for values,p in zip(z,samples):
            scale=fl.distMpcRef*1e6*lal.PC_SI/p.dist
            candidate.append({'plus':np.fft.rfft(values.real)/RATE*fourier_epoch*scale,
                              'cross':-np.fft.rfft(values.imag)/RATE*fourier_epoch*scale})
        offsets=np.array([-.00173,0.,.0013])
        evaluated=np.asarray(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(
            offsets,pvec,*arrays,Lmax=4,xpy=np,source_angular_basis=basis.angular_coefficients,
            time_interp='cubic',return_lnLt=True,explicit_time_values=True))
        for i,(p,coord,pol,reference,bilby_agreement) in enumerate(zip(samples,coords,candidate,references,bilby_errors)):
            native20,native18,matched18=reference
            likelihood=[]
            for j,offset in enumerate(offsets):
                direct=raw20=raw18=common18=0.
                for det,d in data_fd.items():
                    direct+=logl(d,detector_response(pol,p,det,offset))
                    raw20+=logl(d,detector_response(native20,p,det,offset))
                    raw18+=logl(d,detector_response(native18,p,det,offset))
                    common18+=logl(d,detector_response(matched18,p,det,offset))
                likelihood.append(dict(time_offset=float(offset),noloop=float(evaluated[i,j]),
                    component_direct=direct,native20=raw20,native18=raw18,
                    common_conditioned_native18=common18,
                    common_conditioned_native18_difference=abs(float(evaluated[i,j])-common18),
                    factored_direct_difference=abs(float(evaluated[i,j])-direct),
                    native20_difference=abs(float(evaluated[i,j])-raw20),
                    native18_difference=abs(float(evaluated[i,j])-raw18)))
            row=dict(case=ci,coordinates='PE_reconverted' if coord[0] else 'fixed_Cartesian',
                theta=coord[1],phase=coord[2],variation=coord[3],
                bilby_native_pe_residuals=bilby_agreement['requested20'] if bilby_agreement else None,
                bilby_native_generation_pe_residuals=bilby_agreement['generation18'] if bilby_agreement else None,
                polarization_common18={k:metrics(matched18[k],pol[k]) for k in pol},
                polarization20={k:metrics(native20[k],pol[k]) for k in pol},
                polarization18={k:metrics(native18[k],pol[k]) for k in pol},likelihood=likelihood)
            rows.append(row)
            for waveform_gate in ('polarization20','polarization18','polarization_common18'):
                for polarization,metric in row[waveform_gate].items():
                    if metric['relative_residual']>.02 or metric['mismatch']>1e-4:
                        failures.append(dict(case=ci,coordinates=row['coordinates'],theta=coord[1],
                            phase=coord[2],variation=coord[3],gate=waveform_gate,
                            polarization=polarization,metrics=metric))
            for point in likelihood:
                if point['factored_direct_difference']>.1 or point['common_conditioned_native18_difference']>.1:
                    failures.append(dict(case=ci,coordinates=row['coordinates'],theta=coord[1],
                        phase=coord[2],variation=coord[3],gate='matched-object likelihood',metrics=point))
                if point['native20_difference']>.1:
                    diagnostic=dict(case=ci,coordinates=row['coordinates'],theta=coord[1],
                        phase=coord[2],variation=coord[3],gate='requested-start conditioning diagnostic',metrics=point)
                    requested_start_exceedances.append(diagnostic)
                    if require_requested_start:failures.append(diagnostic)
        print('case',ci,'complete; max factored/direct error',max(
            x['factored_direct_difference'] for r in rows if r['case']==ci for x in r['likelihood']),flush=True)
        receipt=dict(source_repository=dict(
            commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(lsu.__file__).resolve().parents[3],text=True).strip(),
            tree=subprocess.check_output(['git','rev-parse','HEAD^{tree}'],cwd=Path(lsu.__file__).resolve().parents[3],text=True).strip(),
            tracked_dirty=bool(subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=Path(lsu.__file__).resolve().parents[3],text=True).strip())),python=platform.python_version(),numpy=np.__version__,
            platform=platform.platform(),packages=packages,fixtures=CASES,selected_cases=selected,
            lalsimulation_version=ls.__version__,
            native_commit=ls.SimulationVCSInfo.vcsId,options=PRECISION_OPTIONS,
            setup=dict(duration=DURATION,sampling_frequency=RATE,analysis_band=[20,512],
                       reference_frequency=20.,generation_minimum_frequency=18.,
                       likelihood_reference='independent native18 with common fixed conditioning',
                       requested_start_criterion_required=require_requested_start,
                       time_interp='cubic',n_cal=1,phase_marginalization=False,
                       batched_crossterms=os.environ.get('RIFT_PRECOMPUTE_BATCHED_CROSSTERMS','0')),
            sources={name:hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                     for name,module in [('lalsimutils',lsu),('factored_likelihood',fl),
                         ('xpnr_source_basis',basis_module),('SphericalHarmonics_gpu',harmonics_module)]},
            diagnostic_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            thresholds=dict(maximum_residual=.02,maximum_mismatch=1e-4,maximum_delta_logl=.1),
            status='failed' if failures else 'passed',required_gate_failures=failures,
            requested_start_diagnostic_exceedances=requested_start_exceedances,
            intrinsic_checks=intrinsic_checks,results=rows)
        output.write_text(json.dumps(receipt,indent=2,allow_nan=False)+'\n')
    assert not failures,('Required native waveform/matched-object likelihood gates failed; see receipt',len(failures))
    print('PASS',len(rows)*3,'full-mixed-U/V points against common-conditioned native18/Bilby and component direct bins; requested-start diagnostic exceedances',len(requested_start_exceedances),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--cases',type=int,nargs='+',default=list(range(len(CASES))))
    parser.add_argument('--require-requested-start',action='store_true',
                        help='also enforce the original native20 likelihood criterion despite differing conditioning')
    args=parser.parse_args();execute(args.output,args.cases,args.require_requested_start)
