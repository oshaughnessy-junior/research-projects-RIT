#!/usr/bin/env python3
"""Validate intrinsic XPNR components against native LAL and Bilby polarizations."""
import argparse,json
from pathlib import Path
import numpy as np
import lal,lalsimulation as ls
import bilby
from RIFT.likelihood.xpnr_source_basis import XPNRSourceBasis,_dictionary,PRECISION_OPTIONS
from check_xpnr_phase_covariance import CASES,parameters


def polarizations(h):
    pos=np.arange(len(h))>=len(h)//2
    conjugate=np.conj(h[::-1])
    return dict(plus=.5*(h+conjugate)[pos],cross=.5j*(h-conjugate)[pos])


def execute(output):
    rows=[]
    for ci,case in enumerate(CASES):
        base=parameters(case);basis=XPNRSourceBasis();modes=basis.generate_raw(base)
        labels=tuple(modes);matrix=np.stack([a.data.data for a in modes.values()])
        freq=np.arange(matrix.shape[1]//2+1)*base.deltaF
        for pe in [False,True]:
            targets=[];coords=[]
            for theta,phase in [(.4,.4),(.8,1.1),(1.4,2.),(2.2,.25)]:
                coords.append((theta,phase))
                if pe:
                    changed=list(case);changed[-2:]=theta,phase;targets.append(parameters(changed))
                else:
                    Q=base.manual_copy();Q.incl=theta;Q.phiref=phase;targets.append(Q)
            P_vec={s:np.array([getattr(Q,s) for Q in targets]) for s in ['s1x','s1y','s1z','s2x','s2y','s2z']}
            coefficients=basis.angular_coefficients(labels,np.array([Q.incl for Q in targets]),
                np.array([Q.phiref for Q in targets]),P_vec=P_vec)
            for Q,(theta,phase),h in zip(targets,coords,coefficients@matrix):
                reconstructed=polarizations(h)
                hp,hc=ls.SimInspiralChooseFDWaveform(Q.m1,Q.m2,Q.s1x,Q.s1y,Q.s1z,
                    Q.s2x,Q.s2y,Q.s2z,Q.dist,Q.incl,Q.phiref,0.,0.,0.,Q.deltaF,
                    18.,1024.,20.,_dictionary(Q,True),Q.approx)
                native=dict(plus=hp.data.data,cross=hc.data.data)
                errors={k:float(np.linalg.norm(reconstructed[k]-native[k])/np.linalg.norm(native[k])) for k in native}
                bilby_errors=None
                if pe:
                    m1,m2,a1,a2,t1,t2,p12,pjl,_,_=case
                    reference=bilby.gw.source.lal_binary_black_hole(freq,mass_1=m1,mass_2=m2,luminosity_distance=400.,a_1=a1,tilt_1=t1,phi_12=p12,a_2=a2,tilt_2=t2,phi_jl=pjl,theta_jn=theta,phase=phase,
                        waveform_approximant='IMRPhenomXPNR',reference_frequency=20.,minimum_frequency=18.,
                        maximum_frequency=1024.,lal_waveform_dictionary=_dictionary(Q,True),catch_waveform_errors=False)
                    bilby_errors={k:float(np.linalg.norm(reference[k]-native[k])/np.linalg.norm(native[k])) for k in native}
                    assert max(bilby_errors.values())<1e-12,bilby_errors
                if not pe:assert max(errors.values())<1e-9,errors
                row=dict(case=ci,coordinates='PE_reconverted' if pe else 'fixed_Cartesian',theta=theta,phase=phase,
                    residuals=errors,bilby_native_residuals=bilby_errors)
                rows.append(row);print(row,flush=True)
    receipt=dict(rift_source=str(Path(__import__('RIFT').__file__).resolve()),native_commit=ls.SimulationVCSInfo.vcsId,
        options=PRECISION_OPTIONS,modes=[list(lm) for lm in __import__('RIFT.likelihood.xpnr_source_basis',fromlist=['COPRECESSING_MODES']).COPRECESSING_MODES],
        cases=CASES,results=rows)
    output.write_text(json.dumps(receipt,indent=2)+'\n')
    print('PASS fixed-Cartesian precision and Bilby/native agreement; PE source-rotation residuals measured separately',flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    execute(parser.parse_args().output)
