#!/usr/bin/env python3
"""Characterize native XPNR phase covariance; does not propose a production fix.

Run with the selected RIFT source on PYTHONPATH and Python -B. Parameters are
fixed explicitly; no dependence on a mutable cross-code harness checkout.
"""
import argparse
import json
from pathlib import Path
import numpy as np
import lal
import lalsimulation as ls
from RIFT import lalsimutils as lsu

from RIFT.likelihood.xpnr_source_basis import PRECISION_OPTIONS as DEFAULTS, COPRECESSING_MODES
CASES = [
    (70.,20.,.45,.3,.8,1.7,.7,1.2,1.4,1.1),
    (55.,35.,.65,.45,1.1,.6,1.3,2.2,.8,.25),
    (80.,15.,.6,.35,.6,2.1,2.,.3,2.2,2.),
]

def parameters(case):
    m1,m2,a1,a2,t1,t2,phi12,phiJL,thetaJN,phase=case
    P=lsu.ChooseWaveformParams()
    P.m1,P.m2=m1*lal.MSUN_SI,m2*lal.MSUN_SI
    spin=ls.SimInspiralTransformPrecessingNewInitialConditions(
        thetaJN,phiJL,t1,t2,phi12,a1,a2,P.m1,P.m2,20.,phase)
    P.incl,P.s1x,P.s1y,P.s1z,P.s2x,P.s2y,P.s2z=map(float,spin)
    P.dist=400e6*lal.PC_SI;P.phiref=phase
    P.fmin=P.fref=20.;P.deltaT=1/2048;P.deltaF=1/16;P.approx=ls.IMRPhenomXPNR
    return P

def raw(P,phase,anti):
    from RIFT.likelihood.xpnr_source_basis import _dictionary
    d=_dictionary(P,anti)
    h=ls.SimInspiralChooseFDModes(P.m1,P.m2,P.s1x,P.s1y,P.s1z,
        P.s2x,P.s2y,P.s2z,P.deltaF,18.,1024.,20.,phase,P.dist,0.,d,P.approx)
    return {lm:a.data.data.copy() for lm,a in lsu.SphHarmFrequencySeries_to_dict(h,4).items()}

def norm(modes,mask):
    return np.sqrt(sum(np.linalg.norm(a[mask])**2 for a in modes.values()))

def execute(output):
    rows=[]
    for case_index,case in enumerate(CASES):
        P=parameters(case)
        S0=raw(P,0.,False);H0=raw(P,0.,True);A0={lm:H0[lm]-S0[lm] for lm in H0}
        freq=np.linspace(-1024.,1024.,len(next(iter(H0.values()))))
        for phase in [.4,1.1,2.]:
            S=raw(P,phase,False);H=raw(P,phase,True);A={lm:H[lm]-S[lm] for lm in H}
            for side,mask,power in [('negative',freq<0,1),('positive',freq>0,-1)]:
                factor=np.exp(1j*power*phase)
                symmetric_error=norm({lm:S[lm]-S0[lm] for lm in S},mask)/norm(S,mask)
                antisymmetric_error=norm({lm:A[lm]-factor*A0[lm] for lm in A},mask)/norm(A,mask)
                naive_error=norm({lm:H[lm]-H0[lm] for lm in H},mask)/norm(H,mask)
                predicted_naive=abs(factor-1)*norm(A0,mask)/norm(H,mask)
                assert symmetric_error < 1e-10,(case_index,phase,side,symmetric_error)
                assert antisymmetric_error < 1e-10,(case_index,phase,side,antisymmetric_error)
                assert np.isclose(naive_error,predicted_naive,rtol=1e-9,atol=1e-12)
                assert naive_error > 1e-4,"Fixture must resolve the missing asymmetric phase term"
                rows.append(dict(case=case_index,phase=phase,side=side,
                    symmetric_error=float(symmetric_error),antisymmetric_error=float(antisymmetric_error),
                    naive_error=float(naive_error),predicted_naive=float(predicted_naive)))
    receipt=dict(rift_import=str(Path(lsu.__file__).resolve()),lalsimulation_version=ls.__version__,
        lalsimulation_commit=ls.SimulationVCSInfo.vcsId,defaults=DEFAULTS,
        coprecessing_modes=COPRECESSING_MODES,cases=CASES,results=rows)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt,indent=2)+'\n')
    print('PASS',len(rows),'native covariance controls; current fixed-mode assumption misses the retained AS phase term',flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='JSON receipt outside the production checkout')
    execute(parser.parse_args().output)
