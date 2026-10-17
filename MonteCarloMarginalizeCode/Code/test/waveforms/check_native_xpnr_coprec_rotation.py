#!/usr/bin/env python3
"""Raw-LAL orbit-averaged symmetric coprecessing XPNR rotation contract.

No RIFT/Bilby import, conditioning, fitted phase, or mode removal. A native (3,2)
numerical covariance failure stays visible; --assert-precision makes it red.
The checked contract is specific to this symmetric orbit-averaged native model,
not a statement that arbitrary physical waveforms ignore in-plane spin angles.
"""
import argparse,json
from pathlib import Path
from types import SimpleNamespace
import numpy as np
import lal,lalsimulation as ls

OPTIONS=dict(PhenomXPrecVersion=330,PhenomXPFinalSpinMod=7,
 PhenomXPNRUseTunedAngles=1,PhenomXPNRUseTunedCoprec=1,
 PhenomXPNRUseTunedCoprec33=0,PhenomXPNRForceXHMAlignment=0,
 PhenomXAntisymmetricWaveform=0,PhenomXHMReleaseVersion=122019,
 PhenomXHMThresholdMband=0.,PhenomXPHMThresholdMband=0.,
 PhenomXPHMMBandVersion=0,PhenomXPHMPrecModes=1,PhenomXPConvention=1)
MODES=((2,1),(2,2),(3,2),(3,3),(4,4))
CASES=((70.,20.,.45,.3,.8,1.7,.7,1.2,1.4,1.1),
       (55.,35.,.65,.45,1.1,.6,1.3,2.2,.8,.25),
       (80.,15.,.6,.35,.6,2.1,2.,.3,2.2,2.))

def parameters(case):
 m1,m2,a1,a2,t1,t2,p12,pjl,theta,phase=case
 spin=ls.SimInspiralTransformPrecessingNewInitialConditions(theta,pjl,t1,t2,p12,a1,a2,
  m1*lal.MSUN_SI,m2*lal.MSUN_SI,20.,phase)
 return SimpleNamespace(m1=m1*lal.MSUN_SI,m2=m2*lal.MSUN_SI,s1x=float(spin[1]),s1y=float(spin[2]),
  s1z=float(spin[3]),s2x=float(spin[4]),s2y=float(spin[5]),s2z=float(spin[6]))

def raw(P):
 d=lal.CreateDict()
 for key,value in OPTIONS.items():getattr(ls,'SimInspiralWaveformParamsInsert'+key)(d,value)
 co=ls.SimInspiralCreateModeArray();inertial=ls.SimInspiralCreateModeArray()
 for ell,mp in MODES:
  ls.SimInspiralModeArrayActivateMode(co,ell,mp);ls.SimInspiralModeArrayActivateMode(co,ell,-mp)
  ls.SimInspiralModeArrayActivateMode(inertial,ell,-mp)
 ls.SimInspiralWaveformParamsInsertModeArray(d,co)
 ls.SimInspiralWaveformParamsInsertModeArrayJframe(d,inertial)
 h=ls.SimInspiralChooseFDModes(P.m1,P.m2,P.s1x,P.s1y,P.s1z,P.s2x,P.s2y,P.s2z,
  1/16,18.,1024.,20.,0.,400e6*lal.PC_SI,0.,d,ls.IMRPhenomXPNR)
 return {(ell,-mp):ls.SphHarmFrequencySeriesGetMode(h,ell,-mp).data.data.copy() for ell,mp in MODES}

def execute(output,assert_precision):
 rows=[]
 for ci,case in enumerate(CASES):
  P=parameters(case);Q=SimpleNamespace(**vars(P));delta=-.9
  for pre in ['s1','s2']:
   z=(getattr(P,pre+'x')+1j*getattr(P,pre+'y'))*np.exp(1j*delta)
   setattr(Q,pre+'x',float(z.real));setattr(Q,pre+'y',float(z.imag))
  reference=raw(P);rotated=raw(Q)
  for lm,a in reference.items():
   freq=(np.arange(len(a))-len(a)//2)/16;mask=freq>0
   reference_norm=float(np.linalg.norm(a[mask]))
   assert np.all(np.isfinite(a)) and np.all(np.isfinite(rotated[lm])) and reference_norm>0.,'Native mode must have finite nonzero power'
   residual=float(np.linalg.norm(rotated[lm][mask]-a[mask])/reference_norm)
   row=dict(case=ci,lm=lm,rotation=delta,reference_norm=reference_norm,residual=residual);rows.append(row);print(row,flush=True)
 failures=[row for row in rows if row['residual']>=1e-7]
 receipt=dict(status='FAIL' if failures else 'PASS',precision_tolerance=1e-7,native_commit=ls.SimulationVCSInfo.vcsId,options=OPTIONS,co_modes=MODES,cases=CASES,results=rows,failures=failures)
 output.write_text(json.dumps(receipt,indent=2)+'\n')
 if assert_precision:
  assert max(row['residual'] for row in rows)<1e-7,'Raw native coprecessing rotation contract failed; see receipt'

if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
 parser.add_argument('--assert-precision',action='store_true');args=parser.parse_args();execute(args.output,args.assert_precision)
