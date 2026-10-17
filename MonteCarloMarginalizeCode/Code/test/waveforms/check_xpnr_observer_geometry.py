#!/usr/bin/env python3
"""Unfitted XPNR observer-gauge diagnostic; no production likelihood correction.

Fixed Cartesian spins, native330 defaults, separate symmetric/antisymmetric
components. Import helper next to this script and use selected RIFT on PYTHONPATH.
"""
import argparse,json
from pathlib import Path
import numpy as np
import lal,lalsimulation as ls
from RIFT import lalsimutils as lsu
from check_xpnr_phase_covariance import parameters, CASES, DEFAULTS, COPRECESSING_MODES, norm

def dictionary(P,anti):
 from RIFT.likelihood.xpnr_source_basis import _dictionary
 return _dictionary(P,anti)
def beta_ref(P):
 _,be,_,f,_,_=ls.SimIMRPhenomX_PNR_GeneratePNRAnglesHM(P.m1,P.m2,P.s1x,P.s1y,P.s1z,P.s2x,P.s2y,P.s2z,P.incl,P.deltaF,18.,1024.,20.,2,2,dictionary(P,True))
 return float(np.interp(20.,f.data,be.data))
def modes(P,anti):
 h=ls.SimInspiralChooseFDModes(P.m1,P.m2,P.s1x,P.s1y,P.s1z,P.s2x,P.s2y,P.s2z,P.deltaF,18.,1024.,20.,P.phiref,P.dist,P.incl,dictionary(P,anti),P.approx)
 raw={lm:a.data.data.copy() for lm,a in lsu.SphHarmFrequencySeries_to_dict(h,4).items()}
 _,_,gamma=lsu.extract_JL_angles(P);beta=beta_ref(P);phiJ=np.pi-gamma;kappa,_=geometry(beta,phiJ,P.incl,P.phiref);alpha=np.pi-kappa
 out={}
 for ell in range(2,5):
  vec=np.stack([raw[(ell,m)] for m in range(-ell,ell+1)])
  D=np.array([[lal.WignerDMatrix(ell,M,m,-gamma-np.pi/2,-beta,-alpha) for m in range(-ell,ell+1)] for M in range(-ell,ell+1)])
  for idx,m in enumerate(range(-ell,ell+1)):out[(ell,m)]=(D@vec)[idx]
 return out
def native_beta(P):
 M=P.m1+P.m2;eta=P.m1*P.m2/M**2;de=(P.m1-P.m2)/M;ch1=P.s1z;ch2=P.s2z;v=(np.pi*(M/lal.MSUN_SI)*lal.MTSUN_SI*P.fref)**(1/3)
 L2=1.5+eta/6
 L3=5*(ch1*(-2-2*de+eta)+ch2*(-2+2*de+eta))/6
 L4=(81+(-57+eta)*eta)/24
 L5=-7*(ch1*(72+de*(72-31*eta)+eta*(-121+2*eta))+ch2*(72+eta*(-121+2*eta)+de*(-72+31*eta)))/144
 L6=(10935+eta*(-62001+eta*(1674+7*eta)+2214*np.pi**2))/1296
 L=eta/v*(1+L2*v**2+L3*v**3+L4*v**4+L5*v**5+L6*v**6)
 J=np.array([(P.m1/M)**2*P.s1x+(P.m2/M)**2*P.s2x,(P.m1/M)**2*P.s1y+(P.m2/M)**2*P.s2y,(P.m1/M)**2*P.s1z+(P.m2/M)**2*P.s2z+L])
 return np.arccos(J[2]/np.linalg.norm(J)),np.arctan2(J[1],J[0])
def geometry(beta,phiJ,inc,ph):
 N=np.array([np.sin(inc)*np.cos(np.pi/2-ph),np.sin(inc)*np.sin(np.pi/2-ph),np.cos(inc)]);X=np.array([-np.cos(inc)*np.sin(ph),-np.cos(inc)*np.cos(ph),np.sin(inc)])
 def rz(v,ang):return np.array([v[0]*np.cos(ang)-v[1]*np.sin(ang),v[0]*np.sin(ang)+v[1]*np.cos(ang),v[2]])
 def ry(v,ang):return np.array([v[0]*np.cos(ang)+v[2]*np.sin(ang),v[1],-v[0]*np.sin(ang)+v[2]*np.cos(ang)])
 Nr=ry(rz(N,-phiJ),-beta);kap=np.arctan2(Nr[1],Nr[0]);Nr=rz(Nr,-kap);Xr=rz(ry(rz(X,-phiJ),-beta),-kap);theta=np.arccos(Nr[2]);zeta=np.arctan2(Xr[1],np.dot(Xr,np.array([np.cos(theta),0,-np.sin(theta)])));return kap,zeta

def execute(output):
 rows=[]
 for ci,case in enumerate(CASES):
  P=parameters(case);bo,phiJ=native_beta(P);bn=beta_ref(P)
  P.incl=P.phiref=0.;S0=modes(P,False);A0={lm:h-S0[lm] for lm,h in modes(P,True).items()};ko0,_=geometry(bo,phiJ,0,0);kn0,zn0=geometry(bn,phiJ,0,0)
  for inc in [.4,.8,1.4,2.2]:
   for ph in [.4,1.1,2.]:
    P.incl=inc;P.phiref=ph;S=modes(P,False);A={lm:h-S[lm] for lm,h in modes(P,True).items()};freq=np.linspace(-1024,1024,len(next(iter(A.values()))))
    ko,_=geometry(bo,phiJ,inc,ph);kn,zn=geometry(bn,phiJ,inc,ph);delta=zn-zn0+2*(kn-kn0)-(ko-ko0)
    errors={}
    for side,mask,sign in [('positive',freq>0,1),('negative',freq<0,-1)]:
     errors[side]=float(norm({lm:A[lm]-A0[lm]*np.exp(sign*1j*delta) for lm in A},mask)/norm(A,mask))
    row=dict(case=ci,incl=inc,phase=ph,delta=float(delta),residuals=errors);rows.append(row);print(row,flush=True)
 receipt=dict(rift_import=str(Path(lsu.__file__).resolve()),lalsimulation_version=ls.__version__,lalsimulation_commit=ls.SimulationVCSInfo.vcsId,defaults=DEFAULTS,coprecessing_modes=COPRECESSING_MODES,cases=CASES,results=rows)
 output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(receipt,indent=2)+'\n')
 print('Observer geometry diagnostic complete; residuals are measured, not a production acceptance gate',flush=True)
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);execute(parser.parse_args().output)
