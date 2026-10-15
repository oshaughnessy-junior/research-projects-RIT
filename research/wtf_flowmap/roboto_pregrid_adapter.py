"""Opt-in research adapter reusing RIFT PR261/274; no interpolation reimplementation."""
import types
import numpy as np
import jax.numpy as jnp
import lal
from RIFT.likelihood.jax_ile import core
from RIFT.likelihood import factored_likelihood as fl

class RobotoPregrid:
    def __init__(self,context,factor):
        original=context['bank']['like'].data;ref=context['bank']['reference']
        closure=dict(zip(ref.__code__.co_freevars,[c.cell_contents for c in ref.__closure__]))
        if set(closure)!={'banks','p','packed'}:raise ValueError('unreviewed reference-bank contract')
        banks=closure['banks'];self.template=closure['p'];self.factor=int(factor)
        packed={}
        for name in original.detector_names:
            d=original.detectors[name]
            np.testing.assert_array_equal(np.asarray(d['Q']).T,banks[1][name])
            packed[name]=dict(lms=np.asarray(d['lms']),rholmArray=banks[1][name],U=banks[2][name],V=banks[3][name],epoch=float(banks[4][name]))
        # Existing native factory calls the SAME reflected Q builder as NoLoop.
        self.data=core.build_likelihood_data(packed,original.deltaT,self.template.tref,np.asarray(original.tvals),distMpcRef=original.distMpcRef,q_time_pregrid_factor=self.factor)
        self.banks=[banks[0],{name:np.asarray(self.data.detectors[name]['Q']).T for name in original.detector_names},banks[2],banks[3],banks[4]]
        self.reports={name:self.data.detectors[name]['q_time_pregrid_report'] for name in original.detector_names}
        assert self.data.q_time_pregrid_factor==self.factor
        self.nodes=self.data.npts+2048
    def primitive(self,q):
        vals=[q[i:i+1] for i in range(6)]
        k,r=core._accumulate_unit(self.data,*vals[:5],interp='cubic',phase_marginalization=False,guard=1024)
        inv=self.data.distMpcRef/q[5]
        return jnp.stack((k[0].real*inv,k[0].imag*inv,r[0]*inv**2))
    def independent_field(self,q,times):
        q=np.asarray(q);p=self.template
        pv=types.SimpleNamespace(phi=q[0:1],theta=q[1:2],psi=q[2:3],incl=q[3:4],phiref=q[4:5],dist=q[5:6]*1e6*lal.PC_SI,tref=p.tref,deltaT=p.deltaT,radec=True)
        return np.asarray(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(np.asarray(times),pv,*self.banks,Lmax=4,xpy=np,time_interp='cubic',q_deltaT=p.deltaT/self.factor,return_lnLt=True,explicit_time_values=True,n_cal=1))[0]
