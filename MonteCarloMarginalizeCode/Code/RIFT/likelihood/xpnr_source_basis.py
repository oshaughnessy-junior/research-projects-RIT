"""Intrinsic native-J XPNR components with analytic observer coefficients.

Opt-in precision basis: explicit native coprecessing modes and both multibanding
thresholds zero. Supports default native convention 1. The auxiliary convention6
phase-pi call separates odd/even coprecessing m without fitting waveform phases.
No waveform generation occurs in ``angular_coefficients``.
"""
from dataclasses import dataclass, field
import numpy as np
import lal
import lalsimulation as lalsim
from RIFT import lalsimutils as lsu

MIN_OLD_J_SIN_TILT = 1e-6
MIN_PE_OBSERVER_J_SIN_ANGLE = 1e-6

COPRECESSING_MODES = ((2, 2), (2, 1), (3, 3), (3, 2), (4, 4))
PRECISION_OPTIONS = dict(PhenomXPrecVersion=330, PhenomXPFinalSpinMod=7,
    PhenomXPNRUseTunedAngles=1, PhenomXPNRUseTunedCoprec=1,
    PhenomXPNRUseTunedCoprec33=0, PhenomXPNRForceXHMAlignment=0,
    PhenomXAntisymmetricWaveform=1, PhenomXHMReleaseVersion=122019,
    PhenomXPHMMBandVersion=0, PhenomXHMThresholdMband=0.,
    PhenomXPHMThresholdMband=0., PhenomXPConvention=1)


def _dictionary(P, anti, convention=1):
    d=P.to_lal_dict_extended(extra_args_dict=dict(PRECISION_OPTIONS,
        PhenomXAntisymmetricWaveform=int(anti), PhenomXPConvention=convention))
    modes=lalsim.SimInspiralCreateModeArray()
    for ell, m in COPRECESSING_MODES:
        lalsim.SimInspiralModeArrayActivateMode(modes,ell,m)
        lalsim.SimInspiralModeArrayActivateMode(modes,ell,-m)
    lalsim.SimInspiralWaveformParamsInsertModeArray(d,modes)
    return d


def _native_old_frame(P):
    """Native330 reference J, including spin-dependent 3PN L3/L5 terms."""
    M=P.m1+P.m2; eta=P.m1*P.m2/M**2; de=(P.m1-P.m2)/M
    c1,c2=P.s1z,P.s2z
    v=(np.pi*(M/lal.MSUN_SI)*lal.MTSUN_SI*P.fref)**(1/3)
    L2=1.5+eta/6
    L3=5*(c1*(-2-2*de+eta)+c2*(-2+2*de+eta))/6
    L4=(81+(-57+eta)*eta)/24
    L5=-7*(c1*(72+de*(72-31*eta)+eta*(-121+2*eta))
           +c2*(72+eta*(-121+2*eta)+de*(-72+31*eta)))/144
    L6=(10935+eta*(-62001+eta*(1674+7*eta)+2214*np.pi**2))/1296
    L=eta/v*(1+L2*v**2+L3*v**3+L4*v**4+L5*v**5+L6*v**6)
    J=np.array([(P.m1/M)**2*P.s1x+(P.m2/M)**2*P.s2x,
                (P.m1/M)**2*P.s1y+(P.m2/M)**2*P.s2y,
                (P.m1/M)**2*P.s1z+(P.m2/M)**2*P.s2z+L])
    norm=float(np.linalg.norm(J))
    if not np.all(np.isfinite(J)) or norm<1e-10:
        raise ValueError('degenerate native reference J frame is unsupported by this basis')
    beta=float(np.arccos(np.clip(J[2]/norm,-1.,1.)))
    # Large individual in-plane spins can cancel in the MASS-WEIGHTED J.
    # Native phiJ/alpha0 have special pole branches; the generic observer-gauge
    # derivation does not cover those branches or acos rounding beta to a pole.
    # sqrt(2)*tol covers native per-component atan2tol under every PE rotation.
    if (np.hypot(J[0],J[1])<=np.sqrt(2.)*1e-15 or
        abs(np.sin(beta))<MIN_OLD_J_SIN_TILT):
        raise ValueError('degenerate native reference J frame is unsupported by this basis')
    return beta,float(np.arctan2(J[1],J[0]))


def _geometry(beta,phiJ,incl,phase,xpy=np):
    """Native convention1 kappa/zeta/thetaJN, vectorized on selected backend."""
    si,ci=xpy.sin(incl),xpy.cos(incl)
    az=np.pi/2-phase-phiJ
    nx,ny,nz=si*xpy.cos(az),si*xpy.sin(az),ci
    cb,sb=xpy.cos(beta),xpy.sin(beta)
    xx=cb*nx-sb*nz; yy=ny; zz=sb*nx+cb*nz
    # Native MAX_TOL_ATAN=1e-15, XLALSimIMRPhenomXatan2tol(y,x,tol).
    kap=xpy.where((xpy.abs(xx)<1e-15)&(xpy.abs(yy)<1e-15),0.,xpy.arctan2(yy,xx))
    ck,sk=xpy.cos(kap),xpy.sin(kap)
    # Rotate native waveframe X=(-cos(i)sin(phi),-cos(i)cos(phi),sin(i)).
    xa=-ci*xpy.sin(phase+phiJ); ya=-ci*xpy.cos(phase+phiJ); za=si
    xb=cb*xa-sb*za; yb=ya; zb=sb*xa+cb*za
    xr=ck*xb+sk*yb; yr=-sk*xb+ck*yb
    theta=xpy.arccos(xpy.clip(zz,-1.,1.))
    zeta=xpy.arctan2(yr,xr*xpy.cos(theta)-zb*xpy.sin(theta))
    return kap,zeta,theta


def _series(template,values):
    out=lal.CreateCOMPLEX16FrequencySeries(template.name,template.epoch,
        template.f0,template.deltaF,template.sampleUnits,len(values))
    out.data.data[:]=values
    return out


@dataclass
class XPNRSourceBasis:
    """One intrinsic source, reusable across detectors and extrinsic batches.

    ``generate_raw`` returns native ascending two-sided FD component series.
    A caller must condition every component using the SAME linear operator and
    global_sign=+1. Labels are (ell,m,(kind,mprime,frequency_side)); kind S/A and
    side +/-1. True time-conjugate basis arrays retain the SAME label: the
    conjugation-label involution is identity, with no reflected-m approximation.
    """
    standoff_factor: float=.9
    general_pe: bool=True
    conditioning_options: dict=field(default_factory=dict)
    conditioner: object=None
    _ready: bool=field(default=False,init=False,repr=False)

    def generate_raw(self,P,Lmax=4):
        self._ready=False
        if P.approx != lalsim.IMRPhenomXPNR:
            raise ValueError('XPNRSourceBasis requires IMRPhenomXPNR')
        values=[P.m1,P.m2,P.deltaF,P.deltaT,P.fmin,P.dist,self.standoff_factor]
        if not all(np.isfinite(v) and v>0 for v in values) or self.standoff_factor>1:
            raise ValueError('finite positive deltaF/deltaT/fmin/distance and standoff in (0,1] required')
        if not np.isfinite(P.fref) or P.fref<0:
            raise ValueError('fref must be finite and nonnegative (zero means requested fmin)')
        if P.m1<P.m2:
            raise ValueError('ordered component masses m1>=m2 are required by the source frame')
        if not np.isfinite(Lmax) or Lmax<2:
            raise ValueError('Lmax must include the quadrupole modes')
        Q=P.manual_copy();Q.incl=Q.phiref=Q.psi=0.
        Q.fref=Q.fref or Q.fmin
        self.parameters=Q
        self.beta_old,self.phiJ=_native_old_frame(Q)
        generation_min=Q.fmin*self.standoff_factor
        # This public wrapper explicitly refuses an internal SpinTaylor→MSA
        # fallback. Use it only as a once-per-intrinsic preflight, never as the
        # native mode route's angle/gauge oracle. Explicit ModeArray makes its
        # integration bounds match the requested common native mode setup.
        try:
            lalsim.SimIMRPhenomXPSpinTaylorAngles(Q.m1,Q.m2,Q.s1x,Q.s1y,Q.s1z,
                Q.s2x,Q.s2y,Q.s2z,generation_min,.5/Q.deltaT,Q.deltaF,
                Q.fref,0.,_dictionary(Q,True))
        except (RuntimeError,AttributeError) as exc:
            raise ValueError('native330 SpinTaylor preflight failed; refusing fallback basis') from exc
        _,beta,_,freq,_,_=lalsim.SimIMRPhenomX_PNR_GeneratePNRAnglesHM(
            Q.m1,Q.m2,Q.s1x,Q.s1y,Q.s1z,Q.s2x,Q.s2y,Q.s2z,
            0.,Q.deltaF,generation_min,.5/Q.deltaT,Q.fref,2,2,_dictionary(Q,True))
        self.beta_new=float(np.interp(Q.fref,freq.data,beta.data))
        self.kappa_old0,_,_=_geometry(self.beta_old,self.phiJ,0.,0.)
        self.kappa_new0,self.zeta0,_=_geometry(self.beta_new,self.phiJ,0.,0.)
        self.reference_spin=('s1' if np.hypot(Q.s1x,Q.s1y)>=np.hypot(Q.s2x,Q.s2y) else 's2')
        if max(np.hypot(Q.s1x,Q.s1y),np.hypot(Q.s2x,Q.s2y))<1e-3:
            raise ValueError('This generic precessing basis excludes the native aligned/fallback branch')
        def modes(phase,anti,convention=1):
            h=lalsim.SimInspiralChooseFDModes(Q.m1,Q.m2,Q.s1x,Q.s1y,Q.s1z,
                Q.s2x,Q.s2y,Q.s2z,Q.deltaF,generation_min,.5/Q.deltaT,
                Q.fref,phase,Q.dist,0.,_dictionary(Q,anti,convention),Q.approx)
            return lsu.SphHarmFrequencySeries_to_dict(h,min(Lmax,4))
        symmetric=modes(0.,False);full=modes(0.,True)
        shifted=modes(np.pi,False,6) if self.general_pe else None
        out={}
        for lm,template in symmetric.items():
            ell,m=lm; s=template.data.data
            if self.general_pe and ell in (2,3):
                even=.5*(s+shifted[lm].data.data);odd=s-even
                pieces=[('S',2,even),('S',1 if ell==2 else 3,odd)]
            else:
                pieces=[('S',0 if ell in (2,3) else 4,s)]
            if ell==2:pieces.append(('A',2,full[lm].data.data-s))
            # ChooseFDModes data are ascending [-Nyq,+Nyq], despite f0=0 metadata.
            if len(s)%2!=1 or len(s)!=2*round((.5/Q.deltaT)/Q.deltaF)+1:
                raise ValueError('unexpected native centered frequency grid')
            f=(np.arange(len(s))-len(s)//2)*template.deltaF
            for kind,mprime,values in pieces:
                for side in (1,-1):
                    label=(ell,m,(kind,mprime,side))
                    out[label]=_series(template,values*(side*f>0))
        self.labels=tuple(out)
        self.basis_conjugation={label:label for label in self.labels}
        self.metadata=dict(options=dict(PRECISION_OPTIONS),
            coprecessing_modes=[list(mode) for mode in COPRECESSING_MODES],
            general_pe=self.general_pe,reference_distance=float(Q.dist),
            reference_frequency=float(Q.fref),generation_minimum=float(generation_min),
            generation_nyquist=float(.5/Q.deltaT),beta_old=self.beta_old,
            beta_new=self.beta_new,phiJ=self.phiJ,
            native_commit=lalsim.SimulationVCSInfo.vcsId)
        self._ready=True
        return out

    def __call__(self,P,Lmax=4):
        """Precompute callback: condition components, return FD and true-conjugate FD.

        The shared RIFT conditioner is injected or supplied by lalsimutils. All
        components, already split by native frequency support, pass through the
        identical linear conditioning operator exactly once.
        """
        components=self.generate_raw(P,Lmax)
        conditioner=self.conditioner or getattr(lsu,'condition_fd_mode_components',None)
        if conditioner is None:
            raise RuntimeError('shared condition_fd_mode_components helper is required')
        options=dict(fd_standoff_factor=self.standoff_factor,
                     fd_alignment_postevent_time=2.)
        options.update(self.conditioning_options)
        if options['fd_standoff_factor']!=self.standoff_factor:
            raise ValueError('generation and conditioning standoff must match')
        time_components=conditioner(self.parameters,components,global_sign=1,**options)
        standard={};conjugate={}
        for label,t in time_components.items():
            standard[label]=lsu.DataFourier(t)
            tc=lal.CreateCOMPLEX16TimeSeries(t.name,t.epoch,t.f0,t.deltaT,
                                            t.sampleUnits,t.data.length)
            tc.data.data[:]=np.conj(t.data.data)
            conjugate[label]=lsu.DataFourier(tc)
        return standard,conjugate

    def angular_coefficients(self,labels,incl,phiref,xpy=np,l_max=4,P_vec=None):
        """Analytic complex source coefficients; no generation or interpolation.

        P_vec optionally supplies re-converted Cartesian spins for physical PE
        thetaJN/phase samples. Both spins must differ from the basis spins by the
        same rotation about L. If omitted, the maintained fixed-Cartesian RIFT
        parameterization has delta=0. The NoLoop hook owns shape/broadcasting.
        """
        if not self._ready:
            raise RuntimeError('generate_raw must initialize this intrinsic basis first')
        from RIFT.likelihood.SphericalHarmonics_gpu import SphericalHarmonicsVectorized
        incl,phiref=xpy.broadcast_arrays(xpy.atleast_1d(incl),xpy.atleast_1d(phiref))
        if incl.ndim!=1:raise ValueError('one-dimensional extrinsic batches required')
        if bool(xpy.any(~xpy.isfinite(incl))) or bool(xpy.any(~xpy.isfinite(phiref))):
            raise ValueError('finite source observer angles required')
        delta=xpy.zeros_like(incl)
        if P_vec is not None:
            def field_value(name):
                return P_vec.get(name) if isinstance(P_vec,dict) else getattr(P_vec,name,None)
            sx,sy=field_value(self.reference_spin+'x'),field_value(self.reference_spin+'y')
            if (sx is None)!=(sy is None):
                raise ValueError('both in-plane Cartesian spin fields required')
            if sx is not None:
                delta=xpy.broadcast_to(xpy.arctan2(xpy.asarray(sy),xpy.asarray(sx))-
                    np.arctan2(getattr(self.parameters,self.reference_spin+'y'),
                               getattr(self.parameters,self.reference_spin+'x')),incl.shape)
            if bool(xpy.any(xpy.abs(xpy.sin(delta/2))>1e-12)):
                if any(field_value(name) is None for name in ('s1x','s1y','s1z','s2x','s2y','s2z')):
                    raise ValueError('PE rotation reuse requires all Cartesian fields of both spins')
            # Reuse is valid only for unchanged intrinsic scalars and a COMMON
            # rotation of BOTH spins. This also rejects changed spin magnitudes.
            fref=field_value('fref')
            if fref is not None:
                normalized=xpy.where(xpy.asarray(fref)==0.,self.parameters.fmin,xpy.asarray(fref))
                if bool(xpy.any(~xpy.isfinite(normalized))) or bool(xpy.any(xpy.abs(normalized-self.parameters.fref)>1e-12*max(self.parameters.fref,1.))):
                    raise ValueError('basis reuse changes intrinsic spin reference frequency')
            for name in ('m1','m2','s1z','s2z'):
                value=field_value(name)
                if value is not None and (bool(xpy.any(~xpy.isfinite(xpy.asarray(value)))) or bool(xpy.any(xpy.abs(xpy.asarray(value)-getattr(self.parameters,name))>
                    1e-11*max(abs(getattr(self.parameters,name)),1.)))):
                    raise ValueError('basis reuse changes intrinsic '+name)
            for spin in ('s1','s2'):
                xx,yy=field_value(spin+'x'),field_value(spin+'y')
                if (xx is None)!=(yy is None):raise ValueError('both spin x/y fields required')
                if xx is not None:
                    if bool(xpy.any(~xpy.isfinite(xpy.asarray(xx)))) or bool(xpy.any(~xpy.isfinite(xpy.asarray(yy)))):
                        raise ValueError('finite Cartesian spin fields required')
                    x0,y0=getattr(self.parameters,spin+'x'),getattr(self.parameters,spin+'y')
                    expected_x=x0*xpy.cos(delta)-y0*xpy.sin(delta)
                    expected_y=x0*xpy.sin(delta)+y0*xpy.cos(delta)
                    if bool(xpy.any(xpy.abs(xpy.asarray(xx)-expected_x)>1e-10)) or bool(xpy.any(xpy.abs(xpy.asarray(yy)-expected_y)>1e-10)):
                        raise ValueError('both spins must share a rigid rotation about L')
            if not self.general_pe and bool(xpy.any(xpy.abs(xpy.sin(delta/2))>1e-12)):
                raise ValueError('PE Cartesian spin reconversion requires general_pe basis')
        phiJ=self.phiJ+delta
        old,_,_=_geometry(self.beta_old,phiJ,incl,phiref,xpy)
        new,zeta,theta=_geometry(self.beta_new,phiJ,incl,phiref,xpy)
        # Native beta changes at numerical roundoff under a PE spin rotation.
        # Near N || J_new this moves kappa across its singular gauge and can
        # flip native AS phase. Do not claim an intrinsic bank reproduces that
        # ill-conditioned numerical branch without per-extrinsic regeneration.
        if bool(xpy.any((xpy.abs(xpy.sin(delta/2))>1e-12)&
                       (xpy.abs(xpy.sin(theta))<MIN_PE_OBSERVER_J_SIN_ANGLE))):
            raise ValueError('PE observer at the native PNR J-frame pole is unsupported')
        dnew=new-self.kappa_new0
        das=zeta-self.zeta0+2*dnew-(old-self.kappa_old0)
        physical=np.asarray([(int(label[0]),int(label[1])) for label in labels],dtype=np.int64)
        harmonics=SphericalHarmonicsVectorized(physical,theta,xpy.zeros_like(theta),xpy=xpy,l_max=min(l_max,4))
        m=xpy.asarray([label[1] for label in labels])
        mp=xpy.asarray([label[2][1] for label in labels])
        side=xpy.asarray([label[2][2] for label in labels])
        anti=xpy.asarray([label[2][0]=='A' for label in labels])
        exponent=m[None,:]*dnew[:,None]-side[None,:]*mp[None,:]*delta[:,None]
        exponent=exponent+anti[None,:]*side[None,:]*das[:,None]+2*zeta[:,None]
        return harmonics*xpy.exp(1j*exponent)
