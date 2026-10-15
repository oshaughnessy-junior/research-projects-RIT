"""Research-only driver contracts for current AV/GMM proposals.

Fixed-N adaptive importance sampling uses each draw's conditional mixture q.
Oracle rows never enter this estimator. All densities are on [0,1]^6; AV draws
use its native live-bin law iid, not its deliberately non-density legacy ps.
"""
import math
import numpy as np
from scipy.special import logsumexp,ndtr
from scipy.integrate import quad

PARAMS=['ra_unit','sindec_unit','psi_unit','cosincl_unit','phase_unit','distance_unit']
GROUPS=[(0,1),(2,4),(3,5)]


class FixedNEstimator:
    def __init__(self,n=1000):self.n=n;self.points=[];self.logweights=[];self.labels=[]
    def append(self,x,log_target,q,labels,phase):
        if phase!='evidence':raise ValueError('only evidence draws may enter estimator')
        x=np.asarray(x);log_target=np.asarray(log_target);q=np.asarray(q);labels=np.asarray(labels)
        if x.ndim!=2 or x.shape[1]!=6 or not np.isfinite(x).all() or np.any((x<0)|(x>1)):
            raise ValueError('invalid evidence coordinates')
        if any(v.shape!=(len(x),) for v in [log_target,q,labels]):raise ValueError('evidence row alignment')
        if sum(len(v) for v in self.points)+len(x)>self.n:raise ValueError('fixed evidence population exceeded')
        if np.any(q<=0) or not np.isfinite(q).all():raise ValueError('invalid proposal density')
        self.points.append(np.array(x,copy=True));self.logweights.append(np.asarray(log_target)-np.log(q));self.labels.append(np.array(labels))
    def result(self,require_complete=True):
        if not self.points:raise ValueError('empty evidence population')
        x=np.vstack(self.points);lw=np.concatenate(self.logweights)
        if require_complete and len(x)!=self.n:raise ValueError('fixed evidence population incomplete')
        if np.isnan(lw).any() or np.isposinf(lw).any():raise ValueError('invalid evidence weight')
        if not np.isfinite(lw).any():raise ValueError('zero target mass; posterior undefined')
        # -inf rows are genuine zeros and remain in the population denominator.
        logz=float(logsumexp(lw)-math.log(len(lw)));w=np.exp(lw-logsumexp(lw))
        scaled=np.exp(lw-np.max(lw));mean=scaled.mean()
        relse=float(np.sqrt(np.var(scaled,ddof=1)/len(lw))/mean)
        return dict(x=x,lw=lw,weights=w,lnZ=logz,relative_se=relse,ess=float(1/(w@w)),
            max_weight=float(w.max()),n=len(x),labels=np.concatenate(self.labels))


def validate_cloud(x):
    x=np.asarray(x)
    if x.ndim!=2 or x.shape[1]!=6 or len(x)<20 or not np.isfinite(x).all():raise ValueError('invalid training cloud')
    if np.any((x<0)|(x>1)):raise ValueError('training cloud outside bounds')
    if np.linalg.matrix_rank(x-x.mean(0))<6:raise ValueError('collapsed training cloud')


def draw_av_iid(av,n,rng):
    av._apply_warm_state()
    bins=np.asarray(av.binunique);dx=np.asarray(av.dx)
    chosen=rng.randint(len(bins),size=n)
    return av.my_ranges[:,0]+dx*(bins[chosen]+rng.uniform(size=(n,6)))


def av_mass_check(av):
    av._apply_warm_state();bins=np.asarray(av.binunique);dx=np.asarray(av.dx)
    if len(np.unique(bins,axis=0))!=len(bins):raise ValueError('duplicate AV bins')
    if np.any(bins<0) or np.any(bins>=np.round(av.nbins).astype(int)):raise ValueError('out-of-box AV bin')
    volume=float(len(bins)*np.prod(dx))
    if not 0<volume<=1+1e-12:raise ValueError('invalid AV volume')
    centers=av.my_ranges[:,0]+dx*(bins+.5)
    density=np.asarray(av.sampling_density(centers))
    error=float(np.max(np.abs(density*volume-1)))
    if error>1e-10:raise ValueError('AV normalized-density contract failed')
    return dict(volume=volume,mass=1.,max_density_error=error,bins=len(bins))


def group_box_mass(mean,cov):
    # Independent deterministic conditional-normal integral for a 2D Gaussian
    # in the native GMM's normalized [-1,1]^2 coordinates.
    sx=math.sqrt(cov[0,0]);beta=cov[1,0]/cov[0,0]
    sy=math.sqrt(max(cov[1,1]-cov[1,0]**2/cov[0,0],1e-30))
    low=max((-1-mean[0])/sx,-12.);high=min((1-mean[0])/sx,12.)
    if low>=high:raise ValueError('GMM component outside numerical probability support')
    def integrand(z):
        mu=mean[1]+beta*sx*z
        return math.exp(-z*z/2)/math.sqrt(2*math.pi)*(ndtr((1-mu)/sy)-ndtr((-1-mu)/sy))
    # Split where the conditional mean crosses each bound to resolve near-
    # singular correlations without trusting a coarse rectangular quadrature.
    points=[0.]
    if beta!=0:points +=[(bound-mean[1])/(beta*sx) for bound in [-1,1]]
    points=sorted(p for p in points if low<p<high)
    return quad(integrand,low,high,points=points,epsabs=1e-10,epsrel=1e-8,limit=150)[0]


def gmm_mass_check(gmm):
    from RIFT.integrators import gaussian_mixture_model as G
    errors=[]
    for group,model in gmm.integrator.gmm_dict.items():
        if model is None:continue
        if len(group)!=2:raise ValueError('only validated 2D groups permitted')
        mass=0.
        for mean,cov,weight in zip(model.means,model.covariances,model.weights):
            mean=np.asarray(mean);cov=np.asarray(cov)
            native=float(G.mvnun(np.array([-1.,-1.]),np.array([1.,1.]),mean,cov)[0])
            gold=group_box_mass(mean,cov)
            if native<=1e-12:raise ValueError('unresolved tiny GMM truncation mass')
            mass+=float(weight)*gold/native
        errors.append(abs(mass-1))
    error=max(errors,default=0.)
    if error>1e-5:raise ValueError('GMM normalization failed independent box-mass check')
    return dict(max_group_mass_error=error,groups=len(gmm.integrator.gmm_dict))


def mixture_draw(av,gmm,n,rng,weights):
    weights=np.asarray(weights);assert abs(weights.sum()-1)<1e-12 and weights[0]==.2
    labels=rng.choice(len(weights),size=n,p=weights);x=np.empty((n,6))
    for label in range(len(weights)):
        idx=np.where(labels==label)[0]
        if not len(idx):continue
        if label==0:x[idx]=rng.uniform(size=(len(idx),6))
        elif label==1:x[idx]=draw_av_iid(av,len(idx),rng)
        else:x[idx]=np.asarray(gmm.draw_simplified(len(idx))[2]).T
    inside=np.isfinite(x).all(1)&np.all((x>=0)&(x<=1),axis=1)
    if not inside.all():raise ValueError('native proposal draw outside declared density support')
    q=weights[0]+weights[1]*np.asarray(av.sampling_density(x))
    if gmm is not None:q+=weights[2]*np.asarray(gmm.sampling_density(x))
    return x,q,labels
