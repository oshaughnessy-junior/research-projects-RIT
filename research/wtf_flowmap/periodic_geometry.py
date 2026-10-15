"""Untrained geometry specification: flat T^3 x R^3, standard library only.

This is NOT a replacement trained model. Angular state stores continuous lifts;
periodic network features and equivariant jumps are required before quotienting.
No density is provided. Physical order: RA, dec, psi, incl, phase, distance Mpc.
State order: RA angle, phase angle, twice psi, u_dec, u_incl, log_distance.
Optional bounded_distance=True specifies a separately labelled logit-distance
alternative. It is not needed to repair the observed phase failure.
"""
import math

TAU=2*math.pi


def sigmoid(x):
    if x>=0:
        return 1/(1+math.exp(-x))
    e=math.exp(x)
    return e/(1+e)


def decode(state,dmin=1.,dmax=10000.,bounded_distance=False):
    a,b,c,u,v,w=state
    try:distance=dmin+(dmax-dmin)*sigmoid(w) if bounded_distance else math.exp(w)
    except OverflowError:raise ValueError('floating-point saturation at physical support boundary')
    q=[a,math.asin(math.tanh(u)),c/2,math.acos(math.tanh(v)),b,
       distance]
    if not all(math.isfinite(x) for x in q):raise ValueError('nonfinite coordinate')
    if not dmin<q[5]<dmax or not abs(q[1])<math.pi/2 or not 0<q[3]<math.pi:
        raise ValueError('floating-point saturation at physical support boundary')
    # No angle wrapping is done here. A verified periodic physical backend
    # receives lifted angles; equivalent representatives have the same reward.
    return q


def encode(physical,dmin=1.,dmax=10000.,bounded_distance=False):
    ra,dec,psi,incl,phase,distance=physical
    if not dmin<distance<dmax:raise ValueError('distance outside open support')
    w=math.log((distance-dmin)/(dmax-distance)) if bounded_distance else math.log(distance)
    return [ra,phase,2*psi,math.atanh(math.sin(dec)),math.atanh(math.cos(incl)),w]


def jacobian(state,dmin=1.,dmax=10000.,bounded_distance=False):
    decode(state,dmin,dmax,bounded_distance)  # reject numerical boundary saturation
    j=[[0.]*6 for _ in range(6)]
    j[0][0]=1.;j[4][1]=1.;j[2][2]=.5
    j[1][3]=1/math.cosh(state[3]);j[3][4]=-1/math.cosh(state[4])
    f=sigmoid(state[5]);j[5][5]=(dmax-dmin)*f*(1-f) if bounded_distance else math.exp(state[5])
    return j


def abs_det_jacobian(state,dmin=1.,dmax=10000.,bounded_distance=False):
    j=jacobian(state,dmin,dmax,bounded_distance)
    return abs(j[0][0]*j[4][1]*j[2][2]*j[1][3]*j[3][4]*j[5][5])


def features(state):
    return [f(x) for x in state[:3] for f in (math.sin,math.cos)]+list(state[3:])


def shortest_delta(start,end):
    # The antipodal ambiguity is a measure-zero training-path choice and must
    # be recorded. This does not wrap the outputs of an old nonperiodic net.
    return math.atan2(math.sin(end-start),math.cos(end-start))


def interpolation(start,end,t):
    delta=[shortest_delta(a,b) if k<3 else b-a for k,(a,b) in enumerate(zip(start,end))]
    return [a+t*d for a,d in zip(start,delta)],delta


def semigroup_error(predicted,target):
    return sum(2*(1-math.cos(a-b)) if k<3 else (a-b)**2
               for k,(a,b) in enumerate(zip(predicted,target)))
