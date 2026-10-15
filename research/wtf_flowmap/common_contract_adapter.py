"""Research-only common fixed-window primitive quadrature; no physical imports."""
import numpy as np
from scipy.signal import resample

FACTORS=(32,64,128)

def _manual_resample(x,f):
    n=len(x);spec=np.fft.fft(x,axis=0);p=(n-1)//2
    out=np.zeros((n*f,)+x.shape[1:],dtype=np.complex128)
    out[:p+1]=spec[:p+1]
    if n%2==0:
        out[n//2]=spec[n//2]/2;out[-n//2]=spec[n//2]/2
        out[-p:]=spec[n//2+1:]
    else:out[-p:]=spec[-p:]
    return np.fft.ifft(out,axis=0)*f

def dense_primitive(k,dk,guard,n,method):
    if guard<1 or len(k)!=n+2*guard:raise ValueError('primitive/crop shape')
    ramp=.5*(1-np.cos(np.pi*np.arange(guard+1)/guard))
    taper=np.r_[ramp[:-1],np.ones(n),ramp[:-1][::-1]]
    # Skip identically zero supplied derivative columns, not physical rows.
    active=np.flatnonzero(np.any(dk!=0,axis=0));columns=np.column_stack((k,dk[:,active]))*taper[:,None]
    reflected=np.concatenate((columns,columns[-2:0:-1]),axis=0)
    fine=resample(reflected,len(reflected)*128,axis=0) if method=='simpson_scipy' else _manual_resample(reflected,128)
    fine=fine[guard*128:guard*128+(n-1)*128+1]
    derivative=np.zeros((len(fine),6),dtype=float);derivative[:,active]=fine[:,1:].real
    return fine[:,0].real,derivative,dict(fft_columns=len(active)+1,fft_input_nodes=len(reflected),fft_output_nodes=len(reflected)*128,method=method,guard=guard)

def reduce_field(field,dfield,dx,method):
    field=np.asarray(field);dfield=np.asarray(dfield)
    if not np.isfinite(field).all() or not np.isfinite(dfield).all():raise ValueError('nonfinite primitive/derivative')
    n=len(field)
    if method=='simpson_scipy':
        if n%2!=1:raise ValueError('common Simpson grid must have odd node count')
        w=np.full(n,2.);w[1:-1:2]=4;w[[0,-1]]=1;w*=dx/3
    else:w=np.full(n,dx);w[[0,-1]]*=.5
    m=field.max();scaled=np.exp(field-m)*w;s=scaled.sum()
    if not s>0:raise ValueError('zero integral')
    return float(m+np.log(s)),np.sum(scaled[:,None]*dfield,axis=0)/s

def evaluate(k,rho,dk,drho,n,dt,guard_max=1024):
    k=np.asarray(k);rho=np.asarray(rho);dk=np.asarray(dk);drho=np.asarray(drho)
    if dk.shape!=(len(k),6) or drho.shape!=(6,):raise ValueError('parameter order/derivative shape')
    if np.ptp(rho)>1e-8:raise ValueError('time-dependent norm outside contract')
    grids=[];operations=[]
    for guard in [guard_max//2,guard_max]:
        trim=guard_max-guard;kk=k[trim:-trim] if trim else k;dd=dk[trim:-trim] if trim else dk
        for method in ['simpson_scipy','trapezoid_numpy_fft']:
            fine,df,op=dense_primitive(kk,dd,guard,n,method);operations.append(op)
            for f in FACTORS:
                stride=128//f;field=fine[::stride]-.5*rho[0];dfield=df[::stride]-.5*drho
                value,gradient=reduce_field(field,dfield,dt/f,method)
                grids.append(dict(factor=f,guard=guard,method=method,value=value,gradient=gradient.tolist(),nodes=len(field)))
    ref=next(x for x in grids if x['factor']==128 and x['guard']==guard_max and x['method']=='simpson_scipy')
    tol=.001+.0001*np.abs(ref['gradient'])
    errors=[]
    for guard in [guard_max//2,guard_max]:
        for method in ['simpson_scipy','trapezoid_numpy_fft']:
            vals=[x for x in grids if x['guard']==guard and x['method']==method]
            for a,b in zip(vals,vals[1:]):errors.append(dict(kind='doubling',value=abs(a['value']-b['value']),gradient=float(np.max(np.abs(np.array(a['gradient'])-b['gradient'])/tol))))
    for x in grids:
        if x['factor']==128:errors.append(dict(kind='support_or_method',value=abs(x['value']-ref['value']),gradient=float(np.max(np.abs(np.array(x['gradient'])-ref['gradient'])/tol))))
    passed=all(x['value']<=1e-4 and x['gradient']<=1 for x in errors)
    return dict(passed=passed,raw_log_integral=ref['value'],uniform_time_log_average=ref['value']-float(np.log((n-1)*dt)),gradient=ref['gradient'],grids=grids,errors=errors,operations=operations,
        quadrature_nodes_traversed=sum(x['nodes'] for x in grids),refusal=None if passed else 'value/gradient refinement or support/method mismatch')
