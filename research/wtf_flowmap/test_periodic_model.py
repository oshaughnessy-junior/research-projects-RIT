"""Algebraic and numerical model contracts, including trained-model entry point."""
import math
import torch
from periodic_model import (PeriodicVelocity,base_noise,features,decode,support_mask,
    interpolant,displacement,chord_loss,frozen)


def contract_checks(drift,flowmap,center,scale):
    dtype=next(drift.parameters()).dtype
    g=torch.Generator().manual_seed(261019)
    x=base_noise(8,dtype=dtype,generator=g)
    s=torch.linspace(.1,.6,8,dtype=dtype);t=s+.2
    errors={}
    for label,model in [('drift',drift),('map_velocity',flowmap)]:
        fun=(lambda z:model(z,s)) if label=='drift' else (lambda z:model(z,s,t))
        expected=fun(x)
        for axis in range(3):
            shift=torch.zeros_like(x);shift[:,axis]=2*math.pi*(axis+1)
            diff=float((fun(x+shift)-expected).abs().max())
            errors[label+'_axis_'+str(axis)]=diff
            assert diff<=1e-10,'angular velocity not shift invariant'
            if label=='map_velocity':
                assert float((flowmap.jump(x+shift,s,t)-flowmap.jump(x,s,t)-shift).abs().max())<=1e-10
        scalar=(lambda z:model(z[None],s[:1])[0]) if label=='drift' else (lambda z:model(z[None],s[:1],t[:1])[0])
        def jacobian_at(z):
            if model.meter:model.meter.add('input_jacobian_output_rows',6)
            return torch.autograd.functional.jacobian(scalar,z)
        a=jacobian_at(x[0]);assert torch.isfinite(a).all()
        for axis in range(3):
            shift=torch.zeros(6,dtype=dtype);shift[axis]=2*math.pi
            b=jacobian_at(x[0]+shift)
            error=float((a-b).abs().max());errors[label+'_jacobian_axis_'+str(axis)]=error
            assert error<=1e-10,'derivative shift invariance failed'
        # Cross the old canonical seam without a coordinate or network jump.
        h=1e-6
        for axis in range(3):
            left=x[0].clone();right=x[0].clone();left[axis]=-h;right[axis]=2*math.pi+h
            # Independent central difference around zero, using two equivalent
            # lifted representatives on the two sides of the cut.
            slope=(scalar(right)-scalar(left))/(2*h)
            atzero=x[0].clone();atzero[axis]=0
            jac=jacobian_at(atzero)[:,axis]
            assert torch.allclose(slope,jac,atol=1e-7,rtol=1e-5),'seam derivative mismatch'
    torch.testing.assert_close(flowmap.jump(x,s,s),x,rtol=0,atol=0)
    # Full standardized decoder Jacobian and physical psi half-angle.
    point=torch.tensor([.4,.8,1.2,.2,-.3,.1],dtype=dtype)
    fun=lambda z:decode(z,center,scale)
    jac=torch.autograd.functional.jacobian(fun,point);assert torch.isfinite(jac).all()
    h=1e-5
    for axis in range(6):
        shift=torch.zeros(6,dtype=dtype);shift[axis]=h
        torch.testing.assert_close((fun(point+shift)-fun(point-shift))/(2*h),jac[:,axis],atol=1e-5,rtol=1e-6)
    assert float(jac[2,2])==.5 and float(jac[3,4])<0
    assert support_mask(fun(point))
    from periodic_geometry import decode as scalar_decode
    raw=point[3:]*scale+center
    expected=scalar_decode(point[:3].tolist()+raw.tolist())
    torch.testing.assert_close(fun(point),torch.tensor(expected,dtype=dtype),atol=1e-10,rtol=1e-10)
    return errors


def test_neural_shift_jump_derivative_and_decoder_contracts():
    torch.manual_seed(261018)
    d=PeriodicVelocity().double();m=PeriodicVelocity(two_time=True).double()
    contract_checks(d,m,torch.tensor([0.,0.,7.]),torch.tensor([1.,1.,.2]))


def test_base_and_flow_matching_quotient_consistency():
    g=torch.Generator().manual_seed(5);x=base_noise(5000,generator=g)
    assert torch.all((x[:,:3]>=-math.pi)&(x[:,:3]<math.pi))
    # Broad, deterministic distribution checks; not claims about learned draws.
    assert torch.abs(torch.sin(x[:,:3]).mean(0)).max()<.05
    assert torch.abs(torch.cos(x[:,:3]).mean(0)).max()<.05
    assert torch.abs(x[:,3:].mean(0)).max()<.05
    y=base_noise(5000,generator=g);t=torch.linspace(0,1,5000,dtype=x.dtype)
    z,v=interpolant(x,y,t)
    assert torch.abs(v[:,:3]).max()<=math.pi
    torch.testing.assert_close(displacement(z[:1],x[:1]),torch.zeros(1,6,dtype=x.dtype))
    torch.testing.assert_close(displacement(z[-1:],y[-1:]),torch.zeros(1,6,dtype=x.dtype),atol=1e-14,rtol=0)
    # Velocity and path transform consistently under independent target lifts.
    y2=y.clone();y2[:,1]+=2*math.pi
    z2,v2=interpolant(x,y2,t)
    torch.testing.assert_close(z,z2,atol=1e-13,rtol=1e-13)
    torch.testing.assert_close(v,v2,atol=1e-13,rtol=1e-13)


def test_antipodal_convention_and_no_velocity_modulo():
    x=torch.zeros(2,6,dtype=torch.float64);y=x.clone();y[:,0]=torch.tensor([math.pi,-math.pi],dtype=x.dtype)
    _,v=interpolant(x,y,torch.ones(2,dtype=x.dtype)*.5)
    torch.testing.assert_close(v[:,0],torch.full((2,),math.pi,dtype=x.dtype))
    a=torch.zeros_like(x);b=a.clone();b[:,0]=2*math.pi
    assert float(chord_loss(a,b))==0
    # Tangent controls are ordinary vectors: a 2pi velocity difference is real.
    assert float((a-b).square().mean())>1


def test_frozen_weights_retain_input_derivatives_and_training_gradient():
    torch.manual_seed(9);model=PeriodicVelocity(two_time=True).double();ref=frozen(model)
    x=base_noise(4).requires_grad_(True);s=torch.zeros(4,dtype=x.dtype);t=s+.5
    val=ref.jump(x,s,t).square().sum();grad=torch.autograd.grad(val,x)[0]
    assert torch.isfinite(grad).all();assert all(p.grad is None for p in ref.parameters())
    loss=chord_loss(model.jump(x.detach(),s,t),x.detach()+.1);loss.backward()
    assert all(p.grad is not None and torch.isfinite(p.grad).all() for p in model.parameters())
