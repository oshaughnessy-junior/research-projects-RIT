"""Geometry/algebra checks on mini, zero training and physical evaluations."""
import math
import random
import unittest
from periodic_geometry import (TAU,decode,encode,jacobian,abs_det_jacobian,
                               features,interpolation,semigroup_error)


class GeometryTests(unittest.TestCase):
    def test_roundtrip_and_strict_physical_support(self):
        rng=random.Random(261017)
        for _ in range(100):
            x=[rng.uniform(-20,20) for _ in range(3)]+[rng.uniform(-4,4) for _ in range(3)]
            x[5]=rng.uniform(.1,math.log(10000)-.1)
            q=decode(x);back=encode(q)
            self.assertTrue(1<q[5]<10000)
            for a,b in zip(x,back):self.assertAlmostEqual(a,b,places=10)

    def test_full_jacobian_and_sign_by_independent_finite_difference(self):
        x=[.4,-1.2,3.1,.7,-.8,1.3];j=jacobian(x);h=1e-5
        for col in range(6):
            hi=x.copy();lo=x.copy();hi[col]+=h;lo[col]-=h
            for row,(a,b) in enumerate(zip(decode(hi),decode(lo))):
                self.assertAlmostEqual((a-b)/(2*h),j[row][col],delta=1e-5)
        self.assertLess(j[3][4],0)
        self.assertGreater(abs_det_jacobian(x),0)

    def test_periodic_features_and_equivariant_residual_jump(self):
        # Toy periodic velocity demonstrates the proposed ARCHITECTURE
        # property. This is not a trained neural checkpoint or real reward.
        def velocity(x):
            f=features(x)
            return [f[k]+.1*f[(k+2)%9] for k in range(6)]
        def jump(x):return [a+.3*v for a,v in zip(x,velocity(x))]
        x=[.2,.8,-2.4,.1,-.9,.7]
        shifts=[TAU,-2*TAU,3*TAU,0,0,0]
        moved=[a+b for a,b in zip(x,shifts)]
        for a,b in zip(features(x),features(moved)):self.assertAlmostEqual(a,b,places=12)
        for a,b,k in zip(jump(x),jump(moved),shifts):self.assertAlmostEqual(a+k,b,places=12)

    def test_short_arc_crosses_seam_without_state_clipping(self):
        e=1e-4;start=[TAU-e,0,0,0,0,1];end=[e,0,0,0,0,1]
        middle,delta=interpolation(start,end,.5)
        self.assertAlmostEqual(delta[0],2*e,places=12)
        self.assertAlmostEqual(middle[0],TAU,places=12)
        # Lifted physical angle is continuous, not snapped to either side.
        self.assertAlmostEqual(decode(middle)[0],TAU,places=12)

    def test_torus_error_invariant_to_representatives(self):
        a=[.2,.4,.7,1.,2.,3.];b=[-.1,.8,1.1,2.,3.,4.]
        c=b.copy();c[0]+=TAU;c[1]-=2*TAU;c[2]+=3*TAU
        self.assertAlmostEqual(semigroup_error(a,b),semigroup_error(a,c),places=12)

    def test_naive_wrapping_of_nonperiodic_net_is_not_a_repair(self):
        def bad_jump(x):return (x+.2*x*x)%TAU
        self.assertGreater(abs(bad_jump(0)-bad_jump(TAU)),.1)

    def test_distance_saturation_is_rejected_not_clipped(self):
        for w in [1000.,-1000.]:
            with self.assertRaisesRegex(ValueError,'saturation'):decode([0,0,0,0,0,w])

    def test_polarization_jacobian_preserves_uniform_prior_mass(self):
        # psi in [0,pi), theta_psi=2psi in [0,2pi).
        density_theta=(1/math.pi)*.5
        self.assertAlmostEqual(density_theta*TAU,1.,places=14)
        self.assertAlmostEqual(jacobian([0,0,0,0,0,1])[2][2],.5,places=14)

    def test_optional_bounded_distance_is_separate_from_minimal_phase_repair(self):
        x=[.4,-1.2,3.1,.7,-.8,1.3];h=1e-5
        q=decode(x,bounded_distance=True)
        for a,b in zip(x,encode(q,bounded_distance=True)):self.assertAlmostEqual(a,b,places=12)
        hi=x.copy();lo=x.copy();hi[5]+=h;lo[5]-=h
        fd=(decode(hi,bounded_distance=True)[5]-decode(lo,bounded_distance=True)[5])/(2*h)
        self.assertAlmostEqual(fd,jacobian(x,bounded_distance=True)[5][5],delta=1e-5)
        for w in [-1000.,1000.]:
            with self.assertRaises(ValueError):decode([0,0,0,0,0,w],bounded_distance=True)


if __name__=='__main__':unittest.main(verbosity=2)
