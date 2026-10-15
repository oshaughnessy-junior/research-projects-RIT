import numpy as np
import pytest
from five_arm_sampling import FixedNEstimator,validate_cloud,group_box_mass


def test_fixed_population_and_training_isolation():
    s=FixedNEstimator(2)
    with pytest.raises(ValueError,match='evidence'):s.append(np.zeros((1,6)),np.array([999.]),np.ones(1),np.zeros(1),'oracle')
    s.append(np.zeros((2,6)),np.array([0.,-np.inf]),np.ones(2),np.zeros(2),'evidence')
    r=s.result();assert r['n']==2;assert r['lnZ']==pytest.approx(np.log(.5))
    assert len(s.points)==1


def test_adaptive_conditional_density_exact_enumeration():
    # Two-state normalized target (0.2,0.8), q2 depends on first draw.
    # E[ fixed-N mean of p/q_i ]=1. Posterior weighting is checked separately.
    expectation=0.
    for first in [0,1]:
        q1=np.array([.5,.5]);q2=np.array([.9,.1]) if first==0 else np.array([.3,.7])
        for second in [0,1]:
            estimate=(np.array([.2,.8])[first]/q1[first]+np.array([.2,.8])[second]/q2[second])/2
            expectation+=q1[first]*q2[second]*estimate
    assert expectation==pytest.approx(1.)


def test_collapsed_cloud_rejected():
    with pytest.raises(ValueError,match='collapsed'):validate_cloud(np.ones((100,6))*.5)


def test_independent_box_mass_product_case():
    from scipy.special import ndtr
    mean=np.array([.3,-.2]);cov=np.diag([.4,.7])
    target=np.prod(ndtr((1-mean)/np.sqrt(np.diag(cov)))-ndtr((-1-mean)/np.sqrt(np.diag(cov))))
    assert group_box_mass(mean,cov)==pytest.approx(target,rel=1e-9,abs=1e-10)


def make_native():
    from RIFT.integrators import mcsamplerAdaptiveVolume as A,mcsamplerEnsemble as E
    from five_arm_sampling import PARAMS,GROUPS
    av=A.MCSampler(n_chunk=100);gm=E.MCSampler()
    for obj in [av,gm]:
        for p in PARAMS:
            obj.add_parameter(p,pdf=lambda x:np.ones_like(x),prior_pdf=lambda x:np.ones_like(x),
                              left_limit=0.,right_limit=1.,adaptive_sampling=True)
    av.setup();gm.setup(n_comp={group:2 for group in GROUPS},gmm_dict={group:None for group in GROUPS},gmm_defensive_frac=0)
    return av,gm


def test_native_wrong_oracle_normalization_and_fresh_reset():
    from five_arm_sampling import PARAMS,av_mass_check,gmm_mass_check,mixture_draw
    np.random.seed(261050);rng=np.random.RandomState(261050)
    av,gm=make_native();cloud=rng.uniform(.8,.9,(100,6));validate_cloud(cloud)
    av.bootstrap_from_samples(cloud,loglkl=None,cover_frac=0,dilate=1,inflate=1,seed=261050)
    gm.update_sampling_prior(np.zeros(100),n_history=100,
        external_rvs={p:cloud[:,i] for i,p in enumerate(PARAMS)},tempering_exp=.1,log_scale_weights=True)
    av_mass_check(av);gmm_mass_check(gm)
    x,q,labels=mixture_draw(av,gm,10000,rng,[.2,.4,.4])
    est=FixedNEstimator(len(x));est.append(x,np.zeros(len(x)),q,labels,'evidence')
    r=est.result();assert abs(np.expm1(r['lnZ'])) < 5*r['relative_se']
    assert len(r['x'])==10000
    av2,gm2=make_native();assert av_mass_check(av2)['volume']==pytest.approx(1.)
    assert all(m is None for m in gm2.integrator.gmm_dict.values())
    assert av2 is not av and gm2 is not gm


def test_evidence_bounds_alignment_and_fixed_count():
    s=FixedNEstimator(2)
    with pytest.raises(ValueError,match='coordinates'):s.append(np.full((1,6),2.),np.zeros(1),np.ones(1),np.zeros(1),'evidence')
    with pytest.raises(ValueError,match='alignment'):s.append(np.zeros((1,6)),np.zeros(2),np.ones(1),np.zeros(1),'evidence')
    with pytest.raises(ValueError,match='exceeded'):s.append(np.zeros((3,6)),np.zeros(3),np.ones(3),np.zeros(3),'evidence')
    with pytest.raises(ValueError,match='empty'):s.result()
    s.append(np.zeros((1,6)),np.zeros(1),np.ones(1),np.zeros(1),'evidence')
    with pytest.raises(ValueError,match='incomplete'):s.result()


def test_correlated_box_mass_matches_independent_scipy():
    from scipy.stats import multivariate_normal
    mean=np.array([.1,-.2]);cov=np.array([[.4,.3],[.3,.7]])
    rv=multivariate_normal(mean=mean,cov=cov)
    exact=rv.cdf([1,1])-rv.cdf([-1,1])-rv.cdf([1,-1])+rv.cdf([-1,-1])
    assert group_box_mass(mean,cov)==pytest.approx(exact,abs=1e-8)
