import numpy as np
import pytest
from scipy.integrate import quad
from RIFT.integrators.evaluation_cost import EvaluationLedger, cost_phase
from RIFT.integrators.unreliable_oracle.transport import TransportOracle
from transport_oracle_benchmark import SeparatedTarget, UniformBox, build


def test_ledger_counts_rows_failures_phases_and_optimizer():
    from scipy.optimize import minimize
    ledger = EvaluationLedger()
    fn = ledger.wrap(lambda x: np.asarray(x)**2)
    with cost_phase('setup'):
        fn(np.zeros(3))
    with cost_phase('warm_start'):
        fn(x=np.zeros(4))
    with cost_phase('optimization_gradient'):
        minimize(lambda x: float(fn(x[0])), [2.], options={'maxiter': 2})
    broken = ledger.wrap(lambda x: (_ for _ in ()).throw(ValueError()))
    with pytest.raises(ValueError):
        broken(np.ones(5))
    snap = ledger.snapshot()
    assert snap['records']['true:setup']['rows'] == 3
    assert snap['records']['true:warm_start']['rows'] == 4
    opt = snap['records']['true:optimization_gradient']['rows']
    assert opt >= 4  # includes finite-difference reward calls, not merely steps
    assert snap['records']['true:unclassified']['failures'] == 1
    assert snap['true_rows'] == 12 + opt


def test_target_is_normalized_and_prior_nonuniform():
    t = SeparatedTarget()
    for i in range(2):
        assert quad(lambda x: t.prior(i, x), -5, 5)[0] == pytest.approx(1.)
        assert t.prior(i, -4) != t.prior(i, 4)
    # Product truncnorm components each integrate exactly to one.
    assert sum(t.masses) == pytest.approx(1.)
    assert t.mode_mass == pytest.approx(0.2, abs=1e-4)
    assert np.exp(t.lnL(-2., -1.))*t.prior(0, -2.)*t.prior(1, -1.) > 0


def oracle_fixture(order=('angle', 'x'), **kwargs):
    o = TransportOracle()
    for p in order:
        o.add_parameter(p, lambda x: np.ones_like(x), left_limit=-5., right_limit=5.)
    o.setup(periodic=['angle'], seed=44, **kwargs)
    x = np.random.RandomState(1).uniform(-5, 5, (50, 2))
    history = {'angle': x[:, 0], 'x': x[:, 1], 'log_integrand': -(x[:, 0]-4.9)**2-x[:, 1]**2}
    return o, history


def test_transport_bounds_order_periodic_reproducibility_reset():
    a, h = oracle_fixture()
    a.update_sampling_prior(None, 50, external_rvs=h)
    first = a.draw_simplified(50)[2]
    assert np.all((first >= -5) & (first <= 5))
    assert a.params_ordered == ['angle', 'x']
    # Circular displacement takes the short path across the seam.
    assert a._delta(np.array([[.99, .99]]), np.array([[.01, .01]]))[0, 0] == pytest.approx(-.02)
    b, _ = oracle_fixture()
    b.update_sampling_prior(None, 50, external_rvs=h)
    np.testing.assert_array_equal(first, b.draw_simplified(50)[2])
    a.setup(periodic=['angle'], seed=44)
    assert a.reference_samples is None and a.centers is None
    a.update_sampling_prior(None, 50, external_rvs=h)
    np.testing.assert_array_equal(first, a.draw_simplified(50)[2])
    c, _ = oracle_fixture(order=('x', 'angle'))
    c.update_sampling_prior(None, 50, external_rvs=h)
    assert c.params_ordered == ['x', 'angle']
    assert c.periodic.tolist() == [False, True]
    with pytest.raises(ValueError):
        c.setup(periodic=['typo'])


def test_oracle_setup_args_are_independent_and_reset_replays():
    from RIFT.integrators.mcsamplerPortfolio import MCSampler
    t = SeparatedTarget()
    oracles = [TransportOracle() for _ in range(3)]  # more oracles than members
    s = MCSampler(portfolio=[UniformBox()], oracle_realizations=oracles)
    for p in t.params:
        s.add_parameter(p, lambda x: np.ones_like(x)/10., left_limit=-5., right_limit=5.,
                        prior_pdf=lambda x: np.ones_like(x)/10.)
    s.setup(oracle_args=[dict(seed=k, steps=k+1) for k in range(3)])
    assert [o.steps for o in oracles] == [1, 2, 3]
    s.reset_adaptation()
    assert [o.steps for o in oracles] == [1, 2, 3]
    assert all(o.reference_samples is None for o in oracles)
    s.setup(oracle_realizations=[], oracle_args=[])
    assert s.oracle_realizations == []


def test_wrong_collapsed_oracle_never_enters_evidence_cache_or_fairdraw():
    from RIFT.integrators.mcsamplerPortfolio import MCSampler
    class Wrong(TransportOracle):
        def update_sampling_prior(self, *args, **kwargs):
            pass
        def draw_simplified(self, n, *args, **kwargs):
            return None, None, np.full((n, 2), 4.999)
    class RecordingBox(UniformBox):
        def update_sampling_prior(self, lw, n_history, external_rvs=None, **kwargs):
            self.training = external_rvs
    results = []
    for enabled in [False, True]:
        ledger = EvaluationLedger()
        np.random.seed(1)
        members = [RecordingBox(), RecordingBox()]
        s = MCSampler(portfolio=members, oracle_realizations=[Wrong()] if enabled else [])
        for p in ['x', 'y']:
            s.add_parameter(p, lambda x: np.ones_like(x)/10., prior_pdf=lambda x: np.ones_like(x)/10.,
                            left_limit=-5., right_limit=5.)
        s.setup()
        # constant likelihood makes evidence independent of ANY proposal training.
        result = s.integrate_log(ledger.wrap(lambda x, y: np.zeros_like(x)), 'x', 'y',
                                 no_protect_names=True, n=100, nmax=200, neff=float('inf'),
                                 n_adapt=10, tempering_exp=1., save_intg=True,
                                 igrand_fairdraw_samples=True, igrand_fairdraw_samples_max=50)
        assert int(s.ntotal) == 200
        assert not np.any(s._rvs['x'] == 4.999)
        reserve = s._warm_seed_reserve
        assert reserve['n_retained'] == 200
        if enabled:
            assert np.any(members[0].training['x'] == 4.999), 'oracle path did not execute'
            assert ledger.snapshot()['true_rows'] == 220
            assert ledger.snapshot()['records']['true:oracle_scoring']['rows'] == 20
        results.append(result[0])
    assert results == pytest.approx([0., 0.], abs=1e-12)


def test_zero_drift_transport_is_not_bayesian_tilt():
    a, s2, lam = 2., .5, 1.
    mean = lam*a/(s2+lam)
    variance = (1+lam/s2)**-2
    bayesian_variance = (1+lam/s2)**-1
    assert mean == pytest.approx(4./3.)
    assert variance < bayesian_variance
    # q narrower than posterior by >=sqrt(2): integral p^2/q diverges.
    assert 2/bayesian_variance - 1/variance <= 0


def test_benchmark_counts_setup_and_warm_start(monkeypatch):
    import benchmark_integrators as B
    class Sampler:
        ntotal = 2
        _rvs = dict(log_integrand=np.zeros(2), log_joint_prior=np.zeros(2),
                    log_joint_s_prior=np.zeros(2), **{'0': np.zeros(2), '1': np.zeros(2)})
        def __init__(self, target):
            self.target = target
        def setup(self):
            self.target.lnL(np.zeros((3, 2)))
        def integrate(self, f, *args, **kwargs):
            f(np.zeros(2), np.zeros(2))
            return 1., .01, 2., {}
    monkeypatch.setattr(B, 'build_sampler', lambda kind, target, **kw: (Sampler(target), 'mock'))
    target = B.CorrelatedGaussian(ndim=2)
    original = target.lnL
    result = B.run('default', target, warm_start=lambda s, t: t.lnL(np.zeros((7, 2))))
    assert result['n_eval'] == 12
    assert result['n_sampler'] == 2
    assert result['evaluation_cost']['records']['true:setup']['rows'] == 3
    assert result['evaluation_cost']['records']['true:warm_start']['rows'] == 7
    assert target.lnL == original


def test_hidden_climbing_likelihood_calls_are_counted():
    from RIFT.integrators.mcsamplerPortfolio import MCSampler
    from RIFT.integrators.unreliable_oracle.hill_climber import ClimbingOracle
    ledger = EvaluationLedger()
    fn = ledger.wrap(lambda x, y: -(x*x+y*y))
    s = MCSampler(portfolio=[UniformBox()], oracle_realizations=[ClimbingOracle()])
    for p in ['x', 'y']:
        s.add_parameter(p, lambda x: np.ones_like(x)/10., prior_pdf=lambda x: np.ones_like(x)/10.,
                        left_limit=-5., right_limit=5.)
    s.setup(oracle_args=[dict(lnL=fn, n_climbers=1)])
    s.integrate_log(fn, 'x', 'y', no_protect_names=True, n=100, nmax=200,
                    neff=float('inf'), save_intg=True, n_adapt=10, tempering_exp=1.)
    cost = ledger.snapshot()
    assert cost['records']['true:oracle_training']['rows'] > 2
    assert cost['records']['true:oracle_scoring']['rows'] == 20
    assert cost['records']['true:sampling']['rows'] == 200
    assert cost['true_rows'] > s.ntotal + 20
