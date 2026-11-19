"""Bounded synthetic training-only oracle experiment; see TRANSPORT_PROTOTYPE.md."""
import time
_PROCESS_START = time.perf_counter()
import argparse
import json
import subprocess
import sys
import tempfile
import numpy as np
from scipy.special import logsumexp
from scipy.stats import truncnorm
from RIFT.integrators.evaluation_cost import EvaluationLedger, cost_phase
from RIFT.integrators.mcsampler_generic import MCSamplerGeneric
from RIFT.integrators.unreliable_oracle.transport import TransportOracle
from RIFT.integrators.unreliable_oracle.puffball import PuffballOracle


class SeparatedTarget:
    params = ['x', 'y']
    masses = np.array([0.8, 0.2])
    means = np.array([[-2., -1.], [2., 1.]])
    sigmas = np.array([0.45, 0.6])
    true_lnZ = np.log(3.)

    def __init__(self):
        self.components = [[truncnorm((-5-m)/s, (5-m)/s, loc=m, scale=s)
                            for m, s in zip(mu, self.sigmas)] for mu in self.means]
        self.mean = sum(w*np.array([d.mean() for d in ds]) for w, ds in zip(self.masses, self.components))
        self.second = sum(w*np.array([d.var()+d.mean()**2 for d in ds]) for w, ds in zip(self.masses, self.components))
        self.mode_mass = sum(w*(1-ds[0].cdf(0.)) for w, ds in zip(self.masses, self.components))
        self.tail_mass = sum(w*(ds[0].cdf(-3.)+1-ds[0].cdf(3.)) for w, ds in zip(self.masses, self.components))

    @staticmethod
    def prior(dim, x):
        return np.where(np.abs(x) <= 5., (1. + [0.12, -0.1][dim]*x)/10., 0.)

    def lnL(self, x, y):
        cols = [np.asarray(x), np.asarray(y)]
        terms = [np.log(w) + sum(d.logpdf(c) for d, c in zip(ds, cols))
                 for w, ds in zip(self.masses, self.components)]
        return self.true_lnZ + logsumexp(terms, axis=0) - sum(np.log(self.prior(i, c)) for i, c in enumerate(cols))


class UniformBox(MCSamplerGeneric):
    """Exact normalized, frozen broad-support component for this diagnostic."""
    def sampling_density(self, x):
        return np.where(np.all(np.abs(x) <= 5., axis=1), 0.01, 0.)

    def draw_simplified(self, n, *args, **kwargs):
        x = np.random.uniform(-5., 5., (int(n), 2))
        prior = np.prod([self.prior_pdf[p](x[:, j]) for j, p in enumerate(self.params_ordered)], axis=0)
        return self.sampling_density(x), prior, x.T

    def update_sampling_prior(self, *args, **kwargs):
        pass


def build(target, arm, seed, ledger):
    from RIFT.integrators import mcsamplerEnsemble as G, mcsamplerPortfolio as P
    if G.cupy_ok:
        raise RuntimeError('CPU diagnostic: launch with CUDA_VISIBLE_DEVICES empty before import')
    oracle = [] if arm == 'none' else [PuffballOracle() if arm == 'cheap' else TransportOracle()]
    s = P.MCSampler(portfolio=[UniformBox(), G.MCSampler()],
                    oracle_realizations=oracle)
    s.portfolio_weights = np.array([0.25, 0.75])
    for j, p in enumerate(target.params):
        s.add_parameter(p, lambda x: np.ones_like(x)/10.,
                        prior_pdf=lambda x, j=j: target.prior(j, x),
                        left_limit=-5., right_limit=5., adaptive_sampling=True)
    s.setup(portfolio_args=[{}, dict(n_comp=2, correlate_all_dims=True)],
            oracle_args=[] if not oracle else [dict(seed=seed, ledger=ledger)])
    return s


def run(arm, seed, budget=2000):
    # Reserve 100 calls for proposal training: 100 initial rows for no oracle,
    # or 40 initial + 6*10 oracle scores. No setup row enters the estimate.
    if budget < 800 or budget % 100:
        raise ValueError('budget must be a multiple of 100 and >=800')
    started = time.perf_counter()
    ledger = EvaluationLedger()
    target = SeparatedTarget()
    fn = ledger.wrap(target.lnL)
    from RIFT.integrators.seeding import seed_everything
    seed_everything(seed, verbose=False)
    with cost_phase('setup'):
        s = build(target, arm, seed, ledger)
    setup = time.perf_counter() - started
    t = time.perf_counter()
    with cost_phase('warm_start'):
        n_initial = 100 if arm == 'none' else 40
        x = np.random.uniform(-5, 5, (n_initial, 2))
        ll = fn(*x.T)
        # These are actual uniform-proposal posterior training weights.
        lw = ll + sum(np.log(target.prior(j, x[:, j])) for j in range(2)) + np.log(100.)
        history = dict(zip(target.params, x.T))
        history['log_integrand'] = ll
        with cost_phase('member_training', ledger):
            s.portfolio_realizations[1].update_sampling_prior(lw, n_initial, external_rvs=history, tempering_exp=1.)
    warm = time.perf_counter() - t
    t = time.perf_counter()
    lnZ, _, _, _ = s.integrate_log(fn, *target.params, no_protect_names=True,
                                  n=100, nmax=budget-100, neff=float('inf'),
                                  n_adapt=100, tempering_exp=1., save_intg=True,
                                  portfolio_schedule=lambda dat, previous, **kw: np.array([0.25, 0.75]))
    integration = time.perf_counter() - t
    cost = ledger.snapshot()
    assert cost['true_rows'] == budget, cost
    rvs = s._rvs
    assert len(rvs['x']) == budget-100
    lw = rvs['log_integrand'] + rvs['log_joint_prior'] - rvs['log_joint_s_prior']
    weights = np.exp(lw-logsumexp(lw))
    out = np.column_stack([rvs[p] for p in target.params])
    ess = float(1./np.sum(weights**2))
    return dict(arm=arm, seed=seed, budget=budget, cost=cost, n_sampler=int(s.ntotal),
                lnZ=float(lnZ), evidence_error=float(lnZ-target.true_lnZ),
                mode_mass_error=float(weights @ (out[:, 0]>0)-target.mode_mass),
                mean_error=(weights @ out-target.mean).tolist(),
                second_moment_error=(weights @ (out**2)-target.second).tolist(),
                tail_mass_error=float(weights @ (np.abs(out[:, 0])>3)-target.tail_mass),
                ess=ess, ess_per_true_row=ess/cost['true_rows'],
                setup_seconds=setup, warm_start_seconds=warm,
                integration_seconds=integration,
                training_seconds=sum(cost['phase_seconds'].get(p, 0.) for p in ['oracle_training', 'member_training']),
                end_to_end_seconds=time.perf_counter()-started,
                amortized_reuses=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--seeds', nargs='+', type=int, default=[101, 202, 303, 404, 505])
    parser.add_argument('--budget', type=int, default=2000)
    parser.add_argument('--single-arm', choices=['none', 'cheap', 'transport'])
    parser.add_argument('--json', required=True)
    args = parser.parse_args()
    if args.single_arm:
        if len(args.seeds) != 1:
            parser.error('--single-arm requires exactly one seed')
        result = run(args.single_arm, args.seeds[0], args.budget)
        result['process_seconds'] = time.perf_counter() - _PROCESS_START
        results = [result]
    else:
        # Fresh process per arm includes imports and avoids first-arm cache bias.
        results = []
        with tempfile.TemporaryDirectory(prefix='rift-transport-') as directory:
            for seed in args.seeds:
                for arm in ['none', 'cheap', 'transport']:
                    output = directory + '/result.json'
                    subprocess.run([sys.executable, __file__, '--single-arm', arm,
                                    '--seeds', str(seed), '--budget', str(args.budget),
                                    '--json', output], check=True)
                    with open(output) as f:
                        results.extend(json.load(f))
    with open(args.json, 'w') as f:
        json.dump(results, f, indent=2)


if __name__ == '__main__':
    main()
