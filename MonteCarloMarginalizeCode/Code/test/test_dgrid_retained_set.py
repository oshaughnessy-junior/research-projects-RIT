#!/usr/bin/env python
"""
Regression tests for the .dgrid exporter reading the RETAINED set instead of the fair draw
(RIFT/integrators/*, RIFT/misc/distance_grid.py, bin/integrate_likelihood_extrinsic_batchmode).

Background.  `--export-marginal-distance-grid` built its likelihood-vs-distance table from
`sampler._rvs`.  By then every integrator has REPLACED `_rvs` with an export resample:
n_extr = min(--fairdraw-extrinsic-output-n-max (5 by default), 1.5*eff_samp, 1.5*neff) rows
drawn WITH REPLACEMENT.  Measured on the ILE-GPU-Paper demo at GMM n_eff 5.9, that is FIVE
rows holding THREE distinct distances -- and the exporter wrote a five-row curve from them.

Fixing the binder (test_distance_grid_degenerate_bins.py) stops that table being degenerate.
It does not make it a curve: three points is three points.  The rows the pass actually kept
were still in `_rvs` one statement earlier.

  1. ONLY TWO SAMPLERS KEPT THEM.  `make_warm_seed_reserve` was built for the L0 rescue, so
     mcsamplerAdaptiveVolume and mcsamplerPortfolio kept a bounded copy of the retained rows
     and mcsampler / mcsamplerGPU (both entry points) / mcsamplerEnsemble / mcsamplerNFlow
     passed `reserve=getattr(self,'_warm_seed_reserve',None)` -- always None -- into their
     records.  For an L0 rescue that was the honest answer.  For anything that EXPORTS A
     SHAPE it was a gap, and the sampler in the bug report (GMM = Ensemble) was one of them.

  2. THE RESERVE IS NOT EQUAL WEIGHT.  A fair-drawn record must NOT be re-weighted by w
     (ln_weights_for_posterior exists for that reason).  The retained rows are the opposite
     case: they carry real importance weights, and both prior components ride along in the
     reserve precisely so a consumer can rebuild them.  Reading the reserve and then
     weighting it uniformly would be the same defect with the sign flipped.

  3. MORE ROWS IS NOT MORE INFORMATION.  The retained set has thousands of distinct
     distances at the same n_eff, so the duplicate-distance warning goes quiet exactly when
     the table stops LOOKING broken.  The resolution check is therefore effective sample
     size against bin count, not distinct values alone.

The requirement: every integrator that fair-draws leaves the retained rows reachable, the
exporter uses them with their own weights, and a starved pass is still called starved.
"""

import ast
import os

import numpy as np
import pytest

import RIFT.integrators.mcsampler as MC
import RIFT.integrators.mcsamplerAdaptiveVolume as AV
import RIFT.integrators.mcsamplerEnsemble as ENS
from RIFT.integrators.mcsamplerAdaptiveVolume import (make_reserve_from_rvs,
                                                     make_warm_seed_reserve)
from RIFT.misc.distance_grid import (
    _ess,
    build_distance_grid,
    distance_grid_resolution_warning,
    reconstruct_marginal_lnL,
    reserve_distance_and_ln_weights,
)

NAMES = ['right_ascension', 'declination', 'phi_orb', 'inclination', 'psi', 'distance']
_HERE = os.path.dirname(os.path.abspath(__file__))
_ILE = os.path.join(_HERE, '..', 'bin', 'integrate_likelihood_extrinsic_batchmode')
_INTEGRATORS = os.path.join(_HERE, '..', 'RIFT', 'integrators')

FAIRDRAW_MAX = 5          # the --fairdraw-extrinsic-output-n-max default


def _sharp(lnLmax=50.0, width=0.01, log=True):
    """A peak narrow enough that the fair draw really does collapse, as it does at the
    production SNR this defect was found at."""
    def f(*args, **kwargs):
        x = np.array([np.asarray(a, dtype=float).ravel() for a in args]).T
        v = -0.5 * np.sum(((x - 0.5) / width) ** 2, axis=-1) + lnLmax
        return v if log else np.exp(v)
    return f


def _av():
    s = AV.MCSampler(n_chunk=5000)
    s.xpy = AV.xpy_default
    s.identity_convert = AV.identity_convert
    for n in NAMES:
        s.add_parameter(n, pdf=None, left_limit=0.0, right_limit=1.0,
                        prior_pdf=lambda x: np.ones(np.shape(x)), adaptive_sampling=True)
    return s


def _vectorized(mod):
    def build():
        s = mod.MCSampler()
        v = np.vectorize(lambda x: 1.0)
        for n in NAMES:
            s.add_parameter(n, v, prior_pdf=v, left_limit=0.0, right_limit=1.0,
                            adaptive_sampling=True)
        return s
    return build


def _backends():
    """The three backends that can be driven in-process here.  mcsamplerGPU and
    mcsamplerNFlow need cupy / torch, so their sites are covered by the source sweep
    below rather than left unchecked."""
    return [
        ('AV', _av, 'integrate_log', _sharp(log=True)),
        ('Ensemble', _vectorized(ENS), 'integrate', _sharp(log=False)),
        ('mcsampler', _vectorized(MC), 'integrate', _sharp(log=False)),
    ]


def _run(build, method, target):
    np.random.seed(11)
    s = build()
    getattr(s, method)(target, *NAMES, nmax=40000, n=4000, neff=30, save_intg=True,
                       no_protect_names=True, verbose=False,
                       igrand_fairdraw_samples=True,
                       igrand_fairdraw_samples_max=FAIRDRAW_MAX)
    return s


def _reserve_of(sampler):
    rec = sampler.samples() if hasattr(sampler, 'samples') else None
    if rec is not None and getattr(rec, 'reserve', None) is not None:
        return rec.reserve
    return getattr(sampler, '_warm_seed_reserve', None)


###
### 1. every fair-drawing backend leaves the retained rows reachable
###

@pytest.mark.parametrize('label,build,method,target', _backends())
def test_the_fair_draw_no_longer_takes_the_retained_rows_with_it(label, build, method, target):
    s = _run(build, method, target)

    n_rvs = len(np.asarray(s._rvs['distance']).ravel())
    assert n_rvs <= FAIRDRAW_MAX, \
        '{}: the draw did not collapse, so this run proves nothing'.format(label)

    reserve = _reserve_of(s)
    assert reserve is not None, '{}: the retained rows were discarded with no record'.format(label)
    assert len(reserve['X']) > n_rvs, \
        '{}: reserve holds {} rows, no more than the {}-row draw'.format(
            label, len(reserve['X']), n_rvs)
    for key in ('lnL', 'log_joint_prior', 'log_joint_s_prior', 'params_ordered'):
        assert key in reserve, '{}: reserve cannot rebuild a weight without {}'.format(label, key)


@pytest.mark.parametrize('label,build,method,target', _backends())
def test_the_grid_built_from_the_reserve_resolves_where_the_draw_cannot(label, build, method, target):
    s = _run(build, method, target)
    got = reserve_distance_and_ln_weights(_reserve_of(s))
    assert got is not None, '{}: the exporter cannot read this reserve'.format(label)
    d_res, ln_w_res = got

    d_draw = np.asarray(s._rvs['distance'], dtype=float).ravel()
    assert len(np.unique(d_res)) > len(np.unique(d_draw)), \
        '{}: the reserve resolves no better than the fair draw'.format(label)

    grid = build_distance_grid(d_res, ln_w_res, 12.0, 0.0, {},
                               ln_prior_d_at_samples=np.zeros(len(d_res)), n_grid=50)
    assert len(grid) == 50, '{}: got {} rows from {} distinct distances'.format(
        label, len(grid), len(np.unique(d_res)))
    assert np.all(np.diff(grid['dist']) > 0)
    assert np.isclose(reconstruct_marginal_lnL(grid), 12.0), \
        '{}: swapping in the retained set moved the evidence'.format(label)


###
### 2. the reserve is weighted by w; the fair draw is not
###

def test_the_reserve_carries_real_importance_weights_not_uniform_ones():
    """If these rows were handed to the exporter and then weighted uniformly -- the correct
    treatment for a FAIR-DRAWN record -- the grid would be the wrong posterior."""
    names = ['distance', 'psi']
    lnL = np.array([0.0, 3.0, -2.0, 1.0])
    ln_prior = np.array([0.5, 0.5, 0.5, 0.5])
    ln_s_prior = np.array([0.1, 0.2, 0.3, 0.4])
    reserve = dict(X=np.array([[100.0, 0.1], [200.0, 0.2], [300.0, 0.3], [400.0, 0.4]]),
                   lnL=lnL, log_joint_prior=ln_prior, log_joint_s_prior=ln_s_prior,
                   params_ordered=names)

    d, ln_w = reserve_distance_and_ln_weights(reserve)

    assert np.array_equal(d, [100.0, 200.0, 300.0, 400.0])
    assert np.allclose(ln_w, lnL + ln_prior - ln_s_prior)
    assert not np.allclose(ln_w, ln_w[0]), 'the weights came back uniform'


@pytest.mark.parametrize('broken,why', [
    (None, 'no reserve at all'),
    ({}, 'empty reserve'),
    (dict(X=np.zeros((3, 2)), lnL=np.zeros(3), log_joint_prior=np.zeros(3),
          log_joint_s_prior=np.zeros(3), params_ordered=['psi', 'phi_orb']), 'no distance column'),
    (dict(X=np.zeros((0, 2)), lnL=np.zeros(0), log_joint_prior=np.zeros(0),
          log_joint_s_prior=np.zeros(0), params_ordered=['distance', 'psi']), 'no rows'),
    (dict(X=np.zeros((3, 2)), lnL=np.zeros(3), log_joint_s_prior=np.zeros(3),
          params_ordered=['distance', 'psi']), 'prior component missing'),
    (dict(X=np.zeros((3, 2)), lnL=np.zeros(4), log_joint_prior=np.zeros(3),
          log_joint_s_prior=np.zeros(3), params_ordered=['distance', 'psi']), 'ragged columns'),
])
def test_an_unusable_reserve_returns_none_so_the_caller_falls_back(broken, why):
    """None means "fall back to _rvs", which still works.  A raise here would take down an
    export that has already finished the expensive part."""
    assert reserve_distance_and_ln_weights(broken) is None, why


###
### 3. the builder must read either integrand convention
###

@pytest.mark.parametrize('columns,is_log', [
    ({'log_integrand': np.array([0.0, 1.0, 2.0]),
      'log_joint_prior': np.array([0.1, 0.1, 0.1]),
      'log_joint_s_prior': np.array([0.2, 0.2, 0.2])}, None),
    ({'integrand': np.array([0.0, 1.0, 2.0]),
      'joint_prior': np.exp(np.array([0.1, 0.1, 0.1])),
      'joint_s_prior': np.exp(np.array([0.2, 0.2, 0.2]))}, True),
    ({'integrand': np.exp(np.array([0.0, 1.0, 2.0])),
      'joint_prior': np.exp(np.array([0.1, 0.1, 0.1])),
      'joint_s_prior': np.exp(np.array([0.2, 0.2, 0.2]))}, False),
])
def test_the_reserve_builder_recovers_the_same_weights_on_either_convention(columns, is_log):
    """`integrand` is lnL on some backends and linear L on others, and only the sampler knows
    which.  Reading it under the wrong one is not a rounding error: it is a different posterior."""
    cols = dict(columns)
    cols['distance'] = np.array([100.0, 200.0, 300.0])
    cols['psi'] = np.array([0.1, 0.2, 0.3])

    reserve = make_reserve_from_rvs(cols, ['distance', 'psi'], integrand_is_log=is_log)
    d, ln_w = reserve_distance_and_ln_weights(reserve)

    order = np.argsort(d)
    assert np.allclose(d[order], [100.0, 200.0, 300.0])
    assert np.allclose(ln_w[order] - ln_w[order][0], [0.0, 1.0, 2.0]), \
        'the weights differ by more than an additive constant: the convention was misread'


def test_a_zero_weight_row_is_dropped_rather_than_becoming_minus_inf_ballast():
    cols = {'integrand': np.array([1.0, 0.0, 2.0]),
            'joint_prior': np.ones(3), 'joint_s_prior': np.ones(3),
            'distance': np.array([100.0, 200.0, 300.0])}
    reserve = make_reserve_from_rvs(cols, ['distance'], integrand_is_log=False)
    d, ln_w = reserve_distance_and_ln_weights(reserve)
    assert np.all(np.isfinite(ln_w))
    assert 200.0 not in set(d.tolist())


###
### 3b. the bounded reserve is a SAMPLE here, not a seed
###

def _skewed_rvs(n=4000, seed=0):
    """Rows whose importance weights have a real tail, so the peak row matters."""
    rng = np.random.default_rng(seed)
    d = rng.uniform(100.0, 900.0, size=n)
    ln_w = -0.5 * ((d - 400.0) / 40.0) ** 2
    return dict(distance=d, psi=rng.uniform(0, 1, n),
                log_integrand=ln_w, log_joint_prior=np.zeros(n),
                log_joint_s_prior=np.zeros(n))


def test_the_export_reserve_does_not_force_the_peak_row_in():
    """A seed wants the peak unconditionally -- it defines the seed's centre.  A WEIGHTED
    SAMPLE must not have it: forcing it gives the largest weight probability 1 where
    uniform subsampling gives it n_max/n_finite, and that one row then carries the export.
    """
    cols = _skewed_rvs()
    peak = float(cols['distance'][int(np.argmax(cols['log_integrand']))])

    seen_forced, seen_free = 0, 0
    for seed in range(30):
        X = np.vstack([cols['distance'], cols['psi']]).T
        common = dict(n_max=100, log_joint_prior=cols['log_joint_prior'],
                      log_joint_s_prior=cols['log_joint_s_prior'])
        forced = make_warm_seed_reserve(X, cols['log_integrand'], ['distance', 'psi'],
                                        rng=np.random.RandomState(seed), force_peak=True, **common)
        free = make_warm_seed_reserve(X, cols['log_integrand'], ['distance', 'psi'],
                                      rng=np.random.RandomState(seed), force_peak=False, **common)
        seen_forced += peak in set(forced['X'][:, 0].tolist())
        seen_free += peak in set(free['X'][:, 0].tolist())

    assert seen_forced == 30, 'force_peak=True must always keep the peak (the seed needs it)'
    assert seen_free < 30, 'force_peak=False still admits the peak every time'


def _spiked_population(n=8000, seed=3, boost=0.0):
    """Broad importance weights with one row optionally lifted above the rest."""
    rng = np.random.default_rng(seed)
    d = rng.uniform(100.0, 900.0, n)
    ln_w = -0.5 * ((d - 400.0) / 150.0) ** 2
    if boost:
        ln_w = ln_w.copy()
        ln_w[int(np.argmax(ln_w))] += boost
    X = np.vstack([d, rng.uniform(0, 1, n)]).T
    return X, ln_w


def _median_reserve_ess(X, ln_w, force, n_max=800, seeds=9):
    out = []
    for seed in range(seeds):
        r = make_warm_seed_reserve(X, ln_w, ['distance', 'psi'], n_max=n_max,
                                   log_joint_prior=np.zeros(len(ln_w)),
                                   log_joint_s_prior=np.zeros(len(ln_w)),
                                   rng=np.random.RandomState(seed), force_peak=force)
        out.append(_ess(reserve_distance_and_ln_weights(r)[1]))
    return float(np.median(out))


def test_forcing_the_peak_distorts_the_export_only_when_one_row_dominates():
    """Stated with its regime, because the effect is not a constant.  Uniform subsampling
    gives every row the same inclusion probability, which cancels in a normalized
    histogram; admitting one row at probability 1 does not.  Measured over a sweep of how
    far the top weight sits above the rest (cap 800 of 8000, median of 15 seeds): no
    dominant row, ESS 506 forced against 506 free; 3 e-folds up, 223 against 505; 6
    e-folds, 3.6 against 505.  A starved high-amplitude extrinsic pass is the last case."""
    X, flat = _spiked_population(boost=0.0)
    forced_flat, free_flat = _median_reserve_ess(X, flat, True), _median_reserve_ess(X, flat, False)
    assert abs(forced_flat - free_flat) < 0.05 * free_flat, \
        'forcing the peak moved a sample that has no dominant row'

    X, spiked = _spiked_population(boost=6.0)
    assert _median_reserve_ess(X, spiked, False) > 10.0 * _median_reserve_ess(X, spiked, True), \
        'the forced peak no longer dominates the reserve it is forced into'


def test_the_reserve_records_the_population_ess_the_subsample_cannot_show():
    """The other half of the same problem, and the reason dropping the forced peak is not
    on its own enough: an unbiased subsample that misses the dominant row reports a healthy
    effective sample size for a pass that has none."""
    X, spiked = _spiked_population(boost=9.0)
    population = _ess(spiked)

    r = make_warm_seed_reserve(X, spiked, ['distance', 'psi'], n_max=800,
                               log_joint_prior=np.zeros(len(spiked)),
                               log_joint_s_prior=np.zeros(len(spiked)),
                               rng=np.random.RandomState(0), force_peak=False)
    subsample = _ess(reserve_distance_and_ln_weights(r)[1])

    assert r['ess_finite'] is not None, 'the pre-cap effective sample size was not recorded'
    assert np.isclose(r['ess_finite'], population, rtol=1e-9), \
        'ess_finite describes the subsample, not the population'
    assert subsample > 10.0 * population, \
        'this subsample did not miss the dominant row, so it does not show the problem'


def test_the_warning_prefers_the_population_ess_over_the_subsample_it_was_handed():
    rng = np.random.default_rng(8)
    d = rng.uniform(100.0, 900.0, size=2000)
    ln_w = -0.5 * ((d - 500.0) / 300.0) ** 2          # a healthy-looking subsample

    assert distance_grid_resolution_warning(d, n_grid=50, ln_weights=ln_w) is None
    warned = distance_grid_resolution_warning(d, n_grid=50, ln_weights=ln_w,
                                              n_eff_population=2.1)
    assert warned is not None, 'the pass had n_eff 2.1 and the check believed the subsample'
    assert 'n_eff=2.1' in warned


def test_the_export_adapter_opts_out_and_records_that_it_did():
    cols = _skewed_rvs(n=3000)
    reserve = make_reserve_from_rvs(cols, ['distance', 'psi'], n_max=250)
    assert reserve['force_peak'] is False, 'the .dgrid reserve forces the peak row in'
    assert reserve['capped'] is True
    assert reserve['n_finite'] == 3000, 'the pre-cap population was not recorded'
    assert len(reserve['X']) <= 250


def test_an_uncapped_reserve_is_not_marked_as_a_subsample():
    cols = _skewed_rvs(n=300)
    reserve = make_reserve_from_rvs(cols, ['distance', 'psi'], n_max=20000)
    assert reserve['capped'] is False
    assert len(reserve['X']) == 300


###
### 4. a starved pass is still called starved once the rows look healthy
###

def test_more_distinct_distances_at_the_same_neff_does_not_silence_the_warning():
    """This is the trap in reading the retained set: thousands of distinct distances, the
    same handful of effective samples.  The duplicate-distance clause goes quiet; the
    effective-sample-size clause must not."""
    rng = np.random.default_rng(5)
    d = rng.uniform(100.0, 900.0, size=20000)          # all distinct
    ln_w = np.full(len(d), -700.0)
    ln_w[:6] = 0.0                                      # n_eff ~ 6

    warn = distance_grid_resolution_warning(d, n_grid=50, ln_weights=ln_w)

    assert warn is not None, 'a 50-bin grid from n_eff ~ 6 was reported as fine'
    assert 'distinct distance' not in warn, 'the wrong clause fired: the distances are distinct'
    assert 'n_eff' in warn


def test_the_warning_describes_the_rows_the_BUILDER_will_bin():
    """The helper and _weighted_blocks must filter the same way.

    _weighted_blocks drops every row whose normalized probability is not strictly
    positive.  A helper that filters only on isfinite is describing a different sample
    set, and the divergence is one-sided: the pass where a handful of weights survive and
    the rest underflow is the genuinely starved one, and it was the silent one.
    """
    ln_pi = np.zeros(5)

    # (a) three rows underflow to p == 0 exactly, and the three that survive share one
    #     distance -- so the builder cannot make a grid at all.
    d = np.array([100.0, 100.0, 100.0, 400.0, 500.0])
    ln_w = np.array([0.0, 0.0, 0.0, -1e4, -1e4])
    assert np.sum(np.exp(ln_w - np.max(ln_w)) > 0) == 3, 'the setup no longer underflows'
    with pytest.raises(ValueError):
        build_distance_grid(d, ln_w, 0.0, 0.0, {}, ln_prior_d_at_samples=ln_pi, n_grid=3)
    warn = distance_grid_resolution_warning(d, n_grid=3, ln_weights=ln_w)
    assert warn is not None, 'the builder refused this grid and the check called it fine'
    assert 'distinct distance' in warn

    # (b) the other direction: two usable rows, two bins, one effective sample per bin.
    #     Counting the three discarded rows instead reports a five-bin grid that is not
    #     what gets written.
    d = np.array([100.0, 200.0, 300.0, 400.0, 500.0])
    ln_w = np.array([0.0, 0.0, -1e4, -1e4, -1e4])
    grid = build_distance_grid(d, ln_w, 0.0, 0.0, {}, ln_prior_d_at_samples=ln_pi, n_grid=5)
    assert len(grid) == 2, 'the builder kept rows this test assumes it drops'
    assert distance_grid_resolution_warning(d, n_grid=5, ln_weights=ln_w) is None, \
        'flagged a two-row grid built from two effective samples'


def test_a_healthy_weighted_sample_draws_no_warning():
    rng = np.random.default_rng(6)
    d = rng.uniform(100.0, 900.0, size=20000)
    ln_w = -0.5 * ((d - 500.0) / 300.0) ** 2           # n_eff of order the sample size
    assert distance_grid_resolution_warning(d, n_grid=50, ln_weights=ln_w) is None


def test_both_clauses_report_together_when_both_apply():
    d = np.repeat([100.0, 200.0], 3)
    ln_w = np.array([0.0, -700.0, -700.0, -700.0, -700.0, -700.0])
    warn = distance_grid_resolution_warning(d, n_grid=6, ln_weights=ln_w)
    assert warn is not None and 'distinct distance' in warn and 'n_eff' in warn


###
### 5. every site, including the two that need a GPU or torch to run
###

def _fairdraw_sites(src):
    """(line, text) for every in-place fair-draw gather in a sampler module."""
    return [(i + 1, ln) for i, ln in enumerate(src.splitlines()) if 'replace=True' in ln]


@pytest.mark.parametrize('module', ['mcsampler.py', 'mcsamplerGPU.py', 'mcsamplerEnsemble.py',
                                    'mcsamplerNFlow.py', 'mcsamplerAdaptiveVolume.py',
                                    'mcsamplerPortfolio.py'])
def test_every_fair_draw_site_keeps_the_retained_rows_first(module):
    """The sweep, because the four new sites were wired by one patcher: a single mistake
    would be replicated, and two of them (GPU, NFlow) cannot be driven on this host."""
    path = os.path.join(_INTEGRATORS, module)
    src = open(path).read()
    sites = _fairdraw_sites(src)
    assert sites, '{}: no fair draw found; this sweep is looking at the wrong thing'.format(module)
    lines = src.splitlines()
    for line_no, _ in sites:
        before = '\n'.join(lines[:line_no])
        assert ('keep_reserve_from_rvs(' in before
                or 'make_warm_seed_reserve(' in before), \
            '{}: the fair draw at line {} discards the retained rows with no reserve'.format(
                module, line_no)


def _dgrid_export_block(src):
    """The AST subtree of the ILE's .dgrid export, or None.

    PARSED, not sliced.  A window of source text around the call matched the import line
    at the top of the block and the comment that quotes `is_equal_weight()` by name --
    so two mutations that sent the exporter straight back to the fair draw left this test
    green.  Comments and imports are not code; ast does not see them.
    """
    for node in ast.walk(ast.parse(src)):
        if not isinstance(node, ast.If):
            continue
        if 'export_marginal_distance_grid' not in ast.dump(node.test):
            continue
        if 'build_distance_grid' in _called_names(node):
            return node
    return None


def _called_names(node):
    """Every function/method NAME called anywhere under this node."""
    out = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            if isinstance(n.func, ast.Name):
                out.add(n.func.id)
            elif isinstance(n.func, ast.Attribute):
                out.add(n.func.attr)
    return out


def _keywords_of_call(node, name):
    for n in ast.walk(node):
        if isinstance(n, ast.Call) and getattr(n.func, 'id', None) == name:
            return set(kw.arg for kw in n.keywords)
    return set()


@pytest.mark.skipif(not os.path.exists(_ILE), reason='ILE executable not in this tree')
def test_the_ile_prefers_the_retained_set_and_asks_the_pooling_safe_question():
    block = _dgrid_export_block(open(_ILE).read())
    assert block is not None, 'no .dgrid export block found; this test is looking at nothing'
    called = _called_names(block)

    assert 'reserve_distance_and_ln_weights' in called, \
        'the .dgrid exporter still builds the curve from the fair draw'
    assert 'is_equal_weight' in called, \
        'a POOLED record is resampled per block; one replica reserve is not its retained set'
    assert 'rows_are_resampled' not in called, \
        'rows_are_resampled() is true for a pooled record, whose reserve is one replica only'
    assert 'ln_weights_for_posterior' in called, \
        'the fall-back for a sampler with no reserve is gone'
    assert 'ln_weights' in _keywords_of_call(block, 'distance_grid_resolution_warning'), \
        'the resolution check no longer sees the weights, so it cannot see a starved pass'
    assert "'capped'" in ast.dump(block), \
        'nothing tells the user the curve came from a SUBSAMPLE of the retained rows'
    assert 'n_eff_population' in _keywords_of_call(block, 'distance_grid_resolution_warning'), \
        'the check reads the subsample own n_eff, which a capped reserve can overstate'
    assert "'ess_finite'" in ast.dump(block), \
        'the pre-cap effective sample size never reaches the check'
