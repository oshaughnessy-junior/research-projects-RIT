"""pseudo_pipe's CIP-line rewrites must leave mass coordinates in an order CIP accepts.

The rewrite blocks of util_RIFT_pseudo_pipe.py and util_RIFT_pseudo_pipe_lowlatency.py are
run over the CIP line shapes helper_LDG_Events.py emits, for every combination of the flags
that touch mass coordinates.  CIP calls check_mass_coordinate_order on parameter+nofit, so a
line that fails it here fails at CIP startup.
"""
import itertools
import os
import textwrap
import types

import pytest

from RIFT import lalsimutils

BIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'bin')

# CIP line shapes from helper_LDG_Events.py
HELPER_LINES = {
    'std': "--parameter mc --parameter delta_mc --parameter-implied xi --parameter-nofit s1z --parameter-nofit s2z",
    'mu': "--parameter-implied mu1 --parameter-implied mu2 --parameter-nofit mc --parameter delta_mc --parameter-nofit s1z --parameter-nofit s2z",
    'quadratic': "--fit-method quadratic --parameter mc --parameter eta --parameter xi",
    'tides': "--parameter mc --parameter delta_mc --parameter-implied xi --parameter-nofit s1z --parameter-nofit s2z --input-tides --parameter-implied LambdaTilde --parameter-nofit lambda2 --parameter-nofit lambda1",
}

FLAGS = ['cip_internal_use_eta_in_sampler', 'quadratic_fit', 'use_quadratic_early', 'use_mtot_coords',
         'hierarchical_merger_prior_1g', 'hierarchical_merger_prior_2g']


def _block(script, start, stop):
    src = open(os.path.join(BIN, script)).read().splitlines()
    i0 = next(i for i, l in enumerate(src) if start in l)
    i1 = next(i for i, l in enumerate(src) if i > i0 and stop in l)
    return compile(textwrap.dedent('\n'.join(src[i0:i1])), script, 'exec')


BLOCKS = {
    'pseudo_pipe': _block('util_RIFT_pseudo_pipe.py', 'if opts.cip_internal_use_eta_in_sampler:',
                          'elif opts.assume_highq and'),
    'lowlatency': _block('util_RIFT_pseudo_pipe_lowlatency.py', 'if opts.hierarchical_merger_prior_1g:',
                         'elif opts.assume_highq and'),
}


def _low_level(line):
    toks = line.split()
    p = [b for a, b in zip(toks, toks[1:]) if a == '--parameter']
    nf = [b for a, b in zip(toks, toks[1:]) if a == '--parameter-nofit']
    return p + nf if p else nf


def _combos():
    for combo in itertools.product([False, True], repeat=len(FLAGS)):
        d = dict(zip(FLAGS, combo))
        if not (d['hierarchical_merger_prior_1g'] and d['hierarchical_merger_prior_2g']):
            yield d


@pytest.mark.parametrize("script", sorted(BLOCKS))
def test_rewritten_cip_lines_pass_the_mass_order_check(script):
    failures, n_mtot = [], 0
    for d in _combos():
        opts = types.SimpleNamespace(
            cip_internal_use_eta_in_sampler=d['cip_internal_use_eta_in_sampler'],
            cip_fit_method='quadratic' if d['quadratic_fit'] else None,
            use_quadratic_early=d['use_quadratic_early'], use_cov_early=False,
            force_lambda_no_linear_init=False, use_mtot_coords=d['use_mtot_coords'],
            hierarchical_merger_prior_1g=d['hierarchical_merger_prior_1g'],
            hierarchical_merger_prior_2g=d['hierarchical_merger_prior_2g'],
            assume_highq=False, internal_correlate_default=False)
        for name, base in HELPER_LINES.items():
            for indx in (0, 1):
                ns = {'opts': opts, 'line': ' ' + base + ' ', 'indx': indx}
                exec(BLOCKS[script], ns)
                low = _low_level(ns['line'])
                n_mtot += 'mtot' in low
                try:
                    lalsimutils.check_mass_coordinate_order(low)
                except ValueError as exc:
                    failures.append((name, indx, {k for k, v in d.items() if v}, low, str(exc)))
    assert n_mtot > 0  # premise: the mtot rewrites ran
    assert not failures, failures[:5]
