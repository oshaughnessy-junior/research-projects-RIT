#!/usr/bin/env python3
"""Replay legacy inputs against the frozen NoLoop body from a named Git commit.

Run inside a checkout with the candidate package first on PYTHONPATH. This
verification compares an independent historical body, not a wrapper composed
from the new hook. A source-only component prototype must not alter old inputs.
"""
import argparse
import ast
import importlib.util
import json
from pathlib import Path
import subprocess

import numpy as np
from RIFT.likelihood import factored_likelihood as fl

SOURCE = 'MonteCarloMarginalizeCode/Code/RIFT/likelihood/factored_likelihood.py'
NAME = 'DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop'


def execute(ref, output):
    original = subprocess.check_output(['git', 'show', ref + ':' + SOURCE], text=True)
    tree = ast.parse(original)
    body = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name == NAME)
    namespace = dict(vars(fl))
    exec(compile(ast.Module(body=[body], type_ignores=[]), ref + ':' + SOURCE, 'exec'), namespace)
    baseline = namespace[NAME]
    spec = importlib.util.spec_from_file_location('basis_fixture',
        Path(__file__).parents[1] / 'test_source_angular_basis.py')
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    problem = fixture.problem.__wrapped__()
    legacy = fixture.legacy_problem(problem)
    samples = []
    for incl, phase, psi, distance in [(1.1, .7, .4, 1.), (.4, 1.3, .9, 1.4),
                                      (2.2, 2.5, -.2, .7), (1.7, -.4, 1.2, 2.)]:
        p = problem.p.manual_copy()
        p.incl, p.phiref, p.psi = incl, phase, psi
        p.dist *= distance
        samples.append(p)
    def original_call(*args, **kwargs):
        kwargs.pop('source_angular_basis')
        return baseline(*args, **kwargs)
    candidate = fixture.evaluate(legacy, samples, [-.00173, 0., .00217], angular=None)
    expected = fixture.evaluate(legacy, samples, [-.00173, 0., .00217],
                                function=original_call, angular=None)
    np.testing.assert_array_equal(candidate, expected)
    receipt = dict(baseline=ref, function=NAME, detector_count=2,
                   sample_count=len(samples), time_count=3, time_interp='cubic',
                   bitwise_equal=True, maximum_difference=float(np.max(abs(candidate-expected))))
    if output:
        output.write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-ref', required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    execute(args.baseline_ref, args.output)
