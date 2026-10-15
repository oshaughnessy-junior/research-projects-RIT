"""Exact coverage calculation and counterexample; no neural/likelihood work."""
from fractions import Fraction
import math


def original_pair_coverage():
    return dict(full_interval_probability=float(Fraction(1,1024)),
        expected_full_interval_rows_in_6000_batches=float(Fraction(6000*128,1024)),
        span_at_least_25_of_32_probability=float(sum(Fraction(8-i,32*(32-i)) for i in range(8))),
        mean_span=float(sum(Fraction(33-i,64*32) for i in range(32))))


def semigroup_counterexample():
    # Teacher identity; candidate constant translation .4 in one of six axes.
    # Candidate is an exact semigroup but is the wrong teacher flow.
    return dict(semigroup_rms=0.,diagonal_rms=.4/math.sqrt(6),one_step_rms=.4/math.sqrt(6))
