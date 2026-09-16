"""Analytic supplementary likelihood factor with a CLOSED-FORM marginal.

Used by test_e2e_analytic_pipeline.py, and importable on its own so the same factor can be
pointed at a hand-run pipeline:

    --supplementary-likelihood-factor-code analytic_supplement_for_e2e \\
    --supplementary-likelihood-factor-function ln_analytic_phi_factor

ILE calls the factor as f(ra, dec, phi_orb, inclination, psi, distance) and ADDS it to lnL.
Under --zero-likelihood the signal term is exactly 0, so the marginal likelihood ILE reports
for every intrinsic grid point is

    ln Z = ln E_prior[ exp(A cos(phi_orb)) ] = ln I0(A)

because phi_orb's prior is uniform on [0, 2pi) and the factor depends on nothing else.  I0 is
the modified Bessel function of the first kind.  That gives a full pipeline run an exact
expected answer with no fit error and no MC scatter in the TARGET -- the constant-integrand
technique of the sampler unit tests, lifted to the pipeline.

A is read from E2E_A_COEFF so one module serves both a nearly-flat target (small A) and a
sharply peaked one (large A); a sampler can be right on one and wrong on the other.
"""
import os

import numpy as np

A_COEFF = float(os.environ.get("E2E_A_COEFF", "0.75"))


def ln_analytic_phi_factor(right_ascension, declination, phi_orb, inclination, psi, distance):
    return A_COEFF * np.cos(phi_orb)


def exact_ln_Z(a=None):
    """ln I0(A): the exact marginal of the factor above against a uniform phi_orb prior."""
    from scipy.special import i0
    return float(np.log(i0(A_COEFF if a is None else a)))
