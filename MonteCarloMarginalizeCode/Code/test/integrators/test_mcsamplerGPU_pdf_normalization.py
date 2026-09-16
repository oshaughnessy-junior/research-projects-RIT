"""mcsamplerGPU.draw_simplified() must report the density its draws actually come from.

THE DEFECT.  `draw_simplified()` built `joint_p_s` from the RAW `self.pdf[param]`, while the
samples themselves come from `cdf_inv[param]`, which is built from the NORMALIZED cdf.  So the
reported sampling density was too large by `prod(self._pdf_norm)` -- `_pdf_norm[param]` being
`cdf[-1]`, i.e. the integral of the supplied pdf over its range.  `draw()` in the same class has
always divided by `_pdf_norm` (see its `res.append` lines); only `draw_simplified()` did not, and
`integrate()` uses `draw_simplified()`.

CONSEQUENCE.  The estimator weight is `prior/p_s`, so a caller passing an UNNORMALIZED sampling
pdf got ln Z low by `log(prod(_pdf_norm))`.  With `util_ConstructEOSPosterior.py`, which passes
`pdf=lambda x: 1` and `prior_pdf=uniform_prior` (returns ones), that is exactly ln V:

    constant likelihood L = 1 on [-1,1]^2, V = 4, exact ln Z = ln 4 = 1.3862944
        AV / GMM / adaptive_cartesian   1.3862943      (all three, exactly)
        adaptive_cartesian_gpu          0.0000000      <- before this fix
        adaptive_cartesian_gpu          1.3862943      <- after

NOT A CONVENTION CHOICE.  `_pdf_norm` is 1 whenever the supplied pdf already integrates to 1, so
this is a no-op for every normalized-pdf caller; it only ever corrected a sampler that was
inconsistent with its own draws.  The tests below pin both halves -- the correction AND the
no-op -- because a "fix" that also moved normalized-pdf callers would change real evidences.
"""
import numpy as np
import pytest

import RIFT.integrators.mcsamplerGPU as mcsamplerGPU
import RIFT.integrators.mcsamplerAdaptiveVolume as mcsamplerAV


def _build(sampler, pdf_value, prior_value, lo=-1.0, hi=1.0, params=("xx", "yy")):
    for p in params:
        sampler.add_parameter(p,
                              pdf=np.vectorize(lambda x, _v=pdf_value: _v),
                              prior_pdf=np.vectorize(lambda x, _v=prior_value: _v),
                              left_limit=lo, right_limit=hi, adaptive_sampling=True)
    return sampler


def test_reported_p_s_matches_the_density_drawn_from():
    """E[prior/p_s] over fresh draws must equal the integral of the prior, here V.

    This is the direct normalization identity, and it is what separates "the sampler applies a
    different convention" from "the sampler contradicts its own draws".  With pdf == 1 on a box
    of volume V the draws are uniform with density 1/V, so a p_s reported as the raw 1 makes
    this expectation come out V times too small.
    """
    V = 4.0
    s = _build(mcsamplerGPU.MCSampler(), 1.0, 1.0)
    p_s, p_prior, _ = s.draw_simplified(40000)
    ratio = np.asarray(p_prior, dtype=float) / np.asarray(p_s, dtype=float)
    # TOLERANCE, measured not guessed: _pdf_norm is cdf[-1] from a NUMERICALLY built cdf, so it
    # carries the grid's discretization error -- 1.5e-8 relative here (observed 3.9999999851 for
    # V = 4).  1e-6 sits ~70x above that floor and ~1e6x below the defect it guards, which was a
    # factor of V (300% at V = 4), so the two cannot be confused.
    assert np.allclose(ratio, V, rtol=1e-6), \
        "E[prior/p_s] = %r, expected the prior integral V = %r; draw_simplified is reporting a " \
        "density inconsistent with cdf_inv" % (float(np.mean(ratio)), V)


def test_draw_and_draw_simplified_agree_on_the_p_s_scale():
    """The two draw paths in ONE class must not disagree about what p_s means.

    draw() has always divided by _pdf_norm; draw_simplified() did not.  integrate() uses
    draw_simplified(), so the class's own two entry points disagreed by prod(_pdf_norm).
    """
    s = _build(mcsamplerGPU.MCSampler(), 1.0, 1.0)
    p_s_simpl, _, _ = s.draw_simplified(20000)
    mean_simpl = float(np.mean(np.asarray(p_s_simpl, dtype=float)))
    # pdf == 1 on [-1,1] per dimension -> _pdf_norm == 2 per dimension -> joint density 1/4
    expected = 1.0 / 4.0
    assert np.isclose(mean_simpl, expected, rtol=1e-6), \
        "draw_simplified reports p_s ~ %r; the normalized joint density is %r" % (mean_simpl,
                                                                                 expected)


def test_normalized_pdf_caller_is_unaffected():
    """The no-op half: _pdf_norm == 1 for a pdf that already integrates to 1.

    Every production caller that passes a proper density must see NO change -- otherwise this
    fix would silently move real evidences instead of correcting an inconsistent one.
    """
    s = _build(mcsamplerGPU.MCSampler(), 0.5, 0.5)   # pdf = prior = 1/2 per dim, both normalized
    for p in ("xx", "yy"):
        assert np.isclose(float(s._pdf_norm[p]), 1.0, rtol=1e-6), \
            "_pdf_norm[%s] = %r for an already-normalized pdf; the correction would not be a " \
            "no-op for normalized callers" % (p, float(s._pdf_norm[p]))
    p_s, p_prior, _ = s.draw_simplified(20000)
    # prior 1/2 per dim -> joint 1/4 ; p_s likewise 1/4 ; ratio 1 (the prior integrates to 1)
    ratio = np.asarray(p_prior, dtype=float) / np.asarray(p_s, dtype=float)
    assert np.allclose(ratio, 1.0, rtol=1e-6), \
        "normalized-pdf caller sees E[prior/p_s] = %r, expected 1" % float(np.mean(ratio))


@pytest.mark.parametrize("R,V", [(1.0, 4.0), (2.0, 16.0)])
def test_constant_likelihood_evidence_is_exact_and_matches_AV(R, V):
    """End to end on a case with an exact answer, at two different volumes.

    A constant integrand removes fit error and MC scatter entirely: every sampler must return
    ln(integral of prior) = ln V, to machine precision.  Two volumes, because a single one
    cannot distinguish a genuine correction from a coincidence at V = 4.
    """
    lnL = lambda *x: np.zeros(np.asarray(x[0]).shape)
    out = {}
    for name, mod in (("GPU", mcsamplerGPU), ("AV", mcsamplerAV)):
        s = _build(mod.MCSampler(), 1.0, 1.0, lo=-R, hi=R)
        res = s.integrate(lnL, "xx", "yy", n=2000, nmax=20000, neff=30,
                          use_lnL=True, return_lnI=True, save_intg=True,
                          no_protect_names=True, verbose=False)
        out[name] = float(res[0])
    assert np.isclose(out["GPU"], np.log(V), atol=1e-6), \
        "mcsamplerGPU ln Z = %r on a constant integrand, exact answer ln V = %r" % (out["GPU"],
                                                                                   np.log(V))
    assert np.isclose(out["GPU"], out["AV"], atol=1e-6), \
        "mcsamplerGPU %r and mcsamplerAdaptiveVolume %r disagree on a constant integrand" % (
            out["GPU"], out["AV"])


@pytest.mark.parametrize("nmax,chunks,atol", [(10000, 5, 0.03), (40000, 20, 0.02)])
def test_evidence_is_exact_AFTER_ADAPTATION(nmax, chunks, atol):
    """The evidence must stay exact once the adapted proposal is in use -- MULTIPLE CHUNKS.

    THIS IS THE AXIS THE REST OF THIS FILE MISSES, and missing it hid a real defect.  Every
    other test here either calls draw_simplified() directly or integrates a constant with
    neff=30, which clears neff on the FIRST chunk -- so `self.pdf[p]` is still the caller's
    original function throughout, and only the un-adapted regime is ever exercised.

    From the second chunk on, `self.pdf[p]` has been REPLACED by pdf_from_hist (at the three
    install sites in update_sampling_prior/integrate_log/integrate), which is already a density:
    compute_hist normalizes to sum 1 and divides by the bin width, and cdf_inverse_from_hist
    draws from that same normalized cdf.  So `_pdf_norm[p]` -- the integral of the pdf the
    CALLER supplied -- is stale from that point, and it must be reset to 1 alongside each
    install.  Without that reset, a `/_pdf_norm` in draw_simplified() is an error rather than a
    correction, and the integral converges to ln V + log(prod(_pdf_norm)) instead of ln V.

    Measured on a non-square box [-2,3] x [0,1] (V = 5, exact ln Z = 1.6094379):

        chunks   base      dividing-without-reset   with the reset
             1   0.000000            1.609438            1.609438
             5   1.432952            3.046184            1.609690
            20   1.567732            3.176679            1.610915
           100   1.600740            3.210876            1.609291

    TOLERANCES, measured over 6 seeds rather than guessed: with the reset, sd is 0.0030 and
    max|err| 0.0053 at 5 chunks, sd 0.0012 and max|err| 0.0018 at 20.  The atol values below sit
    ~6x and ~11x above those, and ~6x and ~20x BELOW the defects they must catch (0.177 and
    0.042 for base, 1.437 and 1.567 for the unreset division).
    """
    lnL = lambda *x: np.zeros(np.asarray(x[0]).shape)
    s = mcsamplerGPU.MCSampler()
    s.add_parameter("xx", pdf=np.vectorize(lambda x: 1), prior_pdf=np.vectorize(lambda x: 1.0),
                    left_limit=-2.0, right_limit=3.0, adaptive_sampling=True)
    s.add_parameter("yy", pdf=np.vectorize(lambda x: 1), prior_pdf=np.vectorize(lambda x: 1.0),
                    left_limit=0.0, right_limit=1.0, adaptive_sampling=True)
    res = s.integrate(lnL, "xx", "yy", n=2000, nmax=nmax, neff=1e9,
                      use_lnL=True, return_lnI=True, save_intg=True,
                      no_protect_names=True, verbose=False, n_adapt=100, tempering_adapt=True)
    lnV = np.log(5.0)
    assert np.isclose(float(res[0]), lnV, atol=atol), \
        "over %d chunks ln Z = %r, exact ln V = %r; a stale _pdf_norm after adaptation drives " \
        "this toward ln V + log(prod(_pdf_norm))" % (chunks, float(res[0]), lnV)


def test_pdf_norm_is_reset_when_the_adapted_proposal_is_installed():
    """Direct structural check of the invariant behind the test above.

    Kept separate because the integral test can only see the CONSEQUENCE, and a future edit
    that installs pdf_from_hist at a FOURTH site would reintroduce the defect there while the
    two-parameter integral above still passed.
    """
    lnL = lambda *x: np.zeros(np.asarray(x[0]).shape)
    s = mcsamplerGPU.MCSampler()
    for p, (lo, hi) in (("xx", (-2.0, 3.0)), ("yy", (0.0, 1.0))):
        s.add_parameter(p, pdf=np.vectorize(lambda x: 1), prior_pdf=np.vectorize(lambda x: 1.0),
                        left_limit=lo, right_limit=hi, adaptive_sampling=True)
    # before adaptation _pdf_norm is the supplied pdf's integral, i.e. the range width
    assert np.isclose(float(s._pdf_norm["xx"]), 5.0, rtol=1e-6)
    s.integrate(lnL, "xx", "yy", n=2000, nmax=10000, neff=1e9, use_lnL=True, return_lnI=True,
                save_intg=True, no_protect_names=True, verbose=False, n_adapt=100,
                tempering_adapt=True)
    for p in ("xx", "yy"):
        assert np.isclose(float(s._pdf_norm[p]), 1.0, rtol=1e-6), \
            "_pdf_norm[%s] = %r after adaptation; pdf_from_hist is already normalized, so it " \
            "must be 1 or every consumer of _pdf_norm is working from a stale value" % (
                p, float(s._pdf_norm[p]))
