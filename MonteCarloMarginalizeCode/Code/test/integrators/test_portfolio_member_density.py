"""A portfolio member must report its sampling density as a NORMALIZED density.

THE DEFECT.  mcsamplerPortfolio builds the balance-heuristic mixture denominator
q_mix = sum_m frac_m q_m from each member's sampling_density(X).  mcsamplerGPU did not
implement that method, so every portfolio containing one fell back to the legacy STRATIFIED
denominator, which uses each sample's own member joint_p_s from draw_simplified().  That is
unbiased only if every member's joint_p_s is a normalized density -- and mcsamplerAdaptiveVolume
deliberately reports V_s/V, which is V_s**2 times the density it draws from, compensating inside
its own integrate_log.  Mixing the two pooled weights on scales differing by V_s**2.

    constant likelihood L = 1 on [-1,1]^2, prior_pdf == 1, exact ln Z = ln V = 1.386294
        [AV]                       1.386294      (exact, then and now)
        [adaptive_cartesian_gpu]   1.386294      (exact, after PR #354)
        [AV, adaptive_cartesian_gpu]
                                   0.753772      <- before this fix
                                   1.386294      <- after

0.753772 is not a mysterious number: half the 2000 draws came from AV with weight
prior/p_s = 1/(V_s/V) = 0.25 and half from GPU with weight V = 4, and log((0.25+4)/2) =
0.7537718.  That arithmetic is what identifies this as a member-contract problem rather than
either sampler's bug -- each is exact on its own scale.

SECOND DEFECT, same contract.  mcsamplerGPU swaps self.pdf[p] for the adaptive histogram
proposal, which compute_hist already normalizes to integrate to 1 over the box, but left
_pdf_norm[p] at the ANALYTIC pdf's mass.  pdf/_pdf_norm -- what draw(), draw_simplified() and
now sampling_density() all report -- was then too small by that mass once adaptation started
driving the draws, and ln Z came out HIGH by log(prod(_pdf_norm)).  Invisible to a run that
converges in one chunk, which is why the constant-integrand test above does not see it: it needs
an integrand demanding enough to adapt.  A caller passing an already-normalized pdf (production
CIP passes pdf = 1/range) has _pdf_norm == 1 and is unaffected either way.

WHAT IS NOT CHANGED.  mcsamplerAdaptiveVolume's draw_simplified still reports V_s/V.  Its
sampling_density() -- the contract -- already returned the true density, and its own evidence
must not move.
"""
import numpy as np
import pytest

import RIFT.integrators.mcsamplerGPU as mcsamplerGPU
import RIFT.integrators.mcsamplerAdaptiveVolume as mcsamplerAV
import RIFT.integrators.mcsamplerEnsemble as mcsamplerGMM
import RIFT.integrators.mcsamplerPortfolio as mcsamplerPortfolio

# a genuinely non-square box, so nothing here can be satisfied by tuning to V = 4
BOX = ((-1.0, 3.0), (-2.0, 0.5))
V_BOX = (BOX[0][1] - BOX[0][0]) * (BOX[1][1] - BOX[1][0])    # 10.0

CONSTANT_LNL = lambda *x: np.zeros(np.asarray(x[0]).shape)


def _add_params(sampler, box=BOX):
    for p, (lo, hi) in zip(("xx", "yy"), box):
        sampler.add_parameter(p,
                              pdf=np.vectorize(lambda x: 1.0),
                              prior_pdf=np.vectorize(lambda x: 1.0),
                              left_limit=lo, right_limit=hi, adaptive_sampling=True)
    return sampler


def _portfolio(mods, box=BOX):
    s = mcsamplerPortfolio.MCSampler(portfolio=[m.MCSampler() for m in mods])
    _add_params(s, box=box)
    s.setup()
    return s


def _lnZ(mods, box=BOX, seed=4321):
    np.random.seed(seed)
    res = _portfolio(mods, box=box).integrate(
        CONSTANT_LNL, "xx", "yy", n=2000, nmax=20000, neff=30,
        use_lnL=True, return_lnI=True, save_intg=True,
        no_protect_names=True, verbose=False)
    return float(res[0])


# ---------------------------------------------------------------------------------------
# the mixture itself

@pytest.mark.parametrize("box", [BOX, ((-1.0, 1.0), (-1.0, 1.0))])
def test_mixed_backend_portfolio_is_exact_on_a_constant_integrand(box):
    """The headline.  A constant integrand has no fit error and no MC scatter, so a
    normalization error cannot hide behind noise: the answer is ln(prior mass), exactly.

    Two boxes, one of them non-square, because the pre-fix error was a function of the box
    (-0.632523 at V = 4, -0.683197 at V = 10) and a single volume cannot tell a correction from
    a coincidence."""
    V = (box[0][1] - box[0][0]) * (box[1][1] - box[1][0])
    got = _lnZ([mcsamplerAV, mcsamplerGPU], box=box)
    assert np.isclose(got, np.log(V), atol=1e-6), \
        "mixed AV+GPU portfolio ln Z = %r on a constant integrand over a box of volume %r; " \
        "exact answer is ln V = %r" % (got, V, np.log(V))


def test_single_backend_portfolios_do_not_move():
    """The other direction, and the reason this is a fix rather than a regression: every
    single-backend portfolio was ALREADY exact, so it must be exactly where it was."""
    for mods, label in (([mcsamplerAV], "AV"), ([mcsamplerGPU], "GPU")):
        got = _lnZ(mods)
        assert np.isclose(got, np.log(V_BOX), atol=1e-6), \
            "%s-only portfolio ln Z = %r, exact ln V = %r" % (label, got, np.log(V_BOX))


def test_member_order_does_not_matter():
    """q_mix is symmetric in its members; the stratified fallback happened to be too, but only
    because the two members drew equal counts.  Pin the symmetry."""
    a = _lnZ([mcsamplerAV, mcsamplerGPU])
    b = _lnZ([mcsamplerGPU, mcsamplerAV])
    assert np.isclose(a, b, atol=1e-6), \
        "portfolio ln Z depends on member ORDER: %r vs %r" % (a, b)


def test_gpu_gmm_mixture_is_exact_too():
    """Not an AV-specific patch: mcsamplerEnsemble already implemented sampling_density, and a
    GPU+GMM portfolio could not form q_mix either, for the same missing method."""
    got = _lnZ([mcsamplerGMM, mcsamplerGPU])
    assert np.isclose(got, np.log(V_BOX), atol=1e-6), \
        "mixed GMM+GPU portfolio ln Z = %r, exact ln V = %r" % (got, np.log(V_BOX))


# ---------------------------------------------------------------------------------------
# the contract, stated as a property of each member

def test_gpu_sampling_density_integrates_to_one():
    """The contract: sampling_density must be a normalized density over the box.  Checked by
    quadrature on a grid, not by sampling, so this cannot be satisfied by an offsetting error."""
    s = _add_params(mcsamplerGPU.MCSampler())
    nx, ny = 401, 397
    xg = np.linspace(BOX[0][0], BOX[0][1], nx)
    yg = np.linspace(BOX[1][0], BOX[1][1], ny)
    XX, YY = np.meshgrid(xg, yg, indexing="ij")
    q = s.sampling_density(np.column_stack([XX.ravel(), YY.ravel()])).reshape(nx, ny)
    total = np.trapz(np.trapz(q, yg, axis=1), xg)
    assert np.isclose(total, 1.0, rtol=1e-5), \
        "mcsamplerGPU.sampling_density integrates to %r over its own box, not 1" % total


def test_gpu_sampling_density_matches_its_own_draws():
    """sampling_density(X) and draw_simplified()'s joint_p_s must be the SAME function.  A
    member whose two answers disagree makes q_mix inconsistent with the pool it denominates."""
    s = _add_params(mcsamplerGPU.MCSampler())
    p_s, _, rv = s.draw_simplified(5000, "xx", "yy")
    X = np.asarray(rv, dtype=float).T
    q = s.sampling_density(X)
    assert np.allclose(np.asarray(p_s, dtype=float), q, rtol=1e-8), \
        "sampling_density disagrees with draw_simplified's joint_p_s on the sampler's own draws"


def test_gpu_sampling_density_matches_its_own_draws_after_adaptation():
    """The same agreement, but with the HISTOGRAM proposal supplying the draws.

    The check above runs on a fresh sampler, where both paths read the analytic pdf.  Nothing
    there would notice if the two diverged once self.pdf[p] became pdf_from_hist -- which is
    exactly the state the second defect lived in, and exactly the state a long portfolio run
    spends almost all of its chunks in."""
    s = _add_params(mcsamplerGPU.MCSampler())
    peak = np.array([0.4, -0.8])
    lnL = lambda *x: -0.5*sum(((np.asarray(xi) - c)/0.25)**2 for xi, c in zip(x, peak))
    np.random.seed(7)
    s.integrate(lnL, "xx", "yy", n=1000, nmax=8000, neff=2000, use_lnL=True, return_lnI=True,
                save_intg=True, no_protect_names=True, verbose=False)
    # GUARD: if adaptation never fired, both paths still read the analytic pdf and this test
    # silently degenerates into the one above.  pdf_from_hist is wrapped in a local closure,
    # so identity against pdf_initial is the predicate that is actually available.
    assert all(s.pdf[p] is not s.pdf_initial[p] for p in s.params_ordered), \
        "the histogram proposal never replaced the analytic pdf; this test is not testing " \
        "what it claims"
    p_s, _, rv = s.draw_simplified(4000, "xx", "yy", save_no_samples=True)
    q = s.sampling_density(np.asarray(rv, dtype=float).T)
    assert np.allclose(np.asarray(p_s, dtype=float), q, rtol=1e-8), \
        "after adaptation, sampling_density and draw_simplified's joint_p_s disagree"


def test_gpu_sampling_density_is_zero_outside_the_box():
    """pdf_from_hist clamps its bin index, so a point outside the box would otherwise be given
    the edge bin's density.  cdf_inv cannot produce such a point, so the density there is 0."""
    s = _add_params(mcsamplerGPU.MCSampler())
    outside = np.array([[BOX[0][0] - 1.0, 0.0], [0.0, BOX[1][1] + 1.0], [99.0, 99.0]])
    q = s.sampling_density(outside)
    assert np.all(q == 0.0), "sampling_density is nonzero outside the box: %r" % (q,)


def test_AV_reported_p_s_is_left_alone():
    """AV's draw_simplified p_s is NOT a density and is deliberately not changed: its own
    integrate_log is written against that scale.  This test exists so a later 'cleanup' that
    normalizes it has to argue with a named decision instead of a silent convention."""
    s = _add_params(mcsamplerAV.MCSampler())
    s.setup()
    p_s, _, _ = s.draw_simplified(2000)
    reported = float(np.mean(np.asarray(p_s, dtype=float)))
    assert np.isclose(reported, V_BOX / s.V, rtol=1e-8), \
        "mcsamplerAdaptiveVolume.draw_simplified now reports %r, not V_s/V = %r; if that is " \
        "intended, its integrate_log and this decision both have to change" % (
            reported, V_BOX / s.V)
    # ... and its sampling_density, the actual contract, IS the density
    X = np.random.uniform([b[0] for b in BOX], [b[1] for b in BOX], size=(500, 2))
    q = s.sampling_density(X)
    assert np.allclose(q, 1.0 / (V_BOX * s.V), rtol=1e-8), \
        "mcsamplerAdaptiveVolume.sampling_density is not 1/(V_s*V)"


# ---------------------------------------------------------------------------------------
# the second defect: the adaptive histogram's normalization

def test_gpu_evidence_is_exact_once_adaptation_drives_the_draws():
    """Forces many chunks, so the histogram proposal -- not the analytic pdf -- supplies the
    draws.  compute_hist already normalizes, so _pdf_norm must become 1 when the swap happens;
    leaving it at the analytic pdf's mass made ln Z HIGH by log(prod(_pdf_norm)) (+1.386 on this
    box, measured).  A one-chunk run cannot see this."""
    lo, hi = -1.0, 3.0
    s = mcsamplerGPU.MCSampler()
    s.add_parameter("xx", pdf=np.vectorize(lambda x: 1.0),       # UNNORMALIZED -> _pdf_norm = 4
                    prior_pdf=np.vectorize(lambda x: 1.0),
                    left_limit=lo, right_limit=hi, adaptive_sampling=True)
    lnL = lambda x: -0.5 * ((np.asarray(x) - 0.2) / 0.4) ** 2
    np.random.seed(11)
    res = s.integrate(lnL, "xx", n=1000, nmax=20000, neff=4000, use_lnL=True,
                      return_lnI=True, save_intg=True, no_protect_names=True, verbose=False)
    from scipy.special import erf
    exact = np.log(0.4 * np.sqrt(2 * np.pi) * 0.5 *
                   (erf((hi - 0.2) / 0.4 / np.sqrt(2)) - erf((lo - 0.2) / 0.4 / np.sqrt(2))))
    # TOLERANCE: this run is a real MC estimate, not a constant integrand, so it carries
    # sampling scatter.  Measured over seeds 0..9 on ldas-grid (numpy backend): std 0.0095 nats,
    # max |error| 0.0206.  0.10 sits ~5x above the worst of those and ~14x below the defect it
    # guards, log(4) = 1.386, so the two cannot be confused.
    assert abs(res[0] - exact) < 0.10, \
        "adaptive mcsamplerGPU ln Z = %r, exact %r (defect size log(_pdf_norm) = %r)" % (
            res[0], exact, np.log(4.0))
    for p in s.params_ordered:
        assert np.isclose(float(s._pdf_norm[p]), 1.0), \
            "_pdf_norm[%s] = %r after the histogram swap; pdf_from_hist is already a density" % (
                p, float(s._pdf_norm[p]))


def test_reset_sampling_restores_the_analytic_normalization():
    """_pdf_norm travels with the pdf.  reset_sampling puts the unnormalized analytic pdf back,
    so it must put its mass back too -- otherwise the density would be reported 4x too large
    here, in the other direction from the defect above."""
    lo, hi = -1.0, 3.0
    s = mcsamplerGPU.MCSampler()
    s.add_parameter("xx", pdf=np.vectorize(lambda x: 1.0), prior_pdf=np.vectorize(lambda x: 1.0),
                    left_limit=lo, right_limit=hi, adaptive_sampling=True)
    norm_initial = float(s._pdf_norm["xx"])
    assert np.isclose(norm_initial, hi - lo, rtol=1e-6)
    s._pdf_norm["xx"] = 1.0          # as the histogram swap leaves it
    s.reset_sampling("xx")
    assert np.isclose(float(s._pdf_norm["xx"]), norm_initial, rtol=1e-12), \
        "reset_sampling restored the analytic pdf but left _pdf_norm at the histogram's 1.0"
    p_s, p_prior, _ = s.draw_simplified(20000, "xx")
    ratio = np.asarray(p_prior, dtype=float) / np.asarray(p_s, dtype=float)
    assert np.allclose(ratio, hi - lo, rtol=1e-6), \
        "after reset_sampling, E[prior/p_s] = %r, expected the prior integral %r" % (
            float(np.mean(ratio)), hi - lo)


# ---------------------------------------------------------------------------------------
# the refusal

def test_portfolio_refuses_a_member_with_no_sampling_density():
    """The contract has to be enforceable, or the next member without a density silently
    reintroduces exactly this bias.  The stratified fallback is now a refusal by default."""
    s = _portfolio([mcsamplerAV, mcsamplerGPU])
    # take the method away from one member, as a member that never had it would look
    for m in s.portfolio_realizations:
        if type(m).__module__.endswith("mcsamplerGPU"):
            m.sampling_density = lambda X: None
    with pytest.raises(Exception, match="sampling_density"):
        np.random.seed(4321)
        s.integrate(CONSTANT_LNL, "xx", "yy", n=2000, nmax=4000, neff=30,
                    use_lnL=True, return_lnI=True, save_intg=True,
                    no_protect_names=True, verbose=False)


def test_the_refusal_can_be_opted_out_of():
    """...and the opt-out still gives the OLD, biased answer, which is what makes it an opt-out
    rather than a second code path nobody checked."""
    s = _portfolio([mcsamplerAV, mcsamplerGPU])
    for m in s.portfolio_realizations:
        if type(m).__module__.endswith("mcsamplerGPU"):
            m.sampling_density = lambda X: None
    np.random.seed(4321)
    res = s.integrate(CONSTANT_LNL, "xx", "yy", n=2000, nmax=20000, neff=30,
                      use_lnL=True, return_lnI=True, save_intg=True,
                      no_protect_names=True, verbose=False,
                      portfolio_allow_stratified_density=True)
    # the stratified pool: half the draws at prior/p_s = V/V_s = 1/V, half at V
    expected_bad = np.log(0.5 * (1.0 / V_BOX) + 0.5 * V_BOX)
    assert np.isclose(float(res[0]), expected_bad, atol=1e-6), \
        "the opt-out path returned %r, not the legacy stratified value %r" % (
            float(res[0]), expected_bad)
