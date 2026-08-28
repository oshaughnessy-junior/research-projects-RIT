"""The time-marginalization quadrature: pin the CPU/GPU divergence and the selector.

RIFT's CPU and GPU likelihood paths integrate exp(lnL(t)) with two DIFFERENT
Simpson conventions -- scipy's (Cartwright, >=1.11) and a vendored copy of the
pre-1.11 ``even='avg'`` rule in ``optimized_gpu_tools``.  They are bit-identical
for odd ``npts`` and different rules for even ``npts``, which is what
``marginalization_time_grid`` returns at srate 4096 and 8192.

These tests are characterisation, not aspiration: they pin the divergence that
production HAS, so that it cannot change size silently and so that a fix has a
gate to move.  Rationale and the accuracy measurements:
``RIFT/likelihood/DESIGN_time_quadrature.md``.

Pure numpy/scipy; no GPU needed.  The one test that needs cupy skips without it.
"""
import numpy as np
import pytest

from RIFT.likelihood import factored_likelihood as fl

NPTS_EVEN, NPTS_ODD = 614, 613
DX = 1.0/4096


def _tupleset(t, i, v):
    l = list(t); l[i] = v; return tuple(l)


def _basic_simps(y, start, stop, dx, axis=-1):
    nd = y.ndim
    sa = (slice(None),)*nd
    s0 = _tupleset(sa, axis, slice(start, stop, 2))
    s1 = _tupleset(sa, axis, slice(start+1, stop+1, 2))
    s2 = _tupleset(sa, axis, slice(start+2, stop+2, 2))
    return np.sum(dx/3.0*(y[s0] + 4*y[s1] + y[s2]), axis=axis)


def vendored_simps_avg(y, dx=1.0, axis=-1):
    """numpy transliteration of ``optimized_gpu_tools.simps`` (pre-1.11 scipy, even='avg').

    Verified element-for-element against the shipped cupy routine -- that check
    is ``test_transliteration_matches_shipped_cupy_rule`` below, which runs
    wherever cupy is available.
    """
    y = np.asarray(y)
    nd, N = y.ndim, y.shape[axis]
    sa = (slice(None),)*nd
    if N % 2:
        return _basic_simps(y, 0, N-2, dx, axis)
    val = 0.5*dx*(y[_tupleset(sa, axis, -1)] + y[_tupleset(sa, axis, -2)])
    result = _basic_simps(y, 0, N-3, dx, axis)
    val = val + 0.5*dx*(y[_tupleset(sa, axis, 1)] + y[_tupleset(sa, axis, 0)])
    result = result + _basic_simps(y, 1, N-2, dx, axis)
    return result/2.0 + val/2.0


def weights(rule, npts, dx=1.0):
    """Both rules are linear, so applying them to the identity IS their weight vector."""
    return np.asarray(rule(np.eye(npts), dx=dx, axis=-1))


# ---------------------------------------------------------------- the divergence

def test_odd_npts_the_two_rules_are_bit_identical():
    ws = weights(fl.my_simps, NPTS_ODD)
    wv = weights(vendored_simps_avg, NPTS_ODD)
    assert np.array_equal(ws, wv)


def test_even_npts_the_gpu_rule_is_the_trapezoid_rule_in_the_interior():
    """even='avg' averages Simpson's two panel alignments -> interior weight 1."""
    wv = weights(vendored_simps_avg, NPTS_EVEN)
    assert np.allclose(wv[2:-2], 1.0, rtol=0, atol=1e-14)
    assert np.allclose(wv[:2], [5.0/12, 13.0/12], rtol=0, atol=1e-14)
    assert np.allclose(wv[-2:], [13.0/12, 5.0/12], rtol=0, atol=1e-14)

    ws = weights(fl.my_simps, NPTS_EVEN)
    assert np.allclose(ws[:3], [1.0/3, 4.0/3, 2.0/3], rtol=0, atol=1e-14)
    assert np.max(np.abs(ws - wv)) == pytest.approx(1.0/3, rel=1e-12)


def test_both_rules_integrate_a_constant_exactly():
    """Why this never tripped a normalization check."""
    for npts in (NPTS_ODD, NPTS_EVEN):
        for rule in (fl.my_simps, vendored_simps_avg):
            assert weights(rule, npts, DX).sum() == pytest.approx((npts-1)*DX, rel=1e-13)


def test_divergence_bound_for_a_nonnegative_integrand():
    """ln(I_scipy/I_avg) is in [ln(2/3), ln(4/3)]; the extremes are single-sample peaks.

    The upper end, 0.2877 nats, and the lower, -0.4055, are what a shipped-path
    numpy-vs-cupy comparison measured (max 0.3947 nats): the bound, saturated.
    """
    ws = weights(fl.my_simps, NPTS_EVEN)
    wv = weights(vendored_simps_avg, NPTS_EVEN)
    r = np.log(ws/wv)
    assert r.min() == pytest.approx(np.log(2.0/3), rel=1e-12)
    assert r.max() == pytest.approx(np.log(4.0/3), rel=1e-12)


def test_which_production_sample_rates_are_affected():
    """npts = int(2*iwh/deltaT); only EVEN npts diverges.  4096 and 8192 are even."""
    got = {s: len(fl.marginalization_time_grid(0.075, 1.0/s))
           for s in (1024, 2048, 4096, 8192, 16384)}
    assert got == {1024: 153, 2048: 307, 4096: 614, 8192: 1228, 16384: 2457}
    assert [s for s, n in got.items() if n % 2 == 0] == [4096, 8192]


# ------------------------------------------------------------------- the selector

def test_default_is_auto_and_reproduces_the_historical_choice():
    """The default must not move any shipped number: 'auto' returns the old callables."""
    assert fl.TIME_QUADRATURE == 'auto'
    assert fl.time_quadrature_rule(np) is fl.my_simps
    if fl.optimized_gpu_tools is not None:
        class _NotNumpy(object):
            pass
        assert fl.time_quadrature_rule(_NotNumpy()) is fl.optimized_gpu_tools.simps


def test_default_numpy_weights_are_scipys():
    """Numeric, not just identity: the default CPU rule IS scipy's, weight for weight."""
    w = weights(fl.time_quadrature_rule(np), NPTS_EVEN, DX)
    assert np.array_equal(w, weights(fl.my_simps, NPTS_EVEN, DX))
    # and it is NOT the GPU rule -- the divergence this file exists to pin
    assert not np.allclose(w, weights(vendored_simps_avg, NPTS_EVEN, DX), rtol=0, atol=1e-14)


@pytest.mark.parametrize("npts", (NPTS_ODD, NPTS_EVEN))
def test_named_rules_are_what_they_claim(npts):
    y = np.random.default_rng(0).random((5, npts))
    trap = fl.time_quadrature_rule(np, 'trapezoid')(y, dx=DX, axis=-1)
    assert np.allclose(trap, np.trapz(y, dx=DX, axis=-1), rtol=0, atol=1e-13)
    simp = fl.time_quadrature_rule(np, 'simpson')(y, dx=DX, axis=-1)
    assert np.allclose(simp, fl.my_simps(y, dx=DX, axis=-1), rtol=1e-11, atol=0)


def test_named_rules_are_backend_independent_by_construction():
    """The whole point: the weights depend on (npts, dx, kind), never on the backend."""
    for kind in ('simpson', 'trapezoid'):
        w = fl.time_quadrature_weights(NPTS_EVEN, DX, kind, xpy=np)
        assert w.shape == (NPTS_EVEN,)
        y = np.eye(NPTS_EVEN)
        assert np.allclose(fl.time_quadrature_rule(np, kind)(y, dx=DX, axis=-1), w,
                           rtol=0, atol=1e-15)


def test_unknown_rule_is_rejected():
    with pytest.raises(ValueError):
        fl.time_quadrature_rule(np, 'romberg')
    with pytest.raises(ValueError):
        fl.time_quadrature_weights(NPTS_EVEN, DX, 'romberg')


# --------------------------------------------------------------------- accuracy

def test_trapezoid_beats_simpson_on_a_peak_inside_the_window():
    """Euler-Maclaurin, on the integrand the time marginalization actually has.

    exp(lnL(t)) is a peak that decays to nothing well inside the window, so every
    boundary correction term of the trapezoidal rule vanishes and it converges
    exponentially in sigma/deltaT; Simpson mixes in the 2h trapezoid, whose
    aliasing error is exponentially larger.  Do NOT "fix" this by assuming the
    higher formal order wins -- on this integrand it loses, at BOTH parities.
    """
    from scipy.special import erf
    for npts in (NPTS_ODD, NPTS_EVEN):
        j = np.arange(npts)
        sig = 1.5
        worst = {'simpson': 0.0, 'trapezoid': 0.0}
        for k in range(8):
            t0 = (npts-1)/2.0 + k/8.0 - 0.5
            y = np.exp(-0.5*((j-t0)/sig)**2)
            exact = sig*np.sqrt(np.pi/2)*(erf((npts-1-t0)/(sig*np.sqrt(2)))
                                          + erf(t0/(sig*np.sqrt(2))))
            for kind in worst:
                v = float(fl.time_quadrature_rule(np, kind)(y, dx=1.0))
                worst[kind] = max(worst[kind], abs(np.log(v/exact)))
        assert worst['trapezoid'] < 1e-13
        assert worst['simpson'] > 1e-6
        assert worst['simpson'] > 1e6*max(worst['trapezoid'], 1e-16)


# ------------------------------------------------------------- GPU-only cross-check

def test_transliteration_matches_shipped_cupy_rule():
    try:
        import cupy
    except Exception as exc:              # no cupy, or no libcuda on this host
        pytest.skip("cupy unavailable: {}".format(type(exc).__name__))
    if fl.optimized_gpu_tools is None:
        pytest.skip("optimized_gpu_tools unavailable")
    rng = np.random.default_rng(0)
    for npts in (NPTS_ODD, NPTS_EVEN, 8, 9):
        y = rng.random((3, npts))
        got = cupy.asnumpy(fl.optimized_gpu_tools.simps(cupy.asarray(y), dx=DX, axis=-1))
        assert np.allclose(got, vendored_simps_avg(y, dx=DX, axis=-1), rtol=0, atol=1e-15)


def test_default_cupy_weights_are_the_vendored_rules():
    """The GPU half of the no-op claim, numerically: 'auto' on cupy is still even='avg'.

    This is the assertion a "let's just unify the backends" edit has to break, and
    it is checked in the units that matter (weights), not by object identity.
    """
    try:
        import cupy
    except Exception as exc:
        pytest.skip("cupy unavailable: {}".format(type(exc).__name__))
    if fl.optimized_gpu_tools is None:
        pytest.skip("optimized_gpu_tools unavailable")
    rule = fl.time_quadrature_rule(cupy)
    w = cupy.asnumpy(rule(cupy.eye(NPTS_EVEN), dx=DX, axis=-1))
    assert np.allclose(w, weights(vendored_simps_avg, NPTS_EVEN, DX), rtol=0, atol=1e-15)
    assert np.allclose(w[2:-2], DX, rtol=0, atol=1e-16)          # trapezoid, not Simpson
    assert not np.allclose(w, weights(fl.my_simps, NPTS_EVEN, DX), rtol=0, atol=1e-14)


# ============================================================================
# Wide-radius routing: every time-quadrature site goes through the ONE selector
# ============================================================================
#
# Three instruments, because no single one is sufficient:
#
#   (1) VALUE IDENTITY of each substitution -- for every routed site, the new
#       expression and the ORIGINAL expression (written out literally here) are
#       run on representative arrays, on both backends, and must agree bit for
#       bit.  Mutating the selector breaks all of these.
#   (2) A SOURCE GUARD -- no unrouted quadrature expression survives in the
#       routed modules.  (1) cannot catch a site that was never routed, because
#       the two expressions agree whether or not the site calls either of them;
#       this is what catches partial routing and re-introduction.
#   (3) DIRECT DRIVES where the enclosing site is cheaply callable
#       (jax_ile._simpson_weights, study_stencil.time_marginalize).  The three
#       array-vector likelihood functions need a full PSD/waveform precompute,
#       so they are covered by (1)+(2) rather than driven here.

ROUTED_MODULES = (
    "factored_likelihood.py",
    "factored_likelihood_freqresponse.py",
    "factored_likelihood_with_rotation.py",
    "factored_likelihood_LISA.py",
    "study_stencil_lnL_sensitivity.py",
    "jax_ile/core.py",
)


def _cupy_or_skip():
    try:
        import cupy
    except Exception as exc:
        pytest.skip("cupy unavailable: {}".format(type(exc).__name__))
    if fl.optimized_gpu_tools is None:
        pytest.skip("optimized_gpu_tools unavailable")
    return cupy


@pytest.mark.parametrize("npts", (NPTS_ODD, NPTS_EVEN))
def test_routing_noop_numpy_sites(npts):
    """Sites whose original expression resolved to scipy on numpy.

    Covers factored_likelihood_{freqresponse,with_rotation} at xpy=np,
    factored_likelihood_LISA, and study_stencil_lnL_sensitivity.
    """
    rng = np.random.default_rng(1)
    y = rng.random((4, npts))
    on_gpu = False
    # ORIGINAL expressions, transcribed from the pre-routing source:
    orig_flfr = fl.optimized_gpu_tools.simps if on_gpu else fl.my_simps
    orig_lisa = __import__("scipy.integrate", fromlist=["integrate"]).simpson
    orig_stencil = fl.my_simps
    routed = fl.time_quadrature_rule(np)
    assert np.array_equal(routed(y, dx=DX, axis=-1), orig_flfr(y, dx=DX, axis=-1))
    assert np.array_equal(routed(y, dx=DX, axis=-1), orig_stencil(y, dx=DX, axis=-1))
    # LISA integrates axis=0 -- the axis the routed rule previously could not do
    yT = np.ascontiguousarray(y.T)
    assert np.array_equal(routed(yT, dx=DX, axis=0), orig_lisa(yT, dx=DX, axis=0))


@pytest.mark.parametrize("npts", (NPTS_ODD, NPTS_EVEN))
def test_routing_noop_cupy_sites(npts):
    """The GPU half: on cupy the original expression was the VENDORED rule, not scipy.

    This is the assertion that fails if a routing edit quietly standardises the
    freqresponse / with_rotation GPU paths on scipy.
    """
    cupy = _cupy_or_skip()
    rng = np.random.default_rng(2)
    y = rng.random((4, npts))
    yg = cupy.asarray(y)
    on_gpu = True
    orig = fl.optimized_gpu_tools.simps if on_gpu else fl.my_simps   # ORIGINAL expression
    routed = fl.time_quadrature_rule(cupy)
    assert np.array_equal(cupy.asnumpy(routed(yg, dx=DX, axis=-1)),
                          cupy.asnumpy(orig(yg, dx=DX, axis=-1)))


def test_rule_supports_axis_0_as_well_as_the_last_axis():
    """The axis fix.  factored_likelihood_LISA integrates on axis=0; a rule that
    silently worked on the last axis only would make 'consistent everywhere' false."""
    rng = np.random.default_rng(3)
    y = rng.random((NPTS_EVEN, 7))
    for kind in ('simpson', 'trapezoid'):
        rule = fl.time_quadrature_rule(np, kind)
        got = rule(y, dx=DX, axis=0)
        assert got.shape == (7,)
        # Same rule applied to the transpose on the last axis.  NOT bit-identical:
        # summing down axis 0 and along the last axis visit memory in different
        # orders, so this is float reassociation, not a different rule.  (The
        # substitution tests above DO demand bit-identity -- same array, same axis.)
        assert np.allclose(got, rule(np.ascontiguousarray(y.T), dx=DX, axis=-1),
                           rtol=1e-14, atol=0)
    # and 'simpson' on axis=0 reproduces scipy on axis=0
    assert np.allclose(fl.time_quadrature_rule(np, 'simpson')(y, dx=DX, axis=0),
                       fl.my_simps(y, dx=DX, axis=0), rtol=1e-11, atol=0)


def test_no_unrouted_time_quadrature_expression_survives():
    """Source guard: catches a site that was never routed, or a re-introduced one.

    The value-identity tests above cannot see an unrouted site -- the two
    expressions agree whether or not the site calls either.  Only this does.
    """
    import os
    import RIFT.likelihood as _pkg
    root = os.path.dirname(_pkg.__file__)
    # Bare NAMES, not call syntax.  The original defect's form was a callable
    # REFERENCE -- `FL.optimized_gpu_tools.simps if on_gpu else FL.my_simps` --
    # which a "(" -terminated pattern does not match; a mutation that un-routed
    # freqresponse back to exactly that line sailed through the first version of
    # this guard.  Match the name, not the call.
    banned = ("my_simps", "optimized_gpu_tools.simps", "integrate.simpson",
              "integrate.simps")
    offenders = []
    for rel in ROUTED_MODULES:
        path = os.path.join(root, rel)
        # Exemption is STRUCTURAL, not a string allowlist: the bodies of the
        # time_quadrature_* functions are where the rules are DEFINED, so calling
        # my_simps there is the point.  A string allowlist would silently stop
        # matching if those lines were reworded, and the guard would go quiet.
        inside_selector = False
        for i, line in enumerate(open(path), 1):
            stripped = line.lstrip()
            if line and not line[:1].isspace() and stripped.startswith(("def ", "class ")):
                inside_selector = stripped.startswith("def time_quadrature_")
            if inside_selector:
                continue
            code = line.split("#", 1)[0]
            # `my_simps = ...` is the DEFINITION of the scipy handle, not a use.
            if code.lstrip().startswith("my_simps ="):
                continue
            if any(b in code for b in banned):
                offenders.append("%s:%d: %s" % (rel, i, code.strip()))
    assert not offenders, "unrouted time-quadrature call(s):\n  " + "\n  ".join(offenders)


def test_jax_simpson_weights_is_routed_and_a_noop():
    """Direct drive of the jax_ile site."""
    try:
        from RIFT.likelihood.jax_ile import core as jcore
    except Exception as exc:
        pytest.skip("jax_ile unavailable: {}".format(type(exc).__name__))
    for npts in (NPTS_ODD, NPTS_EVEN):
        got = np.asarray(jcore._simpson_weights(npts, DX))
        assert np.array_equal(got, np.asarray(fl.my_simps(np.eye(npts), dx=DX, axis=-1)))


def test_study_stencil_time_marginalize_is_routed_and_a_noop():
    """Direct drive of the study_stencil site."""
    try:
        import RIFT.likelihood.study_stencil_lnL_sensitivity as ss
    except Exception as exc:
        pytest.skip("study_stencil unavailable: {}".format(type(exc).__name__))
    rng = np.random.default_rng(4)
    for npts in (NPTS_ODD, NPTS_EVEN):
        lnL_t = rng.normal(size=(6, npts))
        m = np.max(lnL_t, axis=-1, keepdims=True)
        want = m[:, 0] + np.log(fl.my_simps(np.exp(lnL_t - m), dx=DX, axis=-1))
        assert np.array_equal(ss.time_marginalize(lnL_t, DX), want)
