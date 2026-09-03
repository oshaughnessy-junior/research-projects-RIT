#!/usr/bin/env python
"""Issue #227: a Gaussian importance proposal must be SCORED under the matrix it
was DRAWN from.

WHAT WENT WRONG.  Seven sites drew ``theta ~ N(mu, cov + 1e-12 I)`` by Cholesky
and then evaluated ``logq`` under bare ``cov``.  The regularizer is ABSOLUTE, so
it is negligible only while ``cov`` is O(1).  RIFT extrinsic posteriors at
rho ~ 50-80 are ~1e-5 rad wide (variances ~1e-10), and ``run_laplace_is``'s
adaptation contracts further: on real S250114ax H1/L1 data ``cov`` reached
3e-21, the Mahalanobis term became ``(1e-6/sqrt(3e-21))**2 ~ 3e8`` per
dimension, and ``--mode laplace-is`` -- the driver's DEFAULT -- returned
``lnZ = 5.85e9`` with ``neff = 1.0`` and exited 0.

WHY THESE TESTS LOOK LIKE THIS.  A unit test of the jitter helper cannot see the
defect: the defect is not in either matrix, it is in the two of them being
different at the call site.  So the behavioural tests below drive the REAL
``run_laplace_is`` on a synthetic likelihood (pure numpy -- no frames, no PSDs,
no jax evaluation), and a separate AST test gates the CLASS across both files,
because a fix at one of seven sites is not a fix.

FLOATING POINT.  Nothing in this file is precision-sensitive: the synthetic
likelihood, the proposal algebra and the reference integral are all numpy
float64 regardless of jax's x64 flag, and the tolerances (0.05 nats, 0.1 rad)
are far above float32 resolution.  x64 is still requested below so that running
this file FIRST in a session cannot change what any later file sees.
"""

import ast
import importlib.machinery
import importlib.util
import io
import contextlib
import os

import numpy as np
import pytest

jax = pytest.importorskip("jax")
jax.config.update("jax_enable_x64", True)

_CODE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
_DRIVER = os.path.join(_CODE, "bin", "integrate_likelihood_extrinsic_jax")
_SAMPLERS = os.path.join(_CODE, "RIFT", "likelihood", "jax_ile", "samplers.py")


def _driver():
    loader = importlib.machinery.SourceFileLoader("_isj_drv", _DRIVER)
    spec = importlib.util.spec_from_loader("_isj_drv", loader)
    mod = importlib.util.module_from_spec(spec)
    mod.__name__ = "_isj_drv"          # keep the __main__ guard from firing
    loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------
# Synthetic likelihood: an isotropic Gaussian in the five angles.
#
# ``sig`` is the whole experiment.  sig ~ 1e-5 is the production regime this
# issue was found in (rho ~ 49) and the regime no prior pilot can resolve;
# sig ~ 0.15 is a posterior a prior pilot CAN find, and is where the estimator
# is supposed to work and must keep working.
# --------------------------------------------------------------------------
_MU = np.array([1.3, 0.2, 1.0, 1.1, 3.0])


class _GaussianAngles(object):
    def __init__(self, sig, peak):
        self.sig = float(sig)
        self.peak = float(peak)

    def log_likelihood(self, *cols):
        th = np.stack([np.asarray(c) for c in cols], axis=-1)
        d = (th[..., :5] - _MU[None, :]) / self.sig
        return self.peak - 0.5 * np.sum(d * d, axis=-1)


def _run(sig, peak, n_max=120000, seed=3):
    """Drive the real ``run_laplace_is``; return its outputs and its log."""
    mod = _driver()
    optp = mod.build_parser()
    opts, _ = optp.parse_args(["--inj-mode", "--n-max", str(n_max),
                               "--seed", str(seed)])
    rng = np.random.default_rng(seed)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        out = mod.run_laplace_is(_GaussianAngles(sig, peak), opts, rng, 5, False)
    return mod, opts, out, buf.getvalue()


def _reference_logZ(mod, opts, sig, peak, n=2000000, seed=99):
    """Independent estimate of ln Z = ln int L(theta) p(theta) dtheta.

    Deliberately shares NO machinery with the estimator under test: the
    proposal is written out here, drawn directly, and centred on the known peak
    at twice its width, which makes this a near-perfect importance proposal
    (ESS ~ 0.25 n).  It uses the driver's ``log_prior`` only because both
    estimators must integrate against the SAME prior to be comparable.
    """
    rng = np.random.default_rng(seed)
    s = 2.0 * sig
    th = _MU[None, :] + s * rng.standard_normal((n, 5))
    lnL = _GaussianAngles(sig, peak).log_likelihood(*[th[:, i] for i in range(5)])
    logp = mod.log_prior(th, opts, False)
    logq = (-0.5 * np.sum(((th - _MU[None, :]) / s) ** 2, axis=1)
            - 0.5 * 5 * np.log(2 * np.pi * s * s))
    lw = lnL + logp - logq
    lw = lw[np.isfinite(lw)]
    m = lw.max()
    w = np.exp(lw - m)
    return float(m + np.log(w.mean()))


###
### 1. The helper's contract
###

def test_regularize_cov_regularizer_is_relative_to_the_covariance():
    """The whole defect is an absolute epsilon meeting a 1e-21 covariance."""
    from RIFT.likelihood.jax_ile.samplers import regularize_cov
    base = np.diag([1.0, 2.0, 3.0, 4.0, 5.0])
    for scale in (1.0, 1e-10, 1e-20, 1e-30):
        cov = scale * base
        out = regularize_cov(cov)
        added = np.diag(out - cov)
        assert np.allclose(added, added[0])                    # isotropic
        # the nudge is a fixed FRACTION of the covariance scale, never a floor
        assert added[0] == pytest.approx(1e-12 * np.trace(cov) / 5, rel=1e-12)
        assert 0 < added[0] < 1e-11 * float(np.max(np.diag(cov)))


def test_regularize_cov_is_scale_equivariant():
    """f(a C) == a f(C): the property an absolute jitter does not have, and the
    reason the proposal can no longer be swamped by its own regularizer."""
    from RIFT.likelihood.jax_ile.samplers import regularize_cov
    rng = np.random.default_rng(0)
    A = rng.standard_normal((5, 5))
    C = A @ A.T
    for a in (1e-8, 1.0, 1e8):
        assert np.allclose(regularize_cov(a * C), a * regularize_cov(C),
                           rtol=1e-12, atol=0.0)


def test_regularize_cov_still_conditions_a_degenerate_covariance():
    """trace == 0 has no scale to be relative to; it must still come back
    Cholesky-able rather than raising inside a sampler."""
    from RIFT.likelihood.jax_ile.samplers import regularize_cov
    out = regularize_cov(np.zeros((4, 4)))
    np.linalg.cholesky(out)                                    # must not raise
    assert np.all(np.diag(out) > 0)


def test_weight_ess_matches_a_hand_computation():
    """The precondition the guards key on. (sum w)^2 / sum w^2, in log space."""
    mod = _driver()
    assert mod._weight_ess(np.log(np.ones(10))) == pytest.approx(10.0)
    w = np.array([1.0, 1e-300, 1e-300])                        # one live sample
    assert mod._weight_ess(np.log(w)) == pytest.approx(1.0, abs=1e-6)
    assert mod._weight_ess(np.array([-np.inf, -np.inf])) == 0.0


###
### 2. The CLASS gate.  Seven sites shared the pattern; a fix at one is not a fix.
###

def _cholesky_calls_with_an_inline_identity(path):
    """Every ``np.linalg.cholesky(X)`` in ``path`` whose argument builds an
    identity inline -- i.e. regularizes a matrix at the DRAW while some other
    expression is what gets scored.  Returns (lineno, source) pairs."""
    with open(path) as f:
        src = f.read()
    tree = ast.parse(src)
    lines = src.splitlines()
    bad = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if not (isinstance(fn, ast.Attribute) and fn.attr == "cholesky"):
            continue
        arg = node.args[0] if node.args else None
        if arg is None:
            continue
        for sub in ast.walk(arg):
            if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr in ("eye", "identity")):
                bad.append((node.lineno, lines[node.lineno - 1].strip()))
                break
    return bad


@pytest.mark.parametrize("path", [_DRIVER, _SAMPLERS])
def test_no_cholesky_regularizes_a_matrix_inline(path):
    """Structural gate on the #227 pattern.

    HONEST SCOPE: this is a shape check, not a correctness proof -- it cannot
    see a caller that passes two different *named* matrices.  It exists because
    the behavioural test below reaches only ONE of the seven sites (the default
    mode); the other six are inside flowMC / NUTS / SMC paths that need numpyro,
    flowMC and a real likelihood to run.  Reverting ANY of the seven to
    ``cholesky(cov + 1e-12*np.eye(d))`` fails this test.

    The fix is to call ``regularize_cov(cov)`` once and hand the SAME object to
    the Cholesky and to _gaussian_logq/_mixture_logq.
    """
    bad = _cholesky_calls_with_an_inline_identity(path)
    assert not bad, (
        "%s regularizes a covariance inside np.linalg.cholesky(...):\n%s\n"
        "Call regularize_cov(cov) once and score under the SAME matrix (#227)."
        % (os.path.basename(path),
           "\n".join("  line %d: %s" % b for b in bad)))


def test_the_class_gate_can_actually_fail():
    """A structural test that never fails on anything is not coverage.  Prove
    the detector fires on the exact pre-fix source line."""
    import tempfile
    src = ("import numpy as np\n"
           "def f(cov, dim):\n"
           "    Lc = np.linalg.cholesky(cov + 1e-12 * np.eye(dim))\n"
           "    return Lc\n")
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(src)
        tmp = f.name
    try:
        found = _cholesky_calls_with_an_inline_identity(tmp)
        assert len(found) == 1 and found[0][0] == 3
    finally:
        os.unlink(tmp)


def test_regularize_cov_is_the_single_definition_both_files_use():
    """The driver must not grow its own copy: the bug was one rule written
    twice (drawn one way, scored another), and a second helper is the same
    mistake one level up."""
    with open(_DRIVER) as f:
        drv = f.read()
    assert "from RIFT.likelihood.jax_ile.samplers import regularize_cov" in drv
    assert "def regularize_cov" not in drv


###
### 3. Behaviour: the default mode on a posterior no prior pilot can resolve.
###    This is the configuration that returned 5.85e9 on real data.
###

_NARROW = dict(sig=2e-5, peak=1539.0)


def test_laplace_is_never_reports_evidence_above_the_peak_likelihood():
    """The #227 regression assertion, stated as the issue asks for it.

    For a NORMALIZED prior, Z = E_prior[L] <= max L, so ln Z <= max lnL always.
    The shipped code returned ln Z = 5.85e9 against a peak lnL of 2323 on real
    data, and 5.6e9 against a peak of -7.6e7 here.  Reproduces in ~10 s with no
    frames because the defect is in the proposal algebra, not in the physics.
    """
    _mod, _opts, out, _log = _run(n_max=200000, seed=11, **_NARROW)
    logZ, _sig, _neff, _n, _theta, lnL, _logw = out
    max_lnL = float(np.nanmax(lnL[np.isfinite(lnL)]))
    assert np.isnan(logZ) or logZ <= max_lnL + 5.0, (
        "ln Z = %r exceeds max lnL = %r; the weights were computed against a "
        "distribution that was never sampled (#227)" % (logZ, max_lnL))


def test_laplace_is_reports_nan_rather_than_a_number_it_cannot_stand_behind():
    """``neff = 1.0`` means one sample carries the estimate.  The library
    samplers already refuse to report such a value; this driver reported it and
    exited 0.  Deleting the _finalize_evidence call fails here."""
    _mod, _opts, out, _log = _run(n_max=200000, seed=11, **_NARROW)
    logZ, _sig, neff, _n, _theta, _lnL, _logw = out
    assert neff < 1.5
    assert np.isnan(logZ)


def test_pilot_that_cannot_resolve_the_peak_leaves_a_covering_proposal():
    """Guard 1 (the pilot).  A prior pilot whose weights are dominated by one
    draw moment-matches to a POINT MASS; every subsequent round then samples a
    ~1e-10 rad blob.  With the guard, the first round still covers the prior.

    Deleting the pilot ESS guard fails this test."""
    _mod, _opts, out, log = _run(n_max=120000, seed=11, **_NARROW)
    theta = out[4]
    first_round = theta[:len(theta) // 3]
    assert first_round[:, 0].std() > 0.1, (
        "first-round ra spread %.3g rad: the proposal collapsed onto a single "
        "pilot draw" % first_round[:, 0].std())
    assert "pilot ESS" in log            # and it said so, rather than silently


def test_a_collapsed_round_does_not_contract_the_next_one():
    """Guard 2 (adaptation).  ``_moment_match`` on weights with ESS ~ 1 returns
    a point mass, and the contraction compounds round over round.  The LAST
    round is the one that has contracted twice, so it is what this measures.

    Deleting the per-round ESS guard fails this test while leaving guard 1's
    test green -- which is why they are two tests."""
    _mod, _opts, out, log = _run(n_max=120000, seed=11, **_NARROW)
    theta = out[4]
    last_round = theta[2 * len(theta) // 3:]
    assert last_round[:, 0].std() > 0.1, (
        "last-round ra spread %.3g rad: the proposal contracted across "
        "adaptation rounds" % last_round[:, 0].std())
    assert "round 1 ESS" in log


###
### 4. The regime the estimator is supposed to work in must be UNCHANGED.
###

def test_laplace_is_matches_an_independent_reference_where_it_works():
    """A posterior a prior pilot CAN resolve (sig = 0.15 rad, peak lnL = 20).

    This is the "the fix did not break the working case" gate: the covariance
    here is ~2e-2, twelve orders above the old absolute jitter, so old and new
    code differ only in the twelfth significant figure -- and the answer must
    still land on an independently computed ln Z.  If either ESS guard fired
    here it would be firing on a healthy run, so the log is checked too.
    """
    mod, opts, out, log = _run(sig=0.15, peak=20.0, n_max=120000, seed=3)
    logZ, _sig, neff, _n, _theta, _lnL, _logw = out
    ref = _reference_logZ(mod, opts, sig=0.15, peak=20.0)
    assert neff > 1000.0
    assert abs(logZ - ref) < 0.05, "ln Z = %.5f vs reference %.5f" % (logZ, ref)
    assert "laplace-is]" not in log, "an ESS guard fired on a healthy run: %s" % log


###
### 5. A non-finite evidence must FAIL the event, not be published as one.
###

def test_require_finite_evidence_passes_a_number_and_refuses_a_nan():
    mod = _driver()
    mod.require_finite_evidence(1462.36, 4.5, "laplace-is")      # must not raise
    for bad in (np.nan, np.inf, -np.inf):
        with pytest.raises(RuntimeError) as ei:
            mod.require_finite_evidence(bad, 1.0, "laplace-is")
        assert "laplace-is" in str(ei.value)


def test_analyze_one_refuses_before_it_writes_either_artifact():
    """WIRING, not presence.  On real S250114ax data the pre-fix driver wrote
    ``lnL = 5.848741051595e+09`` into ``out_0_.dat`` and exited 0; the estimator
    now returns nan there, and a nan row published to a CIP fit is no better.
    So the refusal has to come BEFORE write_samples/write_dat -- checked by
    statement order inside ``analyze_one``, because a call that runs after the
    files exist is the same defect with a tidier log.
    """
    with open(_DRIVER) as f:
        tree = ast.parse(f.read())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "analyze_one")
    seen = {}
    for node in ast.walk(fn):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            seen.setdefault(node.func.id, node.lineno)
    for name in ("require_finite_evidence", "write_samples", "write_dat"):
        assert name in seen, "analyze_one never calls %s" % name
    assert seen["require_finite_evidence"] < seen["write_samples"]
    assert seen["require_finite_evidence"] < seen["write_dat"]
