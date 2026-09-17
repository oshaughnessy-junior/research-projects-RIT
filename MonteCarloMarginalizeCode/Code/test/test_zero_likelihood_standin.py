"""The --zero-likelihood stand-in, exercised directly out of the ILE driver's own source.

Its companion, test_e2e_analytic_pipeline.py, runs ILE to completion and checks ln Z against a
closed form.  That cannot see everything.  An end-to-end marginal CANNOT distinguish a
permutation among right_ascension, phi_orb and psi, because all three are sampled uniformly on
[0, 2pi) and are independent: any factor's marginal is identical under the swap, so handing the
factor kwargs['psi'] where it wanted kwargs['phi_orb'] passes every lane.  That is exactly the
defect class the stand-in can have.

So this file checks the WIRING instead of the answer: the argument order against the driver's
own real call sites, the generated signature against the live likelihood_function signatures,
and that the values handed to the factor are the RAW sampled ones.  It is pure AST + exec, runs
in under a second, and needs no data, no network and no GPU.

It also covers the device question the end-to-end gate is structurally blind to: that gate pins
CUDA_VISIBLE_DEVICES="" and the CI runners have no cupy, so nothing there can catch the
stand-in reaching for numpy on a host where the integrand is on a device.  Here the array module
is INJECTED, and a fake one records every call.
"""
import ast
import os
import re

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.abspath(os.path.join(HERE, "..", "bin"))
_ILE = os.path.join(BIN, "integrate_likelihood_extrinsic_batchmode")

pytestmark = pytest.mark.skipif(not os.path.exists(_ILE),
                                reason="ILE executable not in this tree")

# P attribute -> the sampled parameter it carries, for reading the vectorized call sites.
_P_ATTR = {"phi": "right_ascension", "theta": "declination", "phiref": "phi_orb",
           "incl": "inclination", "psi": "psi", "dist": "distance"}


def _driver_source():
    with open(_ILE) as f:
        return f.read()


def _driver_ns():
    """exec just the stand-in factory and its constant, out of the driver's real source."""
    src = _driver_source()
    tree = ast.parse(src)
    wanted = ("make_zero_likelihood_standin", "_SUPPLEMENT_ARG_ORDER")
    nodes = [n for n in tree.body
             if (isinstance(n, ast.FunctionDef) and n.name in wanted)
             or (isinstance(n, ast.Assign) and any(
                 isinstance(t, ast.Name) and t.id in wanted for t in n.targets))]
    assert len(nodes) == 2, "expected the factory and its constant, found %d" % len(nodes)
    ns = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), _ILE, "exec"), ns)
    return ns


# How many times the driver calls the factor.  Pinned EXACTLY, not as a lower bound: with a
# `>=` the extractor could lose a site, or fail to see one, and still report "no mismatch".
# If this number changes, look at the new site and check its argument order by hand before
# updating it.
_EXPECTED_CALL_SITES = 5


def _textual_call_site_count():
    """Count the factor's call sites WITHOUT the AST, as a cross-check on the walker.

    The failure mode this exists for: a call the AST pass cannot see -- reached through an
    alias, or any shape that is not a Call whose func is the bare Name -- contributes nothing
    to the comparison below, so the argument-order test would pass on a driver it had only
    partly read.  A textual count cannot be fooled the same way."""
    return len(re.findall(r"(?<![\w.])supplemental_ln_likelihood\s*\(", _driver_source()))


def _likelihood_signatures():
    """Every `def likelihood_function(...)` signature in the driver, as tuples of names."""
    out = []
    for n in ast.walk(ast.parse(_driver_source())):
        if isinstance(n, ast.FunctionDef) and n.name == "likelihood_function":
            out.append(tuple(a.arg for a in n.args.args))
    assert out, "no likelihood_function definitions found"
    return out


def _real_supplement_call_orders():
    """The positional argument order of every real supplemental_ln_likelihood(...) call.

    Returns (parameter-name tuple, source line) per site.  A site that passes a literal for
    distance -- the distance-marginalized ones pass 0 -- reports 'distance' for it, which is
    what that slot means."""
    out = []
    for n in ast.walk(ast.parse(_driver_source())):
        if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                and n.func.id == "supplemental_ln_likelihood"):
            continue
        names = []
        for i, a in enumerate(n.args):
            if isinstance(a, ast.Name):
                names.append(a.id)
            elif isinstance(a, ast.Attribute) and isinstance(a.value, ast.Name):
                assert a.attr in _P_ATTR, "unmapped attribute %s.%s at line %d" % (
                    a.value.id, a.attr, n.lineno)
                names.append(_P_ATTR[a.attr])
            elif isinstance(a, ast.Constant):
                # a literal in the distance slot: the distance-marginalized sites pass 0
                names.append("distance")
            else:
                pytest.fail("un-readable argument %d at line %d of the ILE" % (i, n.lineno))
        out.append((tuple(names), n.lineno))
    assert out, "no supplemental_ln_likelihood call sites found in the ILE"
    return out


# ---------------------------------------------------------------------------------------
# 1. the argument order, against the driver's own call sites

def test_the_stand_in_uses_the_same_argument_order_as_every_real_call_site():
    """THE CHECK THE END-TO-END GATE CANNOT MAKE.  phi_orb, psi and right_ascension are iid
    uniform on [0, 2pi), so no marginal can tell a permutation of them apart; only the wiring
    can be read.  Every real site is read, not one of them, so a site that disagrees with the
    others is a failure here rather than a silent inconsistency."""
    order = _driver_ns()["_SUPPLEMENT_ARG_ORDER"]
    sites = _real_supplement_call_orders()
    assert len(sites) == _EXPECTED_CALL_SITES, (
        "the AST pass found %d call sites, expected %d.  If a site was added, read its argument "
        "order and update _EXPECTED_CALL_SITES; if one vanished, the extractor broke."
        % (len(sites), _EXPECTED_CALL_SITES))
    for names, lineno in sites:
        assert names == tuple(order), (
            "the --zero-likelihood stand-in passes the factor %r, but the real call site at "
            "line %d passes %r" % (tuple(order), lineno, names))


def test_the_extractor_reads_every_call_site_the_source_has():
    """The guard on the guard.  A site the AST pass cannot see would make the order comparison
    above pass on a driver it had only partly read, which is the quiet way this whole file stops
    being worth anything."""
    ast_count = len(_real_supplement_call_orders())
    textual = _textual_call_site_count()
    assert ast_count == textual, (
        "the AST pass sees %d call sites but the source text has %d: at least one call is in a "
        "shape the walker does not recognise, and its argument order is NOT being checked."
        % (ast_count, textual))
    assert textual == _EXPECTED_CALL_SITES, (
        "the driver has %d call sites, expected %d; read the new one before updating the number."
        % (textual, _EXPECTED_CALL_SITES))


def test_the_argument_order_is_the_documented_one():
    """A second, independent statement of the same fact, so that changing BOTH the stand-in and
    the call sites together still trips something.  This is the order in --help."""
    assert _driver_ns()["_SUPPLEMENT_ARG_ORDER"] == (
        "right_ascension", "declination", "phi_orb", "inclination", "psi", "distance")


# ---------------------------------------------------------------------------------------
# 2. the generated signature

def test_the_stand_in_reproduces_every_live_likelihood_signature():
    """Consumers decide what to pass by reading func.__code__.co_varnames[:co_argcount] --
    mcsampler does exactly that.  A `def zero_like(*args, **kwargs)` stand-in reports ZERO
    arguments there, which killed --zero-likelihood --sampler-method adaptive_cartesian."""
    make = _driver_ns()["make_zero_likelihood_standin"]
    sigs = _likelihood_signatures()
    assert len(sigs) >= 6, "only %d signatures found; the extractor probably broke" % len(sigs)
    for sig in sigs:
        f = make(sig, True, None, _DEFAULTS, np)
        got = f.__code__.co_varnames[:f.__code__.co_argcount]
        assert got == sig, "stand-in signature %r does not match likelihood_function's %r" % (
            got, sig)


_DEFAULTS = {"right_ascension": 0.1, "declination": 0.2, "phi_orb": 0.3,
             "inclination": 0.4, "psi": 0.5, "distance": 0.0}


def _spy():
    seen = {}

    def supplement(*args):
        seen["args"] = args
        return np.zeros(len(args[0]))
    return supplement, seen


@pytest.mark.parametrize("sig", _likelihood_signatures())
def test_the_factor_gets_the_raw_sampled_value_for_every_argument(sig):
    """Positionally AND by keyword, for every signature the driver can build -- because the
    integrand is called both ways (mcsampler by keyword, AV's selfish update positionally), and
    a hand-written unpack was correct for exactly one of the eight signatures."""
    make = _driver_ns()["make_zero_likelihood_standin"]
    order = _driver_ns()["_SUPPLEMENT_ARG_ORDER"]
    # a distinct, recognisable value per sampled parameter
    vals = {nm: np.arange(4, dtype=float) + 100.0 * (i + 1) for i, nm in enumerate(sig)}
    for how in ("positional", "keyword"):
        supplement, seen = _spy()
        f = make(sig, True, supplement, _DEFAULTS, np)
        if how == "positional":
            f(*[vals[nm] for nm in sig])
        else:
            f(**vals)
        got = seen["args"]
        assert len(got) == len(order)
        for nm, g in zip(order, got):
            want = vals[nm] if nm in sig else _DEFAULTS[nm]
            assert np.all(np.asarray(g) == np.asarray(want)), (
                "%s call, signature %r: the factor's %r argument got %r, expected %r "
                "(raw sampled value, or the default when not sampled)" % (how, sig, nm, g, want))


def test_a_signature_missing_a_factor_argument_with_no_default_is_refused():
    """Silently substituting something would be a wrong answer with no diff line on it."""
    make = _driver_ns()["make_zero_likelihood_standin"]
    supplement, _ = _spy()
    with pytest.raises(ValueError) as exc:
        make(("right_ascension", "declination"), True, supplement,
             {"right_ascension": 0.0}, np)
    assert "no value available" in str(exc.value)


# ---------------------------------------------------------------------------------------
# 3. the convention, and the array module

class _FakeXpy(object):
    """Stands in for cupy: every array it makes is tagged, so a stand-in that reached for
    numpy instead produces an untagged array and is caught.  The CI runners have no cupy and
    the end-to-end gate pins CUDA_VISIBLE_DEVICES="", so this is the only place that can see
    it."""

    def __init__(self):
        self.calls = []

    def _tag(self, a):
        a = np.asarray(a)
        out = a.view(_TaggedArray)
        out.from_fake = True
        return out

    def zeros(self, n):
        self.calls.append(("zeros", n))
        return self._tag(np.zeros(n))

    def ones(self, n):
        self.calls.append(("ones", n))
        return self._tag(np.ones(n))

    def exp(self, x):
        self.calls.append(("exp", None))
        return self._tag(np.exp(np.asarray(x, dtype=float)))


class _TaggedArray(np.ndarray):
    """The tag has to survive arithmetic, or the check below passes on the base array alone and
    says nothing about `base + supp`.  __array_finalize__ is what carries it through a ufunc."""
    from_fake = False

    def __array_finalize__(self, obj):
        if obj is not None:
            self.from_fake = getattr(obj, "from_fake", False)


@pytest.mark.parametrize("return_lnL,expect_maker", [(True, "zeros"), (False, "ones")])
def test_the_base_array_comes_from_the_injected_module_not_numpy(return_lnL, expect_maker):
    make = _driver_ns()["make_zero_likelihood_standin"]
    sig = ("right_ascension", "declination", "phi_orb", "inclination", "psi", "distance")
    xpy = _FakeXpy()
    supplement, _ = _spy()
    f = make(sig, return_lnL, supplement, _DEFAULTS, xpy)
    out = f(**{nm: np.zeros(3) for nm in sig})
    assert [c[0] for c in xpy.calls][0] == expect_maker, \
        "the stand-in built its base with %r, expected %r" % (xpy.calls, expect_maker)
    assert getattr(out, "from_fake", False), \
        "the result did not come from the injected array module: %r" % type(out)


def test_the_two_conventions_give_the_right_arithmetic():
    """return_lnL says whether the INTEGRAND is ln L or L, which is not the same question as
    opts.internal_use_lnL; they disagree for adaptive_cartesian, which is the one lane that
    exercises the linear branch below."""
    make = _driver_ns()["make_zero_likelihood_standin"]
    sig = ("right_ascension", "declination", "phi_orb", "inclination", "psi", "distance")
    supp_vals = np.array([0.5, -1.25, 2.0])

    def supplement(*args):
        return supp_vals

    kw = {nm: np.zeros(3) for nm in sig}
    log_out = make(sig, True, supplement, _DEFAULTS, np)(**kw)
    lin_out = make(sig, False, supplement, _DEFAULTS, np)(**kw)
    assert np.allclose(log_out, supp_vals), "ln-convention: expected 0 + ln f"
    assert np.allclose(lin_out, np.exp(supp_vals)), "linear convention: expected 1 * f"
    # and with no factor at all, the exact zero / one the option promises
    assert np.allclose(make(sig, True, None, _DEFAULTS, np)(**kw), 0.0)
    assert np.allclose(make(sig, False, None, _DEFAULTS, np)(**kw), 1.0)
