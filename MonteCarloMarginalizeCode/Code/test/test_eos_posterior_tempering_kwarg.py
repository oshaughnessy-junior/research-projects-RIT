"""The EOS driver must hand the sampler ``tempering_exp``, not ``adapt_weight_exponent``.

``adapt_weight_exponent`` is an argparse *dest* in the ILE drivers, which translate it
to ``tempering_exp`` before calling a sampler.  No integrator reads the argparse spelling,
so passing it to ``integrate()`` is silently swallowed by ``**kwargs`` and the sampler
falls back to its module default.  util_ConstructIntrinsicPosterior_GenericCoordinates.py
had this bug and it was fixed in f9f4456c0 ("cip: bug with arg name passing"); the EOS
driver, forked from the same code, kept it.

The first test is a call-site spy: it executes the driver's real ``sampler.integrate(...)``
expression against a recording stand-in and asserts on the keyword dict the sampler
receives, so it fails whether the kwarg is renamed back, dropped, or shadowed by a
colliding entry in ``extra_args``.
"""
import ast
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE_ROOT = os.path.dirname(HERE)                      # .../Code
BIN_DIR = os.path.join(CODE_ROOT, "bin")
EOS_DRIVER = os.path.join(BIN_DIR, "util_ConstructEOSPosterior.py")

BAD_KWARG = "adapt_weight_exponent"
GOOD_KWARG = "tempering_exp"
INTEGRATE_METHODS = ("integrate", "integrate_log")


def _integrate_calls(tree):
    """Every ``<something>.integrate(...)`` / ``.integrate_log(...)`` Call in a module."""
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr in INTEGRATE_METHODS]


class _SamplerSpy(object):
    """Stands in for the sampler object at the driver's call site."""

    def __init__(self):
        self.args = None
        self.kwargs = None

    def integrate(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs
        # The driver unpacks four values from the return.
        return 1.0, 0.0, 1.0, {}

    integrate_log = integrate


class _Opts(object):
    n_max = 1000
    n_eff = 100
    force_no_adapt = False
    tripwire_fraction = 2


def test_eos_driver_hands_the_sampler_tempering_exp():
    """Execute the driver's own integrate() expression and inspect the kwargs it sends."""
    with open(EOS_DRIVER) as f:
        source = f.read()
    tree = ast.parse(source, filename=EOS_DRIVER)
    calls = [c for c in _integrate_calls(tree)
             if isinstance(c.func.value, ast.Name) and c.func.value.id == "sampler"]
    assert len(calls) == 1, (
        "expected exactly one sampler.integrate* call site in %s, found %d; "
        "this test pins the call site and must be updated if the driver grows another"
        % (EOS_DRIVER, len(calls)))

    my_exp_sentinel = object()   # identity, so we also prove the VALUE is my_exp
    spy = _SamplerSpy()
    namespace = {
        "sampler": spy,
        "fn_passed": lambda *a: 1.0,
        "low_level_coord_names": ["x0", "x1"],
        "opts": _Opts(),
        "n_step": 10,
        "test_converged": None,
        "my_exp": my_exp_sentinel,
        "extra_args": {"n_adapt": 100, "history_mult": 10,
                       "force_no_adapt": False, "tripwire_fraction": 2},
    }
    expr = ast.Expression(body=calls[0])
    ast.fix_missing_locations(expr)
    try:
        eval(compile(expr, EOS_DRIVER, "eval"), namespace)
    except NameError as e:
        raise AssertionError(
            "the driver's integrate() call uses a name this test does not bind (%s); "
            "add it to `namespace` above so the call site stays under test" % e)

    assert spy.kwargs is not None, "the spy was never called"
    assert BAD_KWARG not in spy.kwargs, (
        "%s passes %s=... to the sampler.  No integrator reads that name -- it is the "
        "argparse dest used by the ILE drivers, which rename it to %s before calling a "
        "sampler.  Passing it here is swallowed by **kwargs and the sampler silently "
        "uses its module default instead of my_exp."
        % (os.path.basename(EOS_DRIVER), BAD_KWARG, GOOD_KWARG))
    assert GOOD_KWARG in spy.kwargs, (
        "%s no longer passes %s to the sampler, so the weight exponent it computes "
        "(my_exp) never reaches the integrator."
        % (os.path.basename(EOS_DRIVER), GOOD_KWARG))
    assert spy.kwargs[GOOD_KWARG] is my_exp_sentinel, (
        "%s is set from something other than my_exp (got %r); the driver computes and "
        "prints my_exp as the weight exponent, so that is the value that must be sent."
        % (GOOD_KWARG, spy.kwargs[GOOD_KWARG]))


def _driver_sources():
    for name in sorted(os.listdir(BIN_DIR)):
        path = os.path.join(BIN_DIR, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path) as f:
                head = f.read(200)
                f.seek(0)
                source = f.read()
        except (UnicodeDecodeError, OSError):
            continue
        if not (name.endswith(".py") or (head.startswith("#!") and "python" in head)):
            continue
        yield path, source


def test_no_driver_passes_the_argparse_spelling_to_a_sampler():
    """Fleet-wide guard: the same rename bug in any other bin/ driver."""
    offenders = []
    for path, source in _driver_sources():
        try:
            tree = ast.parse(source, filename=path)
        except SyntaxError:
            continue                      # non-python executable in bin/
        for call in _integrate_calls(tree):
            for kw in call.keywords:
                if kw.arg == BAD_KWARG:
                    offenders.append("%s:%d" % (os.path.relpath(path, CODE_ROOT),
                                                call.lineno))
    assert not offenders, (
        "these integrate() call sites pass %s, which no integrator reads; the samplers "
        "take %s: %s" % (BAD_KWARG, GOOD_KWARG, ", ".join(offenders)))


@pytest.mark.parametrize("module_name", [
    "RIFT.integrators.mcsampler",
    "RIFT.integrators.mcsamplerGPU",
    "RIFT.integrators.mcsamplerAdaptiveVolume",
    "RIFT.integrators.mcsamplerEnsemble",
    "RIFT.integrators.mcsamplerPortfolio",
])
def test_integrators_read_tempering_exp_from_kwargs(module_name):
    """The premise of the rename: every sampler the EOS driver can build reads this name."""
    mod = pytest.importorskip(module_name)
    with open(mod.__file__) as f:
        source = f.read()
    assert '"%s"' % GOOD_KWARG in source or "'%s'" % GOOD_KWARG in source, (
        "%s no longer reads %s from kwargs; the drivers' rename target has moved"
        % (module_name, GOOD_KWARG))


def test_the_argparse_spelling_is_inert_at_the_sampler():
    """Behavioural proof that the misspelling does nothing: it must not change a result.

    Forces the numpy backend, so it is skipped where cupy is importable (cupy's RNG is
    not seeded by numpy.random.seed, and the run would not be bit-reproducible).
    """
    try:
        import cupy                                    # noqa: F401
        pytest.skip("cupy importable: run is not bit-reproducible under numpy seeding")
    except ImportError:
        pass
    import numpy as np
    mcsamplerGPU = pytest.importorskip("RIFT.integrators.mcsamplerGPU")

    def build():
        s = mcsamplerGPU.MCSampler()
        s.xpy = np
        s.identity_convert = lambda x: x
        for p in ("x0", "x1"):
            s.add_parameter(p, pdf=np.vectorize(lambda x: 1),
                            prior_pdf=np.vectorize(lambda x: 0.1),
                            left_limit=-5.0, right_limit=5.0, adaptive_sampling=True)
        return s

    def integrand(*args):
        x = np.atleast_2d(np.array([*args], dtype=np.float64).T)
        return np.exp(30.0 - 0.5 * ((x - 1.0) ** 2).sum(axis=1) / 0.35 ** 2)

    def go(**extra):
        np.random.seed(11)
        s = build()
        res, _, neff, _ = s.integrate(
            integrand, "x0", "x1", verbose=False, nmax=3000, n=500, neff=1e9,
            save_intg=True, tempering_adapt=True, floor_level=1e-3,
            igrand_threshold_p=1e-3, convergence_tests=None, no_protect_names=True,
            n_adapt=100, history_mult=10, **extra)
        return float(np.log(res)), float(neff)

    default = go()
    misspelled = go(**{BAD_KWARG: 1.0})
    correct = go(**{GOOD_KWARG: 1.0})

    assert misspelled == default, (
        "%s=1.0 changed the result, so some integrator now consumes that name; the "
        "driver-side rename in this PR needs rechecking" % BAD_KWARG)
    assert correct != default, (
        "%s=1.0 did not change the result, so this toy no longer exercises the "
        "adaptation weights and the inertness check above proves nothing" % GOOD_KWARG)
