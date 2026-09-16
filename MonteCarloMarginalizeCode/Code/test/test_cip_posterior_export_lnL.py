"""The lnL field the CIP posterior export reads, across every sampler convention.

util_ConstructIntrinsicPosterior_GenericCoordinates.py resolves the sampler's lnL field
ONCE, into dat_logL: 'log_integrand' if the integrator ran in log mode, else log of the
linear 'integrand'.  The posterior export then re-read the raw samples["integrand"]
instead, so --sampler-method adaptive_cartesian_gpu --internal-use-lnL died with
KeyError: 'integrand' AFTER a converged integral (n_eff ~3600, *_int.dat written) and
never wrote the posterior samples.  mcsamplerGPU.integrate dispatches to integrate_log and
leaves only 'log_integrand'; AV, NFlow and portfolio additionally alias 'integrand' to it,
which is why one flag's difference separated a working arm from a dead one.

Two things are asserted, because exit 0 alone does not distinguish a correct fix from a
wrong one:

* every sampler arm RUNS to a written samples file (the crash), and
* the exported lnL is lnL -- bracketed against the analytic peak of the synthetic input,
  which catches a "fix" that exports exp(lnL), log(lnL), or a zero default.

test_every_sampler_supplies_a_resolvable_lnL_field covers the contract one level down, on
the integrators themselves, so a new sampler that leaves NEITHER key fails here rather
than in a production export.
"""
import os
import re
import subprocess
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
DRIVER = os.path.join(CODE, "bin", "util_ConstructIntrinsicPosterior_GenericCoordinates.py")

# Analytic peak of the synthetic likelihood below.  The exported lnL must land near it:
# too high means something exponentiated, too low means something took a log again.
LNL_PEAK = 20.0

# (id, extra driver args).  Each arm pins one leg of the convention matrix:
#   log-mode GPU  -- 'log_integrand' only; THE arm that used to crash
#   linear GPU    -- same sampler, other entry point, 'integrand' only
#   AV            -- log mode AND the 'integrand' alias
#   GMM           -- log mode via mcsamplerEnsemble
#   adaptive_cartesian -- the linear reference (mcsampler)
ARMS = [
    ("acgpu_lnL", ["--sampler-method", "adaptive_cartesian_gpu", "--internal-use-lnL"]),
    ("acgpu_linear", ["--sampler-method", "adaptive_cartesian_gpu"]),
    ("AV", ["--sampler-method", "AV"]),
    ("GMM", ["--sampler-method", "GMM", "--internal-use-lnL"]),
    ("adaptive_cartesian", ["--sampler-method", "adaptive_cartesian"]),
]


def _write_ile(dirname, n=300):
    """A tiny ILE .dat in the standard 13-column layout, lnL peaked in chirp mass so the
    fit has something to find and the peak value is known in closed form."""
    rng = np.random.default_rng(7)
    m1 = rng.uniform(25.0, 45.0, n)
    m2 = np.minimum(rng.uniform(15.0, 30.0, n), m1)
    mc = (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2
    lnL = LNL_PEAK - 0.5 * ((mc - 28.0) / 1.5) ** 2
    z = np.zeros(n)
    path = os.path.join(dirname, "ile.dat")
    np.savetxt(path, np.column_stack([np.arange(n), m1, m2, z, z, z, z, z, z,
                                      lnL, np.full(n, 0.01), np.full(n, 1000.0),
                                      np.full(n, 100.0)]))
    return path


def _run_arm(tmp_path, extra):
    fname = _write_ile(str(tmp_path))
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["OMP_NUM_THREADS"] = "1"
    env["MPLBACKEND"] = "Agg"
    env["XDG_CACHE_HOME"] = os.path.join(str(tmp_path), "cache")
    env["MPLCONFIGDIR"] = os.path.join(str(tmp_path), "mpl")
    cmd = [sys.executable, DRIVER,
           "--fname", fname,
           "--parameter", "mc", "--parameter", "delta_mc",
           "--fit-method", "rf",
           "--n-max", "20000", "--n-eff", "30",
           "--n-output-samples", "50",
           "--fname-output-samples", "out",
           "--fname-output-integral", "out_int",
           "--no-plots"] + extra
    # timeout MUST stay under the integration-check job's timeout, for the reason
    # test_cip_portfolio_members.py records: at a larger value GitHub cancels the job
    # first and the hung driver yields no test diagnostic.
    return subprocess.run(cmd, cwd=str(tmp_path), env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          universal_newlines=True, timeout=900)


@pytest.fixture(scope="module", params=[a[1] for a in ARMS], ids=[a[0] for a in ARMS])
def cip_export_run(request, tmp_path_factory):
    if not os.path.exists(DRIVER):
        pytest.fail("driver missing: %s" % DRIVER)
    tmp_path = tmp_path_factory.mktemp("cip_export")
    proc = _run_arm(tmp_path, request.param)
    return proc, tmp_path


def test_posterior_export_completes(cip_export_run):
    """The regression proper.  Before the fix the acgpu_lnL arm reached this point with a
    converged integral and then exited 1 on KeyError: 'integrand'."""
    proc, tmp_path = cip_export_run
    assert "KeyError" not in proc.stdout, \
        "driver raised a KeyError in the export path:\n%s" % proc.stdout[-4000:]
    assert proc.returncode == 0, \
        "driver exited %d:\n%s" % (proc.returncode, proc.stdout[-4000:])
    # The integral alone is not the deliverable -- the crash happened AFTER *_int.dat was
    # written, so a test that only checked the integral would have passed on the bug.
    assert os.path.exists(os.path.join(str(tmp_path), "out.xml.gz")), \
        "no posterior samples written:\n%s" % proc.stdout[-2000:]
    assert os.path.exists(os.path.join(str(tmp_path), "out_lnL.dat"))


def test_exported_lnL_is_lnL_not_L(cip_export_run):
    """Bracket the exported column against the input's analytic peak.

    This is what separates the fix from the wrong fixes.  Populating the missing key with
    exp(log_integrand) exports ~e^20 = 5e8; taking a further log of an already-log field
    exports ~log(20) = 3.0; a .get(...) default exports 0.  All three pass an exit-code
    check and all three are caught here.  The upper bound is the driver's OWN reported
    maximum, not a literal, so the fit's approximation error cannot make it flaky.
    """
    proc, tmp_path = cip_export_run
    lnL = np.atleast_1d(np.loadtxt(os.path.join(str(tmp_path), "out_lnL.dat")))
    assert lnL.size > 0
    assert np.all(np.isfinite(lnL)), "non-finite exported lnL: %r" % lnL[~np.isfinite(lnL)][:5]

    m = re.search(r"^ Max lnL\s+(\S+)", proc.stdout, re.M)
    assert m, "driver did not report its own max lnL:\n%s" % proc.stdout[-2000:]
    reported_max = float(m.group(1))
    # The export is a weighted draw from the same array the driver took its max over, so
    # it can only be <=; equality up to float32 storage in the .dat.
    assert lnL.max() <= reported_max + 1e-4, \
        "exported lnL exceeds the driver's own max: %g > %g" % (lnL.max(), reported_max)
    # ...and the draw is dominated by the peak, so it must get near it.  A 3-nat floor is
    # far wider than the fit error (measured spread of lnL.max() across these five arms:
    # under 0.2 nats) and far narrower than any convention error.
    assert lnL.max() > LNL_PEAK - 3.0, \
        "exported lnL peaks at %g, nowhere near the input's analytic peak %g" % (
            lnL.max(), LNL_PEAK)


def test_every_sampler_supplies_a_resolvable_lnL_field():
    """The contract one level down: the driver's resolution rule must find a field.

    The export crash was a SECOND site reading the raw key after this rule had already
    run.  Removing that site is only safe while the rule itself always succeeds, so assert
    it directly on the integrators rather than only through a driver run -- a new sampler
    that leaves neither key fails here in seconds instead of in a production export.
    """
    from RIFT.integrators import (mcsampler, mcsamplerGPU, mcsamplerAdaptiveVolume,
                                  mcsamplerEnsemble)

    def run(sampler, use_lnL):
        sampler.add_parameter("x", pdf=lambda x: np.ones_like(x) / 2.0,
                              prior_pdf=lambda x: np.ones_like(x) / 2.0,
                              left_limit=-1.0, right_limit=1.0,
                              adaptive_sampling=True)
        # mcsampler hands the integrand an object-dtype array, so cast before any ufunc.
        lnf = lambda x: -0.5 * np.asarray(x, dtype=float) ** 2
        fn = lnf if use_lnL else (lambda x: np.exp(lnf(x)))
        kw = dict(n=200, nmax=2000, neff=10, save_intg=True, verbose=False)
        if use_lnL:
            kw.update(use_lnL=True, return_lnI=True)
        sampler.integrate(fn, "x", **kw)
        return sampler._rvs

    cases = [
        ("mcsampler linear", mcsampler.MCSampler(), False),
        ("mcsamplerGPU linear", mcsamplerGPU.MCSampler(), False),
        ("mcsamplerGPU log", mcsamplerGPU.MCSampler(), True),
        ("mcsamplerAdaptiveVolume log", mcsamplerAdaptiveVolume.MCSampler(), True),
        ("mcsamplerEnsemble log", mcsamplerEnsemble.MCSampler(), True),
    ]
    for name, sampler, use_lnL in cases:
        rvs = run(sampler, use_lnL)
        # The driver's rule, in the driver's order: log_integrand wins if present.
        if "log_integrand" in rvs:
            dat_logL = np.asarray(rvs["log_integrand"], dtype=float)
        elif "integrand" in rvs:
            _raw = np.asarray(rvs["integrand"], dtype=float)
            dat_logL = _raw if use_lnL else np.log(_raw)
        else:
            pytest.fail("%s left neither 'log_integrand' nor 'integrand' in _rvs: %r"
                        % (name, sorted(str(k) for k in rvs)))
        # -0.5 x^2 on x in [-1,1]: lnL in [-0.5, 0].  A field stored under the wrong
        # convention lands outside this by orders of magnitude.
        x = np.asarray(rvs["x"], dtype=float).reshape(-1)
        np.testing.assert_allclose(dat_logL.reshape(-1), -0.5 * x ** 2,
                                   rtol=1e-5, atol=1e-6,
                                   err_msg="%s: resolved lnL is not lnL" % name)
