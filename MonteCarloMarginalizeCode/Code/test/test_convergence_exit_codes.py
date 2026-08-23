"""A crashing convergence test must not look like convergence.

`convergence_test_samples.py` uses exit 1 to mean "converged, stop", and the
DAG is wired to treat that as success (`ABORT-DAG-ON <node> 1 RETURN 0`).  So
any *crash* that also exits 1 ends the workflow early and reports success.

That is not hypothetical.  numpy removed `np.asscalar` in 1.23, which broke
`--method lame` outright -- and the failure was invisible: the M1 comparison
run showed BasicIteration reporting `ABORT-DAG-ON ... exit 1, workflow returns
0` while the identical crash under a builder without that wiring surfaced as a
node failure.  Same defect, two different stories, neither of them "the
convergence test is broken".

Two further problems in the same place:

* `--always-succeed` was evaluated AFTER the test value was computed, so it
  could not protect against a method that raised -- the opposite of what the
  flag says.
* `np.asscalar` itself.

These tests pin all three.
"""

import os
import subprocess
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
EXE = os.path.join(CODE, "bin", "convergence_test_samples.py")


#: The loader derives chi1/chi2 from the Cartesian spin columns, so a
#: two-column fixture crashes during READ and never reaches the code under
#: test.  The first version of this file did exactly that and mis-attributed
#: the resulting failures to the guards.
COLUMNS = ["m1", "m2", "a1x", "a1y", "a1z", "a2x", "a2y", "a2z"]


def _samples(path, n=400, shift=0.0, seed=0):
    rng = np.random.default_rng(seed)
    data = np.column_stack(
        [rng.normal(30.0 + shift, 1.0, n), rng.normal(20.0, 1.0, n)]
        + [rng.normal(0.0, 0.01, n) for _ in COLUMNS[2:]])
    np.savetxt(str(path), data, header=" ".join(COLUMNS))
    return str(path)


def _run(args, timeout=300):
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run([sys.executable, EXE] + args, env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=timeout)


def test_the_lame_method_runs_at_all(tmp_path):
    """np.asscalar was removed in numpy 1.23; --method lame died on import."""
    a = _samples(tmp_path / "a.dat", seed=1)
    b = _samples(tmp_path / "b.dat", seed=2)
    result = _run(["--samples", a, "--samples", b, "--method", "lame",
                   "--parameter", "m1", "--threshold", "1e9"])
    assert "asscalar" not in result.stdout, result.stdout[-1500:]
    assert result.returncode in (0, 1), result.stdout[-1500:]


def test_always_succeed_survives_a_method_that_raises(tmp_path):
    """The flag must short-circuit BEFORE the test runs, or it is decorative."""
    a = _samples(tmp_path / "a.dat", seed=1)
    result = _run(["--samples", a, "--samples", a, "--method", "lame",
                   "--parameter", "does_not_exist", "--always-succeed"])
    assert result.returncode == 0, result.stdout[-1500:]


def test_a_crash_exits_2_not_1(tmp_path):
    """Exit 1 is claimed by 'converged'; a crash must be distinguishable.

    A method that raises must not produce the one status the DAG converts into
    overall success.
    """
    a = _samples(tmp_path / "a.dat", seed=1)
    b = _samples(tmp_path / "b.dat", seed=2)
    result = _run(["--samples", a, "--samples", b, "--method", "js_lame",
                   "--parameter", "not_a_column", "--threshold", "0.5"])
    assert result.returncode != 1, (
        "a failing convergence test exited 1, which the DAG treats as "
        "'converged' and converts into a successful workflow:\n"
        + result.stdout[-2000:])
    if result.returncode != 0:
        assert result.returncode == 2, result.stdout[-2000:]
        assert "exiting 2, NOT 1" in result.stdout


def test_an_unknown_method_still_never_converges(tmp_path):
    """The pre-existing loud-but-safe behaviour must survive the new guard."""
    a = _samples(tmp_path / "a.dat", seed=1)
    b = _samples(tmp_path / "b.dat", seed=2)
    result = _run(["--samples", a, "--samples", b, "--method", "nonsense",
                   "--parameter", "m1", "--threshold", "0.5"])
    assert result.returncode == 0, result.stdout[-1500:]
    assert "UNKNOWN METHOD" in result.stdout
