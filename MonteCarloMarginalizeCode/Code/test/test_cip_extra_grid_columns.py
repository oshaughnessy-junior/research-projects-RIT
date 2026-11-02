"""CIP and PUFF: named extra columns (lambda) of a Hyperpipe grid -- fit, pin, ignore, refuse.

ILE's extra-parameter seam (--supplementary-waveform-transform-parameter) echoes each grid
point's lambda (e.g. a lensing amplitude) into its Hyperpipe output row.  CIP used to reshape
that table to the fixed legacy layout and drop every column it did not know, so rows differing
only in lambda were pooled and the next-iteration grid lost lambda.  Asserted here, all by
driving util_ConstructIntrinsicPosterior_GenericCoordinates.py end to end:

* recovery: lnL is Gaussian in mc and in lens_amp; with --parameter lens_amp and a uniform
  prior range, the exported posterior recovers the lens_amp peak and width, the exported lnL
  matches the analytic likelihood AT EACH ROW'S OWN (mc, lens_amp) -- which pins the row
  alignment of the carried column -- and a pinned column is written back as a constant;
* refusal: an extra column that is neither fit, pinned nor ignored stops the run (exit 2,
  named), as do a sampled column without a range and --parameter-implied alone;
* ignore == drop: with --extra-column-ignore the run is byte-identical (same numpy seed) to a
  run on the same grid with the column deleted, and the ordinary no-extra-column Hyperpipe run
  is byte-identical to the same data in the legacy ILE .dat layout;
* the quadpuff contingency carries (puffed) lambda inside its prior range;
* util_ParameterPuffball.py puffs a lambda given with --parameter inside its declared range,
  passes every other lambda through with its own row, and changes nothing else.
"""
import hashlib
import os
import subprocess
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
DRIVER = os.path.join(CODE, "bin", "util_ConstructIntrinsicPosterior_GenericCoordinates.py")
sys.path.insert(0, CODE)
from RIFT.misc import hyperpipeline_io as hpio  # noqa: E402

MC0, MC_W, LAM0, LAM_W, LAM_RANGE = 28.0, 1.5, 1.1, 0.1, (0.5, 1.5)

# Seeded entry point: CIP has no seed option, so seed numpy's global stream and run the driver
# in-process.  Used only by the byte-identity comparisons.
_SEEDED = ("import sys, runpy, random, numpy; numpy.random.seed(1234); random.seed(1234); "
           "sys.argv = [sys.argv[1]] + sys.argv[2:]; runpy.run_path(sys.argv[0], run_name='__main__')")


def _lnL(mc, lam):
    return 20.0 - 0.5 * ((mc - MC0) / MC_W) ** 2 - 0.5 * ((lam - LAM0) / LAM_W) ** 2


def _grid(n=600, seed=7):
    rng = np.random.default_rng(seed)
    m1 = rng.uniform(25.0, 45.0, n)
    m2 = np.minimum(rng.uniform(15.0, 30.0, n), m1)
    mc = (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2
    lam = rng.uniform(LAM_RANGE[0], LAM_RANGE[1], n)
    z = np.zeros(n)
    return m1, m2, mc, lam, z


def _write_lambda_grid(path, other=0.25):
    m1, m2, mc, lam, z = _grid()
    cols = hpio.build_column_list() + ("lens_amp", "lens_other")
    hpio.write_table(path, cols, np.column_stack(
        [_lnL(mc, lam), np.full(len(m1), 0.01), m1, m2, z, z, z, z, z, z, lam,
         np.full(len(m1), other)]))
    return path


def _env(tmp):
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.update(OMP_NUM_THREADS="1", MPLBACKEND="Agg", GW_SURROGATE="",
               XDG_CACHE_HOME=os.path.join(tmp, "cache"), MPLCONFIGDIR=os.path.join(tmp, "mpl"))
    env.pop("RIFT_HYPERPIPELINE_FORMAT", None)
    return env


def _run(cwd, fname, args, seeded=False):
    os.makedirs(cwd, exist_ok=True)
    pre = [sys.executable, "-c", _SEEDED, DRIVER] if seeded else [sys.executable, DRIVER]
    cmd = pre + ["--fname", fname, "--fit-method", "quadratic",
                 "--fname-output-samples", "out", "--fname-output-integral", "out_int",
                 "--no-plots"] + list(args)
    # timeout below the CI job cap (see test_cip_portfolio_members.py)
    return subprocess.run(cmd, cwd=cwd, env=_env(cwd), stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, universal_newlines=True, timeout=900)


_FIT = ["--parameter", "mc", "--parameter", "delta_mc", "--parameter", "lens_amp",
        "--integration-parameter-range", "lens_amp:[%g,%g]" % LAM_RANGE]


@pytest.fixture(scope="module")
def recovery_run(tmp_path_factory):
    tmp = str(tmp_path_factory.mktemp("cip_extra"))
    grid = _write_lambda_grid(os.path.join(tmp, "grid.dat"))
    proc = _run(os.path.join(tmp, "run"), grid,
                _FIT + ["--extra-column-pin", "lens_other:0.25", "--n-max", "100000",
                        "--n-eff", "300", "--n-output-samples", "2000"])
    return proc, os.path.join(tmp, "run")


def test_extra_column_posterior_recovers_lambda(recovery_run):
    proc, d = recovery_run
    assert proc.returncode == 0, proc.stdout[-4000:]
    arr, cols = hpio.read_table(os.path.join(d, "out.dat"))
    assert cols[-2:] == ("lens_amp", "lens_other"), cols
    assert len(arr) > 500
    lam = arr["lens_amp"]
    assert np.all((lam >= LAM_RANGE[0]) & (lam <= LAM_RANGE[1]))
    # Truth N(1.1, 0.1) times a uniform prior wide enough not to clip it.  2000 weighted
    # draws: the mean is good to ~0.005, the width to ~5 percent; measured 1.098 / 0.0955.
    assert abs(lam.mean() - LAM0) < 0.02, lam.mean()
    assert abs(lam.std() / LAM_W - 1.0) < 0.15, lam.std()
    mc = (arr["m1"] * arr["m2"]) ** 0.6 / (arr["m1"] + arr["m2"]) ** 0.2
    assert abs(mc.mean() - MC0) < 0.2 and abs(mc.std() / MC_W - 1.0) < 0.15
    # Row alignment of the carried column: exported lnL equals the analytic likelihood at the
    # row's own (mc, lens_amp).  Quadratic fit of an exactly quadratic lnL: measured 2e-6;
    # permuting lens_amp across rows moves this to O(10).
    assert np.max(np.abs(arr["lnL"] - _lnL(mc, lam))) < 1e-2
    np.testing.assert_array_equal(arr["lens_other"], 0.25)


@pytest.mark.parametrize("args,needle", [
    (["--parameter", "mc", "--parameter", "delta_mc"], "neither fits nor pins nor ignores"),
    (["--parameter", "mc", "--parameter", "delta_mc", "--parameter", "lens_amp",
      "--extra-column-pin", "lens_other:0.25"], "--integration-parameter-range lens_amp"),
    (["--parameter", "mc", "--parameter", "delta_mc", "--parameter-implied", "lens_amp",
      "--extra-column-pin", "lens_other:0.25"], "must be both fit and sampled"),
    (_FIT + ["--extra-column-pin", "lens_other:0.25", "--extra-column-ignore", "lens_other"],
     "conflicting treatments"),
    (_FIT + ["--extra-column-pin", "lens_other:0.25", "--integration-parameter-range", "mc:[1,2]"],
     "not an extra column"),
], ids=["unhandled", "no_range", "implied_only", "pin_and_ignore", "range_for_standard"])
def test_extra_column_refusals(tmp_path, args, needle):
    grid = _write_lambda_grid(str(tmp_path / "grid.dat"))
    proc = _run(str(tmp_path / "run"), grid, args + ["--n-max", "2000", "--n-eff", "10"])
    assert proc.returncode == 2, proc.stdout[-3000:]
    assert needle in proc.stdout, proc.stdout[-3000:]
    assert not os.path.exists(str(tmp_path / "run" / "out.dat"))


def test_extra_column_options_refused_on_legacy_input(tmp_path):
    m1, m2, mc, lam, z = _grid(50)
    fname = str(tmp_path / "ile.dat")
    np.savetxt(fname, np.column_stack([np.arange(50), m1, m2, z, z, z, z, z, z, _lnL(mc, LAM0),
                                       np.full(50, 0.01), np.full(50, 1e3), np.full(50, 1e2)]))
    proc = _run(str(tmp_path / "run"), fname, ["--parameter", "mc", "--extra-column-ignore", "x"])
    assert proc.returncode == 2 and "Hyperpipe" in proc.stdout, proc.stdout[-3000:]


def _digest(d):
    """sha256 of every output file except logs, args.txt (records paths) and figures."""
    out = {}
    for name in sorted(os.listdir(d)):
        if name == "args.txt" or name.endswith((".png", ".log")) or os.path.isdir(os.path.join(d, name)):
            continue
        with open(os.path.join(d, name), "rb") as fp:
            out[name] = hashlib.sha256(fp.read()).hexdigest()
    return out


_SMALL = ["--parameter", "mc", "--parameter", "delta_mc", "--n-max", "20000", "--n-eff", "30",
          "--n-output-samples", "50", "--sampler-method", "adaptive_cartesian"]


def test_ignore_is_byte_identical_to_dropping_the_column(tmp_path):
    m1, m2, mc, lam, z = _grid(300)
    lnL = _lnL(mc, LAM0)
    base = [lnL, np.full(len(m1), 0.01), m1, m2, z, z, z, z, z, z]
    plain = str(tmp_path / "plain.dat")
    hpio.write_table(plain, hpio.build_column_list(), np.column_stack(base))
    extra = str(tmp_path / "extra.dat")
    hpio.write_table(extra, hpio.build_column_list() + ("lens_amp",), np.column_stack(base + [lam]))
    legacy = str(tmp_path / "ile.dat")
    np.savetxt(legacy, np.column_stack([np.arange(len(m1)), m1, m2, z, z, z, z, z, z, lnL,
                                        np.full(len(m1), 0.01), np.full(len(m1), 1e3),
                                        np.full(len(m1), 1e2)]))
    runs = {}
    for tag, fname, extra_args in (("plain", plain, []), ("ignored", extra, ["--extra-column-ignore", "lens_amp"]),
                                   ("legacy", legacy, [])):
        d = str(tmp_path / tag)
        proc = _run(d, fname, _SMALL + extra_args, seeded=True)
        assert proc.returncode == 0, proc.stdout[-3000:]
        runs[tag] = _digest(d)
    assert runs["ignored"] == runs["plain"]
    # The legacy run writes XML samples (with process timestamps) where the Hyperpipe run
    # writes out.dat; every other product must agree byte for byte.
    common = sorted(set(runs["plain"]) & set(runs["legacy"]))
    assert "out_int.dat" in common and "out_lnL.dat" in common
    assert dict((k, runs["plain"][k]) for k in common) == dict((k, runs["legacy"][k]) for k in common)
    assert "lens_amp" not in hpio.read_header(str(tmp_path / "ignored" / "out.dat"))


def test_quadpuff_contingency_carries_lambda(tmp_path):
    grid = _write_lambda_grid(str(tmp_path / "grid.dat"))
    d = str(tmp_path / "run")
    proc = _run(d, grid, _FIT + ["--extra-column-pin", "lens_other:0.25", "--n-max", "3000",
                                 "--n-chunk", "1000", "--n-eff", "100000", "--n-output-samples", "200",
                                 "--contingency-unevolved-neff", "quadpuff"])
    assert proc.returncode == 0, proc.stdout[-3000:]
    assert "Contingency: quadpuff" in proc.stdout
    arr, cols = hpio.read_table(os.path.join(d, "out.dat"))
    assert cols[-2:] == ("lens_amp", "lens_other") and len(arr) > 0
    lam = arr["lens_amp"]
    assert np.all((lam >= LAM_RANGE[0]) & (lam <= LAM_RANGE[1]))
    # puffed, not copied: the output lambda are not all input grid values
    _, _, _, lam_in, _ = _grid()
    assert np.mean(np.isin(lam, lam_in)) < 0.5
    np.testing.assert_array_equal(arr["lens_other"], 0.25)


# ---------------------------------------------------------------------------
# util_ParameterPuffball.py: the PUFF stage of the ILE -> CIP -> PUFF -> ILE loop.  Before this
# change a Hyperpipe grid carrying lambda made puff call P.extract_param('lens_amp') at write
# time, which prints "No attribute" and exits 0 with NO output file.
# ---------------------------------------------------------------------------
PUFF = os.path.join(CODE, "bin", "util_ParameterPuffball.py")


def _posterior_grid(path, n=800, seed=3):
    rng = np.random.default_rng(seed)
    mc = rng.normal(MC0, MC_W, n)
    eta = rng.uniform(0.2, 0.24, n)
    m1 = mc * eta ** -0.6 * (1 + np.sqrt(1 - 4 * eta)) / 2
    m2 = mc * eta ** -0.6 * (1 - np.sqrt(1 - 4 * eta)) / 2
    lam = np.clip(rng.normal(LAM0, LAM_W, n), *LAM_RANGE)
    z = np.zeros(n)
    hpio.write_table(path, hpio.build_column_list() + ("lens_amp", "lens_other"),
                     np.column_stack([z, z, m1, m2, z, z, z, z, z, z, lam, np.full(n, 0.25)]))
    return path


def _puff(cwd, args):
    os.makedirs(cwd, exist_ok=True)
    return subprocess.run([sys.executable, PUFF, "--random-seed", "11", "--inj-file-out", "puff"] + args,
                          cwd=cwd, env=_env(cwd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          universal_newlines=True, timeout=900)


def test_puff_puffs_lambda_inside_range_and_passes_pinned(tmp_path):
    grid = _posterior_grid(str(tmp_path / "post.dat"))
    d = str(tmp_path / "run")
    proc = _puff(d, ["--inj-file", grid, "--parameter", "mc", "--parameter", "delta_mc",
                     "--parameter", "lens_amp", "--integration-parameter-range", "lens_amp:[%g,%g]" % LAM_RANGE,
                     "--puff-factor", "1"])
    assert proc.returncode == 0, proc.stdout[-3000:]
    arr, cols = hpio.read_table(os.path.join(d, "puff.dat"))
    a_in, _ = hpio.read_table(grid)
    assert cols[-2:] == ("lens_amp", "lens_other") and len(arr) > 0.5 * len(a_in)
    lam = arr["lens_amp"]
    assert np.all((lam >= LAM_RANGE[0]) & (lam <= LAM_RANGE[1]))
    # puff-factor 1 doubles the variance (before the range cut, which barely bites at 4 sigma)
    assert 1.2 < lam.std() / a_in["lens_amp"].std() < 1.6, lam.std()
    assert not np.isin(lam, a_in["lens_amp"]).any()
    np.testing.assert_array_equal(arr["lens_other"], 0.25)


def test_puff_passes_unpuffed_lambda_through_row_coherently(tmp_path):
    """lambda not given with --parameter rides with its own row; and carrying it changes no
    other output value (same seed, same draws) relative to the grid without the columns."""
    grid = _posterior_grid(str(tmp_path / "post.dat"))
    a_in, cols_in = hpio.read_table(grid)
    # Tag each row: a distinct a1z (not puffed, so it survives on P) and lens_other = row id.
    n = len(a_in)
    a_in["a1z"] = np.linspace(-0.5, 0.5, n)
    a_in["lens_other"] = np.arange(n, dtype=float)
    hpio.write_table(grid, cols_in, np.column_stack([a_in[c] for c in cols_in]))
    plain = str(tmp_path / "plain.dat")
    std = hpio.build_column_list()
    hpio.write_table(plain, std, np.column_stack([a_in[c] for c in std]))
    common = ["--parameter", "mc", "--parameter", "delta_mc", "--puff-factor", "1"]
    p1 = _puff(str(tmp_path / "lam"), ["--inj-file", grid] + common)
    p2 = _puff(str(tmp_path / "plain"), ["--inj-file", plain] + common)
    assert p1.returncode == 0 and p2.returncode == 0, (p1.stdout[-2000:], p2.stdout[-2000:])
    a1, c1 = hpio.read_table(str(tmp_path / "lam" / "puff.dat"))
    a2, c2 = hpio.read_table(str(tmp_path / "plain" / "puff.dat"))
    assert c1 == c2 + ("lens_amp", "lens_other") and len(a1) == len(a2) > 0.5 * n
    for c in c2:
        np.testing.assert_array_equal(a1[c], a2[c])
    rows = a1["lens_other"].astype(int)
    np.testing.assert_array_equal(a1["lens_amp"], a_in["lens_amp"][rows])
    np.testing.assert_allclose(a1["a1z"], a_in["a1z"][rows], rtol=0, atol=1e-12)


def test_puff_refuses_puffed_lambda_without_range(tmp_path):
    grid = _posterior_grid(str(tmp_path / "post.dat"))
    proc = _puff(str(tmp_path / "run"), ["--inj-file", grid, "--parameter", "mc", "--parameter", "lens_amp"])
    assert proc.returncode == 2 and "--integration-parameter-range lens_amp" in proc.stdout, proc.stdout[-2000:]
