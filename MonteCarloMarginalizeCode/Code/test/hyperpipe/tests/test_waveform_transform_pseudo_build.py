"""pseudo_pipe --internal-ile-supplementary-waveform-transform-* (ILE extra-parameter seam).

Real util_RIFT_pseudo_pipe.py builds (empty fake-data cache, no network), as in
integration/run_pseudo_build_gate.py:

  * Hyperpipe route: the ILE flags reach args_ile.txt with all four NoLoop flags; under
    --use-osg the plugin and ini are on the ILE transfer list (absolute, in
    helper_transfer_files.txt and the MARG spec) and referenced by basename; on a shared
    filesystem they keep absolute paths.  The build then stops before the DAG, because the
    loop's posterior (CIP) and puff stages cannot carry lambda -- that refusal is pinned too.
  * Refusals before any helper runs: legacy builders, --add-extrinsic, --use-jax-ile, no or
    wrong initial grid, a dotted module under --use-osg, an orphan -parameter.
Unit checks of RIFT.misc.waveform_transform_pipeline cover the ILE-line audit and the OSG
basename collision.
"""
import json
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

CODE = Path(__file__).resolve().parents[3]
BIN = CODE / "bin"
PSEUDO_PIPE = BIN / "util_RIFT_pseudo_pipe.py"
NOLOOP = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")

PLUGIN = '''
import numpy
EXTRA_PARAMETERS = ["lens_amplitude"]
def transfer_function(f, extra_params, P=None, xpy=numpy, **kw):
    return extra_params["lens_amplitude"] * numpy.ones(numpy.shape(f), dtype=complex)
'''


def _env(base):
    env = os.environ.copy()
    env.pop("RIFT_HYPERPIPELINE_FORMAT", None)
    env["RIFT_DAG_BACKEND"] = "htcondor"
    env["PYTHONPATH"] = os.pathsep.join([str(CODE), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env["PATH"] = os.pathsep.join([str(BIN), env.get("PATH", "")]).rstrip(os.pathsep)
    env["GW_SURROGATE"] = ""
    for name in ("XDG_CACHE_HOME", "MPLCONFIGDIR"):
        d = base / name.lower()
        d.mkdir(parents=True, exist_ok=True)
        env[name] = str(d)
    return env


@pytest.fixture
def inputs(tmp_path):
    import lal
    import RIFT.lalsimutils as lsu
    from RIFT.misc import hyperpipeline_io as hpio
    P = lsu.ChooseWaveformParams()
    P.m1, P.m2, P.fref, P.fmin = 36 * lal.MSUN_SI, 29 * lal.MSUN_SI, 20.0, 20.0

    def grid(name, extra_cols, extra_vals):
        cols = tuple(hpio.DEFAULT_BASE_COLUMNS) + tuple(extra_cols)
        rows = [[0, 0, 36, 29, 0, 0, 0.1, 0, 0, 0.05] + list(v) for v in extra_vals]
        hpio.write_table_with_metadata(str(tmp_path / name), cols, rows, P=P)
        return tmp_path / name

    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    (plugin_dir / "lens_plug.py").write_text(PLUGIN)
    (plugin_dir / "lens_plug.ini").write_text("[lens]\nscale = 1\n")
    (tmp_path / "empty.cache").touch()
    return types.SimpleNamespace(
        base=tmp_path,
        grid=grid("seed.dat", ("lens_amplitude", "lens_extra"), [(0.8, 1.0), (1.2, 2.0)]),
        grid_no_lambda=grid("seed_plain.dat", (), [()]),
        plugin=plugin_dir / "lens_plug.py", ini=plugin_dir / "lens_plug.ini")


def _build(inp, label, extra, builder="Hyperpipe", grid=None):
    rundir = inp.base / label
    cmd = [sys.executable, str(PSEUDO_PIPE), "--use-rundir", str(rundir)]
    if builder:
        cmd += ["--pipeline-builder", builder]
    if grid is not False:
        cmd += ["--manual-initial-grid", str(grid or inp.grid)]
    cmd += ["--fake-data-cache", str(inp.base / "empty.cache"),
            "--event-time", "1126259462.391", "--manual-ifo-list", "H1,L1",
            "--approx", "IMRPhenomD", "--fmin-template", "20",
            "--internal-force-iterations", "2", "--internal-n-evaluations-per-iteration", "4",
            "--n-output-samples", "7", "--n-output-samples-last", "7",
            "--ile-jobs-per-worker", "3", "--cip-explode-jobs", "1",
            "--skip-reproducibility"] + list(extra)
    r = subprocess.run(cmd, cwd=str(inp.base), env=_env(inp.base), text=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
    return r, rundir


def _seam(inp, ini=True, parameter=True):
    args = ["--internal-ile-supplementary-waveform-transform-code", str(inp.plugin),
            "--internal-ile-supplementary-waveform-transform-function", "transfer_function"]
    if ini:
        args += ["--internal-ile-supplementary-waveform-transform-ini", str(inp.ini)]
    if parameter:
        args += ["--internal-ile-supplementary-waveform-transform-parameter", "lens_extra"]
    return args


def _assert_stopped_before_dag(r, rundir):
    assert r.returncode != 0, r.stdout[-3000:]
    assert "cannot yet carry the extra parameters ['lens_amplitude', 'lens_extra']" in r.stdout, \
        r.stdout[-3000:]
    assert not list(rundir.glob("*.dag")), sorted(p.name for p in rundir.iterdir())


def test_hyperpipe_osg_threads_flags_and_stages_plugin_and_ini(inputs):
    r, rundir = _build(inputs, "osg", ["--use-osg", "--use-osg-file-transfer"] + _seam(inputs))
    _assert_stopped_before_dag(r, rundir)
    args = (rundir / "args_ile.txt").read_text().split()
    for flag in NOLOOP:
        assert flag in args, flag
    pairs = list(zip(args, args[1:]))
    assert ("--supplementary-waveform-transform-code", "lens_plug.py") in pairs
    assert ("--supplementary-waveform-transform-ini", "lens_plug.ini") in pairs
    assert ("--supplementary-waveform-transform-function", "transfer_function") in pairs
    assert ("--supplementary-waveform-transform-parameter", "lens_extra") in pairs
    assert str(inputs.base) not in " ".join(a for a in args if "lens_plug" in a)
    transfers = (rundir / "helper_transfer_files.txt").read_text().split()
    assert str(inputs.plugin) in transfers and str(inputs.ini) in transfers
    spec = json.loads((rundir / "marg_job_specs.json").read_text())[0]
    assert spec["args_file"] == str(rundir / "args_ile.txt")
    assert str(inputs.plugin) in spec["execution"]["transfer_files"]
    assert str(inputs.ini) in spec["execution"]["transfer_files"]


def test_hyperpipe_shared_filesystem_keeps_absolute_paths(inputs):
    r, rundir = _build(inputs, "shared", _seam(inputs, ini=False))
    _assert_stopped_before_dag(r, rundir)
    args = (rundir / "args_ile.txt").read_text().split()
    pairs = list(zip(args, args[1:]))
    assert ("--supplementary-waveform-transform-code", str(inputs.plugin)) in pairs
    assert "--supplementary-waveform-transform-ini" not in args
    assert all(flag in args for flag in NOLOOP)


@pytest.mark.parametrize("label,builder,extra,grid,message", [
    ("default-builder", None, [], None, "--pipeline-builder BasicIteration (default)"),
    ("basic", "BasicIteration", [], None, "--pipeline-builder BasicIteration"),
    ("extrinsic", "Hyperpipe", ["--add-extrinsic", "--add-extrinsic-time-resampling"], None,
     "ILE refuses --save-samples"),
    ("jax", "Hyperpipe", ["--use-jax-ile"], None, "--use-jax-ile"),
    ("no-grid", "Hyperpipe", [], False, "no --manual-initial-grid"),
    ("plain-grid", "Hyperpipe", [], "plain", "lacks extra-parameter column(s) ['lens_amplitude', 'lens_extra']"),
])
def test_refusals_before_any_helper_runs(inputs, label, builder, extra, grid, message):
    grid = inputs.grid_no_lambda if grid == "plain" else grid
    r, rundir = _build(inputs, label, extra + _seam(inputs), builder=builder, grid=grid)
    assert r.returncode != 0, r.stdout[-3000:]
    assert "refusing" in r.stdout and message in r.stdout, r.stdout[-3000:]
    assert not rundir.exists()          # refused before the run directory was populated


def test_dotted_module_refused_under_osg_and_orphan_parameter_refused(inputs):
    r, rundir = _build(inputs, "dotted", ["--use-osg", "--use-osg-file-transfer",
                                          "--internal-ile-supplementary-waveform-transform-code",
                                          "some_pkg.lens_module"])
    assert r.returncode != 0 and "dotted module name" in r.stdout, r.stdout[-3000:]
    r, rundir = _build(inputs, "orphan", ["--internal-ile-supplementary-waveform-transform-parameter", "x"])
    assert r.returncode != 0 and "require --internal-ile-supplementary-waveform-transform-code" in r.stdout


def test_default_build_is_unaffected(inputs):
    """No seam option: the Hyperpipe build of the same lambda-free grid still completes."""
    r, rundir = _build(inputs, "plain", [], grid=inputs.grid_no_lambda)
    assert r.returncode == 0, r.stdout[-3000:]
    assert "supplementary-waveform-transform" not in (rundir / "args_ile.txt").read_text()


def test_ile_line_audit_and_osg_basename_collision(tmp_path):
    from RIFT.misc import waveform_transform_pipeline as wtp
    ok = "X --time-marginalization --vectorized --gpu --force-xpy --sim-grid g.dat"
    assert wtp.ile_argument_refusals(ok) == []
    assert wtp.ile_argument_refusals(ok.replace(" --force-xpy", "")) == [
        "missing --force-xpy (the seam runs only on the NoLoop likelihood)"]
    for extra, reason in [(" --rotation-slow", "--rotation-slow"),
                          (" --calibration-envelope-directory d", "--calibration-envelope-directory"),
                          (" --internal-ile-inner-product td-gs", "--internal-ile-inner-product td-gs"),
                          (" --time-marginalization-quadrature=peak-local", "peak-local")]:
        assert any(reason in b for b in wtp.ile_argument_refusals(ok + extra)), extra
    assert wtp.ile_argument_refusals(ok + " --internal-ile-inner-product fd-diagonal") == []

    plug = tmp_path / "a" / "p.py"
    plug.parent.mkdir()
    plug.write_text(PLUGIN)
    other = tmp_path / "b" / "p.py"
    other.parent.mkdir()
    other.write_text(PLUGIN)
    opts = types.SimpleNamespace(
        internal_ile_supplementary_waveform_transform_code=str(plug),
        internal_ile_supplementary_waveform_transform_function=None,
        internal_ile_supplementary_waveform_transform_ini=None,
        internal_ile_supplementary_waveform_transform_parameter=None)
    text, files = wtp.ile_arguments(opts, "/x/psd.xml.gz", use_osg=True)
    assert text.split() == ["--supplementary-waveform-transform-code", "p.py"]
    assert files == "/x/psd.xml.gz," + str(plug)
    with pytest.raises(ValueError, match="basename collision"):
        wtp.ile_arguments(opts, str(other), use_osg=True)
    text, _ = wtp.ile_arguments(opts, str(other), use_osg=False)   # shared FS: absolute, no clash
    assert text.split()[1] == str(plug)
