"""pseudo_pipe --internal-ile-supplementary-waveform-transform-* (ILE extra-parameter seam).

Real util_RIFT_pseudo_pipe.py builds (empty fake-data cache, no network), as in
integration/run_pseudo_build_gate.py:

  * Hyperpipe route: the ILE flags reach args_ile.txt with all four NoLoop flags; under
    --use-osg the plugin and ini are on the ILE transfer list (absolute, in
    helper_transfer_files.txt and the MARG spec) and referenced by basename; on a shared
    filesystem they keep absolute paths.
  * The loop closes: with each lambda ranged (--internal-supplementary-waveform-transform-
    parameter-range) or pinned, the DAG is built and every EOS_POST (CIP) iteration fits or
    pins lambda, PUFF puffs it in the same range, and every stage runs with the Hyperpipe
    grid format.  Hand-supplied treatments (--manual-extra-cip/puff-args) pass the same guard.
  * Refusals before any helper runs: legacy builders, --add-extrinsic, --use-jax-ile, no or
    wrong initial grid, a dotted module under --use-osg, an orphan -parameter, EOS tables,
    out-of-range or unreachable-pin initial grids, undeclared names.
  * Refusals before the DAG: lambda ignored, fit without a range, untreated, puffed with
    --random-parameter or a different range, a pinned lambda puffed.
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


def _build(inp, label, extra, builder="Hyperpipe", grid=None, env_extra=None):
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
    env = _env(inp.base)
    env.update(env_extra or {})
    r = subprocess.run(cmd, cwd=str(inp.base), env=env, text=True,
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


RANGE = "--internal-supplementary-waveform-transform-parameter-range"
PIN = "--internal-supplementary-waveform-transform-parameter-pin"
REFLECT = "--internal-supplementary-waveform-transform-parameter-reflect"
TREAT = [RANGE, "lens_amplitude:[0.5,2]", PIN, "lens_extra:1"]


def _assert_dag_built(r, rundir):
    assert r.returncode == 0, r.stdout[-4000:]
    assert (rundir / "marginalize_hyperparameters.dag").is_file(), sorted(p.name for p in rundir.iterdir())


def _pairs(text):
    toks = text.split()
    return list(zip(toks, toks[1:]))


def test_hyperpipe_osg_threads_flags_and_stages_plugin_and_ini(inputs):
    r, rundir = _build(inputs, "osg", ["--use-osg", "--use-osg-file-transfer"] + _seam(inputs) + TREAT,
                       env_extra={"SINGULARITY_RIFT_IMAGE": "osdf://example.invalid/rift-test.sif",
                                  "SINGULARITY_BASE_EXE_DIR": str(BIN)})
    _assert_dag_built(r, rundir)
    marg = (rundir / "MARG_0.sub").read_text()
    assert str(inputs.plugin) in marg and str(inputs.ini) in marg      # transferred
    assert "--supplementary-waveform-transform-code lens_plug.py" in marg   # by basename
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
    r, rundir = _build(inputs, "shared", _seam(inputs, ini=False) + TREAT)
    _assert_dag_built(r, rundir)
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


def _dag_nodes(rundir):
    """{node: (submit text, VARS macros)} of the generated top-level DAG."""
    import re
    jobs, macros = {}, {}
    for line in (rundir / "marginalize_hyperparameters.dag").read_text().splitlines():
        f = line.split()
        if f and f[0] == "JOB":
            jobs[f[1]] = Path(f[2])
        elif f and f[0] == "VARS":
            macros[f[1]] = dict(re.findall(r'(\w+)="([^"]*)"', line))
    return {n: (p.read_text(), macros.get(n, {}), p.name) for n, p in jobs.items()}


def _arguments(submit, macros):
    import re
    args = re.search(r'^arguments\s*=\s*"?(.*?)"?\s*$', submit, re.M | re.I).group(1)
    for k, v in macros.items():
        args = args.replace("$({})".format(k), v)
    return args


def test_lambda_loop_dag_carries_lambda_through_cip_puff_ile(inputs):
    """The loop closes: DAG built; every CIP/PUFF/MARG node carries lambda as declared."""
    r, rundir = _build(inputs, "loop", _seam(inputs) + TREAT + [REFLECT, "lens_amplitude",
                                                                 "--internal-force-iterations", "3"])
    _assert_dag_built(r, rundir)
    cip_lines = [l for l in (rundir / "args_cip_list.txt").read_text().splitlines() if l.strip()]
    assert cip_lines
    for line in cip_lines:
        pairs = _pairs(line)
        assert pairs.count(("--parameter", "lens_amplitude")) == 1, line
        assert ("--integration-parameter-range", "lens_amplitude:[0.5,2]") in pairs, line
        assert ("--extra-column-pin", "lens_extra:1") in pairs, line
        assert ("--parameter", "lens_extra") not in pairs and "--extra-column-ignore" not in line
    puff = _pairs((rundir / "args_puff.txt").read_text())
    assert ("--parameter", "lens_amplitude") in puff
    assert ("--integration-parameter-range", "lens_amplitude:[0.5,2]") in puff
    assert ("--reflect-parameter", "lens_amplitude") in puff
    assert ("--parameter", "lens_extra") not in puff          # pinned: passed through

    nodes = _dag_nodes(rundir)
    by_sub = {}
    for name, (submit, macros, sub) in nodes.items():
        by_sub.setdefault(sub, []).append((name, submit, macros))
    post = [v for k, vs in by_sub.items() if k.startswith("EOS_POST") for v in vs]
    puffs = by_sub.get("PUFF.sub", [])
    margs = [v for k, vs in by_sub.items() if k.startswith("MARG") for v in vs]
    assert any(k == "EOS_POST_prior.sub" for k in by_sub) and len(post) >= 4, sorted(by_sub)
    assert puffs and len(margs) >= 3, sorted(by_sub)
    for name, submit, macros in post:      # every CIP iteration, and the prior integral
        a = _arguments(submit, macros)
        assert "--parameter lens_amplitude --integration-parameter-range lens_amplitude:[0.5,2]" in a, a
        assert "--extra-column-pin lens_extra:1" in a and "--extra-column-ignore" not in a, a
        assert "--fname=" in a and "all.marg_net" in a, a
    for name, submit, macros in puffs:
        a = _arguments(submit, macros).replace("'", "")
        assert "--parameter lens_amplitude --integration-parameter-range lens_amplitude:[0.5,2]" in a, a
        assert "--reflect-parameter lens_amplitude" in a and "--parameter lens_extra" not in a, a
        assert "--inj-file=" in a and "grid-" in a and a.count(".dat") >= 2 and ".xml" not in a, a
    for name, submit, macros in margs:     # every ILE: the seam on NoLoop, reading .dat grids
        a = _arguments(submit, macros).split()
        assert all(f in a for f in NOLOOP), a
        assert ("--supplementary-waveform-transform-code", str(inputs.plugin)) in list(zip(a, a[1:]))
        assert any(t.endswith(".dat") for t in a[a.index("--sim-grid") + 1:a.index("--sim-grid") + 2])
    # The grids passed between stages are Hyperpipe tables: the workflow exports the format.
    assert "ENV SET RIFT_HYPERPIPELINE_FORMAT=1" in (rundir / "marginalize_hyperparameters.dag").read_text()
    assert all("getenv" in submit.lower() for submit, _m, _s in nodes.values())


def test_hand_supplied_treatments_pass_the_guard(inputs):
    r, rundir = _build(inputs, "manual", _seam(inputs) + [
        "--manual-extra-cip-args", "--parameter lens_amplitude --integration-parameter-range "
        "lens_amplitude:[0.5,2] --extra-column-pin lens_extra:2",
        "--manual-extra-puff-args", "--parameter lens_amplitude --integration-parameter-range "
        "lens_amplitude:[0.5,2]"])
    _assert_dag_built(r, rundir)


@pytest.mark.parametrize("label,extra,message", [
    ("ignore", [RANGE, "lens_amplitude:[0.5,2]", "--manual-extra-cip-args", "--extra-column-ignore lens_extra"],
     "--extra-column-ignore lens_extra"),
    ("untreated", [RANGE, "lens_amplitude:[0.5,2]"], "no treatment for extra parameter lens_extra"),
    ("no-range", [PIN, "lens_extra:1", "--manual-extra-cip-args", "--parameter lens_amplitude"],
     "lens_amplitude is fit (--parameter) without --integration-parameter-range"),
    ("random", TREAT + ["--manual-extra-puff-args", "--random-parameter lens_amplitude"],
     "--random-parameter lens_amplitude"),
    ("puff-range", TREAT + ["--manual-extra-puff-args", "--integration-parameter-range lens_extra:[0,3] "
                            "--parameter lens_extra"], "lens_extra is pinned in CIP"),
    ("twice", TREAT + ["--manual-extra-cip-args", "--parameter lens_amplitude"], "given 2 times"),
])
def test_posterior_stage_refusals_before_the_dag(inputs, label, extra, message):
    r, rundir = _build(inputs, label, _seam(inputs) + extra)
    assert r.returncode != 0, r.stdout[-3000:]
    assert "refusing to build the DAG" in r.stdout and message in r.stdout, r.stdout[-3000:]
    assert not list(rundir.glob("*.dag"))


@pytest.mark.parametrize("label,extra,message", [
    ("eos", TREAT + ["--internal-tabular-eos-file", "/dev/null"], "--internal-tabular-eos-file"),
    ("out-of-range", [RANGE, "lens_amplitude:[1,2]", PIN, "lens_extra:1"],
     "lens_amplitude values [0.8] outside the declared range"),
    ("pin-unreached", [RANGE, "lens_amplitude:[0.5,2]", PIN, "lens_extra:3"], "no row has lens_extra = 3"),
    ("undeclared", TREAT + [RANGE, "mystery:[0,1]"], "mystery is not a declared extra parameter"),
    ("both", TREAT + [RANGE, "lens_extra:[0,3]"], "lens_extra is both ranged"),
    ("bad-range", [RANGE, "lens_amplitude:[2,1]"], "expected finite a < b"),
])
def test_treatment_option_refusals_before_any_helper_runs(inputs, label, extra, message):
    r, rundir = _build(inputs, label, _seam(inputs) + extra)
    assert r.returncode != 0, r.stdout[-3000:]
    assert "refusing" in r.stdout and message in r.stdout, r.stdout[-3000:]
    assert not rundir.exists()


def test_posterior_stage_guard_units():
    from RIFT.misc import waveform_transform_pipeline as wtp
    names = ["a", "b"]
    ok = "4 --parameter mc --parameter=a --integration-parameter-range a:[0,1] --extra-column-pin b:2"
    treat, bad = wtp.check_posterior_stage([ok, "Z" + ok[1:]], "X --parameter a "
                                           "--integration-parameter-range=a:[0,1]", names)
    assert bad == [] and treat == {"a": ("fit", (0.0, 1.0)), "b": ("pin", 2.0)}
    for value in ("nan", "inf", "-inf", "1e309"):
        with pytest.raises(ValueError, match="finite"):
            wtp.parse_pin("b:" + value, "--extra-column-pin")
        _, bad = wtp.check_posterior_stage([ok.replace("b:2", "b:" + value)], None, names)
        assert any("finite" in error for error in bad), value
    _, bad = wtp.check_posterior_stage([ok, ok.replace("[0,1]", "[0,2]")], None, names)
    assert any("on an earlier line" in b for b in bad)
    _, bad = wtp.check_posterior_stage([ok.replace("--parameter=a", "--parameter-implied a")], None, names)
    assert any("--parameter-implied" in b for b in bad)
    _, bad = wtp.check_posterior_stage([ok + " --report-best-point"], None, names)
    assert any("--report-best-point" in b for b in bad)
    _, bad = wtp.check_posterior_stage([ok], "X --parameter a --integration-parameter-range a:[0,3]", names)
    assert any("CIP's prior range" in b for b in bad)
    _, bad = wtp.check_posterior_stage([ok], "X --reflect-parameter a", names)
    assert any("--reflect-parameter a without --parameter a" in b for b in bad)
    _, bad = wtp.check_posterior_stage([ok], "X --puff-coordinate-system mu", names)
    assert any("--puff-coordinate-system" in b for b in bad)
    assert wtp.cip_injection({"a": (0.5, 2.0)}, {"b": 1.0}).split() == [
        "--parameter", "a", "--integration-parameter-range", "a:[0.5,2]", "--extra-column-pin", "b:1"]
    assert wtp.puff_injection({"a": (0.5, 2.0)}, ["a"]).split() == [
        "--parameter", "a", "--integration-parameter-range", "a:[0.5,2]", "--reflect-parameter", "a"]
