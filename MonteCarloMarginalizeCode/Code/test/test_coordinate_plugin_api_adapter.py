"""Coordinate-plugin API adapter: the ``rift`` and ``inplace`` (PR #434) shapes.

  * auto-classification by signature, the explicit PLUGIN_API attribute and the
    --supplementary-coordinate-api flag, and refusal of ambiguous plugins;
  * the inplace wrapper: rift call signature, no mutation of the caller's array,
    selection by name, and drivers that support only the rift API refuse it;
  * the fixed rotation demo (demo/hyperpipe/inplace_plugin/demo_conversion.py)
    through CEP's conversion and output-inversion path, and through PUFF.

Cost: unit tests are instant; six driver subprocesses, a few seconds each.
"""
import os
import subprocess
import sys
import types
import warnings

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
CEP = os.path.join(CODE, "bin", "util_ConstructEOSPosterior.py")
PUFF = os.path.join(CODE, "bin", "util_HyperparameterPuffball.py")
DEMO = os.path.join(CODE, "demo", "hyperpipe", "inplace_plugin", "demo_conversion.py")
LINEAR = os.path.join(CODE, "demo", "hyperpipe", "linear_coordinate_convert.py")
LINEAR_INI = os.path.join(CODE, "demo", "hyperpipe", "linear_coordinate_convert.ini")
sys.path.insert(0, CODE)
from RIFT.misc import coordinate_plugin as cp  # noqa: E402

ROOT2 = np.sqrt(2.0)


def _module(**attrs):
    mod = types.ModuleType("_fake_plugin")
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


def _inplace_fn(X, coord_names, **kw):
    return X


def _rift_fn(x_in, coord_names, low_level_coord_names, **kw):
    return x_in


# ------------------------------------------------------------------ classification

def test_existing_in_tree_plugins_stay_rift():
    for spec in ("rift_default", LINEAR):
        mod = cp._load_module_from_spec(spec)
        assert cp.classify_plugin_api(mod) == "rift"


def test_auto_rift_by_signature_and_by_hooks():
    assert cp.classify_plugin_api(_module(convert_coordinates=_rift_fn)) == "rift"
    # inplace-shaped entry point, but a rift-only hook pins it to the current contract
    for hook in ("prepare", "register_priors", "CHARTS", "INPUT_PARAMETERS"):
        mod = _module(rot=_inplace_fn, inverse_rot=_inplace_fn, **{hook: (lambda **kw: None)})
        assert cp.classify_plugin_api(mod, "rot") == "rift", hook


def test_auto_inplace():
    mod = _module(rot=_inplace_fn, inverse_rot=_inplace_fn, get_bounds=lambda p, b, **kw: b)
    assert cp.classify_plugin_api(mod, "rot") == "inplace"
    demo = cp._load_module_from_spec(DEMO)
    del demo.PLUGIN_API                       # the demo is inplace by signature alone, too
    assert cp.classify_plugin_api(demo, "demo_rotation") == "inplace"


def test_forward_only_inplace_shape_stays_rift_with_warning():
    with pytest.warns(RuntimeWarning, match="treated as a rift plugin"):
        assert cp.classify_plugin_api(_module(rot=_inplace_fn), "rot") == "rift"


def test_explicit_api():
    mod = _module(rot=_inplace_fn, inverse_rot=_inplace_fn)
    assert cp.classify_plugin_api(mod, "rot", api="rift") == "rift"
    assert cp.classify_plugin_api(_module(PLUGIN_API="inplace", convert_coordinates=_rift_fn)) == "inplace"
    assert cp.classify_plugin_api(_module(PLUGIN_API="rift", rot=_inplace_fn, inverse_rot=_inplace_fn),
                                  "rot", api="auto") == "rift"
    with pytest.raises(ValueError, match="PLUGIN_API='rift' in the plugin"):
        cp.classify_plugin_api(_module(PLUGIN_API="rift", rot=_inplace_fn), "rot", api="inplace")
    with pytest.raises(ValueError, match="is not one of"):
        cp.classify_plugin_api(_module(PLUGIN_API="same-basis", rot=_inplace_fn), "rot")


@pytest.mark.parametrize("mod,fn", [
    # default name: inverse_convert_coordinates is the rift inverse name too
    (_module(convert_coordinates=_inplace_fn, inverse_convert_coordinates=_inplace_fn), None),
    # inplace forward, rift-shaped inverse
    (_module(rot=_inplace_fn, inverse_rot=_rift_fn), "rot"),
    # fits neither call
    (_module(rot=lambda X, coord_names: X, inverse_rot=_inplace_fn), "rot"),
])
def test_ambiguous_refused(mod, fn):
    with pytest.raises(ValueError, match="cannot tell which plugin API"):
        cp.classify_plugin_api(mod, fn)


def test_classification_never_calls_the_plugin():
    def boom(X, coord_names, **kw):
        raise AssertionError("called during classification")
    assert cp.classify_plugin_api(_module(rot=boom, inverse_rot=boom), "rot") == "inplace"


# ------------------------------------------------------------------ loader / wrapper

def test_rift_driver_default_refuses_inplace():
    with pytest.raises(ValueError, match="does not support"):
        cp.load_coordinate_converter(DEMO, "demo_rotation")


def test_rift_plugin_unchanged_by_loader():
    conv, mod = cp.load_coordinate_converter(LINEAR, ini_path=LINEAR_INI, allowed_apis=("rift", "inplace"),
                                             coord_names=["u", "v", "w"],
                                             low_level_coord_names=["x", "y", "z"])
    assert conv.plugin_api == "rift"
    assert conv.inverse is mod.inverse_convert_coordinates


def test_inplace_wrapper_selects_by_name_and_copies():
    conv, _ = cp.load_coordinate_converter(DEMO, "demo_rotation", allowed_apis=("rift", "inplace"),
                                           external_range_args=["theta=0.3"])
    assert conv.plugin_api == "inplace"
    rng = np.random.default_rng(1)
    X = rng.normal(size=(50, 3))
    X_keep = X.copy()
    names = ["z", "x0", "x1"]
    Y = conv(X, coord_names=["x1", "x0"], low_level_coord_names=names)
    assert np.array_equal(X, X_keep)                     # caller's array untouched
    R = cp._load_module_from_spec(DEMO).rotate(0.3)      # theta reached the forward map
    expect = X[:, 1:3] @ R.T
    assert np.allclose(Y, expect[:, ::-1])
    back = conv.inverse(Y, coord_names=["x1", "x0"], low_level_coord_names=["x0", "x1"])
    assert np.array_equal(Y, conv(X, coord_names=["x1", "x0"], low_level_coord_names=names))
    assert np.allclose(back, X[:, 1:3])
    with pytest.raises(ValueError, match="cannot produce"):
        conv(X, coord_names=["u"], low_level_coord_names=names)


def test_explicit_inplace_needs_inverse():
    path_mod = _module(rot=_inplace_fn)
    sys.modules["_rift_test_fwd_only"] = path_mod
    try:
        with pytest.raises(AttributeError, match="inverse_rot"):
            cp.load_coordinate_converter("_rift_test_fwd_only", "rot", api="inplace",
                                         allowed_apis=("rift", "inplace"))
    finally:
        del sys.modules["_rift_test_fwd_only"]


@pytest.mark.parametrize("theta", [np.pi / 4, 0.3])
def test_demo_get_bounds_is_the_rotated_box(theta):
    demo = cp._load_module_from_spec(DEMO)
    file_box = {"x0": [1.0, 4.0], "x1": [-2.0, 0.5], "z": [0, 1]}
    out = demo.get_bounds(["x0", "x1", "z"], file_box, theta=theta)
    g = np.stack(np.meshgrid(np.linspace(1, 4, 41), np.linspace(-2, 0.5, 41)), -1).reshape(-1, 2)
    r = g @ demo.rotate(theta).T
    for k, name in enumerate(["x0", "x1"]):
        assert np.isclose(out[name][0], r[:, k].min()) and np.isclose(out[name][1], r[:, k].max())
    assert out["z"] == [0.0, 1.0]
    with pytest.raises(ValueError, match="needs file-basis ranges"):
        demo.get_bounds(["x0"], {"x0": [0, 1]})


# ------------------------------------------------------------------ drivers

def _run(tmp, driver, args):
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.update(OMP_NUM_THREADS="1", MPLBACKEND="Agg",
               XDG_CACHE_HOME=os.path.join(tmp, "cache"), MPLCONFIGDIR=os.path.join(tmp, "mpl"))
    return subprocess.run([sys.executable, driver] + list(args), cwd=tmp, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          universal_newlines=True, timeout=900)


def _cep_grid(tmp, theta):
    """lnL is a narrow Gaussian in the ROTATED frame, peaked at r = (c, 0)."""
    rng = np.random.default_rng(7)
    n = 600
    R = cp._load_module_from_spec(DEMO).rotate(theta)
    c = 3 * ROOT2
    centre = R.T @ np.array([c, 0.0])                    # the peak in the file basis
    x = centre + rng.uniform(-1.5, 1.5, (n, 2))
    r = x @ R.T
    lnL = -0.5 * ((r[:, 0] - c) / 0.25) ** 2 - 0.5 * (r[:, 1] / 0.6) ** 2 + 10
    path = os.path.join(tmp, "cep_grid.dat")
    np.savetxt(path, np.column_stack([lnL, np.zeros(n), x, rng.uniform(0, 1, n)]),
               header="lnL sigma_lnL x0 x1 z")
    return path, centre


def _cep(tmp, grid, extra):
    return _run(tmp, CEP, ["--fname", grid, "--parameter", "x0", "--parameter", "x1",
                           "--supplementary-coordinate-code", DEMO,
                           "--supplementary-coordinate-function", "demo_rotation",
                           "--fit-method", "rf", "--n-max", "40000", "--n-step", "4000",
                           "--n-eff", "200", "--n-output-samples", "2000",
                           "--no-plots", "--ignore-errors-in-data"] + extra)


@pytest.mark.parametrize("theta,extra", [
    (np.pi / 4, ["--get-range-from-external"]),
    (0.3, ["--get-range-from-external", "--external-range-args", "theta=0.3",
           "--supplementary-coordinate-api", "inplace"]),
    (np.pi / 4, []),                                 # ranges from the transformed grid
])
def test_cep_inplace_round_trip(tmp_path, theta, extra):
    tmp = str(tmp_path)
    grid, centre = _cep_grid(tmp, theta)
    proc = _cep(tmp, grid, extra)
    assert proc.returncode == 0, proc.stdout[-3000:]
    assert "api=inplace" in proc.stdout
    out = np.genfromtxt(os.path.join(tmp, "output-EOS-samples.dat"), names=True)
    # Output is in the FILE basis: without the inverse the mean would sit at (3 sqrt2, 0).
    assert abs(np.mean(out["x0"]) - centre[0]) < 0.15, (np.mean(out["x0"]), centre)
    assert abs(np.mean(out["x1"]) - centre[1]) < 0.15, (np.mean(out["x1"]), centre)
    r = np.column_stack([out["x0"], out["x1"]]) @ cp._load_module_from_spec(DEMO).rotate(theta).T
    assert abs(np.mean(r[:, 0]) - 3 * ROOT2) < 0.1 and np.std(r[:, 0]) < 0.4


def test_cep_inplace_cli_range_without_get_bounds_refused(tmp_path):
    tmp = str(tmp_path)
    grid, _ = _cep_grid(tmp, np.pi / 4)
    proc = _cep(tmp, grid, ["--integration-parameter-range", "x0:[0,6]"])
    assert proc.returncode != 0
    assert "frame of those ranges is ambiguous" in proc.stdout


def test_puff_inplace_round_trip(tmp_path):
    tmp = str(tmp_path)
    rng = np.random.default_rng(3)
    n = 3000
    x = np.column_stack([rng.uniform(1, 4, n), rng.uniform(-2, 0.5, n)])
    grid = os.path.join(tmp, "grid.dat")
    np.savetxt(grid, np.column_stack([np.zeros(n), np.zeros(n), x]), header="lnL sigma_lnL x0 x1")
    proc = _run(tmp, PUFF, ["--inj-file", grid, "--inj-file-out", "out.dat",
                            "--parameter", "x0", "--parameter", "x1", "--puff-factor", "1",
                            "--parameter-range", "x0:[1,4]", "--parameter-range", "x1:[-2,0.5]",
                            "--supplementary-coordinate-code", DEMO,
                            "--supplementary-coordinate-function", "demo_rotation",
                            "--get-range-from-external"])
    assert proc.returncode == 0, proc.stdout[-3000:]
    out = np.genfromtxt(os.path.join(tmp, "out.dat"), names=True)
    assert 100 < len(out) < n
    demo = cp._load_module_from_spec(DEMO)
    box = demo.get_bounds(["x0", "x1"], {"x0": [1, 4], "x1": [-2, 0.5]})
    r = np.column_stack([out["x0"], out["x1"]]) @ demo.rotate(np.pi / 4).T
    for k, name in enumerate(["x0", "x1"]):
        assert r[:, k].min() >= box[name][0] - 1e-9 and r[:, k].max() <= box[name][1] + 1e-9
    # without --get-range-from-external the same ranges are refused
    proc = _run(tmp, PUFF, ["--inj-file", grid, "--inj-file-out", "out2.dat",
                            "--parameter", "x0", "--parameter", "x1",
                            "--parameter-range", "x0:[1,4]",
                            "--supplementary-coordinate-code", DEMO,
                            "--supplementary-coordinate-function", "demo_rotation"])
    assert proc.returncode != 0 and "ambiguous" in proc.stdout
