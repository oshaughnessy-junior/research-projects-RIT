import pytest

from RIFT.calmarg.calibration import correction_type_for_ifo


@pytest.mark.parametrize(
    "ifo_name, expected",
    [("H1", "data"), ("L1", "data"), ("K1", "data"), ("V1", "template")],
)
def test_bilby_pipe_default_correction_types(ifo_name, expected):
    assert correction_type_for_ifo(None, ifo_name) == expected


@pytest.mark.parametrize("setting", ["data", "template"])
def test_global_correction_type(setting):
    assert correction_type_for_ifo(setting, "V1") == setting


def test_detector_specific_correction_types():
    setting = {"H1": "template", "V1": "data"}
    assert correction_type_for_ifo(setting, "H1") == "template"
    assert correction_type_for_ifo(setting, "V1") == "data"


def test_string_detector_specific_correction_types():
    def parse_dict(value):
        assert value == "{H1: data, V1: template}"
        return {"H1": "data", "V1": "template"}

    assert correction_type_for_ifo(
        "{H1: data, V1: template}", "V1", parse_dict=parse_dict
    ) == "template"


def test_missing_detector_is_rejected():
    with pytest.raises(ValueError, match="No calibration correction type"):
        correction_type_for_ifo({"H1": "data"}, "V1")


@pytest.mark.parametrize("setting", ["H1:data,V1:template", "{H1: data, V1: template}",
                                     "{'H1': 'data', 'V1': 'template'}"])
def test_compact_instrument_conventions(setting):
    assert correction_type_for_ifo(setting, "H1") == "data"
    assert correction_type_for_ifo(setting, "V1") == "template"


def _envelope(path, amplitude=1.1, phase=0.2, varying=False):
    import numpy as np
    f = np.geomspace(20, 128, 12)
    amp = np.full(f.shape, amplitude)
    ph = np.full(f.shape, phase)
    if varying:
        amp += .03 * np.log(f / 20)
        ph += .02 * np.log(f / 20)
    table = np.column_stack([f, amp, ph, amp-.02, ph-.01, amp+.03, ph+.015])
    np.savetxt(path, table)
    return table




def test_mixed_conventions_and_overrides_reach_draws(tmp_path):
    import numpy as np
    from RIFT.calmarg import generate_realizations as gr
    for ifo in ("H1", "V1"):
        _envelope(tmp_path / f"{ifo}.txt")
    kw = dict(env_dir=tmp_path, dets=["H1", "V1"], T_segment=1, dT=1/256,
              fmin=20, fmax=120, n_spline_points=4, n_realizations=3)
    default = gr.draw_prior_realizations_with_nodes(**kw, rng=np.random.default_rng(8))
    override = gr.draw_prior_realizations_with_nodes(
        **kw, correction_types={"H1": "template", "V1": "data"}, rng=np.random.default_rng(8))
    # These deterministic priors expose the inverse response even when phase is marginalized.
    assert default["prior_mean"][0] == pytest.approx(1/1.1)
    assert default["prior_mean"][4] == pytest.approx(-.2)
    assert default["prior_mean"][8] == pytest.approx(1.1)
    assert default["prior_mean"][12] == pytest.approx(.2)
    assert override["prior_mean"][0] == pytest.approx(1.1)
    assert override["prior_mean"][8] == pytest.approx(1/1.1)
    assert not np.allclose(default["realizations"]["H1"], override["realizations"]["H1"])


def test_saved_proposal_uses_live_convention_prior(tmp_path):
    import numpy as np
    from RIFT.calmarg import generate_realizations as gr, adaptive
    _envelope(tmp_path / "H1.txt")
    # A saved proposal may have been learned with the old response convention.
    old = gr.prior_cal_breadcrumb_dict(tmp_path, ["H1"], 20, 120, 4,
                                       correction_types="template")
    live = gr.prior_cal_breadcrumb_dict(tmp_path, ["H1"], 20, 120, 4,
                                        correction_types="data")
    _, log_w, nodes = gr.seed_realizations_from_breadcrumb(
        old, 1, 1/256, 20, 120, 4, 5, rng=np.random.default_rng(3), prior=live)
    expected = (adaptive.log_prior(nodes, live["prior_mean"], live["prior_sigma"])
                - adaptive._mvn_logpdf(nodes, old["proposal_mean"], old["proposal_cov"]))
    np.testing.assert_allclose(log_w, expected)
    assert not np.allclose(log_w, 0)


@pytest.mark.parametrize("convention", ["data", "template"])
def test_real_cross_term_and_self_term_match_distorted_template(tmp_path, convention):
    """Exercise production overlaps and norm construction with complex non-unity response."""
    import numpy as np
    import lal
    from RIFT.calmarg import generate_realizations as gr
    from RIFT.likelihood import factored_likelihood as fl
    path = tmp_path / "envelope.txt"
    table = _envelope(path)
    # Zero-width prior gives a deterministic response independent of RNG draws.
    table[:, 3] = table[:, 5] = table[:, 1]
    table[:, 4] = table[:, 6] = table[:, 2]
    np.savetxt(path, table)
    alpha = gr.create_realizations(path, 1, 1/256, 20, 120, 4, 1,
                                    correction_type=convention)
    expected = 1/(1.1*np.exp(.2j)) if convention == "data" else 1.1*np.exp(.2j)
    assert alpha[64, 0] == pytest.approx(expected)
    rng = np.random.default_rng(9)
    def series(values):
        fs = lal.CreateCOMPLEX16FrequencySeries("test", lal.LIGOTimeGPS(1000),
                                               0, 1, lal.DimensionlessUnit, 256)
        fs.data.data[:] = values
        return fs
    h = series(rng.normal(size=256) + 1j*rng.normal(size=256))
    d = series(rng.normal(size=256) + 1j*rng.normal(size=256))
    ah = series(alpha[:, 0] * h.data.data)
    psd = lambda f: 1.0
    actual = fl.ComputeModeIPTimeSeries({(2,2):h}, d, psd, 20, 120, 128, 0, 32,
                                        analyticPSD_Q=True, calibration_realizations=alpha)
    ref = fl.ComputeModeIPTimeSeries({(2,2):ah}, d, psd, 20, 120, 128, 0, 32,
                                     analyticPSD_Q=True)
    np.testing.assert_allclose(actual[(2,2)].data.data, ref[(2,2)].data.data,
                               rtol=1e-12, atol=1e-10)
    actual_u = fl.ComputeModeCrossTermIPCal({(2,2):h}, {(2,2):h}, psd, 20, 120, 128,
                                            1, alpha, analyticPSD_Q=True)
    ref_u = fl.ComputeModeCrossTermIP({(2,2):ah}, {(2,2):ah}, psd, 20, 120, 128,
                                      1, analyticPSD_Q=True)
    assert actual_u[0][((2,2),(2,2))] == pytest.approx(ref_u[((2,2),(2,2))], rel=1e-12)


def _driver_parts():
    import ast
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "bin/integrate_likelihood_extrinsic_batchmode"
    return ast, path, ast.parse(path.read_text())


@pytest.mark.parametrize("export", [False, True])
def test_driver_prior_and_growth_propagate_conventions(tmp_path, export):
    import numpy as np
    from types import SimpleNamespace
    from RIFT.calmarg import generate_realizations as gr
    ast, path, tree = _driver_parts()
    for ifo in ["H1", "V1"]:
        table = _envelope(tmp_path / f"{ifo}.txt")
        table[:, 3] = table[:, 5] = table[:, 1]
        table[:, 4] = table[:, 6] = table[:, 2]
        np.savetxt(tmp_path / f"{ifo}.txt", table)
    ns = dict(np=np, opts=SimpleNamespace(calibration_export_posterior=export,
              calibration_proposal_breadcrumb=None, calibration_envelope_directory=str(tmp_path),
              fmin_template=20, calibration_spline_count=4, calibration_n_realizations=2, seed=8),
              P=SimpleNamespace(deltaF=1, deltaT=1/256), fmax=120,
              cal_fmin_ifo={"H1":20, "V1":20},
              calibration_correction_types={"H1":"template", "V1":"data"},
              calibration_realization_dict={}, calibration_nodes=None,
              calibration_log_weights=None, _cal_rng=lambda _: np.random.default_rng(13))
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)
             and n.name in ("_cal_setup_prior_with_nodes", "_draw_more_calibration_draws")]
    exec(compile(ast.Module(body=funcs, type_ignores=[]), str(path), "exec"), ns)
    ns["_cal_setup_prior_with_nodes"]({"H1":None, "V1":None})
    new = ns["_draw_more_calibration_draws"](3, {"H1":None, "V1":None})
    for ifo, expected in [("H1", 1.1*np.exp(.2j)), ("V1", 1/(1.1*np.exp(.2j)))]:
        assert ns["calibration_realization_dict"][ifo].shape == (256, 5)
        np.testing.assert_allclose(ns["calibration_realization_dict"][ifo][64], expected)
        np.testing.assert_allclose(new[ifo][64], expected)
    # Exercise the driver's actual explicit override, not just the library default.
    site = next(n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name)
                        and t.value.id == "extra_kwargs" and isinstance(t.slice, ast.Constant)
                        and t.slice.value == "calibration_conjugate" for t in n.targets))
    ns["extra_kwargs"] = {}
    exec(compile(ast.Module(body=[site], type_ignores=[]), str(path), "exec"), ns)
    assert ns["extra_kwargs"]["calibration_conjugate"] is True


@pytest.mark.parametrize("setting", [None, "H1:template,V1:data"])
def test_pipeline_emitter_and_ile_parser_preserve_convention(setting):
    import ast
    import os
    import shlex
    from pathlib import Path
    from types import SimpleNamespace
    from optparse import OptionParser
    ast, driver, tree = _driver_parts()
    path = driver.parent / "util_RIFT_pseudo_pipe.py"
    pipe_tree = ast.parse(path.read_text())
    block = next(n for n in pipe_tree.body if isinstance(n, ast.If)
                 and "cal_types_arg" in ast.unparse(n))
    class Options(SimpleNamespace):
        def __getattr__(self, name):
            return False
    ns = dict(os=os, line="", event_dict={"IFOs":["H1", "V1"]},
              opts=Options(calmarg_envelope_directory="/unused/envelopes", use_osg_file_transfer=False,
                           calmarg_correction_type=setting, calmarg_n_realizations=3, calmarg_spline_count=4))
    exec(compile(ast.Module(body=[block], type_ignores=[]), str(path), "exec"), ns)
    declarations = [n for n in tree.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
                    and isinstance(n.value.func, ast.Attribute) and n.value.func.attr == "add_option"
                    and any(isinstance(a, ast.Constant) and a.value in
                            ("--calibration-correction-type", "--calibration-envelope-directory",
                             "--calibration-n-realizations", "--calibration-spline-count")
                            for a in n.value.args)]
    env = dict(optp=OptionParser())
    exec(compile(ast.Module(body=declarations, type_ignores=[]), str(driver), "exec"), env)
    opts, _ = env["optp"].parse_args(shlex.split(ns["line"]))
    assert correction_type_for_ifo(opts.calibration_correction_type, "H1") == ("data" if setting is None else "template")
    assert correction_type_for_ifo(opts.calibration_correction_type, "V1") == ("template" if setting is None else "data")


def test_driver_seeded_growth_replaces_saved_prior(tmp_path):
    import numpy as np
    from types import SimpleNamespace
    from RIFT.calmarg import generate_realizations as gr, breadcrumbs, adaptive
    ast, driver, tree = _driver_parts()
    _envelope(tmp_path / "H1.txt")
    old = gr.prior_cal_breadcrumb_dict(tmp_path, ["H1"], 20, 120, 4, correction_types="template")
    live = gr.prior_cal_breadcrumb_dict(tmp_path, ["H1"], 20, 120, 4, correction_types="data")
    bc = tmp_path / "old.npz"
    breadcrumbs.save(bc, cal=old)
    ns = dict(np=np, opts=SimpleNamespace(calibration_proposal_breadcrumb=str(bc),
              fmin_template=20, calibration_spline_count=4),
              P=SimpleNamespace(deltaF=1, deltaT=1/256), fmax=120,
              cal_fmin_ifo={"H1":20}, calibration_current_prior=live,
              calibration_realization_dict={"H1":np.ones((256, 1), dtype=complex)},
              calibration_nodes=np.zeros((1,8)), calibration_log_weights=np.zeros(1),
              _cal_rng=lambda _:np.random.default_rng(13))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_draw_more_calibration_draws")
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(driver), "exec"), ns)
    ns["_draw_more_calibration_draws"](3, {"H1":None})
    nodes = ns["calibration_nodes"][1:]
    expected = (adaptive.log_prior(nodes, live["prior_mean"], live["prior_sigma"])
                - adaptive._mvn_logpdf(nodes, old["proposal_mean"], old["proposal_cov"]))
    np.testing.assert_allclose(ns["calibration_log_weights"][1:], expected)
    assert not np.allclose(expected, 0)
