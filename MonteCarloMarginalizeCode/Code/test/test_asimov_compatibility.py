#!/usr/bin/env python3
"""Cross-version contract tests for the RIFT ASIMOV adapter."""

import os
import types

import pytest

pytest.importorskip("asimov")
rift_asimov = pytest.importorskip("RIFT.asimov.rift")

Rift = rift_asimov.Rift
PipelineException = rift_asimov.PipelineException


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


def _pipe(production):
    pipe = Rift.__new__(Rift)
    pipe.production = production
    pipe.category = production.category
    pipe.logger = _Logger()
    return pipe


def test_before_config_calls_asimov_base_hook_first(monkeypatch):
    class HookCalled(Exception):
        pass

    def base_before_config(self, dryrun=False):
        assert dryrun is True
        raise HookCalled

    monkeypatch.setattr(rift_asimov.Pipeline, "before_config", base_before_config)
    production = types.SimpleNamespace(category="C01_offline")

    with pytest.raises(HookCalled):
        _pipe(production).before_config(dryrun=True)


def test_collect_logs_delegates_to_asimov_08(monkeypatch):
    production = types.SimpleNamespace(category="C01_offline", rundir="/unused")
    expected = {"rift.err": "bounded by ASIMOV"}
    monkeypatch.setattr(
        rift_asimov.Pipeline, "log_patterns", ["*.out"], raising=False
    )
    monkeypatch.setattr(
        rift_asimov.Pipeline, "collect_logs", lambda self: expected
    )

    assert _pipe(production).collect_logs() is expected


def test_collect_logs_legacy_fallback_is_bounded(monkeypatch, tmp_path):
    monkeypatch.delattr(rift_asimov.Pipeline, "log_patterns", raising=False)
    log = tmp_path / "rift.log"
    log.write_bytes(b"x" * (Rift._MAX_LEGACY_LOG_BYTES + 100) + b"tail")
    production = types.SimpleNamespace(
        category="C01_offline", rundir=str(tmp_path)
    )

    messages = _pipe(production).collect_logs()

    assert set(messages) == {"rift.log"}
    assert len(messages["rift.log"].encode()) == Rift._MAX_LEGACY_LOG_BYTES
    assert messages["rift.log"].endswith("tail")


def test_collect_logs_legacy_fallback_skips_non_files(monkeypatch, tmp_path):
    monkeypatch.delattr(rift_asimov.Pipeline, "log_patterns", raising=False)
    (tmp_path / "notes.log").mkdir()
    (tmp_path / "rift.err").write_text("boom")
    production = types.SimpleNamespace(
        category="C01_offline", rundir=str(tmp_path)
    )

    messages = _pipe(production).collect_logs()

    assert set(messages) == {"rift.err"}
    assert messages["rift.err"] == "boom"


def test_asimov_07_completion_defers_to_separate_postprocessing(monkeypatch):
    production = types.SimpleNamespace(
        status="processing", category="C01_offline", meta={"job id": 12}
    )
    pipe = _pipe(production)
    monkeypatch.setattr(rift_asimov, "PESummaryPipeline", None)

    pipe.after_completion()

    assert production.status == "finished"


def test_legacy_completion_submits_pesummary_once(monkeypatch):
    calls = []

    class _LegacyPESummary:
        def __init__(self, production, category=None):
            calls.append((production, category))

        def submit_dag(self):
            return 314

    production = types.SimpleNamespace(
        status="running", category="C01_offline", meta={}
    )
    pipe = _pipe(production)
    monkeypatch.setattr(rift_asimov, "PESummaryPipeline", _LegacyPESummary)

    pipe.after_completion()

    assert calls == [(production, "C01_offline")]
    assert production.meta["job id"] == 314
    assert production.status == "processing"


def test_collect_assets_publishes_pesummary_inputs(tmp_path):
    rundir = tmp_path / "run"
    rundir.mkdir()
    samples = rundir / "extrinsic_posterior_samples.dat"
    samples.write_text("# samples\n")
    config = tmp_path / "repository" / "C01_offline" / "rift.ini"
    config.parent.mkdir(parents=True)
    config.write_text("[analysis]\n")
    psd = tmp_path / "H1-psd.dat"
    psd.write_text("20 1e-46\n")
    calibration = tmp_path / "H1-calibration.dat"
    calibration.write_text("20 0 0\n")

    repository = types.SimpleNamespace(directory=str(tmp_path / "repository"))
    event = types.SimpleNamespace(name="S250202cu", repository=repository)
    production = types.SimpleNamespace(
        name="rift-SEOBNRv5PHM",
        category="C01_offline",
        rundir=str(rundir),
        event=event,
        psds={"H1": str(psd)},
        xml_psds={},
        meta={"data": {"calibration": {"H1": str(calibration)}}},
        get_configuration=lambda: types.SimpleNamespace(ini_loc="rift.ini"),
    )

    assets = _pipe(production).collect_assets(absolute=True)

    assert assets["asset_contract"] == "rift-assets/v1"
    assert assets["samples"] == [str(samples)]
    assert assets["config"] == str(config)
    assert assets["psds"] == {"H1": str(psd)}
    assert assets["calibration"] == {"H1": str(calibration)}
    assert assets["provenance"] == {
        "pipeline": "rift",
        "event": "S250202cu",
        "analysis": "rift-SEOBNRv5PHM",
    }


def test_reweighted_samples_keep_list_contract(tmp_path):
    rundir = tmp_path / "run"
    rundir.mkdir()
    reweighted = rundir / "reweighted_posterior_samples.dat"
    reweighted.write_text("# samples\n")
    event = types.SimpleNamespace(
        name="S250202cu", repository=types.SimpleNamespace(directory=str(tmp_path))
    )
    production = types.SimpleNamespace(
        name="rift-calmarg",
        category="C01_offline",
        rundir=str(rundir),
        event=event,
        psds={},
        meta={"data": {}},
        get_configuration=lambda: (_ for _ in ()).throw(ValueError()),
    )

    assets = _pipe(production).collect_assets(absolute=True)

    assert assets["samples"] == [str(reweighted)]
    assert assets["samples_calmarg"] == str(reweighted)
    assert "asset_contract" not in assets


def test_collect_assets_resolves_relative_detector_paths_from_repository(
        tmp_path, monkeypatch):
    repository_dir = tmp_path / "repository"
    run = tmp_path / "run"
    run.mkdir()
    (run / "extrinsic_posterior_samples.dat").write_text("# samples\n")
    config = repository_dir / "C01_offline" / "rift.ini"
    config.parent.mkdir(parents=True)
    config.write_text("[analysis]\n")
    psd = repository_dir / "assets" / "H1-psd.dat"
    calibration = repository_dir / "assets" / "H1-calibration.dat"
    psd.parent.mkdir()
    psd.write_text("20 1e-46\n")
    calibration.write_text("20 0 0\n")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    event = types.SimpleNamespace(
        name="S250202cu",
        repository=types.SimpleNamespace(directory=str(repository_dir)),
    )
    production = types.SimpleNamespace(
        name="rift-relative",
        category="C01_offline",
        rundir=str(run),
        event=event,
        psds={"H1": "assets/H1-psd.dat"},
        xml_psds={},
        meta={"data": {"calibration": {
            "H1": "assets/H1-calibration.dat"}}},
        get_configuration=lambda: types.SimpleNamespace(ini_loc="rift.ini"),
    )

    assets = _pipe(production).collect_assets(absolute=True)

    assert assets["psds"] == {"H1": str(psd)}
    assert assets["calibration"] == {"H1": str(calibration)}
    assert assets["asset_contract"] == "rift-assets/v1"


def test_collect_assets_distinguishes_standard_calmarg_and_all_net(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    standard = run / "extrinsic_posterior_samples.dat"
    calmarg = run / "reweighted_posterior_samples.dat"
    all_net = run / "all.net"
    standard.write_text("# standard\n")
    calmarg.write_text("# calmarg\n")
    all_net.write_text("# likelihood\n")
    repository = tmp_path / "repository"
    config = repository / "C01_offline" / "rift.ini"
    config.parent.mkdir(parents=True)
    config.write_text("[analysis]\n")
    event = types.SimpleNamespace(
        name="S250202cu",
        repository=types.SimpleNamespace(directory=str(repository)),
    )
    production = types.SimpleNamespace(
        name="rift-both", category="C01_offline", rundir=str(run),
        event=event, psds={}, xml_psds={}, meta={"data": {}},
        get_configuration=lambda: types.SimpleNamespace(ini_loc="rift.ini"),
    )

    assets = _pipe(production).collect_assets(absolute=True)

    assert assets["samples"] == [str(calmarg)]
    assert assets["samples_raw"] == str(standard)
    assert assets["samples_calmarg"] == str(calmarg)
    assert assets["lnL_marg"] == str(all_net)
    assert assets["asset_contract"] == "rift-assets/v1"


def test_asimov_07_psd_attributes_replace_legacy_getter():
    production = types.SimpleNamespace(
        category="C01_offline",
        psds={"H1": "/tmp/H1.dat"},
        xml_psds={"H1": "/tmp/H1.xml.gz"},
    )
    pipe = _pipe(production)

    assert pipe._get_psds("ascii") == production.psds
    assert pipe._get_psds("xml") == ["/tmp/H1.xml.gz"]


def test_single_sample_list_is_unwrapped_for_bootstrap(monkeypatch):
    dependency = types.SimpleNamespace(
        name="pesummary",
        pipeline=types.SimpleNamespace(
            collect_assets=lambda: {"samples": ["combined.h5"]}
        ),
    )
    event = types.SimpleNamespace(productions=[dependency])
    production = types.SimpleNamespace(
        name="rift-bootstrap",
        category="C01_offline",
        dependencies=["pesummary"],
        event=event,
        meta={"scheduler": {}},
    )
    pipe = _pipe(production)
    monkeypatch.setattr(pipe, "_dataset_label", lambda path: "rift-source")

    assert pipe._find_posterior() == "combined.h5"
    assert production.meta["dataset"] == "rift-source"


def test_multiple_sample_files_are_rejected_for_bootstrap():
    dependency = types.SimpleNamespace(
        name="pesummary",
        pipeline=types.SimpleNamespace(
            collect_assets=lambda: {"samples": ["a.h5", "b.h5"]}
        ),
    )
    event = types.SimpleNamespace(productions=[dependency])
    production = types.SimpleNamespace(
        name="rift-bootstrap",
        category="C01_offline",
        dependencies=["pesummary"],
        event=event,
        meta={"scheduler": {}},
    )

    with pytest.raises(PipelineException, match="exactly one PESummary metafile"):
        _pipe(production)._find_posterior()


def test_existing_bootstrap_requires_explicit_unprovenanced_reuse(tmp_path):
    bootstrap = tmp_path / "bootstrap.xml.gz"
    bootstrap.write_text("old grid")
    production = types.SimpleNamespace(
        name="rift-bootstrap", category="C01_offline",
        meta={"scheduler": {}},
    )
    pipe = _pipe(production)

    with pytest.raises(PipelineException, match="bootstrap reuse existing"):
        pipe._reuse_existing_bootstrap(str(bootstrap), "new-posterior.h5")

    production.meta["scheduler"]["bootstrap reuse existing"] = True
    assert pipe._reuse_existing_bootstrap(
        str(bootstrap), "new-posterior.h5") is True


@pytest.mark.parametrize("builder,suffix", [
    ("Hyperpipe", "_bootstrap.dat"),
    ("hyperpipe", "_bootstrap.dat"),
    ("BasicIteration", "_bootstrap.xml.gz"),
    (None, "_bootstrap.xml.gz"),
])
def test_bootstrap_grid_path_follows_selected_builder(tmp_path, builder, suffix):
    pipeline = {} if builder is None else {"pipeline-builder": builder}
    production = types.SimpleNamespace(
        name="rift-bootstrap", category="C01_offline",
        event=types.SimpleNamespace(
            repository=types.SimpleNamespace(directory=str(tmp_path))),
        meta={"scheduler": {"pipeline": pipeline}},
    )

    path = _pipe(production)._bootstrap_grid_path()

    assert path.endswith(os.path.join("C01_offline", "rift-bootstrap" + suffix))


@pytest.mark.parametrize("builder,expected", [
    ("Hyperpipe", "marginalize_hyperparameters.dag"),
    ("hyperpipe", "marginalize_hyperparameters.dag"),
    ("BasicIteration",
     "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag"),
    (None, "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag"),
])
def test_dag_filename_follows_selected_builder(builder, expected):
    pipeline = {} if builder is None else {"pipeline-builder": builder}
    production = types.SimpleNamespace(
        name="rift-dag", category="C01_offline",
        meta={"scheduler": {"pipeline": pipeline}},
    )

    assert _pipe(production)._dag_filename() == expected


@pytest.mark.parametrize("builder,expected", [
    ("Hyperpipe", ["--manual-initial-grid", "/tmp/bootstrap.dat"]),
    ("BasicIteration", ["--manual-initial-grid", "/tmp/bootstrap.xml.gz",
                        "--manual-initial-grid-supplements"]),
])
def test_manual_grid_arguments_omit_xml_supplements_for_hyperpipe(
        builder, expected):
    production = types.SimpleNamespace(
        name="rift-bootstrap", category="C01_offline",
        meta={"scheduler": {"pipeline": {"pipeline-builder": builder}}},
    )

    assert _pipe(production)._manual_grid_arguments(expected[1]) == expected


def test_hyperpipe_bootstrap_writer_emits_named_grid(tmp_path, monkeypatch):
    class _Point:
        m1 = 10.0
        m2 = 8.0
        s1x = 0.1
        s1y = 0.0
        s1z = 0.2
        s2x = 0.0
        s2y = 0.0
        s2z = -0.1
        eccentricity = 0.01
        meanPerAno = 0.3
        lambda1 = 0.0
        lambda2 = 0.0

    import RIFT.lalsimutils as lalsimutils
    from RIFT.misc import hyperpipeline_io

    monkeypatch.setattr(
        lalsimutils, "xml_to_ChooseWaveformParams_array",
        lambda path: [_Point()])
    calls = []

    def capture(path, points, columns, **kwargs):
        calls.append((path, points, columns, kwargs))

    monkeypatch.setattr(hyperpipeline_io, "write_grid_from_P_list", capture)
    target = tmp_path / "bootstrap.dat"
    production = types.SimpleNamespace(
        name="rift-bootstrap", category="C01_offline",
        meta={
            "scheduler": {"pipeline": {"pipeline-builder": "Hyperpipe"}},
            "waveform": {
                "approximant": "IMRPhenomD", "pn amplitude order": -1,
                "reference frequency": 20.0,
            },
            "likelihood": {"start frequency": 19.68},
        },
    )

    _pipe(production)._write_hyperpipe_bootstrap(
        "bootstrap.xml.gz", str(target))

    assert len(calls) == 1
    assert calls[0][0] == str(target)
    assert calls[0][2][:2] == ("lnL", "sigma_lnL")
    assert "eccentricity" in calls[0][2]
    assert "meanPerAno" in calls[0][2]
    assert "lambda1" not in calls[0][2]
    point = calls[0][1][0]
    assert point.ampO == -1
    assert point.fmin == 19.68
    assert point.fref == 20.0
    assert calls[0][3]["metadata_overrides"] == {
        "approx": "IMRPhenomD"}


if __name__ == "__main__":
    raise SystemExit(pytest.main([os.path.abspath(__file__), "-v"]))
