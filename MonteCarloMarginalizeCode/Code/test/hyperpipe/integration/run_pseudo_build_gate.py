#!/usr/bin/env python3
"""Build and semantically compare minimal BasicIteration/Hyperpipe pipelines.

This is intentionally separate from the fast Hyperpipe pytest suite.  It uses
the installed RIFT science stack to generate equivalent XML and Hyperpipe ASCII
seed grids, then drives the real ``util_RIFT_pseudo_pipe.py`` entry point twice.
No DAG is submitted and no likelihood is evaluated.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Optional

import lal

from RIFT import lalsimutils
from RIFT.misc import hyperpipeline_io


HERE = Path(__file__).resolve()
RIFT_ROOT = HERE.parents[5]
CODE = RIFT_ROOT / "MonteCarloMarginalizeCode" / "Code"
BIN = CODE / "bin"
PSEUDO_PIPE = BIN / "util_RIFT_pseudo_pipe.py"


def _make_seed_grids(base: Path):
    points = []
    for m1, m2, s1z in [(36.0, 29.0, 0.1), (34.0, 30.0, -0.1)]:
        point = lalsimutils.ChooseWaveformParams()
        point.m1 = m1 * lal.MSUN_SI
        point.m2 = m2 * lal.MSUN_SI
        point.s1z = s1z
        point.s2z = 0.05
        point.fref = 20.0
        points.append(point)

    xml_base = base / "seed-grid"
    lalsimutils.ChooseWaveformParams_array_to_xml(
        points, fname=str(xml_base), fref=20.0)
    xml_path = base / "seed-grid.xml.gz"
    ascii_path = base / "seed-grid.dat"
    hyperpipeline_io.write_grid_from_P_list(
        str(ascii_path), points, hyperpipeline_io.DEFAULT_BASE_COLUMNS,
        lal_module=lal, lalsimutils_module=lalsimutils)
    return xml_path, ascii_path


def _environment(run_base: Path):
    env = os.environ.copy()
    env.pop("RIFT_HYPERPIPELINE_FORMAT", None)
    env["RIFT_DAG_BACKEND"] = "htcondor"
    env["RIFT_ROOT"] = str(RIFT_ROOT)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(CODE), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env["PATH"] = os.pathsep.join(
        [str(BIN), env.get("PATH", "")]).rstrip(os.pathsep)
    env["GW_SURROGATE"] = ""
    for name in ("XDG_CACHE_HOME", "MPLCONFIGDIR", "PYTHONPYCACHEPREFIX"):
        directory = run_base / name.lower()
        directory.mkdir(parents=True, exist_ok=True)
        env[name] = str(directory)
    return env


def _build(builder: Optional[str], run_base: Path, seed_grid: Path, cache: Path,
           pickle_file: Optional[Path], run_label: Optional[str] = None,
           bilby_ini: Optional[Path] = None,
           osg_calibration: bool = False):
    label = builder or "default"
    rundir = run_base / (run_label or label.lower())
    command = [
        sys.executable, str(PSEUDO_PIPE),
        "--use-rundir", str(rundir),
    ]
    if builder:
        command.extend(["--pipeline-builder", builder])
    command.extend([
        "--manual-initial-grid", str(seed_grid),
        "--fake-data-cache", str(cache),
        "--event-time", "1126259462.391",
        "--manual-ifo-list", "H1,L1",
        "--approx", "IMRPhenomXPHM",
        "--assume-precessing",
        "--fmin-template", "20",
        "--internal-force-iterations", "2",
        "--internal-n-evaluations-per-iteration", "4",
        "--n-output-samples", "7",
        "--n-output-samples-last", "7",
        "--ile-jobs-per-worker", "3",
        "--cip-explode-jobs", "1",
        "--cip-explode-jobs-last", "1",
        "--add-extrinsic",
        "--add-extrinsic-time-resampling",
        "--batch-extrinsic",
        "--internal-mitigate-fd-J-frame", "rotate",
        "--export-marginal-distance-grid",
        "--export-distance-slices", "2",
        "--calibration-reweighting",
        "--calibration-reweighting-count", "25",
        "--calibration-reweighting-batchsize", "2",
        "--distance-reweighting",
        "--archive-pesummary-label", "rift-test",
        "--archive-pesummary-event-label", "test-event",
        "--skip-reproducibility",
    ])
    if pickle_file is not None:
        command.extend(["--bilby-pickle-file", str(pickle_file)])
    elif bilby_ini is not None:
        command.extend(["--bilby-ini-file", str(bilby_ini)])
    env = _environment(run_base)
    if osg_calibration:
        command.extend([
            "--use-osg", "--calibration-reweighting-osg",
            "--internal-use-oauth-files", "scitokens",
        ])
        env["SINGULARITY_RIFT_IMAGE"] = (
            "osdf://example.invalid/rift/test-rift.sif")
        env["SINGULARITY_BASE_EXE_DIR"] = "/usr/bin"
    result = subprocess.run(
        command, cwd=str(run_base), env=env,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(
            "{} build failed ({}):\n{}".format(
                label, result.returncode, result.stdout))
    (rundir / "pipeline-build.log").write_text(result.stdout)
    return rundir


def _submit_executables(rundir: Path):
    executables = set()
    for submit in rundir.glob("*.sub"):
        for line in submit.read_text().splitlines():
            if line.lower().startswith("executable") and "=" in line:
                executables.add(Path(line.split("=", 1)[1].strip()).name)
    return executables


def _assert_shared_semantics(basic: Path, hyper: Path):
    basic_ile = (basic / "args_ile.txt").read_text().replace(
        str(basic), "<RUNDIR>")
    hyper_ile = (hyper / "args_ile.txt").read_text().replace(
        str(hyper), "<RUNDIR>")
    assert basic_ile == hyper_ile
    assert (basic / "args_cip_list.txt").read_text() == (
        hyper / "args_cip_list.txt").read_text()

    required = {
        "integrate_likelihood_extrinsic_batchmode",
        "util_ConstructIntrinsicPosterior_GenericCoordinates.py",
        "convergence_test_samples.py",
        "calibration_reweighting.py",
        "combine_weights_and_rejection_sample.py",
        "convert_output_format_ascii2h5.py",
        "make_uni_comov_skymap.py",
        "util_CIPDirSummarizeEvidence.py",
        "summarypages",
    }
    for label, rundir in [("BasicIteration", basic), ("Hyperpipe", hyper)]:
        missing = required - _submit_executables(rundir)
        assert not missing, "{} is missing submit executables {}".format(
            label, sorted(missing))

    # Executable presence is insufficient: validate the real rendered CLI
    # contracts that historically failed while the DAG still built cleanly.
    basic_convert = (basic / "Convert_ascii2h5.sub").read_text()
    hyper_convert = (hyper / "TERMINAL_posterior_hdf5.sub").read_text()
    assert "--posterior-samples" in basic_convert
    assert "--posterior-file" not in basic_convert
    assert "--posterior-samples" in hyper_convert
    basic_rotation = (basic / "fix_frame_rot.sh").read_text()
    assert "extrinsic_posterior_samples_orig.dat" in basic_rotation
    assert "extrinsic_posterior_samples_orig .dat" not in basic_rotation


def _dag_jobs_and_edges(path: Path):
    jobs = {}
    edges = set()
    for line in path.read_text().splitlines():
        fields = line.split()
        if fields and fields[0] == "JOB":
            jobs[fields[1]] = Path(fields[2]).name
        elif fields and fields[0] == "PARENT":
            split_at = fields.index("CHILD")
            for parent in fields[1:split_at]:
                for child in fields[split_at + 1:]:
                    edges.add((parent, child))
    return jobs, edges


def _nodes_for_submit(jobs, submit_name):
    return {node for node, submit in jobs.items() if submit == submit_name}


def _canonical_dag(path: Path):
    jobs, edges = _dag_jobs_and_edges(path)
    job_counts = Counter(jobs.values())
    edge_counts = Counter((jobs[parent], jobs[child]) for parent, child in edges)
    return job_counts, edge_counts


def _assert_default_basic_unchanged(explicit: Path, default: Path):
    explicit_ile = (explicit / "args_ile.txt").read_text().replace(
        str(explicit), "<RUNDIR>")
    default_ile = (default / "args_ile.txt").read_text().replace(
        str(default), "<RUNDIR>")
    assert explicit_ile == default_ile
    assert (explicit / "args_cip_list.txt").read_text() == (
        default / "args_cip_list.txt").read_text()
    dag_name = "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag"
    assert _canonical_dag(explicit / dag_name) == _canonical_dag(default / dag_name)


def _assert_hyperpipe_terminal_chain(hyper: Path):
    manifest = json.loads((hyper / "terminal_stage_specs.json").read_text())
    stages = {stage["name"]: stage for stage in manifest["stages"]}
    expected = {
        "extrinsic_samples", "extrinsic_collect", "frame_rotation",
        "distance_grid", "distance_slices", "calibration_reweight",
        "calibration_merge", "posterior_hdf5",
        "comoving_distance_reweight", "pesummary",
    }
    assert expected <= set(stages)
    assert stages["extrinsic_collect"]["depends_on"] == ["extrinsic_samples"]
    assert stages["extrinsic_collect"]["kind"] == "command-v1"
    assert stages["frame_rotation"]["depends_on"] == ["extrinsic_collect"]
    assert stages["calibration_reweight"]["depends_on"] == ["frame_rotation"]
    assert len(stages["calibration_reweight"]["instances"]) == 4
    assert stages["extrinsic_samples"]["fanout"]["group_sizes"] == [3, 3, 1]
    assert "--allow-empty" not in stages["distance_grid"]["args"]
    assert "--allow-empty" not in stages["distance_slices"]["args"]
    assert stages["calibration_merge"]["depends_on"] == ["calibration_reweight"]
    assert stages["posterior_hdf5"]["depends_on"] == ["calibration_merge"]
    assert stages["comoving_distance_reweight"]["depends_on"] == ["posterior_hdf5"]
    assert stages["pesummary"]["depends_on"] == ["calibration_merge"]
    assert "--samples " in stages["pesummary"]["args"]
    assert "/reweighted_posterior_samples.dat" in stages["pesummary"]["args"]

    dag = hyper / "marginalize_hyperparameters.dag"
    jobs, edges = _dag_jobs_and_edges(dag)
    batches = _nodes_for_submit(jobs, "TERMINAL_calibration_reweight.sub")
    merge = _nodes_for_submit(jobs, "TERMINAL_calibration_merge.sub")
    hdf5 = _nodes_for_submit(jobs, "TERMINAL_posterior_hdf5.sub")
    comoving = _nodes_for_submit(
        jobs, "TERMINAL_comoving_distance_reweight.sub")
    pesummary = _nodes_for_submit(jobs, "TERMINAL_pesummary.sub")
    assert len(batches) == 4 and len(merge) == len(hdf5) == len(comoving) == 1
    assert len(pesummary) == 1
    assert all((batch, next(iter(merge))) in edges for batch in batches)
    assert (next(iter(merge)), next(iter(hdf5))) in edges
    assert (next(iter(hdf5)), next(iter(comoving))) in edges
    assert (next(iter(merge)), next(iter(pesummary))) in edges

    calibration_sub = (hyper / "TERMINAL_calibration_reweight.sub").read_text()
    assert "--start_index $(macrostartidx)" in calibration_sub
    assert "--end_index $(macroendidx)" in calibration_sub
    assert "--waveform_approximant IMRPhenomXPHM" in calibration_sub


def _assert_automatic_bilby_chain(hyper: Path):
    manifest = json.loads((hyper / "terminal_stage_specs.json").read_text())
    stages = {stage["name"]: stage for stage in manifest["stages"]}
    assert stages["bilby_pickle"]["depends_on"] == ["frame_rotation"]
    assert stages["calibration_reweight"]["depends_on"] == ["bilby_pickle"]
    assert "calmarg_data_dump.pickle" in stages["bilby_pickle"]["args"]
    assert "calmarg_data_dump.pickle" in stages["calibration_reweight"]["args"]

    dag = hyper / "marginalize_hyperparameters.dag"
    jobs, edges = _dag_jobs_and_edges(dag)
    pickle_nodes = _nodes_for_submit(jobs, "TERMINAL_bilby_pickle.sub")
    calibration_nodes = _nodes_for_submit(
        jobs, "TERMINAL_calibration_reweight.sub")
    assert len(pickle_nodes) == 1 and len(calibration_nodes) == 4
    assert all(
        (next(iter(pickle_nodes)), calibration) in edges
        for calibration in calibration_nodes)


def _assert_osg_calibration_contract(hyper: Path):
    manifest = json.loads((hyper / "terminal_stage_specs.json").read_text())
    stages = {stage["name"]: stage for stage in manifest["stages"]}
    calibration = stages["calibration_reweight"]
    commands = calibration["execution"]["backend_commands"]["htcondor"]
    assert commands["use_oauth_services"] == "scitokens"
    assert "test-rift.sif" in commands["MY.SingularityImage"]
    assert "weight_files" in commands["transfer_output_files"]
    assert "calmarg_data_dump.pickle" in commands["transfer_input_files"]
    assert "extrinsic_posterior_samples.dat" in commands["transfer_input_files"]

    submit = (hyper / "TERMINAL_calibration_reweight.sub").read_text()
    assert "use_oauth_services = scitokens" in submit
    assert "weight_files" in submit
    assert "test-rift.sif" in submit


def _assert_basic_reweighting_chain(basic: Path):
    dag = basic / "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag"
    jobs, edges = _dag_jobs_and_edges(dag)
    batches = _nodes_for_submit(jobs, "Calib_reweight.sub")
    merge = _nodes_for_submit(jobs, "CAL_REWEIGHT_COMBINE.sub")
    hdf5 = _nodes_for_submit(jobs, "Convert_ascii2h5.sub")
    comoving = _nodes_for_submit(jobs, "Comov_dist.sub")
    assert len(batches) == 4 and len(merge) == len(hdf5) == len(comoving) == 1
    assert all((batch, next(iter(merge))) in edges for batch in batches)
    assert "--allow-empty" not in (basic / "consolidate_dgrid.sh").read_text()
    assert "--allow-empty" not in (basic / "consolidate_dslice.sh").read_text()
    assert (next(iter(merge)), next(iter(hdf5))) in edges
    assert (next(iter(hdf5)), next(iter(comoving))) in edges
    # With time resampling enabled, BasicIteration deliberately gives the
    # single all-in-one collector precedence over --batch-extrinsic's
    # per-output converter.  Hyperpipe mirrors this production combination.
    assert len(_nodes_for_submit(jobs, "convert_extr.sub")) == 1
    assert (basic / "allinone_convert.sh").is_file()


def _assert_unsupported_gates(run_base: Path):
    cases = [
        (["--calibration-reweighting"],
         "without either --bilby-pickle-file or --bilby-ini-file"),
        (["--distance-reweighting"],
         "--distance-reweighting without --add-extrinsic"),
        (["--archive-pesummary-label", "review"],
         "--archive-pesummary-label without --add-extrinsic"),
        (["--calibration-reweighting", "--add-extrinsic",
          "--add-extrinsic-time-resampling", "--bilby-pickle-file", "p",
          "--calibration-reweighting-osg"],
         "OSG calibration reweighting requires SINGULARITY_RIFT_IMAGE"),
    ]
    for extra, expected in cases:
        result = subprocess.run(
            [sys.executable, str(PSEUDO_PIPE),
             "--pipeline-builder", "Hyperpipe"] + extra,
            cwd=str(run_base), env=_environment(run_base), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert result.returncode != 0
        assert expected in result.stdout


def _assert_invalid_hyperpipe_grid_fails(run_base: Path, cache: Path,
                                         pickle_file: Path):
    bad_grid = run_base / "headerless-grid.dat"
    bad_grid.write_text("0 0 36 29 0 0 0 0 0 0\n")
    try:
        _build("Hyperpipe", run_base, bad_grid, cache, pickle_file,
               run_label="invalid-hyperpipe-grid")
    except RuntimeError as exc:
        assert "self-describing hyperpipeline ASCII grid" in str(exc)
    else:
        raise AssertionError("headerless Hyperpipe grid unexpectedly built")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keep-output", action="store_true",
        help="Keep the temporary BasicIteration and Hyperpipe build trees")
    args = parser.parse_args(argv)

    if args.keep_output:
        run_base = Path(tempfile.mkdtemp(prefix="rift-pseudo-build-gate-"))
        cleanup = False
    else:
        temporary = tempfile.TemporaryDirectory(prefix="rift-pseudo-build-gate-")
        run_base = Path(temporary.name)
        cleanup = True
    try:
        xml_grid, ascii_grid = _make_seed_grids(run_base)
        cache = run_base / "empty.cache"
        cache.touch()
        pickle_file = run_base / "calmarg_data_dump.pickle"
        bilby_ini = run_base / "event.ini"
        bilby_ini.write_text(
            "channel-dict = {H1:FAKE,L1:FAKE}\n"
            "data-dict = {H1:/dev/null,L1:/dev/null}\n"
            "waveform-approximant = IMRPhenomXPHM\n")
        basic = _build("BasicIteration", run_base, xml_grid, cache, pickle_file)
        default = _build(None, run_base, xml_grid, cache, pickle_file)
        hyper = _build("Hyperpipe", run_base, ascii_grid, cache, pickle_file)
        hyper_auto = _build(
            "Hyperpipe", run_base, ascii_grid, cache, None,
            run_label="hyperpipe-auto-bilby", bilby_ini=bilby_ini)
        hyper_osg = _build(
            "Hyperpipe", run_base, ascii_grid, cache, pickle_file,
            run_label="hyperpipe-osg-calibration", osg_calibration=True)
        _assert_default_basic_unchanged(basic, default)
        _assert_shared_semantics(basic, hyper)
        _assert_basic_reweighting_chain(basic)
        _assert_hyperpipe_terminal_chain(hyper)
        _assert_automatic_bilby_chain(hyper_auto)
        _assert_osg_calibration_contract(hyper_osg)
        _assert_unsupported_gates(run_base)
        _assert_invalid_hyperpipe_grid_fails(run_base, cache, pickle_file)
        print("pseudo build gate: PASS")
        if not cleanup:
            print("outputs retained at {}".format(run_base))
    finally:
        if cleanup:
            temporary.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
