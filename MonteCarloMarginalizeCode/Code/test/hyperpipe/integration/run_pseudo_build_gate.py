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
import shlex
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Optional

import lal

from RIFT import lalsimutils
from RIFT.misc import hyperpipeline_io
from RIFT.misc import terminal_stage_products


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
           osg_calibration: bool = False,
           z_convergence: bool = False,
           extra_stage_file: Optional[Path] = None):
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
    if z_convergence:
        command.extend([
            "--internal-propose-converge-last-stage",
            "--internal-n-iterations-subdag-max", "3",
        ])
    if extra_stage_file is not None:
        command.extend(
            ["--terminal-stage-extra-file", str(extra_stage_file)])
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


def _dag_executables(rundir: Path, dag_name: str):
    """Executables reachable from the workflow, via each JOB's submit file."""
    dag = rundir / dag_name
    assert dag.is_file(), "no DAG at {}".format(dag)
    jobs, _ = _dag_jobs_and_edges(dag)
    executables = set()
    for submit_name in set(jobs.values()):
        submit = rundir / submit_name
        if not submit.is_file():
            continue
        for line in submit.read_text().splitlines():
            if line.lower().startswith("executable") and "=" in line:
                executables.add(Path(line.split("=", 1)[1].strip()).name)
    return executables


LEDGER_PATH = HERE.parent / "terminal_parity_ledger.json"


def _assert_terminal_parity(basic: Path, hyper: Path):
    """Every executable one builder runs and the other does not must be declared.

    This is the drift guard.  Hyperpipe reconstructs the post-final-iteration
    pipeline -- extrinsic fan-out, calibration reweighting, distance products,
    archival pages -- from its own manifest, in code that lives in
    util_RIFT_pseudo_pipe.py rather than in the builder BasicIteration uses.
    Two implementations of one policy drift, and the dangerous direction is
    silent: someone adds a terminal stage to BasicIteration, Hyperpipe simply
    does not emit it, and a Hyperpipe run quietly produces less than it claims.

    A fixed allowlist of "executables both must have" cannot catch that -- the
    allowlist does not grow when the pipeline does.  So instead we compare the
    two builders' actual executable sets and require every DIFFERENCE to carry
    a written reason in terminal_parity_ledger.json.  Adding a stage to either
    builder then forces an explicit decision: port it, or record why not.
    """
    # Executables of jobs the DAG actually RUNS, not every .sub written.
    # Both builders emit submit templates they may never instantiate; counting
    # those would fill the ledger with stages nobody executes and hide the
    # real ones.
    basic_exes = _dag_executables(
        basic, "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag")
    hyper_exes = _dag_executables(hyper, "marginalize_hyperparameters.dag")
    ledger = json.loads(LEDGER_PATH.read_text())

    if os.environ.get("RIFT_PARITY_LEDGER_DUMP"):
        Path(os.environ["RIFT_PARITY_LEDGER_DUMP"]).write_text(json.dumps({
            "only_basic": sorted(basic_exes - hyper_exes),
            "only_hyper": sorted(hyper_exes - basic_exes),
            "shared": sorted(basic_exes & hyper_exes),
        }, indent=2))

    problems = []
    for key, observed, owner, other in (
            ("only_basic", basic_exes - hyper_exes, "BasicIteration", "Hyperpipe"),
            ("only_hyper", hyper_exes - basic_exes, "Hyperpipe", "BasicIteration")):
        declared = ledger.get(key, {})
        for name in sorted(observed):
            if name not in declared:
                problems.append(
                    "{} runs {!r} and {} does not, and the parity ledger does "
                    "not say why. Either emit it from {} too, or add an entry "
                    "to {} recording the decision.".format(
                        owner, name, other, other, LEDGER_PATH.name))
        for name in sorted(set(declared) - observed):
            problems.append(
                "parity ledger declares {!r} as {}-only, but this build does "
                "not show that. A stale ledger entry hides the next real "
                "divergence; remove it.".format(name, owner))
    assert not problems, "terminal parity drift:\n  " + "\n  ".join(problems)


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


def _dag_node_macros(path: Path):
    """node name -> {macro: value} from the DAG's VARS lines."""
    macros = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if not fields or fields[0] != "VARS":
            continue
        entry = macros.setdefault(fields[1], {})
        for token in fields[2:]:
            key, sep, value = token.partition("=")
            if sep:
                entry[key] = value.strip('"')
    return macros


def _nodes_for_submit(jobs, submit_name):
    return {node for node, submit in jobs.items() if submit == submit_name}


def _is_reachable(edges, source, target):
    pending = [source]
    visited = set()
    while pending:
        node = pending.pop()
        if node == target:
            return True
        if node in visited:
            continue
        visited.add(node)
        pending.extend(child for parent, child in edges if parent == node)
    return False


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


def _assert_stage_product_table_matches(*hyper_dirs):
    """The pinned stage-name/product table must describe what we actually build.

    `RIFT.misc.terminal_stage_products` promises users two things about every
    generated stage: its name, and the file it writes.  Nothing at build time
    reads that table, so without this check the promise is a comment.  The
    check is deliberately grounded in manifests from real pseudo_pipe builds
    rather than in a static scan of the source, because a static scan cannot
    see the stages whose names come from a loop variable.

    Both directions matter.  A stage we emit but do not list is an undocumented
    name users will discover and depend on anyway; a name we list but never
    emit is a promise about a stage that does not exist.  The union across the
    gate's builds is what covers every option-conditional stage -- no single
    build produces all of them, because bilby_pickle exists only when
    --bilby-pickle-file is absent.
    """
    products = terminal_stage_products.TERMINAL_STAGE_PRODUCTS
    emitted = {}
    for hyper in hyper_dirs:
        manifest_path = hyper / "terminal_stage_specs.json"
        if not manifest_path.exists():
            continue
        for stage in json.loads(manifest_path.read_text())["stages"]:
            emitted.setdefault(str(stage["name"]), (hyper, stage))

    undocumented = sorted(set(emitted) - set(products))
    assert not undocumented, (
        "pseudo_pipe emits terminal stages missing from "
        "RIFT.misc.terminal_stage_products: {}".format(undocumented))
    unemitted = sorted(set(products) - set(emitted))
    assert not unemitted, (
        "RIFT.misc.terminal_stage_products names stages no build in this gate "
        "emits, so the promise is unverified: {}".format(unemitted))

    for name, (hyper, stage) in sorted(emitted.items()):
        product = products[name].product
        if product is None:
            continue
        basename = os.path.basename(product)
        haystack = json.dumps(stage)
        if basename not in haystack:
            # extrinsic_collect and frame_rotation carry no args -- the product
            # name lives inside the generated script the stage executes.
            exe = stage.get("exe") or ""
            if exe and os.path.isfile(exe):
                haystack = Path(exe).read_text()
        assert basename in haystack, (
            "terminal stage {!r} is documented as producing {} but neither its "
            "manifest entry nor its executable mentions that file; the table "
            "and the pipeline have drifted".format(name, product))


def _assert_extra_terminal_stages_merge(
        run_base: Path, ascii_grid: Path, cache, pickle_file):
    """A user manifest attaches to the generated graph, and its errors are loud.

    This is the whole point of --terminal-stage-extra-file: a stage that names
    a generated stage in depends_on and reads the file that stage writes.

    The three rejections below are each checked for the SPECIFIC message
    pseudo_pipe emits, not merely for a nonzero exit.  Mutation-testing showed
    why that matters: with pseudo_pipe's check removed, the contract loader
    still rejects the manifest, so an exit-code assertion would pass while the
    user got "terminal stage names must be unique" over a list of eleven
    instead of "your stage collides with one this build generates; rename it".
    Message quality is the whole deliverable here, so it is what the gate
    asserts.
    """
    extra_dir = run_base / "extra-stages"
    extra_dir.mkdir(exist_ok=True)
    extra_path = extra_dir / "user_stages.json"
    extra_path.write_text(json.dumps({
        "version": 1,
        "stages": [
            {
                "name": "user_summary",
                "kind": "command-v1",
                "depends_on": ["frame_rotation"],
                "exe": "/bin/cat",
                "args": "extrinsic_posterior_samples.dat",
                "initial_dir": ".",
            },
            {
                "name": "user_second",
                "kind": "command-v1",
                "depends_on": ["user_summary"],
                "exe": "/bin/echo",
                "args": "done",
                "initial_dir": ".",
            },
        ],
    }, indent=2))
    hyper_extra = _build(
        "Hyperpipe", run_base, ascii_grid, cache, pickle_file,
        run_label="hyperpipe-extra-stages",
        extra_stage_file=extra_path)
    manifest = json.loads(
        (hyper_extra / "terminal_stage_specs.json").read_text())
    names = [stage["name"] for stage in manifest["stages"]]
    assert names[-2:] == ["user_summary", "user_second"], names
    assert "frame_rotation" in names

    dag = hyper_extra / "marginalize_hyperparameters.dag"
    jobs, edges = _dag_jobs_and_edges(dag)
    summary = _nodes_for_submit(jobs, "TERMINAL_user_summary.sub")
    second = _nodes_for_submit(jobs, "TERMINAL_user_second.sub")
    rotation = _nodes_for_submit(jobs, "TERMINAL_frame_rotation.sub")
    assert len(summary) == len(second) == len(rotation) == 1
    assert (next(iter(rotation)), next(iter(summary))) in edges
    assert (next(iter(summary)), next(iter(second))) in edges

    for index, (label, stages, needle) in enumerate([
            ("collision", [{
                "name": "frame_rotation", "kind": "command-v1",
                "exe": "/bin/true"}], "collides with a stage this build"),
            ("absent dependency", [{
                "name": "user_x", "kind": "command-v1", "exe": "/bin/true",
                "depends_on": ["no_such_stage"]}], "does not emit"),
            ("duplicate", [
                {"name": "user_y", "kind": "command-v1", "exe": "/bin/true"},
                {"name": "user_y", "kind": "command-v1", "exe": "/bin/true"}],
             "twice")]):
        bad = extra_dir / "bad-{}.json".format(index)
        bad.write_text(json.dumps({"version": 1, "stages": stages}))
        try:
            _build("Hyperpipe", run_base, ascii_grid, cache,
                   pickle_file,
                   run_label="hyperpipe-extra-bad-{}".format(index),
                   extra_stage_file=bad)
        except RuntimeError as exc:
            assert needle in str(exc), (label, str(exc)[-2000:])
        else:
            raise AssertionError(
                "--terminal-stage-extra-file accepted a {} it must "
                "reject".format(label))

    # Offered to a builder that cannot consume it, the option must fail rather
    # than build a pipeline silently missing the user's stages.
    proc = subprocess.run(
        [sys.executable, str(PSEUDO_PIPE),
         "--pipeline-builder", "BasicIteration",
         "--terminal-stage-extra-file", str(extra_path)],
        cwd=str(run_base), env=_environment(run_base),
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert proc.returncode != 0
    assert "requires --pipeline-builder Hyperpipe" in proc.stdout, (
        proc.stdout[-2000:])


def _assert_extrinsic_reads_the_final_grid(basic: Path, hyper: Path):
    """The extrinsic stage must read the grid the LAST CIP wrote.

    Stated as an invariant rather than a number, so it holds for any iteration
    count: whichever grid index the final grid-writing node produces is the one
    the extrinsic nodes must consume.  In BasicIteration the writer is
    `join_grids`, whose `macroiterationnext` is the index it writes; the
    extrinsic ILE nodes read `overlap-grid-$(macroiteration)`.

    This exists because those two disagreed.  `it` is the iteration loop's
    leftover value, n_iterations-1, and it was corrected to n_iterations only
    when 'Z' was in the CIP schedule -- so on the default non-Z path the
    extrinsic stage read the grid the last ILE ran on (the previous
    iteration's posterior, after puffball) while the final CIP's output, which
    is explicitly sized for this stage, went unread.  The extrinsic nodes take
    that CIP as their parent, so the workflow waited for a job whose output it
    then ignored.

    Found by running, not reading: the extrinsic posterior's intrinsic points
    matched overlap-grid-0 to 4e-7 and sat ~0.9 Msun from overlap-grid-1.  A
    build-time gate can state it exactly, which is why it lives here.
    """
    dag = basic / "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag"
    jobs, _ = _dag_jobs_and_edges(dag)
    macros = _dag_node_macros(dag)

    extrinsic = {
        macros[node].get("macroiteration")
        for node, submit in jobs.items()
        if submit.endswith("ILE_extr.sub")}
    written = {
        macros[node].get("macroiterationnext")
        for node, submit in jobs.items()
        if submit.endswith("join_grids.sub")}
    extrinsic.discard(None)
    written.discard(None)
    assert extrinsic, "no ILE_extr nodes found in {}".format(dag)
    assert written, "no join_grids nodes found in {}".format(dag)
    assert len(extrinsic) == 1, (
        "extrinsic nodes disagree about which grid to read: "
        + repr(sorted(extrinsic)))
    last_written = max(written, key=int)
    assert next(iter(extrinsic)) == last_written, (
        "BasicIteration extrinsic stage reads overlap-grid-{} but the last "
        "grid written is overlap-grid-{}; the final CIP's output -- which is "
        "sized for exactly this stage -- would go unread".format(
            next(iter(extrinsic)), last_written))

    # The Hyperpipe writer states the same thing declaratively, so check it in
    # its own terms rather than assuming the two agree.
    manifest = json.loads((hyper / "terminal_stage_specs.json").read_text())
    stages = {stage["name"]: stage for stage in manifest["stages"]}
    assert stages["extrinsic_samples"]["grid"].endswith(
        "grid-$(macroiteration).dat"), stages["extrinsic_samples"]["grid"]
    hyper_dag = hyper / "marginalize_hyperparameters.dag"
    hyper_macros = _dag_node_macros(hyper_dag)
    hyper_jobs, _ = _dag_jobs_and_edges(hyper_dag)
    hyper_extrinsic = {
        hyper_macros[node].get("macroiteration")
        for node, submit in hyper_jobs.items()
        if "TERMINAL_extrinsic_samples" in submit}
    hyper_extrinsic.discard(None)
    assert len(hyper_extrinsic) == 1, sorted(hyper_extrinsic)
    assert next(iter(hyper_extrinsic)) == last_written, (
        "the two builders' extrinsic stages read different grids: Hyperpipe "
        "grid-{}, BasicIteration overlap-grid-{}".format(
            next(iter(hyper_extrinsic)), last_written))


def _assert_z_subworkflow_contract(hyper: Path):
    outer_dag = hyper / "marginalize_hyperparameters.dag"
    lines = outer_dag.read_text().splitlines()
    subdags = [line.split() for line in lines
               if line.startswith("SUBDAG EXTERNAL ")]
    assert len(subdags) == 1
    child_dag = Path(subdags[0][3])
    assert child_dag.is_file()
    child_text = child_dag.read_text()
    assert "JUMPSTART_MARG_NET.sub" in child_text
    assert "ABORT-DAG-ON" in child_text
    assert "EOS_POST_prior.sub" in child_text
    assert "evidence_final.sub" in child_text
    assert "MARG_" in child_text

    jobs, edges = _dag_jobs_and_edges(outer_dag)
    fetch_submits = sorted(hyper.glob("FETCH_Z_*.sub"))
    assert len(fetch_submits) == 1
    fetch = _nodes_for_submit(jobs, fetch_submits[0].name)
    extrinsic = _nodes_for_submit(jobs, "TERMINAL_extrinsic_samples.sub")
    assert len(fetch) == 1 and extrinsic
    subdag_node = subdags[0][2]
    fetch_node = next(iter(fetch))
    assert (subdag_node, fetch_node) in edges
    # Production schedules append a posterior-unique-draw iteration after Z.
    # The harvested child grid must therefore feed that iteration's MARG jobs,
    # and all terminal exports must remain downstream of the fetch.
    fetch_children = {child for parent, child in edges if parent == fetch_node}
    assert fetch_children
    assert all(jobs[node] == "MARG_0.sub" for node in fetch_children)
    assert all(_is_reachable(edges, fetch_node, node) for node in extrinsic)
    fetch_specs = list(hyper.glob("fetch_z_*.json"))
    assert len(fetch_specs) == 1
    fetch_spec = json.loads(fetch_specs[0].read_text())
    assert fetch_spec["base_pattern"] == "grid-*.dat"


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


def _assert_pesummary_publishes_both_posteriors(basic: Path, hyper: Path):
    """Both builders must archive the pre- AND post-calibration posteriors.

    This replaces a test that pinned a DIVERGENCE: BasicIteration published
    `extrinsic_posterior_samples.dat` (pre-calibration) while Hyperpipe
    published `reweighted_posterior_samples.dat` (post-calibration), under
    identical flags.  BasicIteration assembled its plot arguments before
    calibration policy had selected a final posterior; PR #181 rebound them on
    the Hyperpipe side only.

    Resolved by publishing both rather than choosing: the question a reviewer
    asks of a calibration-marginalized run is what calibration did to the
    posterior, and that needs both on one page.  `args_plot.txt` -- which both
    builders consume -- now names both with distinct labels, so the two cannot
    diverge again without this failing.

    The ordering half matters as much as the arguments: a page that names a
    file produced by a stage it does not wait for is a race, not a comparison.
    """
    for label, path in (("BasicIteration", basic / "plot.sub"),
                        ("Hyperpipe", None)):
        if path is None:
            continue
        text = path.read_text()
        assert "extrinsic_posterior_samples.dat" in text, label
        assert "reweighted_posterior_samples.dat" in text, (
            "{} does not publish the calibration-reweighted posterior".format(
                label))
        assert text.count("--samples") >= 2, label
        assert "_calmarg" in text, (
            "{} publishes both posteriors under one label, so the page cannot "
            "tell them apart".format(label))

    manifest = json.loads((hyper / "terminal_stage_specs.json").read_text())
    stages = {stage["name"]: stage for stage in manifest["stages"]}
    assert "pesummary" in stages
    pesummary_args = stages["pesummary"].get("args", "")
    assert "extrinsic_posterior_samples.dat" in pesummary_args
    assert "reweighted_posterior_samples.dat" in pesummary_args
    assert "_calmarg" in pesummary_args
    assert stages["pesummary"]["depends_on"] == ["calibration_merge"], (
        "the Hyperpipe archive stage must wait for the calibration product it "
        "names: " + str(stages["pesummary"]["depends_on"]))

    # BasicIteration: the archive node must be downstream of the calibration
    # merge, or it can run before reweighted_posterior_samples.dat exists.
    dag = basic / "marginalize_intrinsic_parameters_BasicIterationWorkflow.dag"
    jobs, edges = _dag_jobs_and_edges(dag)
    plot_nodes = _nodes_for_submit(jobs, "plot.sub")
    merge = _nodes_for_submit(jobs, "CAL_REWEIGHT_COMBINE.sub")
    assert len(plot_nodes) == 1 and len(merge) == 1
    assert _is_reachable(edges, next(iter(merge)), next(iter(plot_nodes))), (
        "BasicIteration's archive node is not downstream of the calibration "
        "merge, so it can publish a posterior file that does not exist yet")


#: Arguments the SUBMIT WRITER adds per node, not the extrinsic policy: which
#: grid, which slice of it, where the output goes.  They differ between the two
#: builders because their topologies differ -- BasicIteration fans out inside
#: its per-iteration ILE directory, Hyperpipe from a terminal stage -- and that
#: difference is structural.
_PER_NODE_EXTRINSIC_ARGS = {
    "--sim-grid", "--sim-xml", "--n-events-to-analyze", "--event",
    "--output-file", "--cache", "--cache-file",
}


def _extrinsic_argument_set(tokens):
    """Physics/convergence arguments only, with per-node plumbing removed."""
    keep, skip_value = set(), False
    for token in tokens:
        if skip_value:
            skip_value = False
            continue
        if token in (">", "2>", "X"):
            skip_value = token in (">", "2>")
            continue
        flag = token.split("=", 1)[0]
        if flag in _PER_NODE_EXTRINSIC_ARGS:
            skip_value = "=" not in token
            continue
        keep.add(token)
    return keep


def _assert_extrinsic_arguments_agree(basic: Path, hyper: Path):
    """Both builders must ask ILE for the SAME extrinsic computation.

    They did not.  BasicIteration took the last `--n-eff` in the argument
    string; the Hyperpipe branch took the maximum over every `--n-eff` in the
    string and the `--ile-n-eff` option.  When those disagree the two builders
    request a different number of extrinsic samples per job -- which is the
    deliverable, not an implementation detail.  Both now derive the arguments
    from RIFT.misc.extrinsic_stage, and this pins that they still agree.

    Only the per-node plumbing may differ, because the two topologies differ.

    **What this cannot see:** it compares the two builders to EACH OTHER, so a
    change to the shared policy moves both and still passes.  That case is
    covered by test_extrinsic_stage_shared.py, whose semantic assertions pin
    the values themselves -- verified by mutation: perturbing
    `extrinsic_n_eff` leaves this gate green and fails three unit tests, while
    perturbing only the Hyperpipe side fails this gate.  Two instruments, two
    failure modes; neither alone is sufficient.
    """
    basic_sub = (basic / "ILE_extr.sub").read_text()
    exec_lines = [l for l in basic_sub.splitlines()
                  if l.startswith("exec ") or l.startswith("arguments")]
    assert exec_lines, "no argument line in ILE_extr.sub"
    line = exec_lines[0]
    if line.startswith("exec "):
        value = line[len("exec "):]
    else:
        # HTCondor's "new arguments syntax" wraps the WHOLE list in one pair of
        # double quotes.  Splitting that with shlex without stripping them
        # first yields a single token, and the comparison then reports every
        # argument as a difference -- which is what the first version of this
        # check did.
        value = line.split("=", 1)[1].strip()
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            value = value[1:-1]
    basic_tokens = shlex.split(value, posix=True)
    if basic_tokens and "integrate_likelihood" in basic_tokens[0]:
        basic_tokens = basic_tokens[1:]

    hyper_tokens = shlex.split((hyper / "args_ile_extrinsic.txt").read_text())

    def _generic(tokens, root):
        return {t.replace(str(root), "<RUN>") for t in tokens}

    only_basic = _generic(_extrinsic_argument_set(basic_tokens), basic) - \
        _generic(_extrinsic_argument_set(hyper_tokens), hyper)
    only_hyper = _generic(_extrinsic_argument_set(hyper_tokens), hyper) - \
        _generic(_extrinsic_argument_set(basic_tokens), basic)
    # Path-valued arguments still differ by run directory; drop anything that
    # still looks like a path after the substitution above.
    only_basic = {t for t in only_basic if not t.startswith("/")}
    only_hyper = {t for t in only_hyper if not t.startswith("/")}
    assert not only_basic and not only_hyper, (
        "the two builders ask ILE for different extrinsic computations.\n"
        "  only BasicIteration: {}\n  only Hyperpipe: {}\n"
        "Both should derive these from RIFT.misc.extrinsic_stage.".format(
            sorted(only_basic), sorted(only_hyper)))


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
        hyper_z = _build(
            "Hyperpipe", run_base, ascii_grid, cache, pickle_file,
            run_label="hyperpipe-z-convergence", z_convergence=True)
        _assert_default_basic_unchanged(basic, default)
        _assert_shared_semantics(basic, hyper)
        _assert_terminal_parity(basic, hyper)
        _assert_extrinsic_arguments_agree(basic, hyper)
        _assert_basic_reweighting_chain(basic)
        _assert_pesummary_publishes_both_posteriors(basic, hyper)
        _assert_hyperpipe_terminal_chain(hyper)
        _assert_automatic_bilby_chain(hyper_auto)
        _assert_osg_calibration_contract(hyper_osg)
        _assert_z_subworkflow_contract(hyper_z)
        _assert_extrinsic_reads_the_final_grid(basic, hyper)
        _assert_stage_product_table_matches(
            hyper, hyper_auto, hyper_osg, hyper_z)
        _assert_extra_terminal_stages_merge(
            run_base, ascii_grid, cache, pickle_file)
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
