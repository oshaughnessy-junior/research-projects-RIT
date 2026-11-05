"""Cross-model marginalization in the multi-approximant workflow.

Two kinds of check, deliberately separated:

* the ARITHMETIC of the combination, against hand-computed values.  This is the
  science: L_marg(lambda) = sum_m p(m) L_m(lambda), linear in L.
* the SHAPE of the emitted DAG.  The workflow merges in the iteration loop (one
  shared grid, one marginalized fit) and forks at the terminal stage (per-model
  posterior and evidence, recombined by p(m) Z_m).

Neither validates the inference on data; see
RIFT/misc/DESIGN_multiapprox_marginalization.md.

The DAG tests build with TWO approximants on purpose.  With one, every
cross-model path is vacuously satisfied -- which is how this builder kept a
severed grid handoff, an approximant-blind extrinsic stage, and a submit file
naming a directory that was never created.

Two builds run the iteration-index tests.  With puff cadence 1 every puff gate
passes, and two iterations cannot show a fault that starts at iteration 2, so
cadence_build uses six iterations, cadence 2, a binding --puff-max-it and an
exploded CIP.
"""

import fnmatch
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

CODE = Path(__file__).resolve().parents[1]
BIN = CODE / "bin"
CLEANILE = BIN / "util_CleanILE.py"
COMBINE = BIN / "util_CombineApproximantPosteriors.py"
BUILDER = BIN / "create_event_parameter_pipeline_BasicMultiApproxIteration"
BASIC_BUILDER = BIN / "create_event_parameter_pipeline_BasicIteration"
ALTERNATE_BUILDER = BIN / "create_event_parameter_pipeline_AlternateIteration"
NR_BUILDER = BIN / "create_event_nr_pipeline_with_cip"
MODEL_RX = r"approx_(.+?)_consolidated"
N_ITERATIONS = 2  # the DAG fixture's --n-iterations
N_GROUP = 2  # the DAG fixture's --ile-n-events-to-analyze
N_SAMPLES_PER_JOB = 5
SEED_GRID_POINTS = 4
N_TERMINAL_EVENTS = 4  # --last-iteration-extrinsic-nsamples
TERMINAL_SAMPLES_PER_ILE = 3
GWSIGNAL_MODELS = {"SEOBNRv4PHM"}  # the DAG fixture's --approx-gwsignal


def _env():
    env = dict(os.environ)
    env["PYTHONPATH"] = str(CODE) + os.pathsep + env.get("PYTHONPATH", "")
    env["PATH"] = str(BIN) + os.pathsep + env.get("PATH", "")
    env.setdefault("OMP_NUM_THREADS", "1")
    env["GW_SURROGATE"] = ""
    return env


def _run(args, cwd):
    return subprocess.run([sys.executable] + [str(a) for a in args], cwd=str(cwd),
                          env=_env(), text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)


def _row(m1, m2, lnL, sigma=0.01, ntot=1000.0):
    """One 13-column ILE row: indx m1 m2 s1x..s2z lnL sigmaOverL ntot neff."""
    return [-1, m1, m2, 0., 0., 0.1, 0., 0., 0.2, lnL, sigma, ntot, 100.]


def _composite(path, rows):
    np.savetxt(str(path), np.array(rows))


def _lnL_column(stdout):
    return [float(line.split()[9]) for line in stdout.strip().splitlines() if line.strip()]


# --------------------------------------------------------------------------
# the arithmetic
# --------------------------------------------------------------------------

@pytest.fixture
def two_models(tmp_path):
    """One shared intrinsic point; model A has TWO replicas, model B has one.

    This is the case that separates a correct marginalization from flat
    pooling: flat pooling weights by replica COUNT, so A gets 2/3 instead of
    1/2.  With one replica each the two agree, which is why that arrangement
    would not have caught the defect.
    """
    _composite(tmp_path / "approx_MODELA_consolidated_0.composite",
               [_row(10., 8., 100.0), _row(10., 8., 100.0)])
    _composite(tmp_path / "approx_MODELB_consolidated_0.composite",
               [_row(10., 8., 104.0)])
    return tmp_path


def test_flat_pooling_uses_replica_counts_as_model_weights(two_models):
    """Without --model-group-regex the models are weighted by how many times
    each was evaluated.  Pinned because it is the wrong answer we are moving
    away from, and its size is the reason the change matters."""
    out = _run([CLEANILE] + sorted(str(p) for p in two_models.glob("*.composite")), two_models)
    assert out.returncode == 0, out.stderr
    flat = _lnL_column(out.stdout)[0]
    expected = np.log((2 * np.exp(100.0) + np.exp(104.0)) / 3.0)
    assert flat == pytest.approx(expected, abs=1e-9)


def test_model_aware_combination_is_marginalization(two_models):
    """L_marg = sum_m p(m) L_m, uniform p(m) -- linear in L, not in lnL."""
    out = _run([CLEANILE, "--model-group-regex", MODEL_RX]
               + sorted(str(p) for p in two_models.glob("*.composite")), two_models)
    assert out.returncode == 0, out.stderr
    got = _lnL_column(out.stdout)[0]
    expected = np.log(0.5 * np.exp(100.0) + 0.5 * np.exp(104.0))
    assert got == pytest.approx(expected, abs=1e-9)

    # and it must differ from flat pooling by a real amount, or the flag is
    # cosmetic
    flat = np.log((2 * np.exp(100.0) + np.exp(104.0)) / 3.0)
    assert got - flat > 0.3


def test_geometric_mean_is_not_what_we_compute(two_models):
    """Averaging lnL (a logarithmic opinion pool) is NOT model marginalization.

    Guards against someone 'simplifying' the combination to a mean of lnL.
    """
    out = _run([CLEANILE, "--model-group-regex", MODEL_RX]
               + sorted(str(p) for p in two_models.glob("*.composite")), two_models)
    got = _lnL_column(out.stdout)[0]
    geometric = 0.5 * 100.0 + 0.5 * 104.0
    assert abs(got - geometric) > 0.2


def test_model_prior_reweights_the_mixture(two_models):
    out = _run([CLEANILE, "--model-group-regex", MODEL_RX,
                "--model-prior", "MODELA=0.3", "--model-prior", "MODELB=0.7"]
               + sorted(str(p) for p in two_models.glob("*.composite")), two_models)
    assert out.returncode == 0, out.stderr
    got = _lnL_column(out.stdout)[0]
    expected = np.log(0.3 * np.exp(100.0) + 0.7 * np.exp(104.0))
    assert got == pytest.approx(expected, abs=1e-9)


def test_large_model_separation_is_stable(tmp_path):
    """A weak model must not become 0/0 on a stronger model's log scale."""
    _composite(tmp_path / "approx_MODELA_consolidated_0.composite",
               [_row(10., 8., 0.0, sigma=0.2)])
    _composite(tmp_path / "approx_MODELB_consolidated_0.composite",
               [_row(10., 8., 1000.0, sigma=0.1)])
    files = sorted(str(p) for p in tmp_path.glob("*.composite"))
    out = _run([CLEANILE, "--model-group-regex", MODEL_RX] + files, tmp_path)
    assert out.returncode == 0, out.stderr
    fields = out.stdout.strip().split()
    assert fields and all(value.lower() != "nan" for value in fields), out.stdout
    # Uniform model prior: log((exp(0) + exp(1000))/2), whose weak term is
    # negligible but whose integration uncertainty remains well-defined.
    assert float(fields[9]) == pytest.approx(1000.0 - np.log(2.0), abs=1e-9)
    assert float(fields[10]) == pytest.approx(0.1, abs=1e-12)


def test_zero_prior_loud_model_does_not_set_the_numerical_scale(tmp_path):
    """A zero-mass model 1000 nats louder must not underflow the real mixture."""
    _composite(tmp_path / "approx_MODELA_consolidated_0.composite",
               [_row(10., 8., 1000.0, sigma=0.1)])
    _composite(tmp_path / "approx_MODELB_consolidated_0.composite",
               [_row(10., 8., 0.0, sigma=0.2)])
    files = sorted(str(p) for p in tmp_path.glob("*.composite"))
    out = _run([CLEANILE, "--model-group-regex", MODEL_RX,
                "--model-prior", "MODELA=0", "--model-prior", "MODELB=1"]
               + files, tmp_path)
    assert out.returncode == 0, out.stderr
    fields = out.stdout.strip().split()
    assert float(fields[9]) == pytest.approx(0.0, abs=1e-12)
    assert float(fields[10]) == pytest.approx(0.2, abs=1e-12)
    assert "nan" not in (out.stdout + out.stderr).lower()


def test_partial_model_prior_is_refused(two_models):
    """Half-specified weights would silently default the rest to 1.0."""
    out = _run([CLEANILE, "--model-group-regex", MODEL_RX, "--model-prior", "MODELA=0.3"]
               + sorted(str(p) for p in two_models.glob("*.composite")), two_models)
    assert out.returncode != 0
    assert "missing weights" in out.stderr


def test_one_model_reduces_to_the_flat_pool(two_models):
    """Enabling the flag must never change a single-model result."""
    only_a = [str(two_models / "approx_MODELA_consolidated_0.composite")]
    plain = _run([CLEANILE] + only_a, two_models)
    aware = _run([CLEANILE, "--model-group-regex", MODEL_RX] + only_a, two_models)
    assert plain.returncode == 0 and aware.returncode == 0, aware.stderr
    assert plain.stdout == aware.stdout


def test_partial_coverage_is_reported_and_can_be_dropped(tmp_path):
    """A point missing one model is marginalized over the subset, so the
    estimator changes point to point.  That must never be silent."""
    _composite(tmp_path / "approx_MODELA_consolidated_0.composite",
               [_row(10., 8., 100.), _row(12., 9., 101.), _row(14., 7., 99.)])
    _composite(tmp_path / "approx_MODELB_consolidated_0.composite",
               [_row(10., 8., 104.), _row(14., 7., 98.)])
    files = sorted(str(p) for p in tmp_path.glob("*.composite"))

    warned = _run([CLEANILE, "--model-group-regex", MODEL_RX] + files, tmp_path)
    assert warned.returncode == 0, warned.stderr
    assert len(_lnL_column(warned.stdout)) == 3
    assert "WARNING" in warned.stderr and "SUBSET" in warned.stderr

    dropped = _run([CLEANILE, "--model-group-regex", MODEL_RX,
                    "--require-all-models"] + files, tmp_path)
    assert dropped.returncode == 0, dropped.stderr
    assert len(_lnL_column(dropped.stdout)) == 2
    assert "DROPPED 1" in dropped.stderr


def test_unlabelled_input_is_refused(tmp_path):
    """An unmatched filename would be pooled as a nameless extra model."""
    _composite(tmp_path / "stray.composite", [_row(10., 8., 100.)])
    out = _run([CLEANILE, "--model-group-regex", MODEL_RX,
                str(tmp_path / "stray.composite")], tmp_path)
    assert out.returncode != 0
    assert "does not match" in out.stderr


# --------------------------------------------------------------------------
# the final mixture
# --------------------------------------------------------------------------

def _posterior(path, mean, n=4000, seed=7):
    rng = np.random.default_rng(seed)
    dat = np.column_stack([rng.normal(mean, 1.0, n), rng.normal(0.0, 1.0, n)])
    with open(str(path), "w") as f:
        f.write("# m1 m2\n")
        np.savetxt(f, dat)


def _annotation(path, ln_z):
    with open(str(path), "w") as f:
        f.write("# lnL sigma_lnL neff\n{!r} 0.01 100\n".format(ln_z))


@pytest.fixture
def two_posteriors(tmp_path):
    _posterior(tmp_path / "post_A.dat", 10.0, seed=7)
    _posterior(tmp_path / "post_B.dat", 20.0, seed=8)
    _annotation(tmp_path / "annot_A.dat", 100.0)
    _annotation(tmp_path / "annot_B.dat", 102.0)
    return tmp_path


def _combine(tmp_path, extra=()):
    return _run([COMBINE,
                 "--model", "A:{}/post_A.dat:{}/annot_A.dat".format(tmp_path, tmp_path),
                 "--model", "B:{}/post_B.dat:{}/annot_B.dat".format(tmp_path, tmp_path),
                 "--output", "{}/out.dat".format(tmp_path), "--seed", "1"] + list(extra),
                tmp_path)


def test_mixture_is_weighted_by_evidence(two_posteriors):
    out = _combine(two_posteriors)
    assert out.returncode == 0, out.stderr
    w = np.exp(np.array([100.0, 102.0]) - 102.0)
    w = w / w.sum()
    combined = np.genfromtxt(str(two_posteriors / "out.dat"), comments="#")
    # the mixture mean is the weighted mean of the two component means
    assert combined[:, 0].mean() == pytest.approx(w[0] * 10.0 + w[1] * 20.0, abs=0.15)


def test_mixture_prior_multiplies_the_evidence(two_posteriors):
    """p(m) is a prior; Z_m still multiplies it.  Passing --model-prior does
    NOT give a prior-weighted mixture, and that is intended."""
    out = _combine(two_posteriors, ["--model-prior", "A=0.9", "--model-prior", "B=0.1"])
    assert out.returncode == 0, out.stderr
    w = np.exp(np.array([100.0, 102.0]) - 102.0) * np.array([0.9, 0.1])
    w = w / w.sum()
    combined = np.genfromtxt(str(two_posteriors / "out.dat"), comments="#")
    assert combined[:, 0].mean() == pytest.approx(w[0] * 10.0 + w[1] * 20.0, abs=0.15)


def test_mismatched_columns_are_refused(two_posteriors):
    """Mixing posteriors with different columns would produce garbage."""
    with open(str(two_posteriors / "post_B.dat")) as f:
        body = f.read().split("\n", 1)[1]
    with open(str(two_posteriors / "post_B.dat"), "w") as f:
        f.write("# m1 chi_eff\n" + body)
    out = _combine(two_posteriors)
    assert out.returncode != 0
    assert "different column header" in out.stderr


def test_final_mixture_never_manufactures_duplicate_rows(tmp_path):
    """Final posterior grids follow PR #180's without-replacement contract."""
    for label, rows in (("A", "1 2\n1 2\n3 4\n"),
                        ("B", "5 6\n5 6\n7 8\n")):
        with open(str(tmp_path / "post_{}.dat".format(label)), "w") as f:
            f.write("# m1 m2\n" + rows)
        _annotation(tmp_path / "annot_{}.dat".format(label), 0.0)
    out = _combine(tmp_path, ["--n-output-samples", "20"])
    assert out.returncode == 0, out.stderr
    combined = np.atleast_2d(np.genfromtxt(str(tmp_path / "out.dat"), comments="#"))
    assert len(combined) == len(np.unique(combined, axis=0))
    assert len(combined) == 4
    assert "reducing output" in out.stderr


# --------------------------------------------------------------------------
# the emitted DAG
# --------------------------------------------------------------------------

def _dag_facts(rundir):
    dag = next(Path(rundir).glob("*.dag"))
    jobs, macros, parents = {}, {}, {}
    for line in dag.read_text().splitlines():
        parts = line.split()
        if line.startswith("JOB"):
            jobs[parts[1]] = parts[2]
        elif line.startswith("VARS"):
            macros[parts[1]] = dict(re.findall(r'(\w+)="([^"]*)"', line))
        elif line.startswith("PARENT"):
            cut = parts.index("CHILD")
            for child in parts[cut + 1:]:
                parents.setdefault(child, set()).update(parts[1:cut])
    return jobs, macros, parents


def _build_multiapprox(rundir, n_iterations, options, extra_cip_lines=0):
    """Build a two-approximant DAG from synthetic inputs.

    Self-contained on purpose: the test supplies the args_*.txt and grid that
    pseudo_pipe would write, so it needs no event data.  CIP line i carries the
    marker --sigma-cut 0.(i+1).
    """
    pytest.importorskip("RIFT.lalsimutils")
    (rundir / "args_ile.txt").write_text(
        "integrate_likelihood_extrinsic_batchmode --fmin-template 20.0 "
        "--n-max 100 --n-eff 17 "
        "--time-marginalization --distance-marginalization "
        "--distance-marginalization-lookup-table distance_lookup.npz "
        "--approx placeholder\n")
    cip_line = "1   --no-plots --fit-method rf --parameter mc --parameter delta_mc {} --sigma-cut 0.{}\n"
    n_lines = n_iterations + extra_cip_lines
    (rundir / "args_cip_list.txt").write_text("".join(
        cip_line.format("--n-output-samples 5", i + 1) for i in range(n_lines - 1))
        + cip_line.format("--n-output-samples=5 --posterior-unique-draw", n_lines))
    # Deliberately omit --always-succeed.  pseudo_pipe normally adds it for a
    # terminal-extrinsic run, but the builder is also a public/direct entry
    # point and must preserve the terminal fork itself.
    (rundir / "args_test.txt").write_text("X --method lame --parameter mc\n")
    # Plotting ON.  It is off by default, which is why an unresolved
    # $(macroapprox) sat in the plot job's log paths through a whole review
    # cycle: the plot job is model-independent and its node binds only the
    # iteration macros, so a model-tagged log path can never resolve.  A stage
    # that is not built is a stage no assertion can check.
    (rundir / "args_plot.txt").write_text("--parameter mc --parameter eta\n")
    (rundir / "args_puff.txt").write_text(
        "--parameter mc --parameter eta --force-away 0.01\n")

    grid = _run(["-c",
                 "import RIFT.lalsimutils as u;"
                 "P=[];\n"
                 "import numpy as np\n"
                 "for i in range({}):\n"
                 "    p=u.ChooseWaveformParams(); p.m1=(10+i)*u.lsu_MSUN; p.m2=8*u.lsu_MSUN; P.append(p)\n"
                 "u.ChooseWaveformParams_array_to_xml(P,'proposed-grid')\n".format(SEED_GRID_POINTS)],
                rundir)
    if grid.returncode:
        pytest.skip("cannot build a seed grid: {}".format(grid.stderr[-400:]))

    build = _run([BUILDER,
                  "--approx", "IMRPhenomXPHM", "--approx", "SEOBNRv4PHM",
                  "--input-grid", "proposed-grid.xml.gz",
                  "--ile-exe", str(BIN / "integrate_likelihood_extrinsic_batchmode"),
                  "--ile-args", str(rundir / "args_ile.txt"),
                  "--cip-args-list", "args_cip_list.txt",
                  "--test-args", "args_test.txt",
                  "--ile-n-events-to-analyze", str(N_GROUP),
                  "--n-samples-per-job", str(N_SAMPLES_PER_JOB),
                  "--request-memory-CIP", "4096", "--request-memory-ILE", "4096",
                  "--working-directory", str(rundir),
                  "--n-iterations", str(n_iterations), "--n-copies", "1",
                  "--approx-gwsignal", "SEOBNRv4PHM",
                  "--puff-args", str(rundir / "args_puff.txt"),
                  "--last-iteration-extrinsic",
                  "--last-iteration-extrinsic-nsamples", str(N_TERMINAL_EVENTS),
                  "--last-iteration-extrinsic-samples-per-ile", str(TERMINAL_SAMPLES_PER_ILE),
                  "--last-iteration-extrinsic-samples-per-ile-internal", "7",
                  "--plot-args", str(rundir / "args_plot.txt")] + options, rundir)
    if build.returncode:
        pytest.fail("builder failed:\n{}".format(build.stdout[-3000:]))


@pytest.fixture(scope="module")
def multiapprox_rundir(tmp_path_factory):
    rundir = tmp_path_factory.mktemp("multiapprox")
    _build_multiapprox(rundir, N_ITERATIONS,
                       ["--puff-cadence", "1", "--puff-max-it", "1"])
    return rundir


def _read_build(rundir, n, puff_cadence, puff_max_it, explode, first_points):
    jobs, macros, parents = _dag_facts(rundir)
    models = {macros[x]["macroapprox"] for x, s in jobs.items() if s == "ILE.sub"}
    assert len(models) == 2, sorted(models)
    return SimpleNamespace(rundir=rundir, n=n, puff_cadence=puff_cadence,
                           puff_max_it=puff_max_it, explode=explode,
                           first_points=first_points, jobs=jobs, macros=macros,
                           parents=parents, models=models)


@pytest.fixture(scope="module")
def two_iteration_build(multiapprox_rundir):
    return _read_build(multiapprox_rundir, N_ITERATIONS, 1, 1, False, SEED_GRID_POINTS)


@pytest.fixture(scope="module")
def cadence_build(tmp_path_factory):
    """Six iterations, puff cadence 2 and max 3, exploded CIP, a grid job, OSG
    transfer, a p(m), and a CIP list longer than the run.

    Cadence stops PUFF at odd iterations and puff ILE at even ones.  --puff-max-it
    stops PUFF at 4 and puff ILE at 5.  The grid job makes the first iteration
    N_SAMPLES_PER_JOB points.
    """
    rundir = tmp_path_factory.mktemp("multiapprox_cadence")
    (rundir / "args_grid.txt").write_text("X --mc-range [1,2]\n")
    _build_multiapprox(rundir, 6,
                       ["--puff-cadence", "2", "--puff-max-it", "3",
                        "--cip-explode-jobs", "2",
                        "--gridinit-args", str(rundir / "args_grid.txt"),
                        "--approx-prior", "IMRPhenomXPHM=0.3",
                        "--approx-prior", "SEOBNRv4PHM=0.7",
                        "--use-osg"], extra_cip_lines=1)
    return _read_build(rundir, 6, 2, 3, True, N_SAMPLES_PER_JOB)


@pytest.fixture(scope="module", params=["two_iteration_build", "cadence_build"])
def build(request):
    return request.getfixturevalue(request.param)


def test_every_model_reads_one_shared_grid(multiapprox_rundir):
    """The loop ILE grid must NOT be per model, or nothing is marginalized."""
    sub = (multiapprox_rundir / "ILE.sub").read_text()
    grid = re.search(r"--sim-xml\s+(\S+)", sub)
    assert grid, "no --sim-xml in ILE.sub"
    assert "$(macroapprox)" not in grid.group(1), grid.group(1)

    unify = (multiapprox_rundir / "unify.sh").read_text()
    assert "--model-group-regex" in unify, (
        "unify.sh pools composites without --model-group-regex, so replica "
        "counts act as model weights")


def test_terminal_extraction_cannot_be_cut_off_by_convergence(multiapprox_rundir):
    """A successful convergence test exits 1, so terminal runs must force the
    diagnostic-only form instead of gating their per-model terminal fork."""
    test_sub = (multiapprox_rundir / "test.sub").read_text()
    assert "--always-succeed" in test_sub

    dag = next(multiapprox_rundir.glob("*.dag")).read_text()
    assert "ABORT-DAG-ON" in dag, (
        "direct multi-approximant builds without terminal extraction would "
        "otherwise turn successful convergence into a failed DAG")


def test_terminal_cip_grid_covers_every_scheduled_intrinsic_event(multiapprox_rundir):
    """Iteration-CIP sample/uniqueness settings must not truncate terminal work.

    Interior repeated intrinsic points are legal; the final cross-model
    combiner, not CIP_terminal, owns exact output uniqueness.
    """
    cip_sub = (multiapprox_rundir / "CIP_terminal.sub").read_text()
    assert "--posterior-unique-draw" not in cip_sub
    counts = re.findall(r"--n-output-samples(?:=|\s+)(\d+)", cip_sub)
    assert counts == ["4"], cip_sub

    jobs, macros, _ = _dag_facts(multiapprox_rundir)
    terminal_starts = [int(macros[node]["macroevent"])
                       for node, sub in jobs.items()
                       if sub.endswith("ILE_extr.sub")]
    assert terminal_starts
    assert max(terminal_starts) + 2 == int(counts[0])


def test_generator_route_is_per_model(multiapprox_rundir):
    """One DAG must be able to mix waveform families.

    The generator route is a property of the MODEL, not of the run: the phenom
    family has no time-domain mode generator in gwsignal ("generator does not
    provide a method to generate time-domain modes"), while SEOBNRv5* exists
    only there.  A single global --use-gwsignal therefore cannot serve an
    EOB-vs-phenom comparison -- which is the comparison this builder exists for.

    Observed before this was per model: with --use-gwsignal applied globally,
    every IMRPhenomD ILE job died with "ValueError: Invalid Argument" from
    gen_modes and contributed ZERO rows, so util_CleanILE saw one model and the
    run silently degraded to single-model with no marginalization at all.
    """
    sub = (multiapprox_rundir / "ILE.sub").read_text()
    assert "$(macrogwsignal)" in sub or "--use-gwsignal" not in sub, (
        "ILE.sub carries a global --use-gwsignal; the route must be per model")

    jobs, macros, _ = _dag_facts(multiapprox_rundir)
    ile = [n for n, s_ in jobs.items() if s_.endswith("ILE.sub")]
    assert ile, "no loop ILE nodes"
    routed = {macros[n].get("macroapprox"): macros[n].get("macrogwsignal")
              for n in ile if "macrogwsignal" in macros.get(n, {})}
    if routed:
        # whatever the fixture asked for, a model must get one route, not both
        for model, route in routed.items():
            assert route is not None, model


def test_puff_ile_uses_the_same_per_model_route(multiapprox_rundir):
    """Puff nodes differ from ordinary ILE only in the grid they evaluate."""
    ordinary = (multiapprox_rundir / "ILE.sub").read_text()
    puff = (multiapprox_rundir / "ILE_puff.sub").read_text()
    for token in ("--approx $(macroapprox)", "$(macrogwsignal)"):
        assert token in ordinary, ordinary
        assert token in puff, puff
    assert "overlap-grid-$(macroiteration).xml.gz" in ordinary
    assert "puffball-$(macroiteration).xml.gz" in puff

    jobs, macros, _ = _dag_facts(multiapprox_rundir)
    puff_nodes = [n for n, sub in jobs.items() if sub.endswith("ILE_puff.sub")]
    assert puff_nodes, "fixture did not build the normally-enabled puff lane"
    assert {macros[n].get("macroapprox") for n in puff_nodes} == {
        "IMRPhenomXPHM", "SEOBNRv4PHM"}
    assert all("macrogwsignal" in macros[n] for n in puff_nodes)


def test_the_loop_fits_once_per_iteration(multiapprox_rundir):
    jobs, macros, parents = _dag_facts(multiapprox_rundir)
    models = {macros.get(n, {}).get("macroapprox") for n, s in jobs.items()
              if s.endswith("ILE.sub")}
    models.discard(None)
    assert len(models) == 2, sorted(models)

    for node, sub in jobs.items():
        if sub.startswith("CIP") and not sub.startswith("CIP_terminal"):
            assert "macroapprox" not in macros.get(node, {}), (
                "in-loop CIP is per model; it must fit the marginalized net once")

    for node, sub in jobs.items():
        if sub.endswith("unify.sub"):
            waited = {macros.get(p, {}).get("macroapprox") for p in parents.get(node, ())
                      if jobs.get(p, "").endswith("join.sub")}
            waited.discard(None)
            assert waited == models, (
                "unify waits on {}, not every model {}".format(sorted(waited), sorted(models)))


def test_odd_grid_size_does_not_drop_the_remainder_batch(multiapprox_rundir):
    """Five requested points in batches of two require starts 0, 2, and 4."""
    jobs, macros, _ = _dag_facts(multiapprox_rundir)
    by_model = {}
    for node, submit in jobs.items():
        values = macros.get(node, {})
        if (submit.endswith("ILE.sub") and values.get("macroiteration") == "1"):
            by_model.setdefault(values.get("macroapprox"), set()).add(
                values.get("macroevent"))
    assert by_model
    assert all(starts == {"0", "2", "4"} for starts in by_model.values())


def test_the_terminal_stage_forks_and_recombines(multiapprox_rundir):
    jobs, macros, parents = _dag_facts(multiapprox_rundir)
    models = {macros.get(n, {}).get("macroapprox") for n, s in jobs.items()
              if s.endswith("ILE.sub")}
    models.discard(None)

    for suffix in ("CIP_terminal.sub", "cat.sub", "ILE_extr.sub"):
        got = {macros.get(n, {}).get("macroapprox") for n, s in jobs.items()
               if s.endswith(suffix)}
        got.discard(None)
        assert got == models, "{} covers {}, not {}".format(suffix, sorted(got), sorted(models))

    combine = [n for n, s in jobs.items() if s.endswith("combine_models.sub")]
    assert len(combine) == 1
    cats = {macros.get(p, {}).get("macroapprox") for p in parents.get(combine[0], ())
            if jobs.get(p, "").endswith("cat.sub")}
    cats.discard(None)
    assert cats == models, "the mixture is built from {}, not {}".format(sorted(cats), sorted(models))


def test_extrinsic_stage_reads_the_grid_the_run_finished_on(multiapprox_rundir):
    """The off-by-one this builder shipped: the terminal ILE read
    overlap-grid-<n-1> while the final CIP wrote overlap-grid-<n>."""
    jobs, macros, _ = _dag_facts(multiapprox_rundir)
    extrinsic = {macros.get(n, {}).get("macroiteration") for n, s in jobs.items()
                 if s.endswith("ILE_extr.sub")}
    written = {macros.get(n, {}).get("macroiterationnext") for n, s in jobs.items()
               if s.endswith("join_grids.sub") or s.startswith("CIP")}
    extrinsic.discard(None)
    written.discard(None)
    assert len(extrinsic) == 1, sorted(extrinsic)
    assert next(iter(extrinsic)) == max(written, key=int)


def _submit_field(build, node, key):
    """One field of the node's submit file, with its DAG macros substituted."""
    text = (build.rundir / build.jobs[node]).read_text()
    found = re.search(r"^{}\s*=\s*(.*)$".format(key), text, re.M)
    value = found.group(1).strip().strip('"') if found else ""
    for name, macro in build.macros.get(node, {}).items():
        value = value.replace("$({})".format(name), macro)
    return value


def _rel(build, path):
    """A path relative to the run directory, so a wrong directory shows."""
    if os.path.isabs(path):
        path = os.path.relpath(path, str(build.rundir))
    return os.path.normpath(path)


def _file_arg(build, args, flag):
    found = re.search(r"{}[= ](\S+)".format(flag), args)
    assert found, "no {} in {}".format(flag, args)
    return _rel(build, found.group(1))


def _sigma_cut(args):
    """The marker of the CIP line these arguments came from."""
    return re.search(r"--sigma-cut (\S+)", args).group(1)


def _ancestors(node, parents):
    seen, stack = set(), [node]
    while stack:
        for parent in parents.get(stack.pop(), ()):
            if parent not in seen:
                seen.add(parent)
                stack.append(parent)
    return seen


def _only(nodes):
    assert len(nodes) == 1, sorted(nodes)
    return next(iter(nodes))


def _at_iteration(build, it, keep):
    return {n for n, sub in build.jobs.items()
            if keep(sub) and build.macros.get(n, {}).get("macroiteration") == str(it)}


def _of_model(build, model, sub):
    return {n for n, s in build.jobs.items()
            if s == sub and build.macros[n].get("macroapprox") == model}


def _events(build, nodes):
    return sorted(int(build.macros[x]["macroevent"]) for x in nodes)


def _batches(points):
    """First event of each ILE batch that covers this many points."""
    return list(range(0, points, N_GROUP))


def _is_loop_cip(sub):
    return re.fullmatch(r"CIP(_\d+)?\.sub", sub) is not None


def _is_cip_worker(sub):
    return re.fullmatch(r"CIP_worker\d*\.sub", sub) is not None


def _is_loop_ile(sub):
    return sub in ("ILE.sub", "ILE_puff.sub")


def _is_ile(sub):
    return _is_loop_ile(sub) or sub == "ILE_extr.sub"


def _check_ile(build, node, output, directory):
    """An ILE node runs its own model and writes where the next stage reads."""
    model = build.macros[node]["macroapprox"]
    args = _submit_field(build, node, "arguments")
    assert re.findall(r"--approx (\S+)", args)[-1] == model
    assert ("--use-gwsignal" in args.split()) == (model in GWSIGNAL_MODELS)
    assert _rel(build, _submit_field(build, node, "initialdir")) == directory
    assert _file_arg(build, args, "--output-file").startswith(
        "{}-{}".format(output, build.macros[node]["macroevent"]))


def _grid_writer(build, it):
    """The node that puts overlap-grid-(it+1) in the run directory."""
    keep = (lambda s: s == "join_grids.sub") if build.explode else _is_loop_cip
    return _only(_at_iteration(build, it, keep))


def _composite_name(model, it):
    return "approx_{}_consolidated_{}.composite".format(model, it)


def _clean_call(build, node):
    """The util_CleanILE.py line of a unify node's script."""
    script = build.rundir / os.path.basename(_submit_field(build, node, "executable"))
    return [line for line in script.read_text().splitlines() if "util_CleanILE" in line][-1]


def _net_inputs(build, node):
    """Composites a unify node pools: its glob, over those already written."""
    glob_path = _clean_call(build, node).split()[-1]
    assert _rel(build, os.path.dirname(glob_path)) == "."
    pattern = os.path.basename(glob_path)
    if pattern == "$1":
        pattern = _submit_field(build, node, "arguments")
    written = {_rel(build, _submit_field(build, join, "arguments").split()[1]) + ".composite"
               for join in _ancestors(node, build.parents) if build.jobs[join] == "join.sub"}
    return {name for name in written if fnmatch.fnmatchcase(name, pattern)}


def test_each_iteration_fits_the_net_of_its_own_evaluations(build):
    """Iteration N: every model's ILE on overlap-grid-N -> composites N -> all.net
    -> one CIP -> overlap-grid-N+1.  all.net is one file rewritten every
    iteration, so its name cannot show which iteration a reader gets; only the
    DAG edge does.  A CIP not ordered after its own unify fits the previous
    iteration's L_marg, and every macro in the DAG still looks right.  A join
    that does not wait for all its ILE jobs writes a partial composite.  The
    batch list comes from the build inputs, because a batch never scheduled
    leaves the DAG consistent with itself.
    """
    jobs, macros, parents = build.jobs, build.macros, build.parents
    ile_iterations = {macros[n]["macroiteration"] for n, s in jobs.items()
                      if s == "ILE.sub"}
    assert ile_iterations == {str(i) for i in range(build.n)}

    for it in range(build.n):
        joins = _at_iteration(build, it, lambda s: s == "join.sub")
        assert {macros[n]["macroapprox"] for n in joins} == build.models
        for node in joins:
            model = macros[node]["macroapprox"]
            args = _submit_field(build, node, "arguments").split()
            assert [_rel(build, a) for a in args] == [
                "approx_{}_iteration_{}_ile".format(model, it),
                "approx_{}_consolidated_{}".format(model, it)]
            evaluations = {x for x in _at_iteration(build, it, _is_loop_ile)
                           if macros[x]["macroapprox"] == model}
            assert parents[node] == evaluations
            loop = {x for x in evaluations if jobs[x] == "ILE.sub"}
            points = build.first_points if it == 0 else N_SAMPLES_PER_JOB
            assert _events(build, loop) == _batches(points)

        unify = _only(_at_iteration(build, it, lambda s: s == "unify.sub"))
        assert parents[unify] == joins
        assert _rel(build, _submit_field(build, unify, "output")) == "all.net"
        assert _net_inputs(build, unify) == {
            _composite_name(m, i) for m in build.models for i in range(it + 1)}
        expected = re.search(r"--expect-models '([^']*)'", _clean_call(build, unify))
        assert set(expected.group(1).split(",")) == build.models

        fit = _only(_at_iteration(build, it, _is_loop_cip))
        assert unify in parents[fit], "CIP {} fits a stale all.net".format(it)
        args = _submit_field(build, fit, "arguments")
        grid = "overlap-grid-{}".format(it + 1)
        if build.explode:
            grid = "iteration_{}_cip/{}".format(it, grid)
        assert _file_arg(build, args, "--fname") == "all.net"
        assert _file_arg(build, args, "--fname-output-samples") == grid
        assert _sigma_cut(args) == "0.{}".format(it + 1)

        for node in _at_iteration(build, it, lambda s: s == "ILE.sub"):
            args = _submit_field(build, node, "arguments")
            assert _file_arg(build, args, "--sim-xml") == "overlap-grid-{}.xml.gz".format(it)
            _check_ile(build, node, "CME_out", "approx_{}_iteration_{}_ile".format(
                macros[node]["macroapprox"], it))
            if it > 0:
                assert _grid_writer(build, it - 1) in _ancestors(node, parents), (
                    "ILE {} can start before overlap-grid-{} exists".format(it, it))


def test_puffed_points_reach_the_net_of_the_iteration_that_evaluates_them(build):
    """PUFF after the fit of iteration N writes puffball-N+1, and iteration N+1
    evaluates it.  A puffball with no later iteration to evaluate it may or may
    not be written; the design doc lists that as a known limit.
    """
    jobs, macros, parents = build.jobs, build.macros, build.parents
    written = {}
    for node, sub in jobs.items():
        if sub == "PUFF.sub":
            n = macros[node]["macroiterationnext"]
            args = _submit_field(build, node, "arguments")
            assert _file_arg(build, args, "--inj-file") == "overlap-grid-{}.xml.gz".format(n)
            assert _file_arg(build, args, "--inj-file-out") == "puffball-{}".format(n)
            assert _grid_writer(build, int(n) - 1) in _ancestors(node, parents)
            written[n] = node

    allowed = {str(i + 1) for i in range(build.n)
               if i <= build.puff_max_it and i % build.puff_cadence == 0}
    assert set(written) <= allowed
    scheduled = {x for x in allowed if int(x) < build.n and int(x) <= build.puff_max_it}
    assert scheduled, "fixture did not build the puff lane"
    for model in build.models:
        evaluated = _of_model(build, model, "ILE_puff.sub")
        assert {macros[x]["macroiteration"] for x in evaluated} == scheduled
        for it in scheduled:
            batch = {x for x in evaluated if macros[x]["macroiteration"] == it}
            assert _events(build, batch) == _batches(N_SAMPLES_PER_JOB)
        for node in evaluated:
            it = macros[node]["macroiteration"]
            args = _submit_field(build, node, "arguments")
            assert _file_arg(build, args, "--sim-xml") == "puffball-{}.xml.gz".format(it)
            _check_ile(build, node, "CME_out", "approx_{}_iteration_{}_ile".format(model, it))
            assert written[it] in _ancestors(node, parents)


def test_convergence_lane_compares_the_two_newest_grids(build):
    """Convert runs on each new grid; test starts at the second one."""
    parents = build.parents
    for it in range(build.n):
        convert = _only(_at_iteration(build, it + 1, lambda s: s == "convert.sub"))
        assert _grid_writer(build, it) in _ancestors(convert, parents)
        source = _submit_field(build, convert, "arguments").split()[-1]
        assert _rel(build, source) == "overlap-grid-{}.xml.gz".format(it + 1)
        target = _submit_field(build, convert, "output")
        assert _rel(build, target) == "posterior_samples-{}.dat".format(it + 1)

        tests = _at_iteration(build, it + 1, lambda s: s == "test.sub")
        if it == 0:
            assert not tests, "no earlier grid to compare with"
            continue
        test = _only(tests)
        assert convert in parents[test]
        args = _submit_field(build, test, "arguments")
        assert {_rel(build, a) for a in re.findall(r"--samples (\S+)", args)} == {
            "posterior_samples-{}.dat".format(it + 1),
            "posterior_samples-{}.dat".format(it)}


def test_every_stage_of_an_iteration_precedes_the_next(build):
    """A stage dropped from this chain still runs, so no filename shows the race."""
    jobs, macros, parents = build.jobs, build.macros, build.parents
    for it in range(build.n):
        stage = _at_iteration(build, it, lambda s: _is_loop_cip(s) or _is_cip_worker(s)
                              or s == "join_grids.sub")
        stage |= _at_iteration(build, it + 1, lambda s: s in ("convert.sub", "test.sub"))
        stage |= {x for x, s in jobs.items() if s == "PUFF.sub"
                  and macros[x]["macroiterationnext"] == str(it + 1)}
        following = _at_iteration(build, it + 1, _is_loop_ile)
        if it + 1 == build.n:
            following = {x for x, s in jobs.items() if s == "unify_model.sub"}
        assert following
        for node in following:
            assert stage <= _ancestors(node, parents)


def test_exploded_fit_writes_the_next_grid(cadence_build):
    """fit -> workers -> join_grids.  Workers draw the samples, so a worker on the
    wrong fit or CIP line changes the grid with no other sign.
    """
    build = cadence_build
    for it in range(build.n):
        fit = _only(_at_iteration(build, it, _is_loop_cip))
        worker = _only(_at_iteration(build, it, _is_cip_worker))
        join = _grid_writer(build, it)
        assert build.parents[worker] == {fit}
        assert build.parents[join] == {worker}

        cip_dir = "iteration_{}_cip".format(it)
        grid = "overlap-grid-{}".format(it + 1)
        fit_args = _submit_field(build, fit, "arguments")
        assert _file_arg(build, fit_args, "--fit-save-gp") == cip_dir + "/my_fit"
        args = _submit_field(build, worker, "arguments")
        assert _file_arg(build, args, "--fit-load-gp") == cip_dir + "/my_fit.pkl"
        assert _sigma_cut(args) == "0.{}".format(it + 1)
        assert _file_arg(build, args, "--fname") == "all.net"
        assert _file_arg(build, args, "--fname-output-samples") == (
            "{}/{}-$(process)".format(cip_dir, grid))

        args = _submit_field(build, join, "arguments").split()
        assert _rel(build, args[0].split("=", 1)[1]) == grid + ".xml.gz"
        assert {_rel(build, a) for a in args[1:]} == {
            "{}/{}{}.xml.gz".format(cip_dir, grid, tail) for tail in ("", "-0", "-1")}


def test_first_iteration_waits_for_the_grid_job(cadence_build):
    build = cadence_build
    grid = _only({x for x, s in build.jobs.items() if s == "grid.sub"})
    first = _at_iteration(build, 0, lambda s: s == "ILE.sub")
    assert first and all(grid in build.parents[x] for x in first)


def test_osg_transfers_carry_the_grid_each_ile_evaluates(cadence_build):
    """Transfer paths are relative to the job's initialdir, the run's subdirectory."""
    build = cadence_build
    for node, sub in build.jobs.items():
        if _is_ile(sub):
            home = _submit_field(build, node, "initialdir")
            transfers = _submit_field(build, node, "transfer_input_files").split(",")
            grid = _file_arg(build, _submit_field(build, node, "arguments"), "--sim-xml")
            assert grid in {_rel(build, os.path.join(home, x)) for x in transfers}, sub


def test_approx_prior_reaches_both_combinations(cadence_build):
    """One p(m) must weight the in-loop marginalization and the final mixture."""
    build = cadence_build
    weights = {"IMRPhenomXPHM=0.3", "SEOBNRv4PHM=0.7"}
    unify = _only(_at_iteration(build, 0, lambda s: s == "unify.sub"))
    combine = _only({x for x, s in build.jobs.items() if s == "combine_models.sub"})
    for text in (_clean_call(build, unify), _submit_field(build, combine, "arguments")):
        assert set(re.findall(r"--model-prior '?([^' ]+)", text)) == weights


def test_terminal_stage_uses_every_evaluation_and_one_grid_index(build):
    """The terminal fit sits at iteration n-1 and writes index n, like the last
    loop CIP: it fits the same evaluations, so overlap-grid-n has had no ILE.

    The per-model net is a glob, so nothing in its name says which composites it
    saw.  It sees all of that model's composites, and no other model's, only if
    the glob and the DAG order agree.  From there one index n must carry the CIP
    output, the extrinsic input and the evidence the mixture is weighted by.
    """
    jobs, macros, parents = build.jobs, build.macros, build.parents
    n = build.n
    combine = _only({x for x, s in jobs.items() if s == "combine_models.sub"})
    combine_args = _submit_field(build, combine, "arguments")
    mixture = {}
    for entry in re.findall(r"--model (\S+)", combine_args):
        label, samples, evidence = entry.split(":")
        mixture[label] = (_rel(build, samples), _rel(build, evidence))
    assert set(mixture) == build.models

    for model in build.models:
        net = _only(_of_model(build, model, "unify_model.sub"))
        fit = _only(_of_model(build, model, "CIP_terminal.sub"))

        assert _rel(build, _submit_field(build, net, "output")) == (
            "approx_{}_all.net".format(model))
        assert _net_inputs(build, net) == {_composite_name(model, i) for i in range(n)}
        assert _at_iteration(build, n - 1, _is_loop_cip) <= _ancestors(net, parents), (
            "the fork must follow the last marginalized fit")

        assert net in parents[fit]
        assert macros[fit]["macroiteration"] == str(n - 1)
        args = _submit_field(build, fit, "arguments")
        grid = "approx_{}_overlap-grid-{}".format(model, n)
        assert _file_arg(build, args, "--fname") == "approx_{}_all.net".format(model)
        assert _file_arg(build, args, "--fname-output-samples") == grid
        assert _file_arg(build, args, "--fname-output-integral") == grid
        assert _sigma_cut(args) == "0.{}".format(n)

        extrinsic = _of_model(build, model, "ILE_extr.sub")
        assert extrinsic
        for node in extrinsic:
            args = _submit_field(build, node, "arguments")
            assert _file_arg(build, args, "--sim-xml") == grid + ".xml.gz"
            assert fit in parents[node]
        assert mixture[model] == ("extrinsic_posterior_samples_{}.dat".format(model),
                                  grid + "+annotation.dat")


def test_each_model_keeps_its_own_extrinsic_chain(build):
    """ILE_extr -> convert_extr -> cat -> combine, with no batch dropped."""
    macros, parents = build.macros, build.parents
    batches = _batches(N_TERMINAL_EVENTS)
    cats = set()
    for model in build.models:
        directory = "approx_{}_iteration_{}_ile".format(model, build.n)
        extrinsic = _of_model(build, model, "ILE_extr.sub")
        converts = _of_model(build, model, "convert_extr.sub")
        assert _events(build, extrinsic) == batches
        for node in extrinsic:
            _check_ile(build, node, "EXTR_out", directory)
        assert len(converts) == N_GROUP * len(batches)
        for node in converts:
            event = macros[node]["macroevent"]
            assert parents[node] == {x for x in extrinsic if macros[x]["macroevent"] == event}
            source = _submit_field(build, node, "arguments").split()[-1]
            assert _rel(build, source) == "{}/EXTR_out-{}.xml_{}_.xml.gz".format(
                directory, event, macros[node]["macroindx"])

        cat = _only(_of_model(build, model, "cat.sub"))
        assert parents[cat] == converts
        args = _submit_field(build, cat, "arguments").split()
        assert _rel(build, args[0]) == directory
        assert _rel(build, args[1]) == "extrinsic_posterior_samples_{}.dat".format(model)
        cats.add(cat)

    combine = _only({x for x, s in build.jobs.items() if s == "combine_models.sub"})
    assert parents[combine] == cats
    target = re.search(r"--n-output-samples (\d+)", _submit_field(build, combine, "arguments"))
    assert int(target.group(1)) == len(batches) * N_GROUP * TERMINAL_SAMPLES_PER_ILE


def test_terminal_extrinsic_export_is_a_bounded_fair_draw(multiapprox_rundir):
    """Never serialize the full terminal importance-sampling cache.

    A nonconverged science-scale point can reach n-max with millions of raw
    draws.  The terminal ILE must fair-draw before --save-samples writes the
    record, and the already-equal-weight result must go straight from convert
    to cat rather than through a second weighted resampler.
    """
    sub = (multiapprox_rundir / "ILE_extr.sub").read_text()
    assert "--save-samples" in sub
    assert "--fairdraw-extrinsic-output" in sub
    assert "--fairdraw-extrinsic-output-n-max 3" in sub
    assert "--resample-time-marginalization" in sub
    assert not re.search(r'(?<!\S)--distance-marginalization(?=\s|$)', sub)
    assert "--distance-marginalization-lookup-table distance_lookup.npz" in sub
    assert "--fmin-template 20.0" in sub
    assert "--20.0" not in sub
    # The terminal-stage helper may ask for fewer samples, but it must never
    # weaken the science configuration's convergence target.
    assert re.findall(r"--n-eff\s+(\d+)", sub)[-1] == "17"

    jobs, _, parents = _dag_facts(multiapprox_rundir)
    assert not any(name.endswith("resample.sub") for name in jobs.values())
    cat_nodes = [n for n, name in jobs.items() if name.endswith("cat.sub")]
    assert cat_nodes
    for cat in cat_nodes:
        assert all(jobs[parent].endswith("convert_extr.sub")
                   for parent in parents.get(cat, ()))


def test_no_condor_macro_survives_into_a_shell_script(multiapprox_rundir):
    """A $(macro) in a .sh is command substitution, not a condor macro.

    Inside bash, $(macroapprox) RUNS a command named macroapprox, which does not
    exist, so it expands to the empty string and any glob built from it silently
    matches nothing.  Condor never sees it: macros are expanded in the SUBMIT
    file, not in the script the submit file invokes.

    This has now bitten twice in this builder -- join_grids.sh globbing
    approx__overlap-grid-*, and unify_model.sh globbing approx__*.composite,
    which produced empty per-model nets and killed the terminal CIP with
    "IndexError: too many indices for array".  Both failures are silent at build
    time and only appear as missing files at run time, which is why this is a
    build-time assertion.

    The fix in both cases is to pass the pattern as an ARGUMENT and let condor
    expand the macro in the .sub.
    """
    offenders = []
    for script in sorted(multiapprox_rundir.glob("*.sh")):
        for lineno, line in enumerate(script.read_text().splitlines(), 1):
            if re.search(r"\$\(macro\w+\)", line):
                offenders.append("{}:{}: {}".format(script.name, lineno, line.strip()))
    assert not offenders, (
        "condor macros interpolated into shell scripts, where bash treats them "
        "as command substitution:\n  " + "\n  ".join(offenders))


def test_model_aware_unify_scripts_fail_closed(multiapprox_rundir):
    """CleanILE validation failure must not silently fall back to raw cat."""
    for name in ("unify.sh", "unify_model.sh"):
        text = (multiapprox_rundir / name).read_text()
        assert "exit $ret_value" in text
        assert not re.search(r"else\s+cat ", text)


# --------------------------------------------------------------------------
# a model name that is a prefix of another
# --------------------------------------------------------------------------
#
# Real pairs exist: IMRPhenomPv2 and IMRPhenomPv2_NRTidal, SEOBNRv4 and
# SEOBNRv4_ROM.  One shared grid point, with unequal replica counts so that the
# marginalized value differs from a flat pool.

PREFIX_PAIR_LNL = {"IMRPhenomPv2": 100.0, "IMRPhenomPv2_NRTidal": 104.0}
PREFIX_PAIR_REPLICAS = {"IMRPhenomPv2": 2, "IMRPhenomPv2_NRTidal": 1}


def _sub_arguments(path):
    return re.search(r'^arguments\s*=\s*"(.*)"$', Path(path).read_text(), re.M).group(1)


def _expand_macros(text, macros):
    return re.sub(r"\$\((\w+)\)", lambda m: macros[m.group(1)], text)


@pytest.fixture(scope="module")
def prefix_pair_rundir(tmp_path_factory):
    """Build a prefix-pair DAG and place each composite where join.sub writes it."""
    pytest.importorskip("RIFT.lalsimutils")
    rundir = tmp_path_factory.mktemp("multiapprox_prefix")
    (rundir / "args_ile.txt").write_text(
        "X --n-eff 20 --time-marginalization --vectorized --gpu --force-xpy "
        "--approx placeholder\n")
    (rundir / "args_test.txt").write_text("X --method lame --parameter mc\n")
    (rundir / "args_cip_list.txt").write_text(
        "2 --no-plots --fit-method rf --parameter mc --n-output-samples 5\n")
    grid = _run(["-c",
                 "import RIFT.lalsimutils as u\n"
                 "p=u.ChooseWaveformParams(); p.m1=30*u.lsu_MSUN; p.m2=20*u.lsu_MSUN\n"
                 "u.ChooseWaveformParams_array_to_xml([p],'proposed-grid')\n"], rundir)
    if grid.returncode:
        pytest.skip("cannot build a seed grid: {}".format(grid.stderr[-400:]))
    cmd = [BUILDER, "--input-grid", "proposed-grid.xml.gz",
           "--ile-exe", str(BIN / "integrate_likelihood_extrinsic_batchmode"),
           "--ile-args", str(rundir / "args_ile.txt"),
           "--cip-args-list", str(rundir / "args_cip_list.txt"),
           "--test-args", str(rundir / "args_test.txt"),
           "--n-samples-per-job", "2",
           "--request-memory-CIP", "4096", "--request-memory-ILE", "4096",
           "--working-directory", str(rundir),
           "--n-iterations", "2", "--n-copies", "1",
           "--last-iteration-extrinsic", "--last-iteration-extrinsic-nsamples", "4"]
    for model in PREFIX_PAIR_LNL:
        cmd += ["--approx", model]
    build = _run(cmd, rundir)
    assert build.returncode == 0, build.stdout[-1500:] + build.stderr[-1500:]

    jobs, macros, _ = _dag_facts(rundir)
    target = _sub_arguments(rundir / "join.sub").split()[1]
    joins = [node for node, sub in jobs.items() if Path(sub).name == "join.sub"]
    assert len(joins) == 2 * len(PREFIX_PAIR_LNL)
    for node in joins:
        model = macros[node]["macroapprox"]
        _composite(_expand_macros(target, macros[node]) + ".composite",
                   [_row(10., 8., PREFIX_PAIR_LNL[model])] * PREFIX_PAIR_REPLICAS[model])
    return rundir


@pytest.mark.parametrize("model", list(PREFIX_PAIR_LNL))
def test_per_model_net_excludes_a_model_whose_name_extends_it(prefix_pair_rundir, model):
    """The terminal CIP fits this net, so a foreign row changes that model's
    posterior and evidence while every job still succeeds."""
    jobs, macros, _ = _dag_facts(prefix_pair_rundir)
    node = next(n for n, sub in jobs.items()
                if Path(sub).name == "unify_model.sub" and macros[n]["macroapprox"] == model)
    pattern = _expand_macros(_sub_arguments(prefix_pair_rundir / "unify_model.sub"), macros[node])
    run = subprocess.run([str(prefix_pair_rundir / "unify_model.sh"), pattern],
                         cwd=str(prefix_pair_rundir), env=_env(), text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert run.returncode == 0, run.stderr
    assert _lnL_column(run.stdout) == pytest.approx([PREFIX_PAIR_LNL[model]], abs=1e-9)


def test_marginalized_net_keeps_a_prefix_pair_as_two_models(prefix_pair_rundir):
    """The lazy regex must stop at _consolidated, not at the first underscore."""
    run = subprocess.run([str(prefix_pair_rundir / "unify.sh")],
                         cwd=str(prefix_pair_rundir), env=_env(), text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert run.returncode == 0, run.stderr
    expected = np.log(np.mean(np.exp(list(PREFIX_PAIR_LNL.values()))))
    assert _lnL_column(run.stdout) == pytest.approx([expected], abs=1e-9)


def test_cat_job_is_model_scoped_at_runtime(multiapprox_rundir):
    """Each cat node must search only its model's terminal ILE directory."""
    cat_sub = (multiapprox_rundir / "cat.sub").read_text()
    arguments = re.search(r'^arguments\s*=\s*"(.*)"$', cat_sub, re.M)
    assert arguments, cat_sub
    assert "approx_$(macroapprox)_iteration_$(macroiteration)_ile" in arguments.group(1)
    assert "extrinsic_posterior_samples_$(macroapprox).dat" in arguments.group(1)

    model_a = multiapprox_rundir / "approx_IMRPhenomXPHM_iteration_2_ile"
    model_b = multiapprox_rundir / "approx_SEOBNRv4PHM_iteration_2_ile"
    expected = ["EXTR_out-0.xml_0_.dat", "EXTR_out-0.xml_1_.dat",
                "EXTR_out-2.xml_0_.dat", "EXTR_out-2.xml_1_.dat"]
    for name in expected:
        (model_a / name).write_text("m1 m2\n11 8\n11 8\n")
        (model_b / name).write_text("m1 m2\n99 8\n")
    # A rescue/rebuild can leave same-format products that are not in the
    # current DAG.  Exact manifest collection must ignore them.
    (model_a / "EXTR_out-999.xml_0_.dat").write_text("m1 m2\n777 8\n")
    output = multiapprox_rundir / "cat_scope_probe.dat"
    run = subprocess.run(
        [str(multiapprox_rundir / "catjob.sh"), str(model_a), str(output), "2", "2"],
        cwd=str(multiapprox_rundir), env=_env(), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert run.returncode == 0, run.stderr
    text = output.read_text()
    # RIFT posterior exports deliberately exclude repeated samples.
    assert text.count("11 8") == 1
    assert "99 8" not in text
    assert "777 8" not in text

    (model_a / expected[-1]).unlink()
    missing = subprocess.run(
        [str(multiapprox_rundir / "catjob.sh"), str(model_a),
         str(multiapprox_rundir / "cat_missing_probe.dat"), "2", "2"],
        cwd=str(multiapprox_rundir), env=_env(), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert missing.returncode != 0
    assert "missing expected input" in missing.stderr


def test_stage_inputs_name_files_the_workflow_produces(multiapprox_rundir):
    """Every stage must read a filename some other stage writes.

    The puffball read 'input-grid-N.xml.gz', which nothing in this workflow
    produces, so it could never run -- and nothing noticed until an iteration
    count high enough to reach it, because short runs never build that node.
    CIP writes overlap-grid-N.

    Checked against the set of names the submit files WRITE, so a rename on
    either side is caught at build time rather than as a FileNotFoundError
    hours into a campaign.
    """
    written = set()
    for sub in multiapprox_rundir.glob("*.sub"):
        text = sub.read_text()
        for m in re.finditer(r"--fname-output-samples[= ](\S+)", text):
            written.add(os.path.basename(m.group(1)))
        for m in re.finditer(r"^output\s*=\s*(\S+)", text, re.M):
            written.add(os.path.basename(m.group(1)))
    def base(n):
        return re.sub(r"\$\(\w+\)", "N", os.path.basename(n)).replace(".xml.gz", "")
    written_bases = {base(w) for w in written}
    problems = []
    for sub in multiapprox_rundir.glob("*.sub"):
        for m in re.finditer(r"--sim-xml\s+(\S+)|--fname\s+(\S+\.xml\.gz)", sub.read_text()):
            name = m.group(1) or m.group(2)
            b = base(name)
            if b.startswith("input-grid"):
                problems.append("{}: reads {}, which nothing writes (CIP writes "
                                "overlap-grid-N)".format(sub.name, os.path.basename(name)))
    assert not problems, "\n  ".join(problems)


def test_every_job_directory_exists(build):
    """A submit file naming a directory the builder never created holds the job
    on the execute node, and no DAG-shape assertion sees it.  An unresolved
    $(macro) is a failure too: that is how ILE_extr came to interpolate an
    empty approximant into both --approx and its initialdir.

    initialdir IS a directory; output/error/log are files whose directory must
    exist.  Taking dirname of both silently checks initialdir's parent.
    """
    jobs, macros = build.jobs, build.macros
    cache, problems = {}, []
    for node, submit in jobs.items():
        if submit not in cache:
            text = (build.rundir / submit).read_text()
            cache[submit] = (
                [(v, True) for v in re.findall(r"^initialdir\s*=\s*(\S+)", text, re.M)]
                + [(v, False) for v in re.findall(r"^(?:output|error|log)\s*=\s*(\S+)", text, re.M)])
        for raw, is_dir in cache[submit]:
            resolved = raw
            for key, value in macros.get(node, {}).items():
                resolved = resolved.replace("$({})".format(key), value)
            resolved = re.sub(r"\$\((?:cluster|process|macromassid)\)", "X", resolved)
            if "$(" in resolved:
                problems.append("{}: unresolved macro in {}".format(submit, resolved))
                continue
            target = resolved if is_dir else os.path.dirname(resolved)
            if target and not os.path.isdir(target):
                problems.append("{}: missing directory {}".format(submit, target))
    assert not problems, "\n  ".join(sorted(set(problems)))


# --------------------------------------------------------------------------
# a CIP schedule that does not match --n-iterations
# --------------------------------------------------------------------------
#
# pseudo_pipe sets --n-iterations from the schedule unless
# --internal-force-iterations overrides it.  A short schedule must extend its
# last line, so every requested iteration has a fit.

def _build_cip_schedule(rundir, builder, prefixes, n_iterations, extra=(), check=True):
    """Build a DAG whose CIP line i carries the marker --sigma-cut 0.(i+1)."""
    pytest.importorskip("RIFT.lalsimutils")
    (rundir / "args_ile.txt").write_text(
        "X --n-eff 20 --time-marginalization --vectorized --gpu --force-xpy "
        "--approx placeholder\n")
    (rundir / "args_test.txt").write_text("X --method lame --parameter mc\n")
    (rundir / "args_cip_list.txt").write_text("".join(
        "{} --no-plots --fit-method rf --parameter mc --sigma-cut 0.{}\n".format(p, i + 1)
        for i, p in enumerate(prefixes)))
    grid = _run(["-c",
                 "import RIFT.lalsimutils as u\n"
                 "p=u.ChooseWaveformParams(); p.m1=30*u.lsu_MSUN; p.m2=20*u.lsu_MSUN\n"
                 "u.ChooseWaveformParams_array_to_xml([p],'proposed-grid')\n"], rundir)
    if grid.returncode:
        pytest.skip("cannot build a seed grid: {}".format(grid.stderr[-400:]))
    cmd = [builder,
           "--input-grid", "proposed-grid.xml.gz",
           "--ile-exe", str(BIN / "integrate_likelihood_extrinsic_batchmode"),
           "--ile-args", str(rundir / "args_ile.txt"),
           "--cip-args-list", str(rundir / "args_cip_list.txt"),
           "--test-args", str(rundir / "args_test.txt"),
           "--n-samples-per-job", "2",
           "--request-memory-CIP", "4096", "--request-memory-ILE", "4096",
           "--working-directory", str(rundir),
           "--n-iterations", str(n_iterations), "--n-copies", "1"]
    if builder == NR_BUILDER:
        (rundir / "args_refine.txt").write_text("X --parameter mc\n")
        cmd += ["--nr-refine-args", str(rundir / "args_refine.txt"),
                "--nr-refine-exe", "/bin/true"]
    else:
        cmd += ["--last-iteration-extrinsic",
                "--last-iteration-extrinsic-nsamples", "4"]
    if builder == BUILDER:
        cmd += ["--approx", "IMRPhenomXPHM", "--approx", "SEOBNRv4PHM"]
    elif builder == BASIC_BUILDER:
        cmd += ["--n-iterations-subdag-max", "1"]
    build = _run(cmd + list(extra), rundir)
    if check:
        assert build.returncode == 0, build.stdout[-1500:] + build.stderr[-1500:]
    return build


def _cip_line_per_iteration(rundir, n_iterations, sub_pattern=r"CIP_(\d+)\.sub"):
    """The CIP list line each iteration fits with; None where no line runs."""
    jobs, macros, _ = _dag_facts(rundir)
    line = {}
    for node, sub in jobs.items():
        match = re.fullmatch(sub_pattern, Path(sub).name)
        if match:
            line[int(macros[node]["macroiteration"])] = int(match.group(1))
    return [line.get(it) for it in range(n_iterations)]


@pytest.mark.parametrize("builder", [BUILDER, BASIC_BUILDER, ALTERNATE_BUILDER],
                         ids=["multiapprox", "basic", "alternate"])
@pytest.mark.parametrize("prefixes,n_iterations,expected", [
    (["1", "2"], 5, [0, 1, 1, 1, 1]),            # short: last line extends
    (["2", "2"], 5, [0, 0, 1, 1, 1]),            # one iteration short
    (["2"], 5, [0, 0, 0, 0, 0]),
    (["1", "1", "1"], 5, [0, 1, 2, 2, 2]),
    (["1", "G2"], 5, [0, 1, 1, 1, 1]),
    (["1", "0"], 3, [0, 1, 1]),
    (["2", "2"], 4, [0, 0, 1, 1]),               # exact: nothing to extend
    (["4", "4"], 6, [0, 0, 0, 0, 1, 1]),         # long: the tail never runs
    (["1", "1", "1"], 2, [0, 1]),
])
def test_cip_schedule_covers_every_iteration(tmp_path, builder, prefixes,
                                             n_iterations, expected):
    _build_cip_schedule(tmp_path, builder, prefixes, n_iterations)
    assert _cip_line_per_iteration(tmp_path, n_iterations) == expected

    # The terminal job must use the line the final iteration fit with, not the
    # last line of the file: the two differ when the schedule is long.
    terminal = {BUILDER: "CIP_terminal.sub", BASIC_BUILDER: "CIP_prior.sub"}.get(builder)
    if terminal:
        text = (tmp_path / terminal).read_text()
        assert re.findall(r"--sigma-cut (\S+)", text) == ["0.{}".format(expected[-1] + 1)]


@pytest.mark.parametrize("prefixes,n_iterations,expected", [
    (["1", "Z", "2"], 6, [0, None, 2, 2, 2, 2]),
    (["1", "Z"], 2, [0, None]),
])
def test_extension_lands_after_a_run_to_convergence_stage(tmp_path, prefixes,
                                                          n_iterations, expected):
    """A Z stage holds exactly one iteration, whatever the requested total."""
    _build_cip_schedule(tmp_path, BASIC_BUILDER, prefixes, n_iterations)
    assert _cip_line_per_iteration(tmp_path, n_iterations) == expected


def test_basic_builder_cannot_extend_a_run_to_convergence_stage(tmp_path):
    """A short schedule ending in Z stops the build; no iterations are invented."""
    build = _build_cip_schedule(tmp_path, BASIC_BUILDER, ["1", "Z"], 5, check=False)
    assert build.returncode != 0
    assert "IndexError" in build.stderr
    # It stops at the terminal lookup, before any subdag is written.
    assert "cip_args_for_iteration[opts.n_iterations-1]" in build.stderr


@pytest.mark.parametrize("builder", [BUILDER, ALTERNATE_BUILDER],
                         ids=["multiapprox", "alternate"])
@pytest.mark.parametrize("prefixes,n_iterations,expected", [
    (["1", "Z"], 5, [0, 1, 1, 1, 1]),
    (["1", "Z", "2"], 6, [0, 1, 2, 2, 2, 2]),
])
def test_builders_without_a_convergence_stage_treat_z_as_one_fit(
        tmp_path, builder, prefixes, n_iterations, expected):
    """Z counts as an ordinary line here, so a final Z line repeats as a fixed fit."""
    _build_cip_schedule(tmp_path, builder, prefixes, n_iterations)
    assert _cip_line_per_iteration(tmp_path, n_iterations) == expected


@pytest.mark.parametrize("builder", [BUILDER, BASIC_BUILDER, ALTERNATE_BUILDER],
                         ids=["multiapprox", "basic", "alternate"])
def test_cip_schedule_extends_with_exploded_jobs(tmp_path, builder):
    _build_cip_schedule(tmp_path, builder, ["1", "2"], 5,
                        extra=["--cip-explode-jobs", "2"])
    workers = _cip_line_per_iteration(tmp_path, 5, r"CIP_worker(\d+)\.sub")
    assert workers == [0, 1, 1, 1, 1]


def test_puff_stage_follows_the_extended_cip_line(tmp_path):
    puff_list = tmp_path / "args_puff_list.txt"
    puff_list.write_text("1 --parameter mc --puff-factor 2\n"
                         "2 --parameter mc --puff-factor 3\n")
    _build_cip_schedule(tmp_path, BASIC_BUILDER, ["1", "2"], 5, extra=[
        "--puff-exe", str(BIN / "util_ParameterPuffball.py"),
        "--puff-args-list", str(puff_list),
        "--puff-cadence", "1", "--puff-max-it", "9"])
    assert _cip_line_per_iteration(tmp_path, 5, r"PUFF_(\d+)\.sub") == [0, 1, 1, 1, 1]


@pytest.mark.parametrize("prefixes,n_iterations,line", [
    (["1", "2"], 5, 1),
    (["2", "2"], 5, 1),
    (["2"], 5, 0),
    (["1", "1", "1"], 5, 2),
    (["1", "0"], 3, 1),
    (["4", "4"], 6, 1),
    (["1", "1", "1"], 2, 1),
])
def test_nr_builder_fits_once_with_the_line_for_the_last_iteration(
        tmp_path, prefixes, n_iterations, line):
    """The NR builder refines in the loop and runs CIP once, at iteration n."""
    _build_cip_schedule(tmp_path, NR_BUILDER, prefixes, n_iterations)
    assert _cip_line_per_iteration(tmp_path, n_iterations + 1) == n_iterations * [None] + [line]
