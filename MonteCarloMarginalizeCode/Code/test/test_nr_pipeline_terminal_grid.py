"""The NR pipeline's terminal extrinsic stage must read the final CIP posterior.

`create_event_nr_pipeline_with_cip` is not reachable from
util_RIFT_pseudo_pipe.py -- its --pipeline-builder selection does not offer it,
and asimov drives pseudo_pipe -- so it is a hand-run bin/ script, and the
terminal extrinsic stage had no build path exercising it at all.  Its one
documented caller, demo/nr_w_rift, does not pass --last-iteration-extrinsic.
That is why the defect this file pins survived: nothing built it.

THE INVARIANT.  With --last-iteration-extrinsic, the terminal ILE runs on
overlap-grid-$(macroiteration).  Unlike the other builders, this workflow's
iteration loop writes its grid with REFINE and the terminal CIP is created
OUTSIDE the loop, at macroiteration=n_iterations / macroiterationnext=n+1.  The
run's final posterior is therefore overlap-grid-<n+1> -- the grid `convert`
turns into posterior_samples-<n+1>.dat -- and that is what the extrinsic stage
must read.

Three properties, not one, because each corresponds to a separate edit and any
one of them alone leaves a workflow that is still wrong:

  1. the index read equals the largest index written;
  2. the node writing it is an ancestor of every ILE_extr node -- before the
     fix it was not, so the extrinsic jobs did not merely read the wrong grid,
     they had no dependency on the CIP writing their input at all;
  3. the directory those nodes name as initialdir exists -- moving the index
     without extending the mkdir loop builds a perfectly well-formed DAG whose
     jobs then fail on the execute node, which no DAG-only check can see.

This builds a real DAG rather than grepping the source.  It writes its own
minimal argument files instead of going through pseudo_pipe, both because
pseudo_pipe cannot reach this builder and so the test does not depend on it.
No DAG is submitted and no likelihood is evaluated.
"""

import os
import re
import subprocess
import sys
from collections import deque

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, os.pardir))
BIN = os.path.join(CODE, "bin")
BUILDER = os.path.join(BIN, "create_event_nr_pipeline_with_cip")

pytest.importorskip("lal", reason="needs the RIFT science stack")
pytest.importorskip("RIFT.lalsimutils", reason="needs the RIFT science stack")

ILE_ARGS = (
    "X --cache local.cache --event-time 1126259462.391"
    " --channel-name H1=FAKE-STRAIN --psd-file H1=H1-psd.xml.gz --fmin-ifo H1=20.0"
    " --channel-name L1=FAKE-STRAIN --psd-file L1=L1-psd.xml.gz --fmin-ifo L1=20.0"
    " --fmin-template 20.0 --reference-freq 20.0 --approx IMRPhenomXPHM"
    " --d-max 10000 --n-max 400000 --n-eff 10 --time-marginalization --vectorized\n")
CIP_LINE = ("1  --no-plots --fit-method rf --parameter mc --parameter delta_mc"
            " --n-output-samples 7 --n-eff 7 --n-max 100000\n")
TEST_ARGS = ("X --method lame --parameter mc --parameter eta"
             " --iteration $(macroiteration) --threshold 0.02 --always-succeed\n")


def _seed_grid(path):
    import lal
    from RIFT import lalsimutils
    points = []
    for m1, m2, s1z in [(36.0, 29.0, 0.1), (34.0, 30.0, -0.1)]:
        P = lalsimutils.ChooseWaveformParams()
        P.m1, P.m2 = m1 * lal.MSUN_SI, m2 * lal.MSUN_SI
        P.s1z, P.s2z, P.fref = s1z, 0.05, 20.0
        points.append(P)
    lalsimutils.ChooseWaveformParams_array_to_xml(points, fname=path, fref=20.0)
    return path + ".xml.gz"


def _build(tmpdir, n_iterations):
    run = str(tmpdir)
    grid = _seed_grid(os.path.join(run, "proposed-grid"))
    with open(os.path.join(run, "args_ile.txt"), "w") as f:
        f.write(ILE_ARGS)
    with open(os.path.join(run, "args_cip_list.txt"), "w") as f:
        f.writelines([CIP_LINE] * max(2, n_iterations))
    with open(os.path.join(run, "args_test.txt"), "w") as f:
        f.write(TEST_ARGS)
    with open(os.path.join(run, "args_refine.txt"), "w") as f:
        f.write("X --test-refinement\n")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [CODE, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env["PATH"] = os.pathsep.join([BIN, env.get("PATH", "")])
    env["OMP_NUM_THREADS"] = "1"
    env["RIFT_DAG_BACKEND"] = "htcondor"
    result = subprocess.run(
        [sys.executable, BUILDER,
         "--ile-n-events-to-analyze", "3",
         "--input-grid", grid,
         "--ile-exe", os.path.join(BIN, "integrate_likelihood_extrinsic_batchmode"),
         "--ile-args", os.path.join(run, "args_ile.txt"),
         "--cip-args-list", "args_cip_list.txt",
         "--test-args", "args_test.txt",
         "--nr-refine-args", "args_refine.txt",
         "--nr-refine-exe", os.path.join(BIN, "util_TestSpokesIO.py"),
         "--nr-group", "Sequence-RIT-All",
         "--request-memory-CIP", "30000", "--request-memory-ILE", "4096",
         "--n-samples-per-job", "4", "--working-directory", run,
         "--n-iterations", str(n_iterations), "--n-copies", "1",
         "--ile-retries", "3", "--general-retries", "3",
         "--last-iteration-extrinsic",
         "--last-iteration-extrinsic-nsamples", "7",
         "--last-iteration-extrinsic-samples-per-ile", "5",
         "--last-iteration-extrinsic-batched-convert"],
        cwd=run, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        pytest.fail("NR pipeline build failed ({}):\n{}".format(
            result.returncode, result.stdout))
    return run, os.path.join(
        run, "marginalize_intrinsic_parameters_NRWorkflow.dag")


def _parse(dag):
    jobs, macros, edges = {}, {}, []
    with open(dag) as stream:
        for line in stream:
            match = re.match(r"^JOB\s+(\S+)\s+(\S+)", line)
            if match:
                jobs[match.group(1)] = os.path.basename(match.group(2))
                continue
            match = re.match(r"^VARS\s+(\S+)\s+(.*)$", line)
            if match:
                macros.setdefault(match.group(1), {}).update(
                    dict(re.findall(r'(\w+)="([^"]*)"', match.group(2))))
                continue
            match = re.match(r"^PARENT\s+(.*?)\s+CHILD\s+(.*)$", line)
            if match:
                for parent in match.group(1).split():
                    for child in match.group(2).split():
                        edges.append((parent, child))
    return jobs, macros, edges


@pytest.mark.parametrize("n_iterations", [1, 2])
def test_terminal_extrinsic_reads_the_final_cip_grid(tmp_path, n_iterations):
    run, dag = _build(tmp_path, n_iterations)
    jobs, macros, edges = _parse(dag)

    extrinsic_nodes = {n for n, s in jobs.items() if s == "ILE_extr.sub"}
    assert extrinsic_nodes, "no ILE_extr nodes in the built DAG"
    read = {macros.get(n, {}).get("macroiteration") for n in extrinsic_nodes}
    read.discard(None)
    assert len(read) == 1, "ILE_extr nodes disagree: {}".format(sorted(read))
    read = read.pop()

    writers = {}
    for node, submit in jobs.items():
        if submit == "refine.sub" or submit.startswith("CIP"):
            index = macros.get(node, {}).get("macroiterationnext")
            if index is not None:
                writers.setdefault(index, set()).add(node)
    assert writers, "no grid-writing nodes in the built DAG"
    last = max(writers, key=int)

    assert read == last, (
        "the terminal ILE reads overlap-grid-{} but the last grid written is "
        "overlap-grid-{}; that is the REFINE proposal grid, not the CIP "
        "posterior that convert turns into posterior_samples-{}.dat".format(
            read, last, last))

    children = {}
    for parent, child in edges:
        children.setdefault(parent, set()).add(child)
    reachable, queue = set(), deque(writers[last])
    while queue:
        for child in children.get(queue.popleft(), ()):
            if child not in reachable:
                reachable.add(child)
                queue.append(child)
    assert extrinsic_nodes <= reachable, (
        "the node writing overlap-grid-{} is not an ancestor of every ILE_extr "
        "node; the extrinsic jobs would race the CIP that writes their "
        "input".format(last))

    logs = os.path.join(run, "iteration_{}_ile".format(read), "logs")
    assert os.path.isdir(logs), (
        "{} does not exist, but ILE_extr.sub names it as initialdir and log "
        "directory; the DAG would build and the jobs would fail on the execute "
        "node".format(logs))
