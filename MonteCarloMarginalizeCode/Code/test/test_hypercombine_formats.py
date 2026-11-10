"""util_HyperCombine must not silently mix on-disk formats (item A10).

PR #181 made `util_HyperCombine.py` detect the self-describing hyperpipeline
format from its first input and then require every other input to match, and
to emit the two-line hyperpipeline header rather than echoing the first input's
first comment line.

Mixing matters because the two layouts put the likelihood in different columns:
hyperpipeline is `lnL sigma_lnL m1 m2 ...` while the legacy composite is
positional with lnL late.  Combining one of each does not fail -- it produces a
file whose rows disagree about what column zero means, and every consumer
downstream reads it happily.

The legacy-only path must stay byte-identical, because that is what existing
EOS/population runs use.
"""

import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
EXE = os.path.join(CODE, "bin", "util_HyperCombine.py")


def _run(args, cwd):
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run([sys.executable, EXE] + args, cwd=str(cwd), env=env,
                          text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=300)


def _write_hyperpipe(path, rows):
    with open(path, "w") as stream:
        stream.write("# RIFT_HYPERPIPELINE_V1\n")
        stream.write("# lnL sigma_lnL m1 m2\n")
        for row in rows:
            stream.write(" ".join("{:.18e}".format(v) for v in row) + "\n")


def _write_legacy(path, rows):
    with open(path, "w") as stream:
        stream.write("# m1 m2 lnL sigma_lnL\n")
        for row in rows:
            stream.write(" ".join("{:.18e}".format(v) for v in row) + "\n")


def test_mixed_formats_are_rejected(tmp_path):
    a = tmp_path / "a.dat"; _write_hyperpipe(a, [[1.0, 0.1, 30.0, 20.0]])
    b = tmp_path / "b.dat"; _write_legacy(b, [[30.0, 20.0, 1.0, 0.1]])
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode != 0, (
        "mixing a hyperpipeline input with a legacy one was accepted; the two "
        "layouts disagree about which column is lnL:\n" + result.stdout[:2000])
    assert "mixed" in (result.stderr + result.stdout).lower()


def test_column_mismatch_between_hyperpipe_inputs_is_rejected(tmp_path):
    a = tmp_path / "a.dat"; _write_hyperpipe(a, [[1.0, 0.1, 30.0, 20.0]])
    b = tmp_path / "b.dat"
    with open(b, "w") as stream:
        stream.write("# RIFT_HYPERPIPELINE_V1\n# lnL sigma_lnL m1 m2 s1z\n")
        stream.write(" ".join(["1.0", "0.1", "30.0", "20.0", "0.5"]) + "\n")
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode != 0
    assert "column mismatch" in (result.stderr + result.stdout).lower()


def test_hyperpipe_only_emits_the_self_describing_header(tmp_path):
    a = tmp_path / "a.dat"; _write_hyperpipe(a, [[1.0, 0.1, 30.0, 20.0]])
    b = tmp_path / "b.dat"; _write_hyperpipe(b, [[2.0, 0.2, 31.0, 21.0]])
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    assert lines[0].strip() == "# RIFT_HYPERPIPELINE_V1", lines[:3]
    assert lines[1].strip() == "# lnL sigma_lnL m1 m2", lines[:3]


def test_legacy_only_still_echoes_its_own_header(tmp_path):
    """The path existing EOS/population runs use must not change."""
    a = tmp_path / "a.dat"; _write_legacy(a, [[30.0, 20.0, 1.0, 0.1]])
    b = tmp_path / "b.dat"; _write_legacy(b, [[31.0, 21.0, 2.0, 0.2]])
    result = _run([str(a), str(b)], tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    assert lines[0].strip() == "# m1 m2 lnL sigma_lnL", lines[:3]
    assert "RIFT_HYPERPIPELINE_V1" not in result.stdout


def test_empty_first_shard_does_not_disable_the_format_contract(tmp_path):
    """Format detection keys off the first READABLE input, not argv[1].

    An empty first shard (a failed worker) used to make the merge fall back
    to the legacy path: mixed-input and column-mismatch checks silently
    disabled, and the output emitted with no header at all -- downstream
    sniffs it as legacy and column zero changes meaning.
    """
    dead = tmp_path / "dead.dat"; dead.touch()
    b = tmp_path / "b.dat"; _write_hyperpipe(b, [[2.0, 0.2, 31.0, 21.0]])
    result = _run([str(dead), str(b)], tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    assert lines[0].strip() == "# RIFT_HYPERPIPELINE_V1", lines[:3]
    assert lines[1].strip() == "# lnL sigma_lnL m1 m2", lines[:3]

    legacy = tmp_path / "c.dat"; _write_legacy(legacy, [[30.0, 20.0, 1.0, 0.1]])
    mixed = _run([str(dead), str(b), str(legacy)], tmp_path)
    assert mixed.returncode != 0, (
        "an empty first shard disabled the mixed-format check:\n"
        + mixed.stdout[:2000])


def test_no_readable_inputs_is_an_error_not_an_empty_success(tmp_path):
    dead = tmp_path / "dead.dat"; dead.touch()
    result = _run([str(dead), str(tmp_path / "absent.dat")], tmp_path)
    assert result.returncode != 0, (
        "a merge with zero readable inputs exited 0 with empty output:\n"
        + result.stdout[:500])


# The fixture workers implement the two public MARG interfaces. They echo every
# parsed grid coordinate, exercising ASCII serialization on both sides of the
# generated per-event consolidation and product scripts.
_EXACT_WORKER = """#!/usr/bin/env python
import argparse
from pathlib import Path
import numpy as np
from RIFT.misc import hyperpipeline_io as hp
p=argparse.ArgumentParser()
for flag in ('sim-grid','using-eos','output-file','fname-output-integral',
             'fname-output-samples','outdir','fname'):
    p.add_argument('--'+flag)
p.add_argument('--event',type=int,default=0)
p.add_argument('--using-eos-index',type=int,default=0)
p.add_argument('--n-events-to-analyze',type=int,default=1)
p.add_argument('--log-factor',type=float,default=0)
o,_=p.parse_known_args()
if not(o.sim_grid or o.using_eos):
    source=o.fname
    target=o.fname_output_samples+'.dat'
    Path(target).parent.mkdir(parents=True,exist_ok=True)
    Path(target).write_text(Path(source).read_text())
else:
    source=o.sim_grid or o.using_eos.removeprefix('file:')
    a,cols=hp.read_table(source)
    start=o.event if o.sim_grid else o.using_eos_index
    rows=a[start:start+o.n_events_to_analyze]
    if o.sim_grid:
        for i,row in enumerate(rows):
            vals=[o.log_factor if c=='lnL' else .1 if c=='sigma_lnL' else row[c] for c in cols]
            hp.write_row(o.output_file+'_'+str(i)+'_.dat',cols,vals)
    else:
        rows=rows.copy()
        rows['lnL']=o.log_factor
        rows['sigma_lnL']=.1
        data=np.column_stack([rows[c] for c in cols])
        np.savetxt(o.fname_output_integral+'+annotation.dat',data,header=' '.join(cols))
"""


def _exact_workflow(tmp_path, protocols):
    import importlib.util
    import json
    from pathlib import Path
    import numpy as np

    code = Path(CODE)
    gate_path = code / "test/hyperpipe/integration/run_constant_likelihood_pipeline_gate.py"
    spec = importlib.util.spec_from_file_location("_grid_product_gate", str(gate_path))
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    tmp_path.mkdir(parents=True, exist_ok=True)
    worker = tmp_path / "factor.py"
    worker.write_text(_EXACT_WORKER)
    worker.chmod(0o755)
    # The first pair collide under BOTH historical rounding precisions. Their
    # likelihoods and physical identities must remain separate.
    xs = np.array([.12345678, .12345679, .87654321])
    seed = tmp_path / "seed.dat"
    np.savetxt(seed, np.column_stack([np.zeros(3), np.zeros(3), xs]),
               header="RIFT_HYPERPIPELINE_V1\nlnL sigma_lnL x")
    manifest = tmp_path / "marg.json"
    manifest.write_text(json.dumps([
        dict(name="event"+str(i), protocol=protocol, exe=str(worker),
             args="--log-factor "+str(i+2), n_chunk=2)
        for i, protocol in enumerate(protocols)]))
    args = tmp_path / "post.txt"
    args.write_text("X\n")
    run = tmp_path / "run"
    run.mkdir()
    env = gate._environment(tmp_path)
    cmd = [sys.executable, os.environ.get("RIFT_TRANSPORT_BUILDER", str(code / "bin/create_eos_posterior_pipeline")),
           "--working-directory", str(run), "--input-grid", str(seed),
           "--marg-job-spec-file", str(manifest),
           "--eos-post-exe", str(worker), "--eos-post-args", str(args),
           "--eos-post-explode-jobs", "1", "--eos-post-explode-jobs-last", "1",
           "--n-iterations", "1", "--n-samples-per-job", "3",
           "--use-full-submit-paths"]
    result = subprocess.run(cmd, cwd=str(run), env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
    (tmp_path / "build.log").write_text(result.stdout)
    assert result.returncode == 0, result.stdout[-6000:]
    gate._execute_without_condor(run, env)
    return run, env, xs


@pytest.mark.parametrize("protocols", [
    ("indexed-grid-v1", "hyperpipe-v1"),
    ("hyperpipe-v1", "indexed-grid-v1"),
    ("indexed-grid-v1", "indexed-grid-v1"),
    ("hyperpipe-v1", "hyperpipe-v1"),
])
def test_generated_manifest_product_preserves_grid_identity(tmp_path, protocols):
    import numpy as np
    run, _env, xs = _exact_workflow(tmp_path, protocols)
    # Check the actual product, plus the downstream posterior serialization.
    for path in (run / "consolidated_0.net_marg", run / "grid-1.dat"):
        data = np.loadtxt(path, ndmin=2)
        assert data.shape == (3, 3), (path, data)
        np.testing.assert_array_equal(np.sort(data[:, 2]), np.sort(xs))
        np.testing.assert_array_equal(data[:, 0], 5.)
    for i in range(2):
        data = np.loadtxt(run / "iteration_0_marg" /
                          ("consolidated_0_"+str(i)+".net_marg"), ndmin=2)
        assert len(data) == 3
        np.testing.assert_array_equal(np.sort(data[:, 2]), np.sort(xs))


@pytest.mark.parametrize("damage", ["missing_file", "missing_row", "duplicate_row", "changed_identity"])
def test_generated_manifest_product_refuses_incomplete_event_factors(tmp_path, damage):
    import numpy as np
    run, env, _xs = _exact_workflow(tmp_path, ("indexed-grid-v1", "hyperpipe-v1"))
    path = run / "iteration_0_marg/consolidated_0_1.net_marg"
    rows = np.loadtxt(path, ndmin=2)
    if damage == "missing_file":
        path.unlink()
    else:
        if damage == "missing_row":
            rows = rows[:-1]
        elif damage == "duplicate_row":
            rows = np.vstack([rows, rows[0]])
        else:
            rows[0, 2] = np.nextafter(rows[0, 2], np.inf)
        np.savetxt(path, rows, header="RIFT_HYPERPIPELINE_V1\nlnL sigma_lnL x")
    result = subprocess.run(["bash", str(run / "con_prod.sh"), "0"], cwd=str(run),
                            env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=120)
    assert result.returncode != 0, (damage, result.stdout, result.stderr)
    assert "strict product" in result.stderr, result.stderr
    # Refusal must precede any apparently valid product rows.
    assert (run / "consolidated_0.net_marg").read_text() == ""


def test_strict_product_retains_zero_eos_event_factor(tmp_path):
    import numpy as np
    a = tmp_path / "a.dat"
    b = tmp_path / "b.dat"
    _write_hyperpipe(a, [[-np.inf, 0., 30., 20.]])
    _write_hyperpipe(b, [[3., .1, 30., 20.]])
    result = _run(["--combination", "product", "--preserve-intrinsic-values",
                   "--expected-inputs", "2", str(a), str(b)], tmp_path)
    assert result.returncode == 0, result.stderr
    data = np.loadtxt(result.stdout.splitlines(), ndmin=2)
    assert data.shape == (1, 4) and np.isneginf(data[0, 0])


def test_strict_product_requires_distinct_input_files(tmp_path):
    a = tmp_path / "a.dat"
    _write_hyperpipe(a, [[2., .1, 30., 20.]])
    result = _run(["--combination", "product", "--expected-inputs", "2",
                   str(a), str(a)], tmp_path)
    assert result.returncode != 0 and "distinct input" in result.stderr



@pytest.mark.parametrize("malformed", [False, True])
def test_strict_product_checks_declared_header_width(tmp_path, malformed):
    a = tmp_path / "a.dat"
    b = tmp_path / "b.dat"
    row = [2., .1, 30.] if malformed else [2., .1, 30., 20.]
    _write_hyperpipe(a, [row])
    _write_hyperpipe(b, [row])
    result = _run(["--combination", "product", "--preserve-intrinsic-values",
                   "--expected-inputs", "2", str(a), str(b)], tmp_path)
    if malformed:
        assert result.returncode != 0 and "row/header column mismatch" in result.stderr
        assert result.stdout == ""
    else:
        import numpy as np
        assert result.returncode == 0, result.stderr
        data = np.loadtxt(result.stdout.splitlines(), ndmin=2)
        assert data.shape == (1, 4) and data[0, 0] == 4.
