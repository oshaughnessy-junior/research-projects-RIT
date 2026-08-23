"""ILE's data-free early exit must not disturb an ordinary invocation (item A12).

PR #181 adds `_run_data_free_early_exit()` at module scope in
`integrate_likelihood_extrinsic_batchmode`, BEFORE the `import cupy` block and
before the RIFT runtime is loaded.  That placement is the point -- the whole
purpose is to emit constant-likelihood shards without touching frames, PSDs or
a GPU -- but it puts new top-level code on the most-used executable in the
project, where Paper 1's JAX work and every production analysis also live.

Two properties matter and neither is obvious from reading:

1. The early exit fires ONLY when the flag is present in argv.  It is guarded
   by a literal `sys.argv` scan rather than by argparse, so a mistake there
   would silently short-circuit ordinary runs.
2. It runs before the heavy imports.  If it were moved after them, the mode
   would still "work" while quietly requiring cupy and the RIFT runtime on a
   worker that was chosen because it has neither.
"""

import ast
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.abspath(os.path.join(HERE, ".."))
ILE = os.path.join(CODE, "bin", "integrate_likelihood_extrinsic_batchmode")


@pytest.fixture(scope="module")
def module_body():
    with open(ILE) as stream:
        return ast.parse(stream.read()).body


def _index_of_call(body, func_name):
    for i, node in enumerate(body):
        if (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and node.value.func.id == func_name):
            return i
    return None


def _index_of_import(body, module_name):
    """First MODULE-SCOPE import of *module_name*.

    Imports inside a function body do not count, and getting that wrong is
    easy: the first draft of this test walked into `_load_hyperpipeline_io`
    and reported that `import RIFT` preceded the early exit.  It does not --
    that import is lazy, which is the whole reason the helper exists.  A
    top-level `try:`/`except ImportError:` block DOES count, because that is
    how the cupy import is written.
    """
    for i, node in enumerate(body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            continue
        candidates = [node]
        if isinstance(node, ast.Try):
            candidates = list(node.body) + [h for hs in node.handlers
                                            for h in hs.body]
        for sub in candidates:
            if isinstance(sub, ast.Import):
                if any(a.name.split(".")[0] == module_name for a in sub.names):
                    return i
            if isinstance(sub, ast.ImportFrom):
                if (sub.module or "").split(".")[0] == module_name:
                    return i
    return None


def test_early_exit_runs_before_the_heavy_imports(module_body):
    call = _index_of_call(module_body, "_run_data_free_early_exit")
    assert call is not None, "_run_data_free_early_exit() is no longer called"
    for heavy in ("cupy", "RIFT", "lalsimulation"):
        where = _index_of_import(module_body, heavy)
        if where is None:
            continue
        assert call < where, (
            "_run_data_free_early_exit() runs after `import {}`; the data-free "
            "mode exists so a worker without that stack can produce shards"
            .format(heavy))


def test_early_exit_is_guarded_on_the_literal_flag():
    """A guard on argparse would fire too late; a wrong guard fires too often."""
    with open(ILE) as stream:
        text = stream.read()
    start = text.index("def _run_data_free_early_exit(")
    body = text[start:text.index("\ndef ", start + 1)]
    assert '"--zero-likelihood-data-free" not in sys.argv' in body, (
        "the early exit is no longer guarded by a literal argv scan")
    assert body.index('not in sys.argv') < body.index("return") + 200


def _run(args, timeout=180):
    env = dict(os.environ)
    env["PYTHONPATH"] = CODE + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run([sys.executable, ILE] + args, env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=timeout)


def test_an_ordinary_invocation_is_not_short_circuited():
    """--help must reach argparse, i.e. the early exit did not fire."""
    result = _run(["--help"])
    assert "--zero-likelihood-data-free" in result.stdout, result.stdout[-2000:]
    assert "Data-free zero likelihood output:" not in result.stdout


def test_the_flag_requires_its_companions_and_says_so():
    """Reachability: the guarded path runs and validates, rather than silently
    doing nothing."""
    result = _run(["--zero-likelihood-data-free"])
    assert result.returncode != 0
    assert "requires --zero-likelihood" in result.stdout, result.stdout[-2000:]
