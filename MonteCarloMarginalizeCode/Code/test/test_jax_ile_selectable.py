"""Pipeline-selectable ILE executable: --use-jax-ile / --ile-exe.

RO'S directive (2026-09-08): util_RIFT_pseudo_pipe.py hard-coded
`` `which integrate_likelihood_extrinsic_batchmode` `` as the ILE executable
handed to create_event_parameter_pipeline_BasicIteration, with no way for a
pipeline builder to name bin/integrate_likelihood_extrinsic_jax instead.
pseudo_pipe now exposes --use-jax-ile (resolves to
`which integrate_likelihood_extrinsic_jax`) and --ile-exe (an explicit path),
threaded through the CEPP's existing --ile-exe option to every ILE/ILE_puff/
ILE_fetch/ILE_extr condor submit file it writes.

These are full subprocess DAG builds against the same reference ini/coinc
fixtures .travis/test-build.sh uses (.travis/ref_ini/GW150914.ini + coinc.xml),
so what is tested is the actual CLI wiring, not a mock of it.  OSG/singularity
are turned off in the ini used here: under --use-osg (without --use-singularity)
write_ILE_sub_simple rewrites the condor "executable" to a fixed OSG wrapper
script (my_wrapper.sh) and carries the real ILE executable inside that
script's body instead, which is orthogonal to the selection wiring under test.
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

CODE = Path(__file__).resolve().parents[1]
BIN = CODE / "bin"
PSEUDO_PIPE = BIN / "util_RIFT_pseudo_pipe.py"
REPO = CODE.parents[1]
REF_INI = REPO / ".travis" / "ref_ini" / "GW150914.ini"
COINC = REPO / ".travis" / "ref_ini" / "coinc.xml"

BATCHMODE_EXE = str((BIN / "integrate_likelihood_extrinsic_batchmode").resolve())
JAX_EXE = str((BIN / "integrate_likelihood_extrinsic_jax").resolve())

pytestmark = pytest.mark.skipif(
    not (REF_INI.exists() and COINC.exists()),
    reason="reference ini/coinc fixtures not present in this checkout")


def _fast_ini(tmp_path):
    """The reference ini with OSG disabled and a tiny initial grid.

    OSG is disabled for the reason in the module docstring.  The grid is
    shrunk from the production value (5000) to keep this a DAG-BUILD test,
    not a several-minute grid-construction benchmark.
    """
    text = REF_INI.read_text()
    for flag in ("use_osg", "use_osg_file_transfer", "use_osg_cip"):
        text = text.replace("{}=True".format(flag), "{}=False".format(flag))
    text = re.sub(r"force-initial-grid-size=\d+", "force-initial-grid-size=4", text)
    out = tmp_path / "ref_fast.ini"
    out.write_text(text)
    return out


def _shim_path_dir(tmp_path):
    """A directory with 'python' -> this interpreter.

    create_event_parameter_pipeline_BasicIteration is invoked by pseudo_pipe
    through `os.system(cmd)` (a bare script name resolved via PATH, run
    through its own `#!/usr/bin/env python` shebang) rather than
    `sys.executable <script>`, so the *shebang's* interpreter needs to be this
    same one -- independent of whatever bare `python` happens to mean on the
    host's PATH.
    """
    shim = tmp_path / "_pyshim"
    shim.mkdir(exist_ok=True)
    link = shim / "python"
    if not link.exists():
        try:
            link.symlink_to(sys.executable)
        except OSError:
            shutil.copy(sys.executable, link)
            os.chmod(link, 0o755)
    return shim


def _env(tmp_path):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(CODE) + os.pathsep + env.get("PYTHONPATH", "")
    shim = _shim_path_dir(tmp_path)
    env["PATH"] = os.pathsep.join([str(shim), str(BIN), env.get("PATH", "")])
    env.setdefault("OMP_NUM_THREADS", "1")
    env["RIFT_LOWLATENCY"] = "True"
    return env


def _build(tmp_path, rundir_name, extra_args):
    ini = _fast_ini(tmp_path)
    cache = tmp_path / "fake.cache"
    cache.write_text("")
    rundir = tmp_path / rundir_name
    cmd = [sys.executable, str(PSEUDO_PIPE),
           "--use-ini", str(ini),
           "--use-coinc", str(COINC),
           "--use-rundir", str(rundir),
           "--fake-data-cache", str(cache)] + list(extra_args)
    out = subprocess.run(cmd, cwd=str(tmp_path), env=_env(tmp_path), text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return out, rundir


def _executable_line(sub_path):
    for line in sub_path.read_text().splitlines():
        if line.strip().startswith("executable"):
            return line
    raise AssertionError("no 'executable' line in {}".format(sub_path))


def test_default_build_uses_batchmode_ile(tmp_path):
    out, rundir = _build(tmp_path, "run_default", [])
    assert out.returncode == 0, out.stdout[-4000:]
    for sub in ("ILE.sub", "ILE_extr.sub"):
        line = _executable_line(rundir / sub)
        assert line.endswith(BATCHMODE_EXE), (sub, line)


def test_use_jax_ile_threads_into_every_ile_stage_sub(tmp_path):
    out, rundir = _build(tmp_path, "run_jax", ["--use-jax-ile"])
    assert out.returncode == 0, out.stdout[-4000:]
    for sub in ("ILE.sub", "ILE_puff.sub", "ILE_extr.sub"):
        line = _executable_line(rundir / sub)
        assert line.endswith(JAX_EXE), (sub, line)
        assert "integrate_likelihood_extrinsic_batchmode" not in line


def test_explicit_ile_exe_path_is_used_verbatim(tmp_path):
    custom_exe = tmp_path / "not_really_ile"
    custom_exe.write_text("#!/bin/sh\nexit 0\n")
    custom_exe.chmod(0o755)
    out, rundir = _build(tmp_path, "run_explicit", ["--ile-exe", str(custom_exe)])
    assert out.returncode == 0, out.stdout[-4000:]
    line = _executable_line(rundir / "ILE.sub")
    assert line.endswith(str(custom_exe)), line


def test_use_jax_ile_with_calmarg_is_refused_at_build_time(tmp_path):
    cal_dir = tmp_path / "cal_env"
    out, _rundir = _build(tmp_path, "run_refused", [
        "--use-jax-ile", "--calmarg-envelope-directory", str(cal_dir)])
    assert out.returncode != 0, "JAX ILE + in-loop calmarg was accepted"
    assert "--use-jax-ile is incompatible with in-loop calibration marginalization" in out.stdout
    assert "--calmarg-envelope-directory" in out.stdout


def test_use_jax_ile_and_ile_exe_are_mutually_exclusive(tmp_path):
    out, _rundir = _build(tmp_path, "run_mutex", [
        "--use-jax-ile", "--ile-exe", "/bin/true"])
    assert out.returncode != 0, "--use-jax-ile + --ile-exe was accepted"
    assert "--use-jax-ile and --ile-exe are mutually exclusive" in out.stdout
