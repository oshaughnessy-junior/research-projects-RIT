"""Real DAG-build regression for --internal-ile-q-time-pregrid-factor.

Companion to test_q_time_pregrid_pipeline.py, whose executable tests stop at
helper_ile_args.txt / args_ile.txt.  Adversarial review of PR #281
(analyses/marginalization_audit_20260908/review/PR281_review.md, MAJOR #1) found
that neither test file drives the flag all the way to a real condor submit file:
create_event_parameter_pipeline_BasicIteration inherits the whole main-iteration
argument string into ILE_extr.sub (`ile_args_extr = ile_args + ...`) and into
ILE_puff.sub, and that inheritance is exactly the kind of link a refactor breaks
silently.  This file drives the real util_RIFT_pseudo_pipe.py -> helper_LDG_Events.py
-> create_event_parameter_pipeline_BasicIteration chain against the same reference
ini/coinc fixtures test_jax_ile_selectable.py (PR #282) uses, and reads the
generated .sub files, rather than mocking any hop of the chain.
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

Q_TIME_PREGRID_FLAG = "--internal-ile-q-time-pregrid-factor"
ILE_Q_TIME_PREGRID_FLAG = "--q-time-pregrid-factor"

pytestmark = pytest.mark.skipif(
    not (REF_INI.exists() and COINC.exists()),
    reason="reference ini/coinc fixtures not present in this checkout")


def _fast_ini(tmp_path):
    """The reference ini with OSG disabled and a tiny initial grid.

    Copied from test_jax_ile_selectable.py's _fast_ini: OSG is disabled so this stays
    a DAG-BUILD test rather than an OSG-submission test, and the grid is shrunk from
    the production value (5000) so this runs in seconds, not minutes.
    """
    text = REF_INI.read_text()
    for flag in ("use_osg", "use_osg_file_transfer", "use_osg_cip"):
        text = text.replace("{}=True".format(flag), "{}=False".format(flag))
    text = re.sub(r"force-initial-grid-size=\d+", "force-initial-grid-size=4", text)
    out = tmp_path / "ref_fast.ini"
    out.write_text(text)
    return out


def _shim_path_dir(tmp_path):
    """A directory with 'python' -> this interpreter, for CEPP's os.system(...) hop.

    Copied from test_jax_ile_selectable.py: create_event_parameter_pipeline_BasicIteration
    is invoked by pseudo_pipe through os.system(cmd), a bare script name resolved via
    PATH and run through its own '#!/usr/bin/env python' shebang, so the shebang's
    interpreter needs to be this same one -- independent of whatever bare 'python'
    happens to mean on the host's PATH.
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


def _sub_text(rundir, name):
    path = rundir / name
    assert path.exists(), "%s was not generated" % name
    return path.read_text()


def test_default_build_emits_no_q_time_pregrid_flag(tmp_path):
    """Baseline: with the pipeline option unset, no .sub file mentions the pregrid
    at all, so the default campaign is unchanged by this option existing."""
    out, rundir = _build(tmp_path, "run_default", [])
    assert out.returncode == 0, out.stdout[-4000:]
    for name in ("ILE.sub", "ILE_extr.sub", "ILE_puff.sub"):
        assert ILE_Q_TIME_PREGRID_FLAG not in _sub_text(rundir, name), name


def test_q_time_pregrid_factor_8_reaches_every_ile_stage_sub(tmp_path):
    """The load-bearing regression (PR #281 review, MAJOR #1).  GW150914.ini's own
    [rift-pseudo-pipe] block already carries ile-force-gpu=True, and pseudo_pipe
    unconditionally passes --propose-fit-strategy --propose-ile-convergence-options
    to the helper, which is what actually emits --vectorized --gpu -- so no extra
    flags are needed to satisfy factor 8's --vectorized prerequisite here.  The ini
    sets no --rotation-slow, --freqresponse, or calibration marginalization, so
    nothing excludes it either."""
    out, rundir = _build(tmp_path, "run_q8", [Q_TIME_PREGRID_FLAG, "8"])
    assert out.returncode == 0, out.stdout[-4000:]
    for name in ("ILE.sub", "ILE_extr.sub", "ILE_puff.sub"):
        text = _sub_text(rundir, name)
        assert ILE_Q_TIME_PREGRID_FLAG + " 8" in text, (name, text)


def test_q_time_pregrid_factor_8_with_calmarg_is_refused_at_build_time(tmp_path):
    """Exercises the exclusion side against the real DAG builder: in-loop calibration
    marginalization is added by pseudo_pipe itself (before the helper ever runs), so
    this must be refused before any .sub file is written."""
    cal_dir = tmp_path / "cal_env"
    out, rundir = _build(tmp_path, "run_q8_calmarg", [
        Q_TIME_PREGRID_FLAG, "8", "--calmarg-envelope-directory", str(cal_dir)])
    assert out.returncode != 0, "q-time-pregrid-factor 8 + in-loop calmarg was accepted"
    assert not rundir.exists(), "run directory was created despite the refusal"
