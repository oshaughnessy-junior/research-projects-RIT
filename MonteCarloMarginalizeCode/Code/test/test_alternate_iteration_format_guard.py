"""AlternateIteration must reject the Hyperpipe ASCII grid protocol.

The legacy builder hard-codes XML filenames and XML readers.  Treating an
ASCII grid as XML fails late and cryptically, after pseudo-pipe has already
performed event setup.  Until that driver is deliberately ported, both its
direct entry point and pseudo-pipe routing must reject the format opt-in.
"""

import os
from pathlib import Path
import subprocess
import sys


HERE = Path(__file__).resolve()
BIN = HERE.parents[1] / "bin"
ALTERNATE = BIN / "create_event_parameter_pipeline_AlternateIteration"
PSEUDO = BIN / "util_RIFT_pseudo_pipe.py"
MESSAGE = "AlternateIteration does not support RIFT_HYPERPIPELINE_FORMAT"


def _format_environment():
    env = os.environ.copy()
    env["RIFT_HYPERPIPELINE_FORMAT"] = "1"
    return env


def test_direct_alternate_entry_point_rejects_ascii_before_importing_rift():
    result = subprocess.run(
        [sys.executable, str(ALTERNATE), "--help"],
        env=_format_environment(), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert result.returncode != 0
    assert "RIFT_HYPERPIPELINE_FORMAT is unsupported" in result.stdout
    assert "XML-only" in result.stdout
    assert "Traceback" not in result.stdout


def test_pseudo_pipe_rejects_explicit_alternate_routing():
    result = subprocess.run(
        [sys.executable, str(PSEUDO),
         "--pipeline-builder", "AlternateIteration"],
        env=_format_environment(), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert result.returncode != 0
    assert MESSAGE in result.stdout
    assert "XML-only" in result.stdout
    assert "Pipeline builder:" not in result.stdout


def test_pseudo_pipe_rejects_implicit_subdag_routing():
    result = subprocess.run(
        [sys.executable, str(PSEUDO), "--use-subdags"],
        env=_format_environment(), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert result.returncode != 0
    assert MESSAGE in result.stdout
    assert "Pipeline builder:" not in result.stdout
