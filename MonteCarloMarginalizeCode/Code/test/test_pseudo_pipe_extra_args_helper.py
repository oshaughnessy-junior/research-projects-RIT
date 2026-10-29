"""--extra-args-helper reaches the standard helper_LDG_Events.py call (build only, never submit)."""
import re

import pytest

from test_rf_transverse_pipeline_build import _build, _opt_in_ini


def test_extra_args_helper_reaches_standard_helper(tmp_path):
    pytest.importorskip("lal")
    # Relative path, read after pseudo_pipe has changed into the run directory.
    (tmp_path / "extra_helper.txt").write_text("--force-chi-max 0.47\n")
    ini = _opt_in_ini(tmp_path, extra=['extra-args-helper="extra_helper.txt"'])
    ini.write_text(re.sub(r'(?m)^rf-transverse-spin-coordinates=.*\n', '', ini.read_text()))
    proc, rundir = _build(tmp_path, ini)
    assert proc.returncode == 0, proc.stdout[-3000:]
    lines = (rundir / "helper_cip_arg_list.txt").read_text().splitlines()
    assert lines and all("--chi-max 0.47" in line for line in lines), lines
