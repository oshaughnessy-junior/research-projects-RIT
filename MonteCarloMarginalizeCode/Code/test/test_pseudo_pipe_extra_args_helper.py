"""--extra-args-helper reaches the standard helper_LDG_Events.py call (build only, never submit)."""
import re

import pytest

from test_rf_transverse_pipeline_build import _build, _opt_in_ini


def test_extra_args_helper_reaches_standard_helper(tmp_path):
    pytest.importorskip("lal")
    # Relative path, read after pseudo_pipe has changed into the run directory.
    # A helper-only flag: without it the helper puts --eta-range on every CIP line.
    (tmp_path / "extra_helper.txt").write_text("--no-propose-limits\n")
    ini = _opt_in_ini(tmp_path, extra=['extra-args-helper="extra_helper.txt"'])
    ini.write_text(re.sub(r'(?m)^rf-transverse-spin-coordinates=.*\n', '', ini.read_text()))
    proc, rundir = _build(tmp_path, ini)
    assert proc.returncode == 0, proc.stdout[-3000:]
    lines = (rundir / "helper_cip_arg_list.txt").read_text().splitlines()
    assert lines and not any("--eta-range" in line for line in lines), lines


def test_missing_extra_args_helper_file_fails_before_the_run_directory(tmp_path):
    pytest.importorskip("lal")
    ini = _opt_in_ini(tmp_path, extra=['extra-args-helper="missing.txt"'])
    proc, rundir = _build(tmp_path, ini)
    assert proc.returncode != 0 and "--extra-args-helper file not found" in proc.stdout, proc.stdout[-3000:]
    assert not rundir.exists()
