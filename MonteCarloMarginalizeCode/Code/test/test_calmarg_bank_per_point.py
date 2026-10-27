"""--calibration-bank-per-point: a fresh calibration bank for each intrinsic point.

1. The driver's own helper (lifted from the driver source): two points get different
   banks, the same (seed, point) reproduces its bank exactly, a different seed changes
   it, and the size resets to the starting --calibration-n-realizations.  Prior and
   export (node-retaining) branches both.
2. With the option off the helper is unreachable: its only call is guarded by the flag.
3. End to end: two intrinsic points in one job with the option on report two different
   banks, and each point still exports its own fair draws.
"""

import ast
import os
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

DRIVER = Path(__file__).resolve().parents[1] / "bin" / "integrate_likelihood_extrinsic_batchmode"


@pytest.fixture(autouse=True)
def _restore_seed_state():
    """seed_everything sets process-global RNG state; leave it as we found it."""
    from RIFT.integrators import seeding
    saved = (seeding._seed_used, dict(seeding._stream_counters), np.random.get_state())
    yield
    np.random.set_state(saved[2])
    seeding._seed_used = saved[0]
    seeding._stream_counters.clear(); seeding._stream_counters.update(saved[1])


def _envelope(path):
    f = np.geomspace(10, 1024, 20)
    one, zero = np.ones_like(f), np.zeros_like(f)
    np.savetxt(path, np.column_stack([f, one, zero, one - .05, zero - .03, one + .05, zero + .03]))


def _lift(tmp_path, export, n_initial=4):
    """Exec the driver's _redraw_calibration_bank_for_point in a stand-in namespace."""
    for ifo in ("H1", "L1"):
        _envelope(tmp_path / ("%s.txt" % ifo))
    tree = ast.parse(DRIVER.read_text())
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)
             and n.name == "_redraw_calibration_bank_for_point"]
    assert len(funcs) == 1
    opts = SimpleNamespace(calibration_export_posterior=export, calibration_proposal_breadcrumb=None,
                           calibration_envelope_directory=str(tmp_path), fmin_template=20,
                           calibration_spline_count=4, calibration_n_realizations=3 * n_initial)
    ns = dict(np=np, opts=opts, P=SimpleNamespace(deltaF=1., deltaT=1 / 256.), fmax=120,
              cal_fmin_ifo={"H1": 20, "L1": 20},
              calibration_correction_types={"H1": "template", "L1": "template"},
              calibration_current_prior=None, calibration_realization_dict={},
              calibration_nodes=None, calibration_log_weights=None,
              _calibration_n_realizations_initial=n_initial)
    exec(compile(ast.Module(body=funcs, type_ignores=[]), str(DRIVER), "exec"), ns)
    return ns


def _bank(ns, seed, point):
    from RIFT.integrators.seeding import seed_everything
    seed_everything(seed, verbose=False)
    ns["_redraw_calibration_bank_for_point"](point, {"H1": None, "L1": None})
    return ({k: v.copy() for k, v in ns["calibration_realization_dict"].items()},
            None if ns["calibration_nodes"] is None else ns["calibration_nodes"].copy())


@pytest.mark.parametrize("export", [False, True])
def test_banks_differ_between_points_and_reproduce(tmp_path, export):
    ns = _lift(tmp_path, export)
    a7, n7 = _bank(ns, 11, 7)
    assert ns["opts"].calibration_n_realizations == 4      # reset from the grown 12
    for ifo in ("H1", "L1"):
        assert a7[ifo].shape == (256, 4)
    a8, _ = _bank(ns, 11, 8)
    a7_again, n7_again = _bank(ns, 11, 7)
    a7_other_seed, _ = _bank(ns, 12, 7)
    for ifo in ("H1", "L1"):
        assert not np.allclose(a7[ifo], a8[ifo])
        np.testing.assert_array_equal(a7[ifo], a7_again[ifo])
        assert not np.allclose(a7[ifo], a7_other_seed[ifo])
        # realizations within one bank are distinct draws
        assert not np.allclose(a7[ifo][:, 0], a7[ifo][:, 1])
    if export:
        assert n7.shape == (4, 2 * 2 * 4)
        np.testing.assert_array_equal(n7, n7_again)
    else:
        assert n7 is None


def test_breadcrumb_branch_redraws_from_the_proposal(tmp_path, monkeypatch):
    """--calibration-proposal-breadcrumb path: realizations AND importance weights are
    replaced per point, and reproduce."""
    import RIFT.calmarg.breadcrumbs as bcmod
    from RIFT.calmarg import generate_realizations as gr
    ns = _lift(tmp_path, export=True)
    prior = gr.prior_cal_breadcrumb_dict(str(tmp_path), ["H1", "L1"], 20, 120, 4)
    monkeypatch.setattr(bcmod, "load", lambda path: {"cal": prior})
    ns["opts"].calibration_proposal_breadcrumb = "unused.npz"
    ns["calibration_current_prior"] = prior
    a7, n7 = _bank(ns, 11, 7)
    w7 = np.array(ns["calibration_log_weights"])
    a8, n8 = _bank(ns, 11, 8)
    a7b, n7b = _bank(ns, 11, 7)
    assert w7.shape == (4,)
    np.testing.assert_allclose(w7, 0, atol=1e-10)   # proposal == prior
    for ifo in ("H1", "L1"):
        assert a7[ifo].shape == (256, 4)
        assert not np.allclose(a7[ifo], a8[ifo])
        np.testing.assert_array_equal(a7[ifo], a7b[ifo])
    np.testing.assert_array_equal(n7, n7b)
    assert not np.allclose(n7, n8)


def test_the_redraw_runs_only_under_the_flag():
    """Option off => the shared startup bank, untouched: the helper's only call site is
    guarded by opts.calibration_bank_per_point."""
    tree = ast.parse(DRIVER.read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "_redraw_calibration_bank_for_point"]
    assert len(calls) == 1
    guards = [n for n in ast.walk(tree) if isinstance(n, ast.If)
              and any(c is calls[0] for st in n.body for c in ast.walk(st))]
    assert len(guards) == 1, "redraw call must sit directly under one guard"
    assert ast.unparse(guards[0].test) == "calibration_marginalization and opts.calibration_bank_per_point"
    assert not any(c is calls[0] for st in guards[0].orelse for c in ast.walk(st))
    # the point index is the GRID row, not the offset within this job
    assert ast.unparse(calls[0].args[0]) == "opts.event + indx_event"
    opt = re.search(r'add_option\("--calibration-bank-per-point",action=\'store_true\',default=False',
                    DRIVER.read_text())
    assert opt, "--calibration-bank-per-point must default to off"


def test_two_points_get_different_banks_end_to_end(tmp_path):
    import test_calexport_multievent as tcm
    d = tcm._make_fixture(tmp_path)
    proc = tcm._run_driver(d, "--calibration-bank-per-point")
    assert proc.returncode == 0, proc.stdout[-4000:]
    lines = [ln for ln in proc.stdout.splitlines() if "[calmarg bank]" in ln]
    assert len(lines) == tcm.N_POINTS, proc.stdout[-4000:]
    assert "point 0:" in lines[0] and "point 1:" in lines[1], lines
    sums = [ln.rsplit("checksum", 1)[1] for ln in lines]
    assert sums[0] != sums[1], lines
    tcm._check_exports(d["outdir"], proc)
    # The same grid row analyzed alone (--event 1, one point) starts from the same bank.
    (tmp_path / "single").mkdir()
    d1 = tcm._make_fixture(tmp_path / "single")
    proc1 = tcm._run_driver(d1, "--calibration-bank-per-point", "--event", "1",
                            "--n-events-to-analyze", "1")
    assert proc1.returncode == 0, proc1.stdout[-4000:]
    single = [ln for ln in proc1.stdout.splitlines() if "[calmarg bank]" in ln]
    assert len(single) == 1 and "point 1:" in single[0], single
    assert single[0].rsplit("checksum", 1)[1] == sums[1], (single, lines)
