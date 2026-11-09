"""EOS branch-prior choices and varying NMB hypotheses through the CIP grid API."""

import os
import shlex
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from RIFT.physics import eos_affine
from RIFT.physics.eos_sequence_grid import SequenceGrid
from test_eos_affine_central import (CODE, DRIVER, FakeTwinEOS, _central,
                                     _check_ok, _ile_file, _nmb_fixture)


def test_conditional_branch_prior_is_normalized_at_fixed_mass():
    eos = FakeTwinEOS()
    u = np.array([[.25, .25], [.25, .75], [.75, .25], [.75, .75]])
    mass = np.tile([1.8, 1.7], (4, 1))
    out, logw, branches = _central(eos, mass, u, coord_names=("lambda1", "lambda2"),
                                   branch_prior="uniform-at-mass")
    np.testing.assert_array_equal(logw, 0.)
    assert len(set(map(tuple, branches))) == 4
    likelihood = np.exp(-np.sum(out, axis=1) / 300.)
    direct = sum(np.exp(-(eos.branch_lambda(1.8, b1) + eos.branch_lambda(1.7, b2)) / 300.)
                 for b1 in [0, 1] for b2 in [0, 1]) / 4
    assert likelihood.mean() == pytest.approx(direct)
    raw = _central(eos, mass, u, branch_prior="mass-per-branch")[1]
    np.testing.assert_allclose(raw, np.log(4))
    with pytest.raises(ValueError, match="branch prior"):
        _central(eos, mass, u, branch_prior="unknown")


def test_sequence_grid_selects_unsorted_native_indices_and_reuses_sequence(tmp_path):
    seq = _nmb_fixture(tmp_path)
    grid = tmp_path / "grid.dat"
    rows = np.array([[0, 0, 1.2, 3], [0, 0, .9, 0]])
    np.savetxt(grid, rows, header="lnL sigma_lnL physical_parameter sequence_index")
    reader = SequenceGrid(grid, seq)
    eos = [reader.eos_for_row(row) for row in rows]
    assert [e.index for e in eos] == [3, 0]
    assert [e.number_of_branches for e in eos] == [3, 1]
    assert eos[0]._seq is eos[1]._seq is reader.sequence
    for value in [-1, .2, np.nan, np.inf, 100]:
        with pytest.raises(ValueError):
            reader.eos_for_row([0, 0, 1.2, value])
    np.savetxt(grid, rows, header="lnL sigma_lnL physical_parameter missing_index")
    with pytest.raises(ValueError, match="sequence_index"):
        SequenceGrid(grid, seq)


def _quadrature(reader, row, conditional, tide_sensitive=False):
    eos = reader.eos_for_row(row)
    n = 900
    mc = np.linspace(1.55, 1.70, n + 1)
    d = np.linspace(0, .1, n + 1)
    MC, D = np.meshgrid((mc[:-1] + mc[1:]) / 2, (d[:-1] + d[1:]) / 2, indexing="ij")
    eta = .25 * (1 - D ** 2)
    M = MC / eta ** .6
    masses = [M * (1 + D) / 2, M * (1 - D) / 2]
    counts = []
    for m in masses:
        count = np.zeros_like(m)
        for b in range(eos.number_of_branches):
            count += (m >= eos.branch_minimum_mass_msun(b)) & (m <= eos.branch_maximum_mass_msun(b))
        counts.append(count if not conditional else (count > 0))
    weights = MC * eta ** -1.2
    if tide_sensitive:
        total = np.zeros_like(MC)
        for b1 in range(eos.number_of_branches):
            for b2 in range(eos.number_of_branches):
                supported = ((masses[0] >= eos.branch_minimum_mass_msun(b1)) &
                             (masses[0] <= eos.branch_maximum_mass_msun(b1)) &
                             (masses[1] >= eos.branch_minimum_mass_msun(b2)) &
                             (masses[1] <= eos.branch_maximum_mass_msun(b2)))
                # Independent branch enumeration, bypassing CIP's branch selector and converter.
                recs = eos._seq.branches_of_indx(eos.index)
                l1 = np.exp(np.interp(masses[0], recs[b1]["M"], np.log(recs[b1]["Lambda"])))
                l2 = np.exp(np.interp(masses[1], recs[b2]["M"], np.log(recs[b2]["Lambda"])))
                lt = (8. / 13) * ((1 + 7 * eta - 31 * eta ** 2) * (l1 + l2)
                      + np.sqrt(1 - 4 * eta) * (1 + 9 * eta - 11 * eta ** 2) * (l1 - l2))
                total += supported * np.exp(-.5 * ((MC - 1.62) / .01) ** 2 - .5 * ((lt - 300) / 250) ** 2)
        return float(np.sum(weights * total) / weights.sum())
    return float(np.sum(weights * counts[0] * counts[1]) / weights.sum())


@pytest.mark.parametrize("prior,tide_sensitive", [("mass-per-branch", False), ("uniform-at-mass", False), ("mass-per-branch", True)])
def test_hyperpipe_assembled_marg_runs_varying_nmb_grid(tmp_path, prior, tide_sensitive):
    """Actual assembled marg args, actual CIP chunk loop, real NMB reader and branch sums."""
    from RIFT.hyperpipe.marg_list import assemble_marg_list
    seq = _nmb_fixture(tmp_path)
    _ile_file(tmp_path / "event.net")
    rows = np.array([[0, 0, 0], [0, 0, 1], [0, 0, 3]])
    grid = tmp_path / "grid.dat"
    np.savetxt(grid, rows, header="lnL sigma_lnL sequence_index")
    # Only platform alias and fixed RNG; no fake EOS or likelihood callback.
    (tmp_path / "sitecustomize.py").write_text(
        "import numpy as np\nif not hasattr(np, 'float128'):\n    np.float128 = np.longdouble\nnp.random.seed(13)\n")
    args = ("--input-tides --eos-param nmbseq --eos-sequence-file " + shlex.quote(str(seq)) +
            " --eos-affine-parameter central --eos-branch-prior " + prior +
            " --parameter mc --parameter delta_mc --parameter-implied LambdaTilde"
            " --mc-range '[1.55,1.70]' --eta-range '[0.2475,0.25]' --fit-method quadratic"
            " --n-max 60000 --n-eff 800 --n-chunk 4000 --n-output-samples 100 --no-plots")
    if not tide_sensitive:
        args += " --integrate-prior"
    run = tmp_path / "run"
    run.mkdir()
    assembly = assemble_marg_list({"marg-list": [{"name": "EOS", "exe": DRIVER, "args": args,
                                                  "event-file": str(tmp_path / "event.net"), "n-chunk": 3}]},
                                  base_dir=str(tmp_path), run_dir=str(run))
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([CODE, str(tmp_path)]), OMP_NUM_THREADS="1",
               OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", MPLBACKEND="Agg", MPLCONFIGDIR=str(tmp_path / "mpl"))
    cmd = [sys.executable, assembly.exe_paths[0]] + shlex.split(assembly.args_lines[0]) + [
        "--fname", assembly.event_files[0], "--using-eos", "file:" + str(grid),
        "--using-eos-index", "0", "--n-events-to-analyze", "3", "--chunk-save",
        "--fname-output-integral", "MARG-0", "--fname-output-samples", "samples-0"]
    result = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300)
    _check_ok(result)
    estimates = np.loadtxt(tmp_path / "MARG-0+annotation.dat")
    np.testing.assert_array_equal(estimates[:, 2:], rows[:, 2:])
    reader = SequenceGrid(grid, seq)
    # The normalized mass prior gives unit integral on the fully supported single branch.
    # Ratios remove the driver's common prior scale and test branch sums independently.
    expected = np.array([_quadrature(reader, r, prior == "uniform-at-mass", tide_sensitive) for r in rows])
    observed = np.exp(estimates[:, 0] - estimates[0, 0])
    np.testing.assert_allclose(observed, expected / expected[0], rtol=.06)
    if prior == "mass-per-branch" and not tide_sensitive:
        assert observed[1] > 1.05 and observed[2] > observed[1]
    # A separate N=1 call uses the identical EOS selection convention as a chunk.
    one = list(cmd)
    one[one.index("--using-eos-index") + 1] = "1"
    one[one.index("--n-events-to-analyze") + 1] = "1"
    one.remove("--chunk-save")
    one[one.index("--fname-output-integral") + 1] = "single-1"
    one[one.index("--fname-output-samples") + 1] = "single-samples-1"
    _check_ok(subprocess.run(one, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300))
    single = np.loadtxt(tmp_path / "single-1+annotation.dat")
    assert abs(single[0] - estimates[1, 0]) < .08
    np.testing.assert_array_equal(single[2:], [1])


def test_branch_prior_cli_refuses_inactive_mode(tmp_path):
    result = subprocess.run([sys.executable, DRIVER, "--fname", "unused.dat", "--eos-branch-prior", "uniform-at-mass"],
                            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=CODE, MPLBACKEND="Agg"),
                            capture_output=True, text=True, timeout=60)
    assert result.returncode != 0
    assert "requires --eos-affine-parameter central" in result.stderr
