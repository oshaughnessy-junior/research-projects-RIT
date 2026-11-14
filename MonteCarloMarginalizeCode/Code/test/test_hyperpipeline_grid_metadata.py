"""The ASCII grid format must carry the settings its columns do not.

A hyperpipeline grid records the intrinsic point, and nothing else.  The
ligolw sim_inspiral table it replaces also carries the waveform-generation
settings -- amplitude and phase PN order, tapering, fmin/fref -- and dropping
them is not neutral: a fresh ChooseWaveformParams defaults `ampO` to 0, which
generates only the (2,+-2) modes, so ILE fails every point with
`KeyError: (2, -1)` and exits 0.

These tests pin the metadata round trip and, just as importantly, the warning
when a grid carries no metadata at all -- because grids written before this
existed will keep arriving, and silently adopting defaults is how the problem
stayed invisible.
"""

import os
import sys
import warnings

import numpy as np
import pytest

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))

from RIFT.misc import hyperpipeline_io as hpio


class FakeP(object):
    """Minimal stand-in with the attributes the metadata block covers."""

    def __init__(self):
        self.lnL = 0.0
        self.m1 = 0.0
        self.m2 = 0.0
        self.ampO = 0          # the default that caused the failure
        self.phaseO = 7
        self.fmin = 40.0
        self.fref = 0.0
        self.taper = 0
        self.radec = False
        self.approx = 84

    def assign_param(self, name, value):
        setattr(self, name, value)


def _write(tmp_path, meta_source):
    path = str(tmp_path / "grid.dat")
    hpio.write_table_with_metadata(
        path, ("lnL", "sigma_lnL", "m1", "m2"),
        [[0.0, 0.0, 30.0, 20.0], [0.0, 0.0, 31.0, 21.0]], P=meta_source)
    return path


def test_metadata_round_trips(tmp_path):
    source = FakeP()
    source.ampO = -1
    source.taper = "TAPER_START"
    source.radec = True
    source.fmin = 35.0
    path = _write(tmp_path, source)

    meta = hpio.parse_metadata(path)
    assert meta["ampO"] == "-1"
    assert meta["taper"] == "TAPER_START"

    with warnings.catch_warnings():
        warnings.simplefilter("error")   # no warning expected: metadata present
        P_list, _ = hpio.read_grid_to_P_list(path, P_factory=FakeP)
    assert P_list[0].ampO == -1, "amplitude order was not restored"
    assert P_list[0].taper == "TAPER_START"
    assert P_list[0].radec is True
    assert P_list[0].fmin == 35.0
    assert len(P_list) == 2


def test_a_grid_without_metadata_warns(tmp_path):
    """Silence here is what made the original defect invisible."""
    path = str(tmp_path / "legacy.dat")
    hpio.write_table(path, ("lnL", "sigma_lnL", "m1", "m2"),
                     [[0.0, 0.0, 30.0, 20.0]])
    assert hpio.parse_metadata(path) == {}
    with pytest.warns(UserWarning, match="ampO"):
        P_list, _ = hpio.read_grid_to_P_list(path, P_factory=FakeP)
    # Behaviour is unchanged for such a grid -- only now it says so.
    assert P_list[0].ampO == 0


def test_metadata_does_not_disturb_the_header_or_the_sniff(tmp_path):
    source = FakeP(); source.ampO = -1
    path = _write(tmp_path, source)
    assert hpio.sniff(path) is True
    assert hpio.read_header(path) == ("lnL", "sigma_lnL", "m1", "m2")
    table, columns = hpio.read_table(path)
    assert columns == ("lnL", "sigma_lnL", "m1", "m2")
    assert np.allclose(np.atleast_1d(table)["m1"], [30.0, 31.0])


def test_unreadable_metadata_warns_and_does_not_abort(tmp_path):
    path = str(tmp_path / "bad.dat")
    hpio.write_table(path, ("lnL", "sigma_lnL", "m1", "m2"),
                     [[0.0, 0.0, 30.0, 20.0]])
    with open(path) as stream:
        body = stream.read()
    with open(path, "w") as stream:
        stream.write("# " + hpio.META_MAGIC + " ampO=not-a-number\n" + body)
    with pytest.warns(UserWarning):
        P_list, _ = hpio.read_grid_to_P_list(path, P_factory=FakeP)
    assert P_list[0].ampO == 0


@pytest.mark.parametrize("active", ("", "1"))
@pytest.mark.parametrize("existing_metrics", (False, True))
@pytest.mark.parametrize("log_res,offset,sigma", ((-10.0, 7.0, 0.125),
                                                (-10.0, 70.0, 0.125),
                                                (-10.0, 7.0, 0.0)))
def test_actual_ile_auxiliary_grid_writer_keeps_uncertainty_separate(
        tmp_path, monkeypatch, active, existing_metrics, log_res, offset, sigma):
    """Run the real post-integration writer, not ILE or a stand-in estimator."""
    import ast
    import copy
    from pathlib import Path
    from types import SimpleNamespace

    driver = Path(__file__).resolve().parents[1] / "bin" / "integrate_likelihood_extrinsic_batchmode"
    tree = ast.parse(driver.read_text())
    condition = ast.dump(ast.parse("opts.output_file and opts.sim_grid", mode="eval").body)
    blocks = [node for node in ast.walk(tree)
              if isinstance(node, ast.If) and ast.dump(node.test) == condition]
    assert len(blocks) == 1, "the actual auxiliary-grid writer must be identified uniquely"
    writer = ast.fix_missing_locations(ast.Module(
        body=[copy.deepcopy(blocks[0])], type_ignores=[]))

    params = [("m1", float), ("m2", float), ("extra_coordinate", float)]
    fields = ([("lnL", float), ("sigma_lnL", float)] if existing_metrics else []) + params
    grid = np.zeros(3, dtype=fields)
    grid["m1"] = [30., 31., 32.]
    grid["m2"] = [20., 21., 22.]
    grid["extra_coordinate"] = [-0.2, 0.3, 0.4]
    if existing_metrics:
        grid["lnL"] = 91.
        grid["sigma_lnL"] = 92.
    before = grid.copy()
    output = tmp_path / "marginal"
    monkeypatch.setenv("RIFT_HYPERPIPELINE_FORMAT", active)
    namespace = dict(np=np, opts=SimpleNamespace(output_file=str(output), sim_grid="input.grid"),
                     grid_in=grid, indx_event=1, log_res=log_res,
                     manual_avoid_overflow_logarithm=offset, sqrt_var_over_res=sigma)
    exec(compile(writer, str(driver), "exec"), namespace)

    # The oracle is the independently supplied uncertainty, not the shifted lnL.
    path = tmp_path / "marginal_1_.grid"
    table = np.atleast_1d(np.genfromtxt(path, names=True))
    expected_names = tuple(fields[i][0] for i in range(len(fields)))
    if not existing_metrics:
        expected_names += ("lnL", "sigma_lnL")
    assert table.dtype.names == expected_names
    assert len(table) == 1
    assert table["lnL"][0] == log_res + offset
    assert table["sigma_lnL"][0] == sigma
    for name, _ in params:
        assert table[name][0] == before[name][1]
    assert np.array_equal(grid, before), "reporting must not mutate the input grid"
