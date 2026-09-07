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
