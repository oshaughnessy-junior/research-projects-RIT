"""The JAX ILE driver as a Hyperpipe MARG worker (indexed-grid-v1 contract).

A Hyperpipe MARG job hands the worker a self-describing grid via --sim-grid and
consolidates its per-event .dat shards with hyperpipeline_io.  These tests run
the driver's own load_templates and write_dat on those formats.
"""
import importlib.machinery
import importlib.util
import pathlib
import subprocess
import sys
from types import SimpleNamespace

import numpy as np
import pytest

CODE = pathlib.Path(__file__).parents[2]


def _load_driver():
    path = CODE / "bin" / "integrate_likelihood_extrinsic_jax"
    loader = importlib.machinery.SourceFileLoader("_jax_hp_marg_driver", str(path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def drv():
    return _load_driver()


def _write_grid(drv, path, masses):
    from RIFT.misc import hyperpipeline_io
    P_list = []
    for m1, m2, s1z in masses:
        P = drv.lalsimutils.ChooseWaveformParams()
        P.m1 = m1 * drv.MSUN
        P.m2 = m2 * drv.MSUN
        P.s1z = s1z
        P.ampO = -1
        P_list.append(P)
    cols = ("lnL", "sigma_lnL", "m1", "m2", "a1x", "a1y", "a1z",
            "a2x", "a2y", "a2z")
    hyperpipeline_io.write_grid_from_P_list(str(path), P_list, cols,
                                            lal_module=drv.lal,
                                            lalsimutils_module=drv.lalsimutils)
    assert hyperpipeline_io.sniff(str(path))


def _template_opts(path, event, n):
    return SimpleNamespace(sim_xml=None, sim_grid=str(path), event=event,
                           n_events_to_analyze=n, random_event=False,
                           reference_freq=20.0, fmin_template=20.0,
                           approximant=None)


def test_hyperpipe_grid_is_read_with_units_and_event_slice(drv, tmp_path):
    grid = tmp_path / "grid-0.dat"
    _write_grid(drv, grid, [(30.0, 20.0, 0.1), (35.0, 25.0, 0.2),
                            (40.0, 30.0, 0.3)])
    P_list = drv.load_templates(_template_opts(grid, 1, 5), 1e9, 0.25, 1 / 4096.)
    assert len(P_list) == 2
    assert P_list[0].m1 / drv.MSUN == pytest.approx(35.0)
    assert P_list[1].m2 / drv.MSUN == pytest.approx(30.0)
    assert P_list[0].s1z == pytest.approx(0.2)
    # The grid's waveform metadata reaches the template (ampO=0 would keep
    # only the (2,+-2) modes).
    assert P_list[0].ampO == -1


def test_hyperpipe_grid_event_out_of_range_soft_exits(drv, tmp_path):
    grid = tmp_path / "grid-0.dat"
    _write_grid(drv, grid, [(30.0, 20.0, 0.0)])
    with pytest.raises(SystemExit) as info:
        drv.load_templates(_template_opts(grid, 3, 1), 1e9, 0.25, 1 / 4096.)
    assert info.value.code == 0


def _template(drv):
    P = drv.lalsimutils.ChooseWaveformParams()
    P.m1, P.m2 = 31.0 * drv.MSUN, 22.0 * drv.MSUN
    P.s1x, P.s1y, P.s1z = 0.01, 0.02, 0.3
    P.s2x, P.s2y, P.s2z = 0.03, 0.04, -0.2
    return P


def test_shard_is_hyperpipe_format_when_active(drv, tmp_path, monkeypatch):
    from RIFT.misc import hyperpipeline_io
    monkeypatch.setenv(hyperpipeline_io.ENV_FLAG, "1")
    opts = SimpleNamespace(output_file=str(tmp_path / "MARG-7-1-0.dat"))
    drv.write_dat(opts, _template(drv), 0, 7, 12.5, 0.04, 1000, 55.0,
                  angle_note="angle_scheme=grid gh_nodes=8")
    fname = drv.dat_path(opts, 0)
    assert hyperpipeline_io.sniff(fname)
    assert hyperpipeline_io.read_header(fname) == tuple(
        hyperpipeline_io.build_column_list())
    arr, cols = hyperpipeline_io.read_table(fname)
    row = np.atleast_1d(arr)[0]
    assert row["lnL"] == pytest.approx(12.5)
    assert row["sigma_lnL"] == pytest.approx(0.04)
    assert row["m1"] == pytest.approx(31.0)
    assert row["a1z"] == pytest.approx(0.3)
    assert row["a2z"] == pytest.approx(-0.2)
    # The evidence label survives as a comment and is invisible to readers.
    assert "angle_scheme=grid gh_nodes=8" in open(fname).read()


def test_hypercombine_consolidates_jax_shards(drv, tmp_path, monkeypatch):
    from RIFT.misc import hyperpipeline_io
    monkeypatch.setenv(hyperpipeline_io.ENV_FLAG, "1")
    shards = []
    for k, lnL in enumerate((10.0, 11.0)):
        opts = SimpleNamespace(output_file=str(tmp_path / "MARG-{}.dat".format(k)))
        P = _template(drv)
        P.m1 = (30.0 + k) * drv.MSUN
        drv.write_dat(opts, P, 0, k, lnL, 0.05, 1000, 40.0,
                      angle_note="angle_scheme=grid")
        shards.append(drv.dat_path(opts, 0))
    out = tmp_path / "all.marg_net"
    with open(out, "w") as stream:
        subprocess.run([sys.executable, str(CODE / "bin" / "util_HyperCombine.py")]
                       + shards, stdout=stream, check=True)
    assert hyperpipeline_io.sniff(str(out))
    arr, _ = hyperpipeline_io.read_table(str(out))
    assert sorted(np.atleast_1d(arr)["lnL"]) == pytest.approx([10.0, 11.0])


def test_shard_stays_legacy_without_the_flag(drv, tmp_path, monkeypatch):
    from RIFT.misc import hyperpipeline_io
    monkeypatch.delenv(hyperpipeline_io.ENV_FLAG, raising=False)
    opts = SimpleNamespace(output_file=str(tmp_path / "ILE.dat"))
    drv.write_dat(opts, _template(drv), 0, 3, 9.0, 0.1, 500, 20.0)
    fname = drv.dat_path(opts, 0)
    assert not hyperpipeline_io.sniff(fname)
    assert open(fname).readline().startswith("# event_id m1 m2")
