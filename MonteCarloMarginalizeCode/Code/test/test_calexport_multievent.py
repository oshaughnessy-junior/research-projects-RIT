"""--calibration-export-posterior with --n-events-to-analyze > 1, end to end.

resample_samples() wrote each point's fair-draw extrinsic arrays into the module-global
P (= P_list[0]), while the export in analyze_event read the local P_list[indx_event].
For every point after the first, the export therefore wrote the last sampler batch: one
row per batch entry, and only the columns of that length.  Seen on GW240426: first-point
_cal.dat files had 5 rows x 71 columns, later ones 4096 x 40.

This runs the driver as a subprocess on a tiny synthetic zero-data H1 frame with a flat
calibration envelope and two intrinsic points in one job, on the NoLoop path (all four
REDLINE flags), and checks for each point that its _cal.dat
  * has one row per fair draw in that point's samples XML,
  * has the first point's columns, and
  * carries the same extrinsic draws as that XML, row for row.
"""

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

import lal
import lal.series
import lalsimulation as lalsim
from igwn_ligolw import lsctables, ligolw
from igwn_ligolw import utils as ligolw_utils
import RIFT.lalsimutils as lalsimutils

BIN = Path(__file__).resolve().parents[1] / "bin" / "integrate_likelihood_extrinsic_batchmode"
N_POINTS = 2


def _make_fixture(outdir):
    outdir = Path(outdir)
    event_time, srate = 1000000000.0, 2048.0
    deltaT = 1.0 / srate
    seg_start, seg_end = event_time - 6.0, event_time + 2.0
    duration = seg_end - seg_start
    npts = int(round(duration / deltaT))

    ht = lal.CreateREAL8TimeSeries(
        "Zero strain", lal.LIGOTimeGPS(seg_start), 0.0, deltaT,
        lalsimutils.lsu_DimensionlessUnit, npts)
    ht.data.data = np.zeros(npts)
    fname = outdir / ("H-fake_strain-%d-%d.gwf" % (int(seg_start), int(duration)))
    lalsimutils.hoft_to_frame_data(str(fname), "H1:FAKE-STRAIN", ht)
    subprocess.run("echo %s | lal_path2cache > %s" % (fname, outdir / "test.cache"), shell=True, check=True)

    psd = lal.CreateREAL8FrequencySeries(
        "psd", lal.LIGOTimeGPS(0), 0, 1.0 / duration, lal.SecondUnit, npts // 2 + 1)
    farr = psd.f0 + np.arange(psd.data.length) * psd.deltaF
    psd.data.data = np.where(farr > 1.0, [lalsim.SimNoisePSDaLIGOZeroDetHighPower(f) for f in farr], 1.0)
    ligolw_utils.write_filename(lal.series.make_psd_xmldoc({"H1": psd}), str(outdir / "H1_psd.xml.gz"))

    # Flat bilby-format envelope: freq, median amp/phase, 16th and 84th percentile amp/phase.
    caldir = outdir / "cal"
    caldir.mkdir()
    f = np.geomspace(10, 1024, 20)
    one, zero = np.ones_like(f), np.zeros_like(f)
    np.savetxt(caldir / "H1.txt", np.column_stack([f, one, zero, one - .05, zero - .03, one + .05, zero + .03]))

    P_list = []
    for m1, m2 in [(35.0, 30.0), (33.0, 31.0)][:N_POINTS]:
        P = lalsimutils.ChooseWaveformParams()
        P.m1, P.m2 = m1 * lal.MSUN_SI, m2 * lal.MSUN_SI
        P.tref = event_time
        P_list.append(P)
    lalsimutils.ChooseWaveformParams_array_to_xml(P_list, str(outdir / "grid"))
    return dict(outdir=outdir, caldir=caldir, event_time=event_time,
                seg_start=seg_start, seg_end=seg_end)


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory):
    return _make_fixture(tmp_path_factory.mktemp("calexport_multievent"))


def _samples_xml(path):
    class _Handler(ligolw.LIGOLWContentHandler):
        pass
    lsctables.use_in(_Handler)
    doc = ligolw_utils.load_filename(str(path), contenthandler=_Handler)
    return lsctables.SimInspiralTable.get_table(doc)


def _run_driver(d, *extra):
    out = d["outdir"]
    cmd = [
        sys.executable, str(BIN),
        "--cache-file", str(out / "test.cache"),
        "--channel-name", "H1=FAKE-STRAIN",
        "--psd-file", "H1=%s" % (out / "H1_psd.xml.gz"),
        "--event-time", str(d["event_time"]),
        "--data-start-time", str(d["seg_start"]),
        "--data-end-time", str(d["seg_end"]),
        "--approximant", "TaylorT4", "--l-max", "2",
        "--reference-freq", "40", "--fmin-template", "40",
        "--srate", "2048", "--inv-spec-trunc-time", "0",
        "--sampler-method", "AV", "--n-max", "20000", "--n-eff", "50",
        "--time-marginalization", "--vectorized", "--gpu", "--force-xpy",
        "--sim-xml", str(out / "grid.xml.gz"), "--n-events-to-analyze", str(N_POINTS),
        "--calibration-envelope-directory", str(d["caldir"]),
        "--calibration-n-realizations", "16", "--calibration-spline-count", "4",
        "--calibration-export-posterior",
        "--save-samples", "--resample-time-marginalization",
        "--fairdraw-extrinsic-output", "--fairdraw-extrinsic-output-n-max", "7",
        "--seed", "11", "--output-file", "out.xml",
    ] + list(extra)
    env = dict(os.environ, CUDA_VISIBLE_DEVICES="", OMP_NUM_THREADS="1")
    return subprocess.run(cmd, cwd=str(out), env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, universal_newlines=True, timeout=1200)


def _check_exports(out, proc):
    """Each point's _cal.dat: its own fair draws, row for row, with the first point's columns."""
    assert "NOT APPLIED" not in proc.stdout   # no stencil downgrade of any kind on this config
    assert "--calibration-export-posterior failed" not in proc.stdout, proc.stdout[-4000:]
    headers = []
    for i in range(N_POINTS):
        cal = out / ("out.xml_%d_cal.dat" % i)
        assert cal.exists(), "point %d wrote no _cal.dat\n%s" % (i, proc.stdout[-4000:])
        header = cal.read_text().splitlines()[0].lstrip("#").split()
        mat = np.atleast_2d(np.loadtxt(cal))
        rows = _samples_xml(out / ("out.xml_%d_.xml.gz" % i))
        n_fair = len(rows)
        assert n_fair > 0
        assert mat.shape == (n_fair, len(header)), (i, mat.shape, n_fair, len(header))
        # Same draws as the samples XML, row for row (longitude = right ascension).
        ra_xml = np.array([r.longitude for r in rows], dtype=float)
        np.testing.assert_allclose(mat[:, header.index("right_ascension")], ra_xml, rtol=1e-6)  # sim_inspiral longitude is REAL4
        assert any(h.startswith("cal_H1_amp_") for h in header)
        headers.append(header)
    for i in range(1, N_POINTS):
        assert headers[i] == headers[0], (i, sorted(set(headers[0]) ^ set(headers[i])))


def test_every_point_exports_its_own_fair_draws(fixture_dir):
    proc = _run_driver(fixture_dir)
    assert proc.returncode == 0, proc.stdout[-4000:]
    _check_exports(fixture_dir["outdir"], proc)


def test_resample_and_export_share_the_point_P():
    """Structural pin for what the row count cannot see: the arrays the export evaluates are
    the ones resample_samples just drew FOR THIS POINT.  In analyze_event, P is bound only to
    P_list[indx_event], resample_samples receives that P, and the export's NoLoop call reads it."""
    import ast
    tree = ast.parse(BIN.read_text())
    analyze = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "analyze_event")
    binds = [ast.unparse(n.value) for n in ast.walk(analyze) if isinstance(n, ast.Assign)
             and any(isinstance(t, ast.Name) and t.id == "P" for t in n.targets)]
    assert binds == ["P_list[indx_event]"], binds
    resample = [n for n in ast.walk(analyze) if isinstance(n, ast.Call)
                and isinstance(n.func, ast.Name) and n.func.id == "resample_samples"]
    assert len(resample) == 1
    kw = {k.arg: ast.unparse(k.value) for k in resample[0].keywords}
    assert kw.get("P") == "P", kw
    export = [n for n in ast.walk(analyze) if isinstance(n, ast.Call)
              and ast.unparse(n.func).endswith("DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop")
              and any(k.arg == "return_cal_components" for k in n.keywords)
              and n.lineno > resample[0].lineno]
    assert len(export) == 1 and ast.unparse(export[0].args[1]) == "P", [ast.unparse(e) for e in export]

