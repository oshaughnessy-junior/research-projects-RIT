"""PrecomputeLikelihoodTermsMultirate: the two-rate schedule against the one-segment (full-rate,
per-|m|) control on a short O4 BNS with higher modes, and the refusals
(DESIGN_early_time_multirate.md, "Opt-in interface")."""
import json
import numpy as np
import pytest
import lal
import lalsimulation as lalsim

import RIFT.lalsimutils as lsu
from RIFT.likelihood import factored_likelihood as fl
from RIFT.likelihood import factored_likelihood_multirate as flm

FS, SEGLEN, FLOW, FMAX, LMAX = 4096, 512, 20., 1700., 4
T_GEO = 1000000000.0
SCHED = dict(fs_e=256, tau_tr=60., guard=4., atten_db=160., late_start=96., late_buffer=128, post=24,
             uv_margin=32., mc_range=[1.1, 1.3])


def _P(s1x=0., mc_scale=1.0):
    P = lsu.ChooseWaveformParams()
    P.m1, P.m2 = 1.40 * mc_scale * lal.MSUN_SI, 1.35 * mc_scale * lal.MSUN_SI
    P.s1x = s1x
    P.fmin, P.fref = FLOW, FLOW
    P.deltaT, P.deltaF = 1. / FS, 1. / SEGLEN
    P.approx = lalsim.IMRPhenomXHM
    P.phi, P.theta, P.tref = 1.0, 0.4, T_GEO
    return P


def _schedule(tmp_path, kind):
    p = tmp_path / ("%s.json" % kind)
    p.write_text(json.dumps(dict(SCHED, kind=kind)))
    return flm.load_schedule(str(p))


@pytest.fixture(scope="module")
def inputs():
    dt, df, N = 1. / FS, 1. / SEGLEN, FS * SEGLEN
    psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0), 0., df, lsu.lsu_HertzUnit, N // 2 + 1)
    lalsim.SimNoisePSDaLIGOAdVO4T1800545(psd, 10.)
    v = np.array(psd.data.data); v[v <= 0] = np.inf; psd.data.data = v
    t_det = float(fl.ComputeArrivalTimeAtDetector("H1", 1.0, 0.4, lal.LIGOTimeGPS(T_GEO)))
    rng = np.random.default_rng(3)
    d = lal.CreateCOMPLEX16TimeSeries("d", lal.LIGOTimeGPS(t_det - (SEGLEN - 24.) - 0.0123), 0., dt,
                                      lsu.lsu_DimensionlessUnit, N)
    d.data.data = 1e-22 * rng.standard_normal(N) + 0j
    return dict(data={"H1": lsu.DataFourier(d)}, psd={"H1": psd})


def _run(tmp_path, inputs, kind, P=None):
    return flm.PrecomputeLikelihoodTermsMultirate(lal.LIGOTimeGPS(T_GEO), 0.05, P or _P(), inputs["data"],
                                                  inputs["psd"], LMAX, FMAX, verbose=False, quiet=True,
                                                  schedule=_schedule(tmp_path, kind), skip_interpolation=True)


def test_two_rate_matches_one_segment(tmp_path, inputs):
    _, U1, V1, rho1, _, _ = _run(tmp_path, inputs, "one_segment")
    _, U2, V2, rho2, _, _ = _run(tmp_path, inputs, "two_rate")
    sU = max(abs(v) for v in U1["H1"].values())
    for key in U1["H1"]:
        assert abs(U2["H1"][key] - U1["H1"][key]) <= 1e-5 * sU, ("U", key)
        assert abs(V2["H1"][key] - V1["H1"][key]) <= 1e-5 * sU, ("V", key)
    sQ = max(np.max(np.abs(v.data.data)) for v in rho1["H1"].values())
    for k in rho1["H1"]:
        a, b = rho1["H1"][k], rho2["H1"][k]
        assert abs(float(a.epoch) - float(b.epoch)) < 1e-9
        assert a.data.length == b.data.length
        assert np.max(np.abs(a.data.data - b.data.data)) <= 1e-5 * sQ, k


def test_schedule_rejects_short_late_buffer(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps(dict(SCHED, kind="two_rate", late_buffer=64)))
    with pytest.raises(ValueError, match="late_buffer"):
        flm.load_schedule(str(p))


@pytest.mark.parametrize("P, msg", [(_P(s1x=0.1), "in-plane"), (_P(mc_scale=1.2), "outside")])
def test_refusals(tmp_path, inputs, P, msg):
    with pytest.raises(ValueError, match=msg):
        _run(tmp_path, inputs, "two_rate", P)
