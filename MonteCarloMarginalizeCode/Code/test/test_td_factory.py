"""
test_td_factory.py:  Tests for the TD inner-product FACTORY threaded through the
factored-likelihood Precompute step (task T3 of TD_LIKELIHOOD.md).

Two groups, both driving the ACTUAL factored_likelihood entry points
(ComputeModeIPTimeSeries, ComputeModeCrossTermIP) on synthetic hlm/data dicts built
from small COMPLEX16FrequencySeries fixtures (the T1-test style):

  (a) DEFAULT-PATH REGRESSION -- call the two FL functions with IP_factory=None and
      assert their outputs equal a DIRECT lsu construction of the same quantity.  The
      default None path IS the old code, so this pins that the threading did not perturb
      the byte-identical default.

  (b) FACTORY PATH -- call the same two FL functions with IP_factory=TDInnerProductFactory
      and assert:
        * Q_lm(t) time series (ComputeModeIPTimeSeries) max relative <= 1e-12 vs default,
        * U and V cross-term matrices (ComputeModeCrossTermIP) <= 1e-12 vs default,
      and that inv_spec_trunc_Q is warned-about-and-ignored on the TD path (plan D5).

Environment: ~/.conda/envs/junior_rift/bin/python with
PYTHONPATH=<worktree>/MonteCarloMarginalizeCode/Code (lal 7.7.0).  Runnable as
    pytest test_td_factory.py
    python  test_td_factory.py
"""

import warnings

import numpy as np

import lal
import RIFT.lalsimutils as lsu
import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood.td_inner_product import TDInnerProductFactory


# --------------------------------------------------------------------------- fixtures

FS = 4096.0
FNYQ = FS / 2.0
N = 4096                    # two-sided length (long segment for a good match)
DELTAF = FS / N            # = 1.0
DELTAT = 1.0 / FS
FLOW = 40.0
FMAX = 1000.0
LEN1 = N // 2 + 1


def analytic_psd(f):
    """Smooth, strictly positive analytic one-sided PSD (matches the T1 test fixture)."""
    f = np.atleast_1d(np.asarray(f, dtype=float))
    fsafe = np.maximum(np.abs(f), 1.0)
    out = 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)
    return out if out.size > 1 else float(out[0])


def make_bandlimited_series(rng, epoch=0.0, flow=FLOW, fmax=FMAX):
    """A random band-limited two-sided COMPLEX16FrequencySeries in lal packing."""
    fvals_1s = np.arange(LEN1) * DELTAF
    spec1 = np.zeros(LEN1, dtype=complex)
    band = (fvals_1s >= flow) & (fvals_1s <= fmax)
    spec1[band] = rng.standard_normal(band.sum()) + 1j * rng.standard_normal(band.sum())
    h2 = np.zeros(N, dtype=complex)
    h2[:LEN1] = spec1[::-1]
    h2[LEN1 - 1:] = spec1[:-1]
    hf = lal.CreateCOMPLEX16FrequencySeries(
        "h", lal.LIGOTimeGPS(epoch), 0., DELTAF, lsu.lsu_HertzUnit, N)
    hf.data.data = h2
    return hf


def make_psd_series():
    """One-sided PSD as a lal.REAL8FrequencySeries (what ILE holds post-resample)."""
    psd = lal.CreateREAL8FrequencySeries(
        "psd", lal.LIGOTimeGPS(0.), 0., DELTAF, lsu.lsu_HertzUnit, LEN1)
    f1s = np.arange(LEN1) * DELTAF
    psd.data.data = analytic_psd(f1s)
    return psd


def make_hlm_dict(rng, modes, epoch=0.0):
    """A SphHarm-style dict {(l,m): COMPLEX16FrequencySeries} of band-limited modes."""
    return {mode: make_bandlimited_series(rng, epoch=epoch) for mode in modes}


MODES = [(2, 2), (2, 1)]
N_SHIFT = 0
N_WINDOW = 128


# --------------------------------------------------------------------------- (a) regression

def test_default_path_crossterm_matches_direct_lsu():
    """ComputeModeCrossTermIP(IP_factory=None) == direct lsu.ComplexIP construction."""
    rng = np.random.default_rng(101)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)

    # Reference: exactly what the default path does internally, built by hand.
    IP = lsu.ComplexIP(FLOW, FMAX, FNYQ, DELTAF, psd, False, False, 0.)
    ref = {}
    for m1 in MODES:
        for m2 in MODES:
            ref[(m1, m2)] = IP.ip(hlms[m1], hlms[m2])

    got = fl.ComputeModeCrossTermIP(
        hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
        analyticPSD_Q=False, inv_spec_trunc_Q=False, T_spec=0.,
        verbose=False, same_waveform_Q=False, IP_factory=None)

    maxdiff = max(abs(got[k] - ref[k]) for k in ref)
    print("[T3-a] ComputeModeCrossTermIP default-path vs direct lsu: max|diff| = %.3e"
          % maxdiff)
    assert maxdiff == 0.0     # default None path IS the old code: bit-identical


def test_default_path_timeseries_matches_direct_lsu():
    """ComputeModeIPTimeSeries(IP_factory=None) == direct lsu.ComplexOverlap construction."""
    rng = np.random.default_rng(202)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)
    data = make_bandlimited_series(rng)

    # Reference: replicate the exact default-path body (lsu.ComplexOverlap + roll + cut).
    IP = lsu.ComplexOverlap(FLOW, FMAX, FNYQ, DELTAF, psd, False, False, 0.,
                            full_output=True)
    ref = {}
    for pair in hlms.keys():
        rho, rhoTS, rhoIdx, rhoPhase = IP.ip(hlms[pair], data)
        rhoTS.epoch = data.epoch - hlms[pair].epoch
        lsu.DataRollBins(rhoTS, N_SHIFT)
        ref[pair] = lal.CutCOMPLEX16TimeSeries(rhoTS, 0, N_WINDOW)

    got = fl.ComputeModeIPTimeSeries(
        hlms, data, psd, FLOW, FMAX, FNYQ, N_SHIFT, N_WINDOW,
        analyticPSD_Q=False, inv_spec_trunc_Q=False, T_spec=0., IP_factory=None)

    maxdiff = 0.0
    for pair in hlms.keys():
        d = np.max(np.abs(got[pair].data.data - ref[pair].data.data))
        maxdiff = max(maxdiff, d)
        assert got[pair].data.length == ref[pair].data.length
    print("[T3-a] ComputeModeIPTimeSeries default-path vs direct lsu: max|diff| = %.3e"
          % maxdiff)
    assert maxdiff == 0.0     # default None path IS the old code: bit-identical


# --------------------------------------------------------------------------- (b) factory path

def test_factory_crossterm_matches_default():
    """U and V cross-term matrices via TDInnerProductFactory match the default <= 1e-12."""
    rng = np.random.default_rng(303)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)
    hlms_conj = {m: _conj_series(hlms[m]) for m in MODES}

    factory = TDInnerProductFactory(solver="auto")

    def _rel(a, b):
        keys = a.keys()
        num = max(abs(a[k] - b[k]) for k in keys)
        den = max(abs(b[k]) for k in keys)
        return num / den

    # U = <h_lm | h_l'm'>
    U_def = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                      analyticPSD_Q=False, verbose=False,
                                      same_waveform_Q=False, IP_factory=None)
    U_td = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                     analyticPSD_Q=False, verbose=False,
                                     same_waveform_Q=False, IP_factory=factory)
    rU = _rel(U_td, U_def)

    # V = <h*_lm | h_l'm'>
    V_def = fl.ComputeModeCrossTermIP(hlms_conj, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                      analyticPSD_Q=False, prefix="V", verbose=False,
                                      same_waveform_Q=False, IP_factory=None)
    V_td = fl.ComputeModeCrossTermIP(hlms_conj, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                     analyticPSD_Q=False, prefix="V", verbose=False,
                                     same_waveform_Q=False, IP_factory=factory)
    rV = _rel(V_td, V_def)

    print("[T3-b] U-matrix factory vs default max rel = %.3e ; V-matrix max rel = %.3e"
          % (rU, rV))
    assert rU <= 1e-12
    assert rV <= 1e-12


def test_factory_timeseries_matches_default():
    """Q_lm(t) time series via TDInnerProductFactory matches the default <= 1e-12."""
    rng = np.random.default_rng(404)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)
    data = make_bandlimited_series(rng)

    factory = TDInnerProductFactory(solver="auto")

    Q_def = fl.ComputeModeIPTimeSeries(
        hlms, data, psd, FLOW, FMAX, FNYQ, N_SHIFT, N_WINDOW,
        analyticPSD_Q=False, IP_factory=None)
    Q_td = fl.ComputeModeIPTimeSeries(
        hlms, data, psd, FLOW, FMAX, FNYQ, N_SHIFT, N_WINDOW,
        analyticPSD_Q=False, IP_factory=factory)

    maxrel = 0.0
    for pair in hlms.keys():
        a = Q_td[pair].data.data
        b = Q_def[pair].data.data
        assert Q_td[pair].data.length == Q_def[pair].data.length
        assert abs(Q_td[pair].deltaT - Q_def[pair].deltaT) <= 1e-15
        maxrel = max(maxrel, np.max(np.abs(a - b)) / np.max(np.abs(b)))
    print("[T3-b] Q_lm(t) factory vs default max rel = %.3e" % maxrel)
    assert maxrel <= 1e-12


def test_factory_ignores_inv_spec_trunc():
    """On the TD path inv_spec_trunc_Q is warned-about and ignored (plan D5)."""
    rng = np.random.default_rng(505)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)
    factory = TDInnerProductFactory(solver="auto")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        U_trunc = fl.ComputeModeCrossTermIP(
            hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF, analyticPSD_Q=False,
            inv_spec_trunc_Q=True, T_spec=8.0, verbose=False,
            same_waveform_Q=False, IP_factory=factory)
    msgs = [str(w.message) for w in caught]
    assert any("inv_spec_trunc" in m for m in msgs), \
        "expected an inv-spec-trunc warning on the TD path; got %r" % msgs

    # And the values are exactly those with inv_spec_trunc off (it was ignored).
    U_off = fl.ComputeModeCrossTermIP(
        hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF, analyticPSD_Q=False,
        inv_spec_trunc_Q=False, T_spec=0., verbose=False,
        same_waveform_Q=False, IP_factory=factory)
    maxdiff = max(abs(U_trunc[k] - U_off[k]) for k in U_off)
    print("[T3-b] TD inv-spec-trunc ignored: max|diff(trunc,off)| = %.3e (warned=%d)"
          % (maxdiff, len(msgs)))
    assert maxdiff == 0.0


# --------------------------------------------------------------------------- helpers

def _conj_series(hf):
    """Return a COMPLEX16FrequencySeries holding conj(hf) in lal's reversed packing.

    Mirrors what factored_likelihood's hlms_conj carries (the conjugate-mode series for V);
    for the fixture we only need a distinct, valid band-limited series, so conjugating the
    reversed-packed data in place is sufficient and keeps deltaF/length/epoch identical.
    """
    out = lal.CreateCOMPLEX16FrequencySeries(
        "hconj", hf.epoch, hf.f0, hf.deltaF, lsu.lsu_HertzUnit, hf.data.length)
    out.data.data = np.conj(np.asarray(hf.data.data))
    return out


# --------------------------------------------------------------------------- script entry

if __name__ == "__main__":
    import time
    tests = [
        ("(a) crossterm default==lsu", test_default_path_crossterm_matches_direct_lsu),
        ("(a) timeseries default==lsu", test_default_path_timeseries_matches_direct_lsu),
        ("(b) crossterm factory==default", test_factory_crossterm_matches_default),
        ("(b) timeseries factory==default", test_factory_timeseries_matches_default),
        ("(b) TD ignores inv-spec-trunc", test_factory_ignores_inv_spec_trunc),
    ]
    t0 = time.time()
    failures = 0
    for name, fn in tests:
        print("\n=== %s ===" % name)
        try:
            fn()
            print("PASS: %s" % name)
        except AssertionError as e:
            failures += 1
            print("FAIL: %s : %s" % (name, e))
    print("\nTotal runtime: %.2f s ; %d/%d passed"
          % (time.time() - t0, len(tests) - failures, len(tests)))
    raise SystemExit(1 if failures else 0)


def test_td_factory_bypasses_fd_batching(monkeypatch):
    """FD batching must not replace a custom TD operator, even when enabled globally."""
    rng = np.random.default_rng(343)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)
    factory = TDInnerProductFactory()
    kwargs = dict(verbose=False, IP_factory=factory)
    ref = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ,
                                   DELTAF, batched=False, **kwargs)
    monkeypatch.setenv("RIFT_PRECOMPUTE_BATCHED_CROSSTERMS", "1")
    for batch in (None, True):
        got = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ,
                                       DELTAF, batched=batch, **kwargs)
        assert got == ref


def test_upstream_positional_batched_argument():
    """Keep upstream's trailing positional batched flag in its original slot."""
    rng = np.random.default_rng(344)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)
    got = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                   False, False, 0.0, False, "U", False, True)
    ref = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                   verbose=False, batched=True)
    assert got == ref


def test_legacy_positional_factory_argument(monkeypatch):
    """Accept the private API's older positional factory without silently using FD."""
    rng = np.random.default_rng(345)
    psd = make_psd_series()
    hlms = make_hlm_dict(rng, MODES)
    factory = TDInnerProductFactory()
    original = factory.ip_scalar
    calls = []
    def tracked(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(factory, "ip_scalar", tracked)
    got = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                   False, False, 0.0, False, "U", False, factory)
    assert calls == [True]
    ref = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                   verbose=False, IP_factory=factory)
    assert got == ref


def test_full_segment_noloop_likelihood_parity():
    """Independent FD/TD statistics give the same maintained NoLoop lnL(t)."""
    from types import SimpleNamespace
    rng = np.random.default_rng(917)
    modes = [(2, -2), (2, 2)]
    # Physical amplitudes keep lnL well scaled; avoid a zero-Q parity test.
    hlms = make_hlm_dict(rng, modes, epoch=-0.1)
    data = make_bandlimited_series(rng, epoch=-0.1)
    for h in [data, *hlms.values()]:
        h.data.data[:] *= 1e-23
    psd = make_psd_series()
    args = []
    for factory in [None, TDInnerProductFactory(window_shape=0.0)]:
        U = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ,
            DELTAF, False, False, 0., verbose=False, IP_factory=factory)
        hc = {m: _conj_series(h) for m, h in hlms.items()}
        V = fl.ComputeModeCrossTermIP(hlms, hc, psd, FLOW, FMAX, FNYQ,
            DELTAF, False, False, 0., verbose=False, IP_factory=factory)
        Q = fl.ComputeModeIPTimeSeries(hlms, data, psd, FLOW, FMAX, FNYQ,
            0, N, False, False, 0., IP_factory=factory)
        packed = fl.PackLikelihoodDataStructuresAsArrays(modes, {}, Q, U, V)
        lookup, _, _, ua, va, qa, _, epoch = packed
        args.append(({'H1': lookup}, {'H1': qa}, {'H1': ua},
                     {'H1': va}, {'H1': epoch - 0.1}))
    P = SimpleNamespace(phi=np.array([0.3, 1.1, 2.0]),
        theta=np.array([-0.2, 0.4, -0.5]), incl=np.array([0.2, 1., 2.]),
        phiref=np.array([0.1, 0.5, 1.2]), psi=np.array([0.2, 0.9, 0.7]),
        dist=np.array([100., 200., 400.])*lal.PC_SI*1e6,
        tref=0., deltaT=DELTAT)
    times = np.arange(-32, 33)*DELTAT
    values = [np.asarray(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(
        times, P, *a, return_lnLt=True, xpy=np)) for a in args]
    assert np.ptp(values[0]) > 1e-8
    np.testing.assert_allclose(values[1], values[0], rtol=1e-11, atol=1e-10)
    # Explicitly pin the time marginalization as well, through the same NoLoop call.
    marginal = [np.asarray(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(
        times, P, *a, xpy=np)) for a in args]
    np.testing.assert_allclose(marginal[1], marginal[0], rtol=1e-11, atol=1e-10)
