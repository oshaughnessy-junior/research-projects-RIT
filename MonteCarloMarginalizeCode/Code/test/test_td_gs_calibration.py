"""
test_td_gs_calibration.py: calibration marginalization on the time-domain td-gs code path
(RIFT/likelihood/td_gs_precompute.py TDCalibrationFilters + precompute(calibration=...),
factored_likelihood NoLoop with time-indexed per-realization self terms).

Run with  python -m pytest test/test_td_gs_calibration.py  from MonteCarloMarginalizeCode/Code
(lal, lalsimulation with IMRPhenomTPHM, tdanalysis >= 1.1 on PYTHONPATH).

  (a) test_identity_calibration_*     : realizations C = 1 reproduce the uncalibrated Q(t), U(t),
      V(t) and the uncalibrated NoLoop lnL to rounding (< 1e-12).
  (b) test_per_realization_vs_brute_force : Q_c(t), U_c(t), V_c(t) for each realization vs a
      dense brute force (template filtered by exact frequency-domain multiplication with the
      full N-tap response on a long zero-padded stream, then cut; dense C^{-1}) on a ringdown
      piece and on two spliced pieces, dense and GS solvers; and the exact reach-truncated taps
      equal the longest symmetric truncation to rounding, while a forced short L differs.
  (c) test_noloop_calmarg_vs_brute_force : the NoLoop calmarg lnL (loop method) with the 4-D
      per-realization self terms vs a brute force over realizations (each realization through
      the n_cal=1 time-indexed path, combined by log-sum-exp with the importance weights), for
      nearest/cubic, phase marginalization, return_lnLt and return_cal_components; and vs the
      independent per-sample python loop of test_td_time_indexed_U.
  (d) test_td_vs_fd_calmarg             : full segment, compact signal: TD calmarg vs RIFT's FD
      calmarg (same realizations) agree to the level TD and FD agree without calibration, for
      the default convention and --calibration-conjugate-phase.
  (e) test_convention_recovers_injection : data = (K_true h) with the code path's own taps; the
      matched convention gives Q_c = U_c at the injection shift (to rounding), the other
      conventions do not: 'rift' vs 'bilby' (H1 'data' type, V1 'template' type) and conjugate.
  (f) test_refusals                     : fused kernel, compressed base U with calibration,
      malformed 4-D arrays, wrong realization grid, bad correction type, forced L out of range.
  Plus the Precompute driver with calibration (two detectors; packed 4-D shapes; global-norm
  variant) and the adaptive-growth layout (concatenating realization blocks).
"""

import os
import sys

import numpy as np
import pytest

import lal
import lalsimulation as lalsim
import scipy.linalg
from scipy.special import logsumexp

import RIFT.lalsimutils as lsu
import RIFT.likelihood.factored_likelihood as fl

try:
    from tdanalysis.forms.sliding import sliding_self_terms  # noqa: F401
    from tdanalysis.utils.filters import fir_taps_from_response, fir_truncate, fir_grid_response
    from tdanalysis.covariance.psd import PSDToAutocorr  # noqa: F401
except Exception as _exc:   # pragma: no cover
    pytest.skip("tdanalysis >= 1.1 not importable (%r)" % (_exc,), allow_module_level=True)

from RIFT.likelihood import td_gs_precompute as tdg
from RIFT.calmarg import generate_realizations as genr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_td_gs_route import make_psd, make_modes, make_P, make_data, fd_versions, peak_time, FS, DT, FNYQ, EPOCH0, MREF  # noqa: E402
from test_td_calibration import write_synthetic_envelope  # noqa: E402
import test_td_time_indexed_U as ttu  # noqa: E402


# --------------------------------------------------------------------------- helpers

def realizations(env_path, N, fmin, fmax, n_cal, seed=0, n_spline=10):
    """RIFT realizations as ILE draws them (create_realizations), RIFT packing, on the data grid."""
    np.random.seed(seed)
    return genr.create_realizations(env_path, N * DT, DT, fmin, fmax, n_spline, n_cal)


def harr_of(hT):
    return {m: np.asarray(hT[m].data.data, dtype=np.complex128) for m in hT}


def brute_force_calibrated(ctx, hT, cal, c, N_shift, idx, conjugate=False, correction_type="rift"):
    """Dense reference for realization c: the template modes filtered by exact frequency-domain
    multiplication with the FULL N-tap response on a long zero-padded stream (a linear
    convolution with the N taps), placed at each grid shift, cut to the pieces, dense block-
    diagonal C^{-1}.  Returns {("Q", m, i), ("U", ma, mb, i), ("V", ma, mb, i)}."""
    N = ctx.N
    filt = tdg.TDCalibrationFilters(ctx, cal, correction_type=correction_type, conjugate=conjugate, verbose=False)
    taps_all, L_neg, L_pos = filt.taps_for(N)          # all N taps
    n_taps = taps_all.shape[1]
    Nh = hT[(2, 2)].data.length
    Nlong = int(2 ** np.ceil(np.log2(Nh + 2 * n_taps + N)))
    tp = np.zeros(Nlong)
    tp[:n_taps] = taps_all[c]
    Hf = np.fft.fft(tp)
    g = {}
    for m in hT:
        hp = np.zeros(Nlong, dtype=complex)
        hp[:Nh] = np.asarray(hT[m].data.data)
        g[m] = np.fft.ifft(Hf * np.fft.fft(hp))[:Nh + n_taps - 1]      # index j' = template index + L_neg
    blocks = []
    for (i0, i1) in ctx.runs:
        Cinv = np.linalg.inv(scipy.linalg.toeplitz(ctx.acf_lags(i1 - i0)))
        blocks.append((i0, i1, Cinv, Cinv @ ctx.d[i0:i1]))
    out = {}
    modes = list(hT.keys())
    for i in idx:
        k = N_shift + i
        src = np.arange(Nh + n_taps - 1) - L_neg + k
        ok = (src >= 0) & (src < N)
        X = {}
        for m in modes:
            x = np.zeros(N, dtype=complex)
            x[src[ok]] = g[m][ok]
            X[m] = x
        for ma in modes:
            out[("Q", ma, i)] = sum(np.conj(X[ma][i0:i1]) @ w for (i0, i1, _, w) in blocks)
            for mb in modes:
                out[("U", ma, mb, i)] = sum(np.conj(X[ma][i0:i1]) @ Cinv @ X[mb][i0:i1] for (i0, i1, Cinv, _) in blocks)
                out[("V", ma, mb, i)] = sum(X[ma][i0:i1] @ Cinv @ X[mb][i0:i1] for (i0, i1, Cinv, _) in blocks)
    return out


def worst_vs_brute(ref, rho, Uc, Vc, c, T, idx, modes, qscale, uscale):
    worst = 0.0
    for i in idx:
        for ma in modes:
            worst = max(worst, abs(ref[("Q", ma, i)] - rho[ma].data.data[c * T + i]) / qscale)
            for mb in modes:
                worst = max(worst, abs(ref[("U", ma, mb, i)] - Uc[c][(ma, mb)][i]) / uscale,
                            abs(ref[("V", ma, mb, i)] - Vc[c][(ma, mb)][i]) / uscale)
    return worst


def pack_calibrated(rholms, crossTerms, crossTermsV, crossTermsCal, crossTermsCalV):
    """ILE's packing for one set of detectors: time-indexed base U/V and 4-D cal arrays."""
    lookupNK, ctU, ctV, rhoA, epoch, ctU_cal, ctV_cal = {}, {}, {}, {}, {}, {}, {}
    for det in rholms:
        keys = list(rholms[det].keys())
        lookupNK[det], lKN, _, ctU[det], ctV[det], rhoA[det], _, epoch[det] = fl.PackLikelihoodDataStructuresAsArrays(
            keys, None, rholms[det], crossTerms[det], crossTermsV[det])
        if crossTermsCal is not None:
            ctU_cal[det], ctV_cal[det] = fl.PackCalCrossTermsAsArrays(keys, lKN, crossTermsCal[det], crossTermsCalV[det])
    if crossTermsCal is None:
        ctU_cal = ctV_cal = None
    return lookupNK, rhoA, ctU, ctV, epoch, ctU_cal, ctV_cal


def rel_err_finite(a, b, min_finite=3):
    """max |a - b| / max |b| over the entries finite in both (the NoLoop exponentiates against
    one global maximum, so samples hundreds of nats below it underflow to -inf on both sides)."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    ok = np.isfinite(a) & np.isfinite(b)
    assert ok.sum() >= min_finite, "too few finite likelihood values (%d)" % ok.sum()
    assert np.array_equal(np.isfinite(a), np.isfinite(b))
    return float(np.max(np.abs(a[ok] - b[ok])) / np.max(np.abs(b[ok])))


class _PVec(object):
    pass


def extrinsic_vector(n, tref, seed=5, dist_range=(400.0, 900.0)):
    rng = np.random.default_rng(seed)
    P = _PVec()
    P.phi = rng.uniform(0, 2 * np.pi, n)
    P.theta = rng.uniform(0.2, np.pi - 0.2, n)
    P.psi = rng.uniform(0, np.pi, n)
    P.incl = rng.uniform(0.2, np.pi - 0.2, n)
    P.phiref = rng.uniform(0, 2 * np.pi, n)
    P.dist = rng.uniform(dist_range[0], dist_range[1], n) * (lal.PC_SI * 1e6)
    P.tref = tref
    P.deltaT = DT
    return P


# --------------------------------------------------------------------------- fixtures

@pytest.fixture(scope="module")
def env_path(tmp_path_factory):
    d = tmp_path_factory.mktemp("cal")
    for ifo in ("H1", "L1", "V1"):
        write_synthetic_envelope(str(d / ("%s.txt" % ifo)))
    return str(d)


@pytest.fixture(scope="module")
def ringdown_case(env_path):
    N = 8192
    psd = make_psd(N)
    hT = make_modes(N)
    k_inj = 3000
    dts = make_data(N, hT, k_inj)
    tpk = peak_time(hT, k_inj)
    msec = MREF * lal.MTSUN_SI
    pieces = [(tpk - 0.002, tpk + 120 * msec)]
    cal = realizations(os.path.join(env_path, "H1.txt"), N, 20., 1500., 3, seed=11)
    return dict(N=N, psd=psd, hT=hT, dts=dts, tpk=tpk, pieces=pieces, N_shift=k_inj - 128, N_window=256, cal=cal)


# --------------------------------------------------------------------------- (a) identity

def test_identity_calibration_reproduces_uncalibrated_terms(ringdown_case):
    s = ringdown_case
    for pieces in (s["pieces"], None):
        ctx = tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=pieces, ifo="H1", verbose=False)
        rho0, U0, V0 = ctx.precompute(s["hT"], s["N_shift"], s["N_window"])
        one = np.ones((s["N"], 2), dtype=complex)
        filt = ctx.calibration_filters(one, verbose=False)
        assert np.max(np.abs(np.delete(filt.taps_full, 0, axis=1))) == 0.0     # exact delta taps
        rho, U, V, Uc, Vc = ctx.precompute(s["hT"], s["N_shift"], s["N_window"], calibration=filt)
        T = s["N_window"]
        assert rho[(2, 2)].data.length == 2 * T and float(rho[(2, 2)].epoch) == float(rho0[(2, 2)].epoch)
        qs = np.max(np.abs(rho0[(2, 2)].data.data))
        us = np.max(np.abs(U0[((2, 2), (2, 2))]))
        for key in U0:
            np.testing.assert_array_equal(U[key], U0[key])          # the uncalibrated block is untouched
        dq = max(np.max(np.abs(rho[m].data.data[c * T:(c + 1) * T] - rho0[m].data.data)) for m in s["hT"] for c in range(2)) / qs
        du = max(np.max(np.abs(Uc[c][k] - U0[k])) for k in U0 for c in range(2)) / us
        dv = max(np.max(np.abs(Vc[c][k] - V0[k])) for k in V0 for c in range(2)) / us
        print("[td-gs cal a] identity calibration (%s): Q %.1e, U %.1e, V %.1e relative" % (
            "ringdown piece" if pieces else "full segment", dq, du, dv))
        assert dq < 1e-13 and du < 1e-13 and dv < 1e-13


def test_identity_calibration_reproduces_uncalibrated_lnL(ringdown_case):
    s = ringdown_case
    dets = ["H1", "L1"]
    ctxs = {d: tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=s["pieces"], ifo=d, verbose=False) for d in dets}
    one = np.ones((s["N"], 1), dtype=complex)
    rholms, ct, ctV, rholms_c, ct_c, ctV_c = {}, {}, {}, {}, {}, {}
    for d in dets:
        rholms[d], ct[d], ctV[d] = ctxs[d].precompute(s["hT"], s["N_shift"], s["N_window"])
        filt = ctxs[d].calibration_filters(one, verbose=False)
        rholms_c[d], _, _, ct_c[d], ctV_c[d] = ctxs[d].precompute(s["hT"], s["N_shift"], s["N_window"], calibration=filt)
    lookupNK, rhoA, ctU, ctVa, epoch, _, _ = pack_calibrated(rholms, ct, ctV, None, None)
    lookupNK_c, rhoA_c, ctU_c, ctV_c2, epoch_c, ctU_cal, ctV_cal = pack_calibrated(rholms_c, ct, ctV, ct_c, ctV_c)
    assert ctU_cal["H1"].shape == (1, s["N_window"], 5, 5)
    P = extrinsic_vector(6, s["tpk"])
    tvals = np.linspace(-0.004, 0.004, 33)
    for interp in ("nearest", "cubic"):
        lnL0 = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctVa, epoch, Lmax=2, xpy=np,
                                                                   time_interp=interp)
        lnL1 = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK_c, rhoA_c, ctU_c, ctV_c2, epoch_c, Lmax=2, xpy=np,
                                                                   time_interp=interp, n_cal=1,
                                                                   ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal)
        err = rel_err_finite(lnL1, lnL0)
        print("[td-gs cal a] identity calibration lnL (%s): %.1e relative" % (interp, err))
        assert err < 1e-12


# --------------------------------------------------------------------------- (b) brute force

@pytest.mark.parametrize("dense_max_n", [4096, 0])
def test_per_realization_vs_brute_force_ringdown(ringdown_case, dense_max_n):
    s = ringdown_case
    ctx = tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=s["pieces"], dense_max_n=dense_max_n,
                                  ifo="H1", verbose=False)
    filt = ctx.calibration_filters(s["cal"], verbose=False)
    T = s["N_window"]
    rho, U, V, Uc, Vc = ctx.precompute(s["hT"], s["N_shift"], T, calibration=filt)
    assert ctx.calibration_last["exact"]
    qs = np.max(np.abs(rho[(2, 2)].data.data))
    us = max(np.max(np.abs(Uc[c][((2, 2), (2, 2))])) for c in range(filt.n_cal))
    idx = [0, 90, 255]
    worst = 0.0
    for c in range(filt.n_cal):
        ref = brute_force_calibrated(ctx, s["hT"], s["cal"], c, s["N_shift"], idx)
        worst = max(worst, worst_vs_brute(ref, rho, Uc, Vc, c, T, idx, list(s["hT"]), qs, us))
    # the realizations differ from one another and from the uncalibrated terms by far more
    spread = max(np.max(np.abs(Uc[c][((2, 2), (2, 2))] - U[((2, 2), (2, 2))])) for c in range(filt.n_cal)) / us
    print("[td-gs cal b] ringdown piece (%s solver, methods %s): per-realization Q_c, U_c, V_c vs dense brute force "
          "(full N-tap FD multiplication) worst %.2e relative; reach %d taps; |U_c - U| up to %.2e"
          % ("dense" if dense_max_n else "gs", ctx.uv_routes_cal, worst, ctx.calibration_last["reach"], spread))
    assert worst < 1e-8
    assert spread > 1e-2


def test_per_realization_vs_brute_force_two_pieces(ringdown_case):
    s = ringdown_case
    tpk = s["tpk"]
    pieces = [(tpk - 0.15, tpk - 0.05), (tpk - 0.01, tpk + 0.04)]
    ctx = tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=pieces, ifo="H1", verbose=False)
    filt = ctx.calibration_filters(s["cal"], verbose=False)
    T = s["N_window"]
    rho, U, V, Uc, Vc = ctx.precompute(s["hT"], s["N_shift"], T, calibration=filt)
    qs = np.max(np.abs(rho[(2, 2)].data.data))
    us = max(np.max(np.abs(Uc[c][((2, 2), (2, 2))])) for c in range(filt.n_cal))
    idx = [0, 128, 255]
    worst = 0.0
    for c in range(filt.n_cal):
        ref = brute_force_calibrated(ctx, s["hT"], s["cal"], c, s["N_shift"], idx)
        worst = max(worst, worst_vs_brute(ref, rho, Uc, Vc, c, T, idx, list(s["hT"]), qs, us))
    print("[td-gs cal b] two spliced pieces: worst %.2e relative vs dense block-diagonal brute force" % worst)
    assert worst < 1e-8


def test_reach_truncation_is_exact_and_forced_L_is_not(ringdown_case):
    s = ringdown_case
    T = s["N_window"]
    ctx = tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=s["pieces"], ifo="H1", verbose=False)
    filt = ctx.calibration_filters(s["cal"], verbose=False)
    rho, U, V, Uc, Vc = ctx.precompute(s["hT"], s["N_shift"], T, calibration=filt)
    R = ctx.calibration_last["reach"]
    assert 0 < R < s["N"] // 2
    us = np.max(np.abs(Uc[0][((2, 2), (2, 2))]))
    out = {}
    for L in (s["N"] // 2 - 1, R, max(R // 4, 8)):
        ctx.calibration_fir_half_length = L
        ctx._calibration_cache = None
        f2 = ctx.calibration_filters(s["cal"], verbose=False)
        rho2, _, _, Uc2, Vc2 = ctx.precompute(s["hT"], s["N_shift"], T, calibration=f2)
        np.testing.assert_array_equal(rho2[(2, 2)].data.data, rho[(2, 2)].data.data)   # Q_c always uses all N taps
        out[L] = max(np.max(np.abs(Uc2[c][k] - Uc[c][k])) for c in range(filt.n_cal) for k in Uc[0]) / us
    print("[td-gs cal b] U_c vs the exact reach truncation (R = %d): L = N/2-1 %.1e, L = R %.1e, L = R/4 %.1e"
          % (R, out[s["N"] // 2 - 1], out[R], out[max(R // 4, 8)]))
    assert out[s["N"] // 2 - 1] < 1e-13 and out[R] == 0.0
    assert out[max(R // 4, 8)] > 1e-6
    # taps: the N-tap filter RIFT builds from the realization (convention, packing) has the
    # realization itself as its grid response
    taps_all, L_neg, _ = filt.taps_for(s["N"])
    from RIFT.likelihood.td_calibration import realizations_to_positive
    H = fir_grid_response(taps_all.T, L_neg, s["N"])
    C_pos = realizations_to_positive(s["cal"])
    Cc = C_pos.copy()
    Cc[0] = Cc[0].real
    Cc[-1] = Cc[-1].real
    assert np.max(np.abs(H - Cc)) < 1e-12


# --------------------------------------------------------------------------- (c) NoLoop calmarg

@pytest.fixture(scope="module")
def network_case(ringdown_case, env_path):
    """Two detectors, three realizations each, packed as ILE packs them."""
    s = ringdown_case
    dets = ["H1", "L1"]
    n_cal = 3
    cal = {d: realizations(os.path.join(env_path, "%s.txt" % d), s["N"], 20., 1500., n_cal, seed=20 + i) for i, d in enumerate(dets)}
    packed = {}
    for label, hT in (("all", s["hT"]), ("22", {k: s["hT"][k] for k in [(2, 2), (2, -2)]})):
        rholms, ct, ctV, ct_c, ctV_c = {}, {}, {}, {}, {}
        for d in dets:
            ctx = tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=s["pieces"], ifo=d, verbose=False)
            filt = ctx.calibration_filters(cal[d], verbose=False)
            rholms[d], ct[d], ctV[d], ct_c[d], ctV_c[d] = ctx.precompute(hT, s["N_shift"], s["N_window"], calibration=filt)
        packed[label] = pack_calibrated(rholms, ct, ctV, ct_c, ctV_c)
    # phase marginalization needs the (2,2), (2,-2) pair only
    return dict(dets=dets, n_cal=n_cal, packed=packed["all"], packed22=packed["22"], T=s["N_window"], tpk=s["tpk"], cal=cal)


def _per_realization_inputs(nc, c, packed=None):
    """Block c of the rholms and realization c's U_c(t), V_c(t) as the base time-indexed terms."""
    lookupNK, rhoA, ctU, ctV, epoch, ctU_cal, ctV_cal = nc["packed"] if packed is None else packed
    T = nc["T"]
    rhoA_c = {d: rhoA[d][:, c * T:(c + 1) * T] for d in nc["dets"]}
    U_c = {d: ctU_cal[d][c] for d in nc["dets"]}
    V_c = {d: ctV_cal[d][c] for d in nc["dets"]}
    return lookupNK, rhoA_c, U_c, V_c, epoch


@pytest.mark.parametrize("interp", ["nearest", "cubic"])
@pytest.mark.parametrize("pm", [False, True])
def test_noloop_calmarg_vs_brute_force(network_case, interp, pm):
    nc = network_case
    packed = nc["packed22"] if pm else nc["packed"]
    lookupNK, rhoA, ctU, ctV, epoch, ctU_cal, ctV_cal = packed
    n_cal = nc["n_cal"]
    P = extrinsic_vector(7, nc["tpk"], seed=3)
    tvals = np.linspace(-0.004, 0.004, 33)
    log_w = np.log(np.array([0.5, 1.7, 0.8]))
    kw = dict(Lmax=2, xpy=np, time_interp=interp, phase_marginalization=pm)
    lnL = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctV, epoch, n_cal=n_cal,
                                                              ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal, **kw)
    lnL_w = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctV, epoch, n_cal=n_cal,
                                                                ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal,
                                                                cal_log_weights=log_w, **kw)
    lnLt = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctV, epoch, n_cal=n_cal,
                                                               ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal,
                                                               return_lnLt=True, **kw)
    comps = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctV, epoch, n_cal=n_cal,
                                                                ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal,
                                                                return_cal_components=True, **kw)
    # brute force: every realization through the n_cal=1 time-indexed path (its own Q_c block and
    # U_c(t), V_c(t) as the base terms), combined by log-sum-exp
    lnL_c, lnLt_c = [], []
    for c in range(n_cal):
        lk, rA, U_c, V_c, ep = _per_realization_inputs(nc, c, packed)
        lnL_c.append(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lk, rA, U_c, V_c, ep, **kw))
        lnLt_c.append(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lk, rA, U_c, V_c, ep, return_lnLt=True, **kw))
    lnL_c = np.array(lnL_c)                 # (n_cal, ne)
    lnLt_c = np.array(lnLt_c)               # (n_cal, ne, npts)
    ref = logsumexp(lnL_c, axis=0) - np.log(n_cal)
    ref_w = logsumexp(lnL_c + log_w[:, None], axis=0) - np.log(n_cal)
    ref_t = logsumexp(lnLt_c, axis=0) - np.log(n_cal)
    scale = np.max(np.abs(ref))
    e1 = rel_err_finite(lnL, ref)
    e2 = rel_err_finite(lnL_w, ref_w)
    # lnL(t): the streaming log-sum-exp underflows where every realization is > 700 nats below
    # the global maximum; compare where the reference is within 600 nats of it
    near = ref_t > np.max(ref_t) - 600.0
    e3 = np.max(np.abs(lnLt[near] - ref_t[near])) / np.max(np.abs(ref_t[near]))
    e4 = rel_err_finite(comps, lnL_c.T)
    print("[td-gs cal c] NoLoop calmarg (%s, phase marg %s) vs per-realization brute force: lnL %.1e, weighted %.1e, "
          "lnL(t) %.1e, components %.1e; realizations spread lnL by %.3g" % (interp, pm, e1, e2, e3, e4, np.ptp(lnL_c, axis=0).max()))
    assert e1 < 1e-12 and e2 < 1e-12 and e3 < 1e-12 and e4 < 1e-12
    assert np.ptp(lnL_c, axis=0).max() > 1e-2 * scale       # the realizations matter
    # ... and realization 1 against the independent per-sample, per-time python loop
    lk, rA, U_c, V_c, ep = _per_realization_inputs(nc, 1, packed)
    case = dict(P=P, tvals=tvals, lookupNKDict=lk, rholmsArrayDict=rA, epochDict=ep, npts=tvals.size,
                npts_extrinsic=len(P.phi), dets=nc["dets"])
    bf = ttu._brute_force(case, U_c, V_c, time_interp=interp, phase_marginalization=pm)
    e5 = rel_err_finite(bf, lnL_c[1])
    print("[td-gs cal c] realization 1 vs the python loop: %.1e" % e5)
    assert e5 < 1e-10


def test_noloop_global_norm_and_burn_in_layout(network_case):
    """calibration_self_term=False (RIFT's --calibration-global-norm): calibrated Q_c blocks with
    the one-block uncalibrated U(t); and the burn-in layout (n_cal=1 on the full arrays)."""
    nc = network_case
    lookupNK, rhoA, ctU, ctV, epoch, ctU_cal, ctV_cal = nc["packed"]
    n_cal = nc["n_cal"]
    P = extrinsic_vector(5, nc["tpk"], seed=4)
    tvals = np.linspace(-0.004, 0.004, 33)
    lnL = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctV, epoch, n_cal=n_cal, Lmax=2, xpy=np)
    ref = []
    T = nc["T"]
    for c in range(n_cal):
        rA = {d: rhoA[d][:, c * T:(c + 1) * T] for d in nc["dets"]}
        ref.append(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rA, ctU, ctV, epoch, Lmax=2, xpy=np))
    ref = logsumexp(np.array(ref), axis=0) - np.log(n_cal)
    assert rel_err_finite(lnL, ref) < 1e-12
    # burn-in: n_cal=1 with the full (n_cal x N_window) rholms and the full 4-D cal arrays -> block 0
    lnL_b = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctV, epoch, n_cal=1, Lmax=2, xpy=np,
                                                                ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal)
    lk, rA, U_c, V_c, ep = _per_realization_inputs(nc, 0)
    ref_b = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lk, rA, U_c, V_c, ep, Lmax=2, xpy=np)
    assert rel_err_finite(lnL_b, ref_b) < 1e-12
    # adaptive growth layout: concatenating blocks along time and realizations along axis 0
    rhoA2 = {d: np.concatenate([rhoA[d], rhoA[d][:, :T]], axis=-1) for d in nc["dets"]}
    ctU_cal2 = {d: np.concatenate([ctU_cal[d], ctU_cal[d][:1]], axis=0) for d in nc["dets"]}
    ctV_cal2 = {d: np.concatenate([ctV_cal[d], ctV_cal[d][:1]], axis=0) for d in nc["dets"]}
    lnL_g = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA2, ctU, ctV, epoch, n_cal=n_cal + 1, Lmax=2, xpy=np,
                                                                ctUArrayDict_cal=ctU_cal2, ctVArrayDict_cal=ctV_cal2, return_cal_components=True)
    assert lnL_g.shape == (5, n_cal + 1)
    assert rel_err_finite(lnL_g[:, n_cal], lnL_g[:, 0]) < 1e-12


# --------------------------------------------------------------------------- (d) TD vs FD calmarg

@pytest.fixture(scope="module")
def full_segment_case(env_path):
    N = 8192
    FLOW, FMAX = 20.0, 2000.0
    psd = make_psd(N)
    hT = make_modes(N, smooth_start=0.5)
    k_inj = N // 2 - 250
    dts = make_data(N, hT, k_inj)
    N_shift, N_window = k_inj - 128, 256          # 62 ms: covers the Earth-crossing delays
    dF, hF, hFc = fd_versions(dts, hT)
    n_cal = 3
    cal = realizations(os.path.join(env_path, "H1.txt"), N, FLOW, FMAX, n_cal, seed=31)
    return dict(N=N, FLOW=FLOW, FMAX=FMAX, psd=psd, hT=hT, dts=dts, dF=dF, hF=hF, hFc=hFc, N_shift=N_shift,
                N_window=N_window, cal=cal, n_cal=n_cal, tpk=peak_time(hT, k_inj))


def _fd_calibrated(s, conjugate):
    rho = fl.ComputeModeIPTimeSeries(s["hF"], s["dF"], s["psd"], s["FLOW"], s["FMAX"], FNYQ, s["N_shift"], s["N_window"],
                                     False, False, 0., calibration_realizations=s["cal"], calibration_conjugate=conjugate)
    U = fl.ComputeModeCrossTermIP(s["hF"], s["hF"], s["psd"], s["FLOW"], s["FMAX"], FNYQ, FS / s["N"], False, False, 0.,
                                  verbose=False, same_waveform_Q=True)
    V = fl.ComputeModeCrossTermIP(s["hFc"], s["hF"], s["psd"], s["FLOW"], s["FMAX"], FNYQ, FS / s["N"], False, False, 0.,
                                  verbose=False, same_waveform_Q=True, prefix="V")
    U_cal = fl.ComputeModeCrossTermIPCal(s["hF"], s["hF"], s["psd"], s["FLOW"], s["FMAX"], FNYQ, FS / s["N"], s["cal"],
                                         prefix="U", same_waveform_Q=True)
    V_cal = fl.ComputeModeCrossTermIPCal(s["hFc"], s["hF"], s["psd"], s["FLOW"], s["FMAX"], FNYQ, FS / s["N"], s["cal"],
                                         prefix="V", same_waveform_Q=True)
    return rho, U, V, U_cal, V_cal


@pytest.mark.parametrize("conjugate", [False, True])
def test_td_vs_fd_calmarg(full_segment_case, conjugate):
    s = full_segment_case
    dets = ["H1"]
    # FD code path (RIFT's calibration marginalization: realization on the data, |C|^2-weighted self term)
    rho_fd, U_fd, V_fd, Ucal_fd, Vcal_fd = _fd_calibrated(s, conjugate)
    rho_fd0 = fl.ComputeModeIPTimeSeries(s["hF"], s["dF"], s["psd"], s["FLOW"], s["FMAX"], FNYQ, s["N_shift"], s["N_window"], False, False, 0.)
    # TD code path
    ctx = tdg.TDGSDetectorContext(s["dts"], s["psd"], s["FLOW"], s["FMAX"], ifo="H1", verbose=False)
    rho_td0, U_td, V_td = ctx.precompute(s["hT"], s["N_shift"], s["N_window"])
    filt = ctx.calibration_filters(s["cal"], conjugate=conjugate, verbose=False)
    rho_td, _, _, Ucal_td, Vcal_td = ctx.precompute(s["hT"], s["N_shift"], s["N_window"], calibration=filt)
    T = s["N_window"]
    # per-realization Q_c and U_c agree to the uncalibrated TD-vs-FD level
    q_un = max(np.max(np.abs(rho_td0[m].data.data - rho_fd0[m].data.data)) for m in s["hT"]) / np.max(np.abs(rho_fd0[(2, 2)].data.data))
    q_cal = max(np.max(np.abs(rho_td[m].data.data - rho_fd[m].data.data)) for m in s["hT"]) / np.max(np.abs(rho_fd[(2, 2)].data.data))
    u22 = abs(U_fd[((2, 2), (2, 2))])
    u_un = max(abs(U_td[k][T // 2] - U_fd[k]) for k in U_fd) / u22
    u_cal = max(abs(Ucal_td[c][k][T // 2] - Ucal_fd[c][k]) for c in range(s["n_cal"]) for k in U_fd) / u22
    v_cal = max(abs(Vcal_td[c][k][T // 2] - Vcal_fd[c][k]) for c in range(s["n_cal"]) for k in V_fd) / u22
    # the likelihood: identical NoLoop reduction on both packings
    keys = list(s["hT"].keys())
    lk, lKN, _, ctU_f, ctV_f, rhoA_f, _, ep_f = fl.PackLikelihoodDataStructuresAsArrays(keys, None, rho_fd, U_fd, V_fd)
    Uc_f, Vc_f = fl.PackCalCrossTermsAsArrays(keys, lKN, Ucal_fd, Vcal_fd)
    lk_t, lKN_t, _, ctU_t, ctV_t, rhoA_t, _, ep_t = fl.PackLikelihoodDataStructuresAsArrays(keys, None, rho_td, U_td, V_td)
    Uc_t, Vc_t = fl.PackCalCrossTermsAsArrays(keys, lKN_t, Ucal_td, Vcal_td)
    assert Uc_f.shape == (s["n_cal"], 5, 5) and Uc_t.shape == (s["n_cal"], T, 5, 5)
    # The injection is loud and the NoLoop exponentiates against one maximum over all samples,
    # so evaluate one extrinsic sample per call (each against its own maximum).
    P_all = extrinsic_vector(8, s["tpk"], seed=6)
    tvals = np.linspace(-0.003, 0.003, 25)
    rhoA_f0 = fl.PackLikelihoodDataStructuresAsArrays(keys, None, rho_fd0, U_fd, V_fd)[5]
    rhoA_t0 = fl.PackLikelihoodDataStructuresAsArrays(keys, None, rho_td0, U_td, V_td)[5]
    lnL_f, lnL_t, lnL_f0, lnL_t0 = [], [], [], []
    for e in range(len(P_all.phi)):
        P = _PVec()
        for name in ("phi", "theta", "psi", "incl", "phiref", "dist"):
            setattr(P, name, getattr(P_all, name)[e:e + 1])
        P.tref, P.deltaT = P_all.tref, P_all.deltaT
        lnL_f.append(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, {"H1": lk}, {"H1": rhoA_f}, {"H1": ctU_f}, {"H1": ctV_f}, {"H1": ep_f},
                                                                         Lmax=2, xpy=np, n_cal=s["n_cal"], ctUArrayDict_cal={"H1": Uc_f}, ctVArrayDict_cal={"H1": Vc_f})[0])
        lnL_t.append(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, {"H1": lk_t}, {"H1": rhoA_t}, {"H1": ctU_t}, {"H1": ctV_t}, {"H1": ep_t},
                                                                         Lmax=2, xpy=np, n_cal=s["n_cal"], ctUArrayDict_cal={"H1": Uc_t}, ctVArrayDict_cal={"H1": Vc_t})[0])
        lnL_f0.append(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, {"H1": lk}, {"H1": rhoA_f0}, {"H1": ctU_f}, {"H1": ctV_f}, {"H1": ep_f}, Lmax=2, xpy=np)[0])
        lnL_t0.append(fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, {"H1": lk_t}, {"H1": rhoA_t0}, {"H1": ctU_t}, {"H1": ctV_t}, {"H1": ep_t}, Lmax=2, xpy=np)[0])
    lnL_f, lnL_t, lnL_f0, lnL_t0 = (np.array(x) for x in (lnL_f, lnL_t, lnL_f0, lnL_t0))
    assert np.all(np.isfinite(lnL_f)) and np.all(np.isfinite(lnL_t))
    e_un = rel_err_finite(lnL_t0, lnL_f0)
    e_cal = rel_err_finite(lnL_t, lnL_f)
    shift = rel_err_finite(lnL_f, lnL_f0)
    print("[td-gs cal d] TD vs FD (%s): Q uncalibrated %.1e, Q_c %.1e; U uncalibrated %.1e, U_c %.1e, V_c %.1e; "
          "calmarg lnL %.1e relative (uncalibrated lnL %.1e); calibration moves lnL by %.1e"
          % ("conjugate phase" if conjugate else "default convention", q_un, q_cal, u_un, u_cal, v_cal, e_cal, e_un, shift))
    assert q_cal < 3e-5 and u_cal < 3e-5 and v_cal < 3e-5
    assert e_cal < 3e-5 and e_cal < 10 * max(e_un, 1e-7)
    assert shift > 1e-3                      # the realizations change the likelihood by much more
    # the two conventions differ (the realizations' phases are not small)
    if conjugate:
        rho_fd_def = _fd_calibrated(s, False)[0]
        assert np.max(np.abs(rho_fd_def[(2, 2)].data.data - rho_fd[(2, 2)].data.data)) / np.max(np.abs(rho_fd[(2, 2)].data.data)) > 1e-3


# --------------------------------------------------------------------------- (e) conventions

def test_convention_recovers_injection(env_path):
    """Noise-free data d = K_true Re(h_22) (the code path's own N-tap filter of eta_true, template
    type): with the matched convention the (2,2) terms obey Q_c = (U_c + conj V_c)/2 at the
    injection shift (to rounding; the data are real, Re h = (h + conj h)/2, and <K h|K conj h>
    = conj V_c for real taps and a real symmetric C^{-1}); the other conventions, and the
    conjugate, do not match."""
    N = 8192
    psd = make_psd(N)
    hT = make_modes(N)
    k_inj = 3000
    eta = realizations(os.path.join(env_path, "H1.txt"), N, 20., 1500., 1, seed=41)      # eta_true
    h22 = np.asarray(hT[(2, 2)].data.data)
    # the N taps of eta_true at face value (correction 'template', no conjugation)
    from RIFT.likelihood.td_calibration import realizations_to_positive
    taps_full = fir_taps_from_response(realizations_to_positive(eta))[:, 0]
    taps, L_neg, L_pos = fir_truncate(taps_full)
    g = scipy.signal.fftconvolve(h22.real, taps)                 # g[j'] = (K h)[j' - L_neg]
    d = np.zeros(N)
    src = np.arange(g.size) - L_neg + k_inj
    ok = (src >= 0) & (src < N)
    d[src[ok]] = g[ok]
    dts = lal.CreateREAL8TimeSeries("d", lal.LIGOTimeGPS(EPOCH0), 0., DT, lsu.lsu_DimensionlessUnit, N)
    dts.data.data = d
    tpk = peak_time(hT, k_inj)
    msec = MREF * lal.MTSUN_SI
    pieces = [(tpk - 0.05, tpk + 120 * msec)]
    N_shift, T = k_inj - 16, 33
    i_inj = k_inj - N_shift
    inv_eta = 1.0 / eta
    conj_eta = np.conj(eta)
    results = {}
    for label, ifo, ctype, real, conj in [
            ("rift, eta", "H1", "rift", eta, False),                 # matched
            ("bilby H1 (data type), eta", "H1", "bilby", eta, False),   # C = 1/eta: mismatched
            ("bilby H1 (data type), 1/eta", "H1", "bilby", inv_eta, False),   # matched
            ("rift, 1/eta", "H1", "rift", inv_eta, False),           # mismatched
            ("bilby V1 (template type), eta", "V1", "bilby", eta, False),    # matched
            ("rift conjugate, conj(eta)", "H1", "rift", conj_eta, True),     # matched
            ("rift conjugate, eta", "H1", "rift", eta, True),        # mismatched
    ]:
        ctx = tdg.TDGSDetectorContext(dts, psd, 20., 1500., pieces=pieces, ifo=ifo, verbose=False,
                                      calibration_correction_type=ctype)
        filt = ctx.calibration_filters(real, conjugate=conj, verbose=False)
        rho, U, V, Uc, Vc = ctx.precompute(hT, N_shift, T, calibration=filt)
        q = rho[(2, 2)].data.data[i_inj]
        u = Uc[0][((2, 2), (2, 2))][i_inj]
        v = Vc[0][((2, 2), (2, 2))][i_inj]
        results[label] = abs(q - 0.5 * (u + np.conj(v))) / abs(u)
        print("[td-gs cal e] %-34s bilby type %-8s |Q_c - (U_c + conj V_c)/2| / U_c at the injection = %.2e"
              % (label, filt.bilby_type, results[label]))
    for label in ("rift, eta", "bilby H1 (data type), 1/eta", "bilby V1 (template type), eta", "rift conjugate, conj(eta)"):
        assert results[label] < 1e-9, label
    for label in ("bilby H1 (data type), eta", "rift, 1/eta", "rift conjugate, eta"):
        assert results[label] > 1e-3, label


def test_template_factor_conventions(env_path):
    N = 4096
    eta = realizations(os.path.join(env_path, "H1.txt"), N, 20., 1500., 2, seed=43)
    C, t = tdg.calibration_template_factor(eta, "H1", "rift")
    assert t == "template" and np.array_equal(C, eta)
    C, t = tdg.calibration_template_factor(eta, "H1", "bilby")
    assert t == "data" and np.allclose(C * eta, 1.0)
    C, t = tdg.calibration_template_factor(eta, "V1", "bilby")
    assert t == "template" and np.array_equal(C, eta)
    C, t = tdg.calibration_template_factor(eta, "L1", "bilby", conjugate=True)
    assert t == "data" and np.allclose(C * np.conj(eta), 1.0)
    with pytest.raises(ValueError):
        tdg.calibration_template_factor(eta, "H1", "lal")


# --------------------------------------------------------------------------- (f) refusals

def test_refusals(ringdown_case, network_case):
    s = ringdown_case
    nc = network_case
    ctx = tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=s["pieces"], ifo="H1", verbose=False)
    with pytest.raises(ValueError, match="expected"):
        ctx.calibration_filters(s["cal"][:-2], verbose=False)                   # wrong grid
    with pytest.raises(ValueError, match="Hermitian"):
        bad = s["cal"].copy()
        bad[5, 0] *= 1.3
        ctx.calibration_filters(bad, verbose=False)
    with pytest.raises(ValueError, match="half-length"):
        tdg.TDCalibrationFilters(ctx, s["cal"], half_length=s["N"] // 2, verbose=False)
    with pytest.raises(ValueError, match="correction_type"):
        tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=s["pieces"], ifo="H1", verbose=False,
                                calibration_correction_type="lal")
    lookupNK, rhoA, ctU, ctV, epoch, ctU_cal, ctV_cal = nc["packed"]
    P = extrinsic_vector(3, nc["tpk"])
    tvals = np.linspace(-0.002, 0.002, 9)
    call = lambda **kw: fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, ctU, ctV, epoch, Lmax=2, xpy=np, **kw)
    with pytest.raises(NotImplementedError, match="fused"):
        call(n_cal=nc["n_cal"], cal_method="fused", ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal)
    with pytest.raises(NotImplementedError, match="fused"):
        call(n_cal=nc["n_cal"], cal_method="fused")
    comp = {d: fl.compress_time_indexed_cross_terms(ctU[d], ctV[d], 1e-13) for d in nc["dets"]}
    with pytest.raises(NotImplementedError, match="compressed"):
        fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(tvals, P, lookupNK, rhoA, comp, comp, epoch, Lmax=2, xpy=np, n_cal=nc["n_cal"])
    with pytest.raises(ValueError, match="n_blocks"):
        call(n_cal=nc["n_cal"], ctUArrayDict_cal={d: ctU_cal[d][:, :-1] for d in nc["dets"]}, ctVArrayDict_cal={d: ctV_cal[d][:, :-1] for d in nc["dets"]})
    with pytest.raises(ValueError, match="realizations"):
        call(n_cal=nc["n_cal"] + 1, ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal=ctV_cal)
    with pytest.raises(ValueError, match="both"):
        call(n_cal=nc["n_cal"], ctUArrayDict_cal=ctU_cal, ctVArrayDict_cal={d: ctV_cal[d][:, 0] for d in nc["dets"]})


# --------------------------------------------------------------------------- Precompute driver

def test_precompute_driver_with_calibration(ringdown_case, env_path):
    s = ringdown_case
    N = s["N"]
    dets = ["H1", "L1"]
    data_dict = {d: s["dts"] for d in dets}
    psd_dict = {d: s["psd"] for d in dets}
    contexts = {d: tdg.TDGSDetectorContext(s["dts"], s["psd"], 20., 1500., pieces=s["pieces"], ifo=d, verbose=False) for d in dets}
    cal = {d: realizations(os.path.join(env_path, "%s.txt" % d), N, 20., 1500., 2, seed=50 + i) for i, d in enumerate(dets)}
    P = make_P(N)
    P.tref = s["tpk"]
    t_window = 0.02
    N_window = int(2 * t_window / DT)
    out = fl.PrecomputeLikelihoodTerms(s["tpk"], t_window, P, data_dict, psd_dict, 2, 1500., False, False, 0.,
                                       verbose=False, quiet=True, skip_interpolation=True, td_gs_contexts=contexts,
                                       return_calibration_crossterms=True, calibration_realizations=cal)
    rholms_intp, crossTerms, crossTermsV, rholms, guess_snr, rest, ct_cal, ct_cal_V = out
    assert np.isfinite(guess_snr) and rest is None
    for det in dets:
        assert len(ct_cal[det]) == 2 and len(ct_cal_V[det]) == 2
        for m in rholms[det]:
            assert rholms[det][m].data.length == 2 * N_window
        lk, lKN, _, ctU, ctV, rhoA, _, ep = fl.PackLikelihoodDataStructuresAsArrays(list(rholms[det].keys()), None, rholms[det], crossTerms[det], crossTermsV[det])
        U_cal, V_cal = fl.PackCalCrossTermsAsArrays(list(rholms[det].keys()), lKN, ct_cal[det], ct_cal_V[det])
        n = len(rholms[det])
        assert ctU.shape == (N_window, n, n) and U_cal.shape == (2, N_window, n, n) and rhoA.shape == (n, 2 * N_window)
        i22 = lKN[(2, 2)]
        np.testing.assert_array_equal(U_cal[1, :, i22, i22], ct_cal[det][1][((2, 2), (2, 2))])
        # the cached filters are reused for the same realization array
        assert contexts[det].calibration_filters(cal[det], verbose=False) is contexts[det].calibration_filters(cal[det], verbose=False)
    # global norm: no per-realization self terms, calibrated Q blocks
    out2 = fl.PrecomputeLikelihoodTerms(s["tpk"], t_window, P, data_dict, psd_dict, 2, 1500., False, False, 0.,
                                        verbose=False, quiet=True, skip_interpolation=True, td_gs_contexts=contexts,
                                        return_calibration_crossterms=True, calibration_realizations=cal, calibration_self_term=False)
    assert out2[6] is None and out2[7] is None
    for det in dets:
        for m in out2[3][det]:
            np.testing.assert_array_equal(out2[3][det][m].data.data, rholms[det][m].data.data)
    # the conjugate option changes Q_c
    out3 = fl.PrecomputeLikelihoodTerms(s["tpk"], t_window, P, data_dict, psd_dict, 2, 1500., False, False, 0.,
                                        verbose=False, quiet=True, skip_interpolation=True, td_gs_contexts=contexts,
                                        return_calibration_crossterms=True, calibration_realizations=cal, calibration_conjugate=True)
    d = np.max(np.abs(out3[3]["H1"][(2, 2)].data.data - rholms["H1"][(2, 2)].data.data)) / np.max(np.abs(rholms["H1"][(2, 2)].data.data))
    assert d > 1e-3
    # the uncalibrated six-value return is unchanged
    out0 = fl.PrecomputeLikelihoodTerms(s["tpk"], t_window, P, data_dict, psd_dict, 2, 1500., False, False, 0.,
                                        verbose=False, quiet=True, skip_interpolation=True, td_gs_contexts=contexts)
    assert len(out0) == 6
    for det in dets:
        for k in out0[1][det]:
            np.testing.assert_array_equal(out0[1][det][k], crossTerms[det][k])
    with pytest.raises(ValueError, match="no calibration realizations"):
        fl.PrecomputeLikelihoodTerms(s["tpk"], t_window, P, data_dict, psd_dict, 2, 1500., False, False, 0.,
                                     verbose=False, quiet=True, skip_interpolation=True, td_gs_contexts=contexts,
                                     calibration_realizations={"H1": cal["H1"]})


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))
