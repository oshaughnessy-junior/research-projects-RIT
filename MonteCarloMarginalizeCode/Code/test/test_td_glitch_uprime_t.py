"""Arrival-time-dependent glitch U'(t) and the sigma_g grid (td-toeplitz glitch path).

U'(t) = U - beta(t)^T M^-1 conj(beta(t)) (paper Eq. Uprime) is checked against a dense
evaluation of the Woodbury-corrected inner product of the explicitly shifted template, at
several shifts, for U and V, through the helper and through PrecomputeLikelihoodTerms.  The
sigma_g grid is checked against direct recomputation at each node (Q', U'(t)) and its
evidence against brute-force quadrature of the dense Gaussian marginal on a toy.
"""
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import scipy.linalg
from scipy.special import logsumexp

import lal
import lalsimulation as lalsim
import RIFT.lalsimutils as lsu
import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood import td_rank_augmentation as tra
from RIFT.likelihood.td_inner_product import (
    TDInnerProductFactory, ToeplitzComplexIP, _ifft_to_td, glitch_grid_logsumexp,
    glitch_sigma_grid_nodes, glitch_time_indexed_cross_terms)

FS = 2048.0
FNYQ = FS / 2.0
N = 4096
DELTAF = FS / N
DELTAT = 1.0 / FS
LEN1 = N // 2 + 1
FLOW, FMAX = 20.0, 900.0
STRAIN = 1.0e-21


def _psd():
    psd = lal.CreateREAL8FrequencySeries("psd", lal.LIGOTimeGPS(0.), 0., DELTAF,
                                         lsu.lsu_HertzUnit, LEN1)
    f = np.maximum(np.arange(LEN1) * DELTAF, 1.0)
    psd.data.data = 1e-46 * (1.0 + (20.0 / f) ** 4 + (f / 250.0) ** 2)
    return psd


def _series(td, epoch=0.0):
    ts = lal.CreateCOMPLEX16TimeSeries("x", lal.LIGOTimeGPS(epoch), 0., DELTAT,
                                       lsu.lsu_DimensionlessUnit, N)
    ts.data.data = np.asarray(td, dtype=complex)
    return lsu.DataFourier(ts)


def _chirp(t0, t1, f0, f1, tau, amp):
    tg = np.arange(N) * DELTAT
    ph = 2 * np.pi * (f0 * (tg - t0) + 0.5 * (f1 - f0) / (t1 - t0) * (tg - t0) ** 2)
    x = amp * np.exp(-((tg - 0.5 * (t0 + t1)) ** 2) / (2 * tau ** 2)) * np.cos(ph)
    x[(tg < t0) | (tg > t1)] = 0.0
    return x.astype(complex)


def _blip(t0, f0, q, amp):
    tg = np.arange(N) * DELTAT
    tau = q / (2 * np.pi * f0)
    return (amp * np.exp(-((tg - t0) ** 2) / (2 * tau ** 2))
            * np.cos(2 * np.pi * f0 * (tg - t0))).astype(complex)


def _context(sigma="profile", grid=None, uprime="time-indexed"):
    """An on-track case: the basis support overlaps the template's high-frequency end."""
    psd = _psd()
    h22 = _chirp(0.6, 1.4, 40.0, 350.0, 0.25, STRAIN)
    h2m2 = 0.5 * np.conj(_chirp(0.65, 1.38, 45.0, 330.0, 0.22, STRAIN)) * 1j
    hlms = {(2, 2): _series(h22), (2, -2): _series(h2m2)}
    hlms_conj = {m: _series(np.conj(_ifft_to_td(hlms[(m[0], -m[1])]))) for m in hlms}
    d = 6 * h22 + _blip(1.2, 250., 6., 80 * STRAIN)
    data = _series(d)
    fac = TDInnerProductFactory(
        solver="auto", window_shape=0.1, glitch_uprime=uprime, glitch_sigma=sigma,
        glitch_sigma_grid_snr=grid,
        glitch_specs={"H1": dict(t1=1.1, t2=1.3, f1=150., f2=350., r=12)})
    fac.register_detector_data("H1", psd, data, event_time_geo=None)
    ctx = fac.context_for(FLOW, FMAX, FNYQ, DELTAF, psd)
    return SimpleNamespace(psd=psd, hlms=hlms, hlms_conj=hlms_conj, data=data, fac=fac, ctx=ctx)


def _beta(s, hl, n_shift=0, n_window=N):
    raw = s.fac.uncorrected(s.ctx)
    cols = [fl.ComputeModeIPTimeSeries(hl, Bj, s.psd, FLOW, FMAX, FNYQ, n_shift, n_window,
                                       IP_factory=raw) for Bj in s.ctx.glitch_basis_fd()]
    return {m: np.stack([np.asarray(c[m].data.data) for c in cols], axis=-1) for m in hl}


def _dense_uprime(ctx, u_td, v_td):
    """Woodbury-corrected Hermitian ip of two already-shifted TD templates."""
    return (ctx.ip_normalization * ctx.dot_td(u_td, v_td)
            - ctx.glitch_ip_correction(u_td, v_td))


SHIFTS = [-300, -120, -40, 0, 25, 90, 260]


def test_uprime_t_matches_dense_shifted_template():
    s = _context()
    base = ToeplitzComplexIP(s.ctx, glitch_correct=False)
    U0 = {(a, b): base.ip(s.hlms[a], s.hlms[b]) for a in s.hlms for b in s.hlms}
    V0 = {(a, b): base.ip(s.hlms_conj[a], s.hlms[b]) for a in s.hlms for b in s.hlms}
    beta, beta_c = _beta(s, s.hlms), _beta(s, s.hlms_conj)
    U_t, V_t = glitch_time_indexed_cross_terms(U0, V0, beta, beta_c, s.ctx.glitch_Minv_apply)
    td = {m: _ifft_to_td(h) for m, h in s.hlms.items()}
    tdc = {m: _ifft_to_td(h) for m, h in s.hlms_conj.items()}
    frac = []
    for n in SHIFTS:
        for a in s.hlms:
            for b in s.hlms:
                ref_u = _dense_uprime(s.ctx, np.roll(td[a], n), np.roll(td[b], n))
                ref_v = _dense_uprime(s.ctx, np.roll(tdc[a], n), np.roll(td[b], n))
                scale = abs(U0[((2, 2), (2, 2))])
                assert abs(U_t[(a, b)][n % N] - ref_u) < 1e-11 * scale
                assert abs(V_t[(a, b)][n % N] - ref_v) < 1e-11 * scale
        k = ((2, 2), (2, 2))
        frac.append(1 - np.real(U_t[k][n % N] / U0[k]))
    # The fixture has teeth: the absorbed fraction depends on the shift, so one
    # reference-time value cannot reproduce the dense U'(t).
    assert np.ptp(frac) > 0.05, frac


def _imr_fixture(uprime, sigma="profile", grid=None):
    """A real IMRPhenomD Precompute with an on-track Slepian glitch basis in H1.

    ``hlms`` holds the modes Precompute generated (captured from internal_hlm_generator),
    so the dense check sees exactly the templates the wiring used."""
    tref = 1000000000.0 + 2.0
    P = lsu.ChooseWaveformParams(m1=35 * lal.MSUN_SI, m2=30 * lal.MSUN_SI, fmin=35.0,
                                 deltaT=DELTAT, deltaF=DELTAF, dist=400e6 * lal.PC_SI,
                                 approx=lalsim.IMRPhenomD)
    P.tref = tref
    psd = _psd()
    epoch = tref - 1.0
    d_td = _blip(tref - 0.005 - epoch, 220., 6., 60 * STRAIN)
    data = _series(d_td, epoch=epoch)
    fac = TDInnerProductFactory(
        solver="auto", window_shape=0.1, glitch_uprime=uprime, glitch_sigma=sigma,
        glitch_sigma_grid_snr=grid,
        glitch_specs={"H1": dict(t1=-0.06, t2=0.03, f1=60., f2=400., r=None)})
    fac.register_detector_data("H1", psd, data, event_time_geo=tref)
    t_window = 0.05
    captured = {}
    gen = fl.internal_hlm_generator

    def _capture(*a, **k):
        captured["hlms"] = gen(*a, **k)
        return captured["hlms"]
    fl.internal_hlm_generator = _capture
    try:
        out = fl.PrecomputeLikelihoodTerms(tref, t_window, P, {"H1": data}, {"H1": psd}, 2,
                                           FMAX, False, False, 0., verbose=False, quiet=True,
                                           skip_interpolation=True, IP_factory=fac,
                                           return_calibration_crossterms=True)
    finally:
        fl.internal_hlm_generator = gen
    return SimpleNamespace(P=P, psd=psd, data=data, fac=fac, tref=tref, t_window=t_window,
                           out=out, ctx=fac.context_for(P.fmin, FMAX, FNYQ, DELTAF, psd),
                           hlms=captured["hlms"][0])


def _hlms_for(s):
    return s.hlms, None


def test_precompute_emits_time_indexed_uprime_matching_dense():
    s = _imr_fixture("time-indexed")
    _, ct, ctV, rholms, guess_snr, _, _, _ = s.out
    assert np.isfinite(guess_snr)
    hlms, _ = _hlms_for(s)
    t_det = fl.ComputeArrivalTimeAtDetector("H1", s.P.phi, s.P.theta, s.tref)
    rho_epoch = s.data.epoch - hlms[(2, 2)].epoch
    N_shift = int(float(float(t_det) - s.t_window - float(rho_epoch)) / DELTAT + 0.5)
    N_window = int(2 * s.t_window / DELTAT)
    k = ((2, 2), (2, 2))
    assert ct["H1"][k].shape == (N_window,) and ctV["H1"][k].shape == (N_window,)
    h22 = _ifft_to_td(hlms[(2, 2)])
    U0 = np.real(ToeplitzComplexIP(s.ctx, glitch_correct=False).ip(hlms[(2, 2)], hlms[(2, 2)]))
    fr = []
    for i in [0, N_window // 4, N_window // 2, N_window - 1]:
        hs = np.roll(h22, i + N_shift)
        ref = _dense_uprime(s.ctx, hs, hs)
        assert abs(ct["H1"][k][i] - ref) < 1e-10 * U0
        fr.append(1 - np.real(ref) / U0)
    assert np.ptp(fr) > 1e-3, fr
    # NoLoop consumes the packed result as a time-indexed self term.
    packed = fl.PackLikelihoodDataStructuresAsArrays(list(rholms["H1"].keys()), None,
                                                     rholms["H1"], ct["H1"], ctV["H1"])
    assert packed[3].shape == (N_window, len(rholms["H1"]), len(rholms["H1"]))


def test_reference_mode_keeps_scalar_cross_terms_and_same_Q():
    a = _imr_fixture("reference")
    b = _imr_fixture("time-indexed")
    k = ((2, 2), (2, 2))
    assert np.ndim(a.out[1]["H1"][k]) == 0
    for m in a.out[3]["H1"]:
        np.testing.assert_array_equal(a.out[3]["H1"][m].data.data, b.out[3]["H1"][m].data.data)


def test_grid_nodes_match_direct_recomputation():
    s = _imr_fixture("time-indexed", sigma="grid", grid=(5.0, 80.0, 4))
    _, ct, ctV, rholms, _, _, _, _ = s.out
    modes = list(rholms["H1"].keys())
    packed = fl.PackLikelihoodDataStructuresAsArrays(modes, None, rholms["H1"], ct["H1"], ctV["H1"])
    lookupKN, rhoArr = packed[1], packed[5]
    offsets, det, sets = s.fac.glitch_grid_sets({"H1": lookupKN}, {"H1": rhoArr})
    ctx = s.ctx
    grid = ctx.glitch_sigma_grid
    assert len(sets) == 4 and det == "H1"
    active = ctx._glitch_sigma_g
    lnw = [g[1] for g in grid]
    np.testing.assert_allclose(np.exp(lnw).sum(), 1.0, rtol=1e-14)
    hlms, _ = _hlms_for(s)
    t_det = fl.ComputeArrivalTimeAtDetector("H1", s.P.phi, s.P.theta, s.tref)
    N_shift = int(float(float(t_det) - s.t_window - float(s.data.epoch - hlms[(2, 2)].epoch))
                  / DELTAT + 0.5)
    N_window = int(2 * s.t_window / DELTAT)
    h22 = _ifft_to_td(hlms[(2, 2)])
    i22 = lookupKN[(2, 2)]
    dQ = []
    try:
        for (sig, lw, dz), off, (Qd, Ud, _) in zip(grid, offsets, sets):
            ctx._glitch_sigma_g = sig
            ctx.set_active_glitch_sign(+1.0)
            Q_direct = fl.ComputeModeIPTimeSeries(hlms, s.data, s.psd, s.P.fmin, FMAX, FNYQ, N_shift,
                                                  N_window, IP_factory=s.fac)
            q = np.asarray(Q_direct[(2, 2)].data.data)
            np.testing.assert_allclose(Qd["H1"][i22], q, rtol=0, atol=1e-9 * np.max(np.abs(q)))
            dQ.append(np.max(np.abs(Qd["H1"][i22] - rhoArr[i22])))
            for i in [0, N_window // 2]:
                hs = np.roll(h22, i + N_shift)
                ref = _dense_uprime(ctx, hs, hs)
                assert abs(Ud["H1"][i, i22, i22] - ref) < 1e-10 * abs(ref)
            np.testing.assert_allclose(off, lw + dz - grid[[g[0] for g in grid].index(active)][2],
                                       rtol=0, atol=1e-9)
    finally:
        ctx._glitch_sigma_g = active
        ctx.set_active_glitch_sign(+1.0)
    # The nodes differ: a grid of identical nodes would make this test vacuous.
    assert sorted(dQ)[-1] > 1e-3 * np.max(np.abs(rhoArr[i22]))


def test_noloop_grid_combination_equals_logsumexp_of_nodes():
    """The ILE helper's combination: identical nodes reproduce lnL + ln(sum w)."""
    rng = np.random.default_rng(3)
    lnL = rng.normal(size=7) * 5
    offs = np.log([0.2, 0.3, 0.5])
    np.testing.assert_allclose(glitch_grid_logsumexp([lnL] * 3, offs), lnL, atol=1e-12)
    a, b = lnL, lnL - 2.0
    np.testing.assert_allclose(glitch_grid_logsumexp([a, b], [0.1, -0.4]),
                               np.logaddexp(a + 0.1, b - 0.4), atol=1e-12)


def _toy(seed=11, n=160, r=4):
    rng = np.random.default_rng(seed)
    acf = np.exp(-np.arange(n) / 3.0) * np.cos(np.arange(n) * 0.7) + 1e-3 * (np.arange(n) == 0)
    C = scipy.linalg.toeplitz(acf)
    t = np.arange(n)
    B = np.stack([np.exp(-((t - 90) / 6.0) ** 2) * np.cos(0.4 * (j + 1) * (t - 90))
                  for j in range(r)], axis=1)
    lam0 = np.array([1.0, 0.6, 0.3, 0.15])
    h = 0.8 * np.sin(0.25 * t) * np.exp(-((t - 80) / 30.0) ** 2)
    a = rng.normal(size=r) * np.sqrt(lam0) * 3.0
    d = h + B.dot(a) + np.linalg.cholesky(C).dot(rng.normal(size=n))
    return C, B, lam0, h, d


class _Dense(object):
    def __init__(self, C):
        self.C, self.N = C, C.shape[0]
        self._cho = scipy.linalg.cho_factor(C)

    def solve(self, b):
        return scipy.linalg.cho_solve(self._cho, b)

    def dot(self, u, v):
        return np.tensordot(u, self.solve(v), axes=([-1], [0]))

    def logdet(self):
        return 2 * np.sum(np.log(np.diag(self._cho[0])))


def _lnL_full(C, B, lam0, sig, d, h):
    """ln N(d - h; 0, C + sig^2 B Lam0 B^T) - ln N(d; 0, C): dense, no Woodbury."""
    Cp = C + sig ** 2 * B.dot(np.diag(lam0)).dot(B.T)
    r = d - h
    lp = -0.5 * r.dot(np.linalg.solve(Cp, r)) - 0.5 * np.linalg.slogdet(Cp)[1]
    l0 = -0.5 * d.dot(np.linalg.solve(C, d)) - 0.5 * np.linalg.slogdet(C)[1]
    return lp - l0


def test_sigma_grid_evidence_against_bruteforce_quadrature():
    C, B, lam0, h, d = _toy()
    aug = tra.RankAugmentation(_Dense(C), [(B, lam0, "toy")])
    snr_scale = float(np.sqrt(np.trace(np.diag(lam0).dot(aug.blocks[0]["G"]))))
    lo, hi = 0.5, 50.0

    def rift_terms(sig):
        # RIFT's split: lnL(theta; sig) + Delta lnZ(sig), with the Woodbury pieces only.
        q = np.real(aug.dot(h, d, sig))
        u = np.real(aug.dot(h, h, sig))
        return q - 0.5 * u + aug.delta_lnZ(d, sig)

    for sig in [0.3 / snr_scale, 3.0 / snr_scale, 30.0 / snr_scale]:
        np.testing.assert_allclose(rift_terms(sig), _lnL_full(C, B, lam0, sig, d, h),
                                   rtol=0, atol=1e-9)
    u = np.linspace(np.log(lo / snr_scale), np.log(hi / snr_scale), 20001)
    vals = np.array([_lnL_full(C, B, lam0, np.exp(x), d, h) for x in u])
    w = np.full(u.size, 1.0); w[0] = w[-1] = 0.5
    lnZ_ref = logsumexp(vals, b=w / w.sum())

    def grid_lnZ(K):
        nodes = glitch_sigma_grid_nodes(snr_scale, lo, hi, K)
        dz_active = aug.delta_lnZ(d, nodes[0][0])
        lnL_k = [np.atleast_1d(rift_terms(sg) - aug.delta_lnZ(d, sg)) for sg, _ in nodes]
        offs = [lw + aug.delta_lnZ(d, sg) - dz_active for sg, lw in nodes]
        return float(glitch_grid_logsumexp(lnL_k, offs)[0] + dz_active)

    err = {K: abs(grid_lnZ(K) - lnZ_ref) for K in (3, 6, 12, 24)}
    print("[sigma grid] |lnZ_grid - lnZ_quad| by K:", err)
    assert err[12] < 0.02 and err[24] < 0.005
    assert err[3] > err[24]


@pytest.mark.parametrize("flags,message", [
    (["--glitch-sigma", "grid", "--glitch-basis", "H1:0,.1,40,100"],
     "--glitch-sigma grid requires a glitch basis and --glitch-uprime time-indexed"),
    (["--glitch-uprime", "time-indexed"],
     "--glitch-uprime time-indexed requires --glitch-basis or --glitch-basis-file"),
    (["--glitch-uprime", "time-indexed", "--glitch-basis", "H1:0,.1,40,100"],
     "--glitch-uprime time-indexed requires --time-marginalization --vectorized"),
    (["--glitch-uprime", "time-indexed", "--glitch-basis", "H1:0,.1,40,100",
      "--time-marginalization", "--vectorized", "--gpu", "--force-xpy",
      "--glitch-sigma", "grid"],
     "--glitch-sigma grid requires --glitch-sigma-grid-snr LO,HI,K"),
])
def test_ile_refuses_unsupported_glitch_uprime_requests(flags, message, tmp_path):
    code = Path(__file__).resolve().parents[1]
    env = dict(os.environ, GW_SURROGATE="", OMP_NUM_THREADS="1",
               OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", PYTHONPATH=str(code))
    proc = subprocess.run([sys.executable, str(code / "bin/integrate_likelihood_extrinsic_batchmode"),
                           "--internal-ile-inner-product", "td-toeplitz", *flags],
                          cwd=tmp_path, env=env, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=600)
    assert proc.returncode != 0
    assert message in proc.stdout, proc.stdout[-3000:]


def test_ile_phase_marginalization_guard_covers_glitch_uprime():
    """The startup refusal of a phase-marginalization table is keyed on the shared flag."""
    import ast
    src = (Path(__file__).resolve().parents[1]
           / "bin/integrate_likelihood_extrinsic_batchmode").read_text()
    tree = ast.parse(src)
    node = next(n for n in tree.body if isinstance(n, ast.If) and isinstance(n.test, ast.BoolOp)
                and {v.id for v in n.test.values if isinstance(v, ast.Name)}
                == {"_time_indexed_uv_requested", "_tq_phase_marg"})
    code = compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                   "guard", "exec")
    with pytest.raises(ValueError, match="explicit orbital-phase sampling"):
        exec(code, dict(_time_indexed_uv_requested=True, _tq_phase_marg=True,
                        _time_indexed_uv_source="--glitch-uprime time-indexed"))
    exec(code, dict(_time_indexed_uv_requested=False, _tq_phase_marg=True,
                    _time_indexed_uv_source=""))
