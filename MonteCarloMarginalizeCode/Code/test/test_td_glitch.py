"""
test_td_glitch.py:  Acceptance tests for glitch (rank-augmentation) marginalization wired
into the RIFT time-domain Toeplitz inner product (task G3 of GLITCH_MARG_DESIGN.md; builds
on G1 td_rank_augmentation.py and the T1/T5 td_inner_product.py context).

The algebra (documented in ToeplitzInnerProductContext.attach_glitch_basis): the base TD
matched filter uses the PRECISION operator P = ip_normalization*K (K = band-limited inverse-
PSD kernel), so C^{-1} = P and the covariance-side Woodbury becomes a DIRECT precision matvec
(Phi = P B, NO solve).  M = Lambda^{-1} + B^T P B is always well-posed (floored by the prior),
so the augmentation is defined even though P is rank-deficient off-band (the glitch basis is
band-limited, living where P is well-conditioned).  Corrected ingredients:

    U'_{lm,l'm'} = U - conj(Phi^T h_lm)^T M^{-1} (Phi^T h_l'm')            (Hermitian scalar)
    Q'_lm(t)     = correlate(h_lm, w'),  w' = P(d - g_hat), g_hat = B M^{-1} gamma
    Delta lnZ    = 1/2 gamma^H M^{-1} gamma - 1/2 ln det(I + sigma_g^2 Lambda_0 G)  (per det)

Groups:
  (a) DENSE PARITY -- corrected U'/g_hat/Delta lnZ vs a brute-force dense
      (C + sigma^2 B Lambda_0 B^T)^{-1} construction.  Because a BAND-LIMITED precision is
      rank-deficient off-band (the off-band ambiguity the design flags), the dense parity is
      done in two rigorous halves: (a1) FORMULA vs BRUTE-FORCE with a genuinely invertible
      covariance C (physical ACF Toeplitz, C = P^{-1} exactly), proving the Woodbury-via-
      precision formula equals the true covariance-augmentation inverse to ~1e-12; (a2)
      IMPLEMENTATION vs the dense formula with the ACTUAL band-limited precision P = ip_norm*K,
      proving the FFT-matvec + scaled-M_hat + conjugation implementation matches straightforward
      dense to ~1e-9 (relative).
  (b) CONJUGATION CORRECTNESS -- the Hermitian corrected ip matches the dense conj-explicit
      reference; the (wrong) bilinear correction is O(1) off (the bilinear-vs-Hermitian trap).
  (c) END-TO-END through the REAL Precompute inner-product functions (fl.ComputeModeIPTimeSeries
      / fl.ComputeModeCrossTermIP): a "signal" template + injected sine-Gaussian blip overlapping
      it in one detector; the glitch-marginalized matched-filter lnL(t) is closer to the glitch-
      free reference than the unmitigated run.  Plus default-OFF byte-identical regression.

Environment: ~/.conda/envs/junior_rift/bin/python with
PYTHONPATH=<worktree>/MonteCarloMarginalizeCode/Code.  Runnable as
    pytest test_td_glitch.py
    python  test_td_glitch.py
"""

import numpy as np
import scipy.linalg

import lal
import RIFT.lalsimutils as lsu
import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood import td_rank_augmentation as tra
from RIFT.likelihood.td_inner_product import (
    ToeplitzInnerProductContext, ToeplitzComplexIP, ToeplitzComplexOverlap,
    TDInnerProductFactory,
)

# --------------------------------------------------------------------------- fixtures

FS = 2048.0
FNYQ = FS / 2.0
N = 4096                      # two-sided length (2 s)
DELTAF = FS / N
DELTAT = 1.0 / FS
LEN1 = N // 2 + 1
FLOW, FMAX = 20.0, 900.0

# glitch time-frequency support (seconds from sample 0, Hz).  For the end-to-end test this
# cell is chosen DISJOINT from the chirp signal's TF track (early time + high frequency, where
# the 40->350 Hz chirp has no power) so the basis removes the blip WITHOUT eating the signal
# (the single-detector-basis-eats-signal failure mode the design warns about).
G_T1, G_T2, G_F1, G_F2 = 0.55, 0.70, 250.0, 380.0

STRAIN = 1.0e-21              # physical amplitude scale (keeps sigma_g profiling in-range)


def colored_psd(f):
    f = np.atleast_1d(np.asarray(f, dtype=float))
    fsafe = np.maximum(np.abs(f), 1.0)
    out = 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)
    return out if out.size > 1 else float(out[0])


def make_psd_series():
    psd = lal.CreateREAL8FrequencySeries(
        "psd", lal.LIGOTimeGPS(0.), 0., DELTAF, lsu.lsu_HertzUnit, LEN1)
    psd.data.data = colored_psd(np.arange(LEN1) * DELTAF)
    return psd


def series_from_td(td_array, epoch=0.0):
    """Forward-FFT a TD numpy array to a COMPLEX16FrequencySeries (lal packing)."""
    ts = lal.CreateCOMPLEX16TimeSeries(
        "x", lal.LIGOTimeGPS(epoch), 0., DELTAT, lsu.lsu_DimensionlessUnit, N)
    ts.data.data = np.asarray(td_array, dtype=complex)
    return lsu.DataFourier(ts)


def bandlimited_series(rng, epoch=0.0, scale=STRAIN):
    f1 = np.arange(LEN1) * DELTAF
    s = np.zeros(LEN1, dtype=complex)
    band = (f1 >= FLOW) & (f1 <= FMAX)
    s[band] = rng.standard_normal(band.sum()) + 1j * rng.standard_normal(band.sum())
    h2 = np.zeros(N, dtype=complex)
    h2[:LEN1] = s[::-1]
    h2[LEN1 - 1:] = s[:-1]
    hf = lal.CreateCOMPLEX16FrequencySeries(
        "h", lal.LIGOTimeGPS(epoch), 0., DELTAF, lsu.lsu_HertzUnit, N)
    hf.data.data = scale * h2
    return hf


def chirp_td(t0, t1, f0, f1_, tau, amp):
    """A tapered linear chirp 'signal' template (band-limited, with a clear peak)."""
    tg = np.arange(N) * DELTAT
    phase = 2 * np.pi * (f0 * (tg - t0) + 0.5 * (f1_ - f0) / (t1 - t0) * (tg - t0) ** 2)
    env = np.exp(-((tg - 0.5 * (t0 + t1)) ** 2) / (2 * tau ** 2))
    x = amp * env * np.cos(phase)
    x[tg < t0] = 0.0
    x[tg > t1] = 0.0
    return x.astype(complex)


def sine_gaussian_td(t0, f0, q, amp):
    tg = np.arange(N) * DELTAT
    tau = q / (2 * np.pi * f0)
    return (amp * np.exp(-((tg - t0) ** 2) / (2 * tau ** 2))
            * np.cos(2 * np.pi * f0 * (tg - t0))).astype(complex)


# --------------------------------------------------------------------------- (a1) formula vs brute-force

def test_formula_vs_bruteforce_invertible_C():
    """Woodbury-via-precision P - PB M^-1 B'P == brute-force (C + sig^2 B Lam0 B')^-1 (~1e-12)."""
    n = 300
    lags = np.arange(n)
    acf = np.exp(-lags / 15.0) * np.cos(0.12 * lags)
    C = scipy.linalg.toeplitz(acf)
    C = 0.5 * (C + C.T)
    C = C + (abs(min(np.linalg.eigvalsh(C))) + 1e-3) * np.eye(n)  # SPD
    P = np.linalg.inv(C)

    rng = np.random.default_rng(1)
    r = 8
    B = rng.standard_normal((n, r))
    Lam0 = np.linspace(1.0, 0.25, r)
    sig = 0.4
    Cp = C + B @ np.diag(sig ** 2 * Lam0) @ B.T
    Cpinv_brute = np.linalg.inv(Cp)

    Phi = P @ B
    G = B.T @ Phi
    M = np.linalg.inv(np.diag(sig ** 2 * Lam0)) + G
    Cpinv_formula = P - Phi @ np.linalg.inv(M) @ Phi.T
    err = np.max(np.abs(Cpinv_brute - Cpinv_formula)) / np.max(np.abs(Cpinv_brute))
    print("[G3-a1] (C+sig^2 B Lam0 B')^-1 brute vs P-PB M^-1 B'P: rel=%.3e" % err)
    assert err < 1e-10


# --------------------------------------------------------------------------- (a2) implementation vs dense

def _dense_reference(ctx, B, Lam0, sig, d_td):
    """Dense P - PB M^-1 B'P reference from the ACTUAL band-limited precision P = ip_norm*K."""
    P = ctx.ip_normalization * ctx._dense
    Phi = P @ B
    G = B.T @ Phi
    M = np.linalg.inv(np.diag(sig ** 2 * Lam0)) + G
    Minv = np.linalg.inv(M)
    Cpinv = P - Phi @ Minv @ Phi.T
    gamma = Phi.T @ d_td
    ghat = B @ (Minv @ gamma)
    dlnZ = (0.5 * np.real(np.conj(gamma) @ Minv @ gamma)
            - 0.5 * np.linalg.slogdet(np.eye(len(Lam0)) + sig ** 2 * np.diag(Lam0) @ G)[1])
    return Cpinv, ghat, dlnZ, P


def test_dense_parity_implementation():
    """Context glitch impl (FFT matvec, scaled M_hat, conj) vs dense formula on the real K."""
    rng = np.random.default_rng(11)
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=FLOW, fMax=FMAX, analyticPSD_Q=True)
    B, Lam0 = tra.slepian_basis(N, FS, G_T1, G_T2, G_F1, G_F2)
    data = bandlimited_series(rng)
    info = ctx.attach_glitch_basis([(B, Lam0, "H1")], data, sigma_g="profile")
    sig = ctx._glitch_sigma_g
    d_td = np.asarray(lsu.DataInverseFourier(data).data.data)

    Cpinv, ghat_ref, dlnZ_ref, P = _dense_reference(ctx, B, Lam0, sig, d_td)

    # g_hat (Q' correction vector)
    rel_ghat = np.max(np.abs(ctx.glitch_reconstruction_td() - ghat_ref)) / np.max(np.abs(ghat_ref))
    # Delta lnZ
    rel_dlnZ = abs(info["delta_lnZ"] - dlnZ_ref) / abs(dlnZ_ref)
    # U' via the real ToeplitzComplexIP
    IP = ToeplitzComplexIP(ctx)
    h1 = bandlimited_series(rng)
    h2 = bandlimited_series(rng)
    h1t = np.asarray(lsu.DataInverseFourier(h1).data.data)
    h2t = np.asarray(lsu.DataInverseFourier(h2).data.data)
    Up = IP.ip(h1, h2)
    Up_ref = np.conj(h1t) @ Cpinv @ h2t
    rel_U = abs(Up - Up_ref) / abs(Up_ref)

    print("[G3-a2] r=%d sigma_g=%.3e  g_hat rel=%.3e  Delta_lnZ rel=%.3e  U' rel=%.3e"
          % (B.shape[1], sig, rel_ghat, rel_dlnZ, rel_U))
    assert rel_ghat < 1e-9
    assert rel_dlnZ < 1e-9
    assert rel_U < 1e-9


def test_Qprime_overlap_consistency():
    """Corrected rhoTS from ToeplitzComplexOverlap == base overlap on dense-corrected data."""
    rng = np.random.default_rng(12)
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=FLOW, fMax=FMAX, analyticPSD_Q=True)
    B, Lam0 = tra.slepian_basis(N, FS, G_T1, G_T2, G_F1, G_F2)
    data = bandlimited_series(rng)
    ctx.attach_glitch_basis([(B, Lam0, "H1")], data, sigma_g="profile")

    hlm = bandlimited_series(rng)
    ov = ToeplitzComplexOverlap(ctx)
    rho_c, rhoTS_c, _, _ = ov.ip(hlm, data)          # glitch-corrected Q'(t)

    # dense reference corrected data d_corr = d - g_hat, run a CLEAN (no-glitch) overlap on it
    d_td = np.asarray(lsu.DataInverseFourier(data).data.data)
    ghat = ctx.glitch_reconstruction_td()
    data_corr = series_from_td(d_td - ghat, epoch=float(data.epoch))
    ctx_clean = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=FLOW, fMax=FMAX, analyticPSD_Q=True)
    ov_clean = ToeplitzComplexOverlap(ctx_clean)
    _, rhoTS_ref, _, _ = ov_clean.ip(hlm, data_corr)

    a = np.asarray(rhoTS_c.data.data)
    b = np.asarray(rhoTS_ref.data.data)
    rel = np.max(np.abs(a - b)) / np.max(np.abs(b))
    print("[G3-a2] Q'(t) corrected-overlap vs base-overlap-on-corrected-data rel=%.3e" % rel)
    assert rel < 1e-10


# --------------------------------------------------------------------------- (b) conjugation trap

def test_conjugation_trap():
    """Hermitian corrected U' matches dense; the bilinear (no-conj) correction is O(1) wrong."""
    rng = np.random.default_rng(21)
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=FLOW, fMax=FMAX, analyticPSD_Q=True)
    B, Lam0 = tra.slepian_basis(N, FS, G_T1, G_T2, G_F1, G_F2)
    data = bandlimited_series(rng)
    ctx.attach_glitch_basis([(B, Lam0, "H1")], data, sigma_g="profile")
    sig = ctx._glitch_sigma_g

    h1 = bandlimited_series(rng)
    h2 = bandlimited_series(rng)
    h1t = np.asarray(lsu.DataInverseFourier(h1).data.data)
    h2t = np.asarray(lsu.DataInverseFourier(h2).data.data)

    P = ctx.ip_normalization * ctx._dense
    Phi = P @ B
    M = np.linalg.inv(np.diag(sig ** 2 * Lam0)) + Phi.T @ B
    Minv = np.linalg.inv(M)
    Cpinv = P - Phi @ Minv @ Phi.T
    Up_ref = np.conj(h1t) @ Cpinv @ h2t
    base = ctx.ip_normalization * ctx.dot_td(h1t, h2t)

    corr_herm = ctx.glitch_ip_correction(h1t, h2t)                 # conj(Phi^T h1)^T M^-1 (Phi^T h2)
    p1 = Phi.T @ h1t
    p2 = Phi.T @ h2t
    corr_bilin = np.tensordot(p1, Minv @ p2, axes=([-1], [-1]))     # WRONG: no conjugation

    rel_herm = abs((base - corr_herm) - Up_ref) / abs(Up_ref)
    rel_bilin = abs((base - corr_bilin) - Up_ref) / abs(Up_ref)
    print("[G3-b] Hermitian U' rel=%.3e (correct) ; bilinear (no-conj) rel=%.3e (must be O(1))"
          % (rel_herm, rel_bilin))
    assert rel_herm < 1e-9
    assert rel_bilin > 1e-2


# --------------------------------------------------------------------------- (c) default OFF regression

def test_default_off_byte_identical():
    """A factory with NO glitch spec is byte-identical to the plain TD factory (glitch OFF)."""
    rng = np.random.default_rng(31)
    psd = make_psd_series()
    hlms = {(2, 2): bandlimited_series(rng), (2, 1): bandlimited_series(rng)}
    data = bandlimited_series(rng)

    fac_plain = TDInnerProductFactory(solver="auto")
    fac_offspec = TDInnerProductFactory(solver="auto", glitch_specs={})  # no IFO registered

    Q_a = fl.ComputeModeIPTimeSeries(hlms, data, psd, FLOW, FMAX, FNYQ, 0, 256,
                                     analyticPSD_Q=False, IP_factory=fac_plain)
    Q_b = fl.ComputeModeIPTimeSeries(hlms, data, psd, FLOW, FMAX, FNYQ, 0, 256,
                                     analyticPSD_Q=False, IP_factory=fac_offspec)
    maxdiff = 0.0
    for k in hlms:
        maxdiff = max(maxdiff, np.max(np.abs(Q_a[k].data.data - Q_b[k].data.data)))
    print("[G3-c] default-OFF Q_lm(t) byte-diff = %.3e" % maxdiff)
    assert maxdiff == 0.0


# --------------------------------------------------------------------------- (c) end-to-end

def _lnL_of_t(Q_ts, U):
    """Single-detector, single-mode matched-filter lnL(t) proxy = |Q(t)|^2 / (2 Re U).

    This is exactly the per-mode SNR^2/2 statistic the Precompute guess_snr uses; it isolates
    the Q'/U' glitch correction without needing the full sky/Ylm response (which multiplies the
    same Q/U and does not change the three-way comparison qualitatively)."""
    q = np.asarray(Q_ts.data.data)
    return np.abs(q) ** 2 / (2.0 * np.real(U))


def test_end_to_end_glitch_mitigation():
    """BBH-like signal + injected blip in one detector: glitch-marg lnL(t) closer to glitch-free."""
    psd = make_psd_series()
    # "signal" template (BBH-like chirp with a peak), band-limited
    h_td = chirp_td(t0=0.6, t1=1.4, f0=40.0, f1_=350.0, tau=0.25, amp=STRAIN)
    hlm = series_from_td(h_td)
    hlms = {(2, 2): hlm}
    A = 6.0                                       # signal amplitude (in units of template)
    d_clean = A * h_td
    # injected sine-Gaussian blip in the glitch TF cell (disjoint from the chirp track); it
    # contaminates the matched filter at the time-shift aligning the template's high-frequency
    # tail with the blip.  Loud relative to the signal-in-that-cell.
    blip = sine_gaussian_td(t0=0.5 * (G_T1 + G_T2), f0=0.5 * (G_F1 + G_F2), q=6.0,
                            amp=80.0 * STRAIN)
    d_glitchy = d_clean + blip

    data_clean = series_from_td(d_clean)
    data_glitchy = series_from_td(d_glitchy)

    def run(data, factory, register=False, evt=None):
        if register:
            factory.register_detector_data("H1", psd, data, event_time_geo=evt)
        U = fl.ComputeModeCrossTermIP(hlms, hlms, psd, FLOW, FMAX, FNYQ, DELTAF,
                                      analyticPSD_Q=False, verbose=False,
                                      same_waveform_Q=False, IP_factory=factory)
        Q = fl.ComputeModeIPTimeSeries(hlms, data, psd, FLOW, FMAX, FNYQ, 0, N,
                                       analyticPSD_Q=False, IP_factory=factory)
        Uval = np.real(U[((2, 2), (2, 2))])
        q = np.asarray(Q[(2, 2)].data.data)
        return q, np.abs(q) ** 2 / (2.0 * Uval)

    # reference: glitch-free data, plain TD (no glitch marginalization)
    Q_ref, lnL_ref = run(data_clean, TDInnerProductFactory(solver="auto"))
    # unmitigated: glitchy data, plain TD
    Q_unmit, lnL_unmit = run(data_glitchy, TDInnerProductFactory(solver="auto"))
    # marginalized: glitchy data, glitch basis on H1 (support/band from the blip)
    fac_g = TDInnerProductFactory(
        solver="auto",
        glitch_specs={"H1": dict(t1=G_T1, t2=G_T2, f1=G_F1, f2=G_F2, r=None)},
        glitch_sigma="profile")
    Q_marg, lnL_marg = run(data_glitchy, fac_g, register=True)
    dlnZ = fac_g.glitch_delta_lnZ_total()

    # L2 distance of the matched-filter time series to the glitch-free reference (captures the
    # blip-induced feature the glitch marginalization removes).
    def l2(a, b):
        return float(np.sqrt(np.sum(np.abs(np.asarray(a) - np.asarray(b)) ** 2)))
    dQ_unmit = l2(Q_unmit, Q_ref)
    dQ_marg = l2(Q_marg, Q_ref)
    # peak lnL (SNR^2/2) three ways, for context (signal peak should survive marginalization)
    pk_ref, pk_unmit, pk_marg = np.max(lnL_ref), np.max(lnL_unmit), np.max(lnL_marg)
    print("[G3-c] ||Q-Q_ref||: unmitigated=%.4g  marginalized=%.4g  (ratio %.3f)"
          % (dQ_unmit, dQ_marg, dQ_marg / dQ_unmit))
    print("[G3-c] peak lnL (SNR^2/2): ref=%.4g  unmitigated=%.4g  marginalized=%.4g  "
          "Delta_lnZ=%.4g" % (pk_ref, pk_unmit, pk_marg, dlnZ))
    # marginalization brings the matched filter closer to the glitch-free reference ...
    assert dQ_marg < dQ_unmit
    # ... while preserving the signal peak (basis is TF-disjoint from the signal track)
    assert pk_marg > 0.5 * pk_ref
    # ... and the evidence constant is nonzero and surfaced
    assert dlnZ != 0.0


# --------------------------------------------------------------------------- script entry

if __name__ == "__main__":
    import time
    tests = [
        ("(a1) formula vs brute-force", test_formula_vs_bruteforce_invertible_C),
        ("(a2) dense parity implementation", test_dense_parity_implementation),
        ("(a2) Q'(t) overlap consistency", test_Qprime_overlap_consistency),
        ("(b) conjugation trap", test_conjugation_trap),
        ("(c) default-OFF byte-identical", test_default_off_byte_identical),
        ("(c) end-to-end glitch mitigation", test_end_to_end_glitch_mitigation),
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
