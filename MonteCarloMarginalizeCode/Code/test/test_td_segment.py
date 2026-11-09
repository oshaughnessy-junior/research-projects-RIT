"""
test_td_segment.py:  Acceptance tests for the truncated / gapped TD segment likelihood
(tasks T5 and T6 of TD_LIKELIHOOD.md; decision D5 precision-kernel convention).

Groups:

  (a) RINGDOWN-ONLY / analytic white-noise closed form -- a pure damped sinusoid in white
      (flat, full-band) noise.  A flat PSD makes the truncated precision kernel K_seg
      diagonal (kappa ~ delta), so the segment matched-filter lnL has the closed form
      lnL = 0.5 * A^2 * <t|t>_seg with <t|t>_seg = (2 deltaT) kappa[0] ||t_window||^2.

  (b) GAP EXCISION -- a controlled signal plus a loud disturbance placed INSIDE a gap.
      Checks (i) the inpainting gap implementation equals the dense marginal
      retained-sample precision K_RR - K_RG K_GG^-1 K_GR of K_fill to ~1e-10; (ii) the gapped likelihood on
      glitchy data equals the gapped likelihood on glitch-free data (the disturbance is
      excised); (iii) WITHOUT the gap the glitch measurably contaminates the inner product;
      (iv) the gapped likelihood equals a dense (C_RR)^-1 reference on a PSD with a line,
      C walled outside [fLow, fMax) as td-gs does, for one gap and two coupled gaps.

  (c) FULL-SEGMENT REGRESSION -- the segment machinery reduces to the full-segment path when
      no segment is configured (segment_dot_td == dot_td), and the default factory stays
      byte-identical to lsu.

  T6 U/V EDGE-SENSITIVITY DIAGNOSTIC -- three scenarios: ~0 variation full-segment;
      tail-level variation for a template-frame ringdown cut; and a deliberately
      mis-configured GPS-frame cut (segment truncated across the sliding template support)
      that the diagnostic flags (rel_var > 1e-2).

Environment: ~/.conda/envs/junior_rift/bin/python with
PYTHONPATH=<worktree>/MonteCarloMarginalizeCode/Code.  Runnable as
    pytest test_td_segment.py
    python  test_td_segment.py
"""

import numpy as np
import pytest
import scipy.linalg

import lal
import RIFT.lalsimutils as lsu
from RIFT.likelihood.td_inner_product import (
    ToeplitzInnerProductContext, TDInnerProductFactory,
    truncate_timeseries_template_frame, template_peak_time,
)

# --------------------------------------------------------------------------- fixtures

FS = 2048.0
FNYQ = FS / 2.0
N = 8192                     # two-sided full length (4 s)
DELTAF = FS / N
DELTAT = 1.0 / FS
LEN1 = N // 2 + 1


def flat_psd(f):
    """Flat (white) analytic one-sided PSD -> diagonal precision kernel."""
    f = np.atleast_1d(np.asarray(f, dtype=float))
    out = np.full(f.shape, 2.0e-46)
    return out if out.size > 1 else float(out[0])


def colored_psd(f):
    """Smooth colored analytic PSD (off-diagonal precision kernel)."""
    f = np.atleast_1d(np.asarray(f, dtype=float))
    fsafe = np.maximum(np.abs(f), 1.0)
    out = 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)
    return out if out.size > 1 else float(out[0])


def damped_sinusoid(n_len, i0, f0, tau_s, amp=1.0):
    """Real damped sinusoid starting at sample i0 (zero before): amp e^{-t/tau} cos(2pi f t)."""
    x = np.zeros(n_len, dtype=complex)
    k = np.arange(n_len - i0)
    t = k * DELTAT
    x[i0:] = amp * np.exp(-t / tau_s) * np.cos(2 * np.pi * f0 * t)
    return x


# --------------------------------------------------------------------------- (a) ringdown

def test_ringdown_white_noise_analytic():
    """Ringdown-only damped sinusoid in white noise: segment lnL matches the closed form."""
    T_seg = 1.0
    N_seg = int(round(T_seg * FS))
    seg_start = 1000
    ctx = ToeplitzInnerProductContext(
        flat_psd, DELTAF, FNYQ, fLow=0.0, fMax=FNYQ, analyticPSD_Q=True,
        segment_duration=T_seg, segment_start_index=seg_start)

    # damped sinusoid fully inside the analysis window
    i0 = seg_start + 50
    t_td = damped_sinusoid(N, i0, f0=200.0, tau_s=0.05, amp=1.0)
    A = 2.7
    d_td = A * t_td                      # zero noise: data = A * template

    ipn = ctx.ip_normalization
    U = ipn * ctx.segment_dot_td(t_td, t_td)
    Q = ipn * ctx.segment_dot_td(t_td, d_td)
    lnL_machine = np.abs(Q) ** 2 / (2.0 * np.real(U))

    # analytic closed form (diagonal kernel): <t|t>_seg = ipn * kappa[0] * ||t_window||^2
    c = ctx.kappa[0]
    win = slice(seg_start, seg_start + N_seg)
    tt = ipn * c * np.sum(np.abs(t_td[win]) ** 2)
    lnL_analytic = 0.5 * A ** 2 * tt

    # kernel diagonality: off-diagonal / diagonal energy ratio
    offdiag = np.sqrt(np.sum(ctx.kappa[1:N_seg] ** 2)) / abs(c)

    rel = abs(lnL_machine - lnL_analytic) / abs(lnL_analytic)
    print("[T5-a] lnL_machine=%.8g lnL_analytic=%.8g rel=%.3e ; K_seg offdiag/diag=%.3e ; "
          "U=%.6g" % (lnL_machine, lnL_analytic, rel, offdiag, np.real(U)))
    # offdiag ~5e-3 is the accumulated single-missing-Nyquist-bin ripple over N_seg samples;
    # it averages out in the quadratic form (the lnL closed-form match below is the real AC).
    assert offdiag < 1e-2
    assert rel < 5e-3


# --------------------------------------------------------------------------- (b) gap excision

def test_gap_excision_and_brute_force():
    """Gap marginalizes the gap samples; matches the dense marginal retained operator."""
    T_seg = 1.0
    N_seg = int(round(T_seg * FS))
    seg_start = 800
    # gap in geocenter seconds relative to segment sample 0 (origin=0 default)
    gap = [(0.40, 0.55)]                  # 0.40-0.55 s into the segment
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=20.0, fMax=900.0, analyticPSD_Q=True,
        segment_duration=T_seg, segment_start_index=seg_start, gaps=gap)

    rng = np.random.default_rng(7)
    # signal spanning the window (touches, but is defined across, the segment)
    t_td = damped_sinusoid(N, seg_start + 20, f0=150.0, tau_s=0.4, amp=1.0)
    # loud disturbance localized STRICTLY inside the gap
    g_i0 = seg_start + int(round(0.45 * FS))
    g_i1 = seg_start + int(round(0.52 * FS))
    glitch = np.zeros(N, dtype=complex)
    glitch[g_i0:g_i1] = 50.0 * rng.standard_normal(g_i1 - g_i0)

    d_clean = t_td.copy()
    d_glitchy = t_td + glitch

    ipn = ctx.ip_normalization
    Q_gap_clean = ipn * ctx.segment_dot_td(t_td, d_clean)
    Q_gap_glitchy = ipn * ctx.segment_dot_td(t_td, d_glitchy)
    U_gap = ipn * ctx.segment_dot_td(t_td, t_td)

    # (i) dense marginal retained-sample precision
    Kret, R = ctx.dense_retained_operator()
    t_seg = t_td[seg_start:seg_start + N_seg]
    U_bf = ipn * np.conj(t_seg[R]) @ (Kret @ t_seg[R])
    d_seg = d_glitchy[seg_start:seg_start + N_seg]
    Q_bf = ipn * np.conj(t_seg[R]) @ (Kret @ d_seg[R])
    rel_U = abs(U_gap - U_bf) / abs(U_bf)
    rel_Q = abs(Q_gap_glitchy - Q_bf) / abs(Q_bf)

    # (ii) glitch excised: gapped(glitchy) == gapped(clean)
    rel_excise = abs(Q_gap_glitchy - Q_gap_clean) / abs(Q_gap_clean)

    # (iii) WITHOUT the gap the glitch contaminates
    ctx_nogap = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=20.0, fMax=900.0, analyticPSD_Q=True,
        segment_duration=T_seg, segment_start_index=seg_start)
    Q_ng_clean = ipn * ctx_nogap.segment_dot_td(t_td, d_clean)
    Q_ng_glitchy = ipn * ctx_nogap.segment_dot_td(t_td, d_glitchy)
    contam = abs(Q_ng_glitchy - Q_ng_clean) / abs(Q_ng_clean)

    print("[T5-b] brute-force: rel_U=%.3e rel_Q=%.3e ; excision rel=%.3e ; "
          "ungapped contamination=%.3e ; is_spd=%s n_retained=%d/%d"
          % (rel_U, rel_Q, rel_excise, contam, ctx.is_spd, len(R), N_seg))
    assert rel_U < 1e-10 and rel_Q < 1e-10          # (i) mask == submatrix
    assert rel_excise < 1e-10                        # (ii) glitch excised
    assert contam > 1e-2                             # (iii) glitch would contaminate


# --------------------------------------------------------------------------- (b') marginal gap

def _line_psd(f):
    """Colored PSD plus a narrow line at 60 Hz (long-range precision kernel)."""
    f = np.atleast_1d(np.asarray(f, dtype=float))
    fsafe = np.maximum(np.abs(f), 1.0)
    out = 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2
                   + 1e4 / (1.0 + ((fsafe - 60.0) / 0.3) ** 2))
    return out if out.size > 1 else float(out[0])


_TDGS_KAPPA = 1e8      # tdanalysis PSDToAutocorrConfig.target_condition_number default


def _walled_covariance(psd, fs, n, fLow, fMax):
    """Dense circulant covariance of the gap noise model, built from the PSD directly.

    In band [fLow, fMax) the one-sided weight is 1/S; every other bin carries the td-gs
    wall weight min(max w / (kappa/2), min w).  C = K_fill^-1 is circulant with symbol 1/w.
    """
    df = fs / n
    k = np.arange(n // 2 + 1)
    band = (k >= int(round(fLow / df))) & (k < int(round(fMax / df)))
    w = np.zeros(k.size)
    w[band] = 1.0 / psd(k[band] * df)
    w[~band] = min(w[band].max() / (0.5 * _TDGS_KAPPA), w[band].min())
    return scipy.linalg.toeplitz(np.fft.irfft(1.0 / w, n=n))


def _gap_errors(ctx, psd, fs, n, h, fMax):
    """Code, cropped and band-pinv kernels against the dense (C_RR)^-1, in noise sigma."""
    R = np.nonzero(ctx.retain_mask)[0]
    C = _walled_covariance(psd, fs, n, 20.0, fMax)
    K_marg = np.zeros((n, n))
    K_marg[np.ix_(R, R)] = np.linalg.inv(C[np.ix_(R, R)])
    K = scipy.linalg.toeplitz(ctx.kappa[:n])     # band-limited K_seg
    K_crop = np.zeros((n, n))
    K_crop[np.ix_(R, R)] = K[np.ix_(R, R)]
    G = ~ctx.retain_mask
    K_pinv = np.zeros((n, n))                    # the earlier pseudo-inverse marginal
    K_pinv[np.ix_(R, R)] = K[np.ix_(R, R)] - K[np.ix_(R, G)] @ np.linalg.pinv(
        K[np.ix_(G, G)], rcond=1e-12, hermitian=True) @ K[np.ix_(G, R)]

    # noise covariance of the data: circulant with the PSD as its symbol
    Sym = psd(np.fft.fftfreq(n, 1.0 / fs))

    def noise_sd(a):
        return np.sqrt(np.sum(np.abs(np.fft.fft(a)) ** 2 * Sym))

    ref = K_marg @ h
    sd = noise_sd(ref)
    Kh_code = np.where(ctx.retain_mask, ctx._apply_seg(ctx._segment_window(h)).real, 0.0)
    U_rel = abs(ctx.segment_dot_td(h, h).real - h @ ref) / (h @ ref)
    return (noise_sd(Kh_code - ref) / sd, noise_sd(K_crop @ h - ref) / sd,
            noise_sd(K_pinv @ h - ref) / sd, U_rel)


def test_gap_matches_marginal_likelihood():
    """A gap must marginalize over the deleted samples: precision (C_RR)^-1, not (C^-1)_RR.

    Reference: C from the PSD with the td-gs wall outside [fLow, fMax), inverted densely
    on the retained samples.  The error of <h|d> is measured in units of its noise spread,
    with noise drawn from the colored PSD.  With fMax < fNyq the band-limited K_GG has
    near-null modes, so the cropped kernel and the band pseudo-inverse both miss.
    """
    fs, n = 512.0, 1024                       # small full-length (circulant) segment
    fnyq, df = fs / 2.0, fs / n
    fMax = 200.0
    gap = (0.80, 0.95)                        # seconds into the segment
    ctx = ToeplitzInnerProductContext(
        _line_psd, df, fnyq, fLow=20.0, fMax=fMax, analyticPSD_Q=True,
        solver="dense", gaps=[gap])
    t = np.arange(n) / fs
    h = np.exp(-0.5 * ((t - 0.85) / 0.15) ** 2) * np.cos(2 * np.pi * (40.0 * t + 30.0 * t ** 2))
    err_code, err_crop, err_pinv, U_rel = _gap_errors(ctx, _line_psd, fs, n, h, fMax)
    print("[T5-b'] err(code)=%.3e  err(cropped)=%.3e  err(band pinv)=%.3e sigma  U_rel=%.3e"
          % (err_code, err_crop, err_pinv, U_rel))
    assert err_crop > 1.0             # the fixture discriminates the cropped kernel
    assert err_pinv > 1e-2            # ... and the unregularized pseudo-inverse
    assert err_code < 1e-6
    assert U_rel < 1e-8


def test_two_gaps_match_marginal_likelihood():
    """Two gaps couple through K_fill; the preconditioned CG solve must keep that coupling."""
    fs, n = 512.0, 1024
    fnyq, df = fs / 2.0, fs / n
    fMax = 200.0
    ctx = ToeplitzInnerProductContext(
        _line_psd, df, fnyq, fLow=20.0, fMax=fMax, analyticPSD_Q=True,
        solver="dense", gaps=[(0.80, 0.95), (1.00, 1.10)])
    assert len(ctx._gap_runs) == 2
    t = np.arange(n) / fs
    h = np.exp(-0.5 * ((t - 0.9) / 0.2) ** 2) * np.cos(2 * np.pi * (40.0 * t + 30.0 * t ** 2))
    err_code, err_crop, _, U_rel = _gap_errors(ctx, _line_psd, fs, n, h, fMax)
    print("[T5-b''] two gaps: err(code)=%.3e sigma  U_rel=%.3e" % (err_code, U_rel))
    assert err_crop > 1.0
    assert err_code < 1e-6
    assert U_rel < 1e-8


def _strong_lines_psd(f):
    """Colored PSD with narrow lines 1e8-1e10 above it: in-band range ~1e10 > _GAP_KAPPA/2."""
    f = np.atleast_1d(np.asarray(f, dtype=float))
    fsafe = np.maximum(np.abs(f), 1.0)
    out = 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2
                   + sum(a * np.exp(-((fsafe - f0) / 0.6) ** 2)
                         for f0, a in ((35.0, 1e8), (60.0, 1e9), (150.0, 1e10))))
    return out if out.size > 1 else float(out[0])


@pytest.mark.parametrize("gaps", [[(0.80, 0.95)], [(0.30, 0.60), (0.61, 0.95)]])
def test_gap_with_wide_dynamic_range_psd(gaps):
    """In-band range above kappa/2: the wall is min(w_band) and the solves stay accurate."""
    fs, n = 512.0, 1024
    ctx = ToeplitzInnerProductContext(
        _strong_lines_psd, fs / n, fs / 2.0, fLow=20.0, fMax=200.0, analyticPSD_Q=True,
        solver="dense", gaps=gaps)
    assert len(ctx._gap_runs) == len(gaps)
    t = np.arange(n) / fs
    h = np.exp(-0.5 * ((t - 0.85) / 0.15) ** 2) * np.cos(2 * np.pi * (40.0 * t + 30.0 * t ** 2))
    err_code, err_crop, _, U_rel = _gap_errors(ctx, _strong_lines_psd, fs, n, h, 200.0)
    print("[T5-wide] %d gap(s): err(code)=%.3e sigma  U_rel=%.3e" % (len(gaps), err_code, U_rel))
    assert err_crop > 1.0
    assert err_code < 1e-4
    assert U_rel < 1e-5


def test_gapped_overlap_high_passes_out_of_band_data():
    """A gap couples out-of-band data into the band; the data high-pass removes it.

    The gap and template sit mid-stream, 8 s from the ends, clear of the filter transients."""
    fs, n = 512.0, 8192
    t = np.arange(n) / fs
    f = np.fft.rfftfreq(n, 1.0 / fs)
    band = (f >= 20.0) & (f < 200.0)
    rms_band = np.sqrt(np.sum(_line_psd(f[band])) * fs / n)
    s = 1e4 * rms_band * np.cos(2 * np.pi * 2.0 * t + 0.3)      # 2 Hz, far above the wall
    h = np.exp(-0.5 * ((t - 8.0) / 0.3) ** 2) * np.cos(2 * np.pi * (40.0 * t + 15.0 * (t - 8) ** 2))
    ctx = ToeplitzInnerProductContext(
        _line_psd, fs / n, fs / 2.0, fLow=20.0, fMax=200.0, analyticPSD_Q=True,
        gaps=[(7.9, 8.0)], data_highpass=10.0)
    ipn = ctx.ip_normalization
    sig = np.sqrt(ipn * ctx.segment_dot_td(h, h).real)
    raw = abs(ipn * ctx.segment_dot_td(h, s).real) / sig
    filtered = abs(ipn * ctx.segment_dot_td(h, ctx.highpass_gapped_data(s)).real) / sig
    print("[T5-hp] 2 Hz line in <h|d>: unfiltered %.3e sigma, high-passed %.3e sigma"
          % (raw, filtered))
    assert raw > 1e-2
    assert filtered < 1e-6
    # gapped data that cannot be high-passed is refused
    ctx_nohp = ToeplitzInnerProductContext(
        _line_psd, fs / n, fs / 2.0, fLow=20.0, fMax=200.0, analyticPSD_Q=True,
        gaps=[(7.9, 8.0)])
    with pytest.raises(ValueError, match="data_highpass"):
        ctx_nohp.highpass_gapped_data(s)


def test_data_highpass_corner_must_spare_flow():
    """Templates are not filtered, so the corner must leave fLow essentially untouched."""
    kw = dict(analyticPSD_Q=True, gaps=[(0.5, 0.6)])
    ToeplitzInnerProductContext(_line_psd, 0.5, 256.0, 20.0, fMax=200.0, data_highpass=10.0, **kw)
    with pytest.raises(ValueError, match="power response at fLow"):
        ToeplitzInnerProductContext(_line_psd, 0.5, 256.0, 20.0, fMax=200.0,
                                    data_highpass=15.0, **kw)


def test_long_gap_avoids_dense_factorization(monkeypatch):
    """A 2 s gap at 4096 Hz (8192 samples) is solved without an O(G^3) factorization."""
    def refuse(*args, **kwargs):
        raise AssertionError("dense O(G^3) factorization in the gap solve")
    for name in ("eigh", "eig", "pinv", "inv", "solve", "cholesky", "svd"):
        monkeypatch.setattr(np.linalg, name, refuse)
    for name in ("eigh", "inv", "solve", "cho_factor", "lu_factor", "svd", "pinvh"):
        monkeypatch.setattr(scipy.linalg, name, refuse)
    fs, n = 4096.0, 8 * 4096
    ctx = ToeplitzInnerProductContext(
        _line_psd, fs / n, fs / 2.0, fLow=20.0, fMax=1700.0, analyticPSD_Q=True,
        solver="auto", gaps=[(3.0, 5.0)])
    h = np.random.default_rng(3).standard_normal(n)
    Kh = ctx._apply_seg(ctx._segment_window(h))
    assert np.all(Kh[~ctx.retain_mask] == 0.0)
    assert np.isfinite(Kh).all()


# --------------------------------------------------------------------------- (c) regression

def test_full_segment_reduces_to_full_path():
    """segment_dot_td on a default (no-segment) context equals the full dot_td."""
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=20.0, fMax=900.0, analyticPSD_Q=True)
    assert ctx.segment_active is False
    rng = np.random.default_rng(11)
    u = rng.standard_normal(N) + 1j * rng.standard_normal(N)
    v = rng.standard_normal(N) + 1j * rng.standard_normal(N)
    a = ctx.segment_dot_td(u, v)
    b = ctx.dot_td(u, v)
    rel = abs(a - b) / abs(b)
    print("[T5-c] segment_dot_td vs dot_td (full segment) rel=%.3e" % rel)
    assert rel < 1e-12


def test_default_factory_byte_identical():
    """Default (segment-off) factory U == direct lsu.ComplexIP (byte-identical)."""
    psd = lal.CreateREAL8FrequencySeries(
        "psd", lal.LIGOTimeGPS(0.), 0., DELTAF, lsu.lsu_HertzUnit, LEN1)
    psd.data.data = colored_psd(np.arange(LEN1) * DELTAF)
    rng = np.random.default_rng(13)
    fvals = np.arange(LEN1) * DELTAF
    spec1 = np.zeros(LEN1, dtype=complex)
    band = (fvals >= 20.0) & (fvals <= 900.0)
    spec1[band] = rng.standard_normal(band.sum()) + 1j * rng.standard_normal(band.sum())
    h2 = np.zeros(N, dtype=complex)
    h2[:LEN1] = spec1[::-1]
    h2[LEN1 - 1:] = spec1[:-1]
    hf = lal.CreateCOMPLEX16FrequencySeries("h", lal.LIGOTimeGPS(0.), 0., DELTAF,
                                            lsu.lsu_HertzUnit, N)
    hf.data.data = h2

    IP_lsu = lsu.ComplexIP(20.0, 900.0, FNYQ, DELTAF, psd, False, False, 0.)
    ref = IP_lsu.ip(hf, hf)
    factory = TDInnerProductFactory()                # segment OFF
    IP_td = factory.ip_scalar(20.0, 900.0, FNYQ, DELTAF, psd, analyticPSD_Q=False)
    got = IP_td.ip(hf, hf)
    rel = abs(got - ref) / abs(ref)
    print("[T5-c] default factory vs lsu.ComplexIP rel=%.3e" % rel)
    assert rel < 1e-12


# --------------------------------------------------------------------------- template truncation

def test_template_truncation_hard_cut():
    """Template-frame truncation zeroes exactly outside [t_ref+start, t_ref+end], no taper."""
    t_td = damped_sinusoid(N, 3000, f0=200.0, tau_s=0.05, amp=1.0)
    epoch = -2.0                          # arbitrary template-frame epoch
    t_ref = template_peak_time(t_td, DELTAT, epoch)
    start, end = 0.0, 0.1                 # ringdown: peak .. peak+0.1 s
    out = truncate_timeseries_template_frame(t_td, DELTAT, epoch, start, end, t_ref=t_ref)
    # retained index range
    i_lo = int(np.ceil((t_ref + start - epoch) / DELTAT))
    i_hi = int(np.floor((t_ref + end - epoch) / DELTAT)) + 1
    assert np.all(out[:i_lo] == 0.0)
    assert np.all(out[i_hi:] == 0.0)
    assert np.array_equal(out[i_lo:i_hi], t_td[i_lo:i_hi])   # hard cut, no taper
    # the peak sample is retained and equals the original (no taper attenuation at cut)
    ipk = int(np.argmax(np.abs(t_td)))
    print("[T5-tt] t_ref=%.6f i_lo=%d i_hi=%d peak_idx=%d retained=%d"
          % (t_ref, i_lo, i_hi, ipk, i_hi - i_lo))
    assert out[ipk] == t_td[ipk]


# --------------------------------------------------------------------------- T6 diagnostic

def test_t6_full_segment_near_zero():
    """T6 scenario 1: full segment -> ~0 U edge-sensitivity."""
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=20.0, fMax=900.0, analyticPSD_Q=True)
    t_td = damped_sinusoid(N, N // 2 - 200, f0=200.0, tau_s=0.03, amp=1.0)
    rep = ctx.uv_edge_sensitivity(t_td, window_half=0.075)
    print("[T6-1] full-segment rel_var=%.3e (n_shift=%d)" % (rep["rel_var"], rep["n_shift"]))
    assert rep["rel_var"] < 1e-6


def test_t6_template_frame_cut_tail_level():
    """T6 scenario 2: template-frame ringdown cut -> tail-level (below warn tol) variation."""
    T_seg = 0.5
    N_seg = int(round(T_seg * FS))
    seg_start = N // 2 - N_seg // 2       # segment centred in the stream
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=20.0, fMax=900.0, analyticPSD_Q=True,
        segment_duration=T_seg, segment_start_index=seg_start,
        template_start_offset=0.0, template_end_offset=None)
    # compact ringdown whose peak sits at the segment CENTRE, so a +-window_half slide
    # (154 samples) keeps its (compact) support well inside the segment (margin ~512 each).
    t_full = damped_sinusoid(N, seg_start + N_seg // 2, f0=200.0, tau_s=0.02, amp=1.0)
    t_trunc = ctx.apply_template_truncation(t_full, epoch=0.0)
    rep = ctx.uv_edge_sensitivity(t_trunc, window_half=0.075)
    print("[T6-2] template-frame cut rel_var=%.3e (warn tol 1e-2)" % rep["rel_var"])
    assert rep["rel_var"] < 1e-2


def test_t6_gps_frame_cut_flagged():
    """T6 scenario 3: GPS-frame cut (narrow segment, full template slides across it) flagged."""
    T_seg = 0.12                          # narrow segment, fixed in "GPS"
    N_seg = int(round(T_seg * FS))
    seg_start = N // 2
    ctx = ToeplitzInnerProductContext(
        colored_psd, DELTAF, FNYQ, fLow=20.0, fMax=900.0, analyticPSD_Q=True,
        segment_duration=T_seg, segment_start_index=seg_start)
    # long template spanning most of the narrow window (NOT template-frame truncated) so a
    # +-window_half slide carries a big chunk of its support out of the retained window
    t_td = damped_sinusoid(N, seg_start + 5, f0=200.0, tau_s=0.5, amp=1.0)
    rep = ctx.uv_edge_sensitivity(t_td, window_half=0.05)
    print("[T6-3] GPS-frame cut rel_var=%.3e (N_seg=%d) -> should exceed 1e-2"
          % (rep["rel_var"], N_seg))
    assert rep["rel_var"] > 1e-2


# --------------------------------------------------------------------------- script entry

if __name__ == "__main__":
    import time
    tests = [
        ("(a) ringdown white-noise analytic", test_ringdown_white_noise_analytic),
        ("(b) gap excision + brute force", test_gap_excision_and_brute_force),
        ("(b') gap == marginal likelihood", test_gap_matches_marginal_likelihood),
        ("(c) full-segment reduction", test_full_segment_reduces_to_full_path),
        ("(c) default factory == lsu", test_default_factory_byte_identical),
        ("template-frame hard cut", test_template_truncation_hard_cut),
        ("T6-1 full-segment ~0", test_t6_full_segment_near_zero),
        ("T6-2 template cut tail-level", test_t6_template_frame_cut_tail_level),
        ("T6-3 GPS-frame cut flagged", test_t6_gps_frame_cut_flagged),
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
