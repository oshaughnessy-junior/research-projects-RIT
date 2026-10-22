# Public entry point: uncalibrated native-grid nearest-time TD-GS; see TD_GS.md.
#
# td_gs_precompute.py
#
# Time-domain Precompute route for ILE (--internal-ile-inner-product td-gs).
#
# Everything here stays in the time domain: the data d is loaded as a time series (no
# Tukey window), the template modes h_lm(t) come from lalsimutils.hlmoft with
# conditioning off (no taper), and neither is ever put through a windowed/periodic
# frequency-domain inner product.  The noise model is a Toeplitz covariance
# C = Toeplitz(ACF) on each contiguous analysis piece (tdanalysis style: a square window
# == hard sample selection, pieces independent, so C is block-diagonal over pieces).  The
# ACF comes from a tdanalysis [tau, R] .npy file or from the PSD through tdanalysis'
# PSDToAutocorr (walled outside the band, explicit condition number).
#
# Per detector and ILE job (built ONCE, TDGSDetectorContext):
#     w_p = C_p^{-1} d_p                 for every piece p (dense inverse for short pieces,
#                                        tdanalysis Gohberg-Semencul for long ones)
# Per intrinsic point (TDGSDetectorContext.precompute), for every shift k on RIFT's
# storage grid (k = N_shift + i, i = 0..N_window-1; template sample j sits at data
# sample j + k):
#     Q_lm(k)        = sum_p  conj(h_lm,k |_p)^T  w_p                  (direct dot products)
#     U_lm,l'm'(k)   = sum_p  conj(h_lm,k |_p)^T  C_p^{-1}  h_l'm',k |_p
#     V_lm,l'm'(k)   = sum_p        (h_lm,k |_p)^T  C_p^{-1}  h_l'm',k |_p
# where h|_p is the shifted template cut to piece p.  U follows lsu.ComplexIP's
# conj(h1) h2 convention; V is RIFT's crossTermsV = <conj(h_lm) | h_l'm'>.  With
# tdanalysis' ACF convention conj(u)^T C^{-1} v IS the matched-filter inner product
# (ip_normalization = 1, see ToeplitzInnerProductContext.ip_normalization).
#
# Q is one matrix-vector product per mode and piece: the rows of a sliding-window view
# of the zero-extended template (no copy) against w_p.
#
# U(t), V(t) are evaluated with the Gohberg-Semencul (GS) representation of C^{-1},
#     C^{-1} = [L(x) U(y~) - L(c) U(d)] / x0,   x = C^{-1} e_1, y = C^{-1} e_n,
# so that  u^H C^{-1} v = [ <E_x[u], E_y~[v]> - <E_c[u], E_d[v]> ] / x0  with
# E_k[u][m] = sum_{j>=m} k[j-m] u[j] a forward correlation of the cut template with a
# kernel.  Four routes evaluate this on the shift grid; all are exact (no truncation,
# interpolation or scalar-U shortcut), they differ only in cost and memory:
#
#   "prefix"     When the shifted template ENDS inside the piece for every grid shift
#                (its last non-zero sample at or before the piece end; the cut at the
#                piece START never matters, E_k[u][m] only looks at j >= m >= 0), the
#                forward correlation of the cut template at shift t is a window of ONE
#                correlation F_k of the kernel with the unshifted zero-extended template,
#                E_k[g_t][m] = F_k[m + off - t], so U_ab(t) is a windowed sum of the fixed
#                product sequence P_ab = [conj(F_x^a) F_y~^b - conj(F_c^a) F_d^b] / x0:
#                four FFT correlations per mode, then O(n + N_window) per mode pair.
#                The mirror case (template STARTS inside the piece for every shift,
#                overrunning its end) is the same identity on the reversed piece,
#                because C^{-1} of a symmetric Toeplitz C is persymmetric.  (The GS
#                form built from the numerical x, y is persymmetric only to ~cond(C)
#                eps, so the mirror orientation and the recursion agree to that level,
#                ~1e-9 at cond 1e8, each as close to the true C^{-1} as the other.)
#   "recursion"  Sliding the template by one sample updates every E_k in O(n) (a shift,
#                one new head entry, one rank-one tail correction for the sample leaving
#                the piece): O(n N_window) per mode, any cut pattern, no per-shift solve.
#   "batched"    Direct reference: stack the cut shifted templates as columns, apply
#                C^{-1} once per mode (memory n x N_window per mode).
#   "chunked"    "batched" in column blocks sized to a memory bound.
#
# "auto" (default): dense pieces use "chunked" (the dense inverse is exact there, and the
# generator columns taken from it lose precision for badly conditioned ACFs); GS pieces
# use "prefix" when its condition holds (the full segment in an ILE run, ringdown
# pieces) and "recursion" otherwise (a piece the template overruns at both ends, where
# the per-column FFT solves of the batched routes cost far more than the recursion).
# FFTs appear only as exact zero-padded linear-correlation engines inside
# this Toeplitz algebra (as inside tdanalysis' GS itself); nothing is windowed.
#
# Output contract (what the likelihood consumes):
#     rholms[mode]            lal.COMPLEX16TimeSeries, length N_window, deltaT, epoch =
#                             data.epoch - hlm.epoch + N_shift*deltaT -- exactly the series
#                             factored_likelihood.ComputeModeIPTimeSeries returns after its
#                             DataRollBins/Cut (the fractional part of (data.epoch -
#                             hlm.epoch)/deltaT is carried in the epoch, as on the FD route;
#                             the template is only ever shifted by whole samples).
#     crossTerms[(p1,p2)]     complex ndarray of length N_window, aligned sample-by-sample
#     crossTermsV[(p1,p2)]    with rholms (entry i <-> shift k = N_shift + i).
#
# Calibration marginalization is deferred; the public entry points refuse it.

import numpy as np
import scipy.linalg
import scipy.fft
import scipy.signal

import lal

import RIFT.lalsimutils as lsu

_GS_BATCH_COLUMNS = 256      # columns per batched GS apply (memory: ~2 L x cols floats)
DEFAULT_ACF_CONDITION_NUMBER = 1.0e8   # as the tdanalysis real-event / XG ACF files
U_METHODS = ("auto", "prefix", "recursion", "batched", "chunked")
DEFAULT_UV_BLOCK_BYTES = 256 * 2 ** 20   # memory bound of one column block on the chunked route


# --------------------------------------------------------------------------- helpers

def _dimensionless_unit():
    try:
        return lal.DimensionlessUnit
    except AttributeError:
        return lsu.lsu_DimensionlessUnit


def load_acf_file(path, deltaT, rel_tol=1e-6):
    """Load a tdanalysis autocorrelation file and check its sampling.

    Format: float64 array of shape (2, N) with rows [tau, R(tau)], tau[k] = k/fs (see
    tdanalysis/data/acorrs/PROVENANCE.md).  The row holding R is chosen by the same rule
    as tdanalysis' loaders: ``arr[1] if abs(arr[1,0]) >= abs(arr[0,0]) else arr[0]``
    (tau[0] = 0, R(0) > 0).  A bare 1-D array is taken as R(tau) at spacing ``deltaT``.

    Raises ValueError if the lag spacing differs from ``deltaT`` (relative tolerance
    ``rel_tol``; the ILE analysis rate must match the ACF's, e.g. --srate 4096 for the
    4096 Hz real-event files) or the array is malformed.  Returns R(tau) as float64.
    """
    arr = np.load(path)
    arr = np.asarray(arr, dtype=np.float64)
    if arr.ndim == 1:
        acf = arr
    elif arr.ndim == 2 and arr.shape[0] == 2:
        if abs(arr[1, 0]) >= abs(arr[0, 0]):
            tau, acf = arr[0], arr[1]
        else:
            tau, acf = arr[1], arr[0]
        if tau.size < 2:
            raise ValueError("ACF file %s: fewer than two lags" % path)
        dtau = np.diff(tau)
        if not np.allclose(dtau, dtau[0], rtol=rel_tol, atol=0):
            raise ValueError("ACF file %s: lag grid is not uniform" % path)
        if abs(dtau[0] - deltaT) > rel_tol * deltaT:
            raise ValueError(
                "ACF file %s: lag spacing %.9g s (ACF sample rate %.6g Hz) does not match "
                "the analysis deltaT %.9g s (%.6g Hz). Run ILE at the ACF's rate, e.g. "
                "--srate %d (ILE defaults to 16384), or supply an ACF at %.6g Hz."
                % (path, dtau[0], 1. / dtau[0], deltaT, 1. / deltaT, int(round(1. / dtau[0])),
                   1. / deltaT))
    else:
        raise ValueError("ACF file %s: expected shape (2, N) [tau, R] or (N,), got %s"
                         % (path, arr.shape))
    if not np.all(np.isfinite(acf)):
        raise ValueError("ACF file %s: non-finite entries" % path)
    if acf[0] <= 0:
        raise ValueError("ACF file %s: R(0) = %g is not positive" % (path, acf[0]))
    return np.array(acf, dtype=np.float64)


def acf_from_psd(psd, deltaF, fs, n_lags, fLow, fMax, condition_number=DEFAULT_ACF_CONDITION_NUMBER):
    """ACF lags 0..n_lags-1 from a one-sided PSD with tdanalysis' PSDToAutocorr.

    Same construction as ToeplitzInnerProductContext.gs_acf (in-band PSD samples, band
    patching: constant wall outside [fLow, fMax], re-gridded onto an FFT long enough that
    the kept lags do not wrap), with the condition number passed explicitly.  Zero /
    non-finite PSD bins (e.g. below a per-detector --fmin-ifo cut) are left out of the
    band.  Returns (acf, info) where info is PSDToAutocorr.patch_info (wall value,
    n_floored = in-band bins clipped up to S_max/cond, ...).
    """
    from tdanalysis.covariance.psd import PSDToAutocorr, PSDToAutocorrConfig
    if isinstance(psd, lal.REAL8FrequencySeries):
        S = np.asarray(psd.data.data, dtype=np.float64)
    else:
        S = np.asarray(psd, dtype=np.float64)
    f = np.arange(S.size) * float(deltaF)
    band = np.isfinite(S) & (S > 0) & (f >= fLow) & (f < fMax)
    if not band.any():
        raise ValueError("acf_from_psd: no positive finite PSD bins in [%g, %g) Hz" % (fLow, fMax))
    fs_int = int(round(fs))
    duration = (n_lags - 1) / float(fs)
    conv = PSDToAutocorr(PSDToAutocorrConfig(
        sample_rate=fs_int, acorr_duration=duration, f_min=float(fLow), f_max=float(fMax),
        patch_band=True, run_diagnostics=False,
        target_condition_number=float(condition_number)))
    _, acf = conv._psd_to_autocorr(f[band], S[band], fs_int, duration)
    info = dict(getattr(conv, "patch_info", {}) or {})
    info["condition_number"] = float(condition_number)
    return np.asarray(acf[:n_lags], dtype=np.float64), info


def highpass_zero_phase(x, fs, f_corner, order=8):
    """Zero-phase Butterworth high-pass of a real time series (second-order sections run
    forward and backward, scipy.signal.sosfiltfilt), for strain whose power below the band
    exceeds what a walled ACF describes.  An order-N Butterworth run forward and backward has
    magnitude response 1/(1 + (f_corner/f)^(2N)):
    at f_corner = 10 Hz, order 8, it is 1 - 2e-5 at 20 Hz.  Edge transients last ~1/f_corner
    seconds, so analysis pieces should stay a few 1/f_corner away from the data edges."""
    sos = scipy.signal.butter(int(order), float(f_corner), btype="highpass", fs=float(fs),
                              output="sos")
    return scipy.signal.sosfiltfilt(sos, np.asarray(x, dtype=np.float64))


def parse_td_pieces(piece_strings, mref_msun):
    """Convert repeatable --td-piece 'START,END' (units of M_ref) to seconds.

    Returns a list of (start_s, end_s) relative to the reference time, sorted, with each
    end > start.  Overlaps are refused (pieces must be disjoint to be independent blocks).
    """
    if not piece_strings:
        return []
    if mref_msun is None or mref_msun <= 0:
        raise ValueError("--td-piece needs --td-mref MSUN (> 0) to convert M to seconds")
    msec = float(mref_msun) * lal.MTSUN_SI
    pieces = []
    for s in piece_strings:
        parts = [p for p in str(s).replace(";", ",").split(",") if p.strip() != ""]
        if len(parts) != 2:
            raise ValueError("--td-piece %r: expected 'START,END' in units of M_ref" % s)
        a, b = float(parts[0]) * msec, float(parts[1]) * msec
        if not b > a:
            raise ValueError("--td-piece %r: END must exceed START" % s)
        pieces.append((a, b))
    pieces.sort()
    for (a0, b0), (a1, b1) in zip(pieces[:-1], pieces[1:]):
        if a1 < b0:
            raise ValueError("--td-piece intervals overlap: [%g,%g] and [%g,%g] s"
                             % (a0, b0, a1, b1))
    return pieces


def template_route_is_fd(approx):
    """True if lalsimutils.hlmoft would build these modes from a frequency-domain
    generator (ChooseFDModes, SimInspiralTD-based IMRPhenomP reconstruction, or
    TDModesFromPolarizations of an FD-implemented approximant).  Those carry lalsuite's
    own conditioning that cannot be switched off, so the td-gs route refuses them.
    Mirrors the branch order of hlmoft."""
    import lalsimulation as lalsim
    fd_modes = [getattr(lsu, n, None) for n in
                ("lalIMRPhenomHM", "lalIMRPhenomXHM", "lalIMRPhenomXPHM", "lalIMRPhenomXO4a",
                 "lalSEOBNRv4HM_ROM", "lalIMRPhenomXPNR")]
    if approx in [a for a in fd_modes if a is not None and a >= 0]:
        return True
    seob = [getattr(lsu, n, None) for n in ("lalSEOBv4", "lalTEOBv2", "lalTEOBv4", "lalSEOBNRv4HM")]
    seob += [getattr(lalsim, n, None) for n in
             ("SEOBNRv2", "SEOBNRv1", "SEOBNRv4_opt", "EOBNRv2", "SEOBNRv3", "SEOBNRv3_opt")]
    if approx in [a for a in seob if a is not None and a >= 0]:
        return False
    name = approx if isinstance(approx, str) else lalsim.GetStringFromApproximant(approx)
    if 'IMRPhenomP' in name or approx == getattr(lsu, "lalIMRPhenomXP", None) \
            or lsu.check_FD_pending(approx):
        return True
    if lalsim.SimInspiralImplementedFDApproximants(approx) == 1:
        return True
    return False


# --------------------------------------------------------------------------- Toeplitz solvers

def _gs_apply_batch(gs, V):
    """C^{-1} V for a real (N, r) matrix with a prepared tdanalysis GSQuadraticForm.

    Same Gohberg-Semencul recursion as ``gs.apply_Cinv`` (w = [L(x)U(y) - L(c)U(d)] v / x0,
    lower/upper-triangular Toeplitz applies as linear convolutions), but with the FFTs
    taken along axis 0 of the whole column block at once.  Falls back to the per-column
    apply if the prepared kernels are not exposed.
    """
    d = getattr(gs, "_d", None)
    if d is None or not all(k in d for k in ("N", "L", "Kx", "Ky", "Kc", "Kd", "x0")):
        return np.stack([np.array(gs.apply_Cinv(V[:, k]), copy=True)
                         for k in range(V.shape[1])], axis=1)
    N, L = int(d["N"]), int(d["L"])
    Kx, Ky, Kc, Kd = (d["Kx"][:, None], d["Ky"][:, None], d["Kc"][:, None], d["Kd"][:, None])
    r = V.shape[1]
    pad = np.zeros((L, r), dtype=np.float64)

    def lower(K, M):
        pad[:N] = M
        pad[N:] = 0.
        return scipy.fft.irfft(K * scipy.fft.rfft(pad, axis=0), n=L, axis=0)[:N]

    def upper(K, M):
        pad[:N] = M[::-1]
        pad[N:] = 0.
        return scipy.fft.irfft(K * scipy.fft.rfft(pad, axis=0), n=L, axis=0)[:N][::-1]

    return (lower(Kx, upper(Ky, V)) - lower(Kc, upper(Kd, V))) / float(d["x0"])


def validate_covariance_acf(acf):
    """Check positive innovations of a real symmetric Toeplitz covariance.

    Durbin recursion is O(N^2) time and O(N) storage. A successful linear solve
    alone cannot distinguish a covariance from an invertible indefinite matrix.
    """
    r = np.asarray(acf, dtype=np.float64)
    if r.ndim != 1 or not len(r) or not np.all(np.isfinite(r)) or r[0] <= 0:
        raise ValueError("ACF must be finite with positive zero-lag variance")
    r = r / r[0]
    innovation = 1.0
    coefficients = np.empty(0)
    for k in range(1, len(r)):
        reflection = -(r[k] + np.dot(coefficients, r[1:k][::-1])) / innovation
        if not np.isfinite(reflection) or abs(reflection) >= 1.0:
            raise ValueError("ACF is not a positive-definite covariance (lag %d)" % k)
        coefficients = np.r_[coefficients + reflection * coefficients[::-1], reflection]
        innovation *= 1.0 - reflection * reflection
        if not np.isfinite(innovation) or innovation <= 0:
            raise ValueError("ACF has nonpositive innovations")


def gs_generators(acf):
    """x = C^{-1} e_1 and y = C^{-1} e_n for C = Toeplitz(acf): a Levinson Toeplitz solve
    (O(n^2)) with two steps of iterative refinement (FFT Toeplitz products), which brings
    the ~1e-8 Levinson residuals on walled ACFs (cond ~ 1e8) down to roundoff."""
    acf = np.asarray(acf, dtype=np.float64)
    validate_covariance_acf(acf)
    n = acf.size
    e1 = np.zeros(n); e1[0] = 1.0
    en = np.zeros(n); en[-1] = 1.0
    x = scipy.linalg.solve_toeplitz(acf, e1, check_finite=False)
    y = scipy.linalg.solve_toeplitz(acf, en, check_finite=False)
    for _ in range(2):
        x = x + scipy.linalg.solve_toeplitz(
            acf, e1 - scipy.linalg.matmul_toeplitz(acf, x, check_finite=False), check_finite=False)
        y = y + scipy.linalg.solve_toeplitz(
            acf, en - scipy.linalg.matmul_toeplitz(acf, y, check_finite=False), check_finite=False)
    return x, y


def gs_kernels(x, y):
    """The four GS correlation kernels (x, y~, c, d) and x0, laid out as in
    GSQuadraticForm.prepare: y~ = y reversed, c = [0, y[:-1]], d = [0, x[n-1..1]]."""
    x = np.asarray(x, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    x0 = float(x[0])
    if x0 == 0.0:
        raise ValueError("GS requires x[0] != 0.")
    return (x, y[::-1].copy(), np.concatenate(([0.0], y[:-1])),
            np.concatenate(([0.0], x[:0:-1])), x0)


def prepare_gs_form(acf, x=None, y=None):
    """A prepared tdanalysis GSQuadraticForm for C = Toeplitz(acf) whose two generator
    columns come from ``gs_generators`` (O(n^2)) instead of the dense inverse
    GSQuadraticForm.prepare() would build (O(n^3): ~1 min and 1 GB at n = 12288).  The
    kernels are laid out exactly as prepare() lays them out, so both gs.apply_Cinv and
    _gs_apply_batch work on the result."""
    from types import SimpleNamespace
    from tdanalysis.forms.gs import GSQuadraticForm
    from importlib.metadata import version
    if version('tdanalysis') != '1.0.0':
        raise RuntimeError("TD-GS fast prepared-kernel adapter requires tdanalysis==1.0.0; install RIFT[td-gs]")
    acf = np.asarray(acf, dtype=np.float64)
    N = acf.size
    if x is None or y is None:
        x, y = gs_generators(acf)
    kx, ky, kc, kd, x0 = gs_kernels(x, y)
    L = scipy.fft.next_fast_len(2 * N)

    def kernel_rfft(k):
        kpad = np.zeros(L)
        kpad[:N] = k
        return scipy.fft.rfft(kpad)

    d = {"N": N, "L": L, "dtype": np.float64, "x0": x0,
         "Kx": kernel_rfft(kx), "Ky": kernel_rfft(ky),
         "Kc": kernel_rfft(kc), "Kd": kernel_rfft(kd),
         "pad": np.zeros(L), "B": np.zeros(N), "D": np.zeros(N),
         "AB": np.zeros(N), "CD": np.zeros(N), "w": np.zeros(N)}
    parent = SimpleNamespace(n_samples_l=N, data_dtype=np.float64, invCij=None,
                             cfft_use_fast_len=True)
    gs = GSQuadraticForm(parent=parent, prepared=True, _d=d)
    # Contract probe uses the public method and Toeplitz product, independently of
    # the batched adapter. Catch incompatible buffer/kernel semantics before use.
    probe = np.zeros(N); probe[0] = 1.0
    answer = np.array(gs.apply_Cinv(probe), copy=True)
    residual = scipy.linalg.matmul_toeplitz(acf, answer, check_finite=False) - probe
    if not np.all(np.isfinite(answer)) or np.linalg.norm(residual, ord=np.inf) > 1e-5:
        raise RuntimeError("tdanalysis GS prepared-kernel contract/residual check failed")
    return gs


class _PieceSolver(object):
    """C_p^{-1} on one contiguous piece of n samples: dense inverse (n <= dense_max_n) or
    tdanalysis GS (prepared once, applied in column batches).  Also holds the GS
    generator columns x, y used by the recursion route for U(t), V(t)."""

    def __init__(self, acf, dense_max_n):
        self.n = int(acf.size)
        self.dense = self.n <= int(dense_max_n)
        if self.dense:
            C = scipy.linalg.toeplitz(acf)
            self._cho = scipy.linalg.cho_factor(C, lower=True, check_finite=False)
            self.Cinv = scipy.linalg.cho_solve(self._cho, np.eye(self.n), check_finite=False)
            self.Cinv = 0.5 * (self.Cinv + self.Cinv.T)
            self.x, self.y = self.Cinv[:, 0].copy(), self.Cinv[:, -1].copy()
            self._gs = None
        else:
            self.Cinv = None
            self._cho = None
            self.x, self.y = gs_generators(acf)
            self._gs = prepare_gs_form(acf, self.x, self.y)
        self.kernels = gs_kernels(self.x, self.y)

    def apply(self, V):
        """C^{-1} V, V real or complex, shape (n,) or (n, r)."""
        V = np.asarray(V)
        one_d = (V.ndim == 1)
        if one_d:
            V = V[:, None]
        if np.iscomplexobj(V):
            out = self._apply_real(np.ascontiguousarray(V.real)) \
                + 1j * self._apply_real(np.ascontiguousarray(V.imag))
        else:
            out = self._apply_real(np.ascontiguousarray(V, dtype=np.float64))
        return out[:, 0] if one_d else out

    def _apply_real(self, V):
        if self.dense:
            return self.Cinv @ V
        r = V.shape[1]
        out = np.empty_like(V)
        for c0 in range(0, r, _GS_BATCH_COLUMNS):
            out[:, c0:c0 + _GS_BATCH_COLUMNS] = _gs_apply_batch(self._gs, V[:, c0:c0 + _GS_BATCH_COLUMNS])
        return out

    def whiten(self, v):
        """L^{-1} v with C = L L^T (dense pieces only; None otherwise)."""
        if self._cho is None:
            return None
        return scipy.linalg.solve_triangular(self._cho[0], np.asarray(v, dtype=np.float64),
                                             lower=True, check_finite=False)


class TDGSDetectorContext(object):
    r"""Per-detector state for the td-gs Precompute (built once per ILE job).

    Parameters
    ----------
    data_td : lal.REAL8TimeSeries (or COMPLEX16TimeSeries)
        The detector strain, unwindowed.  Samples at index >= ``n_valid`` are zero
        padding (ILE pads to a power of two) and may not be part of any piece.
    psd : lal.REAL8FrequencySeries or ndarray
        One-sided PSD on the rfft grid of the full series (deltaF = 1/(N deltaT)); used
        only to build the ACF when ``acf`` is None (tdanalysis PSDToAutocorr, walled
        outside [fLow, fMax)).
    fLow, fMax : float
        Analysis band [Hz] for the PSD->ACF conversion.
    pieces : list of (t0_gps, t1_gps) or None
        Analysis pieces as GPS seconds, fixed for the whole run.  None = the full valid
        data segment (one piece).
    acf : ndarray or None
        R(tau) at lags 0, deltaT, 2 deltaT, ... (e.g. from load_acf_file).  Must cover
        the longest piece.
    n_valid : int or None
        Number of real (unpadded) samples; default = full length.
    dense_max_n : int
        Pieces with at most this many samples use a dense C^{-1}; longer ones use GS.
    u_stride : int
        Compute U(t), V(t) every ``u_stride`` samples of the storage grid and fill the rest
        by cubic interpolation (1 = exact at every sample).
    u_method : {"auto", "prefix", "recursion", "batched", "chunked"}
        How U(t), V(t) are evaluated (see the module header).  All routes are exact.
        "prefix" (windowed sums of one correlation per mode and kernel) needs the
        shifted template to end inside -- or, mirrored, to start inside -- every piece
        for every grid shift and raises otherwise; "recursion" is exact for every cut
        pattern with no per-shift solve; "batched" is the direct reference (stacked cut
        templates, C^{-1} applied once; memory n x N_window per mode); "chunked" is
        "batched" in column blocks of at most ``uv_block_bytes``.  "auto" (default) uses
        "chunked" on dense pieces, and on GS pieces "prefix" where its condition holds,
        "recursion" elsewhere.  The choice made for each piece is in ``self.uv_routes``
        after precompute.
    uv_block_bytes : int
        Memory bound of one column block (stacked templates of all modes, C^{-1} applied
        to one mode, GS work arrays) on the "chunked" route.
    acf_condition_number : float
        target_condition_number for the PSD->ACF wall (ignored when ``acf`` is given).
    whiteness_check : bool
        Also whiten d_p with a Cholesky factor (dense pieces) and report the variance and
        lag-1 autocorrelation of L^{-1} d_p.  The cheap d_p^T C_p^{-1} d_p / n_p is always
        computed (``self.whiteness``).
    Calibration marginalization is not supported by this public port.
    """

    def __init__(self, data_td, psd, fLow, fMax, pieces=None, acf=None, n_valid=None,
                 dense_max_n=4096, u_stride=1, u_method="auto",
                 acf_condition_number=DEFAULT_ACF_CONDITION_NUMBER, whiteness_check=False,
                 ifo=None, verbose=True, uv_block_bytes=DEFAULT_UV_BLOCK_BYTES,
                 calibration_correction_type="rift", calibration_fir_half_length=None):
        self.ifo = ifo
        self.deltaT = float(data_td.deltaT)
        self.epoch = data_td.epoch
        self.N = int(data_td.data.length)
        if calibration_correction_type != "rift" or calibration_fir_half_length is not None:
            raise NotImplementedError("TD-GS calibration is deferred to a separate port")
        self.n_valid = self.N if n_valid is None else int(n_valid)
        self.d = np.asarray(data_td.data.data)
        if np.iscomplexobj(self.d):
            if np.max(np.abs(self.d.imag)) > 0:
                raise ValueError("td-gs data must be real strain")
            self.d = self.d.real
        self.d = np.array(self.d, dtype=np.float64)
        if int(u_stride) != 1:
            raise NotImplementedError("td-gs u_stride must be 1; interpolated norms need positivity validation")
        self.u_stride = max(1, int(u_stride))
        if u_method not in U_METHODS:
            raise ValueError("u_method must be one of %s" % (U_METHODS,))
        self.u_method = u_method
        self.uv_block_bytes = int(uv_block_bytes)
        self.uv_routes = []           # per piece, the route precompute used last
        self.dense_max_n = int(dense_max_n)
        self.fLow, self.fMax = float(fLow), float(fMax)
        self._warned_support = False

        # ---- ACF: supplied, or from the PSD (tdanalysis PSDToAutocorr) ----
        self.acf_supplied = acf is not None
        self.acf_info = {}
        if self.acf_supplied:
            self.acf = np.asarray(acf, dtype=np.float64)
        else:
            self.acf, self.acf_info = acf_from_psd(
                psd, 1.0 / (self.N * self.deltaT), 1.0 / self.deltaT, self.N,
                self.fLow, self.fMax, condition_number=acf_condition_number)

        # ---- pieces -> sample ranges [i0, i1) in the data frame ----
        self.explicit_pieces = pieces is not None and len(pieces) > 0
        if self.explicit_pieces:
            runs = []
            for (t0, t1) in pieces:
                i0 = int(round((float(t0) - float(self.epoch)) / self.deltaT))
                i1 = int(round((float(t1) - float(self.epoch)) / self.deltaT))
                if i0 < 0 or i1 > self.n_valid or i1 <= i0:
                    raise ValueError(
                        "%s: piece [%.6f, %.6f] GPS -> samples [%d, %d) does not lie inside "
                        "the valid data [0, %d) (data start %.6f, %.6g s long)"
                        % (ifo, t0, t1, i0, i1, self.n_valid, float(self.epoch),
                           self.n_valid * self.deltaT))
                runs.append((i0, i1))
            runs.sort()
            for (a0, b0), (a1, b1) in zip(runs[:-1], runs[1:]):
                if a1 < b0:
                    raise ValueError("%s: pieces overlap in samples: %s" % (ifo, runs))
            self.runs = runs
        else:
            self.runs = [(0, self.n_valid)]

        # ---- per piece: solver, w_p = C_p^{-1} d_p (once per job), whiteness ----
        self.solvers = []
        self.w = []
        self.whiteness = []
        for (i0, i1) in self.runs:
            n = i1 - i0
            if self.acf.size < n:
                raise ValueError("%s: ACF too short for piece [%d, %d): %d lags supplied, %d needed"
                                 % (ifo, i0, i1, self.acf.size, n))
            sol = _PieceSolver(self.acf[:n], self.dense_max_n)
            self.solvers.append(sol)
            w = sol.apply(self.d[i0:i1])
            self.w.append(w)
            chi2_per_n = float(self.d[i0:i1] @ w) / n
            entry = dict(i0=int(i0), i1=int(i1), n=int(n), chi2_per_n=chi2_per_n,
                         white_var=None, white_lag1=None)
            if whiteness_check:
                z = sol.whiten(self.d[i0:i1])
                if z is not None:
                    entry["white_var"] = float(np.mean(z * z))
                    entry["white_lag1"] = float(np.mean(z[1:] * z[:-1]) / np.mean(z * z))
            self.whiteness.append(entry)
        if verbose:
            self.report()

    def acf_lags(self, n):
        """ACF lags 0..n-1 (the covariance of an n-sample piece is Toeplitz of this)."""
        if self.acf.size < n:
            raise ValueError("ACF has %d lags; %d needed" % (self.acf.size, n))
        return self.acf[:n]

    # ------------------------------------------------------------------ calibration

    def calibration_filters(self, *args, **kwargs):
        raise NotImplementedError("TD-GS calibration is deferred to a separate port")

    def report(self):
        """Print the one-line summary and the per-piece whiteness diagnostic."""
        print("  [td-gs] %s: %d piece(s) %s samples, solver(s) %s, ACF %s, U/V by %s"
              % (self.ifo, len(self.runs), [(int(a), int(b)) for (a, b) in self.runs],
                 ["dense" if s.dense else "gs" for s in self.solvers],
                 "file (%d lags)" % self.acf.size if self.acf_supplied
                 else "PSD->tdanalysis (cond %.1e)" % self.acf_info.get("condition_number", np.nan),
                 self.u_method))
        if not self.acf_supplied:
            n_floored = int(self.acf_info.get("n_floored", 0) or 0)
            keys = [k for k in ("n_floored", "n_walled", "wall", "s_floor", "s_lo", "s_hi", "nfft", "df")
                    if k in self.acf_info]
            print("  [td-gs] %s: PSD->ACF patch info: %s"
                  % (self.ifo, ", ".join("%s=%s" % (k, ("%.4g" % self.acf_info[k]) if isinstance(
                      self.acf_info[k], float) else self.acf_info[k]) for k in keys)))
            if n_floored > 0:
                print("  [td-gs] %s: WARNING: %d in-band PSD bins were FLOORED at S_max/cond by the "
                      "ACF conditioning; the TD noise model differs from the PSD there. Raise "
                      "--td-acf-cond or narrow the band." % (self.ifo, n_floored))
        for e in self.whiteness:
            flag = "" if 0.7 <= e["chi2_per_n"] <= 1.4 else "   <-- WARNING: far from 1"
            extra = ""
            if e["white_var"] is not None:
                extra = ", whitened var %.3f, lag-1 corr %.3f" % (e["white_var"], e["white_lag1"])
            print("  [td-gs] %s: piece [%d, %d) d^T C^-1 d / n = %.3f%s%s"
                  % (self.ifo, e["i0"], e["i1"], e["chi2_per_n"], extra, flag))

    # ------------------------------------------------------------------ template placement

    def _check_template_support(self, j_first, N_shift, N_window):
        """Refuse (explicit pieces) or warn (implicit full segment) when the template's
        first non-zero sample would fall inside a piece for some shift on the storage grid.

        With no taper the template switches on abruptly at its first sample; a piece that
        extends before that turn-on contains a step the data does not have, which biases
        real-data fits.  Shift k puts the turn-on at data sample k + j_first; the grid is
        k in [N_shift, N_shift + N_window - 1].  (With calibration the same check applies
        to the unfiltered template; the FIR then smears the turn-on step into the piece
        with the taps' magnitude at the turn-on distance, which precompute reports as the
        turn-on leakage -- the FD route's circulant multiplication smears it the same way.)
        """
        lo = N_shift + j_first
        hi = N_shift + N_window - 1 + j_first
        for (i0, i1) in self.runs:
            # some shift puts the turn-on strictly after the piece start and before its end
            if lo < i1 and hi > i0:
                msg = ("%s: the template turn-on (first non-zero sample) lands inside piece "
                       "[%d, %d) for some storage-grid shifts (turn-on at data samples %d..%d, "
                       "i.e. %.3f..%.3f s after the piece start). The template must already be "
                       "on at the piece start for EVERY shift of the +-storage window: lower "
                       "--fmin-template (longer template), start the piece later, or shrink "
                       "--internal-data-storage-window-half."
                       % (self.ifo, i0, i1, lo, hi, (lo - i0) * self.deltaT, (hi - i0) * self.deltaT))
                if self.explicit_pieces:
                    raise ValueError(msg)
                if not self._warned_support:
                    print("  [td-gs] WARNING (full segment, no --td-piece): " + msg)
                    self._warned_support = True

    def _shift_stack(self, h, i0, i1, N_shift, N_window):
        """View X (n, T): X[:, i] = template placed at shift k = N_shift + i, cut to
        [i0, i1).  X[n', i] = h[i0 + n' - N_shift - i] (zero outside the template)."""
        n, T = i1 - i0, N_window
        q_min = i0 - N_shift - T + 1
        g = np.zeros(n + T - 1, dtype=np.complex128)
        a = max(q_min, 0)
        b = min(q_min + n + T - 1, h.size)
        if b > a:
            g[a - q_min:b - q_min] = h[a:b]
        X_rev = np.lib.stride_tricks.sliding_window_view(g, T)[:n]    # X_rev[n', t'] = g[n'+t']
        return X_rev[:, ::-1]

    @staticmethod
    def _template_window(h, q0, length):
        """h[q0 : q0+length] with zeros outside h's support."""
        out = np.zeros(length, dtype=np.complex128)
        a = max(q0, 0)
        b = min(q0 + length, h.size)
        if b > a:
            out[a - q0:b - q0] = h[a:b]
        return out

    def _q_piece(self, h, w, off, T):
        """Q(t) = sum_m conj(g_t[m]) w[m] for t = 0..T-1, g_t[m] = h[m + off - t]: the rows
        of a sliding-window view of the zero-extended template (no copy) against w.
        Row r of the view is g_{T-1-r}; the conjugate is taken on w and the result (w is
        real on the piece, so this only avoids copying the view)."""
        n = w.size
        hp = self._template_window(h, off - (T - 1), n + T - 1)   # hp[j] = h[j + off - (T-1)]
        H = np.lib.stride_tricks.sliding_window_view(hp, n)       # H[r] = hp[r : r+n]
        return np.conj(H @ np.conj(w))[::-1]

    def _add_q(self, harr, modes, N_shift, T, Q, piece_ids):
        """Q for every mode on the pieces in ``piece_ids`` (all when None); nothing when
        ``Q`` is None (the calibrated templates, whose Q_c comes from the adjoint route)."""
        if Q is None:
            return
        for ip, ((i0, i1), w) in enumerate(zip(self.runs, self.w)):
            if piece_ids is not None and ip not in piece_ids:
                continue
            for m in modes:
                Q[m] += self._q_piece(harr[m], w, i0 - N_shift, T)

    def _pair_tables(self, modes, nsel):
        U = {}
        V = {}
        for ia, ma in enumerate(modes):
            for mb in modes[ia:]:
                U[(ma, mb)] = np.zeros(nsel, dtype=np.complex128)
                V[(ma, mb)] = np.zeros(nsel, dtype=np.complex128)
        return U, V

    def _uv_batched(self, harr, modes, N_shift, T, tsel, Q, piece_ids=None, block_bytes=None):
        """Reference route: stacked cut templates, C^{-1} applied once per mode.
        ``piece_ids`` restricts the sum to those pieces (default: all).  With
        ``block_bytes`` the selected columns are processed in blocks whose stacked
        templates (all modes), C^{-1}-applied block (one mode) and GS work arrays stay
        under that many bytes ("chunked"); None = one block (the plain reference)."""
        nsel = tsel.size
        nm = len(modes)
        U, V = self._pair_tables(modes, nsel)
        self._add_q(harr, modes, N_shift, T, Q, piece_ids)
        for ip, ((i0, i1), sol, w) in enumerate(zip(self.runs, self.solvers, self.w)):
            if piece_ids is not None and ip not in piece_ids:
                continue
            n = i1 - i0
            if block_bytes is None:
                B = nsel
            else:
                # per column: nm + 1 complex128 columns of n (stacked templates, C^{-1} of
                # one mode), plus the GS apply's float64 work (zero-padded column, rfft
                # product, inverse; ~6 arrays of L ~ 2 n, real and imaginary parts in turn)
                per_col = 16 * n * (nm + 1) + (0 if sol.dense else 48 * sol._gs._d["L"])
                B = max(1, min(nsel, int(block_bytes // per_col)))
            X = {m: self._shift_stack(harr[m], i0, i1, N_shift, T) for m in modes}
            for c0 in range(0, nsel, B):
                cols = tsel[c0:c0 + B]
                Xs = {m: np.ascontiguousarray(X[m][:, cols]) for m in modes}
                for ib, mb in enumerate(modes):
                    Y = sol.apply(Xs[mb])                   # C^{-1} applied once per mode
                    for ma in modes[:ib + 1]:
                        U[(ma, mb)][c0:c0 + B] += np.einsum('nt,nt->t', Xs[ma].conj(), Y)
                        V[(ma, mb)][c0:c0 + B] += np.einsum('nt,nt->t', Xs[ma], Y)
        return U, V

    # ------------------------------------------------------------------ prefix-sum route

    def _prefix_orientation(self, ip, j_first, j_last, N_shift, T):
        """Whether the prefix-sum identity holds on piece ``ip`` for every grid shift, and
        in which orientation.  Template sample j sits at piece-local index j + t - off,
        off = i0 - N_shift, for grid shift t = 0..T-1.

        "end"   : last non-zero sample inside for every t  <=>  j_last + T - 1 - off <= n - 1
        "start" : first non-zero sample inside for every t <=>  j_first - off >= 0
                  (persymmetry; the reversed piece then satisfies the "end" condition)
        Returns "end", "start" or None.  j_last < j_first (no non-zero sample) is "end".
        """
        i0, i1 = self.runs[ip]
        n = i1 - i0
        off = i0 - N_shift
        if j_last + (T - 1) - off <= n - 1:
            return "end"
        if j_first - off >= 0:
            return "start"
        return None

    def _uv_prefix_piece(self, harr, modes, ip, N_shift, T, orientation):
        r"""U_ab(t), V_ab(t) for all t = 0..T-1 on piece ``ip`` by the prefix-sum identity.

        Piece-local template g_t[m] = hh[m + o_t], m = 0..n-1 (zero outside hh's support),
        where for orientation "end" hh = h and o_t = off - t, and for "start" hh is the
        reversed template, hh[i] = h[N_h-1-i], and o_t = (N_h - n - off) + t: the reversed
        piece-local vector (J g_t)[m] = g_t[n-1-m] = h[n-1-m+off-t] = hh[m + N_h-n-off + t],
        and u^H C^{-1} v = (J u)^H C^{-1} (J v) because J C^{-1} J = C^{-1} (symmetric
        Toeplitz C).  Under the validity condition hh[q] = 0 for q >= n + min_t o_t, so
            E_k[g_t][m] = sum_{s=0}^{n-1-m} k[s] hh[m+s+o_t] = sum_{s=0}^{n-1} k[s] hh[m+s+o_t]
                        = F_k[m + o_t],       F_k[q] = sum_s k[s] hh[q+s]   (one correlation),
        the dropped terms s > n-1-m having hh index >= n + o_t.  Hence
            U_ab(t) = sum_{m=0}^{n-1} P_ab[m + o_t],
            P_ab[q] = [conj(F_x^a[q]) F_y~^b[q] - conj(F_c^a[q]) F_d^b[q]] / x0,
        a window of length n of the fixed sequence P_ab (V_ab: the same without the
        conjugates); the cut at the piece start never enters, E_k[u][m] only uses
        u[j], j >= m >= 0.  The windows [o_t, o_t + n) for consecutive t overlap; their
        sums are the shared core sum plus short running sums of the two edges (a plain
        prefix-sum difference when T - 1 >= n and the windows have no common core).
        """
        i0, i1 = self.runs[ip]
        n = i1 - i0
        off = i0 - N_shift
        kx, ky, kc, kd, x0 = self.solvers[ip].kernels
        kernels = (kx, ky, kc, kd)
        nm = len(modes)
        if orientation == "end":
            hh = {m: harr[m] for m in modes}
            o = off - np.arange(T)                              # o_t, decreasing
        else:
            Nh = max(harr[m].size for m in modes)               # common reversed origin:
            hh = {m: np.concatenate((harr[m], np.zeros(Nh - harr[m].size,                # h[j]
                                                       dtype=np.complex128)))[::-1]      # at Nh-1-j
                  for m in modes}
            o = (Nh - n - off) + np.arange(T)                    # o_t, increasing
        qmin, qmax = int(o.min()), int(o.max()) + n - 1
        Lw = qmax - qmin + 1                                     # = n + T - 1
        # F_k[q], q = qmin..qmax, from hw[p] = hh[qmin + p]; hh vanishes beyond qmax.
        F = np.empty((4, nm, Lw), dtype=np.complex128)
        for ia, ma in enumerate(modes):
            hw = self._template_window(hh[ma], qmin, Lw)
            for ik, k in enumerate(kernels):
                F[ik, ia] = scipy.signal.fftconvolve(hw, k[::-1])[n - 1:n - 1 + Lw]
        lo = o - qmin                                            # window [lo_t, lo_t + n)
        U = np.empty((nm, nm, T), dtype=np.complex128)
        V = np.empty((nm, nm, T), dtype=np.complex128)
        lo_min, lo_max = int(lo.min()), int(lo.max())
        core = (lo_max, lo_min + n)                              # inside every window
        for ia in range(nm):
            for ib in range(ia, nm):
                for out, Fa in ((U, F[:, ia].conj()), (V, F[:, ia])):
                    P = (Fa[0] * F[1, ib] - Fa[2] * F[3, ib]) / x0
                    if core[1] > core[0]:
                        s_core = np.sum(P[core[0]:core[1]])
                        # left edge [lo_t, lo_max): running sum from lo_max downwards
                        left = np.concatenate(([0.], np.cumsum(P[lo_min:lo_max][::-1])))[::-1]
                        # right edge [lo_min + n, lo_t + n): running sum upwards
                        right = np.concatenate(([0.], np.cumsum(P[lo_min + n:lo_max + n])))
                        out[ia, ib] = s_core + left[lo - lo_min] + right[lo - lo_min]
                    else:
                        S = np.concatenate(([0.], np.cumsum(P)))
                        out[ia, ib] = S[lo + n] - S[lo]
        return U, V

    def _uv_prefix(self, harr, modes, N_shift, T, tsel, Q, piece_ids=None, orientations=None):
        """Prefix-sum route on the pieces in ``piece_ids`` (default: all), whose
        orientations ("end"/"start") are given in ``orientations`` (keyed by piece)."""
        nm = len(modes)
        Usel = np.zeros((nm, nm, tsel.size), dtype=np.complex128)
        Vsel = np.zeros((nm, nm, tsel.size), dtype=np.complex128)
        self._add_q(harr, modes, N_shift, T, Q, piece_ids)
        for ip in range(len(self.runs)):
            if piece_ids is not None and ip not in piece_ids:
                continue
            Up, Vp = self._uv_prefix_piece(harr, modes, ip, N_shift, T, orientations[ip])
            Usel += Up[:, :, tsel]
            Vsel += Vp[:, :, tsel]
        U = {}
        V = {}
        for ia, ma in enumerate(modes):
            for ib in range(ia, nm):
                U[(ma, modes[ib])] = Usel[ia, ib]
                V[(ma, modes[ib])] = Vsel[ia, ib]
        return U, V

    def _uv_recursion(self, harr, modes, N_shift, T, tsel, Q, piece_ids=None):
        r"""GS-generator route (module header): for each piece, kernel k in (x, y~, c, d)
        and mode a, keep A_k^a(t)[m] = sum_{j>=m} k[j-m] g_t^a[j] (g_t^a = template a at
        shift t cut to the piece, piece-local index), and slide t -> t+1 with
            A(t+1)[m] = A(t)[m-1] - k[n-m] g_t[n-1]   (m >= 1),
            A(t+1)[0] = sum_s k[s] g_{t+1}[s]          (head: a precomputed correlation),
        then U_ab(t) = [<conj A_x^a, A_y~^b> - <conj A_c^a, A_d^b>](t) / x0 and V without
        the conjugation.  ``piece_ids`` restricts the sum to those pieces (default: all).

        Layout: per kernel one Fortran-ordered real ring buffer B_k of shape (2 nm, n+T-1)
        (rows: real parts of all modes, then imaginary parts) with A_k(t) = B_k[:, s:s+n],
        s = T-1-t, so the shift is a moving window and the slide is one BLAS rank-one
        update on the window that becomes A(t+1)[1:] plus the head column; U and V come
        out of one real product G = A_x A_y~^T - A_c A_d^T per shift, whose nm x nm
        blocks are (ac, ad; bc, bd) for A^a = a + i b, A^b = c + i d:
        conj(A^a).A^b = (ac + bd) + i (ad - bc), A^a.A^b = (ac - bd) + i (ad + bc).
        """
        from scipy.linalg import blas
        nm = len(modes)
        nsel = tsel.size
        Usel = np.zeros((nm, nm, nsel), dtype=np.complex128)
        Vsel = np.zeros((nm, nm, nsel), dtype=np.complex128)
        sel_pos = -np.ones(T, dtype=int)
        sel_pos[tsel] = np.arange(nsel)
        self._add_q(harr, modes, N_shift, T, Q, piece_ids)
        for ip, ((i0, i1), sol, w) in enumerate(zip(self.runs, self.solvers, self.w)):
            if piece_ids is not None and ip not in piece_ids:
                continue
            n = i1 - i0
            off = i0 - N_shift            # g_t[m] = h[m + off - t]
            kx, ky, kc, kd, x0 = sol.kernels
            kernels = (kx, ky, kc, kd)
            krev = [np.ascontiguousarray(k[1:][::-1]) for k in kernels]   # krev[m-1] = k[n-m]
            # Initial A(0): correlation of the cut template at shift 0 with each kernel,
            # A[m] = sum_s k[s] g_0[m+s]; and the heads A(t)[0], t = 1..T-1.
            B = [np.zeros((2 * nm, n + T - 1), order='F') for _ in kernels]
            heads = np.zeros((4, 2 * nm, T))
            g_last = np.zeros((2 * nm, T))                    # g_t[n-1] = h[n-1+off-t], re; im
            for ia, ma in enumerate(modes):
                h = harr[ma]
                g0 = self._template_window(h, off, n)
                # head window: sum_s k[s] h[s + off - t] for t = 0..T-1 needs h[off-T+1 .. off+n-1]
                hw = self._template_window(h, off - (T - 1), n + T - 1)
                gl = self._template_window(h, n - 1 + off - (T - 1), T)[::-1]
                g_last[ia], g_last[nm + ia] = gl.real, gl.imag
                for ik, k in enumerate(kernels):
                    kr = k[::-1]
                    A0 = scipy.signal.fftconvolve(g0, kr)[n - 1:2 * n - 1]
                    B[ik][ia, T - 1:], B[ik][nm + ia, T - 1:] = A0.real, A0.imag
                    # F[q] = sum_s k[s] h[s+q] for q = off-(T-1) .. off  ->  heads[t] = F[off - t]
                    F = scipy.signal.fftconvolve(hw, kr)[n - 1:n - 1 + T][::-1]
                    heads[ik, ia], heads[ik, nm + ia] = F.real, F.imag
            for t in range(T):
                p = sel_pos[t]
                s = T - 1 - t
                if p >= 0:
                    G = blas.dgemm(1.0, B[0][:, s:s + n], B[1][:, s:s + n], trans_b=1) \
                        - blas.dgemm(1.0, B[2][:, s:s + n], B[3][:, s:s + n], trans_b=1)
                    ac, ad, bc, bd = G[:nm, :nm], G[:nm, nm:], G[nm:, :nm], G[nm:, nm:]
                    Usel[:, :, p] += ((ac + bd) + 1j * (ad - bc)) / x0
                    Vsel[:, :, p] += ((ac - bd) + 1j * (ad + bc)) / x0
                if t == T - 1:
                    break
                gl = g_last[:, t]
                for ik in range(4):
                    sub = B[ik][:, s:s + n - 1]
                    out = blas.dger(-1.0, gl, krev[ik], a=sub, overwrite_a=1)
                    if out is not sub and not np.shares_memory(out, sub):
                        raise RuntimeError("td-gs recursion: BLAS rank-one update was not in place")
                    B[ik][:, s - 1] = heads[ik, :, t + 1]
        U = {}
        V = {}
        for ia, ma in enumerate(modes):
            for ib in range(ia, nm):
                U[(ma, modes[ib])] = Usel[ia, ib]
                V[(ma, modes[ib])] = Vsel[ia, ib]
        return U, V

    def precompute(self, hlms, N_shift, N_window, verbose=False, calibration=None,
                   calibration_self_term=True):
        """Q_lm(t), U(t), V(t) on the storage grid for TD modes ``hlms`` ({(l,m): lal
        COMPLEX16TimeSeries}, all with the same epoch/deltaT).

        Returns (rholms, crossTerms, crossTermsV); see the module header for the contract.

        Calibration arguments are refused.
        """
        if calibration is not None:
            raise NotImplementedError("TD-GS calibration is deferred to a separate port")
        modes = list(hlms.keys())
        h0 = hlms[modes[0]]
        if abs(float(h0.deltaT) - self.deltaT) > 1e-12 * self.deltaT:
            raise ValueError("template deltaT %g != data deltaT %g" % (h0.deltaT, self.deltaT))
        N_shift, T = int(N_shift), int(N_window)
        harr = {m: np.asarray(hlms[m].data.data, dtype=np.complex128) for m in modes}
        j_first, j_last = self._template_support(harr, modes)
        self._check_template_support(j_first, N_shift, T)

        # U/V evaluation points on the grid (stride) -- always include both ends.
        tsel = np.arange(0, T, self.u_stride)
        if tsel[-1] != T - 1:
            tsel = np.append(tsel, T - 1)

        routes, orient = self._select_routes(j_first, j_last, N_shift, T)
        self.uv_routes = [(r, orient[ip] if r == "prefix" else None) for ip, r in enumerate(routes)]

        Q = {m: np.zeros(T, dtype=np.complex128) for m in modes}
        U, V = self._uv_all(harr, modes, N_shift, T, tsel, routes, orient, Q)
        crossTerms, crossTermsV = self._complete_pairs(U, V, modes, tsel, T)
        rholms = self._rholm_series(Q, hlms, modes, N_shift, T)
        if verbose:
            print("  [td-gs] %s: N_shift=%d N_window=%d template support %d..%d; U/V routes %s; "
                  "U(2,2) centre %.6g, |U| range %.3g..%.3g"
                  % (self.ifo, N_shift, T, j_first, j_last,
                     [r if o is None else "%s(%s)" % (r, o) for (r, o) in self.uv_routes],
                     np.real(crossTerms[(modes[0], modes[0])][T // 2]),
                     np.min(np.abs(crossTerms[(modes[0], modes[0])])),
                     np.max(np.abs(crossTerms[(modes[0], modes[0])]))))
        return rholms, crossTerms, crossTermsV

    @staticmethod
    def _template_support(harr, modes):
        """(first, last) non-zero sample over the modes; (0, -1) when all are zero."""
        support = [np.flatnonzero(np.abs(harr[m]) > 0) for m in modes]
        support = [s for s in support if s.size]
        j_first = min([int(s[0]) for s in support] or [0])
        j_last = max([int(s[-1]) for s in support] or [-1])
        return j_first, j_last

    def _select_routes(self, j_first, j_last, N_shift, T):
        """Route per piece (module header).  "prefix" needs its validity condition on
        every piece it is used on; "auto" checks it piece by piece."""
        orient = {ip: self._prefix_orientation(ip, j_first, j_last, N_shift, T)
                  for ip in range(len(self.runs))}
        if self.u_method == "auto":
            routes = []
            for ip, sol in enumerate(self.solvers):
                if sol.dense:
                    routes.append("chunked")
                elif orient[ip] is not None:
                    routes.append("prefix")
                else:
                    routes.append("recursion")
        else:
            routes = [self.u_method] * len(self.runs)
            if self.u_method == "prefix":
                bad = [self.runs[ip] for ip in orient if orient[ip] is None]
                if bad:
                    raise ValueError(
                        "%s: u_method='prefix' needs the shifted template to end inside (or "
                        "start inside) the piece for every grid shift; pieces %s fail "
                        "(template support %d..%d, N_shift %d, N_window %d). Use 'auto'."
                        % (self.ifo, bad, j_first, j_last, N_shift, T))
        return routes, orient

    def _uv_all(self, harr, modes, N_shift, T, tsel, routes, orient, Q):
        """U, V pair tables (upper triangle, at tsel) summed over the pieces, each piece on
        its route; Q (dict, or None to skip) accumulates the data term on the way."""
        U, V = self._pair_tables(modes, tsel.size)
        evaluators = {
            "batched": lambda ids: self._uv_batched(harr, modes, N_shift, T, tsel, Q, piece_ids=ids),
            "chunked": lambda ids: self._uv_batched(harr, modes, N_shift, T, tsel, Q, piece_ids=ids,
                                                    block_bytes=self.uv_block_bytes),
            "recursion": lambda ids: self._uv_recursion(harr, modes, N_shift, T, tsel, Q, piece_ids=ids),
            "prefix": lambda ids: self._uv_prefix(harr, modes, N_shift, T, tsel, Q, piece_ids=ids,
                                                  orientations=orient),
        }
        for name, fn in evaluators.items():
            ids = [ip for ip, r in enumerate(routes) if r == name]
            if not ids:
                continue
            U2, V2 = fn(ids)
            for key in U:
                U[key] += U2[key]
                V[key] += V2[key]
        return U, V

    def _complete_pairs(self, U, V, modes, tsel, T):
        """Hermitian (U) / symmetric (V) completion, then stride interpolation."""
        crossTerms, crossTermsV = {}, {}
        for ia, ma in enumerate(modes):
            for mb in modes[ia:]:
                u, v = self._expand(U[(ma, mb)], tsel, T), self._expand(V[(ma, mb)], tsel, T)
                crossTerms[(ma, mb)] = u
                crossTermsV[(ma, mb)] = v
                if mb != ma:
                    crossTerms[(mb, ma)] = np.conj(u)
                    crossTermsV[(mb, ma)] = v.copy()
        return crossTerms, crossTermsV

    def _rholm_series(self, Q, hlms, modes, N_shift, T):
        """lal series of Q (length T, or n_cal*T of contiguous realization blocks) with the
        epoch bookkeeping of ComputeModeIPTimeSeries: rhoTS.epoch = data.epoch - hlm.epoch,
        then DataRollBins adds N_shift*deltaT (block 0's epoch for every block)."""
        rholms = {}
        for m in modes:
            ts = lal.CreateCOMPLEX16TimeSeries("Complex overlap", lal.LIGOTimeGPS(0.), 0.,
                                               self.deltaT, _dimensionless_unit(), Q[m].size)
            ts.epoch = self.epoch - hlms[m].epoch
            ts.epoch += N_shift * self.deltaT
            ts.data.data = Q[m]
            rholms[m] = ts
        return rholms

    def _expand(self, y, tsel, T):
        if tsel.size == T:
            return np.array(y, dtype=np.complex128)
        from scipy.interpolate import CubicSpline
        t = np.arange(T)
        return CubicSpline(tsel, y.real)(t) + 1j * CubicSpline(tsel, y.imag)(t)


# --------------------------------------------------------------------------- Precompute driver

def identify_effective_modes_td(crossTermsOneDetector, fac):
    """td-gs analogue of factored_likelihood.IdentifyEffectiveModesForDetector for
    time-dependent cross terms: a mode p is effective when max_t |U_{p,p2}(t)| exceeds
    fac * |U_{22,22}| at the grid centre for some p2.  Returns the set of kept modes."""
    modes = set(k[1] for k in crossTermsOneDetector)
    ref = abs(crossterm_centre(crossTermsOneDetector[((2, 2), (2, 2))])) * fac
    keep = set()
    for p in modes:
        for p2 in modes:
            if np.max(np.abs(np.asarray(crossTermsOneDetector[(p, p2)]))) > ref:
                keep.add(p)
                break
    return keep


def crossterm_centre(value):
    """Scalar view of a cross term for diagnostics: the array's centre sample when U is
    time-dependent, the value itself otherwise."""
    if np.ndim(value) == 0:
        return value
    v = np.asarray(value)
    return v[v.size // 2]


def generate_td_modes(P, Lmax, extra_waveform_kwargs=None, quiet=True):
    """TD modes for the td-gs route: lalsimutils.hlmoft with conditioning off.

    Refuses frequency-domain generators (see template_route_is_fd).  Sets P.taper to
    TAPER_NONE so the SEOB (complex_hoft) and TEOB branches apply no taper either, and
    passes no_condition=True so hlmoft's own start tapers are skipped.
    """
    import lalsimulation as lalsim
    if template_route_is_fd(P.approx):
        name = P.approx if isinstance(P.approx, str) else lalsim.GetStringFromApproximant(P.approx)
        raise ValueError(
            "td-gs: approximant %s is generated in the frequency domain (lalsuite "
            "conditioning cannot be switched off); use a time-domain-native model "
            "(e.g. IMRPhenomTPHM, SEOBNRv4PHM, NRSur7dq4, SEOBNRv4)." % name)
    P.taper = lsu.lsu_TAPER_NONE
    kw = dict(extra_waveform_kwargs or {})
    kw['no_condition'] = True
    kw.pop('fd_alignment_postevent_time', None)   # ChooseFDModes-only knob
    hlms = lsu.hlmoft(P, Lmax, **kw)
    if not isinstance(hlms, dict):
        hlms = lsu.SphHarmTimeSeries_to_dict(hlms, Lmax)
    if not quiet:
        m0 = list(hlms.keys())[0]
        print("  [td-gs] %d TD modes, length %d, epoch %.6f s, deltaT %g"
              % (len(hlms), hlms[m0].data.length, float(hlms[m0].epoch), hlms[m0].deltaT))
    return hlms


def precompute_likelihood_terms_td_gs(event_time_geo, t_window, P, data_dict, td_gs_contexts,
                                      Lmax, verbose=True, quiet=False,
                                      extra_waveform_kwargs=None, skip_interpolation=False,
                                      return_calibration_crossterms=False,
                                      calibration_realizations=None, calibration_conjugate=False,
                                      calibration_self_term=True, **refused):
    """td-gs branch of PrecomputeLikelihoodTerms.  Same return tuple.

    Calibration realizations are refused by this public port.

    ``refused`` carries the keyword arguments of PrecomputeLikelihoodTerms that this route
    does not support (glitch/TD-Toeplitz factory, ROM/NR/gwsignal/external EOB generators);
    any of them set to a non-default value is an error.
    """
    from RIFT.likelihood import factored_likelihood as fl
    _not_supported = {
        'IP_factory': None, 'ROM_group': None,
        'ROM_param': None, 'NR_group': None, 'NR_param': None, 'nr_lookup': False,
        'use_external_EOB': False, 'use_gwsignal': False, 'hybrid_use': False,
        'ROM_use_basis': False, 'use_provided_strain': False,
    }
    ignore_threshold = refused.pop('ignore_threshold', None)
    for key, default in _not_supported.items():
        if key in refused and refused[key] not in (default, 0, False, None):
            raise ValueError("td-gs: %s=%r is not supported on the time-domain route "
                             "(planned for a later stage)" % (key, refused[key]))
    if calibration_realizations is not None:
        raise NotImplementedError("public TD-GS calibration port is deferred; sample without calibration marginalization")
    detectors = list(data_dict.keys())
    first_data = data_dict[detectors[0]]
    P.dist = fl.distMpcRef * 1e6 * lsu.lsu_PC
    P.deltaT = float(first_data.deltaT)
    P.deltaF = 1.0 / (first_data.data.length * P.deltaT)
    if not quiet:
        print("  ++++ Template data being computed for the following binary (td-gs) +++ ")
        P.print_params()
    hlms = generate_td_modes(P, Lmax, extra_waveform_kwargs, quiet=quiet)

    rholms, rholms_intp, crossTerms, crossTermsV = {}, {}, {}, {}
    crossTermsCal = crossTermsCalV = None
    for det in detectors:
        ctx = td_gs_contexts[det]
        data = data_dict[det]
        if abs(float(data.deltaT) - P.deltaT) > 1e-12 * P.deltaT or data.data.length != first_data.data.length:
            raise ValueError("td-gs: detector %s data grid differs from %s" % (det, detectors[0]))
        t_det = fl.ComputeArrivalTimeAtDetector(det, P.phi, P.theta, event_time_geo)
        # Identical bookkeeping to the FD route (PrecomputeLikelihoodTerms).
        rho_epoch = data.epoch - hlms[list(hlms.keys())[0]].epoch
        t_shift = float(float(t_det) - float(t_window) - float(rho_epoch))
        N_shift = int(t_shift / P.deltaT + 0.5)
        N_window = int(2 * t_window / P.deltaT)
        rholms[det], crossTerms[det], crossTermsV[det] = ctx.precompute(
            hlms, N_shift, N_window, verbose=verbose)
        if ignore_threshold is not None and det == detectors[0]:
            # Mode pruning on the first detector, as the FD route does with its fiducial
            # cross terms, but with time-dependent U: a mode is kept when max_t |U(t)| of
            # any of its pairs exceeds ignore_threshold * |U_22,22| at the grid centre.
            keep = identify_effective_modes_td(crossTerms[det], ignore_threshold)
            keep = keep.union(set([(l, -m) for (l, m) in keep]))
            print("  Worthwhile modes : ", keep)
            if len(keep) == 0:
                raise ValueError("td-gs: no mode passes ignore_threshold")
            hlms = {m: hlms[m] for m in hlms if m in keep}
            rholms[det] = {m: rholms[det][m] for m in keep if m in rholms[det]}
            crossTerms[det] = {k: v for k, v in crossTerms[det].items() if k[0] in keep and k[1] in keep}
            crossTermsV[det] = {k: v for k, v in crossTermsV[det].items() if k[0] in keep and k[1] in keep}
        t = np.arange(N_window) * P.deltaT + float(rho_epoch + N_shift * P.deltaT)
        if verbose:
            print("For detector", det, "(td-gs) ...")
            print("\tData starts at %.20g" % float(data.epoch))
            print("\tEvent time at detector is: %.18g" % float(t_det))
            print("\tComputed t_shift = %.20g, N_shift = %d" % (t_shift, N_shift))
            print("\tInterpolation starts at time %.20g" % t[0])
        if not skip_interpolation:
            rholms_intp[det] = fl.InterpolateRholms(rholms[det], t, verbose=verbose)
        else:
            rholms_intp[det] = None

    # SNR guess as in PrecomputeLikelihoodTerms, using the centre-of-grid U for scale.
    rho_max = 0
    for ifo in rholms:
        for mode in rholms[ifo]:
            u_c = np.real(crossterm_centre(crossTerms[ifo][(mode, mode)]))
            if u_c > 0:
                rho_max_ifo = np.max(np.abs(rholms[ifo][mode].data.data))
                rho_max += rho_max_ifo ** 2 / u_c
    rho_max = np.sqrt(rho_max) / 2.3
    print("SNR guess (internal, from det response) ", rho_max, rho_max ** 2 / 2)
    guess_snr = rho_max

    if return_calibration_crossterms:
        return rholms_intp, crossTerms, crossTermsV, rholms, guess_snr, None, crossTermsCal, crossTermsCalV
    return rholms_intp, crossTerms, crossTermsV, rholms, guess_snr, None
