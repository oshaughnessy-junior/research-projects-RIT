#
# td_gs_precompute.py
#
# Time-domain Precompute code path for ILE (--internal-ile-inner-product td-gs).
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
# The Toeplitz algebra is tdanalysis' (the names of the final tdanalysis 1.1 release,
# docs/toeplitz_api.md there):
#     tdanalysis.forms.toeplitz_solver.ToeplitzSolver   C_p^{-1} on one piece: a Cholesky
#                                                       factor for short pieces, the
#                                                       Gohberg-Semencul (GS) form for
#                                                       long ones (generators by refined
#                                                       Levinson solves of the ACF)
#     tdanalysis.forms.sliding.sliding_data_term        Q of templates sliding through a piece
#     tdanalysis.forms.sliding.sliding_self_terms       U, V of templates sliding through a piece
#     tdanalysis.forms.sliding.is_turn_on_inside_window the template turn-on check
#     ToeplitzSolver.apply_Cinv                         C_p^{-1} d_p, once per piece and job
#     ToeplitzSolver.whiten                             L^{-1} d_p for the whiteness check
# This module adds what is RIFT's: lal series and epochs, the (l,m) mode bookkeeping, the
# analysis pieces (in units of M_ref on the ILE command line), RIFT's storage grid of
# shifts, the sum over pieces, the turn-on refusal and its message, and the output
# contract below.
#
# Per detector and ILE job (built ONCE, TDGSDetectorContext):
#     w_p = C_p^{-1} d_p                 for every piece p (ToeplitzSolver of piece p)
# Per intrinsic point (TDGSDetectorContext.precompute), for every shift k on RIFT's
# storage grid (k = N_shift + i, i = 0..N_window-1; template sample j sits at data
# sample j + k):
#     Q_lm(k)        = sum_p  conj(h_lm,k |_p)^T  w_p
#     U_lm,l'm'(k)   = sum_p  conj(h_lm,k |_p)^T  C_p^{-1}  h_l'm',k |_p
#     V_lm,l'm'(k)   = sum_p        (h_lm,k |_p)^T  C_p^{-1}  h_l'm',k |_p
# where h|_p is the shifted template cut to piece p.  U follows lsu.ComplexIP's
# conj(h1) h2 convention; V is RIFT's crossTermsV = <conj(h_lm) | h_l'm'>.  With
# tdanalysis' ACF convention conj(u)^T C^{-1} v IS the matched-filter inner product
# (ip_normalization = 1, see ToeplitzInnerProductContext.ip_normalization).
#
# Each piece p is the window [i0, i1) of the data: sliding_data_term and
# sliding_self_terms run on it with offset = i0 and the data-frame shifts, so their
# convention (template sample j at window sample j + k) is RIFT's, and the per-piece
# results are summed here.
#
# U(t), V(t) have four exact methods in tdanalysis (no truncation, interpolation or
# scalar-U shortcut; they differ only in cost and memory), chosen with --td-u-method:
#
#   "prefix"     windowed sums of one Gohberg-Semencul kernel correlation per mode, valid
#                when the shifted template ends inside the piece for every grid shift (or,
#                mirrored, starts inside it); four FFT correlations per mode, then
#                O(n + N_window) per mode pair.  Refused (ValueError) on a piece where the
#                condition fails.
#   "recursion"  the kernel correlations slid one sample at a time: O(n N_window) per
#                mode, any cut pattern, no per-shift solve.
#   "batched"    direct reference: the cut shifted templates stacked as columns, C^{-1}
#                applied once per mode (memory n x N_window per mode).
#   "chunked"    "batched" in column blocks under a memory bound (uv_block_bytes).
#
# "auto" (default): pieces on the Cholesky solver use "chunked" (exact there); GS pieces
# use "prefix" when its condition holds (the full segment in an ILE run, ringdown
# pieces) and "recursion" otherwise (a piece the template overruns at both ends, where
# the per-column FFT solves of the batched methods cost far more than the recursion).
# The method used on each piece is kept in TDGSDetectorContext.uv_routes and printed
# with verbose output.  "prefix" and "recursion" use the GS generators of the piece's
# solver, which tdanalysis computes from the ACF for Cholesky pieces too; on a badly
# conditioned ACF they agree with the Cholesky solve to ~cond(C) eps (~1e-9 at cond 1e8).
# FFTs appear only as exact zero-padded linear-correlation engines inside this Toeplitz
# algebra; nothing is windowed.
#
# Output contract (what the likelihood consumes):
#     rholms[mode]            lal.COMPLEX16TimeSeries, length N_window, deltaT, epoch =
#                             data.epoch - hlm.epoch + N_shift*deltaT -- exactly the series
#                             factored_likelihood.ComputeModeIPTimeSeries returns after its
#                             DataRollBins/Cut (the fractional part of (data.epoch -
#                             hlm.epoch)/deltaT is carried in the epoch, as on the FD code path;
#                             the template is only ever shifted by whole samples).
#     crossTerms[(p1,p2)]     complex ndarray of length N_window, aligned sample-by-sample
#     crossTermsV[(p1,p2)]    with rholms (entry i <-> shift k = N_shift + i).
#

import numpy as np

import lal

import RIFT.lalsimutils as lsu


def _tdanalysis_location():
    try:
        import tdanalysis
        return "%s %s" % (getattr(tdanalysis, "__version__", "?"), getattr(tdanalysis, "__file__", "?"))
    except Exception as e:   # not importable at all
        return "not importable: %r" % (e,)


_TDANALYSIS_NEEDED = "tdanalysis 1.1 (the final release)"

try:
    import tdanalysis.forms.toeplitz_solver as _toeplitz_solver
    import tdanalysis.forms.sliding as _sliding
    import tdanalysis.forms.gs as _gs
    import tdanalysis.covariance.psd as _psd
except ImportError as _exc:
    raise ImportError(
        "the td-gs code path (RIFT.likelihood.td_gs_precompute) needs %s ("
        "tdanalysis.forms.toeplitz_solver, tdanalysis.forms.sliding). "
        "The tdanalysis on this path (%s) does not have them: %s"
        % (_TDANALYSIS_NEEDED, _tdanalysis_location(), _exc))


def _missing_tdanalysis_api():
    """Names of the tdanalysis 1.1 API this module uses that the tdanalysis on the path lacks.

    Earlier tdanalysis 1.1 development trees carry the same version string with other
    names (ToeplitzSolver.apply, turn_on_inside_window, want_V, ...), so every name is
    checked here, before anything is imported from them."""
    import dataclasses
    import inspect
    wanted = [
        (_toeplitz_solver, ("ToeplitzSolver",)),
        (_sliding, ("sliding_data_term", "sliding_self_terms", "is_turn_on_inside_window",
                    "SLIDING_METHODS", "DEFAULT_BLOCK_BYTES", "PrefixNotApplicable")),
        (_gs, ("gs_generators_from_acorr",)),
        (_psd, ("PSDToAutocorr", "PSDToAutocorrConfig")),
    ]
    missing = ["%s.%s" % (mod.__name__, name) for mod, names in wanted for name in names
               if not hasattr(mod, name)]
    solver = getattr(_toeplitz_solver, "ToeplitzSolver", None)
    if solver is not None:
        missing += ["tdanalysis.forms.toeplitz_solver.ToeplitzSolver.%s" % name
                    for name in ("apply_Cinv", "whiten") if not hasattr(solver, name)]
    if hasattr(_sliding, "sliding_self_terms"):
        params = inspect.signature(_sliding.sliding_self_terms).parameters
        missing += ["the %s= argument of tdanalysis.forms.sliding.sliding_self_terms" % name
                    for name in ("offset", "compute_V") if name not in params]
    config = getattr(_psd, "PSDToAutocorrConfig", None)
    if config is not None and dataclasses.is_dataclass(config) and \
            "sampling_rate" not in {f.name for f in dataclasses.fields(config)}:
        missing.append("the sampling_rate field of tdanalysis.covariance.psd.PSDToAutocorrConfig")
    return missing


_missing = _missing_tdanalysis_api()
if _missing:
    raise ImportError(
        "the td-gs code path (RIFT.likelihood.td_gs_precompute) needs %s. The tdanalysis "
        "on this path (%s) lacks %s; earlier 1.1 development trees carry the same version "
        "string with other names. Install tdanalysis 1.1 (pip install 'tdanalysis>=1.1,<2') "
        "or update the checkout."
        % (_TDANALYSIS_NEEDED, _tdanalysis_location(), ", ".join(_missing)))

from tdanalysis.forms.toeplitz_solver import ToeplitzSolver  # noqa: E402
from tdanalysis.forms.sliding import (sliding_data_term, sliding_self_terms,  # noqa: E402
                                      is_turn_on_inside_window, SLIDING_METHODS,
                                      DEFAULT_BLOCK_BYTES, PrefixNotApplicable)

DEFAULT_ACF_CONDITION_NUMBER = 1.0e8   # as the tdanalysis real-event / XG ACF files
U_METHODS = tuple(SLIDING_METHODS)     # ("auto", "prefix", "recursion", "batched", "chunked")
DEFAULT_UV_BLOCK_BYTES = DEFAULT_BLOCK_BYTES   # memory bound of one column block of "chunked"


# --------------------------------------------------------------------------- helpers

def _dimensionless_unit():
    try:
        return lal.DimensionlessUnit
    except AttributeError:
        return lsu.lsu_DimensionlessUnit


def load_acf_file(path, deltaT, rel_tol=1e-6):
    """Load a tdanalysis autocorrelation file and check its sampling.

    Format: float64 array of shape (2, N) with rows [tau, R(tau)], tau[k] = k/fs (as
    tdanalysis writes them).  The row holding R is chosen by the same rule
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
        sampling_rate=fs_int, acorr_duration=duration, f_min=float(fLow), f_max=float(fMax),
        patch_band=True, run_diagnostics=False,
        target_condition_number=float(condition_number)))
    _, acf = conv._psd_to_autocorr(f[band], S[band], fs_int, duration)
    info = dict(getattr(conv, "patch_info", {}) or {})
    info["condition_number"] = float(condition_number)
    return np.asarray(acf[:n_lags], dtype=np.float64), info


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
    own conditioning that cannot be switched off, so the td-gs code path refuses them.
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


# --------------------------------------------------------------------------- per-detector context

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
        Pieces with at most this many samples use the Cholesky method of tdanalysis'
        ToeplitzSolver; longer ones its Gohberg-Semencul method.  Both are exact.
    u_stride : int
        Compute U(t), V(t) every ``u_stride`` samples of the storage grid and fill the rest
        by cubic interpolation (1 = exact at every sample).
    u_method : {"auto", "prefix", "recursion", "batched", "chunked"}
        How U(t), V(t) are evaluated (tdanalysis.forms.sliding.sliding_self_terms, see the
        module header).  All methods are exact.
        "prefix" (windowed sums of one correlation per mode and kernel) needs the
        shifted template to end inside -- or, mirrored, to start inside -- every piece
        for every grid shift and raises otherwise; "recursion" is exact for every cut
        pattern with no per-shift solve; "batched" is the direct reference (stacked cut
        templates, C^{-1} applied once; memory n x N_window per mode); "chunked" is
        "batched" in column blocks of at most ``uv_block_bytes``.  "auto" (default) uses
        "chunked" on Cholesky pieces, and on GS pieces "prefix" where its condition holds,
        "recursion" elsewhere.  The choice made for each piece is in ``self.uv_routes``
        after precompute, as (method, orientation or None).
    uv_block_bytes : int
        Memory bound of one column block (stacked templates of all modes, C^{-1} applied
        to one mode, GS work arrays) by the "chunked" method.
    acf_condition_number : float
        target_condition_number for the PSD->ACF wall (ignored when ``acf`` is given).
    whiteness_check : bool
        Also whiten every piece, z = L^{-1} d_p with C_p = L L^T, by the piece solver's
        ``whiten`` (its Cholesky factor on Cholesky pieces; on GS pieces a Levinson-Durbin
        recursion, O(n_p^2) once per job with nothing n_p x n_p formed, whose rounding error
        at cond 1e8 is ~4e-7 relative, far below what the diagnostic resolves), and report
        the variance and lag-1 autocorrelation of z.  The cheap d_p^T C_p^{-1} d_p / n_p is
        always computed (``self.whiteness``).
    """

    def __init__(self, data_td, psd, fLow, fMax, pieces=None, acf=None, n_valid=None,
                 dense_max_n=4096, u_stride=1, u_method="auto",
                 acf_condition_number=DEFAULT_ACF_CONDITION_NUMBER, whiteness_check=False,
                 ifo=None, verbose=True, uv_block_bytes=DEFAULT_UV_BLOCK_BYTES):
        self.ifo = ifo
        self.deltaT = float(data_td.deltaT)
        self.epoch = data_td.epoch
        self.N = int(data_td.data.length)
        self.n_valid = self.N if n_valid is None else int(n_valid)
        self.d = np.asarray(data_td.data.data)
        if np.iscomplexobj(self.d):
            if np.max(np.abs(self.d.imag)) > 0:
                raise ValueError("td-gs data must be real strain")
            self.d = self.d.real
        self.d = np.array(self.d, dtype=np.float64)
        self.u_stride = max(1, int(u_stride))
        if u_method not in U_METHODS:
            raise ValueError("u_method must be one of %s" % (U_METHODS,))
        self.u_method = u_method
        self.uv_block_bytes = int(uv_block_bytes)
        self.uv_routes = []           # per piece, the U/V method precompute used last
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

        # ---- per piece: tdanalysis ToeplitzSolver (validates the covariance), w_p =
        #      C_p^{-1} d_p (once per job), whiteness ----
        self.solvers = []
        self.w = []
        self.whiteness = []
        for (i0, i1) in self.runs:
            n = i1 - i0
            if self.acf.size < n:
                raise ValueError("%s: ACF too short for piece [%d, %d): %d lags supplied, %d needed"
                                 % (ifo, i0, i1, self.acf.size, n))
            sol = ToeplitzSolver(self.acf[:n], dense_max_n=self.dense_max_n)
            self.solvers.append(sol)
            w = sol.apply_Cinv(self.d[i0:i1])
            self.w.append(w)
            chi2_per_n = float(self.d[i0:i1] @ w) / n
            entry = dict(i0=int(i0), i1=int(i1), n=int(n), chi2_per_n=chi2_per_n,
                         white_var=None, white_lag1=None)
            if whiteness_check:
                z = sol.whiten(self.d[i0:i1])
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

    def report(self):
        """Print the one-line summary and the per-piece whiteness diagnostic."""
        print("  [td-gs] %s: %d piece(s) %s samples, solver(s) %s, ACF %s, U/V by %s"
              % (self.ifo, len(self.runs), [(int(a), int(b)) for (a, b) in self.runs],
                 [s.method for s in self.solvers],
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

    @staticmethod
    def _stack_modes(harr, modes):
        """The template modes as the rows of one (n_modes, M) complex array, zero-extended
        to the longest mode (the layout tdanalysis' sliding forms take)."""
        M = max(harr[m].size for m in modes)
        H = np.zeros((len(modes), M), dtype=np.complex128)
        for ia, m in enumerate(modes):
            H[ia, :harr[m].size] = harr[m]
        return H

    @staticmethod
    def _template_support(H):
        """(first, last) non-zero sample over the rows of H; (0, -1) when all are zero."""
        nz = np.flatnonzero(np.any(np.abs(H) > 0, axis=0))
        if nz.size == 0:
            return 0, -1
        return int(nz[0]), int(nz[-1])

    def _check_template_support(self, H, j_first, N_shift, N_window):
        """Refuse (explicit pieces) or warn (implicit full segment) when the template's
        first non-zero sample would fall inside a piece for some shift on the storage grid.

        With no taper the template switches on abruptly at its first sample; a piece that
        extends before that turn-on contains a step the data does not have, which biases
        real-data fits.  Shift k puts the turn-on at data sample k + j_first; the grid is
        k in [N_shift, N_shift + N_window - 1] (tdanalysis.forms.sliding.
        is_turn_on_inside_window on each piece).
        """
        shifts = N_shift + np.arange(N_window)
        lo = N_shift + j_first
        hi = N_shift + N_window - 1 + j_first
        for (i0, i1) in self.runs:
            # some shift puts the turn-on strictly after the piece start and before its end
            if is_turn_on_inside_window(H, shifts, i1 - i0, offset=i0):
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

    # ------------------------------------------------------------------ Q, U, V over the pieces

    def _q_all(self, H, N_shift, T):
        """Q(t) of every row of H at the shifts N_shift + t, t = 0..T-1, summed over the
        pieces: tdanalysis' sliding_data_term on each piece (offset = piece start) against
        w_p = C_p^{-1} d_p.  Returns (n_modes, T) complex."""
        shifts = N_shift + np.arange(T)
        Q = np.zeros((H.shape[0], T), dtype=np.complex128)
        for (i0, i1), w in zip(self.runs, self.w):
            Q += sliding_data_term(H, w, shifts, offset=i0)
        return Q

    @staticmethod
    def _method_label(label):
        """tdanalysis' method label ("prefix(end)", "recursion", ...) as RIFT's
        (method, orientation or None) pair."""
        if label.startswith("prefix(") and label.endswith(")"):
            return ("prefix", label[len("prefix("):-1])
        return (label, None)

    def _uv_all(self, H, N_shift, T, tsel, j_first, j_last):
        """U(t), V(t) of every pair of rows of H at the shifts N_shift + tsel, summed over
        the pieces: tdanalysis' sliding_self_terms on each piece (offset = piece start)
        with that piece's solver and the context's u_method.  Returns (U, V, methods):
        U, V of shape (len(tsel), n_modes, n_modes), Hermitian / symmetric in the modes,
        and per piece the (method, orientation or None) used.  u_method="prefix" on a
        piece where the prefix condition fails raises ValueError naming every such piece
        (template support j_first..j_last)."""
        nm = H.shape[0]
        shifts = N_shift + np.asarray(tsel)
        U = np.zeros((tsel.size, nm, nm), dtype=np.complex128)
        V = np.zeros((tsel.size, nm, nm), dtype=np.complex128)
        methods, bad = [], []
        for (i0, i1), sol in zip(self.runs, self.solvers):
            try:
                Up, Vp, label = sliding_self_terms(H, sol, shifts, method=self.u_method,
                                                   block_bytes=self.uv_block_bytes, offset=i0,
                                                   return_method=True)
            except PrefixNotApplicable:
                # only raised for u_method="prefix"; collect every failing piece
                bad.append((i0, i1))
                methods.append(None)
                continue
            U += Up
            V += Vp
            methods.append(self._method_label(label))
        if bad:
            raise ValueError(
                "%s: u_method='prefix' needs the shifted template to end inside (or "
                "start inside) the piece for every grid shift; pieces %s fail "
                "(template support %d..%d, N_shift %d, N_window %d). Use 'auto'."
                % (self.ifo, bad, j_first, j_last, N_shift, T))
        return U, V, methods

    def _complete_pairs(self, U, V, modes, tsel, T):
        """crossTerms, crossTermsV dicts {(mode_a, mode_b): length-T array} from the
        (len(tsel), n_modes, n_modes) U, V: the upper triangle, filled between the stride
        points by cubic interpolation, and its Hermitian (U) / symmetric (V) completion."""
        crossTerms, crossTermsV = {}, {}
        for ia, ma in enumerate(modes):
            for ib in range(ia, len(modes)):
                mb = modes[ib]
                u, v = self._expand(U[:, ia, ib], tsel, T), self._expand(V[:, ia, ib], tsel, T)
                crossTerms[(ma, mb)] = u
                crossTermsV[(ma, mb)] = v
                if mb != ma:
                    crossTerms[(mb, ma)] = np.conj(u)
                    crossTermsV[(mb, ma)] = v.copy()
        return crossTerms, crossTermsV

    def _rholm_series(self, Q, hlms, modes, N_shift, deltaT):
        """lal series of Q ({mode: array}) at spacing ``deltaT`` with the epoch bookkeeping
        of ComputeModeIPTimeSeries: rhoTS.epoch = data.epoch - hlm.epoch, then DataRollBins
        adds N_shift*deltaT."""
        rholms = {}
        for m in modes:
            ts = lal.CreateCOMPLEX16TimeSeries("Complex overlap", lal.LIGOTimeGPS(0.), 0.,
                                               deltaT, _dimensionless_unit(), Q[m].size)
            ts.epoch = self.epoch - hlms[m].epoch
            ts.epoch += N_shift * self.deltaT
            ts.data.data = Q[m]
            rholms[m] = ts
        return rholms

    def precompute(self, hlms, N_shift, N_window, verbose=False):
        """Q_lm(t), U(t), V(t) on the storage grid for TD modes ``hlms`` ({(l,m): lal
        COMPLEX16TimeSeries}, all with the same epoch/deltaT).

        Returns (rholms, crossTerms, crossTermsV); see the module header for the contract.
        """
        modes = list(hlms.keys())
        h0 = hlms[modes[0]]
        if abs(float(h0.deltaT) - self.deltaT) > 1e-12 * self.deltaT:
            raise ValueError("template deltaT %g != data deltaT %g" % (h0.deltaT, self.deltaT))
        N_shift, T = int(N_shift), int(N_window)
        harr = {m: np.asarray(hlms[m].data.data, dtype=np.complex128) for m in modes}
        H = self._stack_modes(harr, modes)
        j_first, j_last = self._template_support(H)
        self._check_template_support(H, j_first, N_shift, T)

        # U/V evaluation points on the grid (stride) -- always include both ends.
        tsel = np.arange(0, T, self.u_stride)
        if tsel[-1] != T - 1:
            tsel = np.append(tsel, T - 1)

        U, V, self.uv_routes = self._uv_all(H, N_shift, T, tsel, j_first, j_last)
        crossTerms, crossTermsV = self._complete_pairs(U, V, modes, tsel, T)
        Q = self._q_all(H, N_shift, T)
        rholms = self._rholm_series({m: Q[ia] for ia, m in enumerate(modes)}, hlms, modes,
                                    N_shift, self.deltaT)
        if verbose:
            print("  [td-gs] %s: N_shift=%d N_window=%d template support %d..%d; U/V methods %s; "
                  "U(2,2) centre %.6g, |U| range %.3g..%.3g"
                  % (self.ifo, N_shift, T, j_first, j_last,
                     [r if o is None else "%s(%s)" % (r, o) for (r, o) in self.uv_routes],
                     np.real(crossTerms[(modes[0], modes[0])][T // 2]),
                     np.min(np.abs(crossTerms[(modes[0], modes[0])])),
                     np.max(np.abs(crossTerms[(modes[0], modes[0])]))))
        return rholms, crossTerms, crossTermsV

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
    """TD modes for the td-gs code path: lalsimutils.hlmoft with conditioning off.

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
                                      return_calibration_crossterms=False, **refused):
    """td-gs branch of PrecomputeLikelihoodTerms.  Same return tuple.

    ``refused`` carries the keyword arguments of PrecomputeLikelihoodTerms that this code path
    does not support (calibration, glitch/TD-Toeplitz factory, ROM/NR/gwsignal/external
    EOB generators); any of them set to a non-default value is an error.
    """
    from RIFT.likelihood import factored_likelihood as fl
    _not_supported = {
        'IP_factory': None, 'calibration_realizations': None, 'ROM_group': None,
        'ROM_param': None, 'NR_group': None, 'NR_param': None, 'nr_lookup': False,
        'use_external_EOB': False, 'use_gwsignal': False, 'hybrid_use': False,
        'ROM_use_basis': False, 'use_provided_strain': False,
    }
    ignore_threshold = refused.pop('ignore_threshold', None)
    for key, default in _not_supported.items():
        if key in refused and refused[key] not in (default, 0, False, None):
            raise ValueError("td-gs: %s=%r is not supported on the time-domain code path "
                             "(planned for a later stage)" % (key, refused[key]))

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
    for det in detectors:
        ctx = td_gs_contexts[det]
        data = data_dict[det]
        if abs(float(data.deltaT) - P.deltaT) > 1e-12 * P.deltaT or data.data.length != first_data.data.length:
            raise ValueError("td-gs: detector %s data grid differs from %s" % (det, detectors[0]))
        t_det = fl.ComputeArrivalTimeAtDetector(det, P.phi, P.theta, event_time_geo)
        # Identical bookkeeping to the FD code path (PrecomputeLikelihoodTerms).
        rho_epoch = data.epoch - hlms[list(hlms.keys())[0]].epoch
        t_shift = float(float(t_det) - float(t_window) - float(rho_epoch))
        N_shift = int(t_shift / P.deltaT + 0.5)
        N_window = int(2 * t_window / P.deltaT)
        rholms[det], crossTerms[det], crossTermsV[det] = ctx.precompute(
            hlms, N_shift, N_window, verbose=verbose)
        if ignore_threshold is not None and det == detectors[0]:
            # Mode pruning on the first detector, as the FD code path does with its fiducial
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
        return rholms_intp, crossTerms, crossTermsV, rholms, guess_snr, None, None, None
    return rholms_intp, crossTerms, crossTermsV, rholms, guess_snr, None

