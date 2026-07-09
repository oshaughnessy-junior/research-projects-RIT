"""
td_inner_product.py:  Time-domain (Toeplitz) inner-product adapter for RIFT.

This module is the T1 deliverable of RIFT_TD_INTEGRATION_PLAN.md: a drop-in,
call-compatible replacement for RIFT's frequency-domain-diagonal inner products
(``lsu.ComplexIP`` and ``lsu.ComplexOverlap``) that evaluates the same PSD-weighted
matched-filter algebra as a *time-domain quadratic form* against a Toeplitz operator.
It is the seam behind which the rank-structured covariance augmentation
(``td_rank_augmentation.py``: glitches, lines, waveform uncertainty) and the truncated /
gapped segment likelihoods (plan Stage 3) plug in without touching the Precompute call
sites at ``factored_likelihood.py`` FL:920 (ComplexOverlap -> Q_lm(t)) and FL:1005
(ComplexIP -> scalar U/V).

What "the same algebra" means, exactly (derived and pinned by tests here)
------------------------------------------------------------------------
RIFT's ``lsu.ComplexIP.ip`` computes, for two two-sided COMPLEX16FrequencySeries,

    ip_FD(h1, h2) = 2 deltaF * sum_k conj(h1~_k) h2~_k * weights2side_k          (I)

where ``weights2side`` is the band-limited two-sided inverse PSD (1/S, zeroed outside
[round(fLow/deltaF), round(fMax/deltaF)) exactly as ``lsu.InnerProduct`` builds it).
Transforming h1~, h2~ to the time domain with lal's conventions
(``h_td = lsu.DataInverseFourier(h_f)``, a reverse FFT that multiplies by deltaF), the
*same number* is the time-domain bilinear quadratic form

    ip_TD(h1, h2) = 2 deltaT * conj(h1_td)^T K h2_td                             (II)

against the symmetric-Toeplitz PRECISION operator K whose frequency-domain symbol is the
band-limited weight ``weights`` (i.e. K is the "inverse-ACF" Toeplitz built from
kappa[tau] = iFFT(weights2side)).  The load-bearing normalization constant between (I)
and (II) is

    ip_FD = (2 deltaT) * ip_TD_unnormalized                                      (III)

i.e. **2 deltaT = 2 / fs** (verified bit-exact in test (i)).  Equivalently, in the
real one-sided packing this is the familiar (4 / (N * fs)) matched-filter family factor.

Band-limiting convention (documented per plan T1)
-------------------------------------------------
The weight is 1/S, floored to exactly 0 on bins outside [round(fLow/deltaF),
round(fMax/deltaF)) BEFORE forming the operator -- identical to ``lsu.InnerProduct``.
Consequence: K is a *precision* (inverse-covariance) operator, NOT the inverse of an
invertible covariance.  A band-limited weight corresponds to a covariance with INFINITE
variance out of band (a genuinely rank-deficient C), so one cannot recover this inner
product by building an invertible ACF covariance and calling C^{-1}; the operator must be
built directly from the weight.  This is why ``ToeplitzInnerProductContext`` forms K from
``acf_from_psd`` applied to the *weight symbol*, not from the physical PSD.  (The physical
ACF covariance, needed by the augmentation layer's glitch marginalization, is a separate
object built from S itself in ``td_rank_augmentation``.)

Why the rhoTS construction is done in the frequency domain
----------------------------------------------------------
``ToeplitzComplexOverlap.ip`` must return an rhoTS that is a bit-for-bit drop-in for
``lsu.ComplexOverlap.ip`` (same epoch, same argmax bin, same overall scale) at the
FL:920-929 call site.  Because K's FD symbol is exactly ``weights2side``, the impulse
response rhoTS = iFFT(conj(h~) * FFT(K data)) is identical to iFFT(conj(h~) * weights *
data~), which is precisely ``lsu.ComplexOverlap``'s integrand.  We therefore form that
integrand and reverse-FFT it with the *same lal plan* lsu uses -- guaranteeing exact
agreement of the deltaF factor, FFT direction, packing, and epoch handling rather than
re-deriving them.  (Test (ii) confirms 0.0 relative difference and exact argmax.)

Solver
------
For N <= 2^14 the operator is formed densely (a symmetric Toeplitz matrix).  Beyond that,
the optional ``tdanalysis.forms.pcg.PCGQuadraticForm`` iterative solver (PSD
preconditioner, duck-typed SimpleNamespace parent) is used if tdanalysis is importable;
otherwise a NotImplementedError with a clear message is raised.  tdanalysis is a PRIVATE
optional dependency (never vendored into RIFT); see RIFT_TD_INTEGRATION_PLAN.md Sec. 4.

References: RIFT_TD_INTEGRATION_PLAN.md Sec. 5-6 (T1); GLITCH_MARG_DESIGN.md Sec. 2b
(pinned conventions); ``lsu.InnerProduct`` / ``ComplexIP`` / ``ComplexOverlap``.
"""

__author__ = "R. O'Shaughnessy and the RIFT TD team"

import os

import numpy as np
import scipy.linalg

import lal

from RIFT.likelihood import td_rank_augmentation as tra

# The dense Toeplitz operator is only appropriate for short segments; larger N routes to
# the optional iterative (tdanalysis PCG) path.  Mirrors td_rank_augmentation._MAX_DENSE_N.
_MAX_DENSE_N = 1 << 14

# Frequency tolerance for deltaF comparisons, matching lalsimutils.TOL_DF spirit.
_TOL_DF = 1e-6


class ToeplitzInnerProductContext(object):
    r"""Shared context for the TD Toeplitz inner products (built once per detector).

    Holds the band-limited inverse-PSD weight (identical to ``lsu.InnerProduct``), the
    symmetric-Toeplitz PRECISION operator K it implies, and (for N <= 2^14) its dense
    form for the pure-TD quadratic form.  ``ToeplitzComplexIP`` and
    ``ToeplitzComplexOverlap`` are thin consumers of this object.

    Parameters
    ----------
    psd : callable or ndarray or lal.REAL8FrequencySeries
        The one-sided PSD S(f).  A callable S(f) [Hz^-1] (analytic mode), OR a numpy array
        sampled on the one-sided rfft grid at spacing ``deltaF`` (what ILE holds after
        ``resample_psd_series``), OR a lal.REAL8FrequencySeries.
    deltaF : float
        Frequency resolution [Hz] of the (two-sided) series this context operates on.
    fNyq : float
        Nyquist frequency [Hz]; the two-sided series length is len2side = 2*(fNyq/deltaF).
    fLow : float
        Low-frequency band edge [Hz] (weights zero below round(fLow/deltaF)).
    fMax : float or None
        High-frequency band edge [Hz] (weights zero at/above round(fMax/deltaF)).  If None,
        defaults to fNyq (exactly as lsu.InnerProduct).
    analyticPSD_Q : bool
        True if ``psd`` is a callable to be evaluated at bin frequencies; False if it is an
        array / REAL8FrequencySeries to be indexed.  Mirrors lsu.InnerProduct.
    N : int or None
        Two-sided segment length.  If None it is derived as len2side = 2*(round(fNyq/deltaF)).
    """

    def __init__(self, psd, deltaF, fNyq, fLow, fMax=None,
                 analyticPSD_Q=True, N=None, solver="auto",
                 segment_duration=None, segment_start_index=0,
                 gaps=None, gap_time_origin=None, gap_sample_ranges=None,
                 template_start_offset=None, template_end_offset=None,
                 template_ref_time=None, spd_tol=1e-8):
        self.deltaF = float(deltaF)
        self.fNyq = float(fNyq)
        self.fLow = float(fLow)
        self.fMax = float(fMax) if fMax is not None else float(fNyq)
        if self.fMax > self.fNyq + _TOL_DF:
            raise ValueError("fMax ({0}) must be <= fNyq ({1}).".format(self.fMax, self.fNyq))

        self.len1side = int(self.fNyq / self.deltaF) + 1
        self.len2side = 2 * (self.len1side - 1)
        if N is None:
            N = self.len2side
        self.N = int(N)
        self.deltaT = 1.0 / (2.0 * self.fNyq)
        self.fs = 1.0 / self.deltaT

        self.minIdx = int(round(self.fLow / self.deltaF))
        self.maxIdx = int(round(self.fMax / self.deltaF))

        # ---- band-limited one-sided inverse-PSD weight (exactly as lsu.InnerProduct) ----
        self.weights = self._build_weights(psd, analyticPSD_Q)

        # ---- two-sided packed weight (exactly as lsu.InnerProduct.weights2side) ----
        # W(-N/2 df), ..., W(-df), W(0), W(df), ..., W((N/2-1) df)
        self.weights2side = np.zeros(self.len2side)
        self.weights2side[:len(self.weights)] = self.weights[::-1]
        self.weights2side[len(self.weights) - 1:] = self.weights[:-1]

        # ---- precision operator K (symbol = one-sided weight, mirrored to a real,
        #      even two-sided symbol) => "inverse ACF" kappa[tau] = iFFT(symbol) ----
        # kappa is the first column of the symmetric-Toeplitz precision operator.
        # Reuse acf_from_psd's Wiener-Khinchin machinery by feeding it the weight as if it
        # were a PSD: kappa[tau] = irfft(weight_2side) * (nfft * dF) with nfft = len2side.
        self.kappa = self._weight_to_kappa()

        self._solver = solver
        self._dense = None
        self._pcg = None
        # Whether the dense N x N precision Toeplitz is the chosen apply path.
        #   * solver="dense": always dense (any N), but BUILT LAZILY (see _ensure_dense) so
        #     __init__ never pays the O(N^2) memory / build cost -- important because at
        #     N = 2^14 the dense matrix is ~2 GB and building one per context OOMs when many
        #     are constructed in parallel.
        #   * solver="auto": dense only for SMALL segments (N < _MAX_DENSE_N); at
        #     N >= _MAX_DENSE_N "auto" routes to the FFT (circulant-embedding) Toeplitz matvec,
        #     which is solver-independent for the full-segment inner products and needs no dense
        #     matrix at all.
        #   * solver="pcg": FFT matvec for the precision apply; the optional tdanalysis PCG is
        #     used only for the physical-covariance solve (lazy, see _ensure_pcg).
        self._use_dense = (solver == "dense") or (solver == "auto" and self.N < _MAX_DENSE_N)
        if self._use_dense and self.N < _MAX_DENSE_N:
            # Cheap for small N; build eagerly so callers that read ``ctx._dense`` directly
            # (and the byte-identical numerics the T1/glitch tests pin) see the same operator.
            # The large-N explicit-dense case stays lazy (built on first apply_K).
            self._build_dense()
        # cache of w = K^{-1} d (or K d, precision-operator apply) per data vector id
        self._w_cache = {}

        # ---- Stage-3 (T5) truncated / gapped segment support ----
        # Store the template-frame truncation window (D1: user offsets in seconds relative
        # to the template's peak-amplitude reference; applied to the template streams by the
        # IP wrappers via truncate_timeseries_template_frame).
        self.template_start_offset = (None if template_start_offset is None
                                      else float(template_start_offset))
        self.template_end_offset = (None if template_end_offset is None
                                    else float(template_end_offset))
        self.template_ref_time = (None if template_ref_time is None
                                  else float(template_ref_time))
        self.spd_tol = float(spd_tol)
        self._configure_segment(segment_duration, segment_start_index,
                                gaps, gap_time_origin, gap_sample_ranges)

        # ---- G3 glitch marginalization state (attached lazily; OFF by default) ----
        # When no glitch basis is attached the object is byte-identical to the T1/T5
        # context (all glitch branches are gated on self._glitch_active).
        self._glitch_active = False
        self._glitch_aug = None            # RankAugmentation over the precision operator
        self._glitch_sigma_g = None        # profiled amplitude (per detector)
        self._glitch_delta_lnZ = 0.0       # Occam-disciplined evidence term (Sec. 4)
        self._glitch_data_id = None        # id() of the data series used to build gamma/ghat
        self._glitch_data_corr_fd = None   # cached glitch-subtracted data (FD, for Q')
        self._glitch_ghat_td = None        # MAP glitch reconstruction B M^-1 gamma (TD)
        self._glitch_support = None        # (i0, i1) sample support of the basis (diagnostics)

    # ------------------------------------------------------------------ segment (T5)

    def _configure_segment(self, segment_duration, segment_start_index,
                           gaps, gap_time_origin, gap_sample_ranges):
        r"""Configure the truncated / gapped analysis segment (plan T5, decision D5).

        The truncated-segment precision operator is ``K_seg = Toeplitz(kappa[:N_seg])``
        with ``kappa`` the FULLY-RESOLVED (len2side) inverse transform of the band-limited
        weight, cut to ``N_seg`` (STUDY.md recommendation: build kappa on the long grid,
        then truncate -- never sample 1/S at the segment's own coarse resolution).  Per the
        ratified D5 resolution this is the DEFAULT truncation convention: ``K_seg`` is a
        PRECISION operator, applied directly as a quadratic-form kernel and NEVER factored
        (it is commonly non-SPD after truncation for sharp lines -- see ``is_spd``).

        Gaps (plan Eqs. 11-12, precision-kernel version).  "Deleting the kernel entries"
        for a PRECISION kernel means removing the rows/columns of ``K_seg`` that correspond
        to the excised samples.  The clean, exactly-equivalent implementation used here is a
        binary RETAIN MASK on the segment samples: the data and template streams are zeroed
        on the gap samples, and the FULL ``K_seg`` is applied.  Because the quadratic form
        ``conj(u)^T K_seg v`` only receives contributions from index pairs (i,j) with both
        ``u_i`` and ``v_j`` non-zero, zeroing the gap samples is algebraically identical to
        forming the retained-sample submatrix ``K_seg[R][:,R]`` and applying it to the
        retained samples only (validated to ~1e-10 in the AC test against a brute-force dense
        submatrix construction).  This is what ``dense_retained_operator`` returns.

        Parameters
        ----------
        segment_duration : float or None
            Analysis-segment duration [s]; ``N_seg = round(segment_duration * fs)``.  None
            leaves the full segment (``N_seg = N``); the segment path is then active only if
            gaps or a non-zero start index are supplied.
        segment_start_index : int
            Start sample of the analysis window within the full (len2side) stream.
        gaps : list[(t0, t1)] or None
            Gap intervals in seconds relative to ``gap_time_origin`` (the GPS/geocenter time
            of segment sample 0).  Converted to sample ranges within the segment.
        gap_time_origin : float or None
            GPS/geocenter time [s] of segment sample 0, used to place ``gaps``.  If None, the
            gap times are interpreted as seconds relative to segment sample 0 (origin 0).
        gap_sample_ranges : list[(i0, i1)] or None
            Gap intervals given directly as integer sample ranges within the segment
            (half-open [i0, i1)); a convenience for tests, additive with ``gaps``.
        """
        self.segment_start_index = int(segment_start_index)
        if segment_duration is None:
            self.N_seg = self.N
        else:
            self.N_seg = int(round(float(segment_duration) * self.fs))
        if self.N_seg <= 0 or self.segment_start_index < 0 \
                or self.segment_start_index + self.N_seg > self.len2side:
            raise ValueError(
                "segment window [{0}, {0}+{1}) does not fit inside the full stream of "
                "length {2}".format(self.segment_start_index, self.N_seg, self.len2side))

        # retain mask (True = kept, False = gapped/deleted)
        self.retain_mask = np.ones(self.N_seg, dtype=bool)
        gap_ranges = []
        if gap_sample_ranges:
            gap_ranges.extend((int(i0), int(i1)) for (i0, i1) in gap_sample_ranges)
        if gaps:
            origin = 0.0 if gap_time_origin is None else float(gap_time_origin)
            for (t0, t1) in gaps:
                # seconds (geocenter) -> sample index within the segment
                i0 = int(np.floor((float(t0) - origin) / self.deltaT))
                i1 = int(np.ceil((float(t1) - origin) / self.deltaT))
                gap_ranges.append((i0, i1))
        for (i0, i1) in gap_ranges:
            i0c = max(0, min(self.N_seg, i0))
            i1c = max(0, min(self.N_seg, i1))
            if i1c > i0c:
                self.retain_mask[i0c:i1c] = False
        self._gap_ranges = gap_ranges

        # Is a non-trivial (truncated or gapped) segment active?
        self.segment_active = (
            self.N_seg != self.len2side
            or self.segment_start_index != 0
            or (not self.retain_mask.all())
            or self.template_start_offset is not None
            or self.template_end_offset is not None)

        # Build the (dense) truncated precision operator K_seg and run the D5 SPD check.
        # ONLY for a genuinely sub-length or gapped segment.  A pure template-frame
        # truncation leaves N_seg == len2side with no gaps: ``segment_dot_td`` then applies
        # the FULL precision operator (``_apply_seg`` -> ``apply_K``, the FFT matvec), so
        # materialising the len2side x len2side dense K_seg (~2 GB at 2^14) and running its
        # O(N^3) ``eigvalsh`` SPD check would be wasted work.  Skip it there; ``is_spd`` stays
        # None (an informational diagnostic only -- the full-segment kernel is SPD by
        # construction up to the single dropped Nyquist bin).
        self._Kseg = None
        self.is_spd = None
        self._Kseg_min_eig = None
        needs_dense_Kseg = (self.N_seg != self.len2side) or (not self.retain_mask.all())
        if self.segment_active and needs_dense_Kseg and self.N_seg <= _MAX_DENSE_N:
            self._Kseg = scipy.linalg.toeplitz(self.kappa[:self.N_seg])
            self._spd_check_Kseg()

    def _spd_check_Kseg(self):
        r"""Log (do NOT fail) whether the truncated precision kernel K_seg is SPD (D5).

        The truncated precision kernel is commonly non-SPD (STUDY.md: SPD in only 16/51
        corners for sharp lines) -- this does NOT affect the matched filter / lnL, which
        apply K_seg directly and never factor it.  We record ``is_spd`` and the minimum
        eigenvalue and emit a one-time informational log when non-SPD, so a run leaves a
        breadcrumb.  Any code path that tries to Cholesky-factor K_seg is hard-guarded (see
        ``cholesky``).
        """
        try:
            w = np.linalg.eigvalsh(self._Kseg)
        except np.linalg.LinAlgError:
            self.is_spd = None
            return
        self._Kseg_min_eig = float(w[0])
        scale = float(np.max(np.abs(w))) if w.size else 1.0
        self.is_spd = bool(w[0] > -self.spd_tol * max(scale, 1.0))
        if not self.is_spd:
            import logging
            logging.getLogger(__name__).info(
                "ToeplitzInnerProductContext: truncated precision kernel K_seg (N_seg=%d) is "
                "NOT positive-definite (min eig=%.3e, scale=%.3e).  This is expected under "
                "the D5 precision-kernel convention; K_seg is applied directly as a "
                "quadratic-form kernel and is never factored.", self.N_seg,
                self._Kseg_min_eig, scale)

    def cholesky(self):
        """Anti-factoring guard (plan D5): refuse to Cholesky-factor the precision kernel.

        ``K_seg`` is a PRECISION (inverse-covariance) operator and is commonly non-SPD after
        truncation; factoring it is mathematically wrong (it is not an invertible
        covariance).  Any code path reaching here is a bug -- raise loudly rather than return
        a silently-wrong factor.  If a whitening transform is genuinely needed for a
        diagnostic, use ``|Q diag(|lambda|) Q^T|`` (symmetric eigendecomposition), not this.
        """
        raise RuntimeError(
            "Refusing to Cholesky-factor the truncated precision kernel K_seg: it is a "
            "PRECISION operator (D5), commonly non-SPD after truncation, and must be applied "
            "directly as a quadratic-form kernel -- never factored.  (is_spd=%r, "
            "min_eig=%r)" % (self.is_spd, self._Kseg_min_eig))

    def apply_Kseg(self, v):
        """Return K_seg v for a length-N_seg (real or complex) vector v (dense matvec)."""
        if self._Kseg is None:
            raise NotImplementedError(
                "Truncated-segment operator K_seg is only built densely for N_seg <= {0}; "
                "N_seg={1} would need the iterative path (not wired for the segment kernel)."
                .format(_MAX_DENSE_N, self.N_seg))
        v = np.asarray(v)
        if np.iscomplexobj(v):
            return self._Kseg.dot(v.real) + 1j * self._Kseg.dot(v.imag)
        return self._Kseg.dot(v)

    def _apply_seg(self, v):
        """Apply the segment precision operator, falling back to the full K when untruncated.

        When ``K_seg`` is built (truncated and/or gapped segment) use it.  For a full-length
        segment (N_seg == len2side, e.g. the T6 full-segment diagnostic) fall back to the
        full precision operator ``apply_K`` (dense or FFT matvec) rather than materialising a
        second len2side dense matrix.
        """
        if self._Kseg is not None:
            return self.apply_Kseg(v)
        if self.N_seg == self.len2side:
            return self.apply_K(v)
        raise NotImplementedError(
            "truncated K_seg (N_seg={0}) needs a dense build (N_seg <= {1})".format(
                self.N_seg, _MAX_DENSE_N))

    def _segment_window(self, x_full_td):
        """Extract, and gap-mask, the analysis-segment samples from a full TD stream."""
        seg = np.asarray(x_full_td)[
            self.segment_start_index:self.segment_start_index + self.N_seg].copy()
        if not self.retain_mask.all():
            seg[~self.retain_mask] = 0.0
        return seg

    def segment_dot_td(self, u_full_td, v_full_td):
        r"""UNNORMALIZED conj(u_seg)^T K_seg v_seg on the truncated/gapped segment.

        Segment analogue of ``dot_td``: window both full-length TD streams to the analysis
        segment, zero the gap samples (= delete the corresponding K_seg rows/cols), and
        apply the truncated precision kernel.  Multiply by ``ip_normalization`` for the
        physical inner product (identical (2 deltaT) factor as the full path).
        """
        u_seg = self._segment_window(u_full_td)
        v_seg = self._segment_window(v_full_td)
        return np.tensordot(np.conj(u_seg), self._apply_seg(v_seg), axes=([-1], [-1]))

    def u_at_shift(self, h_td, n_shift):
        r"""Scalar segment U = <shift_{n} h | shift_{n} h> for an integer-sample shift (T6).

        The template TD stream is rolled by ``n_shift`` samples (positive = later) and the
        truncated/gapped-segment quadratic form is evaluated on the FIXED analysis window --
        so sliding the template partly out of the retained window (a GPS-frame cut) shows up
        as a change in U.  Returns the physically-normalized ``ip_normalization * quadform``.
        """
        hs = np.roll(np.asarray(h_td), int(n_shift))
        return self.ip_normalization * self.segment_dot_td(hs, hs)

    def uv_edge_sensitivity(self, h_td, window_half):
        r"""U edge-sensitivity diagnostic over the time-marginalization window (plan T6).

        Evaluate the dominant-mode U at the two extremes of the marginalization window
        (t_ref +- ``window_half``) and at the centre, and return the max relative variation.
        Under the template-frame truncation convention (D1) U(t) is ~shift-invariant, so this
        is ~0 (full segment) or tail-level (template-frame ringdown cut); a GPS-frame-cut
        misconfiguration (segment truncated across the sliding template support) makes it
        O(1).  Cheap: two extra scalar IPs (plus the centre).

        Returns dict with U0, U_minus, U_plus, rel_var, n_shift.
        """
        n = int(round(float(window_half) * self.fs))
        U0 = self.u_at_shift(h_td, 0)
        Um = self.u_at_shift(h_td, -n)
        Up = self.u_at_shift(h_td, +n)
        denom = abs(U0) if abs(U0) > 0 else 1.0
        rel = max(abs(Up - U0), abs(Um - U0)) / denom
        return {"U0": U0, "U_minus": Um, "U_plus": Up, "rel_var": float(rel), "n_shift": n}

    def dense_retained_operator(self):
        """Brute-force retained-sample precision submatrix K_seg[R][:,R] (gap validation).

        Returns ``(Kret, retained_indices)``.  ``conj(u[R])^T Kret v[R]`` equals
        ``segment_dot_td`` on gap-zeroed streams to machine precision -- the AC-test check
        that the retain-mask implementation IS the precision-kernel entry deletion.
        """
        if self._Kseg is None:
            raise NotImplementedError("K_seg not built densely for N_seg={0}".format(self.N_seg))
        R = np.nonzero(self.retain_mask)[0]
        return self._Kseg[np.ix_(R, R)], R

    # ------------------------------------------------------------------ builders

    def _build_weights(self, psd, analyticPSD_Q):
        """Band-limited one-sided 1/S weight, byte-compatible with lsu.InnerProduct.

        Also caches ``self.psd_onesided`` (the one-sided S(f) sampled on the rfft grid) for
        the optional physical-covariance iterative path; off-band bins are set to +inf
        (weight 0) so they do not enter the band-limited operator.
        """
        weights = np.zeros(self.len1side)
        S = np.full(self.len1side, np.inf)
        if analyticPSD_Q:
            for i in range(self.minIdx, self.maxIdx):
                sval = psd(i * self.deltaF)
                S[i] = sval
                weights[i] = 1.0 / sval
        else:
            if isinstance(psd, lal.REAL8FrequencySeries):
                data = psd.data.data
            else:
                data = np.asarray(psd)
            for i in range(self.minIdx, self.maxIdx):
                if i < len(data) and data[i] != 0.0:
                    S[i] = data[i]
                    weights[i] = 1.0 / data[i]
        self.psd_onesided = S
        return weights

    def _weight_to_kappa(self):
        r"""First column of the precision Toeplitz operator, kappa[tau], length len2side.

        The precision operator K has frequency-domain symbol equal to the one-sided
        band-limited ``weights`` (= 1/S on band), viewed as the DFT symbol of a real, even
        two-sided sequence.  Its inverse discrete transform IS that sequence:

            kappa = iFFT(symbol)  =  np.fft.irfft(weights, n = len2side)

        (numpy's irfft treats ``weights`` as the non-negative-frequency half of a real,
        Hermitian-symmetric spectrum and divides by nfft, giving exactly Re[iFFT(mirrored
        symbol)]).  K[i,j] = kappa[|i-j|].  The (2 deltaT) physical normalization that maps
        conj(u)^T K v onto lsu.ComplexIP is applied separately (``ip_normalization``); it is
        NOT folded into kappa, so kappa is purely the operator symbol's inverse transform.
        """
        nfft = self.len2side
        kappa = np.fft.irfft(self.weights, n=nfft)
        return kappa.astype(np.float64)

    def _build_dense(self):
        """Dense symmetric-Toeplitz precision operator K (N x N) for short segments."""
        col = self.kappa[:self.N]
        self._dense = scipy.linalg.toeplitz(col)

    def _ensure_dense(self):
        """Return the dense precision operator (building it lazily), or None for the FFT path.

        Returns ``None`` when the dense path is not selected (solver="auto" at
        N >= _MAX_DENSE_N, or solver="pcg"), in which case ``apply_K`` uses the FFT
        (circulant-embedding) Toeplitz matvec.  When dense IS selected (explicit
        solver="dense", or "auto" at small N) the matrix is materialised on first use so a
        large explicit-dense operator is never built eagerly in ``__init__``.
        """
        if not self._use_dense:
            return None
        if self._dense is None:
            self._build_dense()
        return self._dense

    # ------------------------------------------------------------------ operator apply

    def apply_K(self, v):
        """Return K v, the precision operator applied to a (real or complex) vector v.

        For the dense path this is a matrix-vector product; for the iterative path the
        exact Toeplitz matvec via circulant embedding is used.  Bilinear (no conjugation).
        """
        v = np.asarray(v)
        dense = self._ensure_dense()
        if dense is not None:
            if np.iscomplexobj(v):
                return dense.dot(v.real) + 1j * dense.dot(v.imag)
            return dense.dot(v)
        # circulant-embedding exact Toeplitz matvec (matches the dense result), used for
        # large N where forming the dense matrix is too costly.
        return self._toeplitz_matvec(v)

    def _toeplitz_matvec(self, v):
        """Exact symmetric-Toeplitz matvec K v via zero-padded circular convolution.

        Accepts a vector (N,) or a MATRIX (N, r) of stacked column vectors (the
        rank-augmentation layer solves Phi = C^{-1} B column-block-wise; the FFTs
        vectorize over the trailing axis at identical numerics to the per-column loop)."""
        N = self.N
        col = self.kappa[:N]
        # embed the symmetric Toeplitz into a circulant of length >= 2N-1
        m = 2 * N - 1
        c = np.zeros(m)
        c[:N] = col
        c[N:] = col[1:][::-1]        # symmetric first row = first column for symmetric T
        fc = np.fft.fft(c)

        def _mv(x):
            # x: (N,) or (N, r); pad + FFT along axis 0
            shape = (m,) + x.shape[1:]
            xp = np.zeros(shape, dtype=x.dtype)
            xp[:N] = x
            fx = np.fft.fft(xp, axis=0)
            y = np.fft.ifft(fc.reshape((m,) + (1,) * (x.ndim - 1)) * fx, axis=0)[:N]
            return y

        v = np.asarray(v)
        if np.iscomplexobj(v):
            return (_mv(v.real) + 1j * _mv(v.imag))
        return np.real(_mv(v.astype(float)))

    def dot_td(self, u_td, v_td):
        r"""Return the UNNORMALIZED bilinear TD quadratic form conj(u)^T K v.

        NOTE bilinear + Hermitian usage: this applies conj to the FIRST argument (matching
        lsu.ComplexIP's ``conj(h1) h2`` convention).  Callers who want a pure bilinear form
        can pass already-conjugated inputs.  The physical inner product is obtained by
        multiplying by the (2 deltaT) normalization -- see ``ip_normalization``.
        """
        Kv = self.apply_K(v_td)
        return np.tensordot(np.conj(np.asarray(u_td)), Kv, axes=([-1], [-1]))

    @property
    def ip_normalization(self):
        """The scalar (2 deltaT) mapping the TD quadratic form onto lsu.ComplexIP (eq. III)."""
        return 2.0 * self.deltaT

    def apply_template_truncation(self, x_td, epoch):
        """Apply the stored template-frame truncation window to a template TD array (T5).

        No-op if no template window is configured.  Uses ``self.template_ref_time`` as the
        common peak reference if the factory has set one (so every mode shares one t_ref);
        otherwise the per-array argmax is used (correct for a single dominant mode).
        """
        if self.template_start_offset is None and self.template_end_offset is None:
            return x_td
        return truncate_timeseries_template_frame(
            x_td, self.deltaT, epoch,
            self.template_start_offset, self.template_end_offset,
            t_ref=self.template_ref_time)

    # ------------------------------------------------------------------ physical covariance

    def covariance_acf(self, acf_duration=None):
        """Physical noise autocovariance C from the band-limited PSD (for whitening / T3).

        This is the physical covariance object (symbol S, NOT the precision symbol 1/S) that
        the augmentation layer (td_rank_augmentation) and the whitening seam consume.  Built
        via ``tra.acf_from_psd`` on the cached one-sided S; off-band bins (S = inf) are
        floored so C stays well-defined.
        """
        if acf_duration is None:
            acf_duration = self.N / self.fs
        f = np.arange(self.len1side) * self.deltaF
        S = np.array(self.psd_onesided, dtype=np.float64)
        finite = np.isfinite(S) & (S > 0)
        if not finite.any():
            raise ValueError("PSD has no finite in-band samples.")
        # Off-band: use a large-but-finite value so C is invertible (its in-band structure
        # is what matters; off-band variance is arbitrary here).
        S_fill = np.where(finite, S, np.max(S[finite]) * 1e6)
        return tra.acf_from_psd(f, S_fill, self.fs, acf_duration=acf_duration)

    # ------------------------------------------------------------------ glitch marginalization (G3)

    @property
    def glitch_active(self):
        """True iff a rank-augmentation (glitch) basis has been attached to this context."""
        return self._glitch_active

    def attach_glitch_basis(self, blocks, data_fd, sigma_g="profile"):
        r"""Attach a per-detector rank-augmentation (glitch) basis and profile sigma_g (G3).

        THE ALGEBRA (K-vs-C^{-1} Woodbury placement -- the crux of G3).
        ---------------------------------------------------------------
        The base TD matched filter uses the PRECISION operator ``P = ip_normalization * K``
        (``K`` = band-limited inverse-PSD Toeplitz kernel).  ``P`` IS the noise precision
        ``C^{-1}`` in matched-filter normalization: every inner product is
        ``<u|v> = conj(u)^T P v``.  So the standard covariance-side Woodbury of the design
        (Phi = C^{-1} B via r solves) becomes here a DIRECT MATVEC, no linear solve:

            Phi   = C^{-1} B = P B            (apply the precision to each basis column)
            G     = B^T Phi = B^T P B
            M     = Lambda^{-1} + G           (Lambda = sigma_g^2 Lambda_0)
            gamma = Phi^T d = B^T P d
            C'^{-1} = P - Phi M^{-1} Phi^T    (Woodbury; NEVER inverts P)

        This is exactly ``RankAugmentation`` fed an object whose ``.solve(x)`` returns
        ``P x`` (the precision apply) rather than a covariance solve -- see
        ``_PrecisionAsCovariance``.  ``M`` inverts ``Lambda^{-1} + B^T P B`` (a well-posed
        r x r system floored by the prior), so the augmentation is well-defined even though
        ``P`` itself is rank-deficient off-band (the always-dropped Nyquist bin + the
        band-limit): the glitch basis is band-limited into [f1,f2], living where ``P`` is
        well-conditioned.  ``C`` = "inverse of P on the band" is never formed.

        THE TWO CORRECTED INGREDIENTS.
          * U'_{lm,l'm'} = conj(h_lm)^T C'^{-1} h_l'm'
                         = U - conj(Phi^T h_lm)^T M^{-1} (Phi^T h_l'm')      (scalar, per pair)
            -- the base Hermitian ip minus a rank-r correction (``glitch_ip_correction``).
          * Q'_lm(t) = correlate(h_lm, w'),  w' = C'^{-1} d = P (d - B M^{-1} gamma)
                     = the base overlap machinery run on the GLITCH-SUBTRACTED data
                       d_corr = d - g_hat,  g_hat = B M^{-1} gamma  (the MAP glitch).
            Because the overlap is linear in the data, Q'(t) = overlap(h, d_corr); we
            precompute d_corr once per detector (``glitch_corrected_data_fd``).

        THE EVIDENCE CONSTANT (does NOT cancel; surfaced for ILE).
            Delta lnZ(sigma_g) = 1/2 gamma^H M^{-1} gamma - 1/2 ln det(I + sigma_g^2 Lambda_0 G)
        A per-detector data constant (independent of intrinsic/extrinsic params); ILE adds
        sum_det Delta lnZ to lnL_marg (Sec. 4).

        CONJUGATION.  ``RankAugmentation`` is BILINEAR; RIFT ips are HERMITIAN.  The Hermitian
        corrected ip is obtained by passing ``conj`` on the first slot -- validated against a
        dense conj-explicit reference in ``test_td_glitch.py`` (the bilinear-vs-Hermitian trap).

        Parameters
        ----------
        blocks : list of (B, Lambda0[, label])
            Rank-augmentation blocks (N x r basis + dimensionless shape prior), as consumed by
            ``td_rank_augmentation.RankAugmentation``.  B must have N = ``self.N`` rows.
        data_fd : lal.COMPLEX16FrequencySeries
            The detector data (same object the overlap step will receive); used to profile
            sigma_g, form gamma / g_hat / Delta lnZ.
        sigma_g : "profile" or float
            "profile" (default, the only wired mode) maximizes Delta lnZ over sigma_g on d
            alone (Sec. 4, empirical Bayes).  A float pins sigma_g.  Grid marginalization is
            OUT OF SCOPE for G3 (follow-on: precompute {d_corr, M} per grid value and
            logsumexp inside ILE).

        NOTE (scope): glitch marginalization is implemented for the FULL segment only.  On a
        truncated/gapped segment (``segment_active``) the base operator is the truncated
        precision kernel K_seg and Phi = K_seg B must use ``apply_Kseg`` on the retained
        subspace; that extension is documented but deferred (raises here).
        """
        if self.segment_active:
            raise NotImplementedError(
                "Glitch marginalization is wired for the FULL segment only.  On a truncated/"
                "gapped segment the Woodbury base operator is the truncated precision kernel "
                "K_seg (Phi = K_seg B on the retained subspace); that extension is deferred "
                "(see attach_glitch_basis docstring / REPORT.md).")
        from RIFT.likelihood import td_rank_augmentation as tra
        prec = _PrecisionAsCovariance(self)
        self._glitch_aug = tra.RankAugmentation(prec, blocks)
        # record the basis support (union over blocks) for the overlap diagnostic
        i0s, i1s = [], []
        for blk in self._glitch_aug.blocks:
            nz = np.nonzero(np.any(np.abs(blk["B"]) > 0, axis=1))[0]
            if nz.size:
                i0s.append(int(nz[0])); i1s.append(int(nz[-1]) + 1)
        self._glitch_support = (min(i0s), max(i1s)) if i0s else None

        d_td = _ifft_to_td(data_fd)
        if sigma_g == "profile":
            sig, dlnZ = self._glitch_aug.profile_sigma_g(d_td)
        else:
            sig = float(sigma_g)
            dlnZ = self._glitch_aug.delta_lnZ(d_td, sig)
        self._glitch_sigma_g = sig
        self._glitch_delta_lnZ = float(dlnZ)

        # MAP glitch reconstruction g_hat_total = B(mu_a + M^{-1} gamma_tilde) (per block,
        # summed) -- the prior-predicted mean piece B mu_a = sigma_g B mu0 PLUS the data-driven
        # posterior update on the mean-shifted residual (paper-2 Sec. 3.1; reduces to the
        # paper-1 B M^{-1} gamma when the population mean mu0 = 0).  The glitch-subtracted data
        # d_corr = d - g_hat_total -> corrected filter w' = P d_corr, so Q'_lm(t) =
        # overlap(h, d - g_hat_total) holds with the mean-predicted piece included.
        ghat = self._glitch_aug.glitch_reconstruction(d_td, sig).astype(complex)
        self._glitch_ghat_td = ghat
        d_corr_td = d_td - ghat
        self._glitch_data_corr_fd = self._fd_from_td(d_corr_td, data_fd)
        self._glitch_data_id = id(data_fd)
        self._glitch_active = True
        return dict(sigma_g=sig, delta_lnZ=self._glitch_delta_lnZ,
                    support=self._glitch_support)

    def _fd_from_td(self, td_array, template_fd):
        """Forward-FFT a TD numpy array into a COMPLEX16FrequencySeries matching template_fd.

        Uses ``lalsimutils.DataFourier`` (the exact inverse of the ``DataInverseFourier`` used
        by ``_ifft_to_td``), so ``_fd_from_td(_ifft_to_td(x), x)`` round-trips to x's packing
        (verified in test_td_glitch).
        """
        from RIFT import lalsimutils as lsu
        ts = lal.CreateCOMPLEX16TimeSeries(
            "gc", template_fd.epoch, 0., self.deltaT, _dimensionless_unit(), self.len2side)
        ts.data.data = np.asarray(td_array, dtype=complex)
        return lsu.DataFourier(ts)

    def glitch_ip_correction(self, u_td, v_td):
        r"""Hermitian rank-r correction subtracted from U/V to form U'/V' (G3).

            correction = sum_k conj(Phi_k^T u)^T M_k^{-1} (Phi_k^T v)

        so that ``U'(u,v) = ip_normalization*dot_td(u,v) - glitch_ip_correction(u,v)`` equals
        ``conj(u)^T C'^{-1} v``.  ``Phi_k = P B_k`` already carries the ``ip_normalization``
        (2 deltaT) factor (it is ``cov.solve`` on the precision-as-covariance adapter), so the
        correction is in the same physical units as the base ip -- no extra normalization.
        The explicit ``conj`` on ``Phi^T u`` is the Hermitian-vs-bilinear reconciliation.
        """
        if not self._glitch_active:
            return 0.0
        aug = self._glitch_aug
        sig = self._glitch_sigma_g
        u_td = np.asarray(u_td); v_td = np.asarray(v_td)
        total = 0.0 + 0.0j
        for k, blk in enumerate(aug.blocks):
            Phi = blk["Phi"]
            pu = Phi.T.dot(u_td)                       # Phi^T u  (bilinear r-vector)
            pv = Phi.T.dot(v_td)                       # Phi^T v
            Minv_pv = aug._Minv_apply(pv, sig, k)
            total += np.tensordot(np.conj(pu), Minv_pv, axes=([-1], [-1]))
        return total

    def glitch_corrected_data_fd(self, data_fd):
        """Return the cached glitch-subtracted data FD series (for Q'); see attach_glitch_basis.

        Asserts the passed data is the same object the basis was attached with (the profiled
        sigma_g, gamma and g_hat are specific to that data).  If a different data object is
        passed the correction is recomputed on it (keeps the method correct if the caller
        rebuilds the series), which is rare on the ILE path.
        """
        if not self._glitch_active:
            return data_fd
        if id(data_fd) == self._glitch_data_id and self._glitch_data_corr_fd is not None:
            return self._glitch_data_corr_fd
        # Different data object: recompute (data-conditional gamma / g_hat, mean-inclusive).
        d_td = _ifft_to_td(data_fd)
        ghat = self._glitch_aug.glitch_reconstruction(d_td, self._glitch_sigma_g).astype(complex)
        return self._fd_from_td(d_td - ghat, data_fd)

    @property
    def glitch_delta_lnZ(self):
        """Per-detector Occam-disciplined glitch evidence term (0 if no basis attached)."""
        return self._glitch_delta_lnZ

    def glitch_reconstruction_td(self):
        """MAP glitch reconstruction g_hat = B M^-1 gamma (TD, diagnostic); None if inactive."""
        return self._glitch_ghat_td

    # ------------------------------------------------------------------ optional iterative

    def _ensure_pcg(self, covariance_acf=None):
        """Lazily build the optional tdanalysis PCG solver for the physical COVARIANCE.

        The iterative path solves against the physical ACF covariance C (not the precision
        operator K), for the large-N whitening / augmentation seam.  If ``covariance_acf``
        is None it is built from the cached PSD via ``covariance_acf()``.  Raises
        NotImplementedError with an actionable message if tdanalysis is unavailable.
        """
        if self._pcg is not None:
            return self._pcg
        try:
            from tdanalysis.forms.pcg import PCGQuadraticForm  # optional private dependency
        except Exception as exc:
            raise NotImplementedError(
                "Iterative Toeplitz solve (N = {0}) requires the optional private "
                "dependency 'tdanalysis' (tdanalysis.forms.pcg.PCGQuadraticForm), which is "
                "not importable in this environment ({2!r}).  Point PYTHONPATH at a "
                "tdanalysis checkout, or use a segment with N <= {1} for the dense solver."
                .format(self.N, _MAX_DENSE_N, exc))
        from types import SimpleNamespace
        if covariance_acf is None:
            covariance_acf = self.covariance_acf()
        n = self.N
        Cij = scipy.linalg.toeplitz(np.asarray(covariance_acf)[:n]).astype(np.float64)
        S = np.array(self.psd_onesided, dtype=np.float64)
        S = np.where(np.isfinite(S) & (S > 0), S, np.max(S[np.isfinite(S)]) * 1e6)
        # duck-typed parent per tdanalysis' own tests (test_whitening_backends.py):
        parent = SimpleNamespace(
            Cij=Cij,
            n_samples_l=n,
            data_dtype=np.float64,
            sampling_rate=self.fs,
            cfft_use_fast_len=True,
            cg_use_pc=True,
            cg_warm_start=True,
            cg_eps=1e-10,
            cg_maxiter=5000,
            psd_frequencies=np.arange(self.len1side) * self.deltaF,
            psd=S,
        )
        self._pcg = PCGQuadraticForm(parent=parent, preconditioner_kind="psd",
                                     tol=1e-10, maxiter=5000)
        self._pcg.prepare()
        return self._pcg

    def solve_covariance(self, d, method="auto"):
        """Return C^{-1} d for the physical covariance C (dense or tdanalysis PCG).

        method="dense" uses a Cholesky factor (N <= 2^14); method="pcg" uses the optional
        tdanalysis iterative solver; "auto" picks dense for small N, PCG otherwise.
        """
        d = np.asarray(d, dtype=np.float64)
        if method == "auto":
            method = "dense" if self.N <= _MAX_DENSE_N else "pcg"
        if method == "dense":
            acf = self.covariance_acf()
            cov = tra.ToeplitzCovariance(acf, self.N)
            return cov.solve(d)
        elif method == "pcg":
            pcg = self._ensure_pcg()
            return np.array(pcg.apply_Cinv(d), copy=True)
        raise ValueError("method must be 'auto', 'dense', or 'pcg'.")

    # ------------------------------------------------------------------ checks

    def _check_series(self, hf):
        assert hf.data.length == self.len2side, (
            "series length {0} != context len2side {1}".format(hf.data.length, self.len2side))
        assert abs(hf.deltaF - self.deltaF) <= _TOL_DF, (
            "series deltaF {0} != context deltaF {1}".format(hf.deltaF, self.deltaF))


class _PrecisionAsCovariance(object):
    r"""Adapter presenting a ToeplitzInnerProductContext's PRECISION as a covariance-solve.

    ``RankAugmentation`` (td_rank_augmentation) is written against a covariance object whose
    ``.solve(x)`` returns ``C^{-1} x``.  In the RIFT TD path the base operator we hold IS the
    precision ``P = ip_normalization * K`` (``K`` = band-limited inverse-PSD kernel), i.e.
    ``C^{-1} = P``.  So this adapter's ``.solve(x)`` is a DIRECT precision matvec ``P x`` (no
    linear solve), which makes ``RankAugmentation`` compute Phi = C^{-1} B = P B, G = B^T P B,
    gamma = B^T P d, and the Woodbury correction exactly as the K-side algebra of
    ``attach_glitch_basis`` requires -- reusing the validated G1 code unchanged.

    ``.dot(u, v)`` is the BILINEAR form ``u^T P v`` (no conjugation; ``RankAugmentation`` is
    bilinear -- Hermitian forms pass conj explicitly).  ``.C`` exposes only ``C[0,0]`` (the
    physical noise variance from the ACF) for the default sigma_g bounds; it is a 1x1 array,
    never the full covariance (which is not formed).
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.N = ctx.N
        # C[0,0] = physical noise variance (ACF at lag 0) -> default sigma_g scale (Sec. 2b).
        try:
            c00 = float(ctx.covariance_acf(acf_duration=1.0 / ctx.fs)[0])
        except Exception:
            c00 = 1.0
        self.C = np.full((1, 1), c00, dtype=float)

    def solve(self, b):
        """Return C^{-1} b = P b (precision matvec; no solve).  Real or complex, matrix ok."""
        return self.ctx.ip_normalization * self.ctx.apply_K(b)

    def dot(self, u, v):
        """Bilinear u^T C^{-1} v = u^T P v (NO conjugation; Sec. 2b)."""
        Pv = self.solve(v)
        return np.tensordot(np.asarray(u), Pv, axes=([-1], [-1]))

    def logdet(self):
        # Not used by the glitch path (Delta lnZ uses only the logdet CORRECTION); the
        # precision determinant is off-band-singular and deliberately never formed.
        raise NotImplementedError("precision logdet is off-band-singular and not formed")


class ToeplitzComplexIP(object):
    r"""Scalar TD-Toeplitz analogue of ``lsu.ComplexIP`` (no time/phase maximization).

    ``ip(h1, h2)`` accepts the same two-sided ``lal.COMPLEX16FrequencySeries`` objects
    ``lsu.ComplexIP.ip`` accepts (length ``len2side``, spacing ``deltaF``), transforms them
    to the time domain internally, and returns the bilinear PSD-weighted inner product
    ``2 deltaF sum conj(h1~) h2~ / S`` -- computed as ``2 deltaT conj(h1_td)^T K h2_td``.
    The two agree to machine precision on smooth PSDs / long segments (test (i)).

    Epoch handling mirrors ``lsu.ComplexIP.ip`` exactly: NO epoch phase is applied by
    default; passing ``include_epoch_differences=True`` applies the same
    exp(-i (epoch1 - epoch2) f 2 pi) factor lsu applies (kept in the FD integrand so it is
    byte-identical).
    """

    def __init__(self, context):
        self.ctx = context

    def ip(self, h1, h2, include_epoch_differences=False):
        ctx = self.ctx
        ctx._check_series(h1)
        ctx._check_series(h2)
        assert abs(h1.deltaF - h2.deltaF) <= _TOL_DF

        if include_epoch_differences:
            # Mirror lsu.ComplexIP: multiply h2's spectrum by the epoch phase in FD, then
            # transform.  Uses lalsimutils.evaluate_fvals for the (reversed) lal f grid.
            from RIFT import lalsimutils as lsu
            fvals = lsu.evaluate_fvals(h1)
            phase = np.exp(-1j * (float(h1.epoch) - float(h2.epoch)) * fvals * 2 * np.pi)
            h1_data = np.asarray(h1.data.data)
            h2_data = np.asarray(h2.data.data) * phase
            # Do the weighted sum directly in FD (bit-identical to lsu in this branch).
            val = np.sum(np.conj(h1_data) * h2_data * ctx.weights2side)
            return val * 2.0 * ctx.deltaF

        # Default (no epoch phase): TD quadratic form.
        h1_td = _ifft_to_td(h1)
        h2_td = _ifft_to_td(h2)
        if ctx.segment_active:
            # Stage-3 (T5): both arguments are templates for U/V -> apply the template-frame
            # truncation to each, then the truncated/gapped precision quadratic form.
            h1_td = ctx.apply_template_truncation(h1_td, float(h1.epoch))
            h2_td = ctx.apply_template_truncation(h2_td, float(h2.epoch))
            return ctx.ip_normalization * ctx.segment_dot_td(h1_td, h2_td)
        val = ctx.ip_normalization * ctx.dot_td(h1_td, h2_td)
        if ctx.glitch_active:
            # G3: U'/V' = U/V - conj(Phi^T h1)^T M^{-1} (Phi^T h2)  (Hermitian correction).
            val = val - ctx.glitch_ip_correction(h1_td, h2_td)
        return val

    def norm(self, h):
        """sqrt(|ip(h, h)|), matching lsu.ComplexIP.norm (2 deltaF sum |h~|^2 / S)."""
        val = self.ip(h, h)
        return np.sqrt(np.abs(val))


class ToeplitzComplexOverlap(object):
    r"""TD-Toeplitz analogue of ``lsu.ComplexOverlap`` (time/phase-maximized matched filter).

    ``ip(hlm, data)`` returns ``(rho, rhoTS, rhoIdx, rhoPhase)`` exactly like
    ``lsu.ComplexOverlap.ip`` with ``full_output=True``:

      * rhoTS : a ``lal.COMPLEX16TimeSeries`` of the FULL two-sided segment length at
                deltaT, the impulse response rhoTS(t) = iFFT(conj(hlm~) * FFT(K data))
                == iFFT(conj(hlm~) * weights * data~), built with the SAME lal reverse-FFT
                plan (the 2 deltaF factor, FFT direction, epoch conventions) as
                ``lsu.ComplexOverlap``.  Bit-identical drop-in for FL:920-929.
      * rho     = max |rhoTS|
      * rhoIdx  = argmax |rhoTS|
      * rhoPhase = angle(rhoTS[rhoIdx])
    """

    def __init__(self, context):
        self.ctx = context
        self._revplan = lal.CreateReverseCOMPLEX16FFTPlan(self.ctx.len2side, 0)
        self._fwdplan = None    # built lazily for the segment path only
        self._intgd = lal.CreateCOMPLEX16FrequencySeries(
            "SNR integrand", lal.LIGOTimeGPS(0.), 0., self.ctx.deltaF,
            _hertz_unit(), self.ctx.len2side)
        self._ovlp = lal.CreateCOMPLEX16TimeSeries(
            "Complex overlap", lal.LIGOTimeGPS(0.), 0., self.ctx.deltaT,
            _dimensionless_unit(), self.ctx.len2side)

    def ip(self, hlm, data, **kwargs):
        ctx = self.ctx
        ctx._check_series(hlm)
        ctx._check_series(data)
        assert abs(hlm.deltaF - data.deltaF) <= _TOL_DF

        if ctx.segment_active:
            return self._ip_segment(hlm, data)

        # G3: Q'_lm(t) = correlate(h_lm, w'), w' = C'^{-1} d = P (d - g_hat).  Because the
        # overlap is linear in the data, this is the base machinery run on the glitch-
        # subtracted data d_corr = d - g_hat (g_hat = B M^{-1} gamma).  Default (no glitch)
        # returns `data` unchanged -> byte-identical.
        data_use = ctx.glitch_corrected_data_fd(data) if ctx.glitch_active else data

        # Integrand exactly as lsu.ComplexOverlap: 2 conj(hlm~) data~ weights2side.
        # Because K's FD symbol == weights2side, this is 2 conj(hlm~) (K data)~, i.e. the
        # impulse-response construction rhoTS = iFFT(conj(hlm~) FFT(K data)) up to the
        # identical lal reverse-FFT scaling.
        self._intgd.data.data = (2.0 * np.conj(np.asarray(hlm.data.data))
                                 * np.asarray(data_use.data.data) * ctx.weights2side)
        lal.COMPLEX16FreqTimeFFT(self._ovlp, self._intgd, self._revplan)

        rhoSeries = np.abs(self._ovlp.data.data)
        rho = rhoSeries.max()
        rhoIdx = int(rhoSeries.argmax())
        rhoPhase = float(np.angle(self._ovlp.data.data[rhoIdx]))
        # Copy to a fresh TimeSeries (do not hand back the reused internal buffer),
        # matching lsu.ComplexOverlap.
        rhoTS = lal.CreateCOMPLEX16TimeSeries(
            "Complex overlap", lal.LIGOTimeGPS(0.), 0., ctx.deltaT,
            _dimensionless_unit(), ctx.len2side)
        rhoTS.data.data[:] = self._ovlp.data.data[:]
        return rho, rhoTS, rhoIdx, rhoPhase

    def _ip_segment(self, hlm, data):
        r"""Matched-filter Q(t) on the truncated/gapped segment (plan T5, D1).

        Q(t) = 2 deltaT * conj(shift_t h_trunc)^T K_seg (mask . d_seg), for all time shifts
        at once.  Construction: the data is windowed to the analysis segment and gap-masked,
        the truncated precision kernel is applied ONCE (``Kd_seg = K_seg d_seg``), and the
        result is embedded back into a full-length TD stream; then the impulse response
        rhoTS = reverseFFT( conj(h~) * forwardFFT(Kd_full) ) reproduces the same lal scaling
        / epoch contract as the full path (the (2 deltaT) factor lives in the forward FFT
        that produced ``data~`` originally, so it is applied here via the forward FFT of
        ``Kd_full``).  h_trunc is the template after the template-frame cut; it has compact
        support that (under the D1 convention + T6 diagnostic) stays inside the analysis
        window across the time-marginalization shifts, so windowing h at each shift is a
        no-op and this FFT correlation is exact.
        """
        ctx = self.ctx
        if self._fwdplan is None:
            self._fwdplan = lal.CreateForwardCOMPLEX16FFTPlan(ctx.len2side, 0)

        hlm_td = ctx.apply_template_truncation(_ifft_to_td(hlm), float(hlm.epoch))
        data_td = _ifft_to_td(data)
        data_seg = ctx._segment_window(data_td)         # windowed + gap-masked
        Kd_seg = ctx._apply_seg(data_seg)               # length N_seg
        Kd_full = np.zeros(ctx.len2side, dtype=complex)
        Kd_full[ctx.segment_start_index:ctx.segment_start_index + ctx.N_seg] = Kd_seg

        # forward-FFT Kd_full into lal FD packing (multiplies by deltaT, as data~ was)
        kd_ts = lal.CreateCOMPLEX16TimeSeries(
            "Kd", data.epoch, 0., ctx.deltaT, _dimensionless_unit(), ctx.len2side)
        kd_ts.data.data = Kd_full
        kd_f = lal.CreateCOMPLEX16FrequencySeries(
            "Kd_f", data.epoch, 0., ctx.deltaF, _hertz_unit(), ctx.len2side)
        lal.COMPLEX16TimeFreqFFT(kd_f, kd_ts, self._fwdplan)

        # hlm_td back to FD (same forward plan) to form conj(h~) * kd_f
        h_ts = lal.CreateCOMPLEX16TimeSeries(
            "h", hlm.epoch, 0., ctx.deltaT, _dimensionless_unit(), ctx.len2side)
        h_ts.data.data = hlm_td
        h_f = lal.CreateCOMPLEX16FrequencySeries(
            "h_f", hlm.epoch, 0., ctx.deltaF, _hertz_unit(), ctx.len2side)
        lal.COMPLEX16TimeFreqFFT(h_f, h_ts, self._fwdplan)

        self._intgd.data.data = 2.0 * np.conj(np.asarray(h_f.data.data)) \
            * np.asarray(kd_f.data.data)
        lal.COMPLEX16FreqTimeFFT(self._ovlp, self._intgd, self._revplan)

        rhoSeries = np.abs(self._ovlp.data.data)
        rho = rhoSeries.max()
        rhoIdx = int(rhoSeries.argmax())
        rhoPhase = float(np.angle(self._ovlp.data.data[rhoIdx]))
        rhoTS = lal.CreateCOMPLEX16TimeSeries(
            "Complex overlap", lal.LIGOTimeGPS(0.), 0., ctx.deltaT,
            _dimensionless_unit(), ctx.len2side)
        rhoTS.data.data[:] = self._ovlp.data.data[:]
        return rho, rhoTS, rhoIdx, rhoPhase

    def norm(self, h):
        """sqrt(|2 deltaF sum |h~|^2 / S|), matching lsu.ComplexOverlap.norm."""
        self.ctx._check_series(h)
        val = np.sum(np.conj(np.asarray(h.data.data)) * np.asarray(h.data.data)
                     * self.ctx.weights2side)
        return np.sqrt(2.0 * self.ctx.deltaF * np.abs(val))


class TDInnerProductFactory(object):
    r"""Factory of TD-Toeplitz inner products for the Precompute seam (task T3).

    This is the object threaded through ``factored_likelihood.PrecomputeLikelihoodTerms``
    as the ``IP_factory`` kwarg (default None => the classic frequency-domain-diagonal
    lsu inner products, byte-identical).  Passing a ``TDInnerProductFactory`` instead swaps
    in ``ToeplitzComplexOverlap`` (for Q_lm(t)) and ``ToeplitzComplexIP`` (for scalar U/V)
    at the two FL call sites (FL:453/456 via ``ip_scalar``, FL:463 via ``overlap``) WITHOUT
    any other change to the Precompute path.

    It exposes exactly the two constructor-shaped methods FL needs:

      * ``ip_scalar(fmin, fMax, fNyq, deltaF, psd, analyticPSD_Q, inv_spec_trunc_Q,
        T_spec)`` -> a ``ToeplitzComplexIP`` (drop-in for ``lsu.ComplexIP``).
      * ``overlap(fmin, fMax, fNyq, deltaF, psd, analyticPSD_Q, inv_spec_trunc_Q, T_spec,
        full_output=...)`` -> a ``ToeplitzComplexOverlap`` (drop-in for
        ``lsu.ComplexOverlap``).

    The signatures mirror the lsu constructors the FL call sites use, so FL can call the
    factory with the identical positional/keyword arguments it would have handed to lsu.

    One ``ToeplitzInnerProductContext`` is held per DISTINCT detector configuration.  The
    intended usage is "one factory per detector" (build it next to the ILE PSD handling),
    but the factory also memoizes contexts keyed on
    ``(fmin, fMax, fNyq, deltaF, analyticPSD_Q, id(psd), solver)`` so repeated FL calls
    with the same detector configuration reuse the same operator (the cross-term and
    Q calls for a given detector share one context, built once).

    Parameters
    ----------
    solver : {"auto", "dense", "pcg"}
        Solver mode forwarded to ``ToeplitzInnerProductContext``.  Note this affects only
        the physical-covariance / augmentation path (``solve_covariance``); the full-segment
        precision-kernel inner products (this seam) are FFT-based and solver-independent.
        Corresponds to the ILE ``--td-solver`` flag ({auto,dense,pcg}; "gs" is not wired in
        this adapter).
    acf_duration : float or None
        Optional ACF truncation duration [s] carried for the Stage-3 truncated-segment
        path; unused by the full-segment precision-kernel inner products (D5 DEFAULT: the
        full-segment path uses K = irfft(band-limited weights), not the ACF-covariance
        route).  Corresponds to the ILE ``--td-acf-duration`` flag.
    warn_inv_spec_trunc : bool
        If True (default), emit a one-time warning when a call site requests inverse-spectrum
        truncation (``inv_spec_trunc_Q=True``): the TD precision kernel K = irfft(1/S on band)
        IS the exact band-limited inverse-spectrum operator, so the approximate FD
        inverse-spectrum truncation is redundant here and is ignored (see D5 and the note in
        ``_resolve_inv_spec_trunc``).
    """

    def __init__(self, solver="auto", acf_duration=None, warn_inv_spec_trunc=True,
                 template_start_offset=None, template_end_offset=None,
                 gaps=None, segment_start_index=0, gap_time_origin=None,
                 glitch_specs=None, glitch_sigma="profile", glitch_basis_files=None,
                 window_shape=None):
        self.solver = solver
        # Data Tukey window shape the surrounding pipeline applied to the data + PSD BEFORE
        # Precompute (ILE --window-shape, set in production by helper_LDG_Events from
        # --data-tukey-window-time; T1900249 <w^2> PSD factor auto-applied).  The TD path is
        # DOWNSTREAM of windowing: it receives the already-windowed data stream and the
        # already-rescaled PSD, exactly as the fd-diagonal path does, so no windowing lives in
        # this kernel.  Stored only for the one-time provenance banner in context_for (below):
        # window_shape==0/None means the data is un-windowed (rectangular) -- correct for
        # zero-noise equivalence tests, but a foot-gun on real data (see plan T7c: the TD path
        # MUST inherit the production window, not bypass it).
        self.window_shape = window_shape
        self._logged_window = False
        self.acf_duration = acf_duration
        self.warn_inv_spec_trunc = warn_inv_spec_trunc
        # G3 glitch marginalization: per-IFO basis support specs and the sigma_g mode.
        #   glitch_specs: {ifo: dict(t1,t2,f1,f2,r)} parsed from --glitch-basis (Sec. 3 Slepian).
        #   glitch_basis_files: {ifo: path.npz} parsed from --glitch-basis-file (Sec. 3 modes
        #                 3/4, data-informed PCA/SVD basis).  REQUIRED npz keys: 'basis'
        #                 (n_snip x r time-domain snippet columns at the analysis sample rate),
        #                 'lambda0' (length r, dimensionless diagonal shape prior, max entry 1
        #                 per Sec. 2b), 't0' (scalar: geocenter seconds relative to the EVENT of
        #                 snippet sample 0), 'fs' (snippet sample rate [Hz], validated vs ctx).
        #                 OPTIONAL population-informed keys (paper-2 Sec. 3; absent => paper-1
        #                 flat behavior EXACTLY): 'sigma_shape' (r x r dimensionless SPD shape
        #                 covariance, max-eig 1 -- replaces the diagonal lambda0 as Lambda0 ==
        #                 Sigma_shape; COVARIANCE-ONLY or FULL mode), 'mu' (length r
        #                 dimensionless population-mean direction mu_hat, scale mixture
        #                 mu_a = sigma_g mu_hat; FULL mode).  SPD is validated on load.
        #   glitch_sigma: "profile" (only wired mode); grid marginalization is a documented
        #                 follow-on (out of scope for G3).
        # The data + geocenter epoch needed to place the basis and profile sigma_g are supplied
        # per detector via register_detector_data (called by ILE before Precompute), keyed on
        # id(psd) so context_for can attach the basis when it first builds a detector's context.
        self.glitch_specs = glitch_specs or {}
        self.glitch_basis_files = glitch_basis_files or {}
        self.glitch_sigma = glitch_sigma
        self._detector_registry = {}   # id(psd) -> dict(ifo, data_fd, event_time_geo)
        self._glitch_contexts = []     # contexts that had a basis attached (for Delta lnZ sum)
        # Stage-3 (T5) segment specification, forwarded to every context this factory builds.
        #   * acf_duration          -> segment_duration -> N_seg (kernel truncation, D5)
        #   * template_start/end     -> template-frame truncation offsets (ILE
        #                               --td-analysis-start / --td-analysis-end), seconds,
        #                               geocenter, relative to the template peak reference
        #   * gaps                   -> list of (t0, t1) seconds (ILE --td-gap, repeatable)
        # NOTE for the full end-to-end ILE segment run (plan T7): the per-detector geocenter
        # placement (segment_start_index derived from the event time, gap_time_origin from
        # the detector epoch, and the segment margin = data-integration-window-half + Earth
        # light-crossing) is applied by the caller; the context here honors whatever indices
        # / origin it is handed.  The AC-level validation drives the context directly.
        self.template_start_offset = template_start_offset
        self.template_end_offset = template_end_offset
        self.gaps = gaps
        self.segment_start_index = segment_start_index
        self.gap_time_origin = gap_time_origin
        self._contexts = {}
        self._warned_inv_spec_trunc = False

    # -------------------------------------------------------------- inv-spec-trunc policy

    def _resolve_inv_spec_trunc(self, inv_spec_trunc_Q, T_spec):
        r"""Decide how inverse-spectrum truncation interacts with the TD kernel (D5).

        RIFT's FD ``inv_spec_trunc_Q`` builds an APPROXIMATE band-limited inverse-spectrum
        operator by iFFT'ing sqrt(1/S), hard-windowing the TD kernel to +-T_spec/2, and
        FFT'ing back (lsu.InnerProduct L2142-2164).  Its purpose is to tame the edge/line
        ringing of the FD-diagonal kernel.

        The TD Toeplitz path does NOT need it: our operator K = irfft(band-limited 1/S) IS
        the exact band-limited inverse-spectrum (precision) operator -- inverse-spectrum
        truncation is precisely the finite-support approximation of what K already is
        exactly (this is the inv-spec-trunc <-> ACF/kernel-truncation duality noted in
        plan D5).  Applying an additional TD window would only DEGRADE K away from the exact
        operator and break the bit-exact drop-in property verified in the T1 tests.

        Policy: when the TD path is active we IGNORE ``inv_spec_trunc_Q`` (warn once).  The
        exactness is the feature; the redundant approximation is dropped, not layered on.
        """
        if inv_spec_trunc_Q and self.warn_inv_spec_trunc and not self._warned_inv_spec_trunc:
            import warnings
            warnings.warn(
                "TDInnerProductFactory: inv_spec_trunc_Q=True (T_spec={0}) requested, but "
                "the TD Toeplitz kernel K = irfft(band-limited 1/S) IS already the exact "
                "band-limited inverse-spectrum operator that inverse-spectrum truncation "
                "only approximates (plan D5).  Ignoring inv_spec_trunc_Q for the TD path "
                "(the exactness is the point; layering the approximation on top would break "
                "the exact drop-in).".format(T_spec), stacklevel=3)
            self._warned_inv_spec_trunc = True
        return False, 0.0

    # -------------------------------------------------------------- context management

    def context_for(self, fmin, fMax, fNyq, deltaF, psd, analyticPSD_Q=False):
        """Return (memoized) ToeplitzInnerProductContext for this detector configuration."""
        key = (round(float(fmin), 9),
               None if fMax is None else round(float(fMax), 9),
               round(float(fNyq), 9), round(float(deltaF), 9),
               bool(analyticPSD_Q), id(psd), self.solver,
               self.acf_duration, self.template_start_offset, self.template_end_offset,
               tuple(self.gaps) if self.gaps else None,
               self.segment_start_index, self.gap_time_origin)
        ctx = self._contexts.get(key)
        if ctx is None:
            if not self._logged_window:
                # One-time provenance banner: assert the TD kernel inherited the SAME data
                # window the surrounding pipeline applied (windowing + PSD <w^2> rescale happen
                # upstream at data load, so the TD path sees identically-windowed data as fd).
                _ws = self.window_shape
                print("  [TD] inner-product factory: inherited data window_shape=%s "
                      "(windowing + T1900249 <w^2> PSD rescale applied UPSTREAM at data load; "
                      "the TD kernel sees the same windowed data/PSD as the fd-diagonal path)."
                      % (_ws,))
                if _ws is None or float(_ws) == 0.0:
                    import warnings
                    warnings.warn(
                        "TDInnerProductFactory: window_shape=%s (rectangular / un-windowed "
                        "data).  This is correct ONLY for zero-noise equivalence tests; a real "
                        "production run must inherit the pipeline window (helper_LDG_Events "
                        "--data-tukey-window-time -> ILE --window-shape).  See plan T7c." % (_ws,),
                        stacklevel=2)
                self._logged_window = True
            ctx = ToeplitzInnerProductContext(
                psd, deltaF, fNyq, fmin, fMax=fMax,
                analyticPSD_Q=analyticPSD_Q, solver=self.solver,
                segment_duration=self.acf_duration,
                segment_start_index=self.segment_start_index,
                gaps=self.gaps, gap_time_origin=self.gap_time_origin,
                template_start_offset=self.template_start_offset,
                template_end_offset=self.template_end_offset)
            self._maybe_attach_glitch(ctx, psd)
            self._contexts[key] = ctx
        return ctx

    def register_detector_data(self, ifo, psd, data_fd, event_time_geo=None):
        """Register a detector's (psd, data, event time) so its glitch basis can be attached.

        Called by ILE once per detector BEFORE Precompute.  Keyed on ``id(psd)`` (the same
        object Precompute forwards to ``context_for``), so the memoized context for that
        detector is the one that receives the basis.  ``event_time_geo`` is the geocenter
        event GPS time; the basis support offsets (geocenter seconds, per --glitch-basis) are
        placed at sample ``round((event_time_geo + t - float(data.epoch)) / deltaT)``.
        """
        self._detector_registry[id(psd)] = dict(
            ifo=ifo, data_fd=data_fd, event_time_geo=event_time_geo)

    def _maybe_attach_glitch(self, ctx, psd):
        r"""Attach the per-IFO Slepian glitch basis to a freshly built context (G3).

        EPOCH ARITHMETIC (documented).  --glitch-basis gives (t1,t2) in GEOCENTER seconds
        relative to the event; the analysis stream's sample 0 sits at GPS ``float(data.epoch)``.
        The basis support in stream samples is therefore
            i0 = round((event_time_geo + t1 - float(data.epoch)) / deltaT)
            i1 = round((event_time_geo + t2 - float(data.epoch)) / deltaT)
        which we hand to ``slepian_basis`` as seconds-from-sample-0 (t_start = i0*deltaT).  If
        ``event_time_geo`` is None (unit tests), t1/t2 are taken as seconds from sample 0.
        """
        reg = self._detector_registry.get(id(psd))
        if reg is None:
            return
        ifo = reg["ifo"]
        spec = self.glitch_specs.get(ifo)
        basis_file = self.glitch_basis_files.get(ifo)
        if spec is None and basis_file is None:
            return
        if ctx.segment_active:
            import warnings
            warnings.warn(
                "TDInnerProductFactory: --glitch-basis for %s ignored -- glitch marginalization "
                "is wired for the full segment only, but a truncated/gapped segment is active "
                "(deferred; see attach_glitch_basis)." % ifo, stacklevel=2)
            return
        from RIFT.likelihood import td_rank_augmentation as tra
        data_fd = reg["data_fd"]
        evt = reg["event_time_geo"]
        origin = 0.0 if evt is None else (float(evt) - float(data_fd.epoch))
        if basis_file is not None:
            # Data-informed basis from file (--glitch-basis-file, Sec. 3 modes 3/4).  The
            # snippet columns are zero-embedded at t0 (same epoch arithmetic as the Slepian
            # branch: geocenter-relative-to-event -> seconds from segment sample 0 -> sample).
            payload = np.load(basis_file)
            for _key in ("basis", "lambda0", "t0", "fs"):
                if _key not in payload:
                    raise ValueError(
                        "--glitch-basis-file %s: npz %r missing required key %r "
                        "(need basis, lambda0, t0, fs)." % (ifo, basis_file, _key))
            snip = np.asarray(payload["basis"], dtype=np.float64)
            Lambda0 = np.asarray(payload["lambda0"], dtype=np.float64)
            fs_file = float(payload["fs"])
            if not np.isclose(fs_file, ctx.fs, rtol=1e-9):
                raise ValueError(
                    "--glitch-basis-file %s: snippet sample rate %g Hz != analysis rate %g Hz."
                    % (ifo, fs_file, ctx.fs))
            if snip.ndim != 2 or snip.shape[1] != Lambda0.shape[0]:
                raise ValueError(
                    "--glitch-basis-file %s: basis shape %s inconsistent with lambda0 shape %s."
                    % (ifo, snip.shape, Lambda0.shape))
            r_file = snip.shape[1]
            # OPTIONAL population-informed keys (paper-2 Sec. 3; backward compatible: absent
            # keys => the paper-1 flat behavior exactly).
            #   sigma_shape (r,r): dimensionless SPD population SHAPE covariance (max-eig 1);
            #       when present it REPLACES the diagonal lambda0 as the covariance shape
            #       Lambda0 == Sigma_shape (COVARIANCE-ONLY or FULL mode).
            #   mu (r,): dimensionless population MEAN direction mu_hat; scale mixture
            #       mu_a(sigma_g) = sigma_g mu_hat (FULL mode).  Absent => zero mean.
            mu0 = None
            files = getattr(payload, "files", list(payload.keys()))
            if "sigma_shape" in files:
                Sigma_shape = np.asarray(payload["sigma_shape"], dtype=np.float64)
                if Sigma_shape.shape != (r_file, r_file):
                    raise ValueError(
                        "--glitch-basis-file %s: sigma_shape shape %s != (r,r)=(%d,%d)."
                        % (ifo, Sigma_shape.shape, r_file, r_file))
                Sigma_shape = 0.5 * (Sigma_shape + Sigma_shape.T)
                evals = np.linalg.eigvalsh(Sigma_shape)
                if evals[0] <= 0:
                    raise ValueError(
                        "--glitch-basis-file %s: sigma_shape is not SPD (min eig %.3e <= 0)."
                        % (ifo, float(evals[0])))
                Lambda0 = Sigma_shape                    # full-matrix shape prior
            if "mu" in files:
                mu0 = np.asarray(payload["mu"], dtype=np.float64).reshape(-1)
                if mu0.size != r_file:
                    raise ValueError(
                        "--glitch-basis-file %s: mu length %d != r=%d."
                        % (ifo, mu0.size, r_file))
            t_start = origin + float(payload["t0"])
            i0 = int(round(t_start * ctx.fs))
            n_snip = snip.shape[0]
            if i0 < 0 or i0 + n_snip > ctx.N:
                raise ValueError(
                    "--glitch-basis-file %s: snippet [%d,%d) falls outside the segment [0,%d)."
                    % (ifo, i0, i0 + n_snip, ctx.N))
            B = np.zeros((ctx.N, snip.shape[1]))
            B[i0:i0 + n_snip, :] = snip
            t_end = t_start + n_snip / ctx.fs
            # population-prior provenance for the banner
            if mu0 is not None and np.any(mu0 != 0.0):
                band_note = "file:%s [FULL pop-prior: mu+Sigma_shape]" % os.path.basename(basis_file)
            elif Lambda0.ndim == 2:
                band_note = "file:%s [COV-ONLY pop-prior: Sigma_shape]" % os.path.basename(basis_file)
            else:
                band_note = "file:%s" % os.path.basename(basis_file)
        else:
            t_start = origin + float(spec["t1"])
            t_end = origin + float(spec["t2"])
            B, Lambda0 = tra.slepian_basis(
                ctx.N, ctx.fs, t_start, t_end, float(spec["f1"]), float(spec["f2"]),
                r=spec.get("r"))
            mu0 = None
            band_note = "band=[%g,%g]Hz" % (spec["f1"], spec["f2"])
        block = (B, Lambda0, ifo) if mu0 is None else (B, Lambda0, ifo, mu0)
        info = ctx.attach_glitch_basis([block], data_fd, sigma_g=self.glitch_sigma)
        self._glitch_contexts.append(ctx)
        print("  [G3] %s glitch basis attached: r=%d, support=[%.4f,%.4f]s, %s, "
              "sigma_g=%.3e, Delta_lnZ=%.4f"
              % (ifo, B.shape[1], t_start, t_end, band_note,
                 info["sigma_g"], info["delta_lnZ"]))

    def glitch_delta_lnZ_total(self):
        """Sum of per-detector glitch Delta lnZ over all contexts with a basis attached (G3).

        This is the additive evidence constant ILE adds to lnL_marg (it does NOT cancel;
        Sec. 4).  Zero if no glitch basis was attached (default OFF)."""
        return float(sum(c.glitch_delta_lnZ for c in self._glitch_contexts))

    # -------------------------------------------------------------- FL-shaped constructors

    def ip_scalar(self, fmin, fMax, fNyq, deltaF, psd,
                  analyticPSD_Q=False, inv_spec_trunc_Q=False, T_spec=0.):
        """Drop-in for ``lsu.ComplexIP(...)`` -> a ``ToeplitzComplexIP`` (scalar U/V)."""
        self._resolve_inv_spec_trunc(inv_spec_trunc_Q, T_spec)
        ctx = self.context_for(fmin, fMax, fNyq, deltaF, psd, analyticPSD_Q=analyticPSD_Q)
        return ToeplitzComplexIP(ctx)

    def overlap(self, fmin, fMax, fNyq, deltaF, psd,
                analyticPSD_Q=False, inv_spec_trunc_Q=False, T_spec=0.,
                full_output=True):
        """Drop-in for ``lsu.ComplexOverlap(..., full_output=True)`` -> Q_lm(t) overlap."""
        self._resolve_inv_spec_trunc(inv_spec_trunc_Q, T_spec)
        ctx = self.context_for(fmin, fMax, fNyq, deltaF, psd, analyticPSD_Q=analyticPSD_Q)
        return ToeplitzComplexOverlap(ctx)

    # -------------------------------------------------------------- T6 diagnostic

    def report_uv_edge_sensitivity(self, hlms, psd, fmin, fMax, fNyq, deltaF,
                                   analyticPSD_Q=False, det=None, window_half=0.075,
                                   warn_tol=1e-2):
        r"""Log the dominant-mode U edge-sensitivity next to guess_snr (plan T6).

        Called from Precompute when the TD inner product is active.  Picks the dominant mode
        (largest |h| peak), applies the template-frame truncation, and reports the U
        variation across +-window_half of the marginalization window (see
        ``ToeplitzInnerProductContext.uv_edge_sensitivity``).  Warns above ``warn_tol``
        (default 1e-2 on the dominant pair, per the plan).  Returns the diagnostic dict.
        """
        if not hlms:
            return None
        ctx = self.context_for(fmin, fMax, fNyq, deltaF, psd, analyticPSD_Q=analyticPSD_Q)
        # dominant mode = largest time-domain peak amplitude
        dom = max(hlms.keys(),
                  key=lambda m: float(np.max(np.abs(_ifft_to_td(hlms[m])))))
        h_td = ctx.apply_template_truncation(_ifft_to_td(hlms[dom]), float(hlms[dom].epoch))
        rep = ctx.uv_edge_sensitivity(h_td, window_half)
        tag = "" if det is None else (" [%s]" % det)
        msg = ("  [T6]%s U edge-sensitivity (mode %s, +-%.3f s window): rel_var=%.3e "
               "(U0=%.6g); segment_active=%s, N_seg=%d, is_spd=%s"
               % (tag, dom, window_half, rep["rel_var"], abs(rep["U0"]),
                  ctx.segment_active, ctx.N_seg, ctx.is_spd))
        print(msg)
        if rep["rel_var"] > warn_tol:
            import warnings
            warnings.warn(
                "TDInnerProductFactory: U edge-sensitivity rel_var=%.3e exceeds tol %.1e on "
                "the dominant pair%s -- the scalar-U (shift-invariant) assumption at "
                "factored_likelihood FL:1936/2019 may be violated for this segment "
                "configuration (a GPS-frame cut or too-wide time window; see plan D1/T6)."
                % (rep["rel_var"], warn_tol, tag), stacklevel=2)

        # G3 extension: when a glitch basis is attached, check whether it overlaps the SLIDING
        # dominant-template support across the marginalization window.  The scalar U' correction
        # is evaluated at the reference time (template-frame convention); if the glitch support
        # overlaps the template track the correction becomes time-dependent (U'(t)) and the
        # time-indexed U'(t) lookup -- deferred here -- would be needed.  We warn but proceed.
        if ctx.glitch_active and ctx._glitch_support is not None:
            gi0, gi1 = ctx._glitch_support
            hpk = int(np.argmax(np.abs(h_td)))
            nz = np.nonzero(np.abs(h_td) > 1e-6 * np.max(np.abs(h_td)))[0]
            hi0, hi1 = (int(nz[0]), int(nz[-1]) + 1) if nz.size else (hpk, hpk + 1)
            n = rep["n_shift"]
            # sliding support of the template across +-n samples
            overlap = (gi0 < hi1 + n) and (hi0 - n < gi1)
            rep["glitch_overlaps_template"] = bool(overlap)
            print("  [G3]%s glitch basis support=[%d,%d) vs template support=[%d,%d) (+-%d "
                  "slide): overlap=%s; U' evaluated at reference time (time-indexed U'(t) "
                  "deferred)." % (tag, gi0, gi1, hi0, hi1, n, overlap))
            if overlap:
                import warnings
                warnings.warn(
                    "TDInnerProductFactory: glitch basis%s overlaps the sliding template "
                    "support -- the U' correction is time-dependent across the window; G3 uses "
                    "the reference-time scalar U' (time-indexed U'(t) lookup deferred, see "
                    "GLITCH_MARG_DESIGN.md Sec. 2/2b)." % tag, stacklevel=2)
        return rep


# --------------------------------------------------------------------------- template truncation (T5)

def template_peak_time(x_td, deltaT, epoch):
    r"""Peak-amplitude reference time t_ref of a template TD array (plan D1).

    Epoch bookkeeping (documented precisely).  Sample ``i`` of ``x_td`` sits at time
    ``t_i = float(epoch) + i * deltaT`` in the template's OWN frame -- ``epoch`` is the GPS
    time of sample 0 as carried by the lal series the mode was inverse-FFT'd from (for a
    RIFT ``hlmoff`` FD mode this is the ``hlm.epoch`` set at lalsimutils.py:3338,
    ``epoch = -(N deltaT) * fd_centering_factor``, i.e. the peak sits near ``t ~ 0`` in this
    frame).  ``t_ref`` is defined as the argmax of ``|x_td|``:

        t_ref = float(epoch) + argmax(|x_td|) * deltaT.

    This is the reference the user-facing ``--td-analysis-start/--td-analysis-end`` offsets
    are measured from.
    """
    x = np.asarray(x_td)
    ipk = int(np.argmax(np.abs(x)))
    return float(epoch) + ipk * float(deltaT)


def truncate_timeseries_template_frame(x_td, deltaT, epoch,
                                       start_offset, end_offset, t_ref=None):
    r"""Hard-cut a template TD array to [t_ref+start_offset, t_ref+end_offset] (plan D1, T5).

    Zero every sample OUTSIDE the template-frame window; NO taper at the cut -- the whole
    point of the exact finite-segment treatment is a sharp boundary (the correlation
    structure of K_seg handles the template sliding off the edge exactly).  ``t_ref`` is the
    peak-amplitude reference (see ``template_peak_time``); if None it is computed from
    ``x_td``.  ``start_offset`` / ``end_offset`` are seconds relative to ``t_ref`` (either
    may be None to leave that side open).

    Interaction with the always-on start taper in ``lalsimutils.hlmoft`` (LSU:~3341,
    ``hlmsT[mode].data.data[:ntaper] *= vectaper``): that taper multiplies the FIRST
    ``ntaper ~ 1% of TDlen`` samples -- the low-frequency turn-on at the *start* of the
    waveform.  For a ringdown / post-peak cut (``start_offset >= 0``) the retained window
    begins at the peak, tens of thousands of samples AFTER that start-taper region, so the
    taper lies entirely in the discarded pre-peak portion and does NOT touch the analysis
    segment.  (Verified: for the 8 s / 2048 Hz template used here ntaper ~ 160 samples at the
    segment start while the peak sits near the segment centre.)  No taper disabling is needed
    for post-peak cuts; a cut that RETAINS the waveform start (start_offset very negative)
    would include the taper, which is documented here rather than silently altered.

    Returns a NEW array (copy) with the out-of-window samples set to 0.
    """
    x = np.asarray(x_td)
    if start_offset is None and end_offset is None:
        return x.copy()
    if t_ref is None:
        t_ref = template_peak_time(x, deltaT, epoch)
    N = x.shape[-1]
    i_lo = 0
    i_hi = N
    if start_offset is not None:
        # first retained sample: t_i >= t_ref+start_offset
        i_lo = int(np.ceil((t_ref + float(start_offset) - float(epoch)) / float(deltaT)))
    if end_offset is not None:
        # last retained sample: t_i <= t_ref+end_offset  =>  half-open upper index
        i_hi = int(np.floor((t_ref + float(end_offset) - float(epoch)) / float(deltaT))) + 1
    i_lo = max(0, min(N, i_lo))
    i_hi = max(0, min(N, i_hi))
    out = np.zeros_like(x)
    if i_hi > i_lo:
        out[..., i_lo:i_hi] = x[..., i_lo:i_hi]
    return out


# --------------------------------------------------------------------------- helpers

def _ifft_to_td(hf):
    """iFFT a two-sided COMPLEX16FrequencySeries to a TD complex array (lal conventions).

    Uses lal's reverse COMPLEX16 FFT (multiplies by deltaF), matching every convention the
    FD inner products assume.  Returns a plain numpy array (a copy).
    """
    from RIFT import lalsimutils as lsu
    return np.asarray(lsu.DataInverseFourier(hf).data.data).copy()


def _hertz_unit():
    try:
        from RIFT.lalsimutils import lsu_HertzUnit
        return lsu_HertzUnit
    except Exception:  # pragma: no cover
        return lal.HertzUnit


def _dimensionless_unit():
    try:
        from RIFT.lalsimutils import lsu_DimensionlessUnit
        return lsu_DimensionlessUnit
    except Exception:  # pragma: no cover
        return lal.DimensionlessUnit
