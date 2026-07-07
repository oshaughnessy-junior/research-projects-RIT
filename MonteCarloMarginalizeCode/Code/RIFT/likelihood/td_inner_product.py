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
                 analyticPSD_Q=True, N=None, solver="auto"):
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
        if self.N <= _MAX_DENSE_N and solver in ("auto", "dense"):
            self._build_dense()
        elif solver in ("auto", "pcg"):
            # Larger N: iterative path is attempted lazily on first solve/dot.
            pass
        # cache of w = K^{-1} d (or K d, precision-operator apply) per data vector id
        self._w_cache = {}

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

    # ------------------------------------------------------------------ operator apply

    def apply_K(self, v):
        """Return K v, the precision operator applied to a (real or complex) vector v.

        For the dense path this is a matrix-vector product; for the iterative path the
        exact Toeplitz matvec via circulant embedding is used.  Bilinear (no conjugation).
        """
        v = np.asarray(v)
        if self._dense is not None:
            if np.iscomplexobj(v):
                return self._dense.dot(v.real) + 1j * self._dense.dot(v.imag)
            return self._dense.dot(v)
        # circulant-embedding exact Toeplitz matvec (matches the dense result), used for
        # large N where forming the dense matrix is too costly.
        return self._toeplitz_matvec(v)

    def _toeplitz_matvec(self, v):
        """Exact symmetric-Toeplitz matvec K v via zero-padded circular convolution."""
        N = self.N
        col = self.kappa[:N]
        # embed the symmetric Toeplitz into a circulant of length >= 2N-1
        m = 2 * N - 1
        c = np.zeros(m)
        c[:N] = col
        c[N:] = col[1:][::-1]        # symmetric first row = first column for symmetric T
        fc = np.fft.fft(c)

        def _mv(x):
            xp = np.zeros(m, dtype=x.dtype)
            xp[:N] = x
            y = np.fft.ifft(fc * np.fft.fft(xp))[:N]
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
        return ctx.ip_normalization * ctx.dot_td(h1_td, h2_td)

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

        # Integrand exactly as lsu.ComplexOverlap: 2 conj(hlm~) data~ weights2side.
        # Because K's FD symbol == weights2side, this is 2 conj(hlm~) (K data)~, i.e. the
        # impulse-response construction rhoTS = iFFT(conj(hlm~) FFT(K data)) up to the
        # identical lal reverse-FFT scaling.
        self._intgd.data.data = (2.0 * np.conj(np.asarray(hlm.data.data))
                                 * np.asarray(data.data.data) * ctx.weights2side)
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

    def __init__(self, solver="auto", acf_duration=None, warn_inv_spec_trunc=True):
        self.solver = solver
        self.acf_duration = acf_duration
        self.warn_inv_spec_trunc = warn_inv_spec_trunc
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
               bool(analyticPSD_Q), id(psd), self.solver)
        ctx = self._contexts.get(key)
        if ctx is None:
            ctx = ToeplitzInnerProductContext(
                psd, deltaF, fNyq, fmin, fMax=fMax,
                analyticPSD_Q=analyticPSD_Q, solver=self.solver)
            self._contexts[key] = ctx
        return ctx

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
