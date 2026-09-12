"""Device-native aligned-spin IMRPhenomD modes for GPU precomputation.

This module deliberately has no import-time JAX or Ripple dependency.  The
production RIFT image can therefore import the likelihood package before the
optional waveform backend is installed.  Callers receive RIFT's two-sided,
descending frequency packing rather than Ripple's positive-frequency strain.

The public provider fails closed for ``conditioning='rift'``.  IMRPhenomD
currently uses LAL's ``SimInspiralTDModesFromPolarizations`` route, whose conditioning and
epoch differ from RIFT's ChooseFDModes route.  A match maximized over time and
phase is not sufficient to certify that likelihood-sensitive convention.
"""

from dataclasses import dataclass, field
import math


class RippleUnavailableError(ImportError):
    """Raised when the optional current RippleGW package is unavailable."""


class WaveformCompatibilityError(ValueError):
    """Raised when parameters cannot use the restricted native adapter."""


@dataclass(frozen=True)
class DeviceFDModeBank:
    """Conditioned intrinsic modes in RIFT's two-sided FD packing."""

    modes: dict
    conjugate_modes: dict
    frequencies: object
    delta_f: float
    delta_t: float
    epoch: float
    conditioned: bool
    backend: str = "ripplegw.IMRPhenomD"
    metadata: dict = field(default_factory=dict)


def _load_ripple():
    try:
        from ripplegw.waveforms import IMRPhenomD
    except (ImportError, ModuleNotFoundError) as exc:
        raise RippleUnavailableError(
            "GPU IMRPhenomD requires the current `ripplegw` package.  The "
            "historical PyPI ripplegw==0.0.1 imports as `ripple` and is not "
            "compatible with current JAX; install Ripple from its official "
            "repository at a pinned commit."
        ) from exc
    if not hasattr(IMRPhenomD, "gen_IMRPhenomD"):
        raise RippleUnavailableError(
            "Installed RippleGW has no IMRPhenomD.gen_IMRPhenomD API"
        )
    return IMRPhenomD


def _array_namespace(backend):
    if backend is None or backend == "jax":
        try:
            import jax
            import jax.numpy as jnp
        except (ImportError, ModuleNotFoundError) as exc:
            raise RippleUnavailableError("GPU IMRPhenomD requires JAX") from exc
        if not bool(jax.config.x64_enabled):
            raise WaveformCompatibilityError(
                "JAX x64 must be enabled before importing JAX for likelihood "
                "precompute (set JAX_ENABLE_X64=1)"
            )
        return jnp
    if isinstance(backend, str):
        raise WaveformCompatibilityError("waveform backend must be 'jax'")
    # Ripple returns JAX arrays and conditioning uses functional `.at` updates.
    # Accept jax.numpy for ergonomic direct calls, but reject CuPy/NumPy rather
    # than silently moving a multi-gigabyte waveform through host memory.
    if not getattr(backend, "__name__", "").startswith("jax.numpy"):
        raise WaveformCompatibilityError(
            "native Ripple generation requires backend='jax' (the GPU bank "
            "bridges JAX to CuPy with DLPack)"
        )
    import jax
    if not bool(jax.config.x64_enabled):
        raise WaveformCompatibilityError("JAX x64 must be enabled (set JAX_ENABLE_X64=1)")
    return backend


def _scalar(value, name):
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise WaveformCompatibilityError(
            "%s must be scalar for one intrinsic waveform" % name
        ) from exc
    if not math.isfinite(result):
        raise WaveformCompatibilityError("%s must be finite" % name)
    return result


def _validate(P, Lmax):
    if int(Lmax) != 2:
        raise WaveformCompatibilityError(
            "native Ripple IMRPhenomD supplies only (2,+/-2); require Lmax=2"
        )
    try:
        import lalsimulation as lalsim
        approx_name = lalsim.GetStringFromApproximant(P.approx)
    except (ImportError, AttributeError, TypeError, RuntimeError):
        approx_name = str(getattr(P, "approx", ""))
    if approx_name != "IMRPhenomD":
        raise WaveformCompatibilityError(
            "native adapter only supports IMRPhenomD, got %r" % approx_name
        )
    for name in ("s1x", "s1y", "s2x", "s2y"):
        if abs(_scalar(getattr(P, name, 0.0), name)) > 1.0e-12:
            raise WaveformCompatibilityError("IMRPhenomD requires aligned spins")
    for name in ("lambda1", "lambda2", "eccentricity"):
        if _scalar(getattr(P, name, 0.0), name) != 0.0:
            raise WaveformCompatibilityError("native IMRPhenomD does not support %s" % name)
    dt = _scalar(P.deltaT, "deltaT")
    df = _scalar(P.deltaF, "deltaF")
    if dt <= 0 or df <= 0:
        raise WaveformCompatibilityError("deltaT and deltaF must be positive")
    n_float = 1.0 / (dt * df)
    n = int(round(n_float))
    if n < 4 or n % 2 or abs(n - n_float) > 1.0e-7 * n:
        raise WaveformCompatibilityError(
            "1/(deltaT*deltaF) must be an even integer, got %.17g" % n_float
        )
    if _scalar(P.fmin, "fmin") <= 0:
        raise WaveformCompatibilityError("fmin must be positive")
    return dt, df, n


def _component_masses_msun(P):
    # RIFT stores SI masses.  Keeping the constants local avoids importing LAL
    # (and hence host-side waveform code) on the native path.
    msun_si = 1.9884099021470416e30
    m1 = _scalar(P.m1, "m1") / msun_si
    m2 = _scalar(P.m2, "m2") / msun_si
    if m1 <= 0 or m2 <= 0:
        raise WaveformCompatibilityError("component masses must be positive")
    if m2 > m1:
        m1, m2 = m2, m1
        chi1, chi2 = _scalar(P.s2z, "s2z"), _scalar(P.s1z, "s1z")
    else:
        chi1, chi2 = _scalar(P.s1z, "s1z"), _scalar(P.s2z, "s2z")
    if max(abs(chi1), abs(chi2)) > 1:
        raise WaveformCompatibilityError("dimensionless spins must be in [-1,1]")
    return m1, m2, chi1, chi2


def _continuous_inverse(values, dt, xpy):
    # LAL's complex FFT stores the RIFT descending grid directly.  Relative to
    # numpy/JAX FFT convention its centered origin is the (-1)^j time factor;
    # reversing bins here would also conjugate time evolution.
    alternating = 1 - 2 * (xpy.arange(values.shape[0]) % 2)
    return xpy.fft.ifft(values) * alternating / dt


def _continuous_forward(values, dt, xpy):
    alternating = 1 - 2 * (xpy.arange(values.shape[0]) % 2)
    return dt * xpy.fft.fft(values * alternating)


def _conjugate_spectrum(mode, xpy):
    n = mode.shape[0]
    reflection = (-xpy.arange(n)) % n
    return xpy.conj(mode[reflection])


def generate_imrphenomd_fd(
    P,
    Lmax=2,
    backend=None,
    *,
    fd_standoff_factor=0.9,
    fd_centering_factor=0.9,
    conditioning="rift",
    **_ignored,
):
    """Generate direct-FD ``(2,+/-2)`` spectra natively with Ripple/JAX.

    The returned frequency grid is exactly RIFT's convention
    ``f[k] = df * (N/2-k)``.  The waveform normalization and mode mapping were
    checked against LAL IMRPhenomD: Ripple's ``h0`` is the negative-frequency
    ``(2,-2)`` carrier after multiplication by ``sqrt(16*pi/5)``.

    ``conditioning='direct_fd'`` is available for development and is
    intentionally marked unconditioned so likelihood integration rejects it.
    """
    dt, df, n = _validate(P, Lmax)
    if conditioning == "rift":
        raise WaveformCompatibilityError(
            "exact RIFT IMRPhenomD conditioning is not certified: the legacy "
            "path uses SimInspiralTDModesFromPolarizations; use "
            "conditioning='direct_fd' only for explicit waveform tests"
        )
    if conditioning not in ("raw", "direct_fd"):
        raise WaveformCompatibilityError("conditioning must be 'rift' or 'direct_fd'")
    if not 0 < _scalar(fd_standoff_factor, 'fd_standoff_factor') < 1:
        raise WaveformCompatibilityError("fd_standoff_factor must lie strictly between 0 and 1")
    xpy = _array_namespace(backend)
    ripple = _load_ripple()
    m1, m2, chi1, chi2 = _component_masses_msun(P)
    mtot = m1 + m2
    eta = m1 * m2 / mtot**2
    mc = (m1 * m2) ** (3.0 / 5.0) / mtot ** (1.0 / 5.0)
    dist_mpc = _scalar(P.dist, "dist") / 3.085677581491367e22
    if dist_mpc <= 0:
        raise WaveformCompatibilityError("distance must be positive")
    fmin = _scalar(P.fmin, "fmin")
    fref = _scalar(getattr(P, "fref", 0.0), "fref") or fmin
    phiref = _scalar(getattr(P, "phiref", 0.0), "phiref")

    # Positive frequencies are ascending for Ripple; the resulting arrays are
    # assembled directly into RIFT packing, with no host copy.
    fpos = df * xpy.arange(1, n // 2 + 1, dtype=xpy.float64)
    params = xpy.asarray(
        [mc, eta, chi1, chi2, dist_mpc, 0.0, phiref], dtype=xpy.float64
    )
    h0 = ripple.gen_IMRPhenomD(fpos, params, fref)
    fmax = _scalar(getattr(P, "fmax", 0.0), "fmax")
    if fmax <= 0:
        fmax = 0.5 / dt

    low = fmin * float(fd_standoff_factor)
    taper = xpy.where(
        fpos < low,
        0.0,
        xpy.where(
            fpos < fmin,
            0.5
            + 0.5
            * xpy.cos(math.pi * (fpos / fmin - 1.0) / (1.0 - fd_standoff_factor)),
            1.0,
        ),
    )
    h0 = xpy.where(fpos <= fmax, h0 * taper, 0.0)

    norm = math.sqrt(16.0 * math.pi / 5.0)
    z = xpy.zeros(n, dtype=xpy.complex128)
    # RIFT indices 1..N/2-1 are +(Nyquist-df)..+df.  Indices N/2+1..
    # are -df..-(Nyquist-df).  A periodic complex FFT has only one Nyquist
    # bin, so leave it zero to preserve the exact +/-m reflection identity.
    h22 = z.at[1 : n // 2].set(norm * xpy.conj(h0[: n // 2 - 1][::-1]))
    h2m2 = z.at[n // 2 + 1 :].set(norm * h0[: n // 2 - 1])
    raw_modes = {(2, 2): h22, (2, -2): h2m2}

    if conditioning in ("raw", "direct_fd"):
        modes = raw_modes
        conditioned = False
        epoch = 0.0
    else:
        raise WaveformCompatibilityError(
            "conditioning must be 'rift' or 'direct_fd'"
        )

    conjugate = {lm: _conjugate_spectrum(v, xpy) for lm, v in modes.items()}
    frequencies = df * (n // 2 - xpy.arange(n, dtype=xpy.float64))
    return DeviceFDModeBank(
        modes=modes,
        conjugate_modes=conjugate,
        frequencies=frequencies,
        delta_f=df,
        delta_t=dt,
        epoch=epoch,
        conditioned=conditioned,
        metadata={
            "N": n,
            "mode_normalization": "sqrt(16*pi/5)",
            "fd_standoff_factor": float(fd_standoff_factor),
            "conditioning": conditioning,
            "ripple_api": "ripplegw.waveforms.IMRPhenomD.gen_IMRPhenomD",
        },
    )


# Provider name used by the high-level GPU precompute dispatch.
imrphenomd_provider = generate_imrphenomd_fd


__all__ = [
    "DeviceFDModeBank",
    "RippleUnavailableError",
    "WaveformCompatibilityError",
    "generate_imrphenomd_fd",
    "imrphenomd_provider",
]
