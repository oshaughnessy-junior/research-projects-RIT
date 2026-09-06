"""Fixed-shape multi-peak marginalization over time, polarization, phase and distance.

This module is the device-evaluation half of the all-variable peak-local design.
It deliberately separates three jobs which must not be conflated:

* the compact norm table produced by the upstream packed ``U,V`` contraction is
  summarized once and used by the host planner to rank candidate basins;
* JAX gradients and Hessians refine supplied starts and size local boxes;
* a separate completeness/omitted-mass warrant decides whether the local integral
  is usable.  Optimizer convergence is never treated as proof that every mode was
  found.

The integration kernel consumes a padded :class:`AllAxisModePlan`, so its live
workspace is ``O(local_order**4)`` and is independent of the dense time/angle/
distance resolutions.  Time is reconstructed from the reflected primitive only
at the local nodes.  The two angular axes use the exact finite Fourier tables, and
distance uses the exact quadratic likelihood with the volumetric ``x**-4``
Jacobian.  Modes are streamed through ``lax.scan``; no ``mode x local-grid`` tensor
is retained under nested JIT/AD/vmap transformations.  Values are unnormalized:
the caller-provided ``log_normalization`` must include the time-sample Jacobian
and normalized time, angle, and distance priors appropriate to its application.

This is an explicit prototype seam, not a sampler policy.  ``ok=False`` means the
caller must evaluate its dense/exact reserve and keep the sample.  It never means
waveform failure and the diagnostic local value must not be substituted silently.
``ok`` is only a scalar-value usability gate: the outside-mass bound may be
certified, but the nested quadrature comparison is validated rather than a formal
error bound.  Although the fixed-shape kernel is compatible with outer JIT/AD,
differentiating it holds
the host plan and its regions fixed and therefore differentiates the truncated
local integral.  A production gradient/Hessian consumer needs a separate omitted-
derivative warrant; this prototype does not claim one.
"""

from typing import NamedTuple

import jax
import jax.numpy as jnp
import numpy as np

from .time_first_peaklocal import (_evaluate_time_spectrum,
                                   _time_primitive_spectrum)


__all__ = [
    "AllAxisModePlan",
    "UVHarmonicSummary",
    "summarize_uv_norm_table",
    "rank_time_starts_from_uv",
    "algebraic_angle_starts_from_uv",
    "refine_all_axis_starts",
    "select_refined_modes",
    "mode_local_geometry",
    "make_all_axis_mode_plan",
    "all_axis_peak_local_marginalize",
]


class AllAxisModePlan(NamedTuple):
    """Padded host plan consumed by the fixed-shape device kernel.

    Coordinates are ``(time_sample, phi_ref, u=2*psi, x=Dref/D)``.
    ``outside_log_bound`` bounds the *unnormalized* integral outside the union
    of the exact transformed regions
    ``center + local_transform @ [-local_radius,local_radius]^4`` (with the two
    angular coordinates interpreted periodically).  A finite value is
    correctness-bearing only when ``outside_bound_certified`` is true.  The
    axis-aligned ``half_widths`` are derived conservative enclosures used only
    for support and disjointness checks; they never define the certified cover.
    ``time_reconstruction_certified`` is a separate guard/seam warrant.  Real
    unguarded captures must leave it false; an outside-mass certificate cannot
    certify the noninteger reflected-time reconstruction inside a region.
    ``enumeration_complete`` is a separate
    diagnostic statement about the supplied root set.  It is deliberately not
    an acceptance requirement: a missed algebraic root is scientifically
    harmless when the independent bound proves that *all* mass outside the
    integrated regions fits the error budget.  Algebraic roots, optimizer
    starts, and an outside bound have different failure modes and remain
    separate here.
    """

    centers: jax.Array
    half_widths: jax.Array
    local_transforms: jax.Array
    local_radius: jax.Array
    live: jax.Array
    outside_log_bound: jax.Array
    enumeration_complete: jax.Array
    outside_bound_certified: jax.Array
    time_reconstruction_certified: jax.Array
    boxes_disjoint: jax.Array


class UVHarmonicSummary(NamedTuple):
    """Compact structural summary of the norm table derived from ``U,V``.

    ``C_B`` is the exact harmonic table already produced upstream from the
    packed self terms.  This class does not claim to repeat or count that
    contraction.  The lower/upper and derivative entries are triangle-
    inequality bounds, not fits.  They are host-planning data and should be
    cached outside JIT/vmap.
    """

    C_B: np.ndarray
    b_lower: float
    b_upper: float
    phi_derivative_bound: float
    u_derivative_bound: float
    time_invariant: bool
    time_max_deviation: float
    summary_build_count: int
    input_harmonic_coefficients: int


def _kp_weights_numpy(n):
    out = np.ones(int(n), dtype=float)
    out[1:] = 2.0
    return out


def summarize_uv_norm_table(C_B_t, *, invariance_atol=1.0e-10):
    """Collapse the ``U,V``-derived norm table and form exact harmonic bounds.

    ``C_B_t`` may be ``(KP,2KS+1)`` or the historical
    ``(KP,2KS+1,Ntime)`` table.  Ordinary (non-rotation) ILE has a
    time-independent norm; the latter representation repeats it at every time.
    Arrival-time-dependent input is reported in ``time_invariant`` and must make
    a peak-local plan decline rather than being averaged away.
    """
    table = np.asarray(C_B_t, dtype=np.complex128)
    if table.ndim == 2:
        base = table
        deviation = 0.0
    elif table.ndim == 3:
        base = table[..., 0]
        deviation = float(np.max(np.abs(table - base[..., None])))
    else:
        raise ValueError("C_B_t must have shape (KP,2KS+1[,Ntime])")
    scale = max(1.0, float(np.max(np.abs(base))))
    invariant = bool(np.isfinite(deviation)
                     and deviation <= float(invariance_atol) * scale)

    kp = np.arange(base.shape[0], dtype=float)[:, None]
    ks_max = (base.shape[1] - 1) // 2
    ks = np.arange(-ks_max, ks_max + 1, dtype=float)[None, :]
    weight = _kp_weights_numpy(base.shape[0])[:, None]
    magnitude = weight * np.abs(base)
    centre = float(base[0, ks_max].real)
    remainder = float(np.sum(magnitude) - abs(base[0, ks_max]))
    # B=<h|h> is non-negative.  Combining that identity with the harmonic
    # triangle inequality makes the lower bound tighter but never optimistic.
    b_lower = max(0.0, centre - remainder)
    b_upper = abs(centre) + remainder
    m_phi = float(np.sum(magnitude * np.abs(kp)))
    m_u = float(np.sum(magnitude * np.abs(ks)))
    return UVHarmonicSummary(
        np.ascontiguousarray(base), b_lower, b_upper, m_phi, m_u,
        invariant, deviation, 1, int(table.size))


def rank_time_starts_from_uv(C_A_t, uv_summary, x_min, x_max, *,
                             max_starts=16, min_separation=2):
    """Rank time basins with a true ``U,V``-informed likelihood envelope.

    For every retained time sample, ``A_upper=sum w_k |C_A|`` bounds the data
    term over both angles.  ``uv_summary.b_lower`` bounds the norm from below,
    so maximizing ``x*A_upper - B_lower*x**2/2`` on the physical distance
    interval gives an upper envelope.  The volumetric ``-4 log(x)`` term is
    separately bounded at ``x_min``.  This ranks starts cheaply; it does *not*
    certify that unselected time cells are negligible.  That remains the
    outside-mass warrant in :class:`AllAxisModePlan`.
    """
    C_A_t = np.asarray(C_A_t, dtype=np.complex128)
    if C_A_t.ndim != 3:
        raise ValueError("C_A_t must have shape (KP,2KS+1,Ntime)")
    if not isinstance(uv_summary, UVHarmonicSummary):
        raise TypeError("uv_summary must come from summarize_uv_norm_table")
    if not uv_summary.time_invariant:
        raise ValueError("arrival-time-dependent U,V norm cannot be collapsed")
    x_min, x_max = float(x_min), float(x_max)
    if not (0.0 < x_min < x_max):
        raise ValueError("need 0 < x_min < x_max")
    max_starts = int(max_starts)
    min_separation = int(min_separation)
    if max_starts < 1 or min_separation < 0:
        raise ValueError("invalid start-count policy")

    weight = _kp_weights_numpy(C_A_t.shape[0])[:, None, None]
    a_upper = np.sum(weight * np.abs(C_A_t), axis=(0, 1))
    if uv_summary.b_lower > 0.0:
        x_star = np.clip(a_upper / uv_summary.b_lower, x_min, x_max)
    else:
        x_star = np.full_like(a_upper, x_max)
    envelope = (x_star * a_upper
                - 0.5 * uv_summary.b_lower * np.square(x_star)
                - 4.0 * np.log(x_min))

    # Endpoints are legitimate boundary basins.  Interior starts are drawn only
    # from local maxima, then ranked by the structural upper envelope.
    is_peak = np.ones(envelope.size, dtype=bool)
    if envelope.size > 2:
        is_peak[1:-1] = ((envelope[1:-1] >= envelope[:-2])
                         & (envelope[1:-1] >= envelope[2:]))
    candidates = np.flatnonzero(is_peak)
    candidates = candidates[np.argsort(envelope[candidates])[::-1]]
    selected = []
    for index in candidates:
        if all(abs(int(index) - old) > min_separation for old in selected):
            selected.append(int(index))
        if len(selected) == max_starts:
            break
    if not selected:
        selected = [int(np.argmax(envelope))]
    return np.asarray(selected, dtype=np.int32), envelope


def _numpy_angular_field(C, phi, u):
    kp = np.arange(C.shape[0], dtype=float)[:, None]
    ks_max = (C.shape[1] - 1) // 2
    ks = np.arange(-ks_max, ks_max + 1, dtype=float)[None, :]
    weight = _kp_weights_numpy(C.shape[0])[:, None]
    return float(np.sum(weight * C * np.exp(1j * (kp * phi + ks * u))).real)


def _distance_start(K, R, x_min, x_max):
    """Best support-aware stationary/boundary candidate for ``x^-4 L``."""
    candidates = [float(x_min), float(x_max)]
    R = max(float(R), 0.0)
    K = float(K)
    discriminant = K * K - 16.0 * R
    if R > 0.0 and discriminant >= 0.0:
        x_plus = (K + np.sqrt(discriminant)) / (2.0 * R)
        if x_min <= x_plus <= x_max:
            candidates.append(float(x_plus))
    values = [K * x - 0.5 * R * x * x - 4.0 * np.log(x)
              for x in candidates]
    return candidates[int(np.argmax(values))]


def algebraic_angle_starts_from_uv(C_A_t, uv_summary, time_starts,
                                   x_min, x_max):
    """Build sparse four-axis starts from U,V ranking and algebraic maxima.

    One U,V-informed distance probe is used per selected time basin.  At that
    probe the exact bivariate trigonometric stationary system is enumerated by
    :func:`RIFT.likelihood.bivariate_trig_stationary.enumerate_torus_maxima`.
    Every returned maximum is then assigned its support-aware analytic distance
    candidate.  No generic angle or distance seed lattice is constructed.

    The returned ``all_enumerations_ok`` covers only the angular solves at the
    probed time/distance slices.  It is deliberately *not* suitable for
    ``AllAxisModePlan.enumeration_complete``: completeness of the joint 4-D
    modes still needs the independent outside-cover warrant.
    """
    from RIFT.likelihood.bivariate_trig_stationary import enumerate_torus_maxima
    from RIFT.likelihood.joint_angle_peak_local import joint_table

    C_A_t = np.asarray(C_A_t, dtype=np.complex128)
    time_starts = np.asarray(time_starts, dtype=np.int32).ravel()
    if not isinstance(uv_summary, UVHarmonicSummary):
        raise TypeError("uv_summary must come from summarize_uv_norm_table")
    _validate_tables(C_A_t, uv_summary.C_B)
    starts = []
    reports = []
    all_ok = True
    weight = _kp_weights_numpy(C_A_t.shape[0])[:, None]
    b_scale = max(uv_summary.b_lower,
                  float(np.abs(uv_summary.C_B[0,
                      (uv_summary.C_B.shape[1] - 1) // 2])), 1.0e-30)
    for time_index in time_starts:
        if not (0 <= int(time_index) < C_A_t.shape[-1]):
            raise ValueError("time start outside retained support")
        C_A = C_A_t[..., int(time_index)]
        a_upper = float(np.sum(weight * np.abs(C_A)))
        x_probe = float(np.clip(a_upper / b_scale, x_min, x_max))
        result = enumerate_torus_maxima(
            joint_table(C_A, uv_summary.C_B, x_probe))
        report = dict(result.report)
        report.update(time_index=int(time_index), x_probe=x_probe)
        reports.append(report)
        all_ok = all_ok and bool(result.ok)
        for phi, u in result.points:
            K = _numpy_angular_field(C_A, phi, u)
            R = _numpy_angular_field(uv_summary.C_B, phi, u)
            x = _distance_start(K, R, float(x_min), float(x_max))
            starts.append((float(time_index), float(phi), float(u), x))
    return (np.asarray(starts, dtype=float).reshape((-1, 4)),
            bool(all_ok), reports)


def _validate_tables(C_A_t, C_B):
    if C_A_t.ndim != 3:
        raise ValueError("C_A_t must have shape (KP,2KS+1,Ntime)")
    if C_B.ndim != 2:
        raise ValueError("C_B must be the collapsed (KP,2KS+1) norm table")
    if C_A_t.shape[1] % 2 != 1 or C_B.shape[1] % 2 != 1:
        raise ValueError("angular harmonic axes must have odd length")
    if C_A_t.shape[0] > C_B.shape[0] or C_A_t.shape[1] > C_B.shape[1]:
        raise ValueError("C_B must contain every harmonic represented by C_A")


def _angular_field(C, phi, u):
    """Evaluate a real stored-half-plane angular Fourier table at one point."""
    kp = jnp.arange(C.shape[0], dtype=jnp.float64)
    ks_max = (C.shape[1] - 1) // 2
    ks = jnp.arange(-ks_max, ks_max + 1, dtype=jnp.float64)
    weight = jnp.where(kp == 0.0, 1.0, 2.0)
    phase = jnp.exp(1j * (kp[:, None] * phi + ks[None, :] * u))
    return jnp.sum(weight[:, None] * C * phase).real


def _scalar_log_density(theta, coeff, frequency, offset, C_A_shape, C_B,
                        x_min, x_max):
    """Unnormalized four-axis log density at one continuous coordinate."""
    t, phi, u, x = theta
    flat = _evaluate_time_spectrum(
        coeff, frequency, jnp.atleast_1d(t), offset)[:, 0]
    C_A = flat.reshape(C_A_shape[:-1])
    A = _angular_field(C_A, phi, u)
    B = _angular_field(C_B, phi, u)
    inside = ((t >= 0.0) & (t <= C_A_shape[-1] - 1.0)
              & (x >= x_min) & (x <= x_max) & (x > 0.0))
    value = x * A - 0.5 * x * x * B - 4.0 * jnp.log(jnp.maximum(x, 1e-300))
    return jnp.where(inside, value, -jnp.inf)


def refine_all_axis_starts(C_A_t, C_B, starts, x_min, x_max, *,
                           time_guard=0,
                           iterations=12, ridge=1.0e-8,
                           max_step=(2.0, 0.5, 0.5, 0.25)):
    """Refine four-axis starts with fixed-iteration JAX gradient/Hessian steps.

    This is local optimization only.  The return values report stationarity and
    local curvature; they do not assert completeness.  Angular coordinates are
    wrapped, while time and distance remain on their physical support.
    """
    C_A_t = jnp.asarray(C_A_t, dtype=jnp.complex128)
    C_B = jnp.asarray(C_B, dtype=jnp.complex128)
    starts = jnp.asarray(starts, dtype=jnp.float64)
    _validate_tables(C_A_t, C_B)
    if starts.ndim != 2 or starts.shape[1] != 4:
        raise ValueError("starts must have shape (N,4)")
    if int(iterations) < 1:
        raise ValueError("iterations must be positive")
    time_guard = int(time_guard)
    n_time = C_A_t.shape[-1] - 2 * time_guard
    if time_guard < 0 or n_time < 2:
        raise ValueError("time_guard must leave at least two integration samples")
    coeff, frequency, offset = _time_primitive_spectrum(
        C_A_t.reshape((-1, C_A_t.shape[-1])), time_guard)
    model_shape = C_A_t.shape[:-1] + (n_time,)
    fn = lambda th: _scalar_log_density(
        th, coeff, frequency, offset, model_shape, C_B,
        float(x_min), float(x_max))
    grad_fn = jax.grad(fn)
    hess_fn = jax.hessian(fn)
    max_step = jnp.asarray(max_step, dtype=jnp.float64)

    def _project(th):
        return jnp.asarray([
            jnp.clip(th[0], 0.0, n_time - 1.0),
            jnp.mod(th[1], 2.0 * jnp.pi),
            jnp.mod(th[2], 2.0 * jnp.pi),
            jnp.clip(th[3], float(x_min), float(x_max)),
        ])

    def _one(start):
        def _step(th, _):
            g = grad_fn(th)
            H = hess_fn(th)
            eigenvalue, eigenvector = jnp.linalg.eigh(-H)
            safe = jnp.maximum(eigenvalue, float(ridge))
            step = eigenvector @ ((eigenvector.T @ g) / safe)
            step = jnp.clip(step, -max_step, max_step)
            proposals = jax.vmap(
                lambda scale: _project(th + scale * step))(
                    jnp.asarray([1.0, 0.5, 0.25, 0.125, 0.0]))
            values = jax.vmap(fn)(proposals)
            return proposals[jnp.argmax(values)], None

        point, _ = jax.lax.scan(_step, _project(start), None,
                                length=int(iterations))
        value = fn(point)
        gradient = grad_fn(point)
        hessian = hess_fn(point)
        curvature = jnp.linalg.eigvalsh(-hessian)
        return point, value, gradient, hessian, curvature

    return jax.lax.map(jax.checkpoint(_one), starts)


def select_refined_modes(points, values, gradients, curvatures, *,
                         max_modes, gradient_tol=1.0e-6,
                         coordinate_tol=(0.25, 1.0e-4, 1.0e-4, 1.0e-5)):
    """Host-side stationarity filter, rank and periodic deduplication.

    A rejected optimizer result is not a missed-mode decision: callers retain
    the original basin in their completeness accounting and must add starts or
    decline if its mass has not independently been bounded.  In particular,
    constrained maxima on the time or distance boundary need a future one-sided
    local region; the full-gradient/positive-curvature filter here rejects them
    and relies on the outside warrant or conservative reserve.
    """
    points = np.asarray(points, dtype=float)
    values = np.asarray(values, dtype=float).ravel()
    gradients = np.asarray(gradients, dtype=float)
    curvatures = np.asarray(curvatures, dtype=float)
    if (points.ndim != 2 or points.shape[1] != 4
            or gradients.shape != points.shape
            or curvatures.shape != points.shape
            or values.shape[0] != points.shape[0]):
        raise ValueError("inconsistent refined-mode arrays")
    stationary = (np.all(np.isfinite(points), axis=1)
                  & np.isfinite(values)
                  & np.all(np.isfinite(gradients), axis=1)
                  & (np.linalg.norm(gradients, axis=1) <= float(gradient_tol))
                  & np.all(curvatures > 0.0, axis=1))
    order = np.flatnonzero(stationary)
    order = order[np.argsort(values[order])[::-1]]
    tolerance = np.asarray(coordinate_tol, dtype=float)
    max_modes = int(max_modes)
    if max_modes < 1:
        raise ValueError("max_modes must be positive")
    selected = []
    for index in order:
        point = points[index]
        duplicate = False
        for old in selected:
            delta = np.abs(point - points[old])
            delta[1] = _periodic_distance(point[1], points[old, 1])
            delta[2] = _periodic_distance(point[2], points[old, 2])
            if np.all(delta <= tolerance):
                duplicate = True
                break
        if not duplicate:
            selected.append(int(index))
    if len(selected) > max_modes:
        raise ValueError(
            "unique stationary mode count exceeds fixed plan capacity")
    selected = np.asarray(selected, dtype=np.int32)
    return selected, stationary


def mode_local_geometry(hessians, *, w_sigma=8.0,
                        eigenvalue_floor=1.0e-12):
    """Return Hessian-whitening transforms and conservative box enclosures.

    If ``L L.T = inv(-H)``, local coordinates are ``theta=center+L z`` with
    ``z`` in a fixed ``[-w_sigma,w_sigma]^4`` cube.  Cholesky's lower-triangular
    form is computationally important: time depends only on ``z[0]``, so exact
    selected-point time reconstruction still needs just ``local_order`` points,
    not ``local_order**4``.  The returned axis-aligned half-width encloses the
    transformed cube and is used for conservative support/overlap checks.
    """
    hessians = np.asarray(hessians, dtype=float)
    if hessians.ndim != 3 or hessians.shape[1:] != (4, 4):
        raise ValueError("hessians must have shape (N,4,4)")
    transforms = np.full_like(hessians, np.nan)
    half_widths = np.full((hessians.shape[0], 4), np.nan)
    for i, H in enumerate(hessians):
        eigenvalue = np.linalg.eigvalsh(-H)
        if (not np.all(np.isfinite(eigenvalue))
                or np.min(eigenvalue) <= float(eigenvalue_floor)):
            continue
        covariance = np.linalg.inv(-H)
        try:
            transform = np.linalg.cholesky(covariance)
        except np.linalg.LinAlgError:
            continue
        transforms[i] = transform
        half_widths[i] = float(w_sigma) * np.sum(np.abs(transform), axis=1)
    return transforms, half_widths


def _periodic_distance(a, b):
    return abs((float(a) - float(b) + np.pi) % (2.0 * np.pi) - np.pi)


def _boxes_disjoint(centers, half_widths):
    for i in range(len(centers)):
        for j in range(i):
            separated = (
                abs(centers[i, 0] - centers[j, 0])
                >= half_widths[i, 0] + half_widths[j, 0]
                or _periodic_distance(centers[i, 1], centers[j, 1])
                >= half_widths[i, 1] + half_widths[j, 1]
                or _periodic_distance(centers[i, 2], centers[j, 2])
                >= half_widths[i, 2] + half_widths[j, 2]
                or abs(centers[i, 3] - centers[j, 3])
                >= half_widths[i, 3] + half_widths[j, 3])
            if not separated:
                return False
    return True


def make_all_axis_mode_plan(centers, *, max_modes, local_transforms,
                            local_radius=1.0,
                            outside_log_bound=np.inf,
                            enumeration_complete=False,
                            outside_bound_certified=False,
                            time_reconstruction_certified=False):
    """Pad a host mode set and freeze its independent acceptance warrants."""
    centers = np.asarray(centers, dtype=float)
    if centers.ndim != 2 or centers.shape[1] != 4:
        raise ValueError("centers must have shape (N,4)")
    local_transforms = np.asarray(local_transforms, dtype=float)
    if local_transforms.shape != (len(centers), 4, 4):
        raise ValueError("local_transforms must have shape (N,4,4)")
    if not (float(local_radius) > 0.0):
        raise ValueError("local_radius must be positive")
    # This enclosure is a theorem for an affine image of a cube, not caller
    # policy: |L z|_i <= radius * sum_j |L_ij|.  Deriving it here prevents an
    # undersized supplied box from blessing overlapping or out-of-support
    # quadrature regions.
    half_widths = (float(local_radius)
                   * np.sum(np.abs(local_transforms), axis=2))
    max_modes = int(max_modes)
    if max_modes < 1 or len(centers) > max_modes:
        raise ValueError("mode count exceeds fixed plan capacity")
    valid = (np.all(np.isfinite(centers), axis=1)
             & np.all(np.isfinite(half_widths) & (half_widths > 0.0), axis=1)
             & np.all(np.isfinite(local_transforms), axis=(1, 2)))
    if not np.all(valid):
        raise ValueError(
            "every supplied mode needs finite, nondegenerate local geometry")
    upper = np.triu(local_transforms, k=1)
    if not np.all(upper == 0.0):
        raise ValueError(
            "local_transforms must be lower triangular; the selected-point "
            "time topology does not evaluate upper-triangle entries")
    if not np.all(np.diagonal(local_transforms, axis1=1, axis2=2) > 0.0):
        raise ValueError("local_transforms must have positive diagonal")
    kept_centers = centers
    kept_widths = half_widths
    kept_transforms = local_transforms
    padded_centers = np.zeros((max_modes, 4), dtype=float)
    padded_widths = np.ones((max_modes, 4), dtype=float)
    padded_transforms = np.repeat(np.eye(4)[None, ...], max_modes, axis=0)
    # ``lax.scan`` traces/evaluates the padded lanes even though their values
    # are masked from the log-sum.  Reuse one finite live geometry so inactive
    # lanes cannot manufacture NaNs (notably log(x) at x<=0) which would leak
    # into outer gradients or Hessians through the masked branch.
    if len(kept_centers):
        padded_centers[:] = kept_centers[0]
        padded_widths[:] = kept_widths[0]
        padded_transforms[:] = kept_transforms[0]
    live = np.zeros(max_modes, dtype=bool)
    padded_centers[:len(kept_centers)] = kept_centers
    padded_widths[:len(kept_widths)] = kept_widths
    padded_transforms[:len(kept_transforms)] = kept_transforms
    live[:len(kept_centers)] = True
    disjoint = _boxes_disjoint(kept_centers, kept_widths)
    return AllAxisModePlan(
        jnp.asarray(padded_centers), jnp.asarray(padded_widths),
        jnp.asarray(padded_transforms), jnp.asarray(float(local_radius)),
        jnp.asarray(live), jnp.asarray(float(outside_log_bound)),
        jnp.asarray(bool(enumeration_complete)),
        jnp.asarray(bool(outside_bound_certified)),
        jnp.asarray(bool(time_reconstruction_certified)),
        jnp.asarray(disjoint))


def _legendre_rule(order):
    nodes, weights = np.polynomial.legendre.leggauss(int(order))
    return (jnp.asarray(nodes, dtype=jnp.float64),
            jnp.asarray(weights, dtype=jnp.float64))


def _mode_integral(coeff, frequency, offset, C_A_shape, C_B, center,
                   transform, radius, nodes, log_weights, concentration):
    if float(concentration) > 0.0:
        scaled = (jnp.sinh(float(concentration) * nodes)
                  / jnp.sinh(float(concentration)))
        log_jacobian_shape = (
            jnp.log(float(concentration))
            + jnp.log(jnp.cosh(float(concentration) * nodes))
            - jnp.log(jnp.sinh(float(concentration))))
    else:
        scaled = nodes
        log_jacobian_shape = jnp.zeros_like(nodes)
    z = radius * scaled
    # Lower-triangular whitening preserves a separable reconstruction topology:
    # t has n points, (t,phi) n^2, (t,phi,u) n^3, and only the final exponent
    # has n^4.  This is the central memory property of the all-axis kernel.
    t = center[0] + transform[0, 0] * z
    phi = jnp.mod(
        center[1] + transform[1, 0] * z[:, None]
        + transform[1, 1] * z[None, :], 2.0 * jnp.pi)
    u = jnp.mod(
        center[2] + transform[2, 0] * z[:, None, None]
        + transform[2, 1] * z[None, :, None]
        + transform[2, 2] * z[None, None, :], 2.0 * jnp.pi)
    x = (center[3] + transform[3, 0] * z[:, None, None, None]
         + transform[3, 1] * z[None, :, None, None]
         + transform[3, 2] * z[None, None, :, None]
         + transform[3, 3] * z[None, None, None, :])

    flat = _evaluate_time_spectrum(coeff, frequency, t, offset)
    C_A = flat.reshape(C_A_shape[:-1] + (nodes.size,))

    kp_a = jnp.arange(C_A.shape[0], dtype=jnp.float64)
    ks_a = jnp.arange(-(C_A.shape[1] - 1) // 2,
                      (C_A.shape[1] - 1) // 2 + 1, dtype=jnp.float64)
    wa = jnp.where(kp_a == 0.0, 1.0, 2.0)
    EA = jnp.exp(1j * (phi[:, :, None, None, None] * kp_a[None, None, None, :, None]
                       + u[:, :, :, None, None] * ks_a[None, None, None, None, :]))
    EA = EA * wa[None, None, None, :, None]
    A = jnp.einsum("tpukq,kqt->tpu", EA, C_A).real

    kp_b = jnp.arange(C_B.shape[0], dtype=jnp.float64)
    ks_b = jnp.arange(-(C_B.shape[1] - 1) // 2,
                      (C_B.shape[1] - 1) // 2 + 1, dtype=jnp.float64)
    wb = jnp.where(kp_b == 0.0, 1.0, 2.0)
    EB = jnp.exp(1j * (phi[:, :, None, None, None] * kp_b[None, None, None, :, None]
                       + u[:, :, :, None, None] * ks_b[None, None, None, None, :]))
    EB = EB * wb[None, None, None, :, None]
    B = jnp.einsum("tpukq,kq->tpu", EB, C_B).real

    exponent = (A[..., None] * x
                - 0.5 * B[..., None] * jnp.square(x)
                - 4.0 * jnp.log(jnp.maximum(x, 1.0e-300)))
    sign, log_det = jnp.linalg.slogdet(transform)
    mapped_log_weights = (log_weights + log_jacobian_shape
                          + jnp.log(radius))
    logw = (mapped_log_weights[:, None, None, None]
            + mapped_log_weights[None, :, None, None]
            + mapped_log_weights[None, None, :, None]
            + mapped_log_weights[None, None, None, :]
            + log_det)
    return jnp.where(sign != 0.0,
                     jax.scipy.special.logsumexp(exponent + logw), -jnp.inf)


def _evaluate_plan_at_order(C_A_t, C_B, plan, order, concentration,
                            time_guard):
    nodes, weights = _legendre_rule(order)
    log_weights = jnp.log(weights)
    coeff, frequency, offset = _time_primitive_spectrum(
        C_A_t.reshape((-1, C_A_t.shape[-1])), int(time_guard))
    model_shape = C_A_t.shape[:-1] + (
        C_A_t.shape[-1] - 2 * int(time_guard),)

    def _step(total, args):
        center, transform, live = args
        def _live_mode(local_args):
            local_center, local_transform = local_args
            return _mode_integral(
                coeff, frequency, offset, model_shape, C_B,
                local_center, local_transform, plan.local_radius, nodes,
                log_weights, concentration)

        value = jax.lax.cond(
            live, _live_mode, lambda _: jnp.asarray(-jnp.inf),
            (center, transform))
        return jnp.logaddexp(total, value), None

    value, _ = jax.lax.scan(
        jax.checkpoint(_step), jnp.asarray(-jnp.inf),
        (plan.centers, plan.local_transforms, plan.live))
    n_live = jnp.count_nonzero(plan.live)
    n_eval = n_live * int(order) ** 4
    # Conservative live-array accounting for one streamed mode.  It is a
    # deterministic shape counter, not a device allocator measurement.
    nt = int(order)
    lanes = int(np.prod(C_A_t.shape[:-1]))
    n_frequency = 2 * C_A_t.shape[-1] - 2
    angle_terms_a = C_A_t.shape[0] * C_A_t.shape[1]
    angle_terms_b = C_B.shape[0] * C_B.shape[1]
    table_bytes = (((nt + lanes) * n_frequency + lanes * nt
                    + nt ** 3 * (angle_terms_a + angle_terms_b)) * 16
                   + C_B.size * 16)
    field_bytes = (n_frequency + 3 * nt ** 3 + nt ** 2
                   + 3 * nt ** 4 + 10 * nt) * 8
    workspace_bytes = jnp.asarray(table_bytes + field_bytes)
    n_selected_time_points = n_live * nt
    n_time_frequency_terms = n_live * nt * n_frequency * lanes
    n_angle_harmonic_terms = (
        n_live * nt ** 3 * (angle_terms_a + angle_terms_b))
    return (value, n_eval, workspace_bytes, n_selected_time_points,
            n_time_frequency_terms, n_angle_harmonic_terms)


def all_axis_peak_local_marginalize(
        C_A_t, C_B, plan, x_min, x_max, *, local_order=5,
        check_order=9, quadrature_tol_nats=1.0e-5,
        outside_tol_nats=-23.0, log_normalization=0.0,
        node_concentration=1.0, time_guard=0,
        time_guard_tol_nats=1.0e-3):
    """Marginalize a padded multi-mode plan with explicit fail-closed ledger.

    ``C_A_t`` is the primitive data table ``(mmax+1,3,Ntime)`` and ``C_B`` is
    the cached ``U,V`` norm table ``(2*mmax+1,5)``.  The returned value is
    diagnostic unless ``ok`` is true.  Here ``ok`` is a validated value-only
    disposition, not a certified quadrature bound or a gradient/Hessian
    certificate.  On any decline the caller must use the
    dense/exact reserve; ``fallback_required`` is provided to make that branch
    hard to omit accidentally.

    With ``time_guard >= 2``, ``C_A_t`` contains support on both sides of the
    target window.  The high-order local integral is repeated after trimming to
    ``time_guard//2`` support and acceptance requires their difference to meet
    ``time_guard_tol_nats``.  A guarded input must pass that comparison and
    cannot be rescued by the plan's external warrant; an unguarded input needs
    the external warrant.  This is an operational convergence validation, not
    a rigorous interpolation-error bound, and the ledger labels it accordingly.
    """
    C_A_t = jnp.asarray(C_A_t, dtype=jnp.complex128)
    C_B = jnp.asarray(C_B, dtype=jnp.complex128)
    _validate_tables(C_A_t, C_B)
    if not isinstance(plan, AllAxisModePlan):
        raise TypeError("plan must be AllAxisModePlan")
    if not (0.0 < float(x_min) < float(x_max)):
        raise ValueError("need 0 < x_min < x_max")
    if int(local_order) < 2 or int(check_order) <= int(local_order):
        raise ValueError("need 2 <= local_order < check_order")
    if float(node_concentration) < 0.0:
        raise ValueError("node_concentration must be non-negative")
    time_guard = int(time_guard)
    n_time = C_A_t.shape[-1] - 2 * time_guard
    if time_guard < 0 or n_time < 2:
        raise ValueError("time_guard must leave at least two integration samples")
    if time_guard == 1:
        raise ValueError("two-guard validation requires time_guard=0 or >=2")
    if float(time_guard_tol_nats) <= 0.0:
        raise ValueError("time_guard_tol_nats must be positive")

    (value_lo, eval_lo, bytes_lo, time_lo,
     time_terms_lo, angle_terms_lo) = _evaluate_plan_at_order(
        C_A_t, C_B, plan, int(local_order), float(node_concentration),
        time_guard)
    (value_hi, eval_hi, bytes_hi, time_hi,
     time_terms_hi, angle_terms_hi) = _evaluate_plan_at_order(
        C_A_t, C_B, plan, int(check_order), float(node_concentration),
        time_guard)
    if time_guard:
        inner_guard = time_guard // 2
        trim = time_guard - inner_guard
        inner_table = C_A_t[..., trim:-trim]
        (value_guard_inner, guard_eval_hi, guard_bytes_hi, guard_time_hi,
         guard_time_terms_hi, guard_angle_terms_hi) = _evaluate_plan_at_order(
            inner_table, C_B, plan, int(check_order),
            float(node_concentration), inner_guard)
        guard_error = jnp.abs(value_hi - value_guard_inner)
        guard_validated = (jnp.isfinite(value_guard_inner)
                           & (guard_error <= float(time_guard_tol_nats)))
    else:
        inner_guard = 0
        value_guard_inner = jnp.asarray(jnp.nan)
        guard_error = jnp.asarray(jnp.inf)
        guard_validated = jnp.asarray(False)
        guard_eval_hi = jnp.asarray(0)
        guard_bytes_hi = jnp.asarray(0)
        guard_time_hi = jnp.asarray(0)
        guard_time_terms_hi = jnp.asarray(0)
        guard_angle_terms_hi = jnp.asarray(0)
    value_lo = value_lo + float(log_normalization)
    value_hi = value_hi + float(log_normalization)
    outside = plan.outside_log_bound + float(log_normalization)

    centers = plan.centers
    widths = plan.half_widths
    inside_time_distance = (
        (centers[:, 0] - widths[:, 0] >= 0.0)
        & (centers[:, 0] + widths[:, 0] <= n_time - 1.0)
        & (centers[:, 3] - widths[:, 3] >= float(x_min))
        & (centers[:, 3] + widths[:, 3] <= float(x_max)))
    angular_single_cover = jnp.all(widths[:, 1:3] <= jnp.pi, axis=1)
    support_ok = jnp.all(jnp.where(
        plan.live, inside_time_distance & angular_single_cover, True))
    finite = (jnp.all(jnp.isfinite(C_A_t.real))
              & jnp.all(jnp.isfinite(C_A_t.imag))
              & jnp.all(jnp.isfinite(C_B.real))
              & jnp.all(jnp.isfinite(C_B.imag))
              & jnp.isfinite(value_hi)
              & jnp.any(plan.live))
    quadrature_error = jnp.abs(value_hi - value_lo)
    quadrature_ok = quadrature_error <= float(quadrature_tol_nats)
    tail_margin = outside - value_hi
    tail_ok = tail_margin < float(outside_tol_nats)
    # The outside-cover certificate is the correctness-bearing completeness
    # warrant.  Requiring the algebraic root report as well would incorrectly
    # reject an otherwise bounded missed root.  Conversely, a perfect root
    # report cannot replace an integral bound outside the local regions.
    # A supplied guarded reconstruction owns its own convergence check.  Do not
    # let a stale external warrant mask a failed outer/inner comparison.
    time_warranted = (guard_validated if time_guard
                       else plan.time_reconstruction_certified)
    cover_warranted = plan.outside_bound_certified & time_warranted

    decline_nonfinite = ~finite
    decline_incomplete = finite & (~plan.outside_bound_certified)
    decline_time_reconstruction = (
        finite & plan.outside_bound_certified
        & (~time_warranted))
    decline_overlap = finite & cover_warranted & (~plan.boxes_disjoint)
    decline_support = (finite & cover_warranted & plan.boxes_disjoint
                       & (~support_ok))
    decline_quadrature = (finite & cover_warranted & plan.boxes_disjoint & support_ok
                          & (~quadrature_ok))
    decline_tail = (finite & cover_warranted & plan.boxes_disjoint & support_ok
                    & quadrature_ok & (~tail_ok))
    ok = (finite & cover_warranted & plan.boxes_disjoint & support_ok
          & quadrature_ok & tail_ok)
    reconciles = (ok.astype(jnp.int32)
                  + decline_nonfinite.astype(jnp.int32)
                  + decline_incomplete.astype(jnp.int32)
                  + decline_time_reconstruction.astype(jnp.int32)
                  + decline_overlap.astype(jnp.int32)
                  + decline_support.astype(jnp.int32)
                  + decline_quadrature.astype(jnp.int32)
                  + decline_tail.astype(jnp.int32)) == 1
    ledger = {
        "accepted": ok,
        "fallback_required": ~ok,
        "decline_is_waveform_failure": jnp.asarray(False),
        "fixed_plan_autodiff_only": jnp.asarray(True),
        "derivative_warrant_certified": jnp.asarray(False),
        "decline_nonfinite": decline_nonfinite,
        "decline_incomplete": decline_incomplete,
        "decline_time_reconstruction": decline_time_reconstruction,
        "decline_overlap": decline_overlap,
        "decline_support": decline_support,
        "decline_quadrature": decline_quadrature,
        "decline_tail": decline_tail,
        "reconciles": reconciles,
        "enumeration_complete": plan.enumeration_complete,
        "outside_bound_certified": plan.outside_bound_certified,
        "time_reconstruction_certified": plan.time_reconstruction_certified,
        "time_guard_validated": guard_validated,
        "time_reconstruction_warranted": time_warranted,
        "time_guard_error_certified": jnp.asarray(False),
        "time_guard": jnp.asarray(time_guard),
        "time_guard_inner": jnp.asarray(inner_guard),
        "time_guard_error": guard_error,
        "time_guard_tol_nats": jnp.asarray(float(time_guard_tol_nats)),
        "time_guard_inner_value": value_guard_inner + float(log_normalization),
        "boxes_disjoint": plan.boxes_disjoint,
        "support_ok": support_ok,
        "quadrature_ok": quadrature_ok,
        "quadrature_error_certified": jnp.asarray(False),
        "value_warrant_certified": jnp.asarray(False),
        "tail_ok": tail_ok,
        "quadrature_error": quadrature_error,
        "tail_margin": tail_margin,
        "outside_log_bound": outside,
        "n_modes": jnp.count_nonzero(plan.live),
        "n_mode_capacity": jnp.asarray(plan.live.size),
        "n_local_evaluations_lo": eval_lo,
        "n_local_evaluations_hi": eval_hi,
        "n_guard_local_evaluations_hi": guard_eval_hi,
        "n_total_local_evaluations_hi": eval_hi + guard_eval_hi,
        "n_selected_time_points_lo": time_lo,
        "n_selected_time_points_hi": time_hi,
        "n_guard_selected_time_points_hi": guard_time_hi,
        "n_total_selected_time_points_hi": time_hi + guard_time_hi,
        "n_time_frequency_terms_lo": time_terms_lo,
        "n_time_frequency_terms_hi": time_terms_hi,
        "n_guard_time_frequency_terms_hi": guard_time_terms_hi,
        "n_total_time_frequency_terms_hi": time_terms_hi + guard_time_terms_hi,
        "n_angle_harmonic_terms_lo": angle_terms_lo,
        "n_angle_harmonic_terms_hi": angle_terms_hi,
        "n_guard_angle_harmonic_terms_hi": guard_angle_terms_hi,
        "n_total_angle_harmonic_terms_hi": angle_terms_hi + guard_angle_terms_hi,
        "workspace_bytes_lo": bytes_lo,
        "workspace_bytes_hi": bytes_hi,
        "workspace_bytes_guard_hi": guard_bytes_hi,
        "workspace_bytes_peak_bound_hi": jnp.maximum(bytes_hi, guard_bytes_hi),
    }
    return value_hi, ok, ledger
