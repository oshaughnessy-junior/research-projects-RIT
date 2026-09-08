"""Opt-in cross-axis direct-marginalization policy for the JAX ILE arm.

``--direct-marginalization-policy auto`` composes, per likelihood evaluation,
the four-axis peak-local controller of :mod:`all_axis_peaklocal` with the
established exact-angle reserve:

1. build the guarded coefficient tables once (the same U,V/Q contraction the
   exact scheme uses), collapse the time-independent norm table per row;
2. rank a base and an enriched U,V/Q start portfolio on device, refine both in
   one shared optimizer pass, and freeze two nested fixed-shape plans;
3. attempt the four-axis local integral over ``(t, phi_ref, u=2 psi, x)``
   under the empirical enrichment gate;
4. on any decline, execute the band-limited exact-angle reserve on a refined
   time rule, warranted by the two-guard comparison and a half-refined check
   rule evaluated in the same declined branch;
5. return the selected value per row with the complete acceptance ledger.

No SNR threshold is coded.  Which branch runs is decided by the diagnostics
listed in :func:`policy_acceptance_diagnostics`.  A local decline is never a
waveform failure.  A reserve that fails its own warrant is escalated once per
doubling of the time rule up to ``reserve_time_refine_max``; a row that is
still unwarranted, or whose norm table varies with time, returns ``nan``.
The driver refuses to publish a run containing such rows.  The ledger keeps
the finite diagnostic value under ``selected_value`` for the record; it is
never handed to the sampler.

Measures.  The local branch integrates ``x**-4 dx  dt_sample  dphi  du``.
The reserve, and the production exact scheme it must agree with, average the
two angles (``dphi/2pi``, ``dpsi/pi``), weight distance by the normalized
``log_w_grid`` (or the normalized volumetric measure under
``JAX_ILE_DISTMARG_GH``), and integrate time in seconds with Simpson weights.
:func:`policy_log_normalization` derives the constant that converts the local
measure to that convention; it is a derivation from the prior's stated form,
never an inferred number, and it refuses any prior it cannot derive.

Not claimed here: derivative accuracy.  The plans are frozen under
``stop_gradient``; differentiating the composite differentiates the truncated
fixed-plan local integral or the reserve.  Value and gradient parity through
an SNR/HM ladder is the gate before this policy can become a default.
"""

from typing import NamedTuple

import jax
import jax.numpy as jnp
import numpy as np

from . import all_axis_peaklocal as _aap
from . import anglemarg as _anglemarg
from . import core as _core

__all__ = [
    "POLICY_CHOICES",
    "POLICY_DEFAULT",
    "PolicyConfig",
    "validate_policy_request",
    "policy_log_normalization",
    "policy_time_rules",
    "policy_acceptance_diagnostics",
    "fused_log_likelihood_four_axis_policy",
    "summarize_policy_ledger",
]

POLICY_CHOICES = ("off", "auto")
POLICY_DEFAULT = "off"

# The controller's own decline reasons, in the order they gate acceptance.
# Every key is a boolean per row in the returned ledger.
_DECLINE_KEYS = (
    "decline_nonfinite",
    "decline_capacity",
    "decline_no_modes",
    "decline_mode_nesting",
    "decline_time_reconstruction",
    "decline_time_cover_incomplete",
    "decline_time_omitted_mass_bound",
    "decline_time_omitted_mass",
    "decline_geometry",
    "decline_quadrature",
    "decline_enrichment",
    "decline_error_budget",
)


class PolicyConfig(NamedTuple):
    """Operating point of the composite.

    These are the values PR #268's device composition test and real captures
    ran with, exposed so the validation ladder can move them.  They are not a
    measured production operating point yet.
    """

    time_guard: int = 16
    reserve_time_refine: int = 4
    # Bounded escalation of the reserve rule on a failed warrant: the rule is
    # doubled (and re-checked against its own half) until it is warranted or
    # this factor is reached.  Rows still unwarranted return nan.
    reserve_time_refine_max: int = 32
    base_max_starts: int = 32
    base_oversample: int = 1
    enriched_oversample: int = 2
    max_modes: int = 4
    enriched_max_modes: int = 8
    # 6 whitened sigmas, the library default.  PR #268's composition test used
    # 3.0; on the wiring test's analytic fixture that truncated ~1% of the
    # four-dimensional mass and read 0.015 nat LOW against an independent
    # fine-time reference while the gate accepted, because base and enriched
    # plans share the truncation.  The gate cannot see this term; the wiring
    # test pins it against the external reference instead.
    local_radius: float = 6.0
    refine_iterations: int = 14
    base_order: int = 13
    base_check_order: int = 19
    enriched_order: int = 19
    enriched_check_order: int = 25
    convergence_tol_nats: float = 1.0e-3
    time_guard_tol_nats: float = 1.0e-3
    total_value_error_budget_nats: float = 1.0e-3
    time_outside_tol_nats: float = -23.0
    reserve_dense_chunk: int = 8
    reserve_grid_block: int = 32
    # Rows the controller executes together under one ``vmap``.  1 is the
    # row-at-a-time path (``lax.map`` with no ``batch_size``), whose reserve
    # workspace is one row's.  Above 1, ``B`` rows share a scan step, peak
    # device memory scales with ``B`` times one row's reserve workspace, and
    # the tier-escalation ``lax.cond`` inside the controller becomes a
    # ``select`` that evaluates EVERY tier for EVERY row in the batch -- so
    # arithmetic per row rises with the tier count while wall time falls with
    # occupancy.  Values, branch decisions and gradients are unchanged; only
    # cost is.  0 means one full batch of all rows.
    reserve_batch_rows: int = 1
    norm_invariance_rtol: float = 1.0e-10


def validate_batch_rows(batch_rows):
    """Refuse a row-batch size the controller cannot execute.

    Returns the integer.  Negative sizes and non-integers are refused here so
    the driver and the library agree on the same rule.
    """
    try:
        b = int(batch_rows)
    except (TypeError, ValueError):
        raise ValueError("PolicyConfig.reserve_batch_rows must be an integer, "
                         "got %r" % (batch_rows,))
    if b != batch_rows:
        raise ValueError("PolicyConfig.reserve_batch_rows must be an integer, "
                         "got %r" % (batch_rows,))
    if b < 0:
        raise ValueError("PolicyConfig.reserve_batch_rows must be >= 0 "
                         "(1 = row at a time, 0 = one full batch), got %d" % b)
    return b


def validate_policy_request(policy, *, angle_marg_scheme, time_quadrature,
                            d_prior, dist_grid):
    """Refuse every combination the composite cannot honour.

    Refusal is explicit because an ignored request on this arm has a history
    of reading as a result (see the ``--angle-marg-scheme`` notes).
    """
    if policy not in POLICY_CHOICES:
        raise ValueError("direct_marginalization_policy must be one of %r, "
                         "got %r" % (POLICY_CHOICES, policy))
    if policy == "off":
        return
    if angle_marg_scheme != "exact":
        raise ValueError(
            "--direct-marginalization-policy auto needs the exact-angle "
            "reserve: the resolved --angle-marg-scheme is %r, and this policy "
            "does not compose with grid, laplace, peak-local or phi-local.  "
            "Use --angle-marg-scheme exact." % (angle_marg_scheme,))
    if time_quadrature != "simpson":
        raise ValueError(
            "--direct-marginalization-policy auto owns the time integral "
            "(local four-axis or refined band-limited reserve) and only "
            "composes with the simpson terminal rule as its check rule; "
            "got %r." % (time_quadrature,))
    if d_prior not in ("euclidean", "volumetric"):
        raise ValueError(
            "--direct-marginalization-policy auto derives its local measure "
            "from the volumetric distance prior p(d) ~ d^2 only; got %r.  "
            "The cosmological prior is out of scope for the composite."
            % (d_prior,))
    if dist_grid != "uniform":
        raise ValueError(
            "--direct-marginalization-policy auto reads the reserve's distance "
            "normalization off a uniform-in-d grid; --distance-grid-scheme %r "
            "is not supported by the composite." % (dist_grid,))


def policy_log_normalization(data, x_grid, log_w_grid, *, d_prior="euclidean",
                             gh_nodes=None):
    """Constant converting the local ``x**-4 dx dt_sample dphi du`` integral to
    the reserve convention.  Returns ``(local_log_normalization, info)``.

    * angles: the reserve averages, so ``-2 log(2 pi)``;
    * time: sample units to seconds, scaled by whatever constant the
      production Simpson weights carry (``sum w_t == (npts-1) deltaT`` for the
      plain rule; the ratio is measured rather than assumed);
    * distance, fixed grid: ``p(d) dd = d^2 dd / N`` with
      ``d = Dref/x`` gives ``Dref^3 x^-4 dx / N``; ``N`` is recovered from the
      first weight, ``N = d_0^2 |d_1 - d_0| / w_0``, which is exact for the
      uniform grid the request validator requires;
    * distance, adaptive GH: the built-in normalized volumetric measure,
      ``3 / (x_min^-3 - x_max^-3)``.
    """
    if d_prior not in ("euclidean", "volumetric"):
        raise ValueError("policy_log_normalization supports the volumetric "
                         "prior only, got %r" % (d_prior,))
    x = np.asarray(x_grid, dtype=float)
    lw = np.asarray(log_w_grid, dtype=float)
    if x.ndim != 1 or x.size < 2 or lw.shape != x.shape:
        raise ValueError("x_grid/log_w_grid must be matching 1-D grids")
    if gh_nodes is None:
        gh_nodes = int(_core._DISTMARG_GH_N)
    deltaT = float(data.deltaT)
    npts = int(data.npts)
    w_t = np.asarray(data.w_t, dtype=float)
    plain = (npts - 1) * deltaT
    time_scale = float(np.sum(w_t)) / plain
    log_time = np.log(deltaT) + np.log(time_scale)
    log_angles = -2.0 * np.log(2.0 * np.pi)
    if int(gh_nodes) > 0:
        x_min, x_max = float(np.min(x)), float(np.max(x))
        log_dist = np.log(3.0) - np.log(x_min ** -3 - x_max ** -3)
        dist_mode = "gh-volumetric"
    else:
        dref = float(data.distMpcRef)
        d = dref / x
        dd = np.abs(d[1] - d[0])
        if not np.allclose(np.abs(np.diff(d)), dd, rtol=1.0e-8, atol=0.0):
            raise ValueError("policy_log_normalization needs a uniform-in-d "
                             "distance grid")
        norm = d[0] ** 2 * dd / np.exp(lw[0])
        log_dist = 3.0 * np.log(dref) - np.log(norm)
        dist_mode = "fixed-grid-volumetric"
    total = float(log_angles + log_time + log_dist)
    info = dict(log_angles=float(log_angles), log_time=float(log_time),
                log_distance=float(log_dist), distance_mode=dist_mode,
                time_weight_scale=float(time_scale),
                local_log_normalization=total)
    return total, info


def _refined_rule(npts, deltaT, refine, scale):
    """Trapezoid rule on the ``refine``-times finer grid, in seconds.

    Trapezoid, not Simpson: on a peak narrower than the node spacing Simpson's
    alternating weights alias at half the spacing (review of PR #278 measured
    0.03 to 1.8 nat at refine 4 for peaks of 0.05 to 0.2 native samples), while
    the trapezoid rule converges exponentially on a smooth peak as the spacing
    shrinks, so a passed half-rule check means what it says.
    """
    n_nodes = (npts - 1) * refine + 1
    h = deltaT / float(refine)
    nodes = np.arange(n_nodes, dtype=float) / float(refine)
    nodes[-1] = float(npts - 1)
    weights = np.full(n_nodes, h)
    weights[0] = weights[-1] = 0.5 * h
    return nodes, weights * scale


def policy_time_rules(data, refine):
    """Refined reserve rule and its coarser check rule on the target window.

    Positions are in native samples of the unguarded window, ``0 .. npts-1``.
    Weights are trapezoid weights in seconds, carrying the same constant as
    the production ``data.w_t`` (so the reserve lands in production units
    without a separate offset).  The reserve rule refines the native cadence ``refine``
    times; the check rule refines it ``refine/2`` times (the native production
    rule itself when ``refine == 2``).  Agreement between the two is the
    resolution warrant, so the warrant is a convergence statement about the
    refined rules and does not require the native rule to be converged.
    """
    refine = int(refine)
    if refine < 2 or refine % 2:
        raise ValueError("reserve_time_refine must be an even integer >= 2 "
                         "so the check rule is the half-refined rule")
    npts = int(data.npts)
    deltaT = float(data.deltaT)
    w_t = np.asarray(data.w_t, dtype=float)
    scale = float(np.sum(w_t)) / ((npts - 1) * deltaT)
    nodes, weights = _refined_rule(npts, deltaT, refine, scale)
    if refine == 2:
        check_nodes, check_weights = np.arange(npts, dtype=float), w_t
    else:
        check_nodes, check_weights = _refined_rule(
            npts, deltaT, refine // 2, scale)
    return (jnp.asarray(nodes), jnp.asarray(weights),
            jnp.asarray(check_nodes), jnp.asarray(check_weights))


def policy_acceptance_diagnostics():
    """Names of the per-row booleans that must all hold for local acceptance,
    then the reserve warrant flags.  Documentation and audit order only."""
    return dict(
        local=("norm_time_invariant", "base_capacity_ok",
               "enriched_capacity_ok", "base_and_enriched_values_finite",
               "mode_nesting_ok", "geometry_nesting_ok",
               "time_omitted_mass_ok", "value_error_budget_ok",
               "accepted_local"),
        reserve=("reserve_executed", "reserve_finite",
                 "reserve_time_guard_validated",
                 "reserve_time_resolution_validated",
                 "reserve_time_error_budget_ok", "reserve_time_warranted"),
        declines=_DECLINE_KEYS)


def _strong(tree):
    """Strip weak types so both branches of a ``lax.cond`` agree."""
    return jax.tree.map(
        lambda x: jax.lax.convert_element_type(jnp.asarray(x),
                                               jnp.asarray(x).dtype), tree)


def fused_log_likelihood_four_axis_policy(
        data, ra, dec, incl, x_grid, log_w_grid, *, interp, amp_sizing,
        config=None, local_log_normalization=None, return_ledger=False):
    """Distance-, phi_ref-, psi- AND time-marginalized lnL under the policy.

    Same contract as :func:`anglemarg.fused_log_likelihood_distphipsimarg_exact`
    without ``return_lnLt``: the composite owns the time integral, so there is
    no ``lnL(t)`` to hand back.  With ``return_ledger`` the per-row ledger of
    the controller is returned alongside (every leaf shaped ``(S,)``).
    """
    if config is None:
        config = PolicyConfig()
    guard = int(config.time_guard)
    batch_rows = validate_batch_rows(config.reserve_batch_rows)
    if guard < 2:
        raise ValueError("PolicyConfig.time_guard must be >= 2: the local "
                         "path and the reserve both need the two-guard "
                         "comparison")
    if local_log_normalization is None:
        local_log_normalization, _ = policy_log_normalization(
            data, x_grid, log_w_grid)
    x_grid = jnp.asarray(x_grid, dtype=jnp.float64)
    log_w_grid = jnp.asarray(log_w_grid, dtype=jnp.float64)
    x_min = float(np.min(np.asarray(x_grid)))
    x_max = float(np.max(np.asarray(x_grid)))
    nodes, weights, check_nodes, check_weights = policy_time_rules(
        data, config.reserve_time_refine)

    C_A, C_B, meta = _anglemarg.angle_coefficient_tables(
        data, ra, dec, incl, interp, guard=guard)
    # (KP,KS,S,Ntime) -> (S,KP,KS,Ntime): one row per extrinsic sample.
    rows_A = jnp.moveaxis(C_A, 2, 0)
    rows_B = jnp.moveaxis(C_B, 2, 0)
    # Ordinary ILE has an arrival-time-independent norm.  Collapse it per row
    # and record the deviation; a row whose norm moves with time cannot be
    # planned by this composite and is reported, never averaged away.
    norm0 = rows_B[..., 0]
    norm_dev = jnp.max(jnp.abs(rows_B - norm0[..., None]), axis=(1, 2, 3))
    norm_scale = jnp.maximum(1.0, jnp.max(jnp.abs(norm0), axis=(1, 2)))
    norm_time_invariant = norm_dev <= float(config.norm_invariance_rtol) * norm_scale

    def _plan_row(table, norm):
        base = _aap.rank_joint_starts_from_uvq_device(
            table, norm, x_min, x_max, time_guard=guard,
            max_starts=int(config.base_max_starts),
            angular_oversample=int(config.base_oversample))
        extra = _aap.rank_joint_starts_from_uvq_device(
            table, norm, x_min, x_max, time_guard=guard,
            max_starts=int(config.base_max_starts),
            angular_oversample=int(config.enriched_oversample))
        (base_plan, enriched_plan, base_planning, enriched_planning,
         shared_planning) = _aap.make_all_axis_mode_plan_pair_device(
            table, norm, base, extra, x_min, x_max,
            max_modes=int(config.max_modes),
            enriched_max_modes=int(config.enriched_max_modes),
            local_radius=float(config.local_radius),
            time_guard=guard, iterations=int(config.refine_iterations),
            time_reconstruction_certified=False)
        # Row-local control data.  Until derivative parity is established the
        # discrete rank/dedup decisions are not part of the differentiated
        # graph (PR #268's own composition test does the same).
        base_plan = jax.tree.map(jax.lax.stop_gradient, base_plan)
        enriched_plan = jax.tree.map(jax.lax.stop_gradient, enriched_plan)
        planning = dict(
            base_n_selected_modes=base_planning["n_selected_modes"],
            enriched_n_selected_modes=enriched_planning["n_selected_modes"],
            base_n_optimizer_starts=base_planning["n_optimizer_starts"],
            enriched_n_optimizer_starts=enriched_planning["n_optimizer_starts"],
            optimizer_starts_executed=shared_planning[
                "n_optimizer_starts_executed"],
            base_n_lattice_evaluations=base_planning["n_lattice_evaluations"],
            enriched_n_lattice_evaluations=enriched_planning[
                "n_lattice_evaluations"])
        return base_plan, enriched_plan, planning

    base_plans, enriched_plans, planning = jax.vmap(_plan_row)(rows_A, norm0)

    refine0 = int(config.reserve_time_refine)
    refine_max = int(config.reserve_time_refine_max)
    if refine_max < refine0:
        raise ValueError("reserve_time_refine_max must be >= reserve_time_refine")
    tiers = []
    f = refine0
    while f <= refine_max:
        tiers.append((f,) + tuple(policy_time_rules(data, f)))
        f *= 2

    def _controller(table, norm, base_plan, enriched_plan, tier):
        refine, nodes, weights, check_nodes, check_weights = tier
        sel, ok, led = _aap.empirical_enrichment_with_exact_reserve(
            table, norm, base_plan, enriched_plan, x_min, x_max,
            reserve_x_grid=x_grid, reserve_log_weights=log_w_grid,
            time_weights=weights,
            reserve_amp_sizing=float(amp_sizing),
            reserve_m_max=int(meta["m_max"]),
            reserve_dense_chunk=int(config.reserve_dense_chunk),
            reserve_grid_block=int(config.reserve_grid_block),
            reserve_time_nodes=nodes,
            reserve_time_check_nodes=check_nodes,
            reserve_time_check_weights=check_weights,
            reserve_time_resolution_tol_nats=float(
                config.total_value_error_budget_nats),
            base_order=int(config.base_order),
            base_check_order=int(config.base_check_order),
            enriched_order=int(config.enriched_order),
            enriched_check_order=int(config.enriched_check_order),
            convergence_tol_nats=float(config.convergence_tol_nats),
            time_guard=guard,
            time_guard_tol_nats=float(config.time_guard_tol_nats),
            local_log_normalization=float(local_log_normalization),
            time_outside_tol_nats=float(config.time_outside_tol_nats),
            total_value_error_budget_nats=float(
                config.total_value_error_budget_nats),
            reserve_log_offset=0.0)
        led = dict(led)
        led["reserve_time_refine_used"] = jnp.asarray(refine)
        return _strong((sel, ok, led))

    def _row(args):
        table, norm, base_plan, enriched_plan = args
        state = _controller(table, norm, base_plan, enriched_plan, tiers[0])
        escalations = jnp.asarray(0)
        for tier in tiers[1:]:
            sel, ok, led = state
            need = (led["reserve_executed"] & led["reserve_finite"]
                    & (~led["reserve_time_warranted"]))
            state = jax.lax.cond(
                need,
                lambda _: _controller(table, norm, base_plan, enriched_plan,
                                      tier),
                lambda st: st, state)
            escalations = escalations + need.astype(escalations.dtype)
        sel, ok, led = state
        led = dict(led)
        led["reserve_escalations"] = escalations
        return sel, ok, led

    n_rows = int(rows_A.shape[0])
    xs = (rows_A, norm0, base_plans, enriched_plans)
    if batch_rows == 1:
        # Row at a time.  Kept as a distinct call rather than batch_size=1 so
        # the graph is the one PR #268 measured: batch_size=1 would still wrap
        # the body in a vmap, paying the cond-to-select cost for no occupancy.
        selected, usable, ledger = jax.lax.map(_row, xs)
    elif batch_rows == 0 or batch_rows >= n_rows:
        # One full batch.  Spelled as an explicit vmap rather than delegated
        # to batch_size: jax 0.9.2 documents batch_size=0 as a full vmap, but
        # the IGWN environment's jax 0.7.1 computes n // batch_size first and
        # raises ZeroDivisionError, and a batch_size above the row count is a
        # zero-length scan plus a remainder in both.  The explicit vmap is the
        # same computation in every version.
        selected, usable, ledger = jax.vmap(_row)(xs)
    else:
        selected, usable, ledger = jax.lax.map(_row, xs,
                                               batch_size=batch_rows)
    ledger = dict(ledger)
    # Truthful, not decorative: this key read True unconditionally before the
    # batch size was a knob.  A row is executed sequentially only when the
    # requested batch is 1; at any other size no row is guaranteed to be, and
    # lax.map's trailing remainder means the batch a given row landed in is
    # not recoverable per row -- so the requested size is recorded alongside.
    ledger["reserve_batch_execution_sequential"] = jnp.full(
        (n_rows,), batch_rows == 1, dtype=bool)
    ledger["reserve_batch_rows_requested"] = jnp.full(
        (n_rows,), batch_rows if batch_rows else n_rows, dtype=jnp.int32)
    usable = usable & norm_time_invariant
    # Fail closed: a value the controller could not warrant is not a
    # likelihood.  nan, never the finite diagnostic, reaches the sampler; the
    # driver refuses to publish a run that contains such rows.
    lnL = jnp.where(usable, selected, jnp.nan)
    ledger.update(planning)
    ledger["norm_time_invariant"] = norm_time_invariant
    ledger["norm_time_deviation"] = norm_dev
    ledger["usable"] = usable
    ledger["selected_value"] = selected
    ledger["lnL"] = lnL
    if return_ledger:
        return lnL, ledger
    return lnL


def summarize_policy_ledger(ledger):
    """Host-side counts for the run record.  ``ledger`` leaves are ``(S,)``."""
    def _count(key):
        return int(np.sum(np.asarray(ledger[key], dtype=bool)))
    n = int(np.asarray(ledger["usable"]).shape[0])
    out = dict(
        rows=n,
        accepted_local=_count("accepted_local"),
        reserve_executed=_count("reserve_executed"),
        reserve_warranted=_count("selected_value_is_warranted_reserve"),
        usable=_count("usable"),
        unusable=n - _count("usable"),
        norm_time_invariant=_count("norm_time_invariant"),
        reconciles=_count("reconciles"),
        disposition_reconciles=_count("disposition_reconciles"),
    )
    declines = {}
    for key in _DECLINE_KEYS:
        if key in ledger:
            c = _count(key)
            if c:
                declines[key] = c
    out["declines"] = declines
    if "reserve_escalations" in ledger:
        out["reserve_escalations"] = int(np.sum(
            np.asarray(ledger["reserve_escalations"])))
    if "reserve_batch_rows_requested" in ledger:
        req = np.asarray(ledger["reserve_batch_rows_requested"])
        out["reserve_batch_rows"] = int(req[0]) if req.size else 0
        out["reserve_batch_execution_sequential"] = bool(np.all(
            np.asarray(ledger["reserve_batch_execution_sequential"],
                       dtype=bool)))
    if "lnL" in ledger:
        out["nan_rows"] = int(np.sum(~np.isfinite(
            np.asarray(ledger["lnL"], dtype=float))))
    score = np.asarray(ledger["empirical_value_error_score_nats"], dtype=float)
    finite = score[np.isfinite(score)]
    out["max_local_error_score_nats"] = (
        float(np.max(finite)) if finite.size else float("nan"))
    return out
