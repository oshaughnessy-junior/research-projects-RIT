"""Registry of the peak-local marginalization kernels, and what each one localizes.

WHY THIS MODULE EXISTS.  Eight distinct kernels in this package are called
"peak-local".  Two of them are selectable by that exact word on two different
command-line flags, and they localize different axes.  On 2026-09-08 a profiling
session measured ``--angle-marg-scheme peak-local`` and published the result as a
claim about the four-axis branch of ``--direct-marginalization-policy auto``.  The
two answers were opposite: the angle scheme exhausted a 24 GiB card at rungs 160
and 640, while the four-axis branch runs those rungs at 0.146 s per selected call
(RIFT_roboto_paper analyses/multipeak_snr_ladder/NOTE_01_COMBINED_METHOD_LADDER.md).

This module is the single place that says which kernel a name refers to.  Every log
line, ledger field and run record that names a kernel takes the string from here, so
a reader of a run's own output does not need this history.  It is pure data with no
numpy, jax or RIFT imports, so any module may import it.

See RIFT/likelihood/DESIGN_peak_local_framework.md, section "The eight shipped
instances", for the axis contract each kernel satisfies.
"""

from collections import OrderedDict
from typing import NamedTuple, Optional, Tuple

__all__ = [
    "KernelIdentity",
    "KERNELS",
    "kernel",
    "describe",
    "label",
    "ANGLE_MARG_KERNEL",
    "ANGLE_MARG_SPELLINGS",
    "angle_marg_kernel_id",
    "canonical_angle_marg_scheme",
    "FOUR_AXIS_LOCAL",
    "AMBIGUOUS_NAMES",
]


class KernelIdentity(NamedTuple):
    """What one peak-local kernel localizes, what selects it, and where it lives.

    ``localized`` and ``dense`` are the axes of the extrinsic integral
    ``(t, phi_ref, psi, D)``.  An axis in neither tuple is not integrated by that
    kernel.  ``selector`` is the command line that reaches it, or None when the
    kernel is a library or a diagnostic seam that no driver flag selects.
    ``reachable_from_auto`` answers the question that the 2026-09-08 error turned
    on: whether a run that set no scheme by name can execute this kernel.
    """

    kernel_id: str
    module: str
    entry: str
    localized: Tuple[str, ...]
    dense: Tuple[str, ...]
    selector: Optional[str]
    reachable_from_auto: bool
    memory_model: str
    status: str
    #: Free text for an axis fact the two tuples cannot carry, e.g. a library whose
    #: rules differ on one axis.  Empty for a kernel whose axes are fully described.
    axis_note: str = ""


#: Every peak-local kernel in the package, keyed by an id that names its axes.
#: The ids are the strings that go into logs and run records.  Legacy spellings
#: stay accepted on the command line (see ANGLE_MARG_SPELLINGS) but are never
#: emitted alone.
KERNELS = OrderedDict((k.kernel_id, k) for k in (
    KernelIdentity(
        kernel_id="psi_local_phi_dense",
        module="RIFT.likelihood.jax_ile.anglemarg",
        entry="fused_log_likelihood_distphipsimarg_psi_local_phi_dense",
        localized=("psi",),
        dense=("phi_ref", "D", "t"),
        selector="--angle-marg-scheme peak-local",
        reachable_from_auto=False,
        memory_model=("samplers._peaklocal_bytes_per_sample_pt: a streamed "
                      "(phi_chunk, n_x, 4, u_block) body plus the phi scan's "
                      "stacked (n_phi, n_x) output, per sample per time point.  "
                      "n_phi grows as sqrt(amplitude), so the stacked term grows "
                      "with SNR."),
        status="shipped, explicit-only"),
    KernelIdentity(
        kernel_id="psi_local_phi_local",
        module="RIFT.likelihood.jax_ile.anglemarg",
        entry="fused_log_likelihood_distphipsimarg_psi_local_phi_local",
        localized=("psi", "phi_ref"),
        dense=("D", "t"),
        selector="--angle-marg-scheme phi-local",
        reachable_from_auto=False,
        memory_model=("samplers._philocal_bytes_per_sample_pt ADDED to the "
                      "psi_local_phi_dense model, because this entry evaluates "
                      "the dense kernel as its fallback in the same trace."),
        status="shipped, explicit-only"),
    KernelIdentity(
        kernel_id="phi_psi_cell_kernel_jax",
        module="RIFT.likelihood.jax_ile.joint_anglemarg_peaklocal",
        entry="joint_lnL_phi_dense, joint_lnL_phi_local",
        # THE AXES COMMON TO BOTH RULES, and no more.  This entry offers two:
        # joint_lnL_phi_dense localizes psi with phi dense, joint_lnL_phi_local
        # localizes both.  An earlier revision declared dense=("phi_ref",), which is
        # a false claim about the second rule -- caught by internal review, because
        # describe() then emitted it.  psi is localized by both; D is dense in both.
        # The phi axis differs between the rules, so it appears in neither tuple and
        # is stated in axis_note instead.
        localized=("psi",),
        dense=("D",),
        axis_note=("two rules: joint_lnL_phi_dense keeps phi_ref dense, "
                   "joint_lnL_phi_local localizes it"),
        selector=None,
        reachable_from_auto=False,
        memory_model=("bounded by phi_chunk and U_NODE_STREAM_CHUNK; the callers "
                      "above own the per-sample budget."),
        status="library for the two anglemarg entries"),
    KernelIdentity(
        kernel_id="phi_psi_cell_kernel_numpy",
        module="RIFT.likelihood.joint_angle_peak_local",
        entry="joint_marginalize_peak_local",
        localized=("psi", "phi_ref"),
        dense=(),
        selector=None,
        reachable_from_auto=False,
        memory_model="host numpy; not sized for production batches.",
        status="numpy reference, not wired to any driver"),
    KernelIdentity(
        kernel_id="four_axis_local",
        module="RIFT.likelihood.jax_ile.all_axis_peaklocal",
        entry="empirical_enrichment_with_exact_reserve",
        localized=("t", "phi_ref", "psi", "D"),
        dense=(),
        selector="--direct-marginalization-policy auto",
        reachable_from_auto=True,
        memory_model=("O(local_order**4) per mode, streamed through lax.scan and "
                      "independent of the dense time, angle and distance "
                      "resolutions."),
        status="shipped, opt-in; the accepted branch of the policy controller"),
    KernelIdentity(
        kernel_id="four_axis_local_diagnostic",
        module="RIFT.likelihood.jax_ile.multipeak_planner",
        entry="MultiPeakResult",
        localized=("t", "phi_ref", "psi", "D"),
        dense=(),
        selector=None,
        reachable_from_auto=False,
        memory_model="host planner; the frozen hierarchical cover is diagnostic.",
        status="diagnostic seam, not wired to any driver"),
    KernelIdentity(
        kernel_id="time_local_jax",
        module="RIFT.likelihood.jax_ile.time_first_peaklocal",
        entry="time_first_peak_local_marginalize",
        localized=("t",),
        dense=(),
        selector=None,
        reachable_from_auto=False,
        memory_model="fixed-shape cell cover; caller owns the downstream lanes.",
        status=("prototype; four_axis_local imports its spectrum helpers, not "
                "its marginalizer")),
    KernelIdentity(
        kernel_id="time_local_numpy",
        module="RIFT.likelihood.time_marginalization_peak_local",
        entry="time_marginalize_peak_local",
        localized=("t",),
        dense=(),
        selector="--time-marginalization-quadrature peak-local",
        reachable_from_auto=False,
        memory_model="chunked over rows; dense fallback per unaccepted row.",
        status=("shipped, opt-in, on the numpy/cupy ILE arm.  Refuses phase "
                "marginalization.")),
))

#: The four-axis controller branch, named once so callers do not retype the id.
FOUR_AXIS_LOCAL = "four_axis_local"

#: --angle-marg-scheme value -> kernel id.  'grid', 'exact' and 'laplace' are not
#: peak-local kernels and are absent on purpose.
ANGLE_MARG_KERNEL = {
    "peak-local": "psi_local_phi_dense",
    "phi-local": "psi_local_phi_local",
}

#: Accepted --angle-marg-scheme spellings -> the canonical internal value.
#: The legacy spellings appear in archived run records and submit files across the
#: paper repository, so they stay accepted forever.  The descriptive spellings are
#: what new configurations should use; both resolve to the same kernel.
ANGLE_MARG_SPELLINGS = {
    "peak-local": "peak-local",
    "psi-local-phi-dense": "peak-local",
    "phi-local": "phi-local",
    "psi-local-phi-local": "phi-local",
}


def kernel(kernel_id):
    """The :class:`KernelIdentity` for ``kernel_id``, raising on an unknown id."""
    try:
        return KERNELS[kernel_id]
    except KeyError:
        raise KeyError(
            "unknown peak-local kernel id %r; known ids are %s"
            % (kernel_id, ", ".join(KERNELS)))


def label(kernel_id):
    """One-line axis summary, for a log line or a table cell."""
    k = kernel(kernel_id)
    parts = ["local in " + "+".join(k.localized) if k.localized else "no local axis"]
    if k.dense:
        parts.append("dense in " + "+".join(k.dense))
    return "; ".join(parts)


def describe(kernel_id):
    """``kernel_id (local in ...; dense in ...)``, the form log lines use."""
    return "%s (%s)" % (kernel_id, label(kernel_id))


def canonical_angle_marg_scheme(scheme):
    """Resolve an accepted --angle-marg-scheme spelling to its internal value.

    Non-peak-local schemes pass through unchanged, so this is safe to call on any
    scheme string.
    """
    return ANGLE_MARG_SPELLINGS.get(scheme, scheme)


def angle_marg_kernel_id(scheme):
    """Kernel id for an --angle-marg-scheme value, or None for the dense schemes."""
    return ANGLE_MARG_KERNEL.get(canonical_angle_marg_scheme(scheme))


#: Names that mean more than one kernel, and must never appear alone as a kernel id.
#: An earlier revision of this comment said a test "greps the shipped log lines for
#: them"; no such test existed, and internal review caught the claim.  What is
#: enforced is narrower and is what the tuple is for: no kernel id may equal one of
#: these (test_no_kernel_id_is_one_of_the_ambiguous_names), and the driver's own
#: emitted format strings must pair the word with a kernel id
#: (test_the_driver_log_lines_never_emit_a_bare_ambiguous_name).
AMBIGUOUS_NAMES = ("peak-local", "peaklocal", "peak_local", "local")
