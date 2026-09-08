"""The peak-local kernel registry, and the output that has to name a kernel.

WHAT THIS FILE PROTECTS.  Eight kernels in this package are called "peak-local".
Two are selected by that exact word on two different flags.  On 2026-09-08 a
profiling run measured `--angle-marg-scheme peak-local` and published the number
as a claim about the four-axis branch of `--direct-marginalization-policy auto`;
the two kernels disagreed in direction.  The registry in
RIFT.likelihood.peak_local_names is the single answer to "which one", and these
tests fail if it drifts from the code, if a kernel loses its name, or if the
output a run produces stops naming the kernel that ran.

The registry is pure data, so most of this file runs without jax.  The wiring
tests at the end build one likelihood and evaluate nothing.
"""
import importlib
import os
import pathlib

import numpy as np
import pytest

from RIFT.likelihood import peak_local_names as N


# --------------------------------------------------------------------------
# The registry describes the code that exists.
# --------------------------------------------------------------------------

def test_every_kernel_id_is_unique_and_says_which_axes_it_localizes():
    ids = list(N.KERNELS)
    assert len(ids) == len(set(ids))
    for kid, k in N.KERNELS.items():
        assert k.kernel_id == kid
        assert k.localized, kid
        assert not (set(k.localized) & set(k.dense)), kid


def test_no_kernel_id_is_one_of_the_ambiguous_names():
    """An id equal to 'peak_local' would reintroduce the whole defect."""
    for kid in N.KERNELS:
        assert kid not in N.AMBIGUOUS_NAMES
        assert kid.replace("_", "-") not in N.AMBIGUOUS_NAMES


@pytest.mark.parametrize("kernel_id", list(N.KERNELS))
def test_each_registry_entry_points_at_a_module_that_exists(kernel_id):
    """The registry is a claim about the tree, so check it against the tree.

    A renamed or deleted module must break this test rather than leave a run
    record naming a kernel nobody can find.
    """
    k = N.KERNELS[kernel_id]
    if ".jax_ile." in k.module:
        pytest.importorskip("jax")
    mod = importlib.import_module(k.module)
    for entry in k.entry.split(", "):
        assert hasattr(mod, entry), (k.module, entry)


def test_the_two_flags_that_spell_peak_local_reach_different_kernels():
    """The 2026-09-08 error in one assertion."""
    angle = N.KERNELS[N.angle_marg_kernel_id("peak-local")]
    four = N.KERNELS[N.FOUR_AXIS_LOCAL]
    assert angle.kernel_id != four.kernel_id
    assert set(angle.localized) != set(four.localized)
    assert angle.selector == "--angle-marg-scheme peak-local"
    assert four.selector == "--direct-marginalization-policy auto"
    # The one that a run can reach without naming a scheme is the four-axis one.
    assert four.reachable_from_auto and not angle.reachable_from_auto


def test_describe_never_returns_a_bare_ambiguous_name():
    for kid in N.KERNELS:
        text = N.describe(kid)
        assert text.startswith(kid)
        assert "local in" in text or "no local axis" in text


def test_unknown_kernel_id_raises_rather_than_returning_a_default():
    with pytest.raises(KeyError):
        N.kernel("peak-local")


# --------------------------------------------------------------------------
# The scheme spellings.
# --------------------------------------------------------------------------

def test_legacy_spellings_stay_accepted_and_resolve_to_one_internal_value():
    """Archived run records and submit files across the paper repository carry
    'peak-local' and 'phi-local'.  Both must keep working, and both must land on
    the same internal string as their descriptive synonym, or a downstream
    string comparison sees two values for one kernel."""
    assert N.canonical_angle_marg_scheme("peak-local") == "peak-local"
    assert N.canonical_angle_marg_scheme("psi-local-phi-dense") == "peak-local"
    assert N.canonical_angle_marg_scheme("phi-local") == "phi-local"
    assert N.canonical_angle_marg_scheme("psi-local-phi-local") == "phi-local"


def test_non_local_schemes_pass_through_unchanged_and_have_no_kernel():
    for scheme in ("grid", "exact", "laplace", "auto"):
        assert N.canonical_angle_marg_scheme(scheme) == scheme
        assert N.angle_marg_kernel_id(scheme) is None


def test_every_accepted_spelling_is_offered_on_the_command_line():
    """optparse builds --angle-marg-scheme's choices from ANGLE_MARG_CHOICES, so a
    spelling absent from that tuple is unreachable however well it resolves."""
    AM = pytest.importorskip("RIFT.likelihood.jax_ile.anglemarg")
    for spelling, canonical in N.ANGLE_MARG_SPELLINGS.items():
        assert spelling in AM.ANGLE_MARG_CHOICES, spelling
        assert canonical in AM.ANGLE_MARG_CHOICES, canonical


# --------------------------------------------------------------------------
# The kernels the anglemarg schemes select.
# --------------------------------------------------------------------------

def test_descriptive_entry_names_and_legacy_aliases_are_the_same_function():
    """Two spellings must not be able to select two kernels."""
    AM = pytest.importorskip("RIFT.likelihood.jax_ile.anglemarg")
    assert (AM.fused_log_likelihood_distphipsimarg_peaklocal
            is AM.fused_log_likelihood_distphipsimarg_psi_local_phi_dense)
    assert (AM.fused_log_likelihood_distphipsimarg_phi_local
            is AM.fused_log_likelihood_distphipsimarg_psi_local_phi_local)


def test_the_local_schemes_are_still_not_reachable_from_auto():
    """The registry says reachable_from_auto is False for both; check the selector
    rather than the registry's own claim about it."""
    AM = pytest.importorskip("RIFT.likelihood.jax_ile.anglemarg")
    for amp in (1.0, 50.0, 500.0, 5.0e4, 5.0e6):
        scheme, _ = AM.choose_angle_marg_scheme(amp)
        assert scheme not in AM.ANGLE_MARG_LOCAL_SCHEMES, (amp, scheme)


# --------------------------------------------------------------------------
# A run's own output names the kernel that ran.
# --------------------------------------------------------------------------

def test_policy_summary_names_the_local_kernel():
    """"local=%d" is a count with no kernel attached; the summary carries the id."""
    pytest.importorskip("jax")
    from RIFT.likelihood.jax_ile import direct_marginalization_policy as P
    n = 3
    ledger = {"empirical_value_error_score_nats": np.zeros(n),
              "usable": np.ones(n, dtype=bool)}
    # FILL THE LEDGER FROM THE FUNCTION, not from a list typed here.  The summary
    # counts a set of boolean keys that upstream adds to: a hardcoded list went
    # stale within a day (KeyError 'tables_finite' after #285).  This test is about
    # the kernel NAME, so it must not also be a copy of the ledger schema.
    for _ in range(64):
        try:
            summary = P.summarize_policy_ledger(ledger)
            break
        except KeyError as missing:
            ledger[missing.args[0]] = np.ones(n, dtype=bool)
    else:
        pytest.fail("summarize_policy_ledger still wants keys after 64 rounds")
    assert summary["local_kernel"] == N.FOUR_AXIS_LOCAL
    assert summary["local_kernel"] != N.angle_marg_kernel_id("peak-local")


@pytest.mark.parametrize("requested,scheme,kernel_id", [
    ("peak-local", "peak-local", "psi_local_phi_dense"),
    ("psi-local-phi-dense", "peak-local", "psi_local_phi_dense"),
    ("phi-local", "phi-local", "psi_local_phi_local"),
    ("psi-local-phi-local", "phi-local", "psi_local_phi_local"),
])
def test_wrapper_records_the_kernel_and_the_spelling_asked_for(
        requested, scheme, kernel_id):
    """A reader of the run record must not need this history.  The record keeps the
    caller's spelling under requested= and the kernel that ran under kernel=."""
    pytest.importorskip("jax")
    from RIFT.likelihood.jax_ile.wrapper import JAXDistPhiPsiMargLikelihood
    from test_angle_marg_exact import make_synth, INTERP
    data = make_synth(scale=2.0)
    like = JAXDistPhiPsiMargLikelihood(data, 30.0, 3000.0, nphi=32, npsi=8,
                                       interp=INTERP, angle_marg=requested)
    assert like.angle_marg_scheme == scheme
    assert like.angle_marg_info["requested"] == requested
    assert like.angle_marg_info["kernel"] == kernel_id


@pytest.mark.parametrize("spelling,accepted", [
    ("psi-local-phi-dense", True),
    ("not-a-scheme", False),
])
def test_the_driver_accepts_the_descriptive_spelling(spelling, accepted):
    """THE DRIVER SEAM, not the library.  ANGLE_MARG_CHOICES membership is what
    optparse validates against, but nothing in-process proves the driver builds its
    choices from that tuple.  Two subprocesses: one new spelling and one control that
    must be rejected, so a parser that accepted everything would fail this too.

    --help would not do: optparse validates choices during parse_args, and --help
    exits before that.  The driver is expected to fail LATER on missing inputs; only
    the "invalid choice" text is read here.
    """
    import subprocess
    import sys
    code = pathlib.Path(__file__).resolve().parents[2]
    driver = code / "bin" / "integrate_likelihood_extrinsic_jax"
    if not driver.exists():
        pytest.skip("driver script not present in this checkout")
    env = dict(os.environ, PYTHONPATH=str(code), JAX_PLATFORMS="cpu",
               OMP_NUM_THREADS="1")
    out = subprocess.run([sys.executable, str(driver),
                          "--angle-marg-scheme", spelling],
                         capture_output=True, text=True, env=env, timeout=900)
    rejected = "invalid choice" in (out.stderr + out.stdout)
    assert rejected is not accepted, (spelling, out.stderr[-400:])


def test_the_numpy_time_kernel_names_itself_in_its_diagnostics():
    """The OTHER flag that spells "peak-local".  `--time-marginalization-quadrature
    peak-local` selects a TIME kernel on the numpy/cupy arm; a dump of its diagnostics
    must say so, because the identical word on `--angle-marg-scheme` selects an angle
    kernel with a different cost and a different accuracy."""
    T = pytest.importorskip("RIFT.likelihood.time_marginalization_peak_local")
    assert T.last_report is not None
    src = pathlib.Path(T.__file__).read_text()
    # The stats dict is built in one place; assert the field is seeded there rather
    # than running a full marginalization for a string.
    assert 'kernel=_names.kernel("time_local_numpy").kernel_id' in src
    assert N.kernel("time_local_numpy").selector == (
        "--time-marginalization-quadrature peak-local")


def test_the_diagnostic_four_axis_planner_names_itself_when_it_faults():
    """Its fault line is the one thing this module prints to a human, and
    "multipeak" does not say which axes or which of the two four-axis paths."""
    pytest.importorskip("jax")
    from RIFT.likelihood.jax_ile import multipeak_planner
    src = pathlib.Path(multipeak_planner.__file__).read_text()
    assert "[kernel four_axis_local_diagnostic]" in src
    assert N.kernel("four_axis_local_diagnostic").selector is None


@pytest.mark.parametrize("module_name,proposed", [
    ("all_axis_peaklocal", "four_axis_local.py"),
    ("joint_anglemarg_peaklocal", "phi_psi_cell_kernel.py"),
    ("time_first_peaklocal", "time_local.py"),
    ("multipeak_planner", "four_axis_local_diagnostic.py"),
])
def test_the_rename_notes_still_describe_a_rename_not_yet_done(module_name, proposed):
    """The four modules carry a RENAME PENDING note naming their replacement.  If the
    rename happens and the note survives, the note becomes a lie that reads as a
    to-do; if the note is dropped without the rename, the plan is lost.  Fail on
    either."""
    pytest.importorskip("jax")
    mod = importlib.import_module("RIFT.likelihood.jax_ile." + module_name)
    path = pathlib.Path(mod.__file__)
    assert path.name == module_name + ".py"
    assert path.name != proposed, "renamed; delete the RENAME PENDING note"
    doc = mod.__doc__ or ""
    assert "RENAME PENDING" in doc, module_name
    assert proposed in doc, (module_name, proposed)


def test_the_axis_contract_matches_the_kernel_entry_points():
    """M4 from the internal mutation sweep SURVIVED: declaring a false axis contract
    for a kernel failed nothing.  It was not hypothetical -- `phi_psi_cell_kernel_jax`
    shipped `dense=("phi_ref",)` while naming a rule that localizes phi_ref.

    A kernel that names an entry point with `phi_local` in it cannot call phi_ref
    dense, and one whose only entries are `phi_dense` cannot call it localized.  That
    is checkable from the registry alone, so it is checked here.
    """
    for kid, k in N.KERNELS.items():
        entries = k.entry.lower()
        if "phi_local" in entries and "phi_dense" not in entries:
            assert "phi_ref" in k.localized, (kid, "localizes phi but does not say so")
            assert "phi_ref" not in k.dense, (kid, "calls a localized phi dense")
        if "phi_dense" in entries and "phi_local" not in entries:
            assert "phi_ref" not in k.localized, (kid, "phi is dense in its only rule")
        if "phi_local" in entries and "phi_dense" in entries:
            # Two rules that disagree on phi.  The axis tuples cannot carry that, so
            # the entry must say so in prose instead of picking one and being wrong.
            assert "phi_ref" not in k.localized and "phi_ref" not in k.dense, kid
            assert "phi" in k.axis_note.lower(), (kid, "two rules, no axis_note")


def test_the_buffer_cap_resolves_the_spelling_before_it_compares():
    """D3 from the internal review.  `angle_marg_eval_chunk` matched a hardcoded
    scheme list, so a caller setting `angle_marg_scheme = "psi-local-phi-dense"` by
    hand fell through every branch and got an UNCAPPED batch -- the exact failure the
    guard exists for, reached by spelling.  Latent, but fail-open.
    """
    pytest.importorskip("jax")
    import numpy as _np
    from RIFT.likelihood.jax_ile import samplers as S

    def refuses(spelling, monkey_target):
        class D: pass
        class L: pass
        d = D(); d.npts = 1193; d.lms = _np.array([[2, 2], [2, -2]])
        like = L(); like.data = d; like.x_grid = _np.zeros(256)
        like.angle_marg_info = {"amp_sizing": 12500.0}
        like.angle_marg_scheme = spelling
        saved = S._angle_marg_buffer_target
        S._angle_marg_buffer_target = monkey_target
        try:
            S.angle_marg_eval_chunk(like, 8000)
            return False
        except MemoryError:
            return True
        finally:
            S._angle_marg_buffer_target = saved

    tiny = lambda: 1 << 30
    for spelling in ("peak-local", "psi-local-phi-dense",
                     "phi-local", "psi-local-phi-local"):
        assert refuses(spelling, tiny), spelling
    # 'grid' is the sentinel for "runs no dense angle scheme" and must pass through.
    assert not refuses("grid", tiny)


def test_the_driver_log_lines_never_emit_a_bare_ambiguous_name():
    """What AMBIGUOUS_NAMES is for.  Every driver line that prints one of those words
    must print a kernel id in the same statement, or a reader is back where they
    started.  Reads the driver's own format strings; comments are stripped first,
    because a comment is not something a run prints."""
    code = pathlib.Path(__file__).resolve().parents[2]
    driver = code / "bin" / "integrate_likelihood_extrinsic_jax"
    if not driver.exists():
        pytest.skip("driver script not present in this checkout")
    # ast, not tokenize: the driver builds its help text from adjacent string
    # literals, and a tokenizer sees each FRAGMENT.  Half a sentence is not the unit
    # a reader sees, and checking fragments failed on one that merely lacked the
    # word.  ast folds implicit concatenation into one Constant, which is the unit
    # the run actually prints.
    import ast
    tree = ast.parse(driver.read_text())
    # A bare "peak-local" with no whitespace is a COMPARISON OPERAND -- the scheme
    # value itself, in `if scheme in (...)` -- not something a run prints.  Requiring
    # it to carry a kernel id would be requiring the flag's own value to rename
    # itself, which is the compatibility surface this change preserves on purpose.
    # Messages are the literals with whitespace in them.
    suspects = [n.value for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and isinstance(n.value, str)
                and any(c.isspace() for c in n.value)
                and ("peak-local" in n.value.lower()
                     or "peaklocal" in n.value.lower())]
    assert suspects, "no driver string mentions the word; test has gone blind"
    # REQUIRE THE WORD "kernel", and nothing weaker.  An earlier version also accepted
    # a message that merely named a flag.  That let a mutation through: the
    # --direct-marginalization-policy help mentions --angle-marg-scheme in an
    # unrelated clause, so deleting its kernel id still satisfied the check.  A guard
    # that passes for a reason unrelated to what it tests is not a guard.
    for text in suspects:
        assert "kernel" in text.lower(), text[:300]


def test_the_dense_schemes_record_no_kernel():
    """kernel=None is the honest answer for 'exact': it is not a peak-local kernel,
    and inventing an id for it would make the field meaningless."""
    pytest.importorskip("jax")
    from RIFT.likelihood.jax_ile.wrapper import JAXDistPhiPsiMargLikelihood
    from test_angle_marg_exact import make_synth, INTERP
    data = make_synth(scale=2.0)
    like = JAXDistPhiPsiMargLikelihood(data, 30.0, 3000.0, nphi=32, npsi=8,
                                       interp=INTERP, angle_marg="exact")
    assert like.angle_marg_info["kernel"] is None
