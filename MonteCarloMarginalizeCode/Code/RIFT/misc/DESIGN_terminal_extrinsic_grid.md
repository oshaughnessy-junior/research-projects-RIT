# Which grid the terminal extrinsic stage reads

Scope note, first, because this file is easy to over-read: **on this branch
(`rift_O4d`) only `create_event_nr_pipeline_with_cip` has been fixed.** The
other builders still carry the defect described below. Fixes for two of them
exist, but on the `codex/hyperpipe-pseudo-pipe-builder` line (RIFT PR #181/#182
for `BasicIteration`, #187 for `AlternateIteration`), which is not merged here.
Do not read the invariant below as something this branch currently satisfies.

**The invariant.** When `--last-iteration-extrinsic` is set, the terminal ILE
runs on `overlap-grid-$(macroiteration)`, and it must read the grid the run
actually finished on — the CIP posterior, the same grid `convert` turns into
`posterior_samples-N.dat`. Grid-writing nodes carry that index as
`macroiterationnext`, so the check is mechanical on an emitted DAG. Two things
have to hold, not one: the index must match, **and** the node writing it must be
an ancestor of every `ILE_extr` node. Checking only the index passes a DAG in
which the extrinsic jobs race the CIP that writes their input, which is exactly
the state the NR builder was in.

**The common origin.** Every builder carried the guard
`if not ('it' in globals()): it = opts.n_iterations`, some with an added
`elif it < opts.n_iterations and 'Z' in cip_args_prefixes: it += 1`. It reads as
defensive and is not: what it preserves is a stale `it` that the iteration loop
has already left at `n_iterations-1`. But the builders' loop structures differ,
so the same source line does not have the same consequence in each — and in the
NR builder it could not execute at all, which is why the one-line fix that
repairs the others does nothing here.

| builder | state on `rift_O4d` |
|---|---|
| `create_event_nr_pipeline_with_cip` | **fixed here**, with a different change; see below |
| `create_event_parameter_pipeline_BasicIteration` | still carries the guard (with the `Z` branch). Fixed on the hyperpipe line, PR #181/#182 |
| `create_event_parameter_pipeline_AlternateIteration` | still carries the guard, no `Z` branch. Fixed on the hyperpipe line, PR #187 |
| `create_event_parameter_pipeline_BasicMultiApproxIteration` | still carries the guard, no `Z` branch. Defect demonstrated on emitted DAGs but not fixed on any branch |
| `cepp_basic_htcondor` | removed here — unfinished htcondor2 port that could not emit a DAG |

For the three unfixed rows the *guard text* was checked on this branch; the
DAG-level consequence was measured on the hyperpipe line, not re-measured here.

## `create_event_nr_pipeline_with_cip`: same invariant, different arithmetic

The one-line fix used on the other builders does nothing here. `it =
opts.n_iterations` is already set unconditionally just above the terminal CIP,
so the `if` branch is dead and the `elif` can never be true.

The real shape: this workflow's iteration loop writes its grid with **REFINE**,
not CIP, and the terminal CIP is created *outside* the loop at
`macroiteration=n_iterations` / `macroiterationnext=n_iterations+1`. So the final
posterior is `overlap-grid-<n_iterations+1>`, while the last REFINE grid is
`overlap-grid-<n_iterations>`. The extrinsic stage read the REFINE grid — the NR
proposal grid — and the terminal CIP was not an ancestor of it at all, because
`parent_fit_node` is never advanced past the loop (the line that would do it is
commented out in the CIP block).

Measured on this branch before the change, at two iterations: the extrinsic
stage read `overlap-grid-2` while the last grid written was `overlap-grid-3`,
and the CIP writing it was not an ancestor of either `ILE_extr` node.

The fix is three coupled edits, not one:

* `it = opts.n_iterations + 1`, the index the terminal CIP writes;
* `parent_fit_node = fit_node`, so the extrinsic nodes wait for it;
* one more iteration directory, **only** when the extrinsic stage is on —
  `ILE_extr.sub` names `iteration_$(macroiteration)_ile` as its initialdir and
  log directory, and the mkdir loop stopped at `n_iterations`. Without this the
  DAG builds correctly and the jobs fail on the execute node, which no
  DAG-only assertion would catch.

`test/test_nr_pipeline_terminal_grid.py` pins all three, at two iteration counts,
by building a real DAG. Each edit was reverted on its own and each is
independently lethal, caught by a different assertion.

**A consequence worth being explicit about**, because it changes what the stage
computes and not just which file it names: the terminal ILE now runs on the CIP
posterior draw rather than on the REFINE spoke grid. Under `--nr-lookup`, which
an NR run's `args_ile.txt` carries and which is passed through to `ILE_extr.sub`
verbatim, those posterior points are matched to NR simulations, where the REFINE
grid's points already were simulations. This is the intended behaviour — it is
what makes the extrinsic samples a posterior draw, as in `BasicIteration` — but
it is a change in the physics of the stage, not a path correction.

**Still open here:** with `--cip-explode-jobs` this builder dies in
`write_concrete_dag` with `'list' object has no attribute 'get_sub_file'` — it
never unpacks the `(main, worker)` job pair the way `BasicMultiApproxIteration`
does. Every configuration measured above is therefore non-exploded.

## `cepp_basic_htcondor`: removed

A regex translation of `BasicIteration` (by `port_cepp.py`, removed with it) that
was never finished and could not construct a DAG. Recorded so it is not
recreated by the same route — all three checked against this branch's copy:

* `dag = DAG(log=os.getcwd())` — neither `htcondor2.dags.DAG` nor
  `htcondor.dags.DAG` accepts a `log` keyword (both `__init__` signatures read
  off the installed bindings, htcondor 24.9.2). This is glue's
  `pipeline.CondorDAG(log=...)` left untranslated; `TypeError` before any node.
* `Node` was called at 31 live sites and never imported or defined.
* its backend `dag_utils_htcondor.py` references an undefined name `pipeline` in
  21 of its sub-writing functions; `dag_utils_generic.py` defines
  `pipeline = _PipelineNamespace()` and the htcondor copy was made without it.

**`RIFT/misc/dag_utils_htcondor.py` is left in place and now has no importer.**
Retained for future consideration rather than because anything needs it; RO's
read (2026-08-27) is that it is probably completely stale. Its module header
claims it "retains all original functionality", which is false — 21 of its
writers raise `NameError` on the first call. Anyone picking it up should treat it
as an unfinished port, not a working backend.
