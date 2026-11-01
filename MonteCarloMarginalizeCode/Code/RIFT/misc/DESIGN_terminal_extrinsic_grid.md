# Which grid the terminal extrinsic stage reads

Status of the DAG builders in `bin/` against one invariant, as of 2026-08-25
(RIFT PR #181/#182, #187, #189).

**The invariant.** When `--last-iteration-extrinsic` is set, the terminal ILE
runs on `overlap-grid-$(macroiteration)`, and it must read the grid the run
actually finished on — the CIP posterior, the same grid `convert` turns into
`posterior_samples-N.dat`. Grid-writing nodes carry that index as
`macroiterationnext`, so the check is mechanical on an emitted DAG: compare the
`macroiteration` VARS of the `ILE_extr` nodes against the `macroiterationnext`
of every grid writer. Two things have to hold, not one — the index must match,
**and** the node writing it must be an ancestor of every `ILE_extr` node.
Checking only the index passes a DAG in which the extrinsic jobs race the CIP
that writes their input, which is exactly the state the NR builder was in.

**Why each builder needed its own diagnosis.** All of them carried the same
guard, `if not ('it' in globals()): it = opts.n_iterations`, two of them with an
added `elif it < opts.n_iterations and 'Z' in cip_args_prefixes: it += 1`. The
guard reads as defensive and is not: what it preserves is a stale `it` the
iteration loop has already left at `n_iterations-1`. But the loop structures
differ, so the same source line does not have the same consequence — in one
builder it was dead, and the fix that repaired the others was a measured no-op
there. Reading was not enough; each was given a build path and the emitted DAG
inspected.

| builder | reachable from `util_RIFT_pseudo_pipe.py` | state |
|---|---|---|
| `create_event_parameter_pipeline_BasicIteration` | yes (default) | fixed, 925fc49a (PR #181/#182) |
| `create_event_parameter_pipeline_AlternateIteration` | yes (`--use-subdags`, `--internal-use-amr`, explicit) | fixed, PR #187 |
| `create_event_nr_pipeline_with_cip` | no | fixed, PR #189 — a *different* change; see below |
| `create_event_parameter_pipeline_BasicMultiApproxIteration` | no | defect demonstrated, fix **not yet landed**; see below |
| `cepp_basic_htcondor` | no | removed in PR #189 (unfinished port, could not emit a DAG) |

`--pipeline-builder` accepts only `BasicIteration`, `AlternateIteration` and
`Hyperpipe`, and asimov's RIFT pipeline drives `util_RIFT_pseudo_pipe.py` and
nothing else, so the last three are hand-run `bin/` scripts. `setup.py` globs
`bin/*`, so being unreachable from the driver is not the same as being absent:
the NR and MultiApprox builders are on `PATH` in every RIFT install checked here
(`rift-0.0.17.9rc0`, `rift-0.0.18.0rc1`; MultiApprox as far back as
`RIFT-0.0.15.6`), read from the installed-file manifests.

## `create_event_nr_pipeline_with_cip`: same invariant, different arithmetic

The one-line fix from PR #187 does nothing here, and was measured doing nothing
before this was diagnosed: `it = opts.n_iterations` is already set
unconditionally just above the terminal CIP, so the `if` branch was dead and the
`elif` could never be true. Applying it verbatim produced an identical DAG in
seven configurations.

The real shape: this workflow's iteration loop writes its grid with **REFINE**,
not CIP, and the terminal CIP is created *outside* the loop at
`macroiteration=n_iterations` / `macroiterationnext=n_iterations+1`. So the final
posterior is `overlap-grid-<n_iterations+1>`, while the last REFINE grid is
`overlap-grid-<n_iterations>`. The extrinsic stage read the REFINE grid — the NR
proposal grid — and the terminal CIP was not an ancestor of it at all, because
`parent_fit_node` is never advanced past the loop (the line that would do it is
commented out in the CIP block).

One consequence worth being explicit about, because it changes what the stage
computes and not just which file it names: the terminal ILE now runs on the CIP
posterior draw rather than on the REFINE spoke grid. Under `--nr-lookup`, which
an NR run's `args_ile.txt` carries and which is passed through to `ILE_extr.sub`
verbatim, those posterior points are matched to NR simulations, where the REFINE
grid's points already were simulations. This is the intended behaviour — it is
what makes the extrinsic samples a posterior draw, as in `BasicIteration` — but
it is a change in the physics of the stage, not a path correction.

The fix is therefore three coupled edits, not one:

* `it = opts.n_iterations + 1`, the index the terminal CIP writes;
* `parent_fit_node = fit_node`, so the extrinsic nodes wait for it;
* one more iteration directory, **only** when the extrinsic stage is on —
  `ILE_extr.sub` names `iteration_$(macroiteration)_ile` as its initialdir and
  log directory, and the mkdir loop stopped at `n_iterations`. Without this the
  DAG builds correctly and the jobs fail on the execute node, which no
  DAG-only assertion would catch. `_assert_nr_extrinsic_reads_the_final_grid`
  checks the directory for that reason.

`it` also feeds the trailing plot node, whose log directory moves with it. It
does **not** change what that node summarises: `samples_files` is `[]`, so no
macro reaches the plot arguments, and the built `plot.sub` is byte-identical
across the change. (Stated because the equivalent claim was made and had to be
retracted in the AlternateIteration change — the VARS moving does not imply the
page moved.)

**Still open here:** with `--cip-explode-jobs` this builder dies in
`write_concrete_dag` with `'list' object has no attribute 'get_sub_file'` — it
never unpacks the `(main, worker)` job pair the way
`BasicMultiApproxIteration` does.

## `create_event_parameter_pipeline_BasicMultiApproxIteration`: demonstrated, not landed

Identical no-`Z` shape to AlternateIteration before PR #187, and confirmed off
by one on emitted DAGs in eight configurations — including the
run-to-convergence `Z` schedule, and including one iteration, where it read
`overlap-grid-0`, the raw seed grid. The one-line fix and a gate assertion are
prepared but deliberately **not** in PR #189: this builder is a separate target
that has never been documented in a paper, and whether it is promoted or left as
development work is being decided separately. Until then, do not assume the
defect is fixed just because the other builders are.

## `cepp_basic_htcondor`: removed

A regex translation of `BasicIteration` (by `port_cepp.py`, removed with it) that
was never finished and could not construct a DAG. Recorded here so it is not
recreated by the same route:

* `dag = DAG(log=os.getcwd())` — neither `htcondor2.dags.DAG` nor
  `htcondor.dags.DAG` accepts a `log` keyword (both `__init__` signatures read
  off the installed bindings, htcondor 24.9.2). This is glue's
  `pipeline.CondorDAG(log=...)` left untranslated; `TypeError` before any node.
* `Node` was called at 31 live sites and never imported or defined — commit
  f8049ee5 removed the import because `htcondor2.dags` exports no `Node`, which
  fixed `--help` (all that `.travis/test-all-bin.sh` ran, and it skipped this
  script anyway) at the cost of guaranteeing `NameError` at DAG build.
* its backend `dag_utils_htcondor.py` references an undefined name `pipeline` in
  21 of its sub-writing functions; `dag_utils_generic.py` defines
  `pipeline = _PipelineNamespace()` and the htcondor copy was made without it.

**`RIFT/misc/dag_utils_htcondor.py` is left in place and now has no importer.**
Retained deliberately, for future consideration rather than because anything
needs it; RO's read (2026-08-27) is that it is probably completely stale. Note
that its module header claims it "retains all original functionality", which is
false — 21 of its writers raise `NameError` on the first call. Anyone picking it
up should treat it as an unfinished port, not as a working backend. The real htcondor
backend is the one inside `dag_utils_generic.py`, selected by
`RIFT_DAG_BACKEND=htcondor`; that one works and is what every builder uses.
