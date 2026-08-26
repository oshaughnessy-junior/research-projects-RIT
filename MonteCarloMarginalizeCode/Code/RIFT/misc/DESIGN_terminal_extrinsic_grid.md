# Which grid the terminal extrinsic stage reads

Status of the DAG builders in `bin/` against one invariant, as of 2026-08-25
(RIFT PR #181/#182, #187, #189, #192).

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
| `create_event_parameter_pipeline_BasicMultiApproxIteration` | no | fixed, PR #192 — and left development-only; see below |
| `cepp_basic_htcondor` | no | removed in PR #189 (unfinished port, could not emit a DAG) |

`--pipeline-builder` accepts only `BasicIteration`, `AlternateIteration` and
`Hyperpipe`, and asimov's RIFT pipeline drives `util_RIFT_pseudo_pipe.py` and
nothing else, so the last three are hand-run `bin/` scripts. `setup.py` globs
`bin/*`, so being unreachable from the driver is not the same as being absent:
the released `rift-0.0.17.8` ships the NR and MultiApprox builders on `PATH`.

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

## `create_event_parameter_pipeline_BasicMultiApproxIteration`: fixed, and left development-only

The extrinsic defect is real and the one-line fix landed in PR #192. The builder is
**not** promoted to `--pipeline-builder` and is **not** described in the O4d
paper set. Both halves of that were decided on evidence, 2026-08-25; this
section is the record.

**The defect, and its fix.** Identical no-`Z` shape to AlternateIteration
before PR #187, and confirmed off by one on emitted DAGs in eight
configurations — including the run-to-convergence `Z` schedule, and including
one iteration, where it read `overlap-grid-0`, the raw seed grid. `it =
opts.n_iterations` repairs it; the gate assertion is
`_assert_multiapprox_extrinsic_reads_the_final_grid`. Blast radius measured:
only the `.dag` differs, JOB/PARENT counts unchanged, and with
`--last-iteration-extrinsic` off the build is byte-identical.

**Why it is not promoted.** Promotion was considered and rejected: the
workflow this builder implements is already done a different way, and this
builder cannot execute the workflow it claims to.

*It is redundant.* `util_RIFT_pseudo_pipe.py --approx` is `type=str` and
required — one waveform per run, by design. asimov's RIFT pipeline reads
`production.meta["waveform"]["approximant"]`, also one per production, and
asimov's model is several productions per event (`bootstrap:` reuses a
converged run's grid for the next waveform) combined by PESummary as a separate
postprocessing analysis. That is the multi-waveform comparison RIFT actually
performs in O4, it is already the documented interface, and it produces
independent posteriors — which is what a systematics comparison needs. This
builder does not couple the approximants either: separate `all.net`, separate
CIP, separate grid per approximant. It buys one DAG instead of N and nothing
else, so there is no method to describe.

*It has never run.* Six defects, all read off emitted submit files from a
two-approximant build (`--approx IMRPhenomXPHM --approx SEOBNRv4PHM`,
`--n-iterations 2`), any one of which stops the run:

1. **The grid handoff is severed.** ILE reads
   `<W>/overlap-grid-$(macroiteration).xml.gz`, untagged — the same name
   BasicIteration uses. Only the seed `overlap-grid-0.xml.gz` ever exists at
   that path. The main CIP writes inside its own directory
   (`<cipdir>/overlap-grid-$(macroiterationnext)`) and nothing promotes it;
   `join_grids.sh`, which would, is a **shell script containing
   `$(macroapprox)`** — bash command substitution, not a condor macro, because
   `join_grids.sub` passes only `$(macroiteration) $(macroiterationnext)` as
   arguments. It therefore globs `approx__iteration_1_cip/approx__overlap-grid-2*`
   and writes `approx__overlap-grid-2.xml.gz`. Iteration 1 has no input.
2. **CIP's `initialdir` is a directory that is never created.** The mkdir loop
   makes `approx_<A>_iteration_N_cip`; `CIP.sub` names
   `iteration_$(macroiteration)_cip`. Every CIP job is held at iteration 0.
   `join_grids.sub` names the tagged form — the two disagree.
3. **`con`/`unify` log directories are the mirror image**: the loop makes
   `iteration_N_con` (untagged), the subs write to
   `approx_$(macroapprox)_iteration_N_con/logs/`.
4. **The terminal extrinsic chain is approximant-blind.** `ILE_extr`,
   `convert_extr`, `resample` and `cat` carry no `macroapprox`, and the chain is
   emitted **once for the whole DAG**, not once per approximant. `ILE_extr.sub`
   interpolates `$(macroapprox)` into both `--approx` and its `initialdir`, so
   it resolves to an empty `--approx` and `approx__iteration_2_ile`; `cat`
   writes `extrinsic_posterior_samples_.dat`. With two approximants, 30 nodes
   are approximant-tagged and these are not among them.
5. **The terminal convert clobbers across approximants.** `convert.sub` writes
   `posterior_samples-$(macroiteration).dat`, untagged, from both approximants'
   nodes; `test.sub` reads `approx_$(macroapprox)_posterior_samples-N.dat`,
   which nothing writes.
6. **The approximants are serialized and cross-coupled.** `parent_fit_node` is
   one variable spanning `for it: for approx:`, so approximant B's
   iteration-0 ILE waits on approximant A's iteration-1 `convert`, and A's
   iteration-1 ILE waits on B's. There is no parallelism and the edges are
   semantically wrong — the opposite of the "simultaneous coordinated
   multi-approximant operation" the original commit (cdc1acfc, 2020-06-04)
   claimed. The builder has not been touched since its 2020 py3 port except by
   sweeps that changed every builder at once.

The header is still BasicIteration's, down to naming `args_cip.txt` and
`create_event_parameter_pipeline_BasicIteration.py` in its own EXAMPLES block.

**What this means for the fix.** The extrinsic index is now right in a builder
that still cannot reach iteration 1. That is worth landing anyway — it removes
the last instance of a defect fixed in three sibling builders, so nobody
re-derives it, and it is a measured no-op on every other emitted file. It is
not a claim that the builder works. Anyone reviving multi-approximant DAGs
should treat items 1–6 as the actual work and this fix as already done.

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
It is kept only because a future htcondor2 port might start from it. Note that
its module header claims it "retains all original functionality", which is false
— 21 of its writers raise `NameError` on the first call. The real htcondor
backend is the one inside `dag_utils_generic.py`, selected by
`RIFT_DAG_BACKEND=htcondor`; that one works and is what every builder uses.
