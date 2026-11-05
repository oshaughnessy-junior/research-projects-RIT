# Cross-model marginalization on a shared grid

A record of what `create_event_parameter_pipeline_BasicMultiApproxIteration`
does and why, as of 2026-08-27. It is expected to be superseded; where this
note and the code disagree, the code and
`test/test_multiapprox_marginalization.py` are live truth.

## What the workflow computes

Running several waveform models is not the same as running several pipelines.
This builder evaluates every model on **one shared intrinsic grid** and
marginalizes over models at each grid point:

    L_marg(lambda) = sum_m  p(m) L_m(lambda)

The iteration loop fits `L_marg`, so the adaptive grid refines on the
model-marginalized posterior: points get placed where the *mixture* has
support, including where one model is good and another is not. This is the
difference from running N single-model pipelines and mixing posteriors
afterwards: there, each run's grid refines on its own model's posterior, and no
amount of post-hoc mixing recovers support that neither run sampled.

The combination is **linear in L**, not in lnL. Averaging lnL would give the
geometric mean (a logarithmic opinion pool), which is not Bayesian model
marginalization and is dominated differently when models disagree.

## Where each piece lives

| stage | model-tagged? | why |
|---|---|---|
| ILE input grid `overlap-grid-N.xml.gz` | no | the shared grid; all models evaluate the same lambda |
| ILE output → `approx_<A>_consolidated_N.composite` | yes | per-model evaluations, kept apart |
| `unify.sh` → `all.net` | no | pools every model, **marginalizes** |
| in-loop CIP → `overlap-grid-N+1` | no | one fit over the marginalized net |
| `unify_model.sh` → `approx_<A>_all.net` | yes | one model only, for the fork |
| terminal CIP → `approx_<A>_overlap-grid-N` + `+annotation.dat` | yes | per-model posterior **and** ln Z |
| extrinsic ILE / convert / resample / cat | yes | per model |
| `util_CombineApproximantPosteriors.py` | — | mixture weighted by p(m) Z_m |

**The loop merges; the terminal stage forks.** The marginalized grid is what
should steer exploration, but the deliverables of a systematics study are the
per-model posteriors and their evidences, so those are produced separately,
left on disk, and combined only at the very end.

## The marginalization is in util_CleanILE.py

It keys on the intrinsic parameters (rounded to 5 decimals), so the same lambda
under several models collapses to one entry. `--model-group-regex` takes the
model label from each input filename and switches on a **two-level** combine:

* **within** a model, replicas estimate one number → ntot-weighted linear mean;
* **across** models, the quantity itself differs → marginalize with p(m).

Flat pooling of everything (the behaviour without the flag) silently uses
*replica counts* as model weights. On a two-model point where one model has two
replicas and the other one, with lnL 100 and 104, flat pooling gives 102.937
where uniform-prior marginalization gives 103.325: **0.39 nats**, growing with
model disagreement and replica imbalance. Correct for replicas of one model,
wrong across models, which is why it is opt-in and why one model reduces to it
exactly (verified byte-identical).

`p(m)` is a *prior over waveform models*, not sampling effort. `ntot` is
sampling effort. Conflating them is the defect above.

## Partial coverage changes the estimator, point by point

A lambda evaluated under only some models is marginalized over that subset, so
the estimator differs across the grid: a model- and lambda-dependent change in
the effective prior. Causes: the `sigmaOverL > 0.9` resolution cut firing for
one model and not another, or one model's ILE failing.

This is reported (never silent) and `--require-all-approx` drops such points
instead. Dropping is not free either. It biases toward regions where the
*worst-resolved* model converged, so it is a choice, not a default.

## Iteration index of each net and grid

Audited 2026-11-04 from emitted two-approximant DAGs, for `n` = 1, 2 and 3, with
and without test, puff, exploded CIP, the terminal stage and `--start-iteration`.
No net or grid is read at the wrong iteration.

| stage | reads | writes |
|---|---|---|
| loop ILE, iteration `it` | `overlap-grid-it` | `approx_<A>_iteration_it_ile/` |
| puff ILE, iteration `it` | `puffball-it` | `approx_<A>_iteration_it_ile/` |
| join, iteration `it` | `approx_<A>_iteration_it_ile/` | `approx_<A>_consolidated_it.composite` |
| `unify.sh`, iteration `it` | composites 0 to `it`, every model | `all.net` |
| loop CIP, iteration `it` | `all.net` | `overlap-grid-(it+1)` |
| `unify_model.sh` | composites 0 to `n-1` of `<A>` | `approx_<A>_all.net` |
| terminal CIP, iteration `n-1` | `approx_<A>_all.net` | `approx_<A>_overlap-grid-n` and its annotation |
| extrinsic ILE, iteration `n` | `approx_<A>_overlap-grid-n` | per-model extrinsic samples |
| combine, iteration `n` | samples and `approx_<A>_overlap-grid-n+annotation.dat` | `extrinsic_posterior_samples.dat` |

The last fits run at iteration `n-1` and write index `n`, so a net one index
behind its grid is correct. No loop ILE evaluates index `n`. The last loop CIP
and the terminal CIPs fit the same evaluations. `overlap-grid-n` is the final
`L_marg` posterior. Convert reads it, and PUFF reads it when PUFF runs at
iteration `n-1`.

`all.net` is one file rewritten every iteration, and `approx_<A>_all.net` is a
glob. Their names do not show which evaluations a reader gets, so only the DAG
edges do. `test_each_iteration_fits_the_net_of_its_own_evaluations` and
`test_terminal_stage_uses_every_evaluation_and_one_grid_index` check those edges,
the glob of each unify and the path it writes. They run on two builds: `n` = 2 with
cadence 1, and `n` = 6 with cadence 2, `puff-max-it` 3, exploded CIP, a grid job and
OSG transfer. Without them, a CIP ordered after the previous iteration's unify passed
every test.

Index `n` is fixed at build time. A DAG cut short by hand keeps `n` on its
terminal products while its nets hold fewer iterations. The builder cannot do
this itself, since it forces `--always-succeed` on terminal runs. The 2026-08-29
C1-missing seed-1 run predates that. Built with `--n-iterations 7`, it stopped at
the convergence gate after iteration 5, and an edited rescue DAG ran the terminal
stage. Its `approx_<A>_overlap-grid-7` files therefore come from iterations 0 to
5, and no ILE ran on `overlap-grid-6`. The evidence is on CIT, in
`~/LVK/RIFT_roboto_paper_multiapprox/analyses/multi_approx/runs/C1-missing-seed1/`:
`sidecars/RUN_PROVENANCE.md` lines 226 to 255 and `condor_history_min.json`.

## Prior work

Done before by hand (Yelikar; Jan), not efficiently in production. The claim
here is the production workflow (one DAG, one shared adaptive grid, evidence-
weighted recombination), not the idea of waveform marginalization.

## Known limits

* The final mixture resamples with replacement, so a model carrying most of the
  weight can be drawn more times than it has samples. Reported as a warning;
  it degrades effective sample size without changing the row count.
* `p(m)` is uniform unless `--approx-prior` is given, and applies at both the
  in-loop marginalization and the final mixture. They are the same p(m) and are
  passed to both from one option.
* A puffball written at iteration `puff-max-it` or `n-1` is never evaluated.
  PUFF writes `puffball-(it+1)`, and puff ILE reads it only if `it+1` is at most
  `puff-max-it` and `n-1`. With cadence 2, `puff-max-it` 3 and `n` = 4, every
  puffball is evaluated. The cost is one local job. BasicIteration has the same
  PUFF gate.
* With `--start-iteration` above 0, the first iteration can schedule puff ILE on
  a puffball no job in the DAG writes. `--start-iteration 1` emits `ILE_puff` on
  `puffball-1`, and it fails unless that file survives from an earlier run. The
  gate here is `it>0`. BasicIteration uses `it>it_start`.
* A large ln Z gap between waveform models usually means the models disagree by
  far more than the statistical error, not that one is right. The combiner
  warns above 0.99 mixture weight.

## Also fixed here

`util_RIFT_pseudo_pipe.py` reaches this builder through `--pipeline-builder
BasicMultiApproxIteration`, and `--approx-extra` selects it. All of the following
were read off an emitted two-approximant DAG.

* The terminal extrinsic stage read `overlap-grid-<n_iterations-1>` while the
  final CIP wrote `overlap-grid-<n_iterations>`: a guard,
  `if not ('it' in globals())`, preserved an `it` the iteration loop had already
  left one short. At one iteration it read the raw seed grid.
* `join_grids.sh` interpolated `$(macroapprox)` **inside a bash script**:
  command substitution, not a condor macro, since the `.sub` passes only
  `$1 $2`. It matched nothing. Gone now that the loop grid is not model-tagged.
* `CIP.sub`'s `initialdir` named `iteration_N_cip` while the mkdir loop created
  `approx_<A>_iteration_N_cip`; the consolidate/unify log directories were the
  mirror image. Either holds the job on the execute node, and no DAG-shape
  check sees it, hence `test_every_job_directory_exists`.
* `parent_fit_node` was a single variable spanning `for it: for approx:`, so
  model B's iteration-0 ILE waited on model A's iteration-1 convert. The models
  were serialized into one chain and cross-coupled; they now run in parallel.
* The terminal convert wrote one untagged `posterior_samples-N.dat` from every
  model, while the convergence test read a per-model name nothing wrote.

## Testing status

`test/test_multiapprox_marginalization.py` covers the combination arithmetic
against hand-computed values, and the emitted DAG's shape. **Neither validates
the inference on data.** The prototype for that is a rerun of one of the
Jan/Yelikar cases (cheap, with large between-model differences), framed as
a reproduction claim first and an efficiency claim second. Nothing from this
workflow belongs in a paper until that runs.
