# Backend prototypes — planning

This document captures the planning decisions for *backend prototypes*
that sit on top of the v2 simulation archive (see `DESIGN.md`). A
backend bundles the science semantics for one class of simulation:
generator, summarizer, similarity, planner, and downstream-user API.
The archive engine is unchanged; what each backend supplies is the
domain knowledge.

## Three classes (per Richard's spec)

1. **Synthetic universes** — Monte Carlo simulations of populations
   (StarTrack, COSMIC, COMPAS, AGN disks, Rapster globular clusters,
   mixtures). Common goal: adaptively place population-synthesis runs
   to fit GW observations. Mixture models reuse simulations of
   individual components.
2. **EM transient (kilonova prototype)** — physics-based simulations
   often staged. Stage 1: SuperNu with fixed composition + a few-dim
   ejecta mass model. Stage 2: WinNet / SkyNet nucleosynthesis →
   SuperNu radiative transfer; entropy / temperature / Yₑ / v_exp
   trajectories drive composition. Goal: adaptive sim placement to
   fit real EM observations.
3. **GW PE** — two flavors:
   * **synthetic-targeted**: user provides synthetic source params,
     backend generates fake data (`.gwf` to disk) and runs a normal
     RIFT PE pipeline. Reference: the "populations" example in this
     repo.
   * **production**: thin frontend over `asimov`. Event identified via
     cbcflow; analyses drawn from the blueprints in
     `https://git.ligo.org/asimov/data`. Our archive is essentially a
     curator on top of asimov's existing event/result database.

## Common backend contract

Each backend lives at `RIFT/simulation_manager/backends/<name>/`:

```
generator.py        # frozen at archive-creation time; level-aware (params, sim_dir, level, prev_levels)
summarizer.py       # rolls level outputs into summary.json
same_q.py           # equality predicate on params
lookup_key.py       # bucket function for fast dedup
planner.py          # adaptive scout: read archive + observations, pick next params
user_api.py         # downstream queries: posterior_for_params(...), components_for_universe(...), ...
cli.py              # bin/-shim entrypoint
```

`backends/_base.py` documents the contract (an abstract `Backend`
class plus a `make_archive(...)` helper that wires generator/
summarizer/etc. into a fresh `Archive` with the right manifest config).
Per-backend overrides go in the leaf module; the contract is more
important than any particular base class.

## Multi-archive composition pattern

Two backends naturally split into a reusable lower stage and a
configurable upper stage:

* **Synthetic universes**: per-(model, metallicity, …) popsynth runs
  feed a convolution that produces a universe. The same popsynth runs
  are reused across many universes and across mixture models.
* **Kilonova Stage 2**: per-(s, T, Yₑ, v_exp) nucleosynthesis
  trajectories feed SuperNu radiative transfer. The same trajectory
  output is reused across many radiative-transfer parameterizations.

Both fit the same shape: **two separate archives**, with the upper
archive's `params` referencing lower-archive sim names by id (or
content-addressable via the lower archive's `lookup_key` + `same_q`).
Cross-archive coordination lives in the upper archive's *planner*,
not in its generator. Pattern:

```
upper_planner.step():
    1. propose new upper-archive params
    2. resolve required lower-archive component params for each
    3. lower_archive.register(component_params, target_level=N)   # idempotent dedup
    4. lower_archive.request_queue.submit_pending()
    5. block until components are 'complete' at target level
       (via the request_sim --ensure CLI as a hyperpipeline DAG node,
        or in-process polling)
    6. upper_archive.register(upper_params)  # references resolved sim_names
    7. upper_archive.request_queue.submit_pending()
```

The upper-archive *worker* only needs to read the resolved lower-
archive output files. On a shared-FS submit host this is trivial; on
OSG without shared FS the upper archive's per-(sim, level) submit
description gets the lower-archive level files added to its
`transfer_input_files`. A small helper `Archive.input_paths_from(other,
sim_name)` will land alongside the existing `transfer_input_files_for`
when the upper backend needs it.

## External tools to know about

### basil (population synthesis on the OSG)
- Repo: `https://gitlab.com/xevra/basil`
- Author: Vera Delfavero
- Purpose: high-level synthetic universes on OSG; manages popsynth
  grids and convolutions
- Integration option A (preferred): basil's per-model runs are this
  archive's lower-stage sims; we wrap basil in a run-queue
  executable. Adapter would land at
  `backends/synthetic_universe/basil_adapter.py`.
- Integration option B: basil produces archive-shaped outputs out-of-
  band; we register / curate them.
- Pre-coding action: read basil's actual API and decide A vs B. Until
  then the popsynth backend uses a stub generator so we can prove the
  multi-archive composition pattern end-to-end without external deps.
- Possible simplification: basil may already own the
  popsynth-as-transfer-function + universe-as-convolution split. If it
  does, the universe archive becomes a thin curator over basil rather
  than re-implementing the convolution.

### asimov (production GW PE)
- Already a known dependency: `RIFT/asimov/` is a thing; setup.py
  registers `asimov.pipelines:rift = RIFT.asimov.rift:Rift`.
- The integration goes the OTHER direction — asimov uses RIFT, not
  vice versa. The production GW PE backend is therefore a *frontend*
  over asimov's existing event-tracking and result database.
- **Decision (Richard):** the production GW PE backend will use the
  *leaky* abstraction. The archive doesn't run anything itself;
  instead a curator process walks asimov's result DB and updates the
  archive's `index.jsonl` + per-sim `summary.json`. Asimov is the
  queue. Less code, less duplication, accepts that the framework's
  symmetry is broken for this one backend. Worth it.
- Implication: no `AsimovRunQueue`. The production backend's
  `submit_pending` is a no-op (or invokes asimov's own CLI); `poll`
  reads asimov's filesystem layout for posteriors.

### SuperNu, WinNet, SkyNet (kilonova)
- These are external scientific codes. **No functional wrappers exist
  in RIFT today** — both the kilonova prototype (#2) and its Stage 2
  extension (#6 in the original ordering) start from scratch.
- Container sizing matters here. The execution environment for OSG
  needs to bake in the Fortran compiler outputs / shared libs / atomic
  data tables. Sketch of what each needs:
    * **SuperNu**: ~100 MB binary + dependencies. Atomic line lists
      can be tens of MB to a few GB depending on options. Per-sim
      memory: a few GB; per-sim wall time: minutes to hours.
    * **WinNet / SkyNet**: nuclear reaction-network data tables run
      hundreds of MB to ~1 GB. Network library + Fortran/C++ binary
      another few hundred MB. Per-sim wall time: similar.
- Deployment options (in order of preference):
    1. Pre-built Singularity image on cvmfs (large binary + tables
       baked in; pulls fast on OSG nodes). Image size budget: 1–3 GB
       for SuperNu-only, 3–5 GB for the full WinNet+SuperNu stack.
    2. OSDF transfer of large data tables, smaller "tooling" image.
       More complex deployment; useful if the tables update often.
    3. Per-job download (worst). Avoid.
- Open: who builds and maintains the container? Likely a
  collaboration with whoever currently runs SuperNu/WinNet builds at
  RIT. Worth pinning down before writing the backend.

## Implementation order

Agreed with Richard:

1. **GW PE synthetic-targeted** — easiest. No external deps; assembles
   pieces already in `bin/` (`util_RIFT_pseudo_pipe.py` against
   synthesized data). Validates the backend skeleton on a real RIFT
   use case.
2. **Kilonova Stage 1 (SuperNu-only)** — second prototype. Single
   archive, no composition. Container concern is real: needs a
   working SuperNu environment on the run pool. The framework code
   is straightforward; the deployment question dominates.
3. **Backend skeleton refactor** — once two prototypes exist, write
   `_base.py` + the shared `make_archive(...)` helpers based on
   what they actually have in common. Resist the urge to abstract
   before that.

Subsequent (not yet agreed):

4. Synthetic universes prototype with stub generators (proves the
   multi-archive composition pattern end-to-end).
5. basil adapter for the popsynth side.
6. Kilonova Stage 2 (WinNet → SuperNu) — reuses the multi-archive
   pattern from (4).
7. GW PE production via asimov — frontend / curator only, leaky
   abstraction (per Richard's decision above).

## Pre-decided design choices

* **Lower-archive references in `params`:** content-addressable via
  the lower archive's `lookup_key` + `same_q`. The upper archive's
  `params` carries the lower-archive's params; resolution happens in
  the planner step. More robust to lower-archive rebuilds than
  embedding sim_names directly.
* **Where the planner runs:** as a hyperpipeline DAG node (one
  planning round per node). Matches existing infrastructure, no
  daemon to manage, lets users embed planner steps in their workflows.
* **Refinement semantics:** keep `level` as a single integer
  everywhere; per-backend `interpret_level(N)` functions in the
  generator translate (more samples / finer time grid / more
  popsynth realizations). One concept across the framework.
* **GW PE synthetic, what to invoke:** `util_RIFT_pseudo_pipe.py` (the
  documented user-facing CLI). The backend supplies the right
  `--use-ini` and synthetic event params.

## Open per-backend questions (TODO before code)

For backend (1) GW PE synthetic-targeted (resolved):
* Reference: `MonteCarloMarginalizeCode/Code/demo/populations/`
  (Makefile + write_mdc.py + pop-example.ini + setup.sh). The flow
  there is: GWKokab → injections.dat → write_mdc.py converts to
  RIFT-ready injections → pp_RIFT_with_ini builds run dirs →
  condor_submit_dag.
* Data format: `.gwf`. Two modes the backend should support:
    * accept user-provided pre-computed noise frames (drop them in,
      inject the signal into them);
    * generate noise on demand via gwpy (canonical IGWN/aLIGO PSDs
      are fine for synthetic studies).
* Channels follow the demo's convention: `FAKE-STRAIN` per IFO,
  default `flow=20` Hz. Make this configurable in the backend's
  params; default to the demo values.
* The PE invocation **maps directly onto `pp_RIFT_with_ini`** — not a
  parallel implementation. Per Richard: no duplicate code paths to
  maintain. The backend's generator builds the ini + injection input
  exactly as `demo/populations/` does, then shells out to
  `pp_RIFT_with_ini` (located at
  `MonteCarloMarginalizeCode/Code/test/pp/`). Output of the run goes
  into the sim_dir as the level output. Refinement maps to additional
  RIFT iterations / increased convergence requirements via the same
  ini knobs.

For backend (2) Kilonova Stage 1:
* Container: **a student is providing one** (Richard, planning
  session). Coordinate to confirm: image location (cvmfs path or
  osdf URL), what's baked in (SuperNu binary, atomic data tables,
  Python tooling), how a job invokes SuperNu inside the container,
  and rough resource budget per run. The backend should reach the
  container via the `singularity_image` knob already supported by
  `DualCondorRunQueue` / `SlurmRunQueue`.
* SuperNu invocation surface: does the run wrapper take an `.in`
  file, or is there a Python API? Affects how the generator passes
  params.
* Output schema: SED is large-ish; do we store the raw SED in
  `level_<N>.json` (probably) or as a separate `output.h5` (probably
  better)?
* Reference observations — for the adaptive planner step we need
  some target light curves. Use AT2017gfo as the canonical reference;
  worth pinning down which dataset and where it lives.

For backend (3) Backend skeleton refactor:
* Decide AFTER (1) and (2) ship. Don't draft the abstraction yet.
