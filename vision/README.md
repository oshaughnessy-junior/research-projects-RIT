# RIFT long-term vision

> **Status:** This is a directional record, not implementation authorization.
> It authorizes no code, API or default, dependency, CI-gate, branch, or
> deployment change. Each implementation slice requires a canonical SOR issue,
> named owner, bounded budget and stop condition, compatibility declaration,
> tests, and independent adversarial review.

## North star

RIFT should become a durable inference architecture for science in which the
forward model is expensive, campaigns are large, and correctness matters more
than adopting any particular numerical fashion. Gravitational-wave inference
is the proving ground: its high signal-to-noise cases, multiple instruments,
strict statistical semantics, and production scale expose weaknesses early.
The architecture should then transfer to radiative-transfer, numerical and
hydrodynamic simulations, population inference, and other costly models.

The goal is not to turn every scientific project into RIFT. It is to extract a
small set of stable inference and campaign contracts so domain-specific models
can share orchestration, evidence, diagnostics, and proven numerical tools.

## Intended architecture

The system should separate four responsibilities:

1. **Domain adapters** define parameters, priors, units, transformations,
   likelihood semantics, and how a model evaluation is requested and decoded.
   Gravitational-wave, SuperNu, hydrodynamic, population, and EOS applications
   remain free to express their own science.
2. **HyperPipe** is the adaptive inference controller. It decides what to
   evaluate next, chooses or trains proposals and sampling oracles, manages
   iteration state, and applies explicit convergence and escalation policies.
3. **The simulation and evaluation substrate** executes costly work, records
   immutable inputs and outputs, caches by scientific identity, and exposes
   provenance, failure, and uncertainty consistently across local and batch
   resources.
4. **Numerical capabilities** provide interchangeable integration, fitting,
   interpolation, surrogate, and posterior-construction methods behind tested
   contracts. Their internal implementation is not a workflow API.

This separation is the architectural direction, not a mandate for a disruptive
rewrite. Existing RIFT workflows may be migrated incrementally only through
separately authorized adapters and versioned contracts while remaining usable.

## Point of the spear

The first cross-domain proof is RIFT plus SuperNu. The pair is deliberately
demanding: RIFT supplies a mature, high-throughput inference loop with difficult
high-SNR and multi-instrument cases, while SuperNu supplies a non-GW,
simulation-dominated application with different parameter, scheduling, and
failure semantics. A capability is not convincingly general merely because it
works for two gravitational-wave analyses.

Work should favor thin end-to-end slices that run in both domains over broad
framework construction. The initial shared slice should demonstrate a versioned
evaluation request, durable result/provenance record, adaptive HyperPipe step,
and reproducible posterior or campaign diagnostic.

## JAX's role

JAX is a targeted numerical capability within this architecture, not the new
architecture itself. Near-term uses are better trained sampling oracles and
more robust integration for loud-signal and three-generation-interferometer
regimes. Each retained JAX path must:

- have a scientifically meaningful reference comparison;
- run in CI often enough to prevent silent decay;
- preserve a supported non-JAX path where compatibility requires it.

Before a JAX-backed capability or its contract is described as cross-domain,
the backend-neutral oracle or integrator contract must be exercised in a
scientifically meaningful non-GW case. That case need not use JAX internally,
and SuperNu must not acquire a JAX dependency merely to demonstrate transfer.

A JAX implementation earns broader use through measured robustness,
calibration, throughput, and maintainability—not through backend uniformity.

## Contracts and compatibility

Shared interfaces must make scientific meaning explicit: schema version,
coordinates, units, priors, normalization, uncertainty, content identity,
failure state, and provenance. File formats and command-line interfaces may
remain as adapters, but must not be the only definition of a contract.

Archive contracts are backend-neutral. RIFT's JSONL archive is one native
implementation, not a required storage format; other implementations may use
plain JSON, SQLite, object storage, or another representation appropriate to
their scale and query needs. Adapters satisfy the shared behavioral contract
without forcing backend convergence. Lightweight archives should remain
lightweight rather than acquiring a database solely for uniformity.

Compatibility is tiered:

- production O4 command lines, formats, and default scientific semantics remain
  supported unless a separately reviewed migration retires them;
- each implementation issue enumerates its Tier-A compatibility surfaces,
  considering supported Python imports and call signatures, output schemas,
  shapes, dtypes and ordering, restart and checkpoint behavior, and agreed
  numerical tolerances in addition to command lines, formats, and defaults;
- unrecorded or uncertain surfaces begin as observation-only evidence and must
  not be silently treated as either compatible or blocking;
- new behavior enters through versioned contracts, explicit opt-ins, or
  adapters;
- migrations use golden fixtures and, where scientifically material, dual-run
  comparisons before defaults change; and
- dependency changes are treated as cross-project interface changes, not local
  maintenance.

The cross-project drift sentinel is a separate, small governance capability. It
should observe these contracts and dependency relationships without becoming
part of HyperPipe's scientific control loop.

## Measures of progress

Progress is demonstrated by outcomes, not code movement:

- a RIFT/SuperNu campaign can share an evaluation and provenance contract;
- HyperPipe can control a costly simulation loop without importing GW-specific
  assumptions;
- loud-signal and multi-instrument cases pass shape-sensitive validation, not
  only scalar-integral checks;
- a maintained JAX capability runs in CI, while any claim of cross-domain
  generality is supported by a non-GW exercise of its backend-neutral contract;
- old production entry points continue to reproduce agreed reference results;
  and
- cross-project contract or dependency drift is detected before a campaign is
  launched.

## Guardrails for future agents

Future work should begin with the narrowest end-to-end scientific capability
that advances this vision. Before implementation, identify the contract being
changed, its current consumers, the compatibility tier, and the validation
evidence required. Integrator changes must satisfy the repository's
shape-recovery merge gate.

Do not use this vision as authorization for repository-wide cleanup, API
renaming, dependency upgrades, a full JAX rewrite, or replacement of working
schedulers. Such work requires a concrete scientific or compatibility need.
Keep domain policy in adapters, keep execution records durable, and prefer
incremental migrations that can be compared with production behavior.

Any feature proposed for landing should receive an independent, adversarial
review focused on scientific semantics, hidden domain assumptions, backward
compatibility, and operational failure modes.
