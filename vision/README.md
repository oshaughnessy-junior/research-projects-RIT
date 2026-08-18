# RIFT long-term vision

> **Status:** Directional architecture only. This document authorizes no code,
> dependency, API, default, workflow, scientific claim, or project adoption.
> Every implementation slice needs a separate reviewed issue, bounded scope,
> compatibility declaration, tests, stop conditions, and adversarial review.

## North star

RIFT should be a proving ground for durable inference around expensive models,
not the boundary of that architecture. Gravitational-wave inference sharpens
the tools through high-SNR signals, multiple instruments, strict statistical
semantics, and large campaigns. The reusable seams should transfer to
radiative transport, hydrodynamic and numerical simulations, population
inference, and other non-GW applications that will be primary future users.

The goal is not to put every science project inside RIFT. It is to maintain a
small set of explicit contracts so independently owned projects can reuse
inference capabilities without sharing a science model, storage backend,
scheduler, or dependency cadence.

## Architectural responsibilities

The architecture keeps four responsibilities distinct:

1. **Domain adapters** own parameters, units, coordinates, priors,
   transformations, validity, and native model execution.
2. **HyperPipe as adaptive inference controller** owns proposal or oracle
   policy, result assimilation, iteration state, convergence, escalation, and
   restart, introduced incrementally around supported workflows.
3. **The simulation and evaluation substrate** executes costly work and
   retains identity, uncertainty, failure, cost, and provenance without
   imposing one scheduler or storage backend.
4. **Numerical capabilities** provide integrators, fits, surrogates, emulators,
   and posterior construction behind tested interfaces.

Those responsibilities interact through separate domain-science, evaluation,
archive-read, and campaign-controller contracts. Scheduler success is not
scientific assimilation, an evaluation result is not an archive record, and a
controller checkpoint is not an archive record. The detailed seams are in
[Contract boundaries](CONTRACT_BOUNDARIES.md).

## Selective JAX

JAX is a targeted numerical tool, not the architecture. Near-term value lies in
better trained sampling oracles and robust integration for loud signals and
three-generation-interferometer regimes. A retained JAX path should have a
scientific reference comparison, enough CI coverage to prevent silent decay,
and a supported non-JAX path where compatibility requires it.

A cross-domain claim additionally requires a backend-neutral, scientifically
meaningful non-GW transfer case. That application need not use JAX internally
and must not acquire JAX merely to demonstrate portability.

## Compatibility and storage

Practical O4 compatibility is a design constraint. Each implementation issue
must name its Tier-A surfaces, including relevant CLIs, Python entry points,
file products, defaults, shapes and ordering, restart behavior, and numerical
tolerances. New behavior is additive or versioned until a separately reviewed
migration changes a supported surface.

Persistence contracts remain backend-neutral. JSON, JSONL, SQLite, object
storage, or a service can satisfy a behavioral contract when appropriate.
Projects retain their native storage and execution systems; shared contracts
must not impose a backend or import a project's science stack.

## Evidence and authorization

Normative architectural decisions may live in `vision/`. Run artifacts, proof
records, and historical observations belong in the `coding-rift` SOR or the
project that owns them. Regression fixtures remain in RIFT only when they test
supported RIFT code. Architecture documents should link durable public
material rather than retaining incubation debris.

Current mechanical evidence informs the direction but does not establish a
stable API, production conformance, scientific validity, or runtime adoption.
The accepted neutral ownership decision is summarized in
[Contract ownership](CONTRACT_OWNERSHIP_DECISION.md); adoption and registry
declarations remain separate project decisions.

## Guardrails for future work

Prefer the narrowest supported consumer slice that advances a strategic gate.
Before implementation, identify the scientific outcome, affected contract and
dependency edges, compatibility tier, reference evidence, resource budget, and
stop conditions. Integrator work remains subject to RIFT's shape-recovery
merge gate.

Do not treat this vision as permission for repository-wide cleanup, API
renaming, dependency upgrades, a full JAX rewrite, storage convergence, or
replacement of working schedulers. Do not infer adoption from experimental
validation. Any proposed feature should receive independent adversarial review
for scientific semantics, hidden domain assumptions, compatibility, and
operational failure modes before landing.
