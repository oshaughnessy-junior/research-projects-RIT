# Architecture roadmap

> **Status:** Strategic gates only. This roadmap authorizes no implementation,
> adoption, dependency, registry declaration, production claim, or scientific
> claim. Each runtime or project change requires a separate reviewed issue.

## Current state

The neutral public owner and experimental release now exist. RIFT and R3 each
completed separately reviewed, opt-in test validation against that release.
RIFT's incubation-comparison edge is useful historical evidence and is
intentionally absent from the durable target tree.

These results establish only cross-domain mechanical evidence for evaluation
and assimilation. They do not establish a stable API, production dependency,
controller integration, operational archive-read conformance, or scientific
validity. Proposal lifecycle, restart, convergence, and supported HyperPipe
controller integration remain open.

## Gate 1: supported RIFT consumption

The next runtime work should begin with a supported RIFT consumer, not another
retained proof record.

- Select one bounded consumer and enumerate its Tier-A behavior.
- Propose opt-in use of the neutral evaluation or assimilation capability with
  a project-specific rollback.
- Compare scientific and operational behavior with the supported native path.
- Keep archive persistence, scheduling, proposal policy, and default changes
  outside the slice unless independently authorized.

Exit evidence: a reviewed RIFT path consumes an explicitly versioned neutral
contract without changing supported defaults or claiming production
conformance beyond the tested surface.

## Gate 2: adaptive inference under stress

- Reference-test trained sampling oracles on representative difficult cases.
- Harden loud-signal and three-generation-interferometer integration with
  shape, calibration, and effective-sample-size checks.
- Exercise the corresponding backend-neutral capability in a scientifically
  meaningful non-GW case before making a cross-domain claim.
- Keep JAX optional and CI-maintained; the non-GW implementation need not use
  JAX.

Exit evidence: measured robustness or cost improves without weakening Tier-A
behavior, and the portability claim depends on contracts rather than an array
backend.

## Gate 3: challenge the boundaries

Add a second non-GW domain, preferably a hydrodynamic, numerical-simulation, or
population-inference application. Use it to challenge identity, uncertainty,
failure, cost, and provenance assumptions rather than merely translating a
fixture.

Operational archive reads, restart semantics, convergence policy, and a
supported HyperPipe controller path each require their own gates. Evaluation
success does not authorize archive or controller conformance.

Exit evidence: independently owned science can use the narrow seams without
importing RIFT, adopting its storage, or sharing its dependency cadence.

## Separate registry and sentinel track

Registry or drift-sentinel declaration follows only after a dependency edge is
intentionally retained in final project trees. The registry observes declared
versions and evidence; it owns no protocol or science semantics. Its scheduled
runner owns operations only and cannot waive compatibility or scientific
review.

## Decision discipline

For every proposed slice, record the outcome, affected edges, compatibility
surface, tests, rollback, budget, and stop conditions. Require an independent
adversarial review before landing. Defer work that is primarily cleanup,
duplicates a workflow system, or demands simultaneous multi-repository
migration.
