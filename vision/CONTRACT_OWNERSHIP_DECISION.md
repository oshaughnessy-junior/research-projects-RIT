# Shared inference-contract ownership decision

> **Status:** Accepted architecture decision. It records ownership but
> authorizes no project adoption, dependency, registry declaration, API or
> default change, migration, production claim, or scientific claim.

## Decision

The canonical owner is the public, MIT-licensed
[`oshaughnessy-junior/inference-campaign-contracts`](https://github.com/oshaughnessy-junior/inference-campaign-contracts)
repository. Its current experimental validation release is
[`v0.1.0a1`](https://github.com/oshaughnessy-junior/inference-campaign-contracts/releases/tag/v0.1.0a1).
That release is not a stable API or a production-conformance claim.

The neutral repository owns the evaluation request/result schemas, campaign
assimilation schemas, and reference reducer semantics. It must remain
dependency-light and must not import project science stacks, adapters,
schedulers, archive implementations, or native execution systems.

## Project and operational ownership

Adopting projects own:

- domain science and payload schemas;
- adapters to the neutral contracts;
- scientific validation and compatibility surfaces;
- native model execution, scheduling, and persistence; and
- adoption, rollback, migration, and deprecation on their release cadence.

The registry observes intentionally declared contract versions and dependency
edges but owns no contract or scientific semantics. A scheduled runner owns
monitoring operations and delivery only. Neither registry nor runner can waive
protocol compatibility or project scientific review.

## Evidence and nonclaims

RIFT and R3 completed separate, opt-in test validation against the experimental
release. This is mechanical evaluation and assimilation evidence, not runtime
adoption, stable API evidence, production conformance, or cross-domain
scientific validity. RIFT's incubation comparison is historical evidence and
is intentionally not retained in the durable target tree.

Public RIFT context remains in the
[`research-projects-RIT`](https://github.com/oshaughnessy-junior/research-projects-RIT)
repository and the bounded
[`#155` integration issue](https://github.com/oshaughnessy-junior/research-projects-RIT/issues/155).
Private R3 and SOR evidence is not linked or copied into public RIFT documents.

## Authority for exceptions

A compatibility exception requires agreement from both the neutral protocol
owner and every affected adopter. The registry and runner may report an
exception but cannot approve it.

A scientific exception belongs solely to the affected project's scientific
review process. The protocol owner, another adopter, registry, and runner have
no authority to reinterpret domain science.

## Adoption and lifecycle

Each project adopts a released contract through its own separately reviewed
issue. That issue must name supported surfaces, tests, rollback, dependency
budget, release interaction, and stop conditions. Opt-in test validation does
not create a retained runtime edge.

Rollback, deprecation, and migration remain project-specific. Contract changes
are additive or versioned, with overlap for active adopters; they do not force
simultaneous releases or shared dependency environments. A registry declaration
is considered only after an adopted edge is intentionally retained in a final
project tree.

This decision does not authorize archive-read conformance, HyperPipe controller
integration, proposal lifecycle, restart semantics, or any runtime change.
