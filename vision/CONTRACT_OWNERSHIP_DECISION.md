# Shared inference-contract ownership decision

> **Status:** proposed decision for Issue #147. This document authorizes no
> repository creation, package scaffold, release, dependency, registry edge,
> CI gate, adapter migration, API/default change, or deployment.

## Decision

Use a small, neutral canonical repository for the cross-domain evaluation and
campaign-assimilation contracts. The proposed repository identity is
`oshaughnessy-junior/inference-campaign-contracts`, with a corresponding
`inference_campaign_contracts` Python namespace if a package is separately
authorized. The proposed repository is public and MIT-licensed, matching the
incubation source; it contains no private gate evidence or native science data.

RIFT is the incubator and first proving ground, not the permanent protocol
owner. The registry/drift-sentinel repository is an observer, not the protocol
owner. Project repositories continue to own their adapters, domain schemas,
scientific policies, and native execution or persistence systems.

The first extraction, if separately approved, is limited to:

- the closed evaluation request/result schemas;
- the closed campaign-assimilation transition schema;
- the standard-library assimilation validator/reducer; and
- normative contract documentation and reference acceptance tests.

It excludes proposal implementations, archives, schedulers, transports,
scientific payload schemas, RIFT or SuperNu adapters, native fixtures, run
records, and historical proof artifacts.

## Why this option

The private RIFT/R3 gate established that genuine non-GW records can cross the
evaluation and assimilation boundaries, including an assimilated result, a
deferred indeterminate result, rebuild, exact replay, and proposal consumption.
It did not establish production conformance. The gate also exposed the durable
dependency problem: leaving the canonical reducer in RIFT makes non-GW users
depend on a GW codebase, while copying validators into each project invites
semantic drift.

A neutral repository gives the contracts an owner and release cadence without
making every adopter inherit RIFT, LALSuite, JAX, NumPy, SuperNu, a scheduler,
or an archive backend. It also gives the drift sentinel a real version edge to
observe after adoption, rather than asking it to infer compatibility from
project source trees.

## Alternatives considered

### Keep RIFT as the canonical source

Rejected for cross-domain production use. This is the smallest immediate move,
but it gives RIFT's branch and release cadence authority over non-GW projects
and encourages imports or file copies from a large scientific dependency tree.
The existing RIFT copy remains the incubation source until an extraction is
authorized.

### Keep a canonical file set in RIFT and pin external Git revisions

Rejected as the durable model. Exact revision pins improve reproducibility but
do not solve ownership, discovery, deprecation, or independent releases. They
remain an acceptable temporary gate technique.

### Let the registry/drift sentinel own the contracts

Rejected. The registry records adopted versions, ownership declarations, and
compatibility evidence. Owning scientific protocol semantics would conflate
observation with authority and let an operational monitor become a dependency
of the systems it observes.

### Vendor a copy in every adopter

Rejected. Vendoring avoids a runtime dependency but creates multiple canonical
copies and makes bug fixes, closed-schema changes, and replay semantics drift.
If an air-gapped deployment must vendor an artifact, it must retain the exact
upstream version and digest and remain a distribution of the canonical source.

### Build a general simulation-inference framework now

Rejected. Two mechanical adapters justify a narrow protocol owner, not a new
scheduler, archive, controller framework, query language, or domain model.

## Namespace and release policy

The extraction issue must confirm the proposed repository and import namespace
before creating either. Contract identifiers remain independently versioned
from the package release. The current `draft/v0` identifiers are experimental;
moving files must not silently promote or rename them.

For an adopted closed contract:

- a field addition, removal, or meaning change requires a new contract version;
- patch releases may fix implementation defects only when accepted/rejected
  instances and transition semantics do not change;
- active adopters receive a declared overlap window for the old and new
  contract versions;
- the canonical repository publishes schemas as package data and keeps the
  reference reducer standard-library only; and
- no release may import project science stacks or adapter implementations.

The initial extraction issue must name the supported Python floor and release
channel. It must not add the package to RIFT or R3 runtime dependencies in the
same change.

## Compatibility budget and adoption sequence

Extraction must preserve the selected schema and reducer source bytes, and must
preserve accepted/rejected instances and transition semantics. Any intentional
semantic difference requires a new contract version. The reference test corpus
must cover validation, atomic compare-and-set, replay, conflict, attempt
monotonicity, rebuild, input bounds, and mutation isolation.

Adoption proceeds through separate project issues:

1. publish the neutral source and verify import isolation;
2. compare it against the pinned RIFT incubation implementation;
3. let RIFT adopt it through an opt-in or test-only edge with no supported
   workflow/default change;
4. let R3 adopt the same released version without importing RIFT; and
5. only then authorize the registry track to record the real dependency edges.

Each project may defer adoption on its own release cadence. Until both adapters
exercise one released version, no production-conformance or stable-cross-domain
claim is permitted. Every adoption issue must name its own rollback with native
data and entry points unchanged. RIFT may temporarily fall back to its pinned
local incubator. R3 and other non-GW projects disable the optional edge or pin
the prior neutral release; they must never vendor the RIFT copy or import RIFT
as rollback.

## Ownership and authority

| Responsibility | Accountable authority |
| --- | --- |
| Protocol namespace, schemas, reducer semantics, releases | Named maintainers of the neutral protocol repository, initially approved by Richard O'Shaughnessy |
| RIFT adapter and GW scientific semantics | RIFT owners / `coding-rift` review process |
| R3/SuperNu adapter and radiative-transfer semantics | R3 and SuperNu project owners |
| Later population, hydro, or numerical adapters | Each adopting project's scientific owners |
| Portfolio registration of adopted version edges | `meta-manager` registry administration, through a separate registry task |
| Runner health and delivery | The named runner operator for each monitored group |
| Contract-compatibility exceptions | Jointly the protocol owner and affected adopter; never the registry or runner alone |
| Scientific exceptions | Solely the affected project's scientific review process; never the protocol owner, registry, or runner |

At least one maintainer from outside the RIFT-only workflow should approve a
stable contract release. This is a governance check, not a requirement that all
projects share one dependency environment or release cadence.

## Required follow-on issue

If this decision is accepted, open one bounded extraction issue. Its stop
condition is a repository shell containing byte-identical approved schemas and
reference reducer, normative documentation, reference acceptance tests,
import-isolation checks, and a provenance comparison to the RIFT incubation
commit. It must not add package/build metadata, publish a release, introduce a
convenience SDK/API or new abstraction, change extracted semantics, claim
conformance, integrate an adopter, perform a migration, or modify RIFT, R3,
HyperPipe, native archives, schedulers, CI gates, or the registry. Packaging and
release require a later authorized issue after all governance fields are
confirmed.

Separate later issues own project adoption and registry declaration. If the
repository name, accountable owner, release channel, Python floor, or initial
adopters cannot be confirmed, stop before creating the repository.
