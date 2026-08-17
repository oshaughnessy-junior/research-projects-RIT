# Drift Sentinel Plan

## Purpose

The drift sentinel is a small, reusable protocol and code module for detecting
contract drift across related scientific repositories. It is intended to make
cross-project changes visible early without turning RIFT into a portfolio-wide
workflow manager or dependency updater.

The sentinel is distinct from the scheduled job that invokes it:

- **Protocol and code module:** defines the registry, graph model, checks,
  evidence, and report format. It runs locally or in CI and has no scheduler
  assumptions.
- **Runner job:** selects a registry and revisions, invokes the module on a
  cadence, retains reports, and notifies owners. Scheduling, credentials,
  retention, and notification policy belong to the runner's deployment
  repository or operations workspace, not to this module.

The durable storage, ownership, escalation, retention, and threat model are
specified in [DRIFT_SENTINEL_OPERATIONS.md](DRIFT_SENTINEL_OPERATIONS.md).

The first pilot is the RIFT/SuperNu boundary. The design must remain usable for
other costly-simulation inference campaigns, including hydrodynamics,
radiative-transfer, and population-inference projects.

## Phase 0/1 reference implementation

The bounded offline core lives at
`MonteCarloMarginalizeCode/Code/rift_drift_sentinel/`. It is a standalone Python
package distributed from the RIFT repository but deliberately outside the
`RIFT` import namespace so it can run without importing RIFT, lalsuite, JAX, or
other science stacks. Its focused tests live at
`MonteCarloMarginalizeCode/Code/test/drift_sentinel/`.

The checked-in registry is a redacted, non-authoritative pilot. The proposed
authoritative desired-state location is the private
`oshaughnessy-junior/drift-sentinel-registry` repository; compact observed run
records use a separate private `oshaughnessy-junior/drift-sentinel-runs`
repository or equivalently protected append-only store. Neither repository is
created or deployed by Phase 1.

## Non-goals and safety boundary

The sentinel is passive and read-only. It may inspect repositories, installed
metadata, declared environments, and explicitly registered fixtures. It must
not:

- upgrade or pin dependencies;
- edit, commit, merge, or push repository content;
- rewrite or migrate stored data;
- submit scientific jobs or alter campaign state;
- decide that a scientific difference is acceptable without an owner-recorded
  exception;
- grow into a general orchestration, package-management, or workflow system.

The core accepts already-resolved local inputs and, optionally, a prior report.
It does not fetch repositories, hold credentials, schedule work, persist report
history, or send notifications. It must remain dependency-light and must not
import RIFT, lalsuite, JAX, or project scientific stacks; project-specific
imports belong in optional adapters or fixture-generation environments.

A finding is evidence for a human or owning agent to evaluate. It is not an
authorization to repair the affected project.

## Registry and graph model

One registry may describe multiple named project groups. Each group is a
directed acyclic graph whose nodes are versioned projects, protocols, schemas,
or deployment artifacts and whose edges declare a dependency or exchanged
contract. A registry may contain disconnected groups; no artificial root or
shared release cadence is required.

The registry should contain only stable coordination metadata:

- project identifier, source location, default ref, and accountable owner;
- node role, such as producer, consumer, protocol, library, or runner;
- directed edges with the contract(s) exchanged across each edge;
- files or commands that expose versioned schemas and public interfaces;
- registered golden fixtures and their expected content identities;
- supported dependency ranges and environment markers;
- severity policy and documented, expiring exceptions.

Project-specific discovery belongs in small adapters. Core graph traversal,
comparison, evidence collection, and reporting must not contain RIFT- or
SuperNu-specific assumptions.

## Initial checks

Checks are enabled explicitly per edge. The minimum useful set is:

1. **Revision identity:** report the resolved revision for every node and, when
   history is available, the first divergent revision relevant to a finding.
2. **Callable and CLI surfaces:** compare registered function signatures,
   command options, exit conventions, and required/optional fields.
3. **Schemas and status vocabularies:** compare version identifiers, field
   names, types, requiredness, enumerations, and compatibility declarations.
4. **Scientific semantics:** verify registered units, coordinate systems,
   parameter names, priors, normalization conventions, and uncertainty
   representations.
5. **Content identity:** compare declared canonicalization rules and hashes so
   equivalent artifacts have stable identities across repositories.
6. **Dependency compatibility:** identify empty or risky intersections among
   supported Python, NumPy, JAX, CUDA, MPI, HDF5, lalsuite, and other registered
   ranges. The sentinel reports conflicts; it does not solve them.
7. **Golden round trips:** run small, deterministic producer-to-consumer and
   migration fixtures, checking schema, semantics, and canonical hashes rather
   than merely successful process exit.

Checks should prefer declared metadata and tiny fixtures over importing an
entire scientific stack. A check that requires expensive simulation is outside
the sentinel and should instead validate a previously produced fixture.

## Report contract

Every run emits one machine-readable report plus a concise human summary. Each
finding includes:

- project group, source node, target node, and contract identifier;
- owner and severity;
- check name and observed versus expected values;
- resolved revisions and, where determinable, the first divergent revision;
- reproducible evidence and the smallest command needed to repeat the check;
- applicable exception, including its rationale and expiry;
- observation status: observed, indeterminate, or intentionally divergent.

Given a prior report, a pure comparator in the module may additionally classify
findings as new, unchanged, or resolved without persisting either report. The
runner supplies the prior report and may suppress unchanged notifications.
Missing evidence is `indeterminate`, never a passing result.

## RIFT/SuperNu pilot

The pilot should exercise one narrow scientific handoff before expanding the
registry:

- register the authoritative RIFT and SuperNu-related repositories and owners;
- identify one real producer/consumer boundary used by the current
  RIFT/SuperNu effort;
- write down its schema, units, coordinates, normalization, uncertainty, and
  content-identity contract;
- add one valid golden round trip and a few deliberately incompatible fixtures;
- verify that a report locates the owning edge and first divergent revision;
- run the passive check in both projects' CI without changing production
  behavior.

The pilot is successful when an incompatible boundary change fails its
compatibility gate with actionable evidence while the current compatible path
continues to pass.

## Delivery phases

### Phase 0 — contract inventory

Document the pilot edge, owners, current compatibility commitments, and one
golden artifact. Choose the smallest serialization and registry formats that
can express the pilot and a disconnected second group. Do not implement a
generic plugin framework.

### Phase 1 — offline MVP

Implement registry and DAG validation, the report contract, and one narrow
schema or golden-fixture check on the pilot edge. Demonstrate two disconnected
groups in one registry, with only the RIFT/SuperNu group required to use real
repositories. Revision attribution, general API inspection, dependency-range
analysis, and additional check types are separate later increments.

### Phase 2 — compatibility gates and CI

Add fast, deterministic CI jobs that run on changes to registered interfaces,
schemas, fixtures, or dependency declarations. Publish the machine report as
an artifact and a short summary as the check result. Gate only declared
breaking changes and malformed contracts initially; observation-only findings
remain non-blocking until owners explicitly promote them.

### Phase 3 — scheduled runner

In a separate operations change, configure a scheduled job to fetch and resolve
the registered refs, provide credentials, invoke the released module, retain
and compare reports, and route new or resolved findings. The runner consumes
the protocol; it does not fork or embed sentinel logic.

Further project groups are added only after the pilot catches a real or seeded
incompatibility without producing excessive noise.

## Compatibility gates

The following gates protect existing users and campaigns:

- no change to current RIFT or SuperNu CLIs, file formats, defaults, or runtime
  behavior is required to adopt the sentinel;
- existing unversioned interfaces are first recorded as baseline contracts,
  not silently reclassified as errors;
- contract evolution uses explicit versions, adapters, or a documented overlap
  window with golden fixtures for both paths;
- newly blocking checks require named owners, deterministic reproduction, and
  an agreed severity policy;
- a sentinel implementation change must pass its own registry/fixture
  compatibility suite before release;
- a temporary exception must name an owner, rationale, affected edge, and
  expiry date.

## Bounded first iteration

The first coordinated iteration is capped at 24 hours and at 25% of the total
weekly agent quota for the group, whichever limit is reached first. Its allowed
outputs are the pilot contract inventory, registry/report schemas, one minimal
offline schema or golden-fixture check, focused tests, and an adversarially
reviewed implementation proposal. Its completion criteria are:

- inventory one owned RIFT/SuperNu contract edge and record its current
  compatibility commitment;
- validate a registry containing that group and one small disconnected group;
- emit the report schema from one tiny compatible and one seeded incompatible
  fixture; and
- record evidence, uncertainties, quota/time used, and one next increment.

The first iteration explicitly defers dependency-range analysis, broad API
introspection, history attribution, real-repository CI changes, and runner
deployment. It must not expand into repository-wide cleanup, dependency
upgrades, broad API standardization, production scheduling, or science
execution.

Work stops at the cap with a short evidence-backed status: what was validated,
what remains uncertain, quota/time used, and the single highest-value next
increment. Any feature proposed for landing receives an independent
adversarial review focused on backward compatibility, scientific semantics,
false confidence, and scope creep.

The coordinator alone may spawn work for this iteration. Deliverables and
maximum concurrency must be declared before delegation, and recursive
delegation is forbidden unless it was included in that budget. If reliable
quota metering is unavailable, use a conservative task and concurrency ceiling
and stop rather than estimating optimistically.

## Phase 2 exit criteria

These are later program criteria, not a commitment for the bounded first
iteration.

Phase 2 is complete only when:

- one registry represents at least two project groups, including a disconnected
  group, and validates each DAG;
- the RIFT/SuperNu pilot has a declared owner and one meaningful contract edge;
- incompatible API/schema semantics, dependency ranges, and golden artifacts
  yield stable findings with severity and revision evidence;
- compatible baselines pass without requiring application-code changes;
- CI runs are deterministic, fast, passive, and retain machine-readable output;
- the code module can be invoked without the scheduled runner; and
- documentation states clearly that remediation and job submission remain
  outside the sentinel.
