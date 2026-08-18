# Contract boundaries for cross-domain inference

> **Status:** Directional architecture only. This record authorizes no
> implementation, API, dependency, migration, production claim, or scientific
> claim. Every slice requires separate review.

## Purpose

RIFT is the proving ground for reusable inference around expensive models.
Reuse should not force radiative-transfer, hydrodynamic, population, or other
projects to import GW science, RIFT's dependency stack, its scheduler, or its
storage implementation. Four narrow contracts keep those concerns separate.

## 1. Domain science

The domain project owns the meaning of its scientific payloads:

- parameters, shapes, units, coordinates, and transformations;
- prior or population context;
- fidelity and approximation controls;
- observables, objectives, uncertainty, and normalization; and
- scientific validity and domain-specific failure conditions.

Other layers may carry a domain-contract identifier and opaque payload, but
must not reinterpret them. Different domains need not share a payload schema.

## 2. Evaluation request and result

This contract is the narrow computational seam. A request identifies one
logical evaluation under a domain contract. A correlated result reports a
bounded outcome, payload, declared uncertainty, cost, and provenance.

It does not prescribe a scheduler, queue, container, archive, database, array
library, or proposal strategy. One logical evaluation may involve batching,
fan-out, retries, caches, emulators, or a costly native simulation.

An evaluation result is not an archive record. Completion of a scheduler job
or DAG is not controller assimilation. Adapters must expose these transitions
honestly rather than treating file existence or process success as science.

## 3. Archive read

Archive read discovers retained evaluations and retrieves records and artifact
metadata. It is separate from work registration, execution, mutation, and
artifact transport.

The behavior is backend-neutral. JSON, JSONL, SQLite, object storage, or a
service may implement it according to scale and query needs. A conforming
adapter does not require projects to converge on one storage engine.

Operational conformance will eventually need bounded pagination, ordering,
identity scope, and snapshot-consistency semantics. Vocabulary or mechanical
evaluation evidence does not establish those properties.

## 4. Campaign controller

The controller owns inference policy:

- campaign and iteration state;
- proposal or sampling-oracle lifecycle;
- creation and correlation of evaluation requests;
- atomic result assimilation;
- convergence and escalation decisions; and
- restart behavior for delayed, duplicated, or failed work.

It may use the evaluation and archive-read contracts while depending on domain
semantics. A controller checkpoint is not an archive record. The controller
does not own native simulation formats, scheduler syntax, scientific kernels,
or artifact transport. HyperPipe can approach this role incrementally without
rewriting supported O4 workflows.

## Dependency direction

These arrows describe import or build dependencies, not runtime data flow:

- controller -> domain, evaluation, and optionally archive-read contracts;
- evaluator adapter -> domain and evaluation contracts plus native runtime;
- archive adapter -> archive-read contract plus native archive or client.

No contract package imports an adapter, native runtime, archive implementation,
or controller. Contract packages must not import RIFT, LALSuite, JAX, SuperNu,
scheduler clients, or other project science stacks. Projects own adapters and
may depend on neutral contracts; neutral contracts must not depend on projects.

## Compatibility and evolution

Supported O4 CLIs, Python entry points, defaults, file products, shapes and
ordering, restart behavior, and numerical tolerances are candidate Tier-A
surfaces. Each implementation issue must enumerate the exact promises it makes.

New contracts and adapters remain additive or opt-in until a separately
reviewed migration says otherwise. A field or meaning change in a closed
contract requires a new version, with an overlap window for active adopters.
Missing compatibility evidence is neither permission to break a caller nor an
automatic blocking gate.

## Honest maturity

Evaluation and atomic assimilation have separately reviewed cross-domain
mechanical evidence. That evidence is experimental: it does not establish a
stable API, production dependency, operational archive read, or scientific
validity.

Proposal lifecycle, archive-read operations, restart, convergence, and
supported HyperPipe controller integration remain open. Each requires a
bounded issue, supported consumer, compatibility declaration, tests, rollback,
and adversarial review. Historical proof artifacts are not part of these
durable boundaries.
