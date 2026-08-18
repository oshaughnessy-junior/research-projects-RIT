# Contract boundaries for cross-domain inference

> **Status:** directional architecture record, not implementation
> authorization. This document changes no API, default, archive, scheduler,
> dependency, CI gate, or scientific behavior. Each implementation slice needs
> a canonical SOR issue, owner, budget and stop condition, compatibility
> declaration, tests, and independent adversarial review.

## Why these boundaries exist

RIFT is the proving ground, not the limit of the architecture. The reusable
system must support costly likelihoods and simulations in radiation transport,
hydrodynamics, population inference, and other domains without importing GW
assumptions or RIFT's dependency stack.

The current systems contain useful but overlapping ideas: HyperPipe constructs
iterative workflows; marginalization drivers evaluate rows from a file-backed
grid; simulation managers register, execute, and retain expensive work; and
archives expose accumulated state. Treating all of that as one universal API
would couple science semantics, campaign policy, execution, and persistence.
Instead, the architecture uses four narrow contracts and backend adapters.

## The four contracts

### 1. Domain science contract

The domain contract defines the meaning of scientific payloads:

- parameter names, shapes, units, coordinates, and transformations;
- prior or population context;
- fidelity and approximation controls;
- returned observables, objective values, uncertainty, and normalization; and
- domain-specific validity and failure conditions.

It is owned and versioned by the scientific project. An archive or controller
may carry its identifiers and opaque payloads, but must not reinterpret them.
GW and SuperNu payloads need not share a schema to use the same evaluation and
controller contracts.

### 2. Evaluation request/result contract

This is the narrow cross-domain computational seam. One request identifies a
domain contract and asks for one logical evaluation. One result correlates to
that request and reports a stable outcome category, domain payload, declared
uncertainty, cost diagnostics, and provenance.

The contract must eventually specify idempotency, attempt versus logical-work
identity, retry-safe correlation, partial or indeterminate results, bounded
diagnostics, and version negotiation. It does not specify a scheduler, queue,
container, archive layout, database, array library, or proposal strategy.

An implementation may execute immediately in-process, submit a batch job,
consult a cache or emulator, or dispatch a costly numerical simulation. One
logical evaluation need not correspond to one scheduler job: batching,
fan-out, retries, and deduplication are adapter or transport capabilities.
Those choices are not different science contracts.

### 3. Archive read contract

The archive contract discovers retained simulations/evaluations and retrieves
their records and artifact metadata. It is backend-neutral: JSONL, a few JSON
files, SQLite, object storage, or a service may satisfy it.

Read conformance is separate from execution. A read adapter need not register
work, submit jobs, resolve artifact credentials, or mutate native state. Large
archives will require bounded pages, ordering, identity scope, and explicit
snapshot-consistency semantics before `archive.read/v1` can be claimed. The
existing `archive.snapshot-draft/v0` is only a record-vocabulary experiment.

### 4. Campaign controller contract

The controller owns inference policy:

- durable campaign state and iteration identity;
- proposal or sampling-oracle selection;
- creation of evaluation requests;
- assimilation of correlated results;
- convergence and escalation decisions; and
- restart behavior when evaluations are delayed, duplicated, or failed.

It consumes domain semantics and the evaluation contract, and may query an
archive through the read contract. An evaluation result is not automatically
an archived record, and a controller checkpoint is not an archive record. It
does not own native simulation formats, scheduler submission syntax,
scientific kernels, or artifact transport. HyperPipe may continue to
materialize DAGs and legacy files while this boundary is introduced
incrementally.

## Dependency direction

The following arrows mean import or build dependencies, not runtime data flow:

- campaign controller -> domain contract;
- campaign controller -> evaluation request/result contract;
- campaign controller -> archive-read contract, only when historical lookup is
  required;
- evaluator adapter -> domain and evaluation contracts plus its native
  runtime; and
- archive adapter -> archive-read contract plus its native archive library or
  service client.

No contract package depends on an adapter, native runtime, or native archive.
The domain contract and evaluation envelope are peers: the evaluation record
references a domain-contract identifier and opaque payload, so neither core
contract package must import the other.

Contract packages must remain small and avoid importing RIFT, LALSuite, JAX,
SuperNu, scheduler clients, or project scientific stacks. Project adapters may
depend on a contract package and their native project; the reverse dependency
is prohibited. A controller may depend on contract types, but not on every
backend it can dispatch.

JAX, NumPy, GPU kernels, trained sampling oracles, and robust integrators live
behind evaluator or controller-policy interfaces. They can improve high-SNR GW
work and selected non-GW workloads without becoming wire-format requirements.

## Mapping current RIFT and SuperNu behavior

The current RIFT `MargDriverBase` file/CLI convention combines a grid transport,
row selection, evaluation, and annotated result file. It is a valuable Tier-A
compatibility surface and a candidate adapter, not the canonical cross-domain
contract.

RIFT and SuperNu simulation managers are native execution and persistence
systems. Their similarly named Python classes and historical file-tree intent
do not establish interoperability. Backend-neutral adapters are preferred over
requiring either manager to adopt the other's storage engine or dependency
cadence.

Current HyperPipe is primarily workflow construction around established RIFT
executables and files. The long-term controller boundary should be extracted
through adapters and durable records, not by rewriting working O4 workflows or
making HyperPipe itself a universal scheduler.

## Paired proof sequence

RIFT/SuperNu remains the paired program because it exposes demanding GW
behavior and a genuinely non-GW expensive-simulation workload. It proceeds as
separately authorized phases, never as one umbrella implementation issue:

1. **Next issue/PR — vocabulary only.** Define a non-conformant draft
   request/result record vocabulary with correlation, status, declared
   uncertainty, provenance, and bounded cost fields. Use synthetic positive and
   negative fixtures. Records reference separately owned domain-schema IDs;
   they do not define units, coordinates, priors, or normalization. Stop here.
2. **Later adapter issues.** Select one scientifically meaningful scalar or
   small-result evaluation in each domain and produce draft records through one
   small adapter per domain, preserving existing entry points and native
   archives. Each adapter has its own budget, compatibility declaration, and
   adversarial review.
3. **Later controller issue.** Drive one bounded controller iteration from
   correlated records, including one failed or indeterminate evaluation and a
   restart/replay check.
4. **Later conformance decision.** Decide which observed semantics justify a
   versioned evaluation profile. Archive-read conformance remains an independent
   decision and is not implied by evaluation or controller evidence.

No phase requires a real production-scale campaign, direct native archive
exchange, a shared scheduler, SQLite, or JAX in the SuperNu path. Hand-authored
fixtures establish vocabulary behavior only; they do not establish domain
neutrality or scientific equivalence. The existing snapshot-v0
`simulations`/`levels` examples likewise demonstrate only a candidate
storage-backend vocabulary.

## Compatibility and evolution

- Existing O4 CLIs, Python entry points, defaults, file products, restart
  behavior, and numerical tolerances remain explicit Tier-A candidates; each
  slice lists the exact surfaces it promises.
- New contracts and adapters are additive until a separately authorized
  migration declares otherwise.
- Closed schemas require a new version for field or meaning changes. Supported
  versions overlap for a declared window when active consumers exist.
- Missing or unrecorded compatibility evidence is observation-only. It cannot
  silently become a blocking gate or permission to break a caller.
- A cross-domain capability claim requires evidence from at least two domains.
  Domain-specific GW maintenance may remain domain-specific and must not imply
  generality.

## Coordination with the registry track

The registry/drift-sentinel track records project DAGs, owners, declared
contract versions, compatibility evidence, and expiring exceptions. It does not
define scientific semantics or campaign policy. This architecture track defines
the contracts that projects may register; the registry observes their declared
relationships and drift. The scheduled runner remains operationally separate
from both the contract specification and the sentinel core.

## Stop conditions for the next slice

Stop and return to design review if a proposed increment requires any of:

- simultaneous native archive migration in RIFT and SuperNu;
- mutation, merge, query, artifact transport, and read pagination in one PR;
- a universal scientific payload schema or content hash;
- a new shared scheduler/runtime dependency in core contracts;
- replacement of current HyperPipe or O4 entry points; or
- claims of semantic equivalence based only on hand-authored fixtures.

The next implementation-sized architecture deliverable should therefore be a
non-conformant evaluation request/result vocabulary plus synthetic negative
fixtures, not the complete controller, archive, and execution stack.

The current vocabulary checkpoint is recorded under
[`contracts/evaluation-record-draft-v0`](contracts/evaluation-record-draft-v0/).
It remains synthetic and non-conformant; its presence does not authorize the
later adapter phase.

The first separately authorized mechanical adapter proof is recorded under
[`proofs/rift-marg-record-sidecar-v0`](proofs/rift-marg-record-sidecar-v0/).
It covers one RIFT row only and does not establish cross-domain or operational
adapter conformance.

After separate RIFT and non-GW adapter evidence exists, the observation-only
[`HyperPipe proposal-boundary trace v0`](proofs/hyperpipe-proposal-boundary-trace-v0/)
pins one current evaluated-table-to-proposed-grid seam. It does not define a
controller API, assimilation contract, durable state, or restart semantics.

The RIFT-only
[`record-batch projection proof v0`](proofs/rift-record-batch-projection-v0/)
fills the mechanical gap from an explicitly preselected, all-complete draft
record batch back to the native evaluated table. It deliberately rejects every
case requiring controller selection, retry, deduplication, or state.
