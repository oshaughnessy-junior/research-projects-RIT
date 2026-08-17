# Backend-neutral archive adapter contract

> **Status:** proposed v1 contract slice, not implementation authorization.
> It does not require an archive migration, storage-engine change, or production
> adapter. Each implementation requires its own SOR issue, owner, compatibility
> statement, tests, and adversarial review.

## Decision

Cross-project compatibility belongs to an adapter contract, not to a native
file tree. RIFT's JSONL archive is one valid implementation. A lightweight
project may use a handful of JSON files or in-memory records; a larger archive
may use SQLite, object storage, or a service. No backend is normative.

Adapters translate native state into a small canonical envelope. They may be
in-process Python objects, CLI/JSON-stream programs, exchange-bundle readers,
or remote services. Transport and storage are implementation choices provided
the observable contract is preserved.

## Contract layers

### Required: `archive.read/v1`

Every conforming adapter can:

1. describe the archive and adapter;
2. enumerate simulations deterministically;
3. retrieve a simulation by its adapter-stable identifier;
4. enumerate its refinement/fidelity levels;
5. expose state, parameters, summaries, artifacts, and provenance without
   requiring native backend access by the consumer.

The read profile may be implemented from static files. It does not require a
database, writable archive, scheduler, or scientific runtime.

### Optional capabilities

- `archive.artifact-read/v1`: open or materialize an artifact reference.
- `archive.register/v1`: idempotently register a requested simulation.
- `archive.refine/v1`: request a higher or different fidelity level.
- `archive.status-write/v1`: record lifecycle transitions.
- `archive.merge/v1`: import records while preserving identity/provenance.
- `archive.query/v1`: backend-accelerated filtering beyond deterministic
  enumeration.

An adapter advertises capabilities explicitly. A missing capability produces
`unsupported`, never emulated mutation or silent partial behavior.

## Canonical envelope

The exchange envelope contains:

- `contract_version` and advertised `capabilities`;
- an opaque `adapter_id` and non-normative backend description;
- archive identity and provenance;
- simulations ordered by stable adapter ID;
- native state plus an optional normalized lifecycle state;
- opaque parameter and summary payloads with their schema/semantic identifiers;
- zero or more levels with explicit fidelity descriptions;
- artifact references carrying media type, size and digest when known.

The envelope describes the canonical record shape; it does not require an
entire campaign to be materialized in one JSON document. Large implementations
may page or stream deterministically using continuation tokens whose consistency
scope is declared by the adapter.

Parameters, summaries and fidelity descriptors remain opaque JSON values. A
domain contract—not the archive contract—defines their physical units,
coordinates, priors, normalization, uncertainty and scientific meaning.

## Identity

The contract distinguishes three identities:

1. **Adapter-stable ID:** stable within one archive lineage and required for
   retrieval and references.
2. **Native ID:** optional backend identifier, namespaced and never assumed
   comparable across projects.
3. **Content identity:** optional digest under an explicitly named
   canonicalization rule. Absence means unknown, not unequal.

No universal cross-project hash is assumed. An adapter must not relabel a
native primary key as content identity.

## States

Every record preserves `{namespace, value}` for the native state. An adapter
may additionally map it to one of:

- `requested`
- `ready`
- `running`
- `complete`
- `failed`
- `unknown`

Normalized state is intentionally coarse. Native sub-states and failure detail
remain available. A lossy mapping must be declared in adapter metadata and
must not be used to drive mutation without project-owner approval.

## Artifacts

Artifact references separate identity from transport. Supported locator kinds
are `inline`, `relative_path`, `uri`, and `opaque`. Consumers must not assume a
local path. Large artifacts need not be copied into an exchange envelope; an
adapter may return a durable URI or require `archive.artifact-read/v1`.

Digests apply to exact artifact bytes under the named algorithm. Missing
digests are `unknown`. Paths and authenticated URLs must be redacted from
durable reports unless explicitly approved.

## Version negotiation

The consumer supplies supported contract versions and required capabilities.
The adapter selects one exact version or returns `unsupported`. Minor behavior
must not be inferred from package versions or native schema numbers.

Contract evolution is additive within a version. Renaming fields, changing
state meaning, altering identity semantics, or strengthening required
capabilities requires a new contract version and an overlap window when active
consumers exist.

## Errors

Adapters return one stable category plus backend-owned detail:

- `not_found`
- `unsupported`
- `invalid`
- `conflict`
- `unavailable`
- `indeterminate`
- `internal`

Error detail must not expose credentials, absolute private paths, environment
dumps, or unbounded backend output. `indeterminate` is not success.

## Compatibility promise

Conformance means that the advertised operations preserve the declared
behavior and semantics. It does not mean:

- native archives can open one another;
- native layouts, indexes or databases match;
- all adapters are writable;
- all archives support refinement or merging;
- artifacts are local; or
- domain payloads share scientific meaning without a separate domain contract.

Existing RIFT CLIs, JSONL archives and default semantics remain independent
Tier-A surfaces. An adapter is additive unless a separately reviewed migration
changes that promise.

## First conformance fixture

The initial synthetic fixture checks only the backend-neutral envelope:

1. a file-backed example and an indexed example expose the same canonical
   semantic record;
2. their snapshots remain equivalent after excluding non-normative native
   identity, locator and backend metadata; and
3. the lightweight example advertises only the required read capability while
   the indexed example independently advertises query support.

The fixture does not execute real adapters or prove ingestion/mutation. It is
not RIFT/SuperNu compatibility evidence. The first real proof must implement
`archive.read/v1` for one sanitized RIFT/SuperNu path, pin both adapter versions,
and verify domain semantics separately.

## Sentinel coverage

The drift sentinel should monitor:

- contract and capability versions;
- canonical envelope schemas;
- native-to-normalized state mapping declarations;
- identity/canonicalization rule versions;
- error categories;
- golden semantic round trips; and
- explicit compatibility exceptions with owners and expiry.

It should not compare JSONL against SQLite, require a database, or treat native
root filenames as contract drift once an adapter is the supported boundary.
