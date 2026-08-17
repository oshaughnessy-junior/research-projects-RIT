# Backend-neutral archive snapshot vocabulary

> **Status:** experimental `archive.snapshot-draft/v0` vocabulary, not an
> operational adapter contract or implementation authorization.
> It does not require an archive migration, storage-engine change, or production
> adapter. Each implementation requires its own SOR issue, owner, compatibility
> statement, tests, and adversarial review.

## Decision

Cross-project compatibility belongs to an adapter contract, not to a native
file tree. RIFT's JSONL archive is one valid implementation. A lightweight
project may use a handful of JSON files or in-memory records; a larger archive
may use SQLite, object storage, or a service. No backend is normative.

Adapters may eventually translate native state into a small canonical record
shape. They may be
in-process Python objects, CLI/JSON-stream programs, exchange-bundle readers,
or remote services. Transport and storage are implementation choices provided
the observable contract is preserved.

This draft defines only a synthetic snapshot vocabulary. It does not define
adapter conformance, operations, pagination, negotiation, consistency,
transport, errors, authentication, or mutation.

## Future operational profile: `archive.read/v1`

A separately reviewed operational profile should eventually define:

1. describe the archive and adapter;
2. enumerate simulations deterministically;
3. retrieve a simulation by its adapter-stable identifier;
4. enumerate its refinement/fidelity levels;
5. expose state, parameters, summaries, artifacts, and provenance without
   requiring native backend access by the consumer.

It must also define global ordering, unique IDs, bounded pages, opaque token
expiry, snapshot-consistent versus explicitly weak consistency, retries,
partial failure, and no-duplicate/no-omission behavior across pages. None of
those behaviors is claimed by this snapshot draft.

The future read profile may be implemented from static files. It must not
require a database, writable archive, scheduler, or scientific runtime.

### Reserved future profiles

- `archive.artifact-read/v1`: open or materialize an artifact reference.
- `archive.register/v1`: idempotently register a requested simulation.
- `archive.refine/v1`: request a higher or different fidelity level.
- `archive.status-write/v1`: record lifecycle transitions.
- `archive.merge/v1`: import records while preserving identity/provenance.
- `archive.query/v1`: backend-accelerated filtering beyond deterministic
  enumeration.

These names are reserved only. This draft does not specify them and no adapter
may claim conformance to them from these fixtures. Mutation profiles require
separate authorization, idempotency keys, expected-revision preconditions,
conflict/atomicity rules, audit provenance, authorization, and dry-run
boundaries. Merge and mutation design are explicitly deferred.

## Canonical envelope

The experimental snapshot contains:

- `vocabulary_version`;
- an opaque producer ID/version and non-normative backend description;
- archive identity and provenance;
- simulations ordered by stable adapter ID;
- native state plus an optional normalized lifecycle state;
- opaque parameter and summary payloads whose schema/semantics remain
  unresolved in this draft;
- zero or more levels with explicit fidelity descriptions;
- artifact references carrying media type, size and digest when known.

The snapshot describes a candidate record shape; it does not require an
entire campaign to be materialized in one JSON document. Large implementations
will require a future paged/streamed operations contract.

Parameters, summaries, fidelity descriptors and provenance remain opaque JSON
values. This draft carries no semantic/schema identifier and makes no content
identity claim. A
domain contract—not the archive contract—defines their physical units,
coordinates, priors, normalization, uncertainty and scientific meaning.

## Identity

The future contract must distinguish three identities:

1. **Adapter-stable ID:** stable within one archive lineage and required for
   retrieval and references.
2. **Native ID:** optional backend identifier, namespaced and never assumed
   comparable across projects.
3. **Content identity:** optional digest under an explicitly named
   canonicalization rule. Absence means unknown, not unequal.

The draft carries adapter-stable and optional native IDs only. It does not
encode content identity or canonicalization. No universal cross-project hash is
assumed, and a native primary key must never be relabeled as content identity.

## States

Every record preserves `{namespace, value}` for the native state. An adapter
may additionally map it to one of:

- `requested`
- `ready`
- `running`
- `complete`
- `failed`
- `unknown`

Normalized state is intentionally coarse and is only a display/filter hint in
this draft. It cannot establish scientific equivalence or drive scheduling or
mutation. A future operational contract must version the mapping, name its
owner and lossiness, and distinguish simulation from level completion.

## Artifacts

Draft artifact records contain metadata and an opaque redacted handle only.
They do not contain inline bytes, paths or URIs. Consumers must not assume the
handle is resolvable. Typed resolution, containment, credentials, expiry,
authorization and size bounds belong to a future `archive.artifact-read/v1`.

Digests apply to exact artifact bytes under the named algorithm. Missing
digests are `unknown`. Paths and authenticated URLs must be redacted from
durable reports unless explicitly approved.

## Versioning

This draft performs no negotiation. A future consumer should supply supported
contract versions and required capabilities, and the adapter should select one
exact version or return `unsupported`. Behavior must not be inferred from
package versions or native schema numbers.

The schema is closed to catch typos. Therefore any field addition, rename,
state-meaning change, identity change, or stronger requirement creates a new
vocabulary/contract version and an overlap window when active consumers exist.

## Errors

Future operational adapters should return one stable category plus bounded
backend-owned detail:

- `not_found`
- `unsupported`
- `invalid`
- `conflict`
- `unavailable`
- `indeterminate`
- `internal`

This draft contains no error envelope. Error detail must not expose credentials,
absolute private paths, environment dumps, or unbounded backend output.
`indeterminate` is not success.

## Compatibility promise

This draft confers no conformance. A future conformance claim should mean that
specified operations preserve declared behavior and semantics. It must not mean:

- native archives can open one another;
- native layouts, indexes or databases match;
- all adapters are writable;
- all archives support refinement or merging;
- artifacts are local; or
- domain payloads share scientific meaning without a separate domain contract.

Existing RIFT CLIs, JSONL archives and default semantics remain independent
Tier-A surfaces. An adapter is additive unless a separately reviewed migration
changes that promise.

## Synthetic vocabulary fixture

The initial synthetic fixture checks only the backend-neutral envelope:

1. a file-backed example and an indexed example carry equal selected synthetic
   fields;
2. the test removes native identity, native state and opaque artifact handles
   before comparing those hand-authored fields; and
3. different backend descriptions do not affect that selected-field
   projection.

The fixture does not execute real adapters, validate state mappings, or prove
semantic equivalence, ingestion, mutation, pagination or conformance. It is
not RIFT/SuperNu compatibility evidence. The first real proof must implement
`archive.read/v1` for one sanitized RIFT/SuperNu path, pin both adapter versions,
and verify domain semantics separately.

## Sentinel coverage

Once operational contracts exist, the drift sentinel should monitor:

- contract and capability versions;
- canonical envelope schemas;
- native-to-normalized state mapping declarations;
- identity/canonicalization rule versions;
- error categories;
- golden semantic round trips; and
- explicit compatibility exceptions with owners and expiry.

It should not compare JSONL against SQLite, require a database, or treat native
root filenames as contract drift once an adapter is the supported boundary.
