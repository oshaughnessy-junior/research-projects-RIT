# RIFT/SuperNu native-archive observation

## Claim boundary

This public Phase 0/1 fixture records one observation about the projects'
native archive roots using sanitized, pinned evidence. It is not evidence that
direct interchange works, is not the target scientific result contract, and
authorizes no archive or API change. The edge remains `inventory_only`, so the
sentinel reports `indeterminate` while retaining the observed root-filename
mismatch.

The public source identities are intentionally limited:

| Side | Role | Public source ID | Public revision identity | Owner route |
|---|---|---|---|---|
| Private simulation manager | candidate archive producer | `private/source-a` | SHA-256 of sanitized producer contract bytes | private project owner |
| RIFT simulation manager | candidate archive consumer | `public/rift-simulation-manager` | SHA-256 of sanitized consumer contract bytes | `coding-rift` |

Exact repository provenance belongs in the private coding-rift system of
record, not in this public repository.

## Proven public seam

- Edge: `native-archive-root-observation`
- Direction: sanitized private-producer evidence toward the RIFT loader seam
- Producer evidence: a production-shaped root descriptor named `archive.json`
- Consumer evidence: the RIFT loader requires a root file named
  `manifest.json`
- Check scope: root filename only

The producer schema also describes the fields present in the sanitized golden
descriptor so the fixture remains reproducible. Those fields are not declared
RIFT consumer requirements and are not used to infer nested compatibility.
No index format, queue configuration, nested directory layout, status model,
callable API, or scientific payload behavior is asserted publicly by this edge.

This is evidence about native implementations, not a requirement that they
converge. The selected direction is a backend-neutral adapter contract: RIFT's
JSONL representation may remain a native backend, while another conforming
implementation may use plain JSON, SQLite, object storage, or another suitable
representation. Lightweight archives do not need SQLite merely to conform.

The adapter contract will govern observable behavior and scientific meaning;
the native root filename is not a compatibility surface once an adapter is the
supported path.

## Golden artifact and reproduction

`nodes/supernu-manager/golden/archive.json` is a sanitized production-shaped
descriptor generated from a pinned local implementation. Its fixed UUID and
description are non-sensitive; only nondeterministic `created_utc` is normalized
to `0.0`. Serialization uses sorted keys, two-space indentation, and no trailing
line feed. Its SHA-256 is:

`2ebdf7b01e9035f99befb84b41352b955cbb1b81ff4db59f20354b8c3281d62f`

`reproduce_supernu_archive_golden.py` regenerates the descriptor from a
caller-supplied checkout in a temporary directory and compares exact normalized
bytes. It performs no network access and records no checkout path. Private
automation may provide the pinned checkout through an environment variable;
public CI is not configured by this change.

The public `PROVENANCE.json` records only the opaque source ID, sanitized
content identities, derivation call, normalization, and claim boundary. Exact
repository commits, blobs, source-file hashes, line locations, and private
design records remain in the private SOR.

## Content identity

Cross-project archive content identity is **not assessed**. The sentinel report
hashes the exact sanitized producer and consumer contract schema bytes for
reproducibility. The golden artifact has its own byte hash. None of these hashes
establish that two scientific archives, parameters, simulations, or results are
equivalent.

## Synthetic compatibility fixtures

Focused fixtures under `Code/test/drift_sentinel/fixtures/archive-root/` use
only unmistakably synthetic group, node, edge, source, and SHA-256 revision
identities. They exercise:

1. a synthetic producer declaring the required `manifest.json` filename;
2. an incompatible synthetic producer declaring another filename; and
3. an incompatible synthetic producer omitting the filename declaration.

These fixtures never reuse the candidate edge's IDs or revisions and cannot be
cited as real RIFT/SuperNu compatibility evidence.

## Next contract questions

Before any promotion from `inventory_only`, both owners must specify the
minimum adapter operations, supported direction or directions, version
negotiation, error/status behavior, identity and provenance semantics,
compatibility owed to existing archives, and the sanitized end-to-end round
trip that proves conformance. Storage technology remains an implementation
choice.

## Disconnected demonstration group

`synthetic-protocol-runner` remains unrelated to the candidate interchange
edge. It demonstrates multiple disconnected groups and the separation between
protocol code and runner deployment.
