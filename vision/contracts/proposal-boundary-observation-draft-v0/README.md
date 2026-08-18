# Proposal-boundary observation draft v0

> **Status:** non-conformant vocabulary experiment. These files authorize no
> API, adapter, proposal dispatch, controller, scheduler, archive, dependency,
> CI gate, or scientific-semantic change.

This directory tests a deliberately high-level record for one completed,
post-hoc observation of a proposal boundary:

```text
domain-owned evaluated-population contract
  + proposal-policy contract and bounded configuration
  + RNG contract and bounded configuration
  -> domain-owned candidate-population contract
```

The record does not carry either population. It describes their separately
owned contracts and cardinalities only. This avoids standardizing native
tables, score columns, placeholder uncertainties, storage backends, or
scientific payloads merely because two projects can both produce proposals.

## Why this is an observation, not a request/result protocol

The reviewed mechanical traces do not establish proposal dispatch,
correlation, outcomes, authoritative campaign or iteration identity, arrival
order, attempt selection, retry safety, or controller-owned state. A
request/result shape would invent those semantics. This draft is therefore a
standalone, intentionally non-correlatable observation. Git and review history
provide evidence provenance outside the record.

The envelope carries only:

- its vocabulary and record type;
- a separately owned domain-contract reference;
- input- and candidate-population contract references and cardinalities; and
- proposal-policy and RNG contract references with bounded flat scalar
  configuration maps.

Configuration keys and values are opaque to this envelope. Their referenced
policy or RNG contract owns their meaning. A key called `seed`, for example,
does not establish a shared RNG algorithm, replay guarantee, or portability.

## Explicit nonclaims

This draft establishes no:

- interoperability, backend neutrality, cross-domain support, conformance, or
  semantic equivalence;
- population, candidate, request, result, campaign, iteration, or attempt
  identity;
- native-table serialization, score, uncertainty, normalization, completion,
  duplicate, weighting, or ordering semantics;
- proposal dispatch, evaluator outcome, diagnostics, timing, cost, retry,
  idempotency, cancellation, or arrival sequence;
- controller assimilation, convergence, lifecycle, checkpoint, restart, or
  durable state;
- scheduler, transport, archive, artifact, locator, content-identity, or
  persistence behavior; or
- portable RNG replay or scientific validity.

Passing the bundled synthetic fixture demonstrates schema shape only. It is
not evidence that two domains or backends satisfy a shared contract.

## Bounds, privacy, and evolution

All objects are closed. Contract identifiers and versions are bounded portable
tokens. Cardinalities are bounded positive integers. Configuration maps have
at most 32 entries and admit only bounded portable strings, finite JSON
numbers within the documented range, or booleans. Nested objects, arrays,
nulls, messages, paths, URIs, binary values, and payloads are not part of the
vocabulary.

The schema cannot determine whether an otherwise portable string contains a
secret. Producers remain responsible for never placing credentials, bearer
tokens, private locators, or unapproved scientific data in configuration. The
bundled fixture is wholly synthetic.

The schema is closed to expose accidental field drift. Any field or meaning
change creates a new draft version. The `urn:rift:vision:...` identifier records
repository-local draft provenance; it is not a namespace or ownership decision
for a future shared contract.

Operational request/result records must wait for a separately authorized live
campaign that observes dispatch and lifecycle behavior. Runtime adoption and
any conformance claim require independent design, compatibility, tests, and
adversarial review.
