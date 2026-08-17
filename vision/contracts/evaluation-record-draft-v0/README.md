# Evaluation record draft v0

> **Status:** non-conformant vocabulary experiment. These files authorize no
> API, adapter, scheduler, archive, controller, dependency, CI gate, or
> scientific-semantic change.

This directory tests a small record shape for correlating one proposed logical
evaluation request with a result. It is intentionally synthetic. It does not
execute RIFT, SuperNu, HyperPipe, a simulation manager, or any scientific
runtime, and it supplies no cross-domain evidence.

## What the draft carries

- a vocabulary version and record type;
- bounded opaque record, logical-evaluation, and producer identifiers;
- an attempt number, without claiming retry or idempotency semantics;
- a separately owned domain-contract identifier and version;
- an opaque domain payload;
- for results, a proposed outcome category, declared uncertainty status,
  bounded cost observations, and bounded diagnostics.

Matching identifiers in the synthetic request and result illustrate proposed
correlation fields only. The schema validates each record independently and
cannot establish cross-record correlation, attempt lineage, idempotency, or
retry safety.

The envelope does not define parameter names, units, coordinates, priors,
normalization, fidelity meaning, objective meaning, uncertainty semantics, or
scientific validity. Those belong to the referenced domain contract. Equal
envelope structure does not imply equal science.

## What the draft does not specify

- transport, serialization beyond these JSON fixtures, or version negotiation;
- whether one logical evaluation maps to one process, batch job, simulation,
  cache lookup, emulator call, or several attempts;
- request idempotency, retry policy, cancellation, deadlines, or consistency;
- archive persistence, artifact resolution, controller checkpoints, or result
  assimilation;
- authentication, authorization, secret handling, or production size limits;
- canonicalization, content identity, or semantic equivalence; or
- operational conformance for any adapter or project.

The schema is closed to expose accidental field drift. Any field or meaning
change creates a new draft version. Opaque payloads can contain arbitrary JSON,
so producers remain responsible for never placing secrets, credentials,
private paths, or unapproved scientific data into durable records. The bundled
fixtures are synthetic and redacted.

The schema's `urn:rift:vision:...` identifier records repository-local draft
provenance. It is not a namespace or ownership decision for a future shared
contract.

## Fixtures

- `valid/request.json`: one synthetic request.
- `valid/result-complete.json`: a correlated complete result with reported
  synthetic uncertainty.
- `valid/result-indeterminate.json`: a correlated indeterminate result with no
  domain payload.
- `invalid/*.json`: deliberately invalid examples proving selected closed
  schema and outcome/payload constraints.

Passing these fixtures proves only that the proposed record vocabulary behaves
as written. The next adapter phase requires separate authorization, budgets,
compatibility declarations, real project owners, and adversarial review.
