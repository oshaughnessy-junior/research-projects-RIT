# RIFT record-batch projection proof v0

> **Status:** RIFT-only, observation-only architecture evidence for Issue #129.
> This is not an installed API, controller assimilation, controller state, or
> a backend-neutral table contract.

This proof fills one mechanical gap between two reviewed boundaries:

```text
RIFT native row -> draft request/result records
             -> preselected complete record batch -> native all.marg_net text
             -> unchanged proposal boundary (proved separately)
```

The repository-local standard-library projector accepts an explicitly ordered
batch of already-selected request/result pairs plus explicit parameter-column
order. It verifies the exact RIFT draft domain, producer, uncertainty schema,
pairwise correlation, finite values, and unique identities, then emits the
current native `# lnL sigma_lnL ...` table. It never invokes HyperPipe or the
tracer and accepts no paths.

The focused proof constructs 25 pairs through the existing RIFT one-row
sidecar, projects them with parameter order `("x", "y")`, and compares their
meaning with the synthetic evaluated-grid fixture merged in proposal-boundary
PR #128 (`6c24938a...`). The projector's stable formatter makes those reviewed
fixture bytes reproducible, but that is fixture-local evidence—not a general
canonical serialization promise or a Tier-A numerical-format commitment.

## Policy deliberately rejected

The projector accepts only complete results with reported RIFT `sigma_lnL`.
It rejects duplicate IDs, multiple attempts for one logical evaluation, mixed
or mismatched records, incomplete outcomes, and malformed payloads rather than
selecting, deduplicating, reordering, retrying, or persisting state. Rejection
defers those decisions to a future controller contract backed by real evidence.

No R3/SuperNu record is consumed. The separate R3 one-row proof demonstrates
evaluation-envelope transfer only; it does not establish a non-GW proposal
population or cross-domain proposal semantics.

## Explicit deferrals

Controller assimilation/conformance, result arrival and selection, retry and
idempotency, attempt lineage, campaign/iteration identity, checkpoint/restart,
failure handling, convergence, generic projection, archives, schedulers,
registry/sentinel work, transport, pagination, streaming, JAX/GP changes, and
production campaigns remain outside this proof.
