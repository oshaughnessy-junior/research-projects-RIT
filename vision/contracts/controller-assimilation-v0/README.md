# Controller assimilation v0

> **Status:** small reference contract. It changes no RIFT runtime, HyperPipe
> workflow, scheduler, archive, scientific payload, dependency, or supported
> API. Adoption requires a separately authorized adapter.

`campaign.assimilation/v0` defines the durable boundary at which a campaign
controller accepts evaluation results. A transition names one campaign and
iteration, identifies the controller policy, compares an expected campaign
revision, and commits exactly the next revision. It contains between 1 and
10,000 decisions. Decision array order has no semantic meaning. Multiple
commits may name the same iteration.

Each decision correlates a request ID, logical evaluation ID, result ID, and
attempt number against separately supplied request and result records. Their
domain-contract identifiers must also match. Correlation is not inferred from
files or workflow completion. Only `complete` and `partial` results may be
assimilated. A `deferred` or `rejected` decision does not close the logical
evaluation: a strictly later attempt may be decided by a later transition.
Every disposition advances the latest decided attempt. After an `assimilated`
decision, that logical evaluation ID cannot be assimilated again.

The reference ledger applies a transition atomically:

- an unseen transition ID must match the current campaign revision;
- `committed_campaign_revision` must equal
  `expected_campaign_revision + 1`;
- every decision and correlated result is validated before state changes;
- exact semantic replay of a committed transition is a no-op and returns its
  prior receipt, even after the campaign revision advances, but its exact
  request/result identity and correlation batch is still validated; and
- reuse of a transition ID for different semantics is a conflict.

Receipts contain the canonical transition. Rebuild consumes a bounded stream
whose items pair one receipt with that transition's exact request/result batch;
it never requires one unbounded global record collection. Receipts do not
archive evaluation payloads. Request and result IDs are externally assigned,
so validating an exact replay establishes identity and correlation, not
payload or scientific-content equivalence. The schema is closed; changing
fields or meaning requires a new contract version.

## Deliberate limits

This contract does not define proposal policy, convergence, evaluation
transport, retries, scheduler success, archive layout, domain parameters,
units, uncertainty meaning, or scientific validity. It provides no universal
content identity and makes no RIFT/R3 or GW/non-GW interoperability claim.
The tests use synthetic in-memory RIFT-shaped and radiation-transport-shaped
opaque payloads only to show that the reducer does not inspect domain content.

The implementation is standalone Python standard library. The JSON Schema is
documentation and interchange validation; the reducer has no `jsonschema`
runtime dependency.

The contract remains draft until the separately authorized private non-GW
consumer gate in the canonical issue has passed.
