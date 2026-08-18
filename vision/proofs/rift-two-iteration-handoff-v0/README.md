# RIFT two-iteration native handoff observation v0

> **Status:** historical, observation-only evidence from one bounded synthetic
> run. This authorizes no controller, schema, runtime, scheduler, dependency,
> retry, CI, compatibility, or success-policy change.

This proof records one unchanged two-iteration HyperPipe tracer DAG at exact
RIFT revision `626379bbeac9f224b6c79d12c9d612d1eb954f3c`. The native DAG had
15 nodes: the same seven roles in each iteration plus one convergence node.
It configured zero retries.

The narrow result is native handoff evidence. The DAG contained a direct edge
from the iteration-zero proposal node to the iteration-one marginalization
node. It contained no edge from the iteration-zero posterior join to that
marginalization node. Native iteration-input wiring bound the proposal node's
next-iteration output (index 1) to the iteration-one marginalization input
(index 1). The proposal population and the direct iteration-one
marginalization output each had 27 finite rows with unique proposal coordinates,
and their coordinates were equal in observed row order. The proposal differed
from the initial grid.

The iteration-one marginalization node completed while the iteration-zero
posterior worker was still running. Both iterations' evaluated-data and
proposal paths completed. Both posterior joins exited 127 because `shuf` was
unavailable in the observed environment, so the convergence node could not do
useful work and the overall DAG terminated `node_failed`.

The direct DAG edge, native iteration-input wiring, and observed coordinate
relationship are the three legs of this bounded handoff evidence. Together
they show that this native tracer DAG carried the proposal directly into the
next iteration's evaluation path while the previous iteration's posterior
branch remained incomplete. They do not show controller assimilation or
controller-owned state.

## What is retained

`sanitized-observation.json` retains only:

- the exact source revision and bounded DAG topology;
- closed, semantic observation variants for the native handoff and overlap;
- structural population facts and the observed coordinate relationships;
- the two bounded join failures and terminal DAG status; and
- an exact map of supported claims and nonclaims.

Raw scheduler records, scheduler identities, timestamps, native artifact
names, local paths, host and user details, hashes, tables, and coordinate
vectors are excluded.

## Nonclaims

This single RIFT observation establishes no:

- controller assimilation, durable controller state, checkpoint, or restart;
- backend-neutral handoff, adapter contract, or lifecycle protocol;
- portable ordering or timing guarantee between sibling branches;
- success policy for a data path, iteration, convergence node, or whole DAG;
- scientific validity, proposal quality, convergence, or calibration;
- cross-domain behavior or non-GW transfer evidence;
- retry or recovery semantics; or
- authorization to add `shuf`, repair a join, or change production behavior.

In particular, ordered coordinate equality is a fact about two artifacts in
this run. It is not a portable row-order contract.
