# RIFT local DAG observation v0

> **Status:** historical, observation-only evidence from one bounded synthetic
> run. This authorizes no controller, lifecycle contract, runtime change,
> dependency, scheduler policy, CI gate, retry, or portability claim.

This proof records one unchanged, one-iteration HyperPipe DAG executed through
a localhost HTCondor personal pool. The scientific workload was a 27-point
synthetic three-dimensional Gaussian grid. Seven native DAG nodes exercised
marginalization, two consolidation steps, population unification, proposal,
posterior sampling, and posterior joining.

Six nodes succeeded. The proposal and posterior-worker artifacts appeared, but
the final posterior join exited 127 because the local macOS environment did not
provide `shuf`. No repair, retry, rescue, or second submission was attempted.
The overall DAG therefore failed.

That failure is the useful architecture evidence:

- a proposal artifact can exist before the overall iteration succeeds;
- a successful proposal node does not imply successful posterior aggregation;
- a final-looking aggregate file can exist while remaining header-only; and
- sibling-branch failure can occur after proposal completion.

In this run, artifact existence did not establish overall DAG success, and no
controller-assimilation event was observed.
The current DAG also embeds resource and sandbox policy below HyperPipe's
configuration boundary: the tiny posterior worker requested 8192 MiB while the
scheduler reported 2 MiB memory use, and vanilla nodes used same-host sandbox
transfer. These are observations, not approved defaults or compatibility
promises.

## What is retained

`sanitized-observation.json` contains only:

- the exact RIFT source revision and bounded public runtime versions;
- seven semantic node roles and mechanically extracted DAG edges;
- a sanitized event sequence without native scheduler identities or absolute
  time;
- structural artifact facts without rows, vectors, native names, paths, or
  content hashes;
- explicitly unit-labelled resource observations; and
- the bounded terminal failure fact.

The event `sequence` is a sanitized relative order reconstructed post hoc from
native scheduler logs and artifact metadata. It supports only the listed event
chronology in this run. It is not evaluation-arrival order, controller
assimilation order, a portable timing guarantee, or a rule that proposal must
finish before posterior work.

Raw DAGMan/HTCondor logs, submit files, scheduler IDs, timestamps, local paths,
host details, and artifact contents are deliberately excluded from the public
repository. Exact raw provenance remains in the private coding-rift SOR.

## Nonclaims

This single environment-specific observation establishes no:

- controller contract, controller-owned iteration, or assimilation event;
- general lifecycle or outcome vocabulary;
- portable macOS/Linux behavior or cross-platform defect characterization;
- scientific correctness, proposal quality, convergence, or calibration;
- request/result correlation, retry, recovery, idempotency, checkpoint, or
  restart semantics;
- deterministic ordering between the proposal and posterior sibling branches;
- compatibility promise or approval for the 8192 MiB request; or
- authorization to add `shuf`, modify the join, or change production code.

The missing local tool is one observed environmental boundary. Fixing it and
repeating the run requires a separate compatibility declaration and review.
