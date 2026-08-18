# HyperPipe proposal-boundary trace v0

> **Status:** observation-only architecture evidence for Issue #126. This is
> not a controller API, controller conformance, backend-neutral protocol,
> restart guarantee, or production integration.

This proof observes one existing RIFT proposal-policy seam at exact base
`59a7a2b176697ed5b7ad61b31f49cc97b30d5b84`:

```text
synthetic evaluated grid
  -> unchanged util_HyperparameterTracerUpdate.py
  -> proposed next grid
```

The input is a 25-row, two-dimensional synthetic quadratic score table with a
small cross-term in the current HyperPipe `# lnL sigma_lnL ...` format. The unchanged tracer executable
uses `smc-mala-bd`, the lightweight quadratic fit, and an explicit seed. The
proof supplies no previous grid, state, coordinate plugin, downselection, or
self-avoidance option.

The RIFT MargDriver sidecar merged at `59a7a2b...`; the separate non-GW R3
historical-row sidecar merged at `bf3d22c...`. Those establish that the draft
evaluation envelope can be mapped in two domains. They are evidence for
sequencing only: this proof neither reads their records nor combines their
one-row examples into a proposal batch.

## What the trace establishes

The test-only reproducer preflights the canonical tracer imports, runs the
proposal twice in separate temporary directories, checks that input bytes stay
unchanged, rejects any fallback or stderr, and requires the two outputs to be
byte-identical within that one environment and enforces format, row-count,
finiteness, zero-placeholder, and coordinate-bound invariants.

The committed stochastic trajectory and hash identify the reviewed reference
run as provenance only. Tests validate the reference artifact's own structure
and hash but do not require another environment to reproduce its coordinates.
It is not a portable byte-level or numeric promise across Python, NumPy, BLAS,
CPU, or platform versions.

## Deliberate boundaries

The trace manifest contains repository-relative identities, revision, fixed
arguments, input shape, and hashes. It contains no absolute path, hostname,
environment dump, credential, private scientific data, or serialized state.

The reproducer executes only the repository's pinned proposal executable and
the bundled synthetic input. It does not accept external paths. Optional
tracer pickle state, dynamic coordinate plugins, previous-grid assimilation,
downselection parsing, DAG construction, convergence, archives, schedulers,
registry/sentinel work, retries, failed-result handling, GP/JAX changes, and
controller decisions are outside this proof.

The next architecture decision must choose, through a separate issue and
review, between a multirow record-to-table projection adapter and a minimal
assimilation/state contract. This trace authorizes neither.
