# RIFT drift sentinel core

`rift_drift_sentinel` is the dependency-light, offline comparison core described
in `vision/DRIFT_SENTINEL_PLAN.md`. It deliberately sits beside the `RIFT`
package: importing it does not execute `RIFT/__init__.py` and therefore does not
require lalsuite, JAX, NumPy, a scheduler, or a scientific environment.

## Boundary

The core validates a versioned registry containing multiple dependency DAGs,
accepts a runner-resolved local input manifest with immutable revisions, performs
one narrow JSON-schema-subset comparison, and emits deterministic JSON and text
reports. It does not fetch, authenticate, schedule, persist, notify, modify a
repository, or decide that a scientific divergence is acceptable.

The first registry is an **inventory**, not a compatibility certification. Its
RIFT/SuperNu native-archive observation compares a sanitized production-shaped
root descriptor with the proven RIFT root-manifest filename requirement. The
filenames differ, but the edge remains `inventory_only`, so it produces
`indeterminate` evidence rather than making storage-layout compatibility a
requirement. The intended shared surface is a backend-neutral adapter contract;
only the disconnected synthetic protocol/runner fixture is marked `verified`.

## Install and invoke

An editable RIFT install discovers the standalone package through the existing
`setup.py`:

```bash
python -m pip install -e .
python -m rift_drift_sentinel validate \
  --registry MonteCarloMarginalizeCode/Code/rift_drift_sentinel/examples/pilot-registry.json
```

The runner then creates a resolved-input manifest. Its
`registry_fingerprint` must equal the value printed by `validate`; every node
must name the registry `source_id`, a local root, and an immutable 40-hex Git
commit or `sha256:` content identity. Relative roots are resolved relative to
the manifest, but absolute roots are never copied into a report.

The bundled example is directly runnable from a source checkout. Public pilot
nodes use SHA-256 identities of sanitized contract bytes; the private repository
revisions that produced them remain in the private system of record. Synthetic
nodes use unmistakably synthetic `sha256:` fixture identities:

```bash
python -m rift_drift_sentinel check \
  --registry MonteCarloMarginalizeCode/Code/rift_drift_sentinel/examples/pilot-registry.json \
  --resolved-inputs MonteCarloMarginalizeCode/Code/rift_drift_sentinel/examples/pilot-resolved-inputs.example.json \
  --run-id 20260817T140000Z-example \
  --as-of 2026-08-17 \
  --machine-output report.json
```

The default exit status is zero for a completed observation even when a contract
is incompatible. Input errors return 2. `--fail-on-incompatible` is an explicit,
later gate-selection mechanism and returns 1 for incompatible findings; it must
not be enabled in production CI without owner approval and a separately reviewed
policy change. Missing, invalid, escaped, or unverified evidence is
`indeterminate`, never compatible.

Run the focused tests without importing RIFT:

```bash
python MonteCarloMarginalizeCode/Code/test/drift_sentinel/test_drift_sentinel.py
```

## Supported v1 comparison

`json_schema_subset_v1` checks only a declared subset: object type, required
fields, property `type` and `enum`, per-field `x-science` annotations, and root
`x-contract` annotations. Consumer declarations are requirements; the producer
may expose additional fields. This is intentionally not a full JSON Schema
validator and must not be represented as one.

Registry parsing is strict: unknown keys, duplicate IDs, unknown endpoints,
cycles, unsafe relative paths, malformed exception dates, and unsupported check
kinds fail validation. A registry may contain multiple groups, and groups need
not share nodes or a release cadence.

## Security and determinism

The core opens only registered files contained by resolved node roots, including
after symlink resolution. It never executes repository code or commands. Reports
contain logical source IDs, immutable revisions, relative evidence paths, and
hashes—not absolute roots, arbitrary schema keys/values, file contents,
environment variables, remote URLs, or credentials. Mismatch keys and values
are fingerprinted; owners reproduce locally to see private detail. Inputs are
untrusted: a successful comparison says only that the
provided bytes satisfy the declared subset at the recorded revisions.

`--run-id` and `--as-of` come from the runner; the core does not read wall-clock
time. Identical registry, resolved files, run metadata, and fixture bytes produce
byte-identical machine and human reports.

Runner deployment, registry governance, durable report storage, retention,
notification, and RACI are specified in `vision/DRIFT_SENTINEL_OPERATIONS.md`.
