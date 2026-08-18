# RIFT MargDriver record sidecar proof v0

> **Status:** RIFT-only mechanical adapter prototype for Issue #124. This is
> not an operational adapter contract, production integration, backend-neutral
> evidence, or cross-domain conformance.

This proof maps one already-produced, one-row HyperPipe MargDriver input/result
pair into the repository's non-conformant `evaluation.record-draft/v0`
vocabulary. It neither invokes `MargDriverBase` nor changes the established
grid and `+annotation.dat` file seam.

## Narrow semantic declaration

The fixture uses the RIFT-owned draft domain identifier
`rift.hyperpipe.marginal-log-likelihood-draft` at version `v0`.

- request payload: the named parameter values from columns after the leading
  `lnL sigma_lnL` placeholders;
- complete-result payload: `log_likelihood`, interpreted as the dimensionless
  natural logarithm of the marginal likelihood produced by the native driver;
- declared uncertainty: non-negative `sigma_lnL`, identified by the draft
  uncertainty schema ID `rift.hyperpipe.sigma-lnL-draft-v0`.

This note records the mapping used by this proof; it does not standardize
parameter names, parameter units, likelihood normalization, or uncertainty
semantics for another project. The fixture is synthetic and production-shaped,
not real event data or scientific validation.

## Compatibility boundary

The sidecar is standard-library-only and repository-local under `vision/`. It
accepts text supplied by its caller and returns two in-memory dictionaries. It
does not import RIFT, execute a likelihood, write a record or archive, inspect
paths or environment variables, infer IDs, or alter native bytes. Correlation
IDs, attempt number, producer identity, and the opaque native reference are all
caller supplied.

The observed Tier-A candidates remain unchanged: MargDriver CLI flags and
defaults, the input header and row layout, `+annotation.dat` bytes and column
order, and native numerical behavior. Passing this proof says nothing about
their future deprecation policy.

## Deliberate rejection and deferral

The prototype rejects malformed, multi-row, duplicate-column, non-finite, or
negative-uncertainty input instead of manufacturing a failed or indeterminate
scientific result. It records no cost because this native seam supplies none.

SuperNu mapping is deferred until its scientific owner selects an observable
and declares units, normalization, validity, and uncertainty. Archive access,
artifact transport, schedulers, batching, retries, idempotency, cancellation,
controller assimilation, registry work, generic adapter base classes, JAX, and
production campaigns are out of scope.
