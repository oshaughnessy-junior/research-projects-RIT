# Sentinel JSON schemas

These Draft 2020-12 schemas publish the v1 wire shapes for registry,
resolved-input, and report documents. The stdlib parser remains normative for
semantic constraints that JSON Schema cannot express compactly, including DAG
acyclicity, global ID uniqueness, registry fingerprint agreement, contained
paths, and immutable revision syntax.

Changing a versioned wire shape requires a compatibility test and a new schema
version or an explicitly backward-compatible additive change. The schemas are
data contracts; the core does not acquire a `jsonschema` dependency to validate
them.
