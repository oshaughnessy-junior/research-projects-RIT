# Synthetic archive snapshot draft v0 fixture

This fixture illustrates the experimental `archive.snapshot-draft/v0` record
vocabulary. It contains no RIFT or SuperNu evidence, executes no adapter, and
proves no conformance, semantic equivalence, round trip, or production
compatibility.

`file-backed.snapshot.json` and `indexed.snapshot.json` deliberately use
different non-normative backend descriptions while carrying equal selected
synthetic fields. The focused test removes producer/backend identity, native
state/IDs and opaque artifact handles before comparing that selected-field
projection.
