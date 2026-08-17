# Synthetic archive-adapter v1 fixture

This fixture illustrates the proposed `archive.read/v1` envelope. It contains
no RIFT or SuperNu evidence and proves no production compatibility.

`file-backed.snapshot.json` and `indexed.snapshot.json` deliberately use
different non-normative backend descriptions while exposing the same canonical
semantic record. The focused test removes adapter/backend identity and verifies
semantic equality plus explicit capability behavior.
