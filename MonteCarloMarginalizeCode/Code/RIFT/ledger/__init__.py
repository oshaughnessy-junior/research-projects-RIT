"""Ledger runs: adaptive importance sampling of the intrinsic posterior with a stop rule.

A ledger run replaces the iteration loop of a RIFT analysis by learning versions,
which adapt a proposal with an evaluable density, and one frozen round, whose
importance-weighted estimate is reported with a measured error.  `bin/rift-run` drives
one run; `RIFT/ledger/DESIGN_ledger_run.md` describes it.
"""
