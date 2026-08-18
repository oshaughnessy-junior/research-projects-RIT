# Architecture roadmap

> **Status:** This is a directional record, not implementation authorization.
> It authorizes no code, API or default, dependency, CI-gate, branch, or
> deployment change. Each implementation slice requires a canonical SOR issue,
> named owner, bounded budget and stop condition, compatibility declaration,
> tests, and independent adversarial review.

This roadmap orders strategic proofs. It is intentionally not a backlog of
local defects or stale code. Each stage should land as small, reviewable slices
with explicit compatibility and scientific validation.

## Now: establish the cross-domain seam

Use RIFT and SuperNu as the first paired application.

- Preserve the four boundaries in `CONTRACT_BOUNDARIES.md`: domain science,
  evaluation request/result, archive read, and campaign controller. Do not
  collapse them into one universal simulation-manager API.

- First define only a non-conformant evaluation request/result vocabulary with
  synthetic positive and negative fixtures. It references separately owned
  domain-schema identifiers and carries declared uncertainty, provenance, and
  failure state without defining units, coordinates, priors, normalization, or
  universal content identity.
- In later, separately authorized issues, map one current RIFT and one SuperNu
  evaluation through adapters while preserving established production entry
  points. Observable adapter behavior must not prescribe JSONL, SQLite, or
  another storage backend.
- Define and test the minimum HyperPipe controller boundary only after adapter
  evidence exists: campaign state, proposal or oracle selection, evaluation
  request, result assimilation, convergence, and escalation.
- First observe one unchanged, deterministic proposal-policy seam without
  treating that trace as controller conformance or durable controller state.
  Assimilation, failure, convergence, and restart remain later, separately
  authorized decisions.
- Run thin campaign slices and retain golden migration evidence only after the
  vocabulary, adapter, and controller increments pass their own reviews.
- Put one scientifically useful JAX path in CI with a reference comparison.
- Exercise the corresponding backend-neutral oracle or integrator contract in
  a SuperNu or related costly-simulation case before claiming cross-domain
  generality. The non-GW implementation need not use JAX internally.

Exit evidence: both domains exercise the same contract and controller boundary,
with provenance and comparable scientific diagnostics, while supported O4
workflows remain unchanged by default.

## Next: harden adaptive inference

- Evaluate trained sampling oracles against representative multimodal,
  high-dimensional, and failure-prone campaigns.
- Harden loud-signal and three-generation-interferometer integration with
  shape-sensitive, effective-sample-size, and calibration gates.
- Make restart, caching, partial failure, and heterogeneous-cost behavior
  explicit in the evaluation substrate.
- Add a second non-GW adapter, preferably for numerical or hydrodynamic
  simulation, to challenge assumptions revealed by the first transfer.
- Version shared status vocabularies and migration rules; use the independent
  drift sentinel to monitor project groups and their dependency DAGs.

Exit evidence: adaptive choices improve robustness or cost on agreed benchmarks,
campaigns resume without scientific ambiguity, and contract drift is reported
before integration failure.

## Later: portfolio-scale reuse

- Support disconnected groups of collaborating projects without forcing a
  single release cadence or dependency environment.
- Permit multiple numerical portfolios behind the same inference contracts,
  chosen by measured regime suitability rather than domain names.
- Establish release and deprecation windows for shared contracts and adapters.
- Build campaign-level comparisons across simulation fidelities, emulators, and
  direct evaluations while retaining uncertainty and provenance.
- Extend the architecture to population and hierarchical inference without
  coupling those scientific models to one scheduler or array backend.

Exit evidence: a new costly-simulation project can adopt the shared seams with a
small domain adapter, can remain on an independent dependency cadence, and can
reproduce a campaign from durable records.

## Decision discipline

For each proposed slice, future agents should record:

1. the scientific outcome and, for a cross-domain or general-capability claim,
   the two or more domains that test it; domain-specific work may remain within
   one domain but must not imply generality;
2. the contract and dependency edges affected;
3. the compatibility tier and migration path;
4. the reference, shape, calibration, and operational tests required;
5. the bounded resource budget and stop conditions; and
6. an adversarial review before landing.

Defer work that only improves internal neatness, duplicates an existing
workflow system, or requires simultaneous migration of many repositories. If a
proposal cannot be validated as a thin cross-domain slice, reduce its scope
before implementation.
