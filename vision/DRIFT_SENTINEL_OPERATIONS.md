# Drift sentinel state and operating model

> **Status:** Phase 0/1 architecture. This document does not authorize a runner
> deployment, credentials, CI gate, new repository, or scientific waiver.

The drift sentinel has three separately governed surfaces: protocol/code,
desired state, and execution state. Keeping them separate prevents a monitoring
job from silently redefining the scientific contracts that it monitors.

## Where each thing lives

| Surface | Proposed authoritative home | Contents | Must not contain |
|---|---|---|---|
| Protocol and offline code | this RIFT repository, `rift_drift_sentinel/` | parsers, graph validation, checks, report renderers, public example, tests | credentials, scheduler code, project checkout logic, notification targets |
| Desired state and approved baselines | a dedicated private GitHub repository, proposed `oshaughnessy-junior/drift-sentinel-registry` | registry groups, owner mappings, symbolic source policy, small sanitized golden fixtures, baseline hashes, severity policy, expiring exceptions, deployment manifests | tokens, SSH configuration, absolute local paths, raw job logs, automatically learned baselines |
| Resolved run input | runner-local ephemeral workspace | exact immutable revisions, local checkout roots, verified registry fingerprint | mutable refs in place of resolved revisions, credentials copied into the manifest |
| Compact immutable run record | a separate private GitHub repository, proposed `oshaughnessy-junior/drift-sentinel-runs`, under `runs/<deployment>/<YYYY>/<MM>/<run-id>/`, or an equivalently protected append-only store | machine report, human digest, registry commit, runner code revision, hashes of resolved manifest and any external raw bundle, attestation metadata | checkout roots, environment dumps, unbounded diffs, secret-bearing URLs or file contents |
| Operational state | runner host, proposed `~/.openclaw/state/drift-sentinel/<deployment>/` | atomic last-success pointer, notification deduplication, retry queue, health timestamps | desired contracts, waiver decisions, sole copy of a report |
| Raw diagnostic bundle | private restricted object storage when needed | bounded stderr/log evidence excluded from compact report | indefinite retention, secrets without redaction, scientific data not approved for that store |
| Portfolio digest | meta-manager digest inputs and generated rollups | latest signed status per registered group, new/resolved counts, stale-run health | authority to edit contracts, close findings, or waive science |

The dedicated private registry is the recommended state anchor. It supports
review, CODEOWNERS, commit identity, offline clones, and small fixtures without
making RIFT releases carry private project topology. It is not a secret store.
Repository names, refs, owner routing, exception rationales, and unpublished
contract details are private coordination metadata even when individual code
repositories are public.

Until that repository is explicitly created, the public pilot registry in this
branch is only a redacted starter and test input. A runner must not treat it as
the authoritative fleet registry.

Desired state and observed state should not share a repository or credential.
The runner receives read-only access to `drift-sentinel-registry` and narrowly
scoped append access to `drift-sentinel-runs`; it cannot approve its own input,
rewrite baselines, or waive a finding. Branch protection or the selected object
store policy must reject history rewrites. A Git repository is appropriate only
for compact sanitized reports at the expected low cadence; raw or high-volume
evidence belongs in restricted object storage and is referenced by hash.

GitHub history is tamper-evident and reviewable, but it is not WORM storage: a
repository administrator can delete or rewrite it. Therefore the Phase 3
deployment must describe compact GitHub run records as content-addressed and
append-only-by-policy, not physically immutable. If regulatory or scientific
policy requires true retention immutability, the report objects must also be
written to object-lock/WORM storage and the GitHub run record becomes their
signed/hash index. Until such storage is selected, at least one independently
held read-only clone and transition digests should make silent rewriting
detectable, but this does not guarantee availability after deletion.

## State flow and provenance

1. A project owner proposes a registry change in the private registry. Required
   review is determined by affected edge ownership; exceptions require the
   scientific owner and an expiry.
2. The runner checks out the registry by commit, resolves every allowed source
   ref to an immutable commit/content digest, and creates an ephemeral local
   manifest. Resolution failure is a runner-health finding, never compatibility.
3. The offline core verifies the registry fingerprint, DAGs, immutable revision
   syntax, contained paths, and declared fixtures. It executes no repository
   code and performs no network access.
4. The core emits deterministic human and machine reports. The runner applies
   redaction and size policy, writes a content-addressed immutable run directory,
   updates its atomic last-success pointer, and compares against the prior
   report for notification purposes.
5. Project-specific digests route to project owners. A compact signed summary is
   available to meta-manager for portfolio aggregation. Neither the runner nor
   meta-manager changes the registry from observations.

Every durable run record must identify the registry commit and fingerprint,
runner revision and deployment ID, core version, immutable node revisions,
runner-assigned UTC run ID/date, compact report hash, and raw-bundle hash when a
bundle exists. The manifest's local roots are excluded from the report; its hash
is retained so an authorized operator can correlate it with short-lived local
diagnostics.

### Retention proposal

- registries, approved baselines, exception history, and release attestations:
  retain indefinitely through Git history;
- compact machine reports and human digests: retain 18 months, plus one monthly
  report and every finding transition indefinitely;
- raw diagnostic bundles: 30 days by default, 90 days for an open incident, then
  delete under the storage owner's policy;
- runner ephemeral checkouts and resolved manifests: remove after a durable run
  record is verified, retaining only hashes;
- notification deduplication state: retain while a finding is open and 30 days
  after resolution.

Retention deletion is a runner/storage operation and never erases the registry
commit, report hash, or transition digest that explains a decision.

## Ownership and RACI

| Work | Accountable | Responsible | Consulted / informed |
|---|---|---|---|
| Sentinel protocol, schemas, and releases | drift-sentinel maintainers designated by coding-rift initially | feature author plus independent adversarial reviewer | all registered project owners |
| RIFT contract declarations and interpretation of RIFT findings | coding-rift | coding-rift digest owner | adjacent producer/consumer owner |
| SuperNu contract declarations and scientific interpretation | `sim_manager_supernu` scientific maintainers | their designated project agent/maintainer | coding-rift for shared edges |
| Other project group registration | that group's named scientific owner | project maintainer | drift-sentinel maintainer for schema review |
| Private registry administration and portfolio owner map | meta-manager coordination owner | meta-manager automation after explicit deployment | coding-rift, coding-sysadmin, affected owners |
| Portfolio aggregation and stale-owner/group reporting | meta-manager | meta-manager digest job | project owners and coding-sysadmin |
| RIFT runner deployment, credentials, cadence, host health, retention execution | coding-sysadmin | coding-sysadmin monitor/operator | coding-rift, infra-atlas owner |
| RIFT-specific scientific digest | coding-rift | coding-rift digest job/agent | coding-sysadmin receives runner-health status only |
| Infrastructure facts and placement constraints | infra-atlas owner | infra-atlas maintainers | coding-sysadmin and runner deployer |
| Scientific exception approval | owners on both affected sides of a cross-project edge | registry PR author records it | meta-manager and runner are informed |

`coding-sysadmin` may restart or repair the approved runner deployment under its
operations policy, but cannot downgrade severity, approve a baseline, mark an
incompatible result acceptable, or close a scientific finding. `coding-rift`
interprets RIFT findings but cannot unilaterally waive a SuperNu-owned semantic
change. Meta-manager provides the broad view across disconnected groups and
detects missing/stale ownership; it is not the scientific arbiter.

Runner operation is deployment-specific, not permanently assigned to
`coding-sysadmin` for the entire portfolio. `coding-sysadmin` is the default
operator for the shared Mac/fleet service and the RIFT deployment. Another
project group either names its own operational owner or explicitly opts into the
shared service. Meta-manager rejects an active group with no named operator and
rolls up health from every deployment; it does not inherit operational or
scientific authority merely because it aggregates the result.

## Escalation and exceptions

- Registry invalid, source resolution failed, stale last-success, storage write
  failed, or runner unavailable: coding-sysadmin operational incident; affected
  project owners are informed that compatibility is **unknown**.
- New RIFT/SuperNu incompatibility: route once to both edge owners and include it
  in the coding-rift digest. Repeat only on material change, configured reminder,
  or resolution.
- Cross-group or ownerless finding: meta-manager assigns coordination, but the
  relevant project owner supplies the scientific decision.
- Suspected compromised source, registry, fixture, or runner: stop publication,
  preserve hashes, mark the run untrusted, rotate affected credentials outside
  this system, and require a clean rerun from protected immutable revisions.
- An exception names the edge, precise finding scope, both approving owners for
  a cross-project edge, rationale, approval registry commit, and ISO expiry.
  Expired exceptions cease applying automatically; the finding returns to
  `observed`. Renewal is a new reviewed change, never an automatic extension.

## Threat and failure model

The sentinel assumes repository and pull-request content is untrusted. The core
does not import or execute it. A malicious schema may be large or crafted for
resource exhaustion; the runner must enforce checkout/file/report size and time
limits before invoking the core. Phase 1 hashes exact bytes but does not verify
Git signatures or attest source provenance; the runner must enforce allowed
repository identities and protected registry commits.

Important failure modes and mitigations:

- **Mutable ref moved:** runner resolves once to an immutable revision and records
  it; the core rejects branch names in resolved input.
- **Registry PR weakens a contract or silently updates a baseline:** CODEOWNERS
  review from affected scientific owners; baselines never learn from observations.
- **Compromised producer and consumer change together:** a matching schema is not
  proof of scientific correctness. Retain independently reviewed golden artifacts
  and external scientific validation; report wording remains “declared compatible.”
- **Secret leakage:** never store credentials, environment dumps, absolute paths,
  authenticated URLs, raw Git configuration, or arbitrary file contents in
  reports. The core fingerprints rather than copies mismatch values; runner
  redaction is defense in depth, not permission to collect them.
- **Local path race or symlink swap:** core containment checks reject an already
  escaped resolved path, but they are not a filesystem sandbox and cannot close
  every check/read race against a hostile local process. The runner uses
  read-only isolated snapshots, a low-privilege account, and no ambient secrets;
  it does not inspect a concurrently modified working tree.
- **Missing/stale evidence:** `indeterminate`, never pass. Runner health and
  scientific compatibility remain distinct statuses.
- **Non-determinism or clock skew:** run ID and date are explicit inputs; the core
  reads no clock and reports hashes of exact evidence bytes.
- **Alert fatigue:** transition-only project notifications, bounded reminders,
  and separate operational/scientific channels. Silence is not health; a stale
  run becomes an operational incident.
- **Private registry unavailable:** use a previously verified local clone in
  offline observation mode, label the registry commit age, do not fetch or update
  baselines, and report inability to publish durable state.

## Exact Phase 1 boundary and deferred work

Implemented here: strict versioned registry/resolved-input parsing, multiple
disconnected DAG groups, cycle/path/revision validation, one schema-subset check,
inventory uncertainty, expiring exception application, deterministic reports,
and compatible/incompatible fixtures.

Deferred to separately authorized changes:

- creation and protection of the private registry/state repository;
- source fetch, allowlists, signature/attestation verification, size/time limits,
  credentials, sandboxing, and runner packaging;
- coding-sysadmin deployment, launchd/cron/CI configuration, durable writes,
  retention deletion, notifications, and health monitoring;
- meta-manager ingestion and portfolio digest format;
- production RIFT/SuperNu contract extraction, owner verification, golden
  round-trip, and any promotion from `inventory_only`;
- report-to-report transition comparison and first-divergent-revision search;
- callable/CLI inspection, dependency-range solving, general JSON Schema,
  canonical scientific artifact hashing, and optional project adapters;
- any blocking CI policy or change to RIFT/SuperNu APIs, defaults, archives, or
  campaign execution.

The next highest-value increment is owner-verifying one real RIFT/SuperNu
request or result edge and producing a sanitized golden artifact at two pinned
revisions. It should land before broadening the check engine.
