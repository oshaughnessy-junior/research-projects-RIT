# RIFT TD-likelihood — STABLE PROJECT CHECKOUT

## DO NOT DELETE THIS DIRECTORY

This is the **permanent, stable RIFT checkout** for the TD-likelihood paper-2 production
runs under `~/paper2_runs/`. Live HTCondor DAGs execute submit-host binaries directly out
of this tree. Deleting or moving it will kill running analyses.

| | |
|---|---|
| Path | `/home/richard.oshaughnessy/vaishak_p/rift_tdlike` |
| Repo | `research-projects-RIT` (**full clone**, not a worktree) |
| Branch | `tdlike_paper2` |
| Pinned RIFT code tree | `0a62d736ab066724bdb8d346116bf0ea6cb510d6` (`git rev-parse HEAD:MonteCarloMarginalizeCode`) |
| Pin taken from commit | `3e1287bbc75e406b2680a4163feb70584f3e2593` (`3e1287bb`, provenance only) |
| Created | 2026-07-29 |
| `RIFT_CODE` | `<this dir>/MonteCarloMarginalizeCode/Code` |

## Why this exists

The paper-2 runs were originally instantiated with `RIFT_CODE` pointing at an **agent git
worktree**:

```
/home/richard.oshaughnessy/RIFT_develUWM/src/research-projects-RIT/.claude/worktrees/agent-ac598e8a962e15d3e
```

A worktree is registered with its parent repo and can be removed by `git worktree prune`,
by a stale-worktree cleanup, or by the agent harness that created it. On 2026-07-29 four
production DAGs (GW250114 U / MF / MP_offmerger, GW191109 U) were depending on that path
for:

- every submit-host `local`-universe node — `join.sub`, `evidence.sub`, `convert.sub`,
  `test.sub`, `PUFF.sub`, plus `unify.sh` / `join_grids.sh` / `command-single.sh`;
- the GW250114 MF/MP `ILE.sub` `transfer_input_files`, which ships
  `test/td_paper2_runs/GW250114/basis/glitch_basis_L1.npz` to the worker nodes.

A **clone** has its own `.git` directory and is invisible to the parent repo's worktree
bookkeeping, so it cannot be pruned. That is the whole point of this directory.

## What is in here

A full clone at the pinned commit, verified byte-identical to the original worktree
(`diff -rq` over `MonteCarloMarginalizeCode/Code` clean apart from `__pycache__`; md5 match
on the runtime-critical binaries and the glitch basis `.npz`).

**Plus** the git-ignored but run-referenced per-event data under
`MonteCarloMarginalizeCode/Code/test/td_paper2_runs/`:

- `*/coinc.xml`, `*/data/` (frame caches), `*/ini/` — ignored by that subtree's
  `.gitignore` as "regenerable per-event products", but referenced by the runs'
  `reproducibility/` records.

These were copied in from the original worktree so that every path referenced by the run
directories resolves here. They are regenerable via each event's `fetch_data.sh` /
`build_basis.sh` / `Makefile` if ever lost. Because they are git-ignored, `git status`
stays clean — do not be surprised that they are untracked.

## Remotes

Matching the parent repo:

| Remote | URL |
|---|---|
| `origin` | `https://github.com/oshaughn/research-projects-RIT.git` (public — **do not push project branches here**) |
| `junior` | `https://github.com/oshaughnessy-junior/research-projects-RIT.git` |
| `junior_private` | `https://github.com/oshaughnessy-junior/rift_private.git` (private mirror — push here) |
| `aasim` | `https://github.com/AasimZJan/LISA-RIFT.git` |

## Rules

1. **Never delete or move** while any `~/paper2_runs` DAG is live.
2. **Do not casually `git checkout`/`pull` this tree.** Live DAGs submit new nodes against
   it continuously; changing the code underneath a running analysis silently mixes code
   versions across iterations. Treat it as pinned at `3e1287bb` for the campaign's duration.
   (Repinned 2026-08-13: merge of upstream `rift_O4d` @ `563146ed` / 0.0.18.0rc2, which
   brings the high-SNR extrinsic-sampler flags `--sampler-warmstart-retry-neff` and
   `--portfolio-varaha-min-frac`.  Previous pin `1b96f488`.)
3. Do not point `RIFT_CODE` back at a `.claude/worktrees/...` path for any new run.

## How to update it (between campaigns only)

Only when no DAG is running:

```bash
cd ~/vaishak_p/rift_tdlike
condor_q -constraint 'regexp("paper2_runs", Iwd)'   # MUST be empty first
git fetch junior_private
git checkout tdlike_paper2
git merge --ff-only junior_private/tdlike_paper2
git rev-parse HEAD    # record the new pin, and update this file + ~/rift_td/runs/README.md
```

If a *new* pinned version is needed while the current campaign is still running, make a
**second** clone at a new path rather than moving this one.

## Related

- `~/rift_td/analyses/paper2_runs/STABLE_CHECKOUT.md` — migration record, verification
  evidence, branch/push audit.
- `~/rift_td/runs/repoint_to_stable_checkout.py` — idempotent fixer that rewrote the run
  dirs from the ephemeral worktree path to this one.
- `~/rift_td/runs/README.md` — run instantiation, `RIFT_CODE` conventions.
