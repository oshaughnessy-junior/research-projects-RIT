# AV2: Varaha v2 with a stall guard and a refresh pass

`mcsamplerVaraha2.MCSampler`, reached with `--sampler-method AV2` or as a portfolio member
`--sampler-portfolio AV2`. Default off. Measurements: RIFT_roboto_paper
`analyses/varaha_v2_evaluation_20261114/` (synthetic targets) and the S250114ax ILE record named there.

## Algorithm

Source: Tiwari, arXiv:2405.16568; `github.com/vaibhavtewari/Varaha` `v2/` at `234c820`.

1. Cycle 1 draws `neff_gate/frac` points uniformly over the box (unit cube `u`).
2. Every draw is kept. Its density is the deterministic mixture over all cycles' grids,
   `q(u) = sum_c n_c(bin_c(u)) / |bin_c|` (expected draws per unit volume).
3. The threshold on `g = lnL + ln p` rises when the live set (`g` above threshold) has
   `Kish(1/q) > neff_gate` and `sum(1/q)/max(1/q) > nsel`. The new threshold keeps a fraction
   `frac` of the live set's sampling volume, measured with weights `1/q`.
4. The next grid has about `1/dV` bins over the adaptive axes, `dV = V/sqrt(Kish)`, with
   randomized per-axis counts. Occupied bins are those holding a live draw.
5. Draws per bin: `(1-post_frac)` of them fill under-sampled bins (`max B - B_j`, `B_j` the summed
   density of live draws in bin `j`); `post_frac` follow the bin's posterior weight `sum L p/q`.
   Every bin with weight above `post_floor` times the maximum gets at least one draw.
6. Cycles far below the posterior bulk are removed, with their draws and density terms.
7. Adaptation stops when the posterior mass above the threshold is below `stop_pinside`, or at
   `adapt_budget_frac * nmax` draws. Each grid is sized to fit the remaining adaptation budget
   before it is drawn, so adaptation never takes the refresh pass's share.
8. Refresh: draw from the frozen mixture, with a share `defensive_frac` uniform over the box, until
   `n_eff >= neff` or `nmax`. lnZ, its error and the exported rows come from these draws alone.

Steps 1-4 and 6-7 follow upstream. Step 5's posterior share and step 8 are RIFT additions.

## Why each addition exists

| Addition | Failure without it | Measured |
|---|---|---|
| posterior share in step 5 | 8-D mixture of three elongated Gaussians: each occupied bin holds about one live draw, most bins round to zero new draws, a few high-weight draws are never refined. ESS ~1, lnZ off 2-13 nats at 2e6 calls | with the share: ESS 4e3, lnZ within 0.03 |
| refresh (step 8) | the pooled adaptive estimate is biased low by 0.01-0.015 nats in 6-8 D at `frac=0.8`, from proposals and stopping chosen with the same weights | refresh error consistent with 0 in 4 seeds on each target. It cannot recover a mode the adaptation missed (below) |
| defensive component | the mixture density is zero outside bins ever occupied | makes the estimator consistent; at 5% it reaches a mode of relative volume 1e-13 only in about 1e-14 of passes |
| verdict | a stalled pass returns a finite lnZ | `live_volume_collapsed` set when refresh `n_eff < 10`, or k-hat > 0.7 with Kish < 100 |

The upstream driver also indexes the bin-density array by bin rather than by draw
(`bin_injd[ii] += exp[ii]`). That changes only the allocation, not the estimate. This module
uses `np.add.at`.

## Contracts

- Driver: `add_parameter`, `setup`, `integrate_log(lnF, *names, **kw)`, `integrate`. Pinned
  parameters enter as in AV: drawn at the pinned value, with `1/width` in the sampling density.
- `_rvs`: finite refresh rows; `log_joint_s_prior` is rescaled so `logsumexp(lw) - log(n_rows)`
  reproduces the returned lnZ (the AV convention).
- Portfolio: `joint_p_s_is_normalized_density = True`; `sampling_density(X)` is the normalized
  mixture including the defensive term; `draw_simplified` draws from it; `has_unbounded_support`
  is true while `defensive_frac > 0`; `update_sampling_prior_selfish` runs one cycle on the
  member's own draws; `reset_selfish_state` restarts adaptation.
- Device: bookkeeping is on the host. The integrand is called on the device first, with the AV
  host fallback.

## Parameters

| Name | Default | Note |
|---|---|---|
| `frac` | 0.8 | paper 0.95; 0.95 needs about 5x more calls to converge |
| `neff_gate` | 2000 | paper 1e4 |
| `nsel` | 500 | paper 2000 |
| `stop_pinside` | 0.05 | paper value |
| `post_frac` | 0.3 | 0.6 gives a higher ESS on the 8-D mixture and a lower one on the 6-D needle |
| `post_floor` | 1e-3 | |
| `defensive_frac` | 0.05 | |
| `adapt_budget_frac` | 0.7 | share of `nmax` adaptation may use |

## Known limits

- A missed mode is invisible. Review case: 4-D, two equal-mass modes of width 0.3 and 0.003. In 3 of
  3 seeds lnZ came out ln 2 low with Kish ESS 4e5, k-hat -0.05 and no collapse verdict. AV shares
  this failure.
- On S250114ax (rho 82, real data) AV2 was less efficient than AV: see the paper-repo record. The
  extrinsic posterior's curved degeneracies leave most of each axis-aligned bin below threshold,
  so per-cycle draws grow to the cap while the volume shrinks a fixed `ln(1/frac)` per cycle.
- AV-only driver options (`--sampler-warmstart-*`, `--sampler-save-state`, `--sampler-load-state`)
  are refused with `--sampler-method AV2`; an AV2 portfolio member is left cold, as for any member
  without warm-start support. `--save-deltalnL`/`--save-P` thresholds are not applied to the export.
- A portfolio pass restarts AV2's adaptation (`reset_selfish_state`), so a calibration burn-in pass
  gives an AV2 member nothing. Its selfish step draws at most `n_chunk` per portfolio chunk.

- The estimate is not anytime. Before adaptation reaches the bulk the refresh is dominated by a
  few draws; the verdict reports this, and more budget is the remedy.
- Host cost per cycle grows with the number of live grids and kept draws. On the 8-D mixture
  it reached 25-75 s per 1e6 calls; on the other targets it stayed under 10 s.
- No warm start. `bootstrap_from_samples` is not implemented, so the ILE warm-start options skip
  this sampler.
- Bins are axis-aligned. Strongly correlated posteriors cost more bins; coordinates are the
  driver's job, as for AV.
