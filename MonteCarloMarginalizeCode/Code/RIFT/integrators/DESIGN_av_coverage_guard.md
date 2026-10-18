# AV coverage guard (`MCSampler._coarsen_for_coverage`)

## Problem

Since #412, `integrate_log` weights each retained point by the density of the mixture of all
grids it drew from (`log_retained_density`). Each grid is the set of bins holding a live point.
A bin with no live point leaves the next grid. A retained point is in every later grid, because
it keeps its own bin occupied. So the mixture density is too high at retained points, and the
mass in bins that emptied is not counted. lnZ reads low.

This matters when the live set has fewer points than the live region has bins. That happens
when the peak is narrower than one bin on some axes and other axes are broad. Draws then rarely
land in the live region, the live set stays near `nsel`, and the broad axes hold more bins than
it has points.

Measured (enforce_bounds, n_chunk 1e4, tempering 0.1; lnZ minus exact): a Gaussian of width
0.001 on two [-1,1] axes with three flat angular axes reads -0.69 at n_eff 30 and -2.18 at
n_eff 300. The final grid holds 4-30% of the posterior mass. Replaying the same grids with fresh
draws is unbiased, so the estimator is right for fixed grids and the bias comes from the
grids being built from the points they weight. Records: RIFT_roboto_paper
`development/OPEN_av_412_narrow_flat_axes.md`.

## Guard

After each rebinning, `_coarsen_for_coverage` estimates the uncovered posterior mass as the
likelihood weight of live points alone in their bin (Good-Turing). It acts only when an
adaptive axis is unresolved (live points within 2 bin indices) and another axis spans more.
While the estimate exceeds `coverage_unseen_tol` (0.05), it coarsens the axes spanning the most
bin indices by a factor 0.8.

Choices, each measured:

- Weighted, not a count of singleton bins: the count also fires on the low-weight rim of 6-8
  dimensional live regions, where it cost the mixture matrix most of its n_eff.
- The unresolved-axis condition: without it the guard also fired on d=6-8 mixtures at early
  cycles. With it, all 24 AV matrix rows and the warm and group lanes are bit-identical to base.
- Coarsen the widest axes only: the unresolved axes keep their resolution.

`coverage_unseen_tol=0` disables the guard. `dict_return` reports `n_coarsen_cycles` and
`unseen_frac_final`; the `[AV mc diag]` line prints both.

## Not covered

`update_sampling_prior_selfish` (portfolio members) builds grids the same way but is scored
with `sampling_density`, not `log_retained_density`; it is not guarded. A d=8 mixture
(`mix_d8_n1_s303`) shows a milder form (final-grid coverage 0.95, lnZ -0.1 to -0.2 at three run
seeds) with every axis resolved; the guard does not act there.
