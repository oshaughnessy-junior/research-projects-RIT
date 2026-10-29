# Joint source/lens admission controls

This fixture independently checks the proposed
`RIFT.hyperpipe.drivers.joint_lensing` seam against mathematical oracles. It
requires NumPy, uses production `JointLensingDriver.run` grid I/O and factory
calls, and bypasses only heavyweight `RIFT.__init__` initialization. It does not
replace native Hyperpipe posterior/evidence or physical waveform admission.

Run after incorporating the implementation branch:

```sh
python MonteCarloMarginalizeCode/Code/test/hyperpipe/acceptance/check_joint_lensing.py
```

For independent worktrees, point `RIFT_JOINT_DRIVER_DIR` at the candidate's
`MonteCarloMarginalizeCode/Code/RIFT/hyperpipe/drivers` directory. The fixture
imports the candidate source directly; no candidate code is copied here.

## Registered checks and observed results

On 2026-10-29, base RIFT `03d7fe8467d0216f72c313498603fceaf434d31e`,
Python bundled runtime `26.1026.10807`, NumPy 2.3.5, four checks passed against
candidate joint_lensing.py SHA256
`4a888df477cedd9317685be5d681c45355607947452aeb37c2e821bf22c8d737`:

1. Constant likelihood `L=1` integrated with normalized uniform source prior
   `x in [2,8]` and log-uniform lens prior `y in [.2,20]` gives `Z=1`.
2. Joint likelihood
   `exp(-((x-5)/.65)^2/2 - ((log(y)-log(2))/.45)^2/2)` gives
   `Z=0.06651306560901957`, versus exact truncated-normal
   `0.06651306560901961`. Posterior means are `(5, log(2))`; variances
   are `(0.4224631570262937, 0.202498294023252)` and covariance is zero
   to numerical precision. Independent closed-form erf integrals supply the
   oracle. Tensor Gauss-Legendre quadrature supplies normalized weights.
3. A bounded isotropic inclination prior has the correct first two moments
   of cosine; invalid/missing quantile inputs fail explicitly.
4. Workflow configuration carries explicit `[0,1]` sampling ranges, target
   effective samples, seeded unit-box grid, factory manifest argument, and
   rejects replacement of an existing output directory.

The physical prior density and inverse-CDF Jacobian cancel exactly in the
quantile representation. EOS_POST's live source sets `uniform_prior=1`;
normalization therefore depends on preserving every explicit unit integration
range. A physical-coordinate box with that default density is not normalized.

## Remaining admission gates

- Execute the real EOS_POST sampler with `L=1` and this Gaussian, report its
  integral file and posterior samples, repeat seeds, and compare posterior
  moments with the independent values above. The present quadrature result
  validates the driver and coordinate measure, not adaptive sampler accuracy.
- Generate and inspect the actual native DAG/submit files and terminal evidence
  dependencies. A configuration-builder or dry-run result does not establish
  the complete workflow chain.
- Compare physical detector-strain likelihoods, including lens-dependent norms,
  against an independent direct evaluation; match source, nuisance and noise
  conventions for the unlensed model and a zero-distortion control.
- If ILE is invoked, preserve all four flags `--time-marginalization
  --vectorized --gpu --force-xpy` and assert the NoLoop path from logs. The
  direct StaticNetwork adapter does not invoke ILE and is not NoLoop evidence.
- Shared filesystem accessibility of the absolute factory manifest/data paths
  must hold on every execute node. Transfer-mode execution requires its own
  manifest/data shipping and path-resolution validation.

No event posterior, Bayes factor, model preference, or full-source inference is
admitted by these deterministic seam checks.

## Native collection and independent numerical gate

Source inspection at the pinned base finds EOS_POST workers depending on UNIFY,
JOIN_POST depending on workers, and convergence depending on JOIN_POST. In
`create_eos_posterior_pipeline`, lines 517-520 establish each worker's integral
stem; lines 538-548 join only posterior `.dat` files. In
`util_ConstructEOSPosterior.py`, lines 1549-1550 restore the absolute likelihood
shift and write the plain worker `lnZ` file. JOIN_POST is not an evidence combiner.

The candidate collector therefore supports exactly one final worker and refuses
ambiguous aggregation. Independent negative controls:

```sh
python MonteCarloMarginalizeCode/Code/test/hyperpipe/acceptance/check_joint_collection.py
```

Six tests pass: valid single-worker product, wrong iteration, missing paired
posterior, duplicate workers, multiworker configuration, nonfinite integral,
out-of-prior/wrong-schema/nonfinite posterior, and missing cumulative likelihood.
The validator tightened the production collector to require positive iterations,
explicit unit ranges, exact quantile schema, finite values and bounded quantiles
in the final worker posterior, joined posterior and cumulative likelihood table.
These checks establish artifact validity only. They cannot establish which
likelihood generated an artifact or whether the inference converged.

After a native Gaussian control is executed, use this independent gate on each
of at least two separately seeded runs:

```sh
python MonteCarloMarginalizeCode/Code/test/hyperpipe/acceptance/check_joint_lensing.py \
  --native-integral /path/to/iteration_3_post/output-4-CLUSTER \
  --native-posterior /path/to/iteration_3_post/output-4-CLUSTER.dat
```

The gates are absolute log-evidence error <=0.03 nat, posterior means within
0.1 of the smaller marginal standard deviation, each variance within 10 percent,
and source/log-lens covariance within 0.1 times the product of marginal standard
deviations. Require at least 2000 draws and separately inspect the sampler's
effective sample size; repeated resampled draws do not establish independent ESS.
This command expects the exact Gaussian factory and priors above and uses the
plain absolute `lnZ` product, never the maximum likelihood or joined sample file.

At this checkpoint, native sampler accuracy and the generated DAG have not been
admitted: no completed native control artifacts were available to this validator.
