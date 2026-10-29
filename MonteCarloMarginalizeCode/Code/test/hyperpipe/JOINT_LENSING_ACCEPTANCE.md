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

### First native RF+AV control, independently checked

A completed direct `util_ConstructEOSPosterior.py` Gaussian control subsequently
became available on cardassia, under
`/home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime/gaussian`.
The validator ran the independent gate read-only through SSH against
`native-result` and paired `native-result.dat`, with the existing
`rift-lensing-event-demo/bin/python` runtime. The exact executed gate was:

```sh
ssh cardassia /home/oshaughn/envs_for_claude/rift-lensing-event-demo/bin/python - \
  --native-integral /home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime/gaussian/native-result \
  --native-posterior /home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime/gaussian/native-result.dat \
  < MonteCarloMarginalizeCode/Code/test/hyperpipe/acceptance/check_joint_lensing.py
```

The gate passed: native `lnZ=-2.7166055333934622`, exact
`lnZ=-2.710356875295474`, error `-0.0062486580979882` nat. The 2000 posterior
draws had means `(5.003001808154449, 0.680115091322078)` and covariance
`[[0.4216376441958916, 0.003743908038601144],
[0.003743908038601144, 0.2047347497964037]]` in `(x,log(y))`.
Independent reads of `joint.json` confirmed the Gaussian factory and registered
uniform/log-uniform bounds; `native.log` recorded AV `n_eff=3034.6162716756635`,
`ESS=6059.327583803366`, `sigma_mc=.0115`, `sigma_lnV=.0109`.

This admits the first bounded native interpolation/sampling control only.
Repeated seeds, constant-likelihood native control, actual iterative DAG products,
matched physical null/direct checks, and event results retain their separate gates.
The existing completed environment was preserved; the implementation worker
staged missing native dependencies in an isolated runtime overlay. Exact command
and overlay/source provenance remain part of that worker's execution record.

### Actual generated native DAG inspection

The generated DAG, not a dry-run command, was inspected read-only at
`/home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime/gaussian-dag/run/marginalize_hyperparameters.dag`.
Its SHA256 was
`b4aaadb3a5aa6b488f4b5b1b0d8d632c18dd330f65ea46dda87596e51f2d8075`.
Parsing its JOB/VARS/PARENT lines found exactly four EOS_POST workers, one at each
iteration 0 through 3. Each depends on its UNIFY and parents its JOIN_POST;
the final JOIN_POST parents convergence. `EOS_POST_worker.sub` requests queue 1,
reads cumulative `all.marg_net`, carries explicit unit ranges, RF+AV,
`--internal-use-lnL`, and `--n-eff 2000`. Its final output stem is
`iteration_3_post/output-4-$(cluster)` for plain lnZ and paired `.dat` posterior,
matching the scoped collector's path/count convention. `args_marg_eos.txt`
references the absolute shared-filesystem `joint.json`. No ILE was invoked.

The generated artifact check was performed with read-only SSH, reading the DAG
and `EOS_POST_worker.sub`, and a Python `shlex` parser mapping JOB names to submit
files and VARS iteration macros, then counting EOS_POST workers and parent/child
edges. Equivalent bounded shell inspection is:

```sh
ssh cardassia 'cat /home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime/gaussian-dag/run/EOS_POST_worker.sub'
ssh cardassia 'grep -E "JOB|VARS|PARENT" /home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime/gaussian-dag/run/marginalize_hyperparameters.dag'
ssh cardassia 'cat /home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime/constant/native-result'
```

The final command independently returned exactly `0` for the completed native
constant-likelihood log evidence. The implementation worker reports ESS=10000;
the validator independently read the integral product, not that ESS claim.

Actual DAG generation exposed two issues that are corrected in the candidate:
the builder now explicitly sets `explode-marg-jobs-last=1` (the native default
would otherwise create two final workers), and event staging skips a copy only
when `os.path.samefile` identifies the staged destination as the source. The
independent collection suite now has seven passing tests, including execution
of the actual event-copy loop in isolation: identical path and symlink aliases
preserve content, a distinct legacy source copies correctly, and a missing
source still raises. The guard does not suppress unrelated copy errors.

The iterative DAG was generated and inspected, not submitted or completed.
The Gaussian and constant controls executed the actual native posterior sampler
directly. Repeated seeds and full physical source/lens event inference remain
unadmitted at this checkpoint.

### Repeated native control and matched physical manifests

A second completed Gaussian run was independently gated using the same command
above with `gaussian-seed2` substituted for `gaussian`. It passed:
`lnZ=-2.71153962780717`, exact `-2.710356875295474`, 2000 draws, posterior means
`(5.004428648839601, 0.6921787361431133)`, covariance
`[[0.43615915301565467,-0.0001309081246292907],
[-0.0001309081246292907,0.1947568476407829]]`.
The validator independently read `rift-second-seed.py`: it sets NumPy seed 991
and `--rf-seed 991`, reuses the evaluated Gaussian grid, and supplies explicit
output paths. Thus two native RF+AV fits/sampling runs meet the registered
Gaussian evidence and posterior-shape gates.

Independent reads of `event-manifests/lensed.json` and `unlensed.json` confirmed
matching 15 source/nuisance coordinate priors, fixed geocentric time, data
preparation directory, cleaning and maximum frequency. The lensed model adds only
`MLz` and `y`. These are explicitly declared experimental priors: log-uniform
amplitude and uniform declination, rather than a volume-distance and isotropic
sky prior. Their normalization/matching does not make them astrophysical defaults.
Physical event waveform/posterior/evidence admission remains separate.
