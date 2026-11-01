# Joint source and point lens inference through native Hyperpipe

`util_HyperpipeJointLensing.py` supplies actual joint likelihood rows to the
native MARG -> CON -> UNIFY -> EOS_POST iterative pipeline. Source, amplitude,
time and lens variables can all be sampled. It never calls `profile()` or
maximizes away a sampled coordinate. The executable also builds a native YAML
workflow and initial grid. Both commands build files without submitting. Submit the generated native DAG
explicitly with condor_submit_dag after inspection.

The optional physical adapter imports `lensing_prototype.static_event` and
`point_lens` from the owning project. Add that project's `src` directory to
`PYTHONPATH` on every execute node. Prepared data remain in their owning
project. The waveform is IMRPhenomXPHM using the prepared bundle's flags. The
negative-Fourier point lens transfer is evaluated at every retained frequency,
with the positive-frequency saddle sign +i. This direct physical-polarization
likelihood does not invoke native ILE or establish a signed-mode NoLoop closure.
Do not confuse it with a profile maximum or with an already completed event PE.

Each column `u_NAME` lies in [0,1] and maps to a physical prior quantile:

* uniform: x = lo + u(hi-lo)
* log_uniform: x = exp(log(lo) + u log(hi/lo))
* isotropic angle: x = acos(cos(lo) - u(cos(lo)-cos(hi)))

These inverse CDFs include the entire physical prior measure; no additional
Jacobian belongs in the likelihood. EOS_POST's native unit density on the
explicit [0,1] ranges integrates to one. Thus L=1 has Z=1 in either model,
even with different dimension. Physical samples require applying the same
inverse CDF to posterior columns. All fixed coordinates are conditional
assumptions, not marginalized source parameters.

The factory contract is `factory(factory_config) -> likelihood(parameters,
model) -> float log_likelihood_ratio`. It must use the same noise reference
and data in both models. The physical factory returns (d|h) - (h|h)/2 with
noise-reference ln L=0; common omitted data/noise constants cancel in ln B.
The raw Hyperpipe output evidence is therefore noise-relative. Report ln B
only after convergence and interpolation validation in both runs.

Create explicit demonstration priors and launch both workflows:

```bash
python make_event_manifests.py --data-directory /ABS/prepared-data --outdir /ABS/manifests
util_HyperpipeJointLensing.py build --joint-config /ABS/manifests/lensed.json --outdir /ABS/lensed --n-seed 4096
util_HyperpipeJointLensing.py build --joint-config /ABS/manifests/unlensed.json --outdir /ABS/unlensed --n-seed 4096
util_RIFT_hyperpipe.py --config /ABS/lensed/hyperpipe.yaml
util_RIFT_hyperpipe.py --config /ABS/unlensed/hyperpipe.yaml
```

The supplied source prior has 13 source coordinates plus amplitude/time; the
lensed model adds MLz/y (17D vs 15D). Source chirp mass is log uniform, q is
uniform, spin magnitudes are uniform, spin tilts/inclination are isotropic;
phase, polarization, RA and **declination** are uniform over their listed
ranges. Uniform declination is intentionally an explicit demo prior, not an
isotropic-sky prior. The amplitude at the 1000-Mpc waveform reference is log
uniform; it is not the usual volumetric-distance prior. Replace these
preregistered demonstration choices before interpreting a scientific result.
The null excludes lens coordinates; all source/data priors otherwise match.

Inspect `args_eos_post.txt`, generated submit files and DAG edges before
execution. The initial uniform grid and four RF-fit iterations are runnable
starting settings, not an accuracy guarantee in 17 dimensions. Compare fitted
lnL against held-out physical likelihoods, check multiple seeds and posterior
shapes, increase placement, and compare evidence uncertainty across iterations.
No full event posterior or evidence is claimed by this demo.

The current builder requires a **shared filesystem**: the absolute manifest,
prepared-data paths, optional factory package and native executables must all
exist at identical paths on submit and execute nodes. OSG/transfer-only execution
is not supported by this builder. Do not switch on those modes without staging
all dependencies and rewriting their paths.

For the default four-iteration, one-worker workflow, the final raw evidence is
`run/iteration_3_post/output-4-CLUSTER` (one number, absolute noise-relative ln Z).
The corresponding `.dat` file contains posterior samples. `grid-4.dat` is the
JOIN_POST sample product; it contains no evidence. Collect with:

```bash
util_HyperpipeJointLensing.py collect --workflow /ABS/lensed/hyperpipe.yaml
util_HyperpipeJointLensing.py collect --workflow /ABS/unlensed/hyperpipe.yaml
```

The collector requires exactly one final worker, a finite scalar ln Z, finite
nonempty worker posterior and final joined samples; it rejects ambiguous worker
sets. It does not certify interpolation accuracy, Monte Carlo uncertainty or
statistical convergence. It intentionally refuses multiworker evidence merging.
