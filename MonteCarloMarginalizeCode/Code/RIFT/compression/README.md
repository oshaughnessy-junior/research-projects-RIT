# Automatic posterior compression

The standalone executable compresses a completed RIFT posterior without waveform
calls or new PE sampling. The pipeline can run the same executable after final
extrinsic export and calibration reweighting.

```bash
util_RIFT_compress_posterior.py --config /path/production.ini \
    --rundir /path/completed_run
```

The run directory supplies `args_ile.txt`, `args_cip_list.txt` and
`extrinsic_posterior_samples.dat`. An alternative paired table can be selected
with `--posterior FILE`. Relative posterior and output paths resolve under
`--rundir`. `--output NEW_DIRECTORY` preserves an earlier candidate or failed run.
Existing output directories are never overwritten.

## Configuration

Add this section to the original production INI:

```ini
[rift-auto-fit]
enabled = true
targets = precessing_source_redshift, aligned_distance
component_counts = 1,2,4,8
output_directory = auto_fit
seed = 711
max_iterations = 200
validation_ks = .08
request_cpus = 1
request_memory_mb = 4096
request_disk_mb = 2048
max_runtime_seconds = 1800
```

For weighted tables, set `weight_column` to the posterior weight column.
Fairdraw tables use equal weights. Set `group_column` when the table records
original intrinsic ancestry. Otherwise identical exported intrinsic rows share
a group. Preserve ancestry when doing external resampling or transformation;
distinct rows do not establish independent PE information.

Omitting `[rift-auto-fit]` disables the pipeline stage. BasicIteration accepts
`--auto-fit-config FILE` directly. Pseudo-pipe forwards its high-level INI when
enabled. Both defaults require paired intrinsic and distance samples from final
extrinsic export. Other pipeline generators can use the standalone command.
The bounded terminal job runs in local universe on the submit host, after
the paired products have returned from workers. It does not transfer PE to OSG.

## Products and statistical scope

| Product | Input/evaluation coordinates | Dimension |
|---|---|---:|
| `precessing_source_redshift` | source chirp mass, eta, s1x/s1y/s1z, s2x/s2y/s2z, redshift | 9 |
| `aligned_distance` | source chirp mass, eta, s1z, s2z, luminosity distance | 5 |

Masses use solar masses, distances use Mpc and dimensionless spins use the
generating L frame at the recorded reference frequency. Component masses are
ordered, with their spin labels preserved. The internal chart uses detector
chirp mass, mass asymmetry `delta=sqrt(1-4 eta)`, spins and luminosity distance.

The fitted density represents the native posterior. Dividing by the generating
retained-coordinate prior gives a scalar likelihood up to a fixed event constant.
Marginalized spins and other nuisance parameters retain the original conditional
prior. The aligned product of a precessing analysis integrates transverse spins;
it does not impose an exactly aligned waveform model.

Model selection uses tuning groups. Validation uses disjoint held-out groups,
native marginals and joint projections. A failed gate preserves the model and
report and returns a nonzero exit code. Native recovery alone does not certify
arbitrary-prior tails or population inference. Exact singular spin-prior limits
raise an explicit error; source samples are never jittered to bypass a chart cusp.

Unsupported custom priors and unresolved final distance reweighting are refused.
`all_dslice.dat` with dropped spin columns is not a paired posterior input.
Tidal/EOS and eccentric nuisance assumptions remain those of the generating run.

## Cosmology and distance prior

The resolver reads the analysis INI and executed argument files. An omitted
auto-fit cosmology or distance prior inherits the analysis setting. Current
RIFT ILE cosmological distance priors use Planck15; conflicting declarations
are refused. Existing source columns are checked against the resolved cosmology.
The candidate stores the native cosmology and distance prior with their provenance.

Source-frame likelihood evaluation substitutes
`Mc_detector=(1+z)*Mc_source` and `dL=dL(z)`, without a Jacobian.
Native-prior densities and integration measures carry their Jacobians.
Changing the API evaluation cosmology never changes the generating distance prior.

```python
from RIFT.compression import CompressedFit, CosmologyContext
from astropy.cosmology import Planck18

fit = CompressedFit.load("auto_fit/aligned_distance/model.json")
ctx = CosmologyContext.from_astropy(Planck18, distance_max=2500.)
logL = fit.log_likelihood(points, mass_frame="source",
                          radial_coordinate="luminosity_distance", cosmology=ctx)
```

Load each model and cosmology context once. Mixture Cholesky factors and
monotone cosmology interpolators are cached. Evaluation works on batches with
bounded internal chunks. Cosmology tables reject extrapolation.

For repeated population integrals, create one proposal bank:

```python
bank = fit.integration_bank(n=65536, seed=73, cosmology=ctx)
log_integral, diagnostics = bank.integrate_log_density(log_population_density)
```

The callback receives ordered source component masses, retained spins and redshift.
The bank includes the physical measure and proposal density. Keep its weights
unnormalized between population models. Unsupported proposals retain their original
denominator. Diagnostics report importance ESS; low ESS calls for a better bank.
Absolute event evidence is unavailable from the posterior/prior quotient.

## Synthetic checks

`demo/auto_fit/fake_ile.py` writes paired weighted posterior rows for a known
joint two-component GMM. It needs no strain or waveform library.
The tests compare recovered scalar likelihood shapes to independently evaluated
Gaussian densities and reject a deliberately underfit candidate.

```bash
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python -m pytest -q \
    MonteCarloMarginalizeCode/Code/test/test_posterior_compression.py
```

Port lineage: `oshaughnessy-junior/gw-event-likelihood-fits`, commit
`6ecb81245805a478e67443d667dfd7c9cfd533bd`.
The portable weighted-mixture/native-prior construction follows its fitting
tools. Catalog coefficients and campaign-specific importers are excluded.
