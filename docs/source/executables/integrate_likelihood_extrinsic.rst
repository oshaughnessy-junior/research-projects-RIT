# integrate_likelihood_extrinsic

The `integrate_likelihood_extrinsic` executable integrates the prefactored likelihood function over extrinsic parameters to determine the marginalized likelihood for a given set of intrinsic parameters.

## Overview

This tool is a core component of the RIFT pipeline. It takes a set of intrinsic parameters (like component masses) and integrates over the extrinsic parameters (sky location, distance, inclination, polarization, and time) to compute the marginalized likelihood. This allows the pipeline to efficiently explore the intrinsic parameter space by "marginalizing out" the extrinsic ones.

## Usage

### Basic Command Line
```bash
integrate_likelihood_extrinsic [options]
```

### Primary Options

#### Data and PSDs
- `-c, --cache-file`: LIGO cache file containing the necessary strain data.
- `-C, --channel-name`: Instrument and channel name (e.g., `H1=FAKE-STRAIN`). Can be specified multiple times.
- `-p, --psd-file`: Instrument and PSD file (e.g., `H1=H1_PSD.xml.gz`). Can be specified multiple times.
- `-k, --skymap-file`: Use a provided FITS skymap for sky location integration.
- `-t, --event-time`: GPS time of the event (required if `--coinc-xml` is not provided).
- `-x, --coinc-xml`: `gstlal_inspiral` XML file containing coincidence information and event time.

#### Waveform Configuration
- `-a, --approximant`: Waveform family to use (e.g., `TaylorT4`).
- `-A, --amp-order`: Amplitude corrections order (default is Newtonian).
- `--l-max`: Maximum multipole mode $l$ to include (default is 2).
- `-f, --reference-freq`: Waveform reference frequency (default 100 Hz).
- `--fmin-template`: Waveform starting frequency (default 40 Hz).

#### Integration Controls
- `--n-max`: Maximum number of sample points to draw (default $10^7$).
- `--n-eff`: Target number of effective samples before termination (default 100).
- `--sampler-method`: Integration method. Options: `adaptive_cartesian`, `GMM`, `adaptive_cartesian_gpu`.
- `--gpu`: Enable GPU acceleration. Requires `--vectorized` and a working `cupy` installation.
- `--vectorized`: Use numpy array-based manipulations instead of LAL data structures for speed.

#### Intrinsic Parameters
- `--mass1`, `--mass2`: Component masses in solar masses.
- `--eff-lambda`, `--deff-lambda`: Effective tidal parameters.
- `--pin-to-sim`: Pin intrinsic values to a `sim_inspiral` table entry in a provided XML.

#### Output
- `-o, --output-file`: Filename for the result.
- `-O, --output-format`: Output format, either `xml` or `hdf5`.
- `-S, --save-samples`: Save individual MC sample points to the output file.

## Functional Logic

1. **Data Loading**: Loads strain data from the cache and PSDs for each detector.
2. **Precomputation**: Calls `factored_likelihood.PrecomputeLikelihoodTerms` to compute terms that are independent of the extrinsic parameters.
3. **Integration**:
    - Defines a likelihood function that computes the factored log-likelihood for specific extrinsic parameters.
    - Uses a Monte Carlo sampler (`MCSampler`) to integrate over the extrinsic parameter space.
    - Supports adaptive sampling to focus on regions of high likelihood.
4. **Marginalization**: Computes the integral of the likelihood over the extrinsic parameters:
   $$\mathcal{L}_{marg} = \int \mathcal{L}(\theta_{ext}) d\theta_{ext}$$
5. **Maximization (Optional)**: If `--maximize-only` is set, it uses `scipy.optimize.fmin` to find the maximum likelihood point (MLP) in the extrinsic space.

## Output Details

The tool outputs the marginalized log-likelihood ($\ln \mathcal{L}_{marg}$) and the estimated relative error. If `--save-samples` is used, it also exports the sample points used in the integration, which can be used for posterior estimation.
