.. _util_ConstructIntrinsicPosterior_GenericCoordinates:

util_ConstructIntrinsicPosterior_GenericCoordinates
===================================================

This executable is a core component of the RIFT (Rapid Inference for Tidal) pipeline, responsible for constructing intrinsic posterior distributions of binary parameters. It takes pre-computed log-likelihood (lnL) data from parameter estimation runs, fits a smooth model to this likelihood landscape, and then uses Monte Carlo (MC) sampling to generate posterior samples in the intrinsic parameter space.

It supports a variety of fitting and sampling methods, making it flexible for different analysis scenarios, including those involving neutron star Equations of State (EOS).

Usage
-----

The script is executed from the command line, accepting numerous arguments to control its behavior. A typical invocation might look like this:

.. code-block:: bash

    python util_ConstructIntrinsicPosterior_GenericCoordinates.py \
        --fname-ile-output "ile_data.dat" \
        --lnL-offset 10 \
        --fit-method "gp" \
        --sampler-method "adaptive_cartesian" \
        --n-max 100000 \
        --n-eff 1000 \
        --fname-output-samples "intrinsic_posterior_samples.xml.gz" \
        --fname-output-integral "intrinsic_integral" \
        --coord-names "mc,eta,chi_eff" \
        --prior-in-integrand-correction "uniform_over_volumetric" \
        --desc-ILE "My Analysis"

Inputs
------

*   **lnL data files:** Typically ASCII `.dat` or `.txt` files (e.g., from `integrate_likelihood_extrinsic`) containing parameter points and their corresponding log-likelihood values.
    *   `--fname-ile-output`: Path to the input lnL data file.
*   **LALInference samples (optional):** Can compare results with samples from LALInference.
    *   `--fname-lalinference`: Path to LALInference XML or HDF5 sample file.
*   **EOS files (optional):** For neutron star binaries, files describing the Equation of State.
    *   `--using-eos`, `--tabular-eos-file`: Paths/names for EOS data.
*   **Prior dictionaries (optional):** Pre-defined prior distributions.
    *   `--import-prior-dictionary-file`: Path to a `joblib` dumped prior dictionary.
*   **Oracle reference samples (optional):** For some advanced sampling methods, reference samples can guide initial exploration.
    *   `--oracle-reference-sample-file`: Path to reference samples.

Outputs
-------

*   **Posterior samples:** XML or HDF5 files containing the generated intrinsic posterior samples.
    *   `--fname-output-samples`: Path to save the output samples.
*   **Integral results:** Files containing the marginalized likelihood integral value (evidence).
    *   `--fname-output-integral`: Base name for files storing integral results and annotations.
*   **Plots (optional):** 1D cumulative distribution functions (CDFs) and 2D corner plots of the posterior distributions.
    *   `--no-plots`: Disable plot generation.
*   **Prior dictionary (optional):** Can export the sampler's prior definitions.
    *   `--output-prior-dictionary-file`: Path to save the prior dictionary.

Key Features & Arguments
-------------------------

*   **Likelihood Fitting Methods (`--fit-method`):**
    *   `quadratic`: Fits a quadratic surface to the lnL data (default for many applications).
    *   `polynomial`: Fits a general polynomial surface.
    *   `gp` / `gp_hyper` / `gp-pool` / `gp-torch` / `gp-xgboost` / `gp_lazy` / `gp_sparse`: Various Gaussian Process-based fitting methods, offering different levels of sophistication and computational cost.
    *   `nn`: Neural Network-based fitting.
    *   `rf` / `rf_pca`: Random Forest-based fitting, with an option for PCA preprocessing.
    *   `rbf`: Radial Basis Function-based fitting.
    *   `kde`: Kernel Density Estimation.
    *   `cov`: A placement-only approximation using covariance (no errors used).
    *   `weighted_nearest`: A weighted nearest-neighbor approach.
*   **Sampling Methods (`--sampler-method`):**
    *   `adaptive_cartesian`: Standard adaptive Monte Carlo sampler.
    *   `adaptive_cartesian_gpu`: GPU-accelerated adaptive Monte Carlo.
    *   `GMM`: Gaussian Mixture Model-based sampling.
    *   `AV`: Adaptive Volume sampling.
    *   `NFlow`: Normalizing Flow-based sampling.
    *   `portfolio`: Combines multiple sampling strategies.
*   **Parameter Coordinates (`--coord-names`):** A comma-separated list of intrinsic parameters to use for fitting and sampling (e.g., `mc,eta,chi_eff`).
*   **Prior Reweighting (`--prior-in-integrand-correction`):** Adjusts the integrand to account for different prior choices (e.g., `uniform_over_volumetric`, `volumetric_over_uniform`).
*   **Data Truncation and Selection:**
    *   `--lnL-offset`: Threshold for selecting significant lnL points for fitting.
    *   `--cap-points`: Maximum number of points to retain for fitting.
*   **Convergence Testing:**
    *   `--n-eff`: Target effective number of samples for convergence.
    *   `--fail-unless-n-eff`: Exits if the effective sample size is below this threshold.
*   **EOS Integration:** Allows incorporating Equation of State models for tidal parameters (e.g., `lambda1`, `lambda2`) based on component masses.
    *   `--using-eos`: Specifies the EOS model.
    *   `--eos-param`: Parameterization for the EOS.

Detailed Explanation
-------------------

The script proceeds in several stages:

1.  **Data Loading and Preprocessing:** Input lnL data is loaded. Optionally, it can be filtered based on the `lnL-offset` and `cap-points` arguments to focus on regions of high likelihood. Coordinate transformations are applied as needed.
2.  **Likelihood Fitting:** One of the many available fitting methods is used to model the lnL surface. This creates a continuous representation of the likelihood in the intrinsic parameter space.
3.  **Monte Carlo Integration:** An `mcsampler` instance (or one of its specialized variants) is configured with the fitted likelihood and specified priors. The sampler then explores the parameter space, drawing samples according to the posterior distribution. Adaptive sampling techniques are used to efficiently explore high-likelihood regions.
4.  **Output Generation:** The resulting posterior samples, integral value, and optional plots (1D CDFs, 2D corner plots) are saved. The posterior samples are typically converted into `lal` XML or HDF5 format for compatibility with other gravitational-wave analysis tools.

This executable plays a crucial role in the RIFT pipeline by transforming raw likelihood evaluations into meaningful posterior probability distributions for intrinsic source parameters, which are essential for astrophysical interpretation.
