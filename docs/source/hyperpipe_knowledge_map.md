# Knowledge Map: hyperpipe.rst
**Status**: Baseline Audit
**Source**: /Users/rossma/RIFT/docs/source/hyperpipe.rst

## 1. Conceptual Narratives (The Forest)
- **Purpose**: Adaptive parameter estimation for generic simulation-based inference.
- **Core Loop**: Grid evaluation $\rightarrow$ Posterior MC integration $\rightarrow$ New grid generation $\rightarrow$ Repeat.
- **Requirement**: Needs an external executable for likelihood calculation.
- **Execution**: Highly parallelized via HTCondor.
- **The Tracer Workflow**: A faster alternative to the Puffball that reads `all.marg_net` directly.
- **Convergence**: The DAG halts when JS divergence falls below a threshold.
- **Multi-Constraint Logic**: Ability to combine likelihoods from multiple drivers (sum vs product).
- **RIFT Relationship**: Generalization of the RIFT pipeline ( coordinate-free).

## 2. Technical Specifications (The Trees)
### A. Input Requirements
- **Initial Grid**: Format `# lnL sigma_lnL <params...>`.
- **Grid Generator**: `util_HyperparameterGrid.py` (needs params, ranges, npts).
- **Marg Driver Contract**: Must accept:
    - `--using-eos`: Input grid path.
    - `--eos_start_index` / `--eos_end_index`: Row range.
    - `--fname-output-integral`: Output path.
    - `--outdir`: Output directory.
    - `--conforming-output-name`: Annotation flag.
    - `--fname`: Legacy.

### B. Configuration Logic
- **Coord Transformation**:
    - `coords-fit`: Params for GP fit.
    - `coords-sample`: Ranges for MC integration.
    - `coords-implied`: Fit but not sampled.
    - `coords-nofit`: Sampled but not fit.
- **Tracer Config**: `puff.exe` $\rightarrow$ tracer updater, `puff.input-source: marg_net`, `puff-factor`, `force-away`, etc.
- **Test Config**: `test.exe`, `test.method`, `test.threshold`, `test.settings.always-succeed`.

### C. Pipeline Stages (Internal Logic)
- **MARG/MARG_PUFF**: Likelihood evaluation per parameter.
- **CON/CON_PROD**: Consolidates and joins events (sum/product).
- **UNIFY**: Cumulative grid growth (`all.marg_net`).
- **EOS_POST**: MC integration $\rightarrow$ Posterior samples $\rightarrow$ New grid.
- **PUFF**: Dithering/Covariance-based search to avoid local maxima.

### D. File/Directory Reference
- **Root**: `grid-0.dat`, `local.cache`, `marginalize_hyperparameters.dag`, `*.sub`.
- **Iterations**: `iteration_N_marg/`, `iteration_N_post/`, `iteration_N_con/`, `iteration_N_puff/`.
- **Outputs**: `grid-*.dat` (posterior samples), `consolidated_*.net_marg` (per-iteration results), `posterior-*.dat`.

### E. Legacy / Tooling
- **Interface**: `create_eos_posterior_pipeline` (the args-file approach).
- **CLI Flags for create_eos**: `--marg-event-exe-list-file`, `--marg-event-args-list-file`, etc.
- **Monitoring**: `condor_q`, `condor_tail`, `condor_submit_dag`.
