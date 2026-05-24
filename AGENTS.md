# AGENTS.md - RIFT

## What is RIFT?
RIFT = Iterative parameter estimation pipeline for gravitational wave astronomy (scientific research code). Not a web app or typical software project.

## Key directories
- `MonteCarloMarginalizeCode/Code/` - Main source
- `MonteCarloMarginalizeCode/Code/bin/` - Executable scripts (100+ CLI tools)
- `MonteCarloMarginalizeCode/Code/RIFT/` - Python package
- `MonteCarloMarginalizeCode/Code/test/` - Tests

## Developer commands
```bash
# Install in editable mode inside the active project sandbox
oc-run python -m pip install -e .

# Run a specific test
oc-run python MonteCarloMarginalizeCode/Code/test/test_likelihood.py
```

## Required environment
- **lalsuite** must be installed (LIGO analysis library)
- Set: `export GW_SURROGATE=''`
- On LDG clusters: `export LIGO_USER_NAME=... LIGO_ACCOUNTING=...`

## Environment protocol
- Before installing or running tests, use the global `project-sandboxing` skill.
- Prefer the existing project sandbox via `oc-run` or `pixi run`; do not run bare installs.

## Testing
Tests use pytest but have no standard runner. Run individual test files directly.

## Important CLI tools
- `integrate_likelihood_extrinsic_batchmode` - Main PE engine
- `create_event_parameter_pipeline_BasicIteration` - Full pipeline
- `plot_posterior_corner.py` - Visualization
