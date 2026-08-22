# CIT Hyperpipe production-build runner

This directory contains a deliberately small, non-submitting build gate for
the OSG execution contract seen in GWTC5-HLV production.  It uses synthetic
inputs and renders representative ILE and CIP submit files.  It does not read
production posterior samples, call Asimov, or submit a DAG.

Run locally from any directory:

```sh
python MonteCarloMarginalizeCode/Code/test/hyperpipe/production_build/run_cit_execution_gate.py
```

On CIT, run as the user from a clean RIFT worktree under `~/LVK`, never from
`/home/pe.o4` or a production project directory.  Pin the checkout and retain
the uniquely named private output:

```sh
python MonteCarloMarginalizeCode/Code/test/hyperpipe/production_build/run_cit_execution_gate.py \
  --expected-commit "$(git rev-parse HEAD)" \
  --output-root ~/LVK/hyperpipe-build-gate/runs
```

The runner refuses checkout/output paths beneath `/home/pe.o4`, never reuses
or deletes an output directory, installs failing shims for scheduler commands,
and records `submitted: false` in its report.  An HTCondor parser check is
available only through the explicit `--condor-dry-run --real-condor-submit
/path/to/condor_submit` combination; the resolved executable is never inferred
after the blocking shims are installed.

This is not yet the complete production topology gate.  The next layer will
drive pseudo-pipe through final convergence, batched extrinsic export,
Bilby-pickle generation, calibration fan-out/merge, and PESummary.  Keep that
as a bounded profile matrix (initially 3--5 cases, sequential on a login node),
not hundreds of event rebuilds.  A separate outer service may schedule this
runner later; the inner runner must remain unable to submit work.
