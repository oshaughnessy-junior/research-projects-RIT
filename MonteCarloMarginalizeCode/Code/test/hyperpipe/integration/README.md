# Pseudo-pipe pipeline-build gate

This integration gate drives the real `util_RIFT_pseudo_pipe.py` entry point
through a default builder invocation plus equivalent explicit BasicIteration
and Hyperpipe builds. It requires the normal RIFT science environment, but it
does not submit a DAG or evaluate a likelihood.

Run from the repository root:

```sh
python MonteCarloMarginalizeCode/Code/test/hyperpipe/integration/run_pseudo_build_gate.py
```

The gate generates equivalent XML and self-describing Hyperpipe ASCII seed
grids from the same physical points, compares shared ILE and CIP arguments,
proves the omitted builder is semantically identical to explicit
BasicIteration, checks required submit executables, verifies both reweighting
dependency chains plus Hyperpipe calibration macros, and checks representative
unsupported options fail explicitly. Use `--keep-output` to retain the build
trees for inspection.

This test is deliberately outside the fast `test/hyperpipe/tests` pytest suite
because a full pseudo-pipe build imports the RIFT/LAL stack and takes several
seconds per builder.

The companion execution gate needs no running scheduler:

```sh
python MonteCarloMarginalizeCode/Code/test/hyperpipe/integration/run_terminal_execution_gate.py
```

It renders a generic three-batch command fan-out, merge barrier, and positional
conversion stage. A small local executor reads the generated DAG and submit
files, expands their macros, follows terminal dependencies, runs stub jobs, and
checks the final artifact. This validates filesystem and argument behavior that
pure DAG inspection cannot exercise.
