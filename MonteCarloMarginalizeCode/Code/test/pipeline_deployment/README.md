# Pipeline deployment regression checks

Run in an installed LALSuite/GWPy/HTCondor environment:

```sh
python -m pytest MonteCarloMarginalizeCode/Code/test/pipeline_deployment/test_deployment_cli.py -q
```

These tests execute the real BasicIteration and AlternateIteration builders,
pseudo_pipe, runtime subdag builder, frame truncation entry point, and native
`condor_submit -dry-run`. They submit no jobs and evaluate no likelihoods. The
synthetic frame checks write GWF files, join multiple input files, crop a padded
fractional interval, read the output back, and compare every strain sample and
the complete generated cache against `lalapps_path2cache`.

Fixtures cover Condor quoted waveform arguments, flag-like payload strings,
missing/default and conflicting batch settings, exact caps, shortened CIP
schedules, container/disk/OAuth propagation, extrinsic time exports, actual puff
producer dependencies, missing channels/detectors, gaps/endpoints, paths with
spaces, and rollback on a failed cache publication. Package installation supplies
both the retained shell entry point and the Python implementation; GWPy is already
in the package requirements.

Ordinary default-builder smoke passes on the unmodified branch. Regressions
address shared optional truncation/runtime-subdag paths and AlternateIteration
worker deployment parity; they do not demonstrate a defect in every standard
inference. These build and frame IO tests do not claim posterior recovery.
