#!/bin/bash

python .travis/make_fake_composite.py
# Test default sampler (constant fit)
util_ConstructIntrinsicPosterior_GenericCoordinates.py  --fname fake.composite  --parameter mtot --parameter q --parameter s1z --parameter s2z  --use-precessing --no-plots
# Test alternative sampler (constant fit)
util_ConstructIntrinsicPosterior_GenericCoordinates.py  --fname fake.composite  --parameter mtot --parameter q --parameter s1z --parameter s2z  --use-precessing --no-plots --sampler-method GMM

# Test standard sampler (GP fit)
util_ConstructIntrinsicPosterior_GenericCoordinates.py  --fname fake.composite  --parameter mtot --parameter q --parameter s1z --parameter s2z  --use-precessing --no-plots  --fit-method gp

# Test standard sampler (GP fit)
util_ConstructIntrinsicPosterior_GenericCoordinates.py  --fname fake.composite  --parameter mtot --parameter q --parameter s1z --parameter s2z  --use-precessing --no-plots  --fit-method rf

# Plotting path.  Every arm above passes --no-plots, so the corner-plot block was
# never executed in CI and went on exiting 1 under matplotlib >= 3.10 unnoticed.
rm -f posterior_corner_fit_coords.png posterior_corner_extra_coords_*.png
util_ConstructIntrinsicPosterior_GenericCoordinates.py  --fname fake.composite  --parameter mc --parameter delta_mc --parameter s1z --parameter s2z  --n-output-samples 500
# Exit status covers this today, but the driver has dormant try/except scaffolding
# around these blocks: assert the deepest figure was actually written, so a future
# swallowed plotting failure is a CI failure rather than a missing file.
ls posterior_corner_extra_coords_*.png > /dev/null 2>&1 || { echo "ERROR: corner plots not written" 1>&2; false; }
