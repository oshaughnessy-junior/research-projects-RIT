#!/bin/bash
set -eu
RUNTIME=/home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime
CODE=/home/oshaughn/rift-lensing-prototype/external/rift-o4d-03d7fe8/MonteCarloMarginalizeCode/Code
export PYTHONPATH="$RUNTIME/compatible:$CODE:$RUNTIME"
export GW_SURROGATE=''
MODEL="${1:?constant or gaussian}"
/home/oshaughn/envs_for_claude/rift-lensing-event-demo/bin/python "$CODE/bin/util_ConstructEOSPosterior.py" --fname "$RUNTIME/$MODEL/evaluated.dat" --parameter u_source_x --parameter u_lens_y --integration-parameter-range 'u_source_x:[0,1]' --integration-parameter-range 'u_lens_y:[0,1]' --fit-method rf --rf-seed 347 --sampler-method AV --internal-use-lnL --n-max 100000 --n-step 10000 --n-eff 2000 --fname-output-samples "$RUNTIME/$MODEL/native-result" --no-plots --fname-output-integral "$RUNTIME/$MODEL/native-result" > "$RUNTIME/$MODEL/native.log" 2>&1
