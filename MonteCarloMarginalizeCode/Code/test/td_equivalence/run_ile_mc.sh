#!/bin/bash
# run_ile_mc.sh -- real end-to-end ILE Monte-Carlo runs (the MC-layer gate).
# Runs integrate_likelihood_extrinsic_batchmode twice on the SAME synthetic data,
# same intrinsic point (--sim-xml), same --seed, once fd-diagonal (inv-spec-trunc 0)
# and once td-toeplitz; compares the reported lnL_marg +/- MC error.
#
# Usage: ./run_ile_mc.sh <NMAX> <NEFF>
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
WT="$(cd "$HERE/../../.." && pwd)"       # .../MonteCarloMarginalizeCode/Code -> up to Code? no
# resolve Code dir robustly:
CODE="$(cd "$HERE/../.." && pwd)"        # test/td_equivalence -> test -> Code
export PYTHONPATH="$CODE:$PYTHONPATH"
PY=~/.conda/envs/junior_rift/bin/python
ILE="$CODE/bin/integrate_likelihood_extrinsic_batchmode"

NMAX="${1:-40000}"
NEFF="${2:-3000}"
SEED=1234
EVTIME=1000000000.0

# Force an 8 s data window (N = 2^14 at srate=2048 => dense TD path) strictly inside
# the 32 s frame [999999998.335, 1000000030.335]; the auto-chosen segment would be
# ~14 s and overrun an 8 s frame, hence the longer frames + forced window.
common=( -c "$HERE/frames32.cache"
  -C H1=FAKE-STRAIN -C L1=FAKE-STRAIN
  -p H1="$HERE/H1-psd.xml.gz" -p L1="$HERE/L1-psd.xml.gz"
  -I "$HERE/inj_bbh35.xml.gz" --event 0
  --event-time "$EVTIME"
  --data-start-time 999999999 --data-end-time 1000000007
  --srate 2048
  --fmin-template 25 --fmax 1000 --reference-freq 25
  --l-max 2 --approximant IMRPhenomD
  --time-marginalization --vectorized --force-xpy
  --internal-use-lnL --sampler-method GMM
  --d-min 10 --d-max 3000
  --n-max "$NMAX" --n-eff "$NEFF" --n-chunk 10000
  --seed "$SEED"
  --save-samples )

echo "======== FD-DIAGONAL (inv-spec-trunc 0) ========"
cd "$HERE"
$PY "$ILE" "${common[@]}" \
   --internal-ile-inner-product fd-diagonal --inv-spec-trunc-time 0 \
   -o "$HERE/out_fd" > "$HERE/ile_fd.log" 2>&1 && echo "FD ok" || { echo "FD FAILED"; tail -30 "$HERE/ile_fd.log"; exit 2; }

echo "======== TD-TOEPLITZ ========"
$PY "$ILE" "${common[@]}" \
   --internal-ile-inner-product td-toeplitz --td-solver auto \
   -o "$HERE/out_td" > "$HERE/ile_td.log" 2>&1 && echo "TD ok" || { echo "TD FAILED"; tail -30 "$HERE/ile_td.log"; exit 3; }

echo "======== RESULTS ========"
echo "FD .dat:"; cat "$HERE"/out_fd_0_.dat 2>/dev/null || echo "(none)"
echo "TD .dat:"; cat "$HERE"/out_td_0_.dat 2>/dev/null || echo "(none)"
