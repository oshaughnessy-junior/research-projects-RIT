#!/bin/bash
# run_ile_mc_noloop.sh -- Part-0 MC-layer gate for the TD segment work (plan T4 caveat close).
#
# The T4 study established fd-vs-td parity DETERMINISTICALLY (Precompute + factored lnL) but
# could not produce a real MC lnL_marg because the plain CPU vectorized likelihood
# (DiscreteFactoredLogLikelihoodViaArrayVector) calls the numba `lalylm`, whose @vectorize of
# lal.SpinWeightedSphericalHarmonic fails to compile in junior_rift-class envs.
#
# The ROBUST PRODUCTION likelihood DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop is
# reached with `--gpu --vectorized --force-xpy`.  Without a GPU, xpy falls back to numpy
# (expected + fine) and this path uses the pure-numpy SphericalHarmonicsVectorized -- NOT the
# broken numba lalylm -- so the real MC lnL_marg is produced on both fd and td.
#
# This runs ILE twice on the SAME synthetic data / intrinsic point / --seed, once fd-diagonal
# (inv-spec-trunc 0) and once td-toeplitz, and compares the reported lnL_marg.  It regenerates
# the frame cache locally so it is worktree-portable.
#
# Usage: ./run_ile_mc_noloop.sh <NMAX> <NEFF> [OUTDIR]
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
CODE="$(cd "$HERE/../.." && pwd)"                 # test/td_equivalence -> test -> Code
export PYTHONPATH="$CODE:$PYTHONPATH"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-4}"
PY=~/.conda/envs/junior_rift/bin/python
ILE="$CODE/bin/integrate_likelihood_extrinsic_batchmode"
NMAX="${1:-40000}"; NEFF="${2:-3000}"; OUT="${3:-$HERE/out_noloop}"
mkdir -p "$OUT"
SEED=1234; EVTIME=1000000000.0

# Regenerate the 32 s frame cache with THIS worktree's absolute paths (the committed cache
# may carry a stale path from the worktree it was authored in).
CACHE="$OUT/frames32_local.cache"
printf 'H FAKE 999999998 32 file://localhost%s/frames32/H-fake_strain-999999998-32.gwf\n' "$HERE"  > "$CACHE"
printf 'L FAKE 999999998 32 file://localhost%s/frames32/L-fake_strain-999999998-32.gwf\n' "$HERE" >> "$CACHE"

common=( -c "$CACHE"
  -C H1=FAKE-STRAIN -C L1=FAKE-STRAIN
  -p H1="$HERE/H1-psd.xml.gz" -p L1="$HERE/L1-psd.xml.gz"
  -I "$HERE/inj_bbh35.xml.gz" --event 0 --event-time "$EVTIME"
  --data-start-time 999999999 --data-end-time 1000000007
  --srate 2048 --fmin-template 25 --fmax 1000 --reference-freq 25
  --l-max 2 --approximant IMRPhenomD
  --time-marginalization --vectorized --gpu --force-xpy     # <- reaches the NoLoop likelihood
  --internal-use-lnL --sampler-method GMM
  --d-min 10 --d-max 3000
  --n-max "$NMAX" --n-eff "$NEFF" --n-chunk 10000 --seed "$SEED" --save-samples )

cd "$HERE"
echo "======== FD-DIAGONAL (inv-spec-trunc 0), NoLoop path ========"
$PY "$ILE" "${common[@]}" --internal-ile-inner-product fd-diagonal --inv-spec-trunc-time 0 \
   -o "$OUT/out_fd" > "$OUT/ile_fd.log" 2>&1 && echo "FD ok" || { echo "FD FAILED"; tail -40 "$OUT/ile_fd.log"; exit 2; }
echo "======== TD-TOEPLITZ, NoLoop path ========"
$PY "$ILE" "${common[@]}" --internal-ile-inner-product td-toeplitz --td-solver auto \
   -o "$OUT/out_td" > "$OUT/ile_td.log" 2>&1 && echo "TD ok" || { echo "TD FAILED"; tail -40 "$OUT/ile_td.log"; exit 3; }
echo "======== RESULTS (lnL_marg = column 10 of the .dat) ========"
echo "FD:"; cat "$OUT"/out_fd*.dat 2>/dev/null
echo "TD:"; cat "$OUT"/out_td*.dat 2>/dev/null
