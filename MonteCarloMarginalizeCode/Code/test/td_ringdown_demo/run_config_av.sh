#!/bin/bash
# run_config_av.sh -- one batched-ILE evaluation of the intrinsic grid for a single TD-segment
# configuration, using the ADAPTIVE-VOLUME (AV) extrinsic integrator instead of GMM.
# Usage: run_config_av.sh <label> <cache> <grid.xml.gz> <npts> <outdir> <seed> [extra td flags...]
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
CODE="$(cd "$HERE/../.." && pwd)"
export PYTHONPATH="$CODE:$PYTHONPATH"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-2}"
PY=~/.conda/envs/junior_rift/bin/python
ILE="$CODE/bin/integrate_likelihood_extrinsic_batchmode"
LABEL="$1"; CACHE="$2"; GRID="$3"; NPTS="$4"; OUT="$5"; SEED="$6"; shift 6
EXTRA=("$@")
NMAX="${NMAX:-400000}"; NEFF="${NEFF:-1000}"
mkdir -p "$OUT"; cd "$HERE"
echo "==== AV config=$LABEL grid=$GRID npts=$NPTS seed=$SEED extra='${EXTRA[*]}' ===="
START=$(date +%s)
$PY "$ILE" \
  -c "$CACHE" -C H1=FAKE-STRAIN -C L1=FAKE-STRAIN \
  -p H1="$HERE/H1-psd.xml.gz" -p L1="$HERE/L1-psd.xml.gz" \
  -I "$HERE/$GRID" --event 0 --n-events-to-analyze "$NPTS" --event-time 1000000000.0 \
  --data-start-time 999999996 --data-end-time 1000000004 \
  --srate 2048 --fmin-template 25 --fmax 1000 --reference-freq 25 \
  --l-max 2 --approximant IMRPhenomD \
  --time-marginalization --vectorized --gpu --force-xpy \
  --internal-use-lnL --sampler-method AV \
  --d-min 10 --d-max 3000 \
  --n-max "$NMAX" --n-eff "$NEFF" --n-chunk 10000 --seed "$SEED" \
  --internal-ile-inner-product td-toeplitz --td-solver auto \
  "${EXTRA[@]}" \
  -o "$OUT/pt" > "$OUT/ile.log" 2>&1 && st=ok || st=FAIL
END=$(date +%s)
echo "  status=$st elapsed=$((END-START))s"
$PY - "$OUT" > "$OUT/collated.txt" <<'PYEOF'
import glob, os, sys
import numpy as np
out = sys.argv[1]
rows = []
for f in sorted(glob.glob(os.path.join(out, "pt_*_.dat")), key=lambda p: int(p.split("_")[-2])):
    idx = int(f.split("_")[-2])
    if os.path.getsize(f) == 0:
        continue
    d = np.loadtxt(f)
    rows.append((idx, d[1], d[2], d[9], d[10], d[11], d[12]))
print("# idx m1 m2 lnL sigma ntot neff")
for r in rows:
    print("%3d %10.5f %10.5f %12.5f %10.5f %10d %10.3f" % (r[0], r[1], r[2], r[3], r[4], int(r[5]), r[6]))
PYEOF
echo "  collated -> $OUT/collated.txt ($(grep -vc '^#' "$OUT/collated.txt") pts)"
grep -iE 'neff|n_eff' "$OUT/collated.txt" | head
