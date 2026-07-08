#!/bin/bash
# run_three_way.sh -- one batched-ILE grid evaluation for one PoC configuration (step 4).
# Production conventions per NEXT.md T7c: window-shape 0.1 (= helper 0.4 s/end Tukey for
# 8 s), AV extrinsic integrator, analytic distance+phase marginalization (util_InitMargTable
# lookup), NoLoop --gpu --vectorized --force-xpy, td-toeplitz inner product.
#
# Usage: run_three_way.sh <label> <workdir> <cache> <outdir> [extra ILE flags...]
#   <workdir> must contain {H1,L1}-psd.xml.gz and grid.xml.gz (see README).
# Env overrides: NMAX (200000), NEFF (40), NPTS (35), SEED (1234), EVENT_TIME, START, END.
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
CODE="$(cd "$HERE/../.." && pwd)"
export PYTHONPATH="$CODE:$PYTHONPATH"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-3}"
PY="${PY:-$HOME/.conda/envs/junior_rift/bin/python}"
ILE="$CODE/bin/integrate_likelihood_extrinsic_batchmode"
LABEL="$1"; WORK="$2"; CACHE="$3"; OUT="$4"; shift 4; EXTRA=("$@")
NMAX="${NMAX:-200000}"; NEFF="${NEFF:-40}"; NPTS="${NPTS:-35}"; SEED="${SEED:-1234}"
EVENT_TIME="${EVENT_TIME:-1000000000.0}"
START="${START:-999999996}"; END="${END:-1000000004}"
DMIN=50; DMAX=3000
LOOK="$WORK/distmarg_lookup_d${DMIN}_${DMAX}.npz"
mkdir -p "$OUT"
if [ ! -s "$LOOK" ]; then
  echo "  [build] distance+phase marg lookup -> $LOOK"
  $PY "$CODE/bin/util_InitMargTable" --d-min "$DMIN" --d-max "$DMAX" --max-snr 200 \
      --phase-marginalization --d-prior Euclidean --out "$LOOK" > "$OUT/marg_build.log" 2>&1
fi
echo "==== G4-PoC config=$LABEL cache=$CACHE extra='${EXTRA[*]}' nmax=$NMAX neff=$NEFF ===="
START_S=$(date +%s)
$PY "$ILE" \
  -c "$CACHE" -C H1=FAKE-STRAIN -C L1=FAKE-STRAIN \
  -p H1="$WORK/H1-psd.xml.gz" -p L1="$WORK/L1-psd.xml.gz" \
  -I "$WORK/grid.xml.gz" --event 0 --n-events-to-analyze "$NPTS" --event-time "$EVENT_TIME" \
  --data-start-time "$START" --data-end-time "$END" \
  --srate 2048 --fmin-template 25 --fmax 1000 --reference-freq 25 \
  --l-max 2 --approximant IMRPhenomD \
  --window-shape 0.1 \
  --time-marginalization --vectorized --gpu --force-xpy \
  --internal-use-lnL --sampler-method AV \
  --d-min "$DMIN" --d-max "$DMAX" \
  --distance-marginalization --distance-marginalization-lookup-table "$LOOK" \
  --n-max "$NMAX" --n-eff "$NEFF" --n-chunk 10000 --seed "$SEED" \
  --internal-ile-inner-product td-toeplitz --td-solver auto \
  "${EXTRA[@]}" \
  -o "$OUT/pt" > "$OUT/ile.log" 2>&1 && st=ok || st=FAIL
END_S=$(date +%s)
echo "  status=$st elapsed=$((END_S-START_S))s"
echo "$((END_S-START_S))" > "$OUT/elapsed_s.txt"
$PY - "$OUT" > "$OUT/collated.txt" <<'PYEOF'
import glob, os, sys
import numpy as np
out = sys.argv[1]
print("# idx m1 m2 mchirp eta lnL sigma neff")
import RIFT.lalsimutils as l
for f in sorted(glob.glob(os.path.join(out, "pt_*_.dat")), key=lambda p: int(p.split("_")[-2])):
    idx = int(f.split("_")[-2])
    if os.path.getsize(f) == 0: continue
    d = np.loadtxt(f)
    mc = l.mchirp(d[1]*l.lsu_MSUN,d[2]*l.lsu_MSUN)/l.lsu_MSUN
    eta = l.symRatio(d[1]*l.lsu_MSUN,d[2]*l.lsu_MSUN)
    print("%3d %9.4f %9.4f %9.4f %8.5f %11.5f %9.5f %9.3f"%(idx,d[1],d[2],mc,eta,d[9],d[10],d[12]))
PYEOF
echo "  collated -> $OUT/collated.txt"
grep -E "\[G3\]|rel_var|guess_snr" "$OUT/ile.log" | sort -u | head -20 || true
