#!/bin/bash
# gw250114_av.sh -- properly extrinsic-MARGINALIZED (AV) three-config surface on REAL GW250114
# H1+L1 O4b data, replacing the fixed-extrinsic deterministic surface.  One config per call.
# Usage: gw250114_av.sh <label> <outdir> [extra td flags...]
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
CODE="$(cd "$HERE/../.." && pwd)"
export PYTHONPATH="$CODE:$PYTHONPATH"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-3}"
PY=~/.conda/envs/junior_rift/bin/python
ILE="$CODE/bin/integrate_likelihood_extrinsic_batchmode"
T7C=/home/richard.oshaughnessy/rift_td/analyses/t7c_gw250114
LABEL="$1"; OUT="$2"; shift 2; EXTRA=("$@")
NMAX="${NMAX:-80000}"; NEFF="${NEFF:-5000}"
mkdir -p "$OUT"
echo "==== GW250114 AV config=$LABEL extra='${EXTRA[*]}' ===="
START=$(date +%s)
$PY "$ILE" \
  -c "$T7C/data/frames.cache" -C H1=GWOSC-STRAIN -C L1=GWOSC-STRAIN \
  -p H1="$T7C/psd/H1-psd.xml.gz" -p L1="$T7C/psd/L1-psd.xml.gz" \
  -I "$T7C/grid/grid.xml.gz" --event 0 --n-events-to-analyze 35 --event-time 1420878141.2 \
  --data-start-time 1420878137.2 --data-end-time 1420878145.2 \
  --srate 2048 --fmin-template 25 --fmax 1000 --reference-freq 25 \
  --l-max 2 --approximant IMRPhenomD \
  --time-marginalization --vectorized --gpu --force-xpy \
  --internal-use-lnL --sampler-method AV \
  --d-min 50 --d-max 3000 \
  --n-max "$NMAX" --n-eff "$NEFF" --n-chunk 10000 --seed 1234 \
  --internal-ile-inner-product td-toeplitz --td-solver auto \
  "${EXTRA[@]}" \
  -o "$OUT/pt" > "$OUT/ile.log" 2>&1 && st=ok || st=FAIL
END=$(date +%s)
echo "  status=$st elapsed=$((END-START))s"
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
