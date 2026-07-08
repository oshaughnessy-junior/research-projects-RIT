#!/bin/bash
# run_all_gw250114_av.sh -- the three-way (full / ringdown / inspiral) AV-marginalized
# GW250114 surface, WITH the diagnosed fixes applied by gw250114_av.sh:
#   (1) --window-shape 0.1  (the PRODUCTION pseudo_pipe/helper convention,
#       window_shape = data_tukey_window_time*2/T_window = 0.4*2/8; removes the sub-20Hz
#       spectral-leakage inflation, the root cause of the d->d_min runaway -- see
#       diag_window_sweep.py / t7c REPORT.md.  NOT the earlier ad-hoc 0.4.), and
#   (2) analytic distance+phase marginalization (lookup table) so the loud-event MC converges.
# Ringdown = template kept from peak+10M (0.0035435 s, Mtot_det=71.9); inspiral = up to peak-20ms.
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
OUTROOT="${OUTROOT:-/home/richard.oshaughnessy/rift_td/analyses/t7c_gw250114/results/av_fixed}"
mkdir -p "$OUTROOT"
export NMAX="${NMAX:-200000}" NEFF="${NEFF:-40}"
echo "#### three-way AV (windowed + dist/phase marg)  NMAX=$NMAX NEFF=$NEFF  -> $OUTROOT"
bash "$HERE/gw250114_av.sh" full     "$OUTROOT/full"
bash "$HERE/gw250114_av.sh" ringdown "$OUTROOT/ringdown" --td-analysis-start 0.0035435
bash "$HERE/gw250114_av.sh" inspiral "$OUTROOT/inspiral" --td-analysis-end -0.02
echo "#### done.  neff summaries:"
for c in full ringdown inspiral; do
  echo "== $c =="; cat "$OUTROOT/$c/collated.txt"
done
