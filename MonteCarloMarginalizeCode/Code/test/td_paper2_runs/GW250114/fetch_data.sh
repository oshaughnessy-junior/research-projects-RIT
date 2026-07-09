#!/bin/bash
# fetch_data.sh -- open strain + off-source PSDs + analysis frames + coinc for this event.
# Generic across events: all parameters come from ./event.env.  Products (data/, psd/, coinc.xml)
# are regenerable and gitignored.  See ../common/fetch_event_data.py for the recipe.
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/event.env"
COMMON="$HERE/../common"
CODE="$(cd "$HERE/../../.." && pwd)"                 # MonteCarloMarginalizeCode/Code
export PYTHONPATH="$CODE:$PYTHONPATH"
export PATH="$CODE/bin:$PATH"
PY="${PY:-$HOME/.conda/envs/junior_rift/bin/python}"
export PATH="$(dirname "$PY"):$CODE/bin:$PATH"   # so #!/usr/bin/env python shebangs resolve

echo "==== [$TAG] fetch data + PSDs (gps=$GPS ifos=$IFOS srate=$SRATE) ===="
$PY "$COMMON/fetch_event_data.py" --tag "$TAG" --gps "$GPS" --ifos "$IFOS" \
    --srate "$SRATE" --seglen "$FRAME_SEGLEN" --ile-seglen "$ILE_SEGLEN" --outdir "$HERE"

echo "==== [$TAG] ascii PSD -> per-IFO xml -> combined multi-IFO xml ===="
cd "$HERE/psd"
ASSIGN=""
for ifo in $(echo "$IFOS" | tr ',' ' '); do
  convert_psd_ascii2xml --fname-psd-ascii "${ifo}-psd-ascii.txt" --ifo "$ifo" --conventional-postfix
  ASSIGN="$ASSIGN ${ifo}=$HERE/psd/${ifo}-psd.xml.gz"
done
$PY "$COMMON/merge_psd_xml.py" --out "$HERE/psd/combined-psd.xml.gz" $ASSIGN

echo "==== [$TAG] synthesize coinc.xml (fiducial ${M1_DET}+${M2_DET} Msun, snr $NET_SNR) ===="
cd "$HERE"
$PY "$COMMON/make_coinc.py" --m1 "$M1_DET" --m2 "$M2_DET" --tref "$GPS" --ifos "$IFOS" \
    --snr "$NET_SNR" --fmin "$FMIN" --fref "$FREF" --approx "$APPROX" --outdir "$HERE"

echo "==== [$TAG] fetch_data DONE.  cache=$HERE/data/frames.cache  psd=$HERE/psd/combined-psd.xml.gz ===="
