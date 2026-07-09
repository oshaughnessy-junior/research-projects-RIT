#!/bin/bash
# build_basis.sh -- per-class glitch corpus -> PCA -> strain-colored basis (+ population prior).
# Generic across events: parameters from ./event.env.  Products land in ./work/ (gitignored)
# EXCEPT the small basis npzs, which the Makefile copies into ./basis/ for committing (<1 MB).
#
# Pipeline (paper-1 tooling reused from ../../td_glitch_poc):
#   fetch_class.py -> <class>_corpus.npz   (or REUSE an existing corpus/PCA via event.env)
#   build_pca.py   -> blip_pca_white.npz   (uncentered SVD, r>=90% energy)
#   color_basis.py -> glitch_basis_<IFO>.npz        (whitened PCA colored to the REAL event PSD)
#   fit_population.py --write-basis -> glitch_basis_<IFO>_pop_<mode>.npz  (mu/sigma_shape)
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/event.env"
COMMON="$HERE/../common"
CODE="$(cd "$HERE/../../.." && pwd)"
POC="$CODE/test/td_glitch_poc"
export PYTHONPATH="$CODE:$PYTHONPATH"
PY="${PY:-$HOME/.conda/envs/junior_rift/bin/python}"
export PATH="$(dirname "$PY"):$CODE/bin:$PATH"   # so #!/usr/bin/env python shebangs resolve
WORK="$HERE/work"; mkdir -p "$WORK"
PSD_ASCII="$HERE/psd/${GLITCH_IFO}-psd-ascii.txt"
[ -s "$PSD_ASCII" ] || { echo "ERROR: run fetch_data.sh first (missing $PSD_ASCII)"; exit 1; }

REUSE_CORPUS=$(eval echo "${CORPUS_REUSE_CORPUS:-}")
REUSE_PCA=$(eval echo "${CORPUS_REUSE_PCA:-}")

if [ -n "$REUSE_PCA" ] && [ -s "$REUSE_PCA" ]; then
  CORPUS="$REUSE_CORPUS"; PCA="$REUSE_PCA"
  echo "==== [$TAG] REUSING corpus=$CORPUS  pca=$PCA ===="
else
  echo "==== [$TAG] fetch $GLITCH_CLASS corpus for $GLITCH_IFO ===="
  $PY "$COMMON/fetch_class.py" --catalog "$CORPUS_CATALOG" --ifo "$GLITCH_IFO" \
      --ml-label "$GLITCH_CLASS" --outdir "$WORK" --fs "$SRATE" \
      --window-half "$CORPUS_WINDOW_HALF" --f-lo "$GLITCH_FLO" --f-hi "$GLITCH_FHI" \
      --snr-lo "$CORPUS_SNR_LO" --snr-hi "$CORPUS_SNR_HI" \
      --peakf-lo "$CORPUS_PEAKF_LO" --peakf-hi "$CORPUS_PEAKF_HI" \
      --duration-max "$CORPUS_DURATION_MAX" --n-target "$CORPUS_NTARGET" \
      --min-per-window "$CORPUS_MINPERWIN"
  CORPUS="$WORK/$(echo "$GLITCH_CLASS" | tr 'A-Z' 'a-z')_corpus.npz"
  echo "==== [$TAG] PPCA basis (n_window=$GLITCH_PCA_NWIN) ===="
  $PY "$POC/build_pca.py" --corpus "$CORPUS" --outdir "$WORK" --n-window "$GLITCH_PCA_NWIN"
  PCA="$WORK/blip_pca_white.npz"     # build_pca.py fixed output name (class-agnostic content)
fi

echo "==== [$TAG] color whitened PCA -> strain basis at the REAL $GLITCH_IFO PSD ===="
FLAT="$WORK/glitch_basis_${GLITCH_IFO}.npz"
$PY "$COMMON/color_basis.py" --pca "$PCA" --psd "$PSD_ASCII" \
    --event-gps "$GPS" --glitch-center-gps "$GLITCH_CENTER_GPS" --ifo "$GLITCH_IFO" \
    --n-out "$GLITCH_NOUT" --f-lo "$GLITCH_FLO" --f-hi "$GLITCH_FHI" --out "$FLAT"

echo "==== [$TAG] fit population prior -> augmented basis (mu/sigma_shape) ===="
$PY "$POC/fit_population.py" --corpus "$CORPUS" --pca "$PCA" --outdir "$WORK" \
    --write-basis "$FLAT"
POP="$(ls -t "$WORK"/glitch_basis_${GLITCH_IFO}_pop_*.npz 2>/dev/null | head -1)"
if [ -n "$POP" ]; then
  cp "$POP" "$WORK/glitch_basis_${GLITCH_IFO}_pop.npz"
  echo "==== [$TAG] population basis: $POP -> glitch_basis_${GLITCH_IFO}_pop.npz ===="
fi

# stage the small (<1 MB) basis npzs for committing
mkdir -p "$HERE/basis"
cp "$FLAT" "$HERE/basis/" 2>/dev/null || true
cp "$WORK/glitch_basis_${GLITCH_IFO}_pop.npz" "$HERE/basis/" 2>/dev/null || true
echo "==== [$TAG] build_basis DONE.  flat=$FLAT  pop=$WORK/glitch_basis_${GLITCH_IFO}_pop.npz ===="
ls -la "$HERE/basis/"
