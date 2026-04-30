#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Demo: drive create_event_parameter_pipeline_BasicIteration under each
# workflow backend (htcondor, glue, slurm).
#
# Each invocation emits a complete DAG (or sbatch driver script) plus all
# accompanying submit / sbatch files into a per-backend output directory.
#
# This script does NOT execute the resulting workflow; it only verifies that
# the same pipeline driver script produces the appropriate set of artefacts
# under each backend.
#
# Run:
#     bash demo_create_event_parameter_pipeline_BasicIteration.sh
# -----------------------------------------------------------------------------
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
INPUTS="${HERE}/inputs"
OUTPUTS_BASE="${HERE}/outputs"

# Path to the pipeline driver (defaults to PATH-discovered, override with
# CEPP_BIN if you have a non-standard install).
CEPP_BIN="${CEPP_BIN:-create_event_parameter_pipeline_BasicIteration}"
if ! command -v "${CEPP_BIN}" >/dev/null 2>&1; then
    # fall back to the in-tree copy
    CEPP_BIN="${HERE}/../../bin/create_event_parameter_pipeline_BasicIteration"
fi

if [[ ! -e "${CEPP_BIN}" ]]; then
    echo "ERROR: cannot locate create_event_parameter_pipeline_BasicIteration." >&2
    echo "Set CEPP_BIN=/path/to/create_event_parameter_pipeline_BasicIteration." >&2
    exit 2
fi

run_for_backend() {
    local backend="$1"
    local outdir="${OUTPUTS_BASE}/${backend}/cepp_basic_iteration"
    rm -rf "${outdir}"
    mkdir -p "${outdir}"

    # Stage the input grid / args files into the per-backend working dir.
    cp "${INPUTS}/overlap-grid.xml.gz"  "${outdir}/overlap-grid.xml.gz"
    cp "${INPUTS}/args_ile.txt"         "${outdir}/args_ile.txt"
    cp "${INPUTS}/args_cip.txt"         "${outdir}/args_cip.txt"
    cp "${INPUTS}/args_test.txt"        "${outdir}/args_test.txt"

    echo
    echo "============================================================"
    echo "Backend: ${backend}"
    echo "Output:  ${outdir}"
    echo "============================================================"

    (
        cd "${outdir}"
        RIFT_DAG_BACKEND="${backend}" \
        python3 "${CEPP_BIN}" \
            --working-directory   "${outdir}" \
            --input-grid          "${outdir}/overlap-grid.xml.gz" \
            --ile-args            "${outdir}/args_ile.txt" \
            --cip-args            "${outdir}/args_cip.txt" \
            --test-args           "${outdir}/args_test.txt" \
            --n-iterations 2 \
            --n-events-to-analyze 1 \
            --request-memory-ILE  4096 \
            --request-memory-CIP  4096 \
            --request-disk        "10M" \
            --general-retries 1 \
            --condor-nogrid-nonworker \
            --skip-reproducibility \
            2>&1 | tee "${outdir}/driver.log"
    ) || {
        echo "[${backend}] driver returned non-zero; partial outputs may still be present." >&2
    }

    echo
    echo "Artefacts produced for ${backend}:"
    find "${outdir}" -maxdepth 3 -type f \
        \( -name '*.sub' -o -name '*.sbatch' -o -name '*.dag' -o -name '*_dag.sh' \) \
        | sort | sed "s|^${outdir}/|  |"
}

mkdir -p "${OUTPUTS_BASE}"
for backend in htcondor glue slurm; do
    run_for_backend "${backend}"
done

echo
echo "Done.  Output trees:"
for backend in htcondor glue slurm; do
    echo "  ${OUTPUTS_BASE}/${backend}/cepp_basic_iteration"
done
