#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Demo: drive create_eos_posterior_pipeline under each workflow backend.
# -----------------------------------------------------------------------------
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
INPUTS="${HERE}/inputs"
OUTPUTS_BASE="${HERE}/outputs"

CEPOS_BIN="${CEPOS_BIN:-create_eos_posterior_pipeline}"
if ! command -v "${CEPOS_BIN}" >/dev/null 2>&1; then
    CEPOS_BIN="${HERE}/../../bin/create_eos_posterior_pipeline"
fi
if [[ ! -e "${CEPOS_BIN}" ]]; then
    echo "ERROR: cannot locate create_eos_posterior_pipeline." >&2
    echo "Set CEPOS_BIN=/path/to/create_eos_posterior_pipeline." >&2
    exit 2
fi

run_for_backend() {
    local backend="$1"
    local outdir="${OUTPUTS_BASE}/${backend}/eos_posterior"
    rm -rf "${outdir}"
    mkdir -p "${outdir}"

    cp "${INPUTS}/overlap-grid.xml.gz"   "${outdir}/eos-grid.xml.gz"
    cp "${INPUTS}/args_marg_event.txt"   "${outdir}/args_marg_event.txt"
    cp "${INPUTS}/args_eos_post.txt"     "${outdir}/args_eos_post.txt"
    cp "${INPUTS}/args_test.txt"         "${outdir}/args_test.txt"
    cp "${INPUTS}/event-1.net"           "${outdir}/event-1.net"
    cp "${INPUTS}/event-2.net"           "${outdir}/event-2.net"

    echo
    echo "============================================================"
    echo "Backend: ${backend}"
    echo "Output:  ${outdir}"
    echo "============================================================"

    (
        cd "${outdir}"
        RIFT_DAG_BACKEND="${backend}" \
        python3 "${CEPOS_BIN}" \
            --working-directory   "${outdir}" \
            --input-grid          "${outdir}/eos-grid.xml.gz" \
            --event-file          "${outdir}/event-1.net" \
            --event-file          "${outdir}/event-2.net" \
            --marg-event-args     "${outdir}/args_marg_event.txt" \
            --eos-post-args       "${outdir}/args_eos_post.txt" \
            --test-args           "${outdir}/args_test.txt" \
            --n-iterations 2 \
            --request-memory-marg 8192 \
            --general-request-disk 10M \
            --condor-nogrid-nonworker \
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
    echo "  ${OUTPUTS_BASE}/${backend}/eos_posterior"
done
