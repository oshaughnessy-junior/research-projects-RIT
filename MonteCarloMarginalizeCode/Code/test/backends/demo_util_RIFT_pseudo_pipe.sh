#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Demo: drive util_RIFT_pseudo_pipe.py under each workflow backend.
#
# pseudo_pipe is the high-level driver: it does data discovery (PSDs, frame
# files, channel selection, ...), then invokes a `create_event_parameter_*`
# pipeline.  The full happy-path requires either a GraceDb event ID or a
# fully-populated INI file.
#
# This demo uses the minimal "manual event-time + manual initial grid" entry
# path so that the pipeline can be exercised without contacting GraceDb.
# Replace the --gracedb-id / --use-ini variants with your preferred entry
# point on a real LIGO submission node.
# -----------------------------------------------------------------------------
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
INPUTS="${HERE}/inputs"
OUTPUTS_BASE="${HERE}/outputs"

PIPE_BIN="${PIPE_BIN:-util_RIFT_pseudo_pipe.py}"
if ! command -v "${PIPE_BIN}" >/dev/null 2>&1; then
    PIPE_BIN="${HERE}/../../bin/util_RIFT_pseudo_pipe.py"
fi
if [[ ! -e "${PIPE_BIN}" ]]; then
    echo "ERROR: cannot locate util_RIFT_pseudo_pipe.py." >&2
    echo "Set PIPE_BIN=/path/to/util_RIFT_pseudo_pipe.py." >&2
    exit 2
fi

# pseudo_pipe wants a manual initial grid plus a coinc + cache file when
# bypassing GraceDb.  We supply trivial placeholders here -- the goal is to
# exercise the DAG-emission stage, not to actually run the pipeline.
PSEUDO_COINC="${INPUTS}/event-1.net"      # pseudo_pipe just needs a path that exists
PSEUDO_GRID="${INPUTS}/overlap-grid.xml.gz"
PSEUDO_CACHE="${OUTPUTS_BASE}/empty.cache"
mkdir -p "${OUTPUTS_BASE}"
: > "${PSEUDO_CACHE}"

run_for_backend() {
    local backend="$1"
    local outdir="${OUTPUTS_BASE}/${backend}/pseudo_pipe"
    rm -rf "${outdir}"
    mkdir -p "${outdir}"

    echo
    echo "============================================================"
    echo "Backend: ${backend}"
    echo "Output:  ${outdir}"
    echo "============================================================"

    # NOTE: most pseudo_pipe options below are required by argparse; they do
    # not all influence the DAG-emission stage.  Adjust as needed for your
    # production runs.
    (
        cd "${outdir}"
        RIFT_DAG_BACKEND="${backend}" \
        python3 "${PIPE_BIN}" \
            --use-rundir         "${outdir}" \
            --event-time         1187008882.4 \
            --approx             IMRPhenomD \
            --calibration        C00 \
            --l-max              2 \
            --assume-nospin \
            --manual-initial-grid "${PSEUDO_GRID}" \
            --use-coinc          "${PSEUDO_COINC}" \
            --fake-data-cache    "${PSEUDO_CACHE}" \
            --manual-ifo-list    H1,L1 \
            --skip-reproducibility \
            --condor-nogrid-nonworker \
            2>&1 | tee "${outdir}/driver.log"
    ) || {
        echo "[${backend}] driver returned non-zero; partial outputs may still be present." >&2
    }

    echo
    echo "Artefacts produced for ${backend}:"
    find "${outdir}" -maxdepth 4 -type f \
        \( -name '*.sub' -o -name '*.sbatch' -o -name '*.dag' -o -name '*_dag.sh' \) \
        | sort | sed "s|^${outdir}/|  |"
}

for backend in htcondor glue slurm; do
    run_for_backend "${backend}"
done

echo
echo "Done.  Output trees:"
for backend in htcondor glue slurm; do
    echo "  ${OUTPUTS_BASE}/${backend}/pseudo_pipe"
done
