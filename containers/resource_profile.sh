#!/usr/bin/env bash
# Build a RIFT resource profile: survey a pool, probe its classes, collect the receipts.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODE="${HERE}/../MonteCarloMarginalizeCode/Code"
PYTHON="${PYTHON:-python3}"

usage() {
    cat <<EOF
Usage:
  containers/resource_profile.sh survey PROFILE_DIR [--manifest FILE] [--pool HOST]... [--keep-slots]
  containers/resource_profile.sh harvest PROFILE_DIR RUNDIR --output PROFILE_DIR/observations/NAME.yaml
  containers/resource_profile.sh emit-probes PROFILE_DIR --case DIR --arguments FILE --label L --out DIR
  containers/resource_profile.sh collect-probes PROFILE_DIR --probes DIR --output PROFILE_DIR/observations/NAME.yaml
  containers/resource_profile.sh plan PROFILE_DIR --workload FILE

Each command passes its arguments to bin/util_RIFT_resource_profile.py.
See containers/README.md section 5.
EOF
}

cmd="${1:-}"
case "${cmd}" in
    ""|-h|--help)
        usage
        exit 0
        ;;
    survey|harvest|emit-probes|collect-probes|plan)
        export PYTHONPATH="${CODE}${PYTHONPATH:+:${PYTHONPATH}}"
        exec "${PYTHON}" "${CODE}/bin/util_RIFT_resource_profile.py" "$@"
        ;;
    *)
        echo "Unknown resource_profile command: ${cmd}" >&2
        usage >&2
        exit 2
        ;;
esac
