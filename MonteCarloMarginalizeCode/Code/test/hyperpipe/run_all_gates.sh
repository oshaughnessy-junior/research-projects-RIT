#!/bin/bash
# The gate sweep for the Hyperpipe pipeline-builder work (RIFT PR #181/#182).
#
# This existed as shell history for most of the PR's development, which meant
# "all sixteen gates pass" was a claim only one shell could reproduce.  It is a
# script so anyone can re-run it and so the pass criteria are written down.
#
# The criteria are OUTCOMES, not exit codes: a pytest run that collects zero
# tests exits 5, and a run where every test skips exits 0.  Each pytest lane
# therefore emits JUnit XML and is scored on tests/failures/errors/skipped.
#
#   ./run_all_gates.sh [OUTDIR]        # default: a fresh mktemp -d
#
# Exits nonzero if any lane fails.  Prints one line per lane and a verdict
# table at the end.
set -u

CODE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${1:-$(mktemp -d -t rift-gates-XXXXXX)}"
mkdir -p "$OUT"
PY="${RIFT_PYTHON:-/cvmfs/software.igwn.org/conda/envs/igwn/bin/python}"
if [ ! -x "$PY" ]; then
  echo "CVMFS IGWN environment unavailable; set RIFT_PYTHON" >&2; exit 75
fi

export PYTHONPATH="$CODE${PYTHONPATH:+:$PYTHONPATH}"
export PATH="$CODE/bin:$(dirname "$PY"):$PATH"
export OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
export GW_SURROGATE=
export XDG_CACHE_HOME="$OUT/xdg" MPLCONFIGDIR="$OUT/mpl"
mkdir -p "$XDG_CACHE_HOME" "$MPLCONFIGDIR"

VERDICTS="$OUT/verdicts.txt"; : > "$VERDICTS"
FAILED=0

# Score a JUnit file the way a count-blind exit code cannot: a lane that
# collected nothing, or skipped everything it collected, is not a pass.
score_junit() {  # $1=name $2=rc $3=junit
  local name=$1 rc=$2 xml=$3
  local line
  line=$("$PY" - "$xml" <<'PYEOF'
import sys, xml.etree.ElementTree as ET
try:
    root = ET.parse(sys.argv[1]).getroot()
except Exception as exc:
    print("PARSE-ERROR {}".format(exc)); raise SystemExit(0)
suites = [root] if root.tag == "testsuite" else list(root)
tot = {k: sum(int(s.get(k, 0)) for s in suites)
       for k in ("tests", "failures", "errors", "skipped")}
print("tests={tests} failures={failures} errors={errors} "
      "skipped={skipped}".format(**tot))
PYEOF
)
  local verdict=PASS
  case "$line" in
    PARSE-ERROR*) verdict=FAIL ;;
    *) local t f e s
       t=${line#tests=}; t=${t%% *}
       f=${line#*failures=}; f=${f%% *}
       e=${line#*errors=}; e=${e%% *}
       s=${line#*skipped=}; s=${s%% *}
       [ "$rc" -ne 0 ] && verdict=FAIL
       [ "$t" -eq 0 ] && verdict=FAIL          # collected nothing
       [ "$f" -ne 0 ] && verdict=FAIL
       [ "$e" -ne 0 ] && verdict=FAIL
       [ "$t" -eq "$s" ] && verdict=FAIL       # skipped everything
       ;;
  esac
  printf '%-18s %-5s rc=%-3s %s\n' "$name" "$verdict" "$rc" "$line" >> "$VERDICTS"
  [ "$verdict" = PASS ] || FAILED=1
  printf '%-18s %s\n' "$name" "$verdict"
}

# Lanes known to fail for reasons that predate this work.  A sweep that always
# reports FAIL is not a gate, and deleting the lane hides the failure -- so name
# it, and flip the sweep to FAIL if it ever starts PASSING, which means someone
# fixed it and this entry should go.
declare -A KNOWN_FAIL=()

score_exit() {  # $1=name $2=rc   (for gates that are not pytest)
  local verdict=PASS
  if [ -n "${KNOWN_FAIL[$1]:-}" ]; then
    if [ "$2" -ne 0 ]; then
      verdict=XFAIL
    else
      verdict=XPASS; FAILED=1     # fixed -- remove it from KNOWN_FAIL
    fi
  elif [ "$2" -ne 0 ]; then
    verdict=FAIL; FAILED=1
  fi
  printf '%-18s %-5s rc=%-3s -\n' "$1" "$verdict" "$2" >> "$VERDICTS"
  printf '%-18s %s\n' "$1" "$verdict"
}

pytest_lane() {  # $1=name  rest=paths
  local name=$1; shift
  "$PY" -m pytest -q --junit-xml="$OUT/$name.junit.xml" "$@" \
    > "$OUT/$name.log" 2>&1
  score_junit "$name" "$?" "$OUT/$name.junit.xml"
}

script_lane() {  # $1=name  rest=command
  local name=$1; shift
  "$@" > "$OUT/$name.log" 2>&1
  score_exit "$name" "$?"
}

echo "gate sweep -> $OUT"
echo

# --- unit lanes -------------------------------------------------------------
pytest_lane backends   "$CODE/test/backends"
pytest_lane hyperpipe  "$CODE/test/hyperpipe/tests"
pytest_lane cip        "$CODE/test/test_cip_pipeline.py" \
                       "$CODE/test/test_cip_format_decision.py" \
                       "$CODE/test/test_cip_evidence_consolidation.py" \
                       "$CODE/test/test_cip_priors.py"
pytest_lane core-unit  "$CODE/test/test_worker_partition.py" \
                       "$CODE/test/test_extrinsic_stage_shared.py" \
                       "$CODE/test/test_hyperpipeline_io.py" \
                       "$CODE/test/test_hyperpipeline_grid_metadata.py" \
                       "$CODE/test/test_hypercombine_formats.py" \
                       "$CODE/test/test_grid_loader_parity.py" \
                       "$CODE/test/test_osg_cache_rewrite.py" \
                       "$CODE/test/test_external_grid_fetch.py" \
                       "$CODE/test/test_container_exe_paths.py" \
                       "$CODE/test/test_convergence_exit_codes.py" \
                       "$CODE/test/test_ile_early_exit_order.py" \
                       "$CODE/test/test_pseudo_pipe_option_precedence.py" \
                       "$CODE/test/test_database.py" \
                       "$CODE/test/test_eos_posterior_header.py"
pytest_lane lisa       "$CODE/test/test_lisa_driver_drift.py" \
                       "$CODE/test/test_lisa_pseudo_pipe_contract.py"
pytest_lane calmarg    "$CODE/test/test_calmarg_calibration.py"

# --- build and execution gates ---------------------------------------------
script_lane pseudo-build "$PY" \
  "$CODE/test/hyperpipe/integration/run_pseudo_build_gate.py"
script_lane terminal-exec "$PY" \
  "$CODE/test/hyperpipe/integration/run_terminal_execution_gate.py"
script_lane const-lnL "$PY" \
  "$CODE/test/hyperpipe/integration/run_constant_likelihood_pipeline_gate.py"
script_lane const-lnL-local "$PY" \
  "$CODE/test/hyperpipe/integration/run_constant_likelihood_pipeline_gate.py" \
  --backend local
script_lane const-lnL-legacy "$PY" \
  "$CODE/test/hyperpipe/integration/run_constant_likelihood_pipeline_gate.py" \
  --backend local --builder BasicIteration
script_lane const-lnL-osg "$PY" \
  "$CODE/test/hyperpipe/integration/run_constant_likelihood_pipeline_gate.py" \
  --osg-build-contract-only
script_lane cit-profile "$PY" \
  "$CODE/test/hyperpipe/production_build/run_cit_pseudo_profile_gate.py"

echo
echo "=== verdicts ==="
cat "$VERDICTS"
for name in "${!KNOWN_FAIL[@]}"; do
  echo
  echo "XFAIL $name: ${KNOWN_FAIL[$name]}"
done

echo
if [ "$FAILED" -ne 0 ]; then echo "SWEEP FAILED (logs in $OUT)"; else echo "SWEEP PASS ($OUT)"; fi
exit "$FAILED"
