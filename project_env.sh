#!/bin/bash
# project_env.sh -- the ONE definition of the paper-2 / TD-likelihood runtime environment.
#
# WHY THIS EXISTS
#   Twice now an *ambient*, *inherited* environment has silently corrupted production runs:
#
#     1. 2026-07-22 "false convergence at iteration 2".  test.sub is `getenv = *`, so the
#        convergence-test node inherited the submitting shell's PYTHONPATH, which pointed at
#        ~/sandbox_new_rift -- an OLDER RIFT with no RIFT/misc/hyperpipeline_io.py.  The
#        script died on ImportError and exited 1.  The DAG carries
#        `ABORT-DAG-ON <test> 1 RETURN 0`, i.e. "exit 1 == converged", so the crash was
#        INDISTINGUISHABLE FROM CONVERGENCE and all four DAGs stopped early reporting
#        DAG_STATUS_OK.
#
#     2. The `getenv = *` / bare-`python` trap: resubmitting from a shell whose PATH has no
#        `python` (only python3) kills every local-universe node with exit 127 and burns all
#        the master RETRYs.
#
#   Both share one root cause: the runtime environment was whatever the submitting shell
#   happened to contain.  This file makes the environment DECLARED rather than INHERITED.
#
#   There is also a structural contaminant that is NOT ambient and cannot be fixed by
#   unsetting variables: the `junior_rift` conda env contains an editable-install pth,
#       lib/python3.11/site-packages/__editable__.rift-0.0.18.0rc0.pth
#           -> /home/richard.oshaughnessy/sandbox_new_rift/research-projects-RIT/.../Code
#   so a bare `import RIFT` in that env resolves to sandbox_new_rift EVEN WITH PYTHONPATH
#   UNSET.  Assigning PYTHONPATH to the stable checkout deterministically overrides it
#   (PYTHONPATH precedes site-packages on sys.path; verified empirically -- see validate).
#
# DESIGN RULES
#   * PYTHONPATH and PATH are ASSIGNED, never appended/prepended to ambient values, so
#     nothing from the caller's shell can leak in.
#   * $RIFT_CODE/bin comes FIRST on PATH so the RIFT *scripts* and the RIFT *library* come
#     from the same pinned tree (no script/library version skew).  The conda env follows,
#     supplying `python` and everything else.  The clone's bin/ contains no `python`, so it
#     cannot shadow the interpreter.
#   * Everything is pinned to the STABLE CLONE (a clone, not a prunable worktree).
#
# USAGE
#   source /home/richard.oshaughnessy/vaishak_p/rift_tdlike/project_env.sh          # set env
#   source .../project_env.sh && validate_env                                        # + check
#   bash    .../project_env.sh --validate                                            # check only
#   bash    .../project_env.sh --print-environment    # one-line KEY=VAL for condor `environment=`
#   bash    .../project_env.sh --banner                                              # provenance line
#
# Exit status of --validate: 0 clean, non-zero on any failure.  Add --strict to also fail
# when a shadowed (overridden but still installed) second RIFT is present.

# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------
RIFT_PROJECT_ROOT="/home/richard.oshaughnessy/vaishak_p/rift_tdlike"
RIFT_CODE="${RIFT_PROJECT_ROOT}/MonteCarloMarginalizeCode/Code"
RIFT_CONDA_ENV="/home/richard.oshaughnessy/.conda/envs/junior_rift"
RIFT_CONDA_BIN="${RIFT_CONDA_ENV}/bin"

# Pinned commit this project is supposed to be running.  Validation warns on mismatch.
RIFT_PINNED_COMMIT="3e1287bbc75e406b2680a4163feb70584f3e2593"

# Any of these appearing in PATH/PYTHONPATH means a foreign RIFT has leaked in.
RIFT_CONTAMINANT_PATTERNS=(
    "sandbox_new_rift"
    ".claude/worktrees"
    ".claude-worktrees"
    "RIFT_develUWM/src/research-projects-RIT"
    "RIFT_calmarg_in_loop"
    "RIFT_frozen_distexport"
    "RIFT_o4d_track"
    "RIFT_slowrot"
    "rift_O4d_junior_ralph"
)

export RIFT_PROJECT_ROOT RIFT_CODE RIFT_CONDA_ENV RIFT_CONDA_BIN RIFT_PINNED_COMMIT

# ---------------------------------------------------------------------------
# The environment itself -- ASSIGNED, not appended.
# ---------------------------------------------------------------------------
export PYTHONPATH="${RIFT_CODE}"
export PATH="${RIFT_CODE}/bin:${RIFT_CONDA_BIN}:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# Keep python deterministic and quiet about stale bytecode from other trees.
export PYTHONNOUSERSITE=1     # ignore ~/.local/lib/pythonX.Y/site-packages
export PYTHONDONTWRITEBYTECODE=1

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

rift_env_commit() {
    git -C "${RIFT_PROJECT_ROOT}" rev-parse --short HEAD 2>/dev/null || echo "unknown"
}

# One-line provenance banner -- print this into every run log.
rift_env_banner() {
    printf '[project_env] RIFT_CODE=%s commit=%s python=%s RIFT=%s\n' \
        "${RIFT_CODE}" \
        "$(rift_env_commit)" \
        "$(command -v python 2>/dev/null || echo MISSING)" \
        "$(python -c 'import RIFT,os;print(os.path.dirname(RIFT.__file__))' 2>/dev/null || echo IMPORT-FAILED)"
}

# Emit `KEY=VAL KEY=VAL ...` suitable for an HTCondor `environment = "..."` line.
rift_env_condor_environment() {
    printf 'PATH=%s PYTHONPATH=%s PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 HOME=%s USER=%s\n' \
        "${PATH}" "${PYTHONPATH}" "${HOME}" "${USER}"
}

# ---------------------------------------------------------------------------
# validate_env -- fail loudly, non-zero, on anything that could silently corrupt a run.
# ---------------------------------------------------------------------------
validate_env() {
    local strict=0
    [ "${1:-}" = "--strict" ] && strict=1
    local fail=0 warn=0

    _err()  { printf '  FAIL: %s\n' "$*" >&2; fail=$((fail+1)); }
    _warn() { printf '  WARN: %s\n' "$*" >&2; warn=$((warn+1)); }
    _ok()   { printf '  ok:   %s\n' "$*"; }

    echo "[project_env] validating..."

    # -- 1. the stable checkout itself ------------------------------------------------
    if [ ! -d "${RIFT_CODE}" ]; then
        _err "stable checkout missing: ${RIFT_CODE}"
        echo "[project_env] VALIDATION FAILED (${fail} error(s))" >&2
        return 1
    fi
    _ok "stable checkout present: ${RIFT_CODE}"

    if [ ! -e "${RIFT_PROJECT_ROOT}/PROJECT_CHECKOUT.md" ]; then
        _warn "PROJECT_CHECKOUT.md missing -- is ${RIFT_PROJECT_ROOT} really the project clone?"
    fi

    # A clone has a .git DIRECTORY; a worktree has a .git FILE pointing elsewhere.
    if [ -f "${RIFT_PROJECT_ROOT}/.git" ]; then
        _err "${RIFT_PROJECT_ROOT} is a git WORKTREE (.git is a file), not a clone -- it can be pruned"
    elif [ -d "${RIFT_PROJECT_ROOT}/.git" ]; then
        _ok "checkout is a real clone (unprunable)"
    fi

    local cur; cur=$(git -C "${RIFT_PROJECT_ROOT}" rev-parse HEAD 2>/dev/null)
    if [ -n "${cur}" ] && [ "${cur}" != "${RIFT_PINNED_COMMIT}" ]; then
        _warn "checkout is at ${cur:0:8}, expected pinned ${RIFT_PINNED_COMMIT:0:8} (campaign code changed?)"
    else
        _ok "at pinned commit ${RIFT_PINNED_COMMIT:0:8}"
    fi

    # -- 2. interpreter and required executables --------------------------------------
    local py; py=$(command -v python 2>/dev/null)
    if [ -z "${py}" ]; then
        _err "bare 'python' not on PATH -- getenv=* local nodes will die with exit 127"
    else
        _ok "python -> ${py}"
        case "${py}" in
            "${RIFT_CONDA_BIN}"/*) ;;
            *) _warn "python is not from the project conda env (${RIFT_CONDA_BIN})" ;;
        esac
    fi

    local missing=()
    for exe in util_CleanILE.py util_ILEdagPostprocess.sh util_RandomizeOverlapOrder.py \
               util_CIPDirSummarizeEvidence.py convert_output_format_ile2inference \
               util_ParameterPuffball.py convergence_test_samples.py; do
        command -v "${exe}" >/dev/null 2>&1 || missing+=("${exe}")
    done
    if [ ${#missing[@]} -gt 0 ]; then
        _err "required executables not on PATH: ${missing[*]}"
    else
        _ok "all required RIFT executables resolve on PATH"
    fi

    # helper scripts must come from the pinned tree, not some other checkout
    local clean_exe; clean_exe=$(command -v util_CleanILE.py 2>/dev/null)
    case "${clean_exe}" in
        "${RIFT_CODE}"/*) _ok "util_CleanILE.py from the pinned tree" ;;
        "") ;;
        *) _warn "util_CleanILE.py resolves outside the pinned tree: ${clean_exe}" ;;
    esac

    # -- 3. THE BIG ONE: which RIFT does python actually import? ----------------------
    local riftdir
    riftdir=$(python -c 'import RIFT,os;print(os.path.dirname(os.path.dirname(RIFT.__file__)))' 2>/dev/null)
    if [ -z "${riftdir}" ]; then
        _err "'import RIFT' failed entirely"
    elif [ "${riftdir}" != "${RIFT_CODE}" ]; then
        _err "import RIFT resolves to ${riftdir}, NOT the stable checkout ${RIFT_CODE}"
    else
        _ok "import RIFT -> ${riftdir}"
    fi

    # the specific module whose absence caused the 2026-07-22 silent false convergence
    if ! python -c 'from RIFT.misc import hyperpipeline_io' >/dev/null 2>&1; then
        _err "'from RIFT.misc import hyperpipeline_io' FAILED -- this is exactly the import that silently faked convergence on 2026-07-22"
    else
        _ok "RIFT.misc.hyperpipeline_io imports (no 2026-07-22 skew)"
    fi

    # -- 4. how many RIFTs are reachable at all? -------------------------------------
    local nrift others
    others=$(python - <<'PYEOF' 2>/dev/null
import sys, os, glob
seen = []
for p in sys.path:
    if not p:
        continue
    cand = os.path.join(p, "RIFT", "__init__.py")
    if os.path.exists(cand):
        d = os.path.realpath(p)
        if d not in seen:
            seen.append(d)
for d in seen:
    print(d)
PYEOF
)
    nrift=$(printf '%s\n' "${others}" | grep -c . )
    if [ "${nrift}" -gt 1 ]; then
        # The winner is checked in section 3; a shadowed second copy is survivable but
        # is exactly the latent hazard that bit us, so it is always reported.
        local shadowed
        shadowed=$(printf '%s\n' "${others}" | grep -vF "${RIFT_CODE}" | tr '\n' ' ')
        if [ "${strict}" -eq 1 ]; then
            _err "more than one RIFT reachable (shadowed: ${shadowed})"
        else
            _warn "more than one RIFT reachable; the pinned one wins, but shadowed copies exist: ${shadowed}"
            _warn "  (source: ${RIFT_CONDA_ENV}/lib/python3.11/site-packages/__editable__.rift-*.pth -- an editable install of sandbox_new_rift baked into the conda env)"
        fi
    else
        _ok "exactly one RIFT reachable"
    fi

    # -- 5. no foreign checkout anywhere in PATH/PYTHONPATH --------------------------
    local hit found=0
    for pat in "${RIFT_CONTAMINANT_PATTERNS[@]}"; do
        for var in PYTHONPATH PATH; do
            hit=$(printf '%s' "${!var}" | tr ':' '\n' | grep -F "${pat}" | tr '\n' ' ')
            if [ -n "${hit}" ]; then
                _err "contaminant '${pat}' present in ${var}: ${hit}"
                found=1
            fi
        done
    done
    [ "${found}" -eq 0 ] && _ok "no foreign RIFT checkout in PATH or PYTHONPATH"

    # -- 6. PYTHONPATH must contain ONLY the project ---------------------------------
    local bad=""
    while IFS= read -r entry; do
        [ -z "${entry}" ] && continue
        case "${entry}" in
            "${RIFT_CODE}"|"${RIFT_CODE}"/*) ;;
            *) bad="${bad} ${entry}" ;;
        esac
    done < <(printf '%s' "${PYTHONPATH}" | tr ':' '\n')
    if [ -n "${bad}" ]; then
        _err "PYTHONPATH contains entries outside the project:${bad}"
    else
        _ok "PYTHONPATH is project-only"
    fi

    # -- verdict ----------------------------------------------------------------------
    rift_env_banner
    if [ "${fail}" -gt 0 ]; then
        echo "[project_env] VALIDATION FAILED (${fail} error(s), ${warn} warning(s))" >&2
        return 1
    fi
    echo "[project_env] validation OK (${warn} warning(s))"
    return 0
}

# ---------------------------------------------------------------------------
# CLI mode (when executed rather than sourced)
# ---------------------------------------------------------------------------
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    case "${1:-}" in
        --validate)            validate_env "${2:-}" ;;
        --strict-validate)     validate_env --strict ;;
        --print-environment)   rift_env_condor_environment ;;
        --banner)              rift_env_banner ;;
        --print-path)          printf '%s\n' "${PATH}" ;;
        --print-pythonpath)    printf '%s\n' "${PYTHONPATH}" ;;
        *)
            echo "usage: $0 {--validate [--strict]|--strict-validate|--print-environment|--banner|--print-path|--print-pythonpath}"
            echo "   or: source $0 [&& validate_env]"
            exit 2
            ;;
    esac
    exit $?
fi
