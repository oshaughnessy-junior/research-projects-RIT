#! /usr/bin/env bash

set -euo pipefail

if [[ "${RIFT_CI_REQUIRE_GPU:-0}" == "1" ]]; then
    python - <<'PY'
import sys

try:
    import cupy
except Exception as exc:
    raise SystemExit(f"RIFT_CI_REQUIRE_GPU=1 but cupy could not be imported: {exc}") from exc

try:
    n_devices = cupy.cuda.runtime.getDeviceCount()
except Exception as exc:
    raise SystemExit(f"RIFT_CI_REQUIRE_GPU=1 but CUDA devices could not be queried: {exc}") from exc

if n_devices < 1:
    raise SystemExit("RIFT_CI_REQUIRE_GPU=1 but cupy reported zero CUDA devices")

x = cupy.arange(8, dtype=cupy.float64)
if float(cupy.asnumpy((x * x).sum())) != 140.0:
    raise SystemExit("RIFT_CI_REQUIRE_GPU=1 but a basic cupy device calculation failed")

from RIFT.integrators import mcsamplerGPU

if not getattr(mcsamplerGPU, "cupy_ok", False):
    raise SystemExit("RIFT_CI_REQUIRE_GPU=1 but RIFT.integrators.mcsamplerGPU did not enable cupy")

print(f"GPU preflight OK: cupy={cupy.__version__}, cuda_devices={n_devices}")
PY
fi

# Unit/regression tests for the sampler helpers themselves (fast, no data needed).
# The extrinsic "zoom box" limits under the cosine samplers live here: they are pure
# coordinate-transform + prior-mass identities, so they belong with the integrator gate
# rather than with the end-to-end run tests.
python -m pytest -q MonteCarloMarginalizeCode/Code/test/test_limit_cosine_samplers.py
# --limit-distance: the SAMPLING-only distance box.  Listed separately from the cosine
# file because the failure it guards is a different one -- not "the flag is ignored" but
# "the flag silently renormalized the prior", which looks like success in the obvious
# check (lnZ comes back unchanged) and is only separable with a constant likelihood.
python -m pytest -q MonteCarloMarginalizeCode/Code/test/test_limit_distance.py
python -m pytest -q MonteCarloMarginalizeCode/Code/test/test_mcsampler_ensemble_log_contract.py

# Which _rvs field the CIP posterior export reads, per sampler convention.  The export
# re-read the raw samples["integrand"] after dat_logL had already resolved the field, so
# --sampler-method adaptive_cartesian_gpu --internal-use-lnL died with KeyError:
# 'integrand' AFTER a converged integral: *_int.dat was written, the posterior samples
# were not, and the exit code was 1.  mcsamplerGPU.integrate_log leaves only
# 'log_integrand'; AV/NFlow/portfolio alias 'integrand' to it, which is what made a
# working arm one flag away from a dead one.
#
# Five subprocess arms plus six integrator cases (~55 s total), not a static check: the
# crash is in a flat driver script with no importable export function, and both the value
# written and the ROW it lands on are as much the point as the exit code -- exporting
# exp(lnL), log(lnL) or well-formed lnL against shuffled rows all exit 0.  The synthetic
# input is exactly quadratic in mc and the arms fit with --fit-method quadratic, so the
# exported lnL can be checked against the likelihood at its own row's masses.  Verified to
# FAIL on b281ccff2 without the fix (acgpu_lnL arm only; the other four arms pass there).
_CIP_EXPORT_TESTS=MonteCarloMarginalizeCode/Code/test/test_cip_posterior_export_lnL.py
# Raise EXPECTED by RUNNING collection, never by arithmetic.
_CIP_EXPORT_EXPECTED=16
_CIP_EXPORT_FOUND=$(python -m pytest -q --collect-only "$_CIP_EXPORT_TESTS" 2>/dev/null | grep -c '::' || true)
if [ "$_CIP_EXPORT_FOUND" -ne "$_CIP_EXPORT_EXPECTED" ]; then
    echo "cip-export gate: collected $_CIP_EXPORT_FOUND tests, expected $_CIP_EXPORT_EXPECTED" >&2
    exit 1
fi
python -m pytest -q "$_CIP_EXPORT_TESTS"

# --psi-marginalization: analytic polarization-angle marginalization made reachable on
# the legacy scalar likelihood path (factored_likelihood.NetworkLogLikelihoodPolarizationMarginalized
# was previously dead code, unreachable from any driver and untested by any importable
# test).  Covers the analytic marginal against a brute-force quadrature, the driver's
# refuse-don't-ignore prerequisite checks, and a real subprocess run on synthetic data.
python -m pytest -q MonteCarloMarginalizeCode/Code/test/test_psi_marginalization.py
# psi and phi_orb priors derived from their sampling ranges (evidence normalization): the
# fixed 1/pi psi prior over (0, 2 pi) put +ln 2 on every ILE lnZ (+ln 8 under
# --internal-rotate-phase).  Constructor, driver wiring, and a --zero-likelihood run whose
# lnZ must be ln(total prior mass).
python -m pytest -q MonteCarloMarginalizeCode/Code/test/test_angle_prior_normalization.py
# mcsamplerGPU.draw_simplified() must report the density its draws actually come from.  It
# returned the RAW pdf while drawing from the NORMALIZED cdf_inv, so integrate() -- which uses
# draw_simplified() -- reported ln Z low by log(prod(_pdf_norm)) for any caller passing an
# unnormalized sampling pdf.  THIS GATE ALREADY HAD THE RIGHT TECHNIQUE AND STILL MISSED IT:
# test_limit_distance.py's absolute-evidence check runs mcsampler, not mcsamplerGPU; where it
# does build a mcsamplerGPU it asserts on draw_simplified(...)[-1], the SAMPLES, never [0], the
# reported density; and distance_sampler_kwargs() hands it an already-normalized pdf, which
# makes _pdf_norm 1 and the defect a no-op.  A constant integrand with an UNNORMALIZED pdf,
# checked against ln(prior mass) in absolute terms, is the combination that separates it -- a
# difference of two runs cancels the constant.
# Collection-count guard, matching this script's other gates: a silent shrink reads as green.
# Raise EXPECTED by RUNNING collection, never by arithmetic.
_GPUNORM_TESTS=MonteCarloMarginalizeCode/Code/test/integrators/test_mcsamplerGPU_pdf_normalization.py
_GPUNORM_EXPECTED=14
_GPUNORM_FOUND=$(python -m pytest -q --collect-only "$_GPUNORM_TESTS" 2>/dev/null | grep -c '::' || true)
if [ "$_GPUNORM_FOUND" -ne "$_GPUNORM_EXPECTED" ]; then
    echo "mcsamplerGPU pdf-normalization gate: collected $_GPUNORM_FOUND tests, expected $_GPUNORM_EXPECTED" >&2
    exit 1
fi
python -m pytest -q "$_GPUNORM_TESTS"
# The PORTFOLIO half of the same contract: a member must report its sampling density through
# sampling_density(), normalized.  mcsamplerGPU had no such method, so any portfolio containing
# one fell back to the stratified per-member joint_p_s -- valid only if EVERY member reports a
# normalized density, which mcsamplerAdaptiveVolume does not (it reports V_s/V).  Mixing them
# returned 0.753772 where the exact answer is 1.386294.  Also pins the refusal that keeps the
# next density-less member from reintroducing the bias, and reset_sampling's restore of
# _pdf_norm, which the adapted-proposal fix above leaves stale.
# Same collection-count guard, same reason.
_PORTDENS_TESTS=MonteCarloMarginalizeCode/Code/test/integrators/test_portfolio_member_density.py
_PORTDENS_EXPECTED=37
_PORTDENS_FOUND=$(python -m pytest -q --collect-only "$_PORTDENS_TESTS" 2>/dev/null | grep -c '::' || true)
if [ "$_PORTDENS_FOUND" -ne "$_PORTDENS_EXPECTED" ]; then
    echo "portfolio member-density gate: collected $_PORTDENS_FOUND tests, expected $_PORTDENS_EXPECTED" >&2
    exit 1
fi
python -m pytest -q "$_PORTDENS_TESTS"

# The --zero-likelihood stand-in, checked as WIRING rather than as an answer: its argument order
# against the driver's own supplemental_ln_likelihood call sites, its generated signature against
# every live likelihood_function signature, and its array module.  This is the half the
# end-to-end gate below is structurally blind to -- right_ascension, phi_orb and psi are iid
# uniform on [0, 2pi), so NO marginal can tell a permutation of the three apart, and the runners
# have no cupy, so nothing there sees a host/device mistake.  Mostly AST + exec, ~5 s.
# Its section 4 runs the same device paths against REAL cupy and SKIPS here, because the runners
# have none.  Skipped tests are still collected, so the count below is the same everywhere; but
# a green run here has NOT exercised the device half.  That half is run by hand on a CIT GPU
# node with CUDA_VISIBLE_DEVICES pinned to a slot the installed cupy supports -- see
# _cupy_or_skip in the test file.
# The collected count tracks the number of `def likelihood_function` signatures in the driver
# (one parametrized case each, currently 8); if a signature is added, look at the new one and
# update the number.
_ZLSTANDIN_TESTS=MonteCarloMarginalizeCode/Code/test/test_zero_likelihood_standin.py
_ZLSTANDIN_EXPECTED=27
_ZLSTANDIN_FOUND=$(python -m pytest -q --collect-only "$_ZLSTANDIN_TESTS" 2>/dev/null | grep -c '::' || true)
if [ "$_ZLSTANDIN_FOUND" -ne "$_ZLSTANDIN_EXPECTED" ]; then
    echo "zero-likelihood stand-in gate: collected $_ZLSTANDIN_FOUND tests, expected $_ZLSTANDIN_EXPECTED" >&2
    exit 1
fi
python -m pytest -q "$_ZLSTANDIN_TESTS"

# END-TO-END, ANALYTIC.  ILE run to completion on a case whose answer is known in closed form:
# a zero-strain fixture, --zero-likelihood (exact ln Z = 0), and an analytic supplementary
# factor A*cos(phi_orb) + B*cos(iota) whose marginal is exactly ln I0(A) + ln(sinh(B)/B).  The
# sampler unit tests check samplers; pseudo_pipe/asimov check that the pipeline RUNS; nothing
# checked that it runs and is CORRECT.  That gap hid two defects: --zero-likelihood silently
# discarded a --supplementary-likelihood-factor-* (two runs differing only by it returned ln Z
# bit-identical, while the banner reported the factor as active), and the *args stand-in it was
# replaced with reported co_argcount 0 to mcsampler, killing --zero-likelihood with the
# driver's own default --sampler-method adaptive_cartesian.  Both were invisible because the
# driver catches the exception, prints FAILED ANALYSIS and EXITS 0.  It also caught ILE
# --sampler-method GMM returning an evidence ~30 nats wrong; #359 fixed that, and GMM is now a
# lane here rather than a recorded defect.  Needs no network, no real event and no GPU; about
# 8-12 s per ILE arm plus ~15 s once for the distance-marginalization lookup table.
_E2E_TESTS=MonteCarloMarginalizeCode/Code/test/test_e2e_analytic_pipeline.py
_E2E_EXPECTED=17
_E2E_FOUND=$(python -m pytest -q --collect-only "$_E2E_TESTS" 2>/dev/null | grep -c '::' || true)
if [ "$_E2E_FOUND" -ne "$_E2E_EXPECTED" ]; then
    echo "e2e analytic gate: collected $_E2E_FOUND tests, expected $_E2E_EXPECTED" >&2
    exit 1
fi
python -m pytest -q "$_E2E_TESTS"

# Supplementary-likelihood plugin hook: the NAL reader/evaluator (pure numpy, no data) and the
# static guard on the drivers' prepare-hook wiring, which is what makes the plugin receive the
# SAMPLING basis at all. Both are seconds-long and protect a silent-wrong-answer path.
python -m pytest -q MonteCarloMarginalizeCode/Code/test/test_nal_io.py \
                   MonteCarloMarginalizeCode/Code/test/test_supplementary_likelihood_hook.py

# Time-marginalization quadrature.  The historical rule integrates exp(lnL(t)) with Simpson at
# the FIXED spacing deltaT=1/srate, while the integrand's width sigma_t = 1/(2 pi rho sigma_f) is
# set by the SIGNAL and shrinks as 1/rho -- so production under-resolves its own integrand, worse
# at higher SNR (measured: the reported lnL moves 1.649 nats when the grid phase is scanned over
# 2*deltaT at srate 4096, rho=40).  This gate covers the opt-in band-limited quadrature against an
# ANALYTIC continuous reference, plus its finite-window reconstruction and resolution guards and
# -- the part that matters most
# here -- that the option actually reaches the shipped likelihood rather than being inert.
# The PIPELINE file is listed alongside it deliberately: the quadrature is inert unless it
# survives util_RIFT_pseudo_pipe.py -> helper_LDG_Events.py -> args_ile.txt ->
# create_event_parameter_pipeline_BasicIteration -> ILE*.sub, and the last link is an
# INHERITANCE (ile_args_extr = ile_args + ...), not an explicit forward.  An unlisted test never
# runs in this CI, so wiring the file in is part of shipping the wiring.
_TMARG_TESTS=(
    MonteCarloMarginalizeCode/Code/test/test_time_marginalization_quadrature.py
    MonteCarloMarginalizeCode/Code/test/test_time_marginalization_quadrature_pipeline.py
    MonteCarloMarginalizeCode/Code/test/test_continuous_time_posterior_export.py
)
# Count guard, matching .travis/test-slowrot.sh and test-jax.sh.  `set -e` already
# catches a total collection failure (pytest exits 5), but a silent shrink from 60
# tests to 3 -- a rename, a stale -k, a decorator that stops matching -- reads as
# green.  Raise EXPECTED by RUNNING collection, never by arithmetic.
_TMARG_EXPECTED=171
_TMARG_FOUND=$(python -m pytest -q --collect-only "${_TMARG_TESTS[@]}" 2>/dev/null | grep -c '::' || true)
if [ "$_TMARG_FOUND" -ne "$_TMARG_EXPECTED" ]; then
    echo "time-marginalization gate: collected $_TMARG_FOUND tests, expected $_TMARG_EXPECTED" >&2
    exit 1
fi
# SKIP guard.  `pytest -q` exits 0 with skips, so a test that quietly stops
# running reads as green, and the count guard above catches DESELECTION, not
# SKIPPING.
#
# This IDENTIFIES rather than COUNTS, which an earlier version of it did not.
# Counting is wrong twice over: a compensating pair (one importorskip starts
# firing while the GPU test stops) keeps the total unchanged, and the expected
# total is a property of the RUNNER, not of the code -- on a GPU-equipped runner
# that does not set RIFT_CI_REQUIRE_GPU=1 the cupy test legitimately stops
# skipping, and a count guard then fails a perfectly good run.  So: allow skips
# whose REASON names cupy/GPU, and fail on any other skip whatever the total.
_TMARG_OUT=$(python -m pytest -q -rs "${_TMARG_TESTS[@]}" 2>&1) || { echo "$_TMARG_OUT"; exit 1; }
echo "$_TMARG_OUT" | tail -20
_TMARG_BAD=$(echo "$_TMARG_OUT" | grep -E '^SKIPPED' | grep -vciE 'cupy|gpu|cuda' || true)
_TMARG_BAD=${_TMARG_BAD:-0}
if [ "$_TMARG_BAD" -ne 0 ]; then
    echo "time-marginalization gate: $_TMARG_BAD test(s) skipped for a reason other than an absent GPU:" >&2
    echo "$_TMARG_OUT" | grep -E '^SKIPPED' | grep -viE 'cupy|gpu|cuda' >&2
    exit 1
fi

# Peak-local time-marginalization quadrature.  Same defect, same derived-resolution
# discipline; what changes is WHERE the refined grid is placed -- around the enumerated
# peaks of a band-limited kappa rather than over the whole window, so the cost stops
# growing with SNR.  What this gate has to protect, beyond accuracy: that the intervals
# are MERGED (the un-merged variant double-counts the overlap, +1.6 nats at rho~6), that
# the omitted mass is BOUNDED rather than assumed (a deliberately sabotaged enumeration
# must be caught and sent to the dense path), that the local evaluator reconstructs the
# same interpolant the dense FFT does at every production npts including the odd ones,
# and that the option reaches the shipped likelihood instead of being inert.
_TMARG_PL_TESTS=MonteCarloMarginalizeCode/Code/test/test_time_marginalization_peak_local.py
# Raise EXPECTED by RUNNING collection, never by arithmetic.
_TMARG_PL_EXPECTED=121
_TMARG_PL_FOUND=$(python -m pytest -q --collect-only "$_TMARG_PL_TESTS" 2>/dev/null | grep -c '::' || true)
if [ "$_TMARG_PL_FOUND" -ne "$_TMARG_PL_EXPECTED" ]; then
    echo "peak-local gate: collected $_TMARG_PL_FOUND tests, expected $_TMARG_PL_EXPECTED" >&2
    exit 1
fi
# SKIP guard, IDENTIFYING rather than counting, for the reasons the band-limited gate
# above gives: a compensating pair leaves the total unchanged, and the expected total is
# a property of the RUNNER (a GPU-equipped runner that does not set RIFT_CI_REQUIRE_GPU=1
# legitimately stops skipping, and a count guard then fails a good run).  Allow skips
# whose REASON names cupy/GPU, and fail on any other skip whatever the total.
_TMARG_PL_OUT=$(python -m pytest -q -rs "$_TMARG_PL_TESTS" 2>&1) || { echo "$_TMARG_PL_OUT"; exit 1; }
echo "$_TMARG_PL_OUT" | tail -20
_TMARG_PL_BAD=$(echo "$_TMARG_PL_OUT" | grep -E '^SKIPPED' | grep -vciE 'cupy|gpu|cuda' || true)
_TMARG_PL_BAD=${_TMARG_PL_BAD:-0}
if [ "$_TMARG_PL_BAD" -ne 0 ]; then
    echo "peak-local gate: $_TMARG_PL_BAD test(s) skipped for a reason other than an absent GPU:" >&2
    echo "$_TMARG_PL_OUT" | grep -E '^SKIPPED' | grep -viE 'cupy|gpu|cuda' >&2
    exit 1
fi

# Joint (phi,psi) peak-local angle marginalization, numpy reference kernel.  Gated here
# because test/ has no manifest check of its own: an unlisted test file is simply never
# run, which is the failure test-jax.sh exists to prevent one level up.  What this has to
# protect: that the outside supremum is CERTIFIED (a straddling cell must count as
# outside -- classifying grid centres once returned "nothing uncovered" and accepted
# unconditionally), that a distance node is only dropped when the drop is provable
# against the computed value, and that an undersized region is routed to the finite
# dense fallback rather than returned locally.  The algebraic follow-up also pins
# the BKK/resultant enumerator on co-dominant, near-annihilating, exactly degenerate,
# and amplitude-scaled systems, requires inside-cover convergence even after a
# complete enumeration, and keeps the NumPy fallback independent of optional JAX.
_JOINT_PL_TESTS=MonteCarloMarginalizeCode/Code/test/test_joint_angle_peak_local.py
# Raise EXPECTED by RUNNING collection, never by arithmetic.
_JOINT_PL_EXPECTED=37
_JOINT_PL_FOUND=$(python -m pytest -q --collect-only "$_JOINT_PL_TESTS" 2>/dev/null | grep -c '::' || true)
if [ "$_JOINT_PL_FOUND" -ne "$_JOINT_PL_EXPECTED" ]; then
    echo "joint peak-local gate: collected $_JOINT_PL_FOUND tests, expected $_JOINT_PL_EXPECTED" >&2
    exit 1
fi
python -m pytest -q "$_JOINT_PL_TESTS"

# mcsamplerEnsemble dim-group keys, and the ln Z they decide.  A gmm_dict key indexes the
# POSITIONAL ARGUMENT order of integrate(), and every dimension must be in exactly one group;
# an uncovered one is never sampled (uninitialized memory) and the evidence is meaningless.
# Analytic targets, so these assert an exact answer, not a remembered one.
python -m pytest -q MonteCarloMarginalizeCode/Code/test/integrators/test_ensemble_dim_group_cover.py

python MonteCarloMarginalizeCode/Code/test/test_mcsamplerEnsemble_extended.py --as-test --n-max 100000

python MonteCarloMarginalizeCode/Code/test/test_mcsamplerEnsemble_extended.py --as-test --n-max 100000 --use-lnL

# Q_lm pregrid factor (PR #261), pipeline passthrough.  --q-time-pregrid-factor had NO
# helper/pseudo_pipe wiring at all until this option was added -- it was reachable only
# through --manual-extra-ile-args, which RO'S directive 2026-09-08 says is too easy to get
# wrong for the time-stencil/time-quadrature family.  Same discipline as the
# time-marginalization-quadrature gate above: an unlisted test file is simply never run, so
# wiring the file in is part of shipping the wiring.  What these files protect: the
# driver-mirroring prerequisite check (--vectorized required; --rotation-slow/--freqresponse/
# calibration marginalization excluded), the forced-cubic-stencil conflict (factor 8 refuses
# an explicit --interpolate-time other than cubic, with the driver's OWN wording), the
# two-stage refuse-not-ignore emission guard, and that the option actually reaches
# helper_ile_args.txt / args_ile.txt rather than being inert.  test_q_time_pregrid_driver_
# parity.py (PR #281 follow-up review, MAJOR #2) adds the piece those two files left
# untested: it EXECUTES bin/integrate_likelihood_extrinsic_batchmode as a subprocess for
# every prerequisite above and asserts the builder refuses exactly when the driver refuses.
# The real DAG-build regression (--internal-ile-q-time-pregrid-factor reaching ILE.sub /
# ILE_extr.sub / ILE_puff.sub, PR #281 review MAJOR #1) is test_q_time_pregrid_dag.py,
# registered in .github/workflows/ci.yml's test-run job next to test_jax_ile_selectable.py
# rather than here: it is a full subprocess DAG build, not a fast unit gate.  test-run is
# matrixed over TWO lanes (legacy py3.9, modern py3.12), so this step runs twice per push,
# measured at ~236s/lane -- ~8 minutes total, not ~3 (PR #291 review, NOTE #5: the single-run
# figure this comment used to state undercounted the per-lane doubling every step in that
# job already pays).
_QPREGRID_TESTS=(
    MonteCarloMarginalizeCode/Code/test/test_q_time_pregrid.py
    MonteCarloMarginalizeCode/Code/test/test_q_time_pregrid_pipeline.py
    MonteCarloMarginalizeCode/Code/test/test_q_time_pregrid_driver_parity.py
)
# Raise EXPECTED by RUNNING collection, never by arithmetic.
_QPREGRID_EXPECTED=77
_QPREGRID_FOUND=$(python -m pytest -q --collect-only "${_QPREGRID_TESTS[@]}" 2>/dev/null | grep -c '::' || true)
if [ "$_QPREGRID_FOUND" -ne "$_QPREGRID_EXPECTED" ]; then
    echo "q-time-pregrid gate: collected $_QPREGRID_FOUND tests, expected $_QPREGRID_EXPECTED" >&2
    exit 1
fi
python -m pytest -q "${_QPREGRID_TESTS[@]}"
