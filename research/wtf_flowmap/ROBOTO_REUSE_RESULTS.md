# Existing roboto Q interpolation closes the declared local gate

**Passed on three fixed real-case points.** The missing operating choice was
already implemented: reflected spectral refinement of the stored correlations
by eight, followed by the existing four-tap cubic gather. With the same converged
quadrature/window/prior contract, this removes the earlier between-node mismatch.
It is not coarse-grid cubic, a new interpolation method, or a production default
change. No inference or model training ran.

## Exact paper and implementation

The actual repository is
[oshaughnessy-junior/RIFT_roboto_paper](https://github.com/oshaughnessy-junior/RIFT_roboto_paper),
commit `d668ff9c79930783d18725f74e8fa2cf96bb852e`, active branch
`codex/paper3-transverse-o4b`. Relevant committed text:

- [Paper 3, *RIFT for the O4 era: operating points, calibration marginalization, and cross-code validation*](https://github.com/oshaughnessy-junior/RIFT_roboto_paper/blob/d668ff9c79930783d18725f74e8fa2cf96bb852e/paper/paper3_core_operating.tex), time-interpolation/oracle discussion.
- [Companion Paper 1 Appendix D working source](https://github.com/oshaughnessy-junior/RIFT_roboto_paper/blob/d668ff9c79930783d18725f74e8fa2cf96bb852e/paper/sections/appendix_quadrature_revised.tex), equation `q-time-pregrid`. The companion's posted identity is [Scaling RIFT 1, arXiv 2609.37393v1](https://arxiv.org/abs/2609.37393v1); current working-source evidence is not conflated with the posted version.

The paper explains that cubic evaluates an already spectrally reconstructed fine
Q table. It separately warns that accurate Q does not cure time-quadrature alias.
Its independent full-frequency/CZT oracle, production gate and JAX records live in
`analyses/marginalization_reference_ile/` and
`analyses/paper3_jax_time_check_20261007/`. Those are source-reported validation,
not experiments rerun here. The paper's tolerances and source regimes are not
universal guarantees or permission to relax this investigation's stricter gates.
All ten audited paper/evidence files matched the recorded commit. Unrelated dirty
paper work was preserved; no paper file changed.

Implementation/review history: [PR261](https://github.com/oshaughnessy-junior/research-projects-RIT/pull/261),
branch `codex/q-reflected-pregrid`, merge `552d84b9`; shared JAX wiring
[PR274](https://github.com/oshaughnessy-junior/research-projects-RIT/pull/274),
branch `claude/jax-q-time-pregrid-wire`, merge `b8200920`. Merged PRs and their
source-reported adversarial review are distinguished from formal review/CI
outcomes. RIFT base remains `1621bb6e`; exact identities/hashes are retained.

## Reuse and bounded validation

`roboto_pregrid_adapter.py` calls native `core.build_likelihood_data` with
`q_time_pregrid_factor=8`; that calls the shared native
`factored_likelihood.build_reflected_q_pregrid`. It uses native cubic on both
JAX and NoLoop. No Q-pregrid transform or cubic stencil was reimplemented. Factor 16 is
only an existing-library convergence reference. Round-trip errors were <=3.64e-16.
Direct API calls and metadata assertions prove the selected method; the paper's
legacy PR195 named-string/nearest pitfall is not assumed away.

The physical interval, 614-node cadence, 512/1024 support guards and raw log-integral
convention are unchanged. Normalized uniform-time output still adds 1.8994012307831323,
with unchanged gradient. Both sides use the frozen 32/64/128 quadrature grids;
no coarse Simpson answer, terminal-rule mismatch or per-row fallback is forced
into agreement. This validates the research contract, not every canonical/default
adaptive endpoint.

Five existing oracle/reflection/fourth-order/gradient/metadata regressions passed
in float64. An initial five-pass run emitted float32 warnings; it was not accepted
as the precision gate and its cost remains charged. The three physical points
then passed all-six derivative grid/guard/method checks, Q8->Q16 component checks,
and independent NoLoop mixed-direction finite differences at h and h/2.
Thresholds stayed 1e-3 nat, .01+.001*abs(g_ref), with reference quality targets
1e-4 and .001+.0001*abs(g_ref).

| Point | Q8 reconstructed-vs-direct error, nat | Q8-minus-Q16, nat | Maximum component gradient error / tolerance |
|---|---:|---:|---:|
| Reconstructed failure |2.94e-8|-4.90e-8|.0122|
| Earlier parity0 |1.84e-5|-9.68e-5|.429|
| Earlier parity3 |-1.50e-5|-5.95e-5|.529|

Every ratio is below 1. The old failure remains a **reconstructed**, not originally
captured point. Its old coarse-sinc mismatch 1.347e-3 is preserved; this new recipe
reduces the same-point mismatch to 2.94e-8. Original NaN interpretation remains
**deliberate certificate refusal with finite physical primitives**. Old pilot
values, ESS~1 outcomes and incomplete cache population were not repaired or relabeled.

## Cost, limits and next decision

Pre-dispatch journal: **60/64 work equivalents**, 24 endpoint assemblies plus 36 JVP
directions. It separately records 1,428,342 physical time nodes, 1,524,174 directional
node equivalents, Q-transform allocation/round-trip telemetry, and FFT/reduction
work. All setup, JIT, gradients, refinement and precision retry cost is included in
**132.16 measured CPU seconds / 158.56 command wall seconds**, one CPU, peak
**1809908 KiB (1.73 GiB)**. Both 600-CPU-second and 8-GiB limits were respected; 100 CPU
seconds were conservatively reserved for incompletely timed source/coordination
work, not claimed as a measurement. No GPU, scheduler job, install or shared-runtime
change occurred. Detailed receipts/private-artifact hashes remain in results/stage8/;
raw scientific arrays stay on Caltech.

This is three-point local agreement, not a new full-frequency oracle for this
real case, full per-coordinate FD, production sweep, GPU gate, posterior/evidence
calibration or performance result. Q8/Q16 share a reflected-boundary family.
Production exposure remains unmeasured. The original core/default policies remain
untouched in this stage; no new production PR was opened. No new-head CI run exists.

**Next meaningful step:** use explicit Q8/cubic consistently for a small, separately
budgeted full reward/value-gradient bridge recheck, including both lookaheads and
the existing certificate behavior, before deciding on another matched-cost oracle
pilot. The old checkpoints and cache were built with the old numerical recipe;
they do not gain calibration by relabeling. No automatic training or inference
continuation follows from this gate.

Identified cumulative receipts: **1295.28 CPU seconds (21.59min)**,
**1461.35 summed command wall seconds**, **9996 actual real endpoint dispatches**;
42 directional AD lanes have been separately journaled since stage7. Earlier row
units are not retrospectively converted to equivalent work. Historical teacher
acquisition remains >=15,984,560 sampler rows with hidden work/CPU unknown; untimed
checks, edits/admin/transfers remain additional. No full cold-cost or speedup claim.
