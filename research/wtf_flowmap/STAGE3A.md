# Real-case likelihood parity: bounded diagnostic

This stage checks a physical log-likelihood reward callback, not a posterior
sampler. No real-reward fine-tuning or corrected-inference campaign runs here.
The maintained NumPy NoLoop likelihood is the reference; the existing JAX
six-dimensional likelihood supplies derivatives. Both use the same real
waveform/strain/PSD bank and exactly the same marginalization-time grid.

## Target and coordinates

S250114ax, fixed intrinsic anchor 47536, SEOBNRv5PHM through GWSignal,
lmax 4, 4096 Hz, sinc interpolation, bandlimited time marginalization,
no phase/distance/calibration marginalization. Data/PSD windows .25,
template fmin 13.33 Hz, detector cutoffs 20 Hz, fmax 1792 Hz. Input paths and
hashes are in `results/case_inventory.json`. The physical reward is **log L**;
the prior and coordinate Jacobian are not added to it. Maximizing this
transport-regularized reward does not produce Bayesian posterior draws.

Model coordinates are standardized
`[RA_unwrapped, atanh(sin(dec)), atanh(cos(incl)), log(D), phase_unwrapped,
psi_unwrapped]`. Callback order is `[RA, dec, psi, incl, phase, D_Mpc]`.
The analytic Jacobian includes the permutation and the **negative** inclination
derivative. It is checked against JAX AD, and inversion is checked on four rows.
RA/phase/psi have periods 2pi/2pi/pi. These are local charts with seams, not a
globally differentiable bijection. The probe uses cache indices 0/100/200/300;
the repaired full-mode gradient point is index 300, from the held-out seed.

The archived driver actually uses Planck15, as does the current driver. Its
rotated angular prior is a different normalization contract: densities 1/pi
and 1/(2pi) over two [0,4pi] coordinates integrate to mass **8**. Current code
normalizes the sampled ranges. Historical log evidence therefore includes
ln(8) relative to the normalized contract; normalized posterior shapes are
unchanged by that constant. Future evidence comparisons require fresh,
matched current-code AV and AV+GMM baselines, not unadjusted historical lnZ.

The JAX builder needs .37890625 s stored half-width for its certified guard
(512 initial / 1024 certified), while the integration half-width remains
.075 s. The larger buffer does not extend the integrated time interval.
All 21 modes with 2<=l<=4 are retained per detector in the repaired run.

## Fixed gates, accounting and repair

Before evaluation, declare absolute value/periodicity tolerance 1e-3 nat;
gradient tolerance .01 + .001*abs(AD gradient) per standardized coordinate;
central finite differences at h=1e-4 and h/2 must agree with AD and each other.
Stop on the first nonfinite value, failed certificate or exceeded tolerance.
Do not filter failed rows. Every physical-likelihood row is journaled **before**
dispatch, including failed/interrupted calls and each finite-difference side.
An AD call counts one forward physical row plus an explicitly marked gradient;
its backward/JIT time is included. Waveform precompute is counted separately.

One initial adapter run inherited the low-level precompute's default
`ignore_threshold=1e-4`, unlike the maintained driver's explicit `None`.
It was interrupted when this was found, after 51 attempted rows. Its numerical
comparisons are **not** target-parity acceptance evidence. Preserve its results
and full costs. The repaired adapter passes `ignore_threshold=None` and reduces
the planned two gradient points to one, retaining the held-out point and both
backend finite-difference checks. The stage cap remains 128 attempted rows,
one affinity-pinned CPU, 1200 CPU seconds and 8 GiB; it is not reset for repair.
The repair process has a 950 CPU-second hard limit, after the initial process's
120.55 measured CPU seconds. Both processes have an 8-GiB virtual-memory hard
limit, stricter than a resident-memory limit. No scheduler job/GPU/install used.

The first adapter SHA256 was
`a30e571d36b3c0561e200f2d95a203f2e744cb7709f6e97fc9f9c9218f04d894`;
its exact copy remains in Caltech `stage3a/source/research/wtf_flowmap/`.
The repaired executable is `stage3a/parity_real_full.py`; repository
`parity_real.py` is that repaired version. Do not overwrite either run directory.

## Measured outcome

**Passed after repair**, within the original stage cap. Full-mode maximum
absolute value difference was 2.012643e-5 nat. At the held-out gradient point,
maximum componentwise AD/FD errors divided by the predeclared tolerance were
.016884 (NoLoop FD) and .001758 (JAX FD); step refinement ratios were .005293
for both. Ratios <=1 pass. All three physical periodicities passed on both
backends. Four point values and one gradient point cannot establish global
posterior-domain validity, waveform fidelity across historical backend versions,
or calibrated inference. No Torch/JAX autograd bridge has been exercised yet.

| Attempt | Interpretation | Attempted rows | Complete process CPU s | Wall s | Peak RSS KiB |
|---|---|---:|---:|---:|---:|
| run1 | stopped: wrong pruning contract | 51 | 120.55 | 130.56 | 2497220 |
| run2 | full-mode local gates passed | 65 | 169.01 | 171.59 | 2853072 |
| total | failures/setup/validation included | 116 | 289.56 | 302.15 | 2853072 peak |

These `/usr/bin/time -v` totals include interpreter imports and shutdown, frame/PSD
setup, waveform precompute (3.6442 and 2.2778 wall s), JIT and all validation.
They exceed the in-process snapshots slightly because shutdown is included.
Read-only source audits and source transfer were not timed; no exact combined
administrative-total claim. Run1 was deliberately interrupted (shell exit 130),
even though GNU time's Exit status field says 0. It is not a passed run.
Both runs are interactive on ldas-grid, CPU affinity [0]; no Condor job ID.
The full-mode process PID was not recorded; this is not represented as a batch job.
The first process PID was 1948358. Raw logs and scientific inputs remain on Caltech.

Remote root: `/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage3a`.
Logs SHA256: run1 `ba3b84e48d0e4642e3d1dc741aebf5cae3bf38064cb3aaf2ff0ef8363c0e37d7`,
run2 `d01cc13991ff1105f29e792f7374cf5fc9f1216d6fbe087ba9a5b9e1678728e3`.
The executed full-mode adapter matches repository SHA256
`6a1cb6c06c1faec197a5308430ac6974d31aeb857865502f84dd16fc420a6006`.
JAX wrapper/core hashes were verified identical remotely and locally:
`0db282efa8db272e404cc2d649485feaba3c0339998a36ddde293b7ac4166005` /
`0f499791e796558b7b2460027ef8459302925dcf792801883664ceaf56336ab8`.
Base remains `1621bb6e12391086edc3bfc8730ad807c8ad638c`; research parent
`875d52c9516cf58877aec53c1f55b284e81bfd79`. No likelihood library was edited.

Exact repaired command on Caltech (use a NEW output directory for a repeat;
a repeat requires a new budget, not the spent stage allocation):

```sh
root=/scratch/richard.oshaughnessy/codex_learned_wtf_20261015
export PYTHONPATH="$root/stage3a/source/MonteCarloMarginalizeCode/Code" PYTHONDONTWRITEBYTECODE=1
/usr/bin/time -v -o "$root/stage3a/run2.time" \
 /cvmfs/software.igwn.org/conda/envs/igwn/bin/python "$root/stage3a/parity_real_full.py" \
 --inventory "$root/stage3a/source/research/wtf_flowmap/results/case_inventory.json" \
 --checkpoint "$root/final_seed261015/pretrained.pt" \
 --training "$root/final_seed261015" \
 --previous-result "$root/stage3a/run1/result.json" \
 --output "$root/stage3a/run2" > "$root/stage3a/run2.log" 2>&1
```

Local syntax validation: `PYTHONPYCACHEPREFIX=/tmp/wtf-pycache python3 -m py_compile
research/wtf_flowmap/parity_real.py` passed. This research-directory diagnostic
is not covered by CI. PR445 remains unchanged at acf8dc1; its 34 CI jobs and
private review dispatch are all successful. The production shape-recovery
merge gate was not run or claimed for this research-only change.

## Next decision, not launched

If these local checks pass, the next useful unit is a **real-objective smoke
test**, not a cost-win campaign: bridge JAX values/gradients into the existing
Torch objective with the full chart Jacobian, verify the bridge numerically,
then at most eight updates of batch four with a frozen reference and one
predeclared small reward coefficient. Propose one Caltech CPU, 10 CPU minutes,
8 GiB and 256 attempted physical rows including bridge checks, validation,
failed candidates and any repairs. Stop before evaluating any endpoint outside
the physical prior support [1,10000] Mpc or at a chart seam; preserve the failure
rather than silently clipping coordinates or changing the objective. This is a
budget proposal, not an executed result or permission for a larger campaign.

The inference experiment follows separately: identical corrected broad-support
final sampler for pretrained-map, tuned-map, same-cache resampling/warm-start,
and strong current-code AV/AV+GMM baselines, with matched seeds and total costs.
Map points enter proposal training only. Calibration must cover evidence,
mode masses, moments/tails and ESS per total cost at held-out intrinsic anchors.
Require the existing >=20% measured cost reduction at a declared reuse count,
or repeatable scientifically relevant missed-mode recovery at matched budget.
The historical acquisition lower bound is 15,984,560 likelihood rows; missing
hidden acquisition costs preclude an unconditional break-even claim.
Do not size the inference campaign from this parity probe's cached per-row rate:
measure one corrected-sampler pilot first and predeclare its separate budget.
