# Bounded engineering validation, 2026-10-29

Base: O4d `03d7fe8467d0216f72c313498603fceaf434d31e`.
The requested PR181 is
`oshaughnessy-junior/research-projects-RIT#181`, head
`a671e425ef3523f607e4e7b9b00201deaa4971d3`, base
`152407e85f88a1bc35f1327b239fe75515f3e70e`.
Live GitHub compare of that head to the pinned O4d base reports **diverged**:
247 commits ahead, 82 behind, common ancestor equal to that PR base.
It was not merged or cherry-picked. The current O4d native Hyperpipe
`drivers/base.py`, `coords.py`, `marg_list.py`, `util_RIFT_hyperpipe.py`
and `create_eos_posterior_pipeline` supply the arbitrary-coordinate architecture
used here. The lens driver adapts to these current interfaces.

The real DAG build caught two concrete integration problems: the current
Hydra driver pre-stages event files where the downstream builder then tries
to copy them onto themselves; the downstream final-worker default is two
unless explicitly overridden. The scoped changes skip only same-file copies
and explicitly select one final worker, retaining legacy copies/errors.

Local independent acceptance checks exercise normalized L=1 priors, exact
Gaussian integral and joint posterior moments, isotropic inverse CDF, invalid
quantiles, schema validation and fail-closed terminal collection. Native
sampler results below are separate from these deterministic seam checks.

Remote validation root (retained, not committed):
`cardassia:/home/oshaughn/rift-lensing-prototype/work/20261029-hyperpipe-native-runtime`.
Runtime: existing `rift-lensing-event-demo/bin/python`, NumPy1.26.4; optional
Hydra/scikit-learn/glue dependencies are staged under `compatible/`, without
changing the environment. A pip attempt also downloaded NumPy2 into isolated
`deps/`; it is excluded from `compatible/` and is not part of the inference
lane. `PYTHONPATH` adds `compatible`, the read-only pinned O4d Code directory,
the validation root, and the owning prototype's `src` for physical waveforms.

Actual native EOS_POST RF+AV control: 4096 seed rows from the production joint
driver, unit-box prior bounds, RF seed347 (the first sampler RNG was not explicitly seeded),
n-max100000,n-step10000,n-eff2000,
`--internal-use-lnL`. `run-native-eos-control.sh` records the command with explicit sample outputs;
the initial first run used the native default sample path, which was then
copied into its scoped result directory. The host-specific exact launchers
`rift-native-control.py`, `run-native-eos-control.sh`, and
`rift-second-seed.py` are retained next to this report.
The Gaussian has source x uniform[2,8], lens y log-uniform[.2,20],
lnL=-.5[(x-5)^2/.65^2+(log(y)-log(2))^2/.45^2].
Its exact normalized lnZ is -2.710356875295474.

* `gaussian/native-result`: lnZ=-2.7166055333934622 (error -.00624866nat),
  effective sample estimate3034.616, posterior ESS6059.328.
* The independent 2000-draw posterior check passes: means
  [5.003001808154449,.680115091322078], covariance
  [[.4216376441958916,.003743908038601144],
  [.003743908038601144,.2047347497964037]].
* `constant/native-result`: lnZ=0 exactly, neff=ESS10000. This is the real
  native sampler L=1 normalized prior test, not just quadrature.
* `gaussian-seed2/` uses numpy sampler seed991 and RF seed991; its exact
  launcher is `rift-second-seed.py`. Independent acceptance is recorded in
  the test/hyperpipe acceptance notes. Its lnZ=-2.71153962780717,
  error=-.001182752511696nat; its means are
  [5.004428648839601,.6921787361431133] and covariance diagonals
  [.43615915301565467,.1947568476407829].

The native Gaussian DAG was actually generated in `gaussian-dag/run/` and
independently inspected: four iterations, one worker in each, normalized
unit ranges, AV/RF, UNIFY->EOS_POST->JOIN_POST, final JOIN_POST->CONVERGE.
It was not submitted and the four-iteration inference loop was not run.
The direct native EOS_POST controls above exercise interpolation plus sampling,
not the entire iterative DAG execution.

Physical validation used the existing public GW231123 highpass10 prepared
bundle and IMRPhenomXPHM. The generated manifests contain 17 lensed dimensions
and 15 null dimensions. Both native DAGs exist in
`physical-lensed/run/` and `physical-unlensed/run/`; neither was submitted.
The nonprofile physical row at all prior medians gives lnLR110.38096326674551
(null),135.56876728142456(lensed), and amplitude-zero gives exactly0 for
both. `physical-row.json` records these checks. These are evaluations, not
posteriors or evidence. They establish an executable physical likelihood seam
and common noise reference, not physical inference accuracy.

Remaining admission work: execute iterative source+lens inference, validate
held-out waveform/interpolation residuals, posterior shapes and repeated
converged evidence under scientifically selected priors. Full event PE,
calibration/systematic checks, posterior interpretation, an event Bayes factor,
and signed-mode native NoLoop integration are not established by this pass.
