Minimal eccentric-task-force branch from 0.0.17.13
=================================================

Baseline and scope
------------------

* Repository: ``oshaughn/research-projects-RIT``.
* Base: ``cac3b3e016cb30eec5d11ad227eb0d190edc99ef`` (14 August 2026,
  commit message ``0.0.17.13``), tree ``34185160e0321a114f5dcc2f5a8b067de76a1f2e``.
* Branch: ``codex/eccentric-minimal-17.13-20261029``.
* Live O4c audit endpoint on 29 October 2026:
  ``9ea165fc30bb7de78f1824b7b54c6c646fd9fc22`` (includes #251).
* This is an adapted backport series from the base, with no merge of a later
  release branch. ``setup.py`` and ``requirements.txt`` retain their 17.13 bytes.
  Identify installations by this branch's Git commit, not version metadata alone.
* Scope: eccentric BBH coordinate fitting/sampling and grid exploration, plus
  correctness fixes needed to trust their likelihoods, samples and evidence.
  The waveform model and physical priors remain native; the new chart is opt-in.

Exact inclusion list
--------------------

Every PR number below belongs to ``oshaughn/research-projects-RIT`` and was
verified merged into O4c after the baseline. Each upstream merge SHA and merge
date is recorded in ``minimal-eccentric-17.13-provenance.json``. Adapted commits
are separated by purpose in this branch's history. CI from the intervening
release line is replaced by one focused job for this branch.

Eccentric feature and repairs:

1. `#222 <https://github.com/oshaughn/research-projects-RIT/pull/222>`_:
   invertible eccentric phase shear, sine/cosine fit features, sheared PUFF,
   frozen map/frequency propagation. Default procedure is full-e Peters;
   the PR's optional frozen EOB chart support is retained, with no event map
   shipped. This PR accounts for most of the new production code.
2. `#239 <https://github.com/oshaughn/research-projects-RIT/pull/239>`_:
   sample the scalar sheared angle inside CIP, invert at each proposed binary,
   preserve the native prior/Jacobian/importance weights and exported waveform
   parameters. The helper owns eligible stages and frequency propagation.
3. `#215 <https://github.com/oshaughn/research-projects-RIT/pull/215>`_:
   wrap only the periodic scalar, never the entire PUFF row; reject nonfinite
   spins. This repairs the demonstrated negative-aligned-spin corruption and
   empty eccentric grids. Existing transverse box reflection is retained.
4. `#233, PART ONLY <https://github.com/oshaughn/research-projects-RIT/pull/233>`_:
   generic ``--reflect-parameter`` and finished-output bound checks, required
   by #241. The new mu/random-transverse proposal mode is NOT included.
5. `#241 <https://github.com/oshaughn/research-projects-RIT/pull/241>`_:
   eccentricity reflection within resolved native bounds, force-endpoint and
   logarithmic-floor propagation, iteration counts scaled by added physical
   dimensions. Explicit count overrides retain precedence.
6. `#245 <https://github.com/oshaughn/research-projects-RIT/pull/245>`_:
   correct effective-aligned-spin inverse, including equal masses, and check
   PUFF spin cuts on the actual assigned/exported spins.
7. `#236 <https://github.com/oshaughn/research-projects-RIT/pull/236>`_:
   broad uniform early exploration, requested target prior in convergence/final
   CIP groups, and normalized log-uniform density when sampling e-squared.
8. `#174, TESTS ONLY <https://github.com/oshaughn/research-projects-RIT/pull/174>`_:
   regression tests for intrinsic priors. The native log-e prior runtime fix
   was already present in 17.13; it is not a second runtime change here.

Other correctness changes included, explicitly:

9. `#210 <https://github.com/oshaughn/research-projects-RIT/pull/210>`_:
   fresh waveform-parameter state per conversion row. Coupled setters must not
   borrow spins or other state from preceding rows.
10. `#187 <https://github.com/oshaughn/research-projects-RIT/pull/187>`_:
    stabilize vectorized time integration per extrinsic row; a loud batch
    member must not underflow other finite likelihoods to minus infinity.
11. `#171, PART ONLY <https://github.com/oshaughn/research-projects-RIT/pull/171>`_:
    AV threshold clamp, named empty-volume error, collapse diagnostics and
    corresponding ILE failure attribution. New extrinsic zoom-box options,
    cosine-sampler changes and the rest of that PR are NOT included.
12. `#224 <https://github.com/oshaughn/research-projects-RIT/pull/224>`_:
    AV density from actual proposal grids, multiplicities for pinned variables,
    counted rejected draws and prior-box support. The small EOS-driver
    enforce-bounds call is retained with its regression; no EOS implementation
    or new sampler is imported.
13. `#218 <https://github.com/oshaughn/research-projects-RIT/pull/218>`_:
    fair systematic CIP posterior resampling; final group uses the fair unique
    draw bound. Weighted sampling without replacement followed by truncation
    biases the delivered posterior toward the proposal.
14. `#231 <https://github.com/oshaughn/research-projects-RIT/pull/231>`_:
    log-space CIP evidence and uncertainty annotations; write ln Z rather than
    ln(ln Z). The synthetic eccentric sampling regression requires this fix.
15. `#232 <https://github.com/oshaughn/research-projects-RIT/pull/232>`_:
    preserve extended precision in the Monte Carlo uncertainty helper.
16. `#175 <https://github.com/oshaughn/research-projects-RIT/pull/175>`_:
    TEOBResumSDALI GWSignal mode-sign correction, relevant to cross-code
    polarization conventions. It changes the old TEOB convention, so compare
    historical results using the documented polarization mapping.
17. `#181 <https://github.com/oshaughn/research-projects-RIT/pull/181>`_:
    correct exported time labels while preserving 17.13 likelihood sample
    origin/count. This does not import the later time-integration algorithm.

One direct O4c commit, NOT a GitHub PR:

* `3624cf0ea <https://github.com/oshaughn/research-projects-RIT/commit/3624cf0ea4eef9b5a9442e30441f2e929df22c1f>`_:
  apply source redshift once to input mass columns. The old code scaled output
  rows, corrupting eccentricity and shear predictions at nonzero redshift.
  This arrived in O4c's rc4 integration, not through one of the numbered PRs
  above. Regression tests compare the actual scalar/vector conversions.

Backport-specific adaptations are limited to 17.13 insertion points and API
availability: omit RF/geometric4-only guards, connect the new fit extractor to
both original and mirrored training rows, retain sheared sampler argument
names when integrating #231, combine AV density accounting with #171's guard,
and retain spin optimizations when removing the old redshift rescale. Two
source-location test anchors were adapted to the older layout. An extended
precision test constructs its large finite fixture from a decimal string to
avoid a host NumPy scalar-power overflow before the helper is called.
The source-distribution manifest explicitly includes the coordinate README
and this branch inclusion/provenance report. No package was uploaded to PyPI.

Deliberately excluded and conditional additions
---------------------------------------------

* No geometric4/RF/mu-spin feature stack (#212, #213, #214, #216, #250),
  aligned-Phenom/precessing frame ports (#183, #192, #226, #227, #234),
  new waveform families (#180, #228), GPU performance stack (#223, #229),
  portfolio/Rimsky/interleave changes, calibration, container redesign, or
  terminal-evidence workflow is imported.
* No new detector-time interpolation/quadrature/Q-refinement package
  (#153, #182, #242). The time-integration scheme remains 17.13's, apart from
  the per-row numerical stabilization and export-label corrections above.
  This branch is not an IR1 likelihood-accuracy signoff.
* #237 is required IF initializing from eccentric Bilby/PESummary posteriors:
  it preserves e/anomaly and checks their frequency labels. It is excluded
  from the native/manual-grid minimum here.
* Existing 17.13 Asimov adapter is retained. A modern Asimov 0.8/Bilby-bootstrap
  deployment needs its separately reviewed compatibility chain (#176/#194,
  #237 and applicable #244/#246/#247 repairs). Current Asimov end-to-end support
  is not claimed for this branch; direct helper/pseudo-pipe/DAG generation is
  tested. Do not assume the latest runtime image selects this branch's code.
* Live-head #251 repairs EOS RF-coordinate fref, and is excluded because this
  branch does not add that RF-coordinate feature.
* This does not regularize PUFF's historical singular-covariance fallback,
  choose an optimal PUFFfactor/force-away, or establish posterior convergence.
  Those are distinct from the demonstrated row-wrap and spin-inverse failures.

Use
---

For an eccentric pseudo-pipe run with native eccentricity/anomaly input,
request ``--assume-eccentric --use-meanPerAno --eccentric-phase-sampling``.
Eligible CIP stages and PUFF share a frozen map/frequency decision; the early
anomaly-nofit stage remains native. An explicit fixed interval, such as native
10 Hz to target 20 Hz, may be supplied with
``--eccentric-phase-reference-frequency 10 --eccentric-phase-target-frequency 20``
only when those labels match the selected waveform backend's native epoch.
These chart labels do not change waveform generation or detector fmin.

Use ``--manual-extra-puff-args='--fail-if-empty'`` to activate the existing
fail-empty safeguard in generated PUFF commands. No new automatic policy for
PUFFfactor or force-away is imposed by this branch.

Validation
----------

Run ``bash .travis/test-minimal-eccentric.sh`` using the intended RIFT runtime.
The added GitLab unit-test job runs this same command. Upstream test code is
retained, including actual CLI XML round trips, actual CIP fit/sampling/evidence
and native-XML checks, and real generated DAG/child submit-file assertions.
The modern Asimov/pe-configurator handoff fixture is retained separately but is
not part of this branch's claimed validation scope.

Final gate on 29 October 2026: **372 passed, 4 skipped, 8 subtests passed**
in 158.90 seconds. The four skips are two actual GPU tests (no CuPy/device)
and two unavailable waveform-plugin tests (TEOBResumSDALI and SEOBNRv5EHM). Source parsing and whitespace
checks also pass (preserving existing CRLF in the small EOS-driver hunk).

Runtime: macOS x86_64 process, Python 3.13.1, imported NumPy 2.2.4,
SciPy 1.15.1, lal 7.6.1 and lalsimulation 6.1.0. This runtime has stale package
metadata for NumPy/LALSuite, so module versions and paths, rather than only
pip distribution labels, identify what the tests actually imported. Test code
was loaded from this branch with PYTHONPATH. The isolated test runner's pytest
9.2.0 was installed in the separate audit directory, without changing a
production environment.

The gate executes synthetic CIP/AV posterior-and-evidence recovery, native XML
round trips, curved-band PUFF, prior dispatch in native/e-squared coordinates,
row ordering/redshift tests, and real pseudo-pipe/helper/BasicIteration DAG
builds including convergence children. This is a CPU functional and analytic
gate. No detector-data likelihood campaign, real-GPU gate, full PE convergence
study, NumPy-1 runtime gate, or modern Asimov end-to-end campaign was run.

Detailed logs and port receipts are kept outside Git in
``/Users/rossma/LVK/eccentric-minimal-17.13-audit-20261029``. Intermediate logs
contain superseded failures while adapting the series; ``pytest-final.log``
is the acceptance run.

Requested follow-up: aligned eccentric PUFF and diagnostics
---------------------------------------------------------

PR #249 (open against O4d, pinned head
``ee51fc729ab3641c84857aa676d783373750321d``) is adapted onto this branch.
It adds native ``--unwrap-parameter meanPerAno`` with ``--unwrap-center gap``
(default) or ``mean``, plus ``plot_RIFT.py`` with HTML and PUFF-health diagnostics.
Unwrapping is optional and is an alternative to the sheared mean-anomaly PUFF
chart: requesting both for meanPerAno fails explicitly. Proposals are wrapped
before native range cuts/export; eccentricity reflection and actual-spin cuts
are retained. Broad circle coverage emits the upstream warning rather than
claiming an efficiency improvement. No new waveform, prior, precessing
feature, or inference policy is introduced.

The plotting script did not exist in 17.13, so the upstream file is added.
Its external corner-plot call runs in the requested output folder; otherwise
its plotting/diagnostic behavior is retained. Scope is the aligned eccentric
task-force use case, not general precessing/LISA certification.

Requested commit ``8cd56cf8909a89e36f7e8b05809f039b5d3a6679`` was already
implemented by the #245 port. Its effective-spin inverse, actual-spin
PUFF selection, scalar/vector parity, and strengthened exported-spin-bound
regressions remain present; no duplicate fix is applied.

The focused adversarial gate executes the real native PUFF CLI for seam
crossings under both centering rules, negative aligned spins, periodic output
cuts, broad-circle warnings, incompatible requests and eccentricity reflection.
It exercises actual diagnostic functions for aligned likelihood association,
missing labels and embedded HTML images, and reruns all 14 spin-fix cases.
No production inference, convergence claim or precessing extension is part
of this follow-up. Exact follow-up results accompany the CIT definition.

Requested sky-export correction: PRs #195 and #196
------------------------------------------------

The 30 October follow-up includes O4c #195 and #196. Batch and non-batch ILE
map sampled network-frame angles to physical equatorial coordinates with the
inverse frame for likelihood evaluation and export. The non-batch driver now
also rotates saved samples back before standard output serialization.
The helpers explicitly name each transformation direction; the inverse is an
independent contiguous transpose of the orthonormal frame, avoiding np.matrix.
No sampling/prior or waveform changes are included.

The regression gate includes the upstream frame/round-trip/time-delay tests
and executes both drivers' actual setup/export blocks. The latter checks
likelihood/export agreement, a constant H1-L1 time-delay ring and unchanged
samples when network coordinates are off. The previously recorded legacy
skymap combinations remain outside this narrow backport; see upstream #196.
The CIT handoff is separately pinned to the updated commit; the older
PR249-only handoff is preserved for provenance.

Local CPU validation of this follow-up: 26 passed (8 sky mapping/export
regressions plus 18 adjacent time-integration/export regressions). Both drivers'
help/startup and the CIT source-overlay checks are recorded with the new
handoff. No production PE or new GPU inference was launched.
