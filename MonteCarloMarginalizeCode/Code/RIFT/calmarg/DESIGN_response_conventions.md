# Calibration response conventions

This describes the current convention and supersedes the data-side convention
section of `DESIGN_calmarg_in_loop.md`.

## Envelope conventions and internal template responses

The envelope convention is specified per detector. The shared
`RIFT.calmarg.calibration.correction_type_for_ifo` resolver is also used by
`calibration_reweighting.py` when it passes the bilby-pipe metadata convention to
Bilby's `CalibrationPriorDict.from_envelope_file`.

ILE accepts `--calibration-correction-type data`, `template`, or a mapping such as
`H1:data,L1:data,V1:template`. The pipeline forwards
`--calmarg-correction-type` to every ILE stage. Defaults match bilby-pipe:
H1, L1 and K1 use data corrections; V1 uses template corrections. Overrides are
required when an envelope uses a different convention. Startup logs record the
resolved map.

For a data correction eta, the internal template factor is alpha = 1/eta.
Before drawing Gaussian nodes, the loader inverts amplitude medians and bounds
and negates phase medians, following Bilby's envelope-prior construction. The
Gaussian is in linear template amplitude and phase; this approximates the
transformed envelope, rather than inverting individual Gaussian random draws.
Envelope summaries are interpolated in log frequency. The response spline uses
an exponential phase, while Bilby's spline uses its rational phase parameterization.

The factored cross-term is `<h|conj(alpha)*d>`; the complete template norm is
`<alpha*h|alpha*h>`. Both use the released-strain PSD. Conjugation is enabled by
default and always selected by ILE. The old `--calibration-conjugate-phase` flag
is accepted for command-line compatibility. `--calibration-global-norm` remains
an explicit approximation that omits the amplitude-dependent norm.

Prior draws, pilot draws, exported nodes, and draw growth use the same converted
prior. Exported amplitude nodes are full template-response amplitudes near one.
Saved pilot Gaussians remain sampling proposals; their importance weights use
the current envelopes' prior. An old proposal may be inefficient after changing
conventions, but its stored prior cannot replace the live target prior.

