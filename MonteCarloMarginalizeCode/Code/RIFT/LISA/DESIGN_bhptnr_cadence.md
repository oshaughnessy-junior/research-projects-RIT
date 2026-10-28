# Explicit BHPTNR cadence conversion

This optional module is staged on RIFT PR502, exact head
`aefd9549b44977515d5e0af9128012000c8de106` (target `rift_O4d`). It builds on
the positive-mode adapter, source/SSB synthesis and explicit native time-window
layers. It changes no native generator, response, CLI, ILE or default.

The mathematical source is Paper5 `cadence_v2/cadence_conversion_v2.py` at
paper commit `0fd5d47fdeb447711ed9be861fb54f24f329ebf9`, Git blob
`bb56b87861ae89ba2354b993411357228f0739f8`. The archived registration used
1025 taps, Kaiser beta12, cutoff0.04Hz, spline degree5 and five-second output.
These choices are mandatory call arguments here; no numerical qualification
is inferred from selecting them.

The complete native array passes the existing fixed1ns time-representation
guard. A symmetric FIR is evaluated only in valid convolution mode, retaining
centers whose complete tap stencil lies in genuine native support. An explicit
degree5 or7 spline evaluates only inside that filtered support. There is no
physical extension, endpoint padding or extrapolation. The filter is centered
on native samples, so its group delay is removed by center indexing. The
returned waveform is filtered and interpolated; native_times_preserved is false.

Relative time coordinates avoid cancellation from native origins. The public
epoch uses exact rational anchor arithmetic. A conservative fixed1ns budget
includes the native-grid residual, relative and observation-origin rounding,
and multiplication/addition in both the query and consumer grids. As with
PR502 this is a representation guard, not a waveform-error bound.

Output carries hp/hc, cadence, observation epoch/reference, read-only arrays,
deep-copied source receipt, filter/interpolation choices, FIR and output array
hashes in little-endian float64, valid support and timing budget. External
mutation during evaluation is unsupported.

Local tests compare against independently constructed Kaiser/sinc coefficients,
direct valid convolution and analytic complex tones, including passband and
stopband gains and fractional output phases. Mutation checks detect omitted
filtering, a half-native-sample delay, extrapolation and false native-preservation
metadata. Five saved native-reference replays, the complete LISA CI gate and
remote/runtime qualification remain required before merge or scientific use.
The strict historical native2e-12 gates remain failed and unchanged.
