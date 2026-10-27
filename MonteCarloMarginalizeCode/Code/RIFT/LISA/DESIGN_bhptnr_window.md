# Native BHPTNR time windows

`select_native_window` copies a caller-selected contiguous interval from
`BHPTNRPolarizations` and assigns an explicit observation epoch through
`BHPTNREpochMapping`. Both native and observation references must name their
origin and time scale. The helper infers no merger, peak or cross-model phase
alignment. It interpolates, decimates and pads no samples.

The returned `BHPTNRWindow` carries polarizations, the original selected native
times, endpoint cadence, observation start epoch and reference, sample count,
and a deep-copied metadata receipt. Its sampling fields have the same shape
as the other optional polarization adapters, without adding a CLI model or
changing detector-response guards. The caller must select the proper frame
and a separately qualified cadence and response configuration. A contiguous
window does not provide arbitrary-duration EMRI support.

A fixed one-nanosecond budget checks representation as an affine time grid.
Endpoint cadence is computed with exact rational subtraction of float64 native
timestamps. Every selected timestamp is then compared with the exact relative
native grid using `Fraction`. Constructing that grid at a large native origin
would hide nonuniformity through cancellation; a regression tests that failure.
The observation epoch is likewise formed with exact anchor arithmetic before
measuring its float64 roundoff. Conservative bounds cover multiplication and
addition in a consumer's `epoch + arange(N)*dt` grid. The sum of native residual,
epoch rounding and absolute-grid rounding must remain within the fixed budget.
Unrepresentable epochs and cadences are refused. This is a time-representation
bound, not a bound on waveform error or detector-response error.

Arrays are copied, converted to float64, validated at that precision and marked
read-only. Input receipt count, endpoints, complex convention and native-time
preservation flag must agree. Metadata preserves frame and warnings, records
anchor mapping and timing terms, and hashes the selected normalized arrays in
little-endian order. Receipt consistency does not authenticate arbitrary
caller-constructed objects. External mutation during selection is unsupported.
Read-only NumPy flags protect against accidental writes, not deliberate mutation.

The mathematical origin is the archived Paper5 contiguous-window helper; the
new interface normalizes mapping scalars and fixes native-origin cancellation
in the timing-bound calculation. Tests cover exact sample preservation, rational
anchor mapping, hashes, copies, invalid support/metadata/precision and large
native or observation origins. No waveform generator or native response backend
is imported or evaluated by this layer.
