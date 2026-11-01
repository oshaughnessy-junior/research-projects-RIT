"""DEMO ONLY -- not a validated lensing model.

Constant-amplitude, constant-phase transfer function for ILE's extra-parameter seam
(``--supplementary-waveform-transform-code demo/waveform_transform/constant_amplitude_phase.py``).

    F(f) = lens_amplitude * exp(i lens_phase)          on f >= 0

The seam extends this to f < 0 as conj F(-f), so the applied filter is
A exp(i phi sgn f) (a real time-domain kernel: amplitude A, Hilbert-type phase phi;
phi = pi/2 is the Morse phase of a type-II image, sign convention to be pinned by test).
Sign convention: standard, H(f) = int h(t) exp(-2 pi i f t) dt.

|F| is exactly degenerate with luminosity distance (D -> D/A): state the priors on A and D
together.  Grid columns: lens_amplitude, lens_phase.
"""
import numpy

NAME = "constant_amplitude_phase"
EXTRA_PARAMETERS = ["lens_amplitude", "lens_phase"]


def transfer_function(f, extra_params, P=None, xpy=numpy, **kw):
    A = float(extra_params["lens_amplitude"])
    phi = float(extra_params["lens_phase"])
    return A * numpy.exp(1j * phi) * numpy.ones(numpy.shape(f), dtype=complex)


def is_identity(extra_params):
    return float(extra_params["lens_amplitude"]) == 1.0 and float(extra_params["lens_phase"]) == 0.0
