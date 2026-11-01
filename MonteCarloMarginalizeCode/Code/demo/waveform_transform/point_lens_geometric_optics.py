"""DEMO ONLY -- not a validated lensing model, not for production inference.

Isolated point-mass lens in the GEOMETRIC-OPTICS limit, two images, for ILE's
extra-parameter seam (``--supplementary-waveform-transform-code
demo/waveform_transform/point_lens_geometric_optics.py``).

Grid columns: ``lens_mass_z`` (redshifted lens mass, solar masses) and ``lens_y``
(impact parameter in Einstein radii, > 0).

    mu_+- = 1/2 +- (y^2 + 2) / (2 y sqrt(y^2 + 4))
    Delta t = 4 G M_Lz / c^3 * [ y sqrt(y^2+4)/2 + ln((sqrt(y^2+4)+y)/(sqrt(y^2+4)-y)) ]
    F(f) = sqrt|mu_+| + exp(+i pi/2) sqrt|mu_-| exp(-2 pi i f Delta t)      (f >= 0)

Convention: standard sign, H(f) = int h(t) exp(-2 pi i f t) dt, so the second (type-II,
saddle-point) image arrives LATER by Delta t; the brighter image is undelayed (minimum
image delay zeroed).  This is the complex conjugate of Takahashi & Nakamura (2003) eq. 17,
which uses exp(+2 pi i f t).  The sign of the Morse phase in this convention is an open
item: pin it against a time-domain reference before using this for physics.

Validity: geometric optics needs 8 pi G M_Lz f / c^3 >> 1 across the band, i.e. roughly
M_Lz >~ 1e3 Msun at 20 Hz.  Wave-optics F(w, y) (1F1) is not implemented here.
Delta t must be well inside the analysis segment and the ILE time window
(multiplication in the frequency domain is a cyclic convolution).
"""
import numpy

NAME = "point_lens_geometric_optics_demo"
EXTRA_PARAMETERS = ["lens_mass_z", "lens_y"]

_G_OVER_C3_MSUN = 4.925490947641267e-06   # G Msun / c^3 [s]


def image_parameters(lens_mass_z, lens_y):
    y = float(lens_y)
    if not y > 0:
        raise ValueError("lens_y must be > 0, got %r" % lens_y)
    s = numpy.sqrt(y * y + 4.0)
    mu_p = 0.5 + (y * y + 2.0) / (2.0 * y * s)
    mu_m = 0.5 - (y * y + 2.0) / (2.0 * y * s)
    dt = 4.0 * _G_OVER_C3_MSUN * float(lens_mass_z) * (0.5 * y * s + numpy.log((s + y) / (s - y)))
    return mu_p, mu_m, dt


def transfer_function(f, extra_params, P=None, xpy=numpy, **kw):
    mu_p, mu_m, dt = image_parameters(extra_params["lens_mass_z"], extra_params["lens_y"])
    f = numpy.asarray(f, dtype=float)
    return (numpy.sqrt(abs(mu_p)) + 1j * numpy.sqrt(abs(mu_m)) * numpy.exp(-2j * numpy.pi * f * dt)).astype(complex)

