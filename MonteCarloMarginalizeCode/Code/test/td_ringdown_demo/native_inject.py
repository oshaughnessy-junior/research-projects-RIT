#!/usr/bin/env python
"""native_inject.py -- RIFT-native, self-consistent injection for the T7 segment demo (T7b).

WHY THIS EXISTS (the T7 finding).
The original T7 demo injected with ``bin/util_LALWriteFrame.py``, which builds the detector
strain from ``lalsimutils.hoft`` -> ``SimInspiralChooseTDWaveform`` -- a DIFFERENT waveform
generation (its own tapering/centering) than the likelihood templates, which come from
``internal_hlm_generator`` -> ``std_and_conj_hlmoff`` == ``DataFourier(hlmoft(P))``.  For the
few-cycle high-total-mass 50+50 signal that cross-tool conditioning mismatch left the injected
data NOT exactly equal to the RIFT template at the injected masses, so the distance-maximized
statistic ``0.5 <d|h>^2/<h|h>`` (which provably peaks at ``h = d`` by Cauchy-Schwarz) peaked a
few % off the true total mass (see analyses/t7_ringdown_demo/REPORT.md).

THE FIX (this tool).  Build the injected strain from THE SAME modes the templates use:

    hlms_td = lalsimutils.hlmoft(P, Lmax)                 # the modes templates FFT
    h_det   = lalsimutils.hoft_from_hlm(hlms_td, P)       # RIFT-native detector projection

``hoft_from_hlm`` sums the modes with ``SpinWeightedSphericalHarmonic(incl, -phiref, ...)`` --
identical to ``ComputeYlms(Lmax, incl, -phiref)`` used in ``SingleDetectorLogLikelihood`` -- and
applies ``SimDetectorStrainREAL8TimeSeries(hp, hc, phi, theta, psi, detector)``, i.e. the same
LAL antenna pattern + geocenter->detector time delay that RIFT's ``ComplexAntennaFactor`` /
``ComputeArrivalTimeAtDetector`` wrap.  Because the template FD modes are literally
``DataFourier(hlmoft(P))``, the injected data equals the truth template to machine precision in
the inner-product algebra, so the distance-maximized lnL surface peaks at the injected
intrinsics (validated by the T7 gate in run_gate.py).

Epoch / conditioning conventions matched to the T4/T7 harness:
  * deltaT = 1/srate, deltaF = 1/seglen (so hlmoft forces the SAME T=1/deltaF the templates
    use, hence the SAME start-taper turn-on and centering).
  * fmin = fref (the harness make_P convention); taper = TAPER_START; radec = True.
  * The projected strain is placed in absolute GPS so its geocenter coalescence is at P.tref;
    the analysis window [start, start+seglen] then covers it exactly as the harness reads it
    with frame_data_to_non_herm_hoff(..., start, stop).

Frames are written FAKE-STRAIN (channel ``IFO:FAKE-STRAIN``) via hoft_to_frame_data, and a
frames.cache with absolute file:// paths is emitted -- the exact inputs
run_deterministic_surfaces.py / run_gate.py expect.  Optional colored Gaussian noise
(reproducible per --noise-seed) uses the same smooth analytic PSD as the likelihood
(make_noise_frames.py recipe): zero-noise for the gate, a fresh realization per seed for pp.

Usable as a CLI or imported (``native_inject(...)`` returns the cache path).
"""
import os
import argparse
import numpy as np

import lal
import RIFT.lalsimutils as lalsimutils

HERE = os.path.dirname(os.path.abspath(__file__))


def analytic_psd(f):
    """Smooth analytic one-sided PSD -- IDENTICAL to make_noise_frames / the demo PSD xml."""
    fsafe = np.maximum(np.abs(f), 1.0)
    return 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)


def build_injection_P(m1_msun, m2_msun, dist_mpc, incl, phiref, theta, phi, psi,
                      tref, fmin, fref, approx, srate, seglen, detector):
    """ChooseWaveformParams for the injection, matching the harness make_P conventions."""
    P = lalsimutils.ChooseWaveformParams()
    P.m1 = m1_msun * lalsimutils.lsu_MSUN
    P.m2 = m2_msun * lalsimutils.lsu_MSUN
    P.s1x = P.s1y = P.s1z = 0.0
    P.s2x = P.s2y = P.s2z = 0.0
    P.dist = dist_mpc * 1e6 * lalsimutils.lsu_PC
    P.fmin = fmin
    P.fref = fref
    P.tref = tref
    P.deltaT = 1.0 / srate
    P.deltaF = 1.0 / seglen           # forces hlmoft T = 1/deltaF = seglen (template T)
    P.approx = lalsimutils.lalsim.GetApproximantFromString(approx)
    P.radec = True                    # real source + real instrument (detector projection)
    P.taper = lalsimutils.lsu_TAPER_START
    P.incl = incl
    P.phiref = phiref
    P.theta = theta                   # DEC
    P.phi = phi                       # RA
    P.psi = psi
    P.detector = detector
    return P


def native_detector_strain(P, lmax=2):
    """RIFT-native detector strain h(t) from the SAME hlmoft modes the templates use.

    Returns a REAL8TimeSeries whose epoch is the GPS time of sample 0 (the detector-frame
    coalescence lands at ~ P.tref + detector_delay inside it).

    Convention -- built to match ``SingleDetectorLogLikelihood`` EXACTLY:
      * mode sum with ``SpinWeightedSphericalHarmonic(incl, -phiref, ...)`` == ComputeYlms;
      * hp = Re(sum), hc = -Im(sum)  (the hoft_from_hlm convention, validated single-detector);
      * antenna Fp,Fc from ``lal.ComputeDetAMResponse(..., GreenwichMeanSiderealTime(tref))``
        == the real/imag parts of ``ComplexAntennaFactor(det, RA, DEC, psi, tref)``;
      * the geocenter->detector light-travel delay is applied as an INTEGER number of samples
        (``round(delay/deltaT)``) rather than lal ``SimDetectorStrain``'s sub-sample Fourier
        shift.  This is the load-bearing difference: the factored likelihood looks up rhoTS at
        the arrival sample ``round((t_det - epoch)/deltaT)`` (FL:1936-type integer lookup), so a
        sub-sample injection delay leaves an H1-vs-L1 DIFFERENTIAL timing error that a common
        time-marginalization cannot absorb -- for a few-cycle high-mass signal that biases the
        recovered mchirp/eta by ~1 grid step (the residual T7 offset).  Integer-sample delays on
        both detectors make the injection land on the SAME samples the likelihood reads, so the
        distance/fixed-distance surface peaks exactly at the injected intrinsics (gate).
    """
    hlms = lalsimutils.hlmoft(P, lmax)
    if not isinstance(hlms, dict):
        hlms = lalsimutils.SphHarmTimeSeries_to_dict(hlms, lmax)
    h22 = hlms[(2, 2)]
    dt = h22.deltaT
    N = h22.data.length
    # geocenter complex mode sum  hT(t) = sum_lm Ylm(incl,-phiref) h_lm(t)
    hT = np.zeros(N, dtype=complex)
    for mode, hlm in hlms.items():
        Ylm = lal.SpinWeightedSphericalHarmonic(P.incl, -P.phiref, -2, mode[0], mode[1])
        hT += np.asarray(hlm.data.data) * Ylm
    hp = np.real(hT)
    hc = -np.imag(hT)
    detector = lalsimutils.lalsim.DetectorPrefixToLALDetector(str(P.detector))
    gmst = lal.GreenwichMeanSiderealTime(P.tref)
    Fp, Fc = lal.ComputeDetAMResponse(detector.response, P.phi, P.theta, P.psi, gmst)
    h_geo = Fp * hp + Fc * hc                    # detector strain, geocenter timing
    delay = lal.TimeDelayFromEarthCenter(detector.location, P.phi, P.theta, P.tref)
    n_delay = int(round(delay / dt))             # INTEGER-sample arrival shift
    out = lal.CreateREAL8TimeSeries(
        "FAKE-STRAIN", lal.LIGOTimeGPS(float(h22.epoch) + float(P.tref) + n_delay * dt),
        0.0, dt, lalsimutils.lsu_DimensionlessUnit, N)
    out.data.data[:] = h_geo
    return out


def _place_in_window(h, start, srate, seglen):
    """Copy a REAL8TimeSeries into a fresh zero series spanning [start, start+seglen)."""
    dt = 1.0 / srate
    N = int(round(seglen * srate))
    out = lal.CreateREAL8TimeSeries(
        "FAKE-STRAIN", lal.LIGOTimeGPS(float(start)), 0.0, dt,
        lalsimutils.lsu_DimensionlessUnit, N)
    out.data.data[:] = 0.0
    # integer sample offset of h's sample 0 within the window
    i0 = int(np.round((float(h.epoch) - float(start)) / dt))
    hdata = np.asarray(h.data.data)
    Lh = hdata.shape[0]
    # source slice [s0:s1) of h that lands inside [0:N) of out
    dst0 = max(0, i0)
    dst1 = min(N, i0 + Lh)
    if dst1 > dst0:
        src0 = dst0 - i0
        src1 = src0 + (dst1 - dst0)
        out.data.data[dst0:dst1] = hdata[src0:src1]
    return out


def _add_colored_noise(series, seed, fmin, fmax):
    """Add band-limited PSD-colored Gaussian noise in place (make_noise_frames recipe)."""
    rng = np.random.default_rng(int(seed))
    x = np.asarray(series.data.data)
    N = x.shape[0]
    dt = series.deltaT
    fs = 1.0 / dt
    df = fs / N
    f = np.fft.rfftfreq(N, d=dt)
    S = analytic_psd(f)
    band = (f >= fmin) & (f <= fmax)
    sigma = np.zeros_like(f)
    sigma[band] = np.sqrt(S[band] / (4.0 * df))
    ntilde = (rng.standard_normal(len(f)) + 1j * rng.standard_normal(len(f))) * sigma
    ntilde[0] = ntilde[0].real
    if N % 2 == 0:
        ntilde[-1] = ntilde[-1].real
    noise = np.fft.irfft(ntilde, n=N) * (N * df)
    series.data.data[:] = x + noise
    return float(np.sqrt(np.mean(noise ** 2)))


def native_inject(m1_msun, m2_msun, dist_mpc, incl, phiref, theta, phi, psi,
                  tref, out_dir, out_cache, instruments=("H1", "L1"),
                  fmin=25.0, fref=25.0, approx="IMRPhenomD", srate=2048.0,
                  seglen=8.0, start=None, noise_seed=None, fmax=1000.0, lmax=2,
                  verbose=True):
    """Write native (self-consistent) injection frames + cache; return the cache path.

    ``noise_seed=None`` -> zero-noise (the gate).  An int -> a fresh reproducible colored
    Gaussian realization added on top (pp).  ``start`` defaults to tref - seglen/2.
    """
    if start is None:
        start = float(tref) - seglen / 2.0
    os.makedirs(out_dir, exist_ok=True)
    lines = []
    for ifo in instruments:
        P = build_injection_P(m1_msun, m2_msun, dist_mpc, incl, phiref, theta, phi, psi,
                              tref, fmin, fref, approx, srate, seglen, ifo)
        h = native_detector_strain(P, lmax=lmax)
        series = _place_in_window(h, start, srate, seglen)
        rms = None
        if noise_seed is not None:
            # distinct per-detector stream: offset the seed by a per-IFO constant
            ifo_seed = int(noise_seed) + (0 if ifo == instruments[0] else 101)
            rms = _add_colored_noise(series, ifo_seed, fmin, fmax)
        tstart = int(round(float(series.epoch)))
        duration = int(round(series.data.length * series.deltaT))
        tag = "native_strain" if noise_seed is None else "native_strain_noise"
        fname = "%s-%s-%d-%d.gwf" % (ifo[0], tag, tstart, duration)
        fpath = os.path.abspath(os.path.join(out_dir, fname))
        lalsimutils.hoft_to_frame_data(fpath, ifo + ":FAKE-STRAIN", series)
        lines.append("%s FAKE %d %d file://localhost%s\n"
                     % (ifo[0], tstart, duration, fpath))
        if verbose:
            d = np.asarray(series.data.data)
            ipk = int(np.argmax(np.abs(d)))
            print("[native-inj] %s wrote %s  N=%d peakGPS=%.4f max|h|=%.3e%s"
                  % (ifo, fpath, series.data.length,
                     float(series.epoch) + ipk * series.deltaT, np.max(np.abs(d)),
                     "" if rms is None else (" noise_rms=%.3e" % rms)))
    with open(out_cache, "w") as fh:
        fh.writelines(lines)
    if verbose:
        print("[native-inj] wrote cache", out_cache)
    return out_cache


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--m1", type=float, default=50.0, help="mass 1 [Msun]")
    ap.add_argument("--m2", type=float, default=50.0, help="mass 2 [Msun]")
    ap.add_argument("--dist", type=float, default=700.0, help="distance [Mpc]")
    ap.add_argument("--incl", type=float, default=0.7)
    ap.add_argument("--phiref", type=float, default=1.2)
    ap.add_argument("--theta", type=float, default=0.3, help="DEC [rad]")
    ap.add_argument("--phi", type=float, default=2.1, help="RA [rad]")
    ap.add_argument("--psi", type=float, default=0.8)
    ap.add_argument("--tref", type=float, default=1000000000.0)
    ap.add_argument("--fmin", type=float, default=25.0)
    ap.add_argument("--fref", type=float, default=25.0)
    ap.add_argument("--approx", default="IMRPhenomD")
    ap.add_argument("--srate", type=float, default=2048.0)
    ap.add_argument("--seglen", type=float, default=8.0)
    ap.add_argument("--start", type=float, default=None,
                    help="frame/window start GPS (default tref - seglen/2)")
    ap.add_argument("--instruments", default="H1,L1")
    ap.add_argument("--out-dir", default=os.path.join(HERE, "frames_native"))
    ap.add_argument("--out-cache", default=os.path.join(HERE, "frames_native.cache"))
    ap.add_argument("--noise-seed", type=int, default=None,
                    help="omit for zero-noise; int -> reproducible colored-noise realization")
    ap.add_argument("--fmax", type=float, default=1000.0)
    ap.add_argument("--lmax", type=int, default=2)
    args = ap.parse_args()
    native_inject(
        args.m1, args.m2, args.dist, args.incl, args.phiref, args.theta, args.phi, args.psi,
        args.tref, args.out_dir, args.out_cache,
        instruments=tuple(args.instruments.split(",")),
        fmin=args.fmin, fref=args.fref, approx=args.approx, srate=args.srate,
        seglen=args.seglen, start=args.start, noise_seed=args.noise_seed,
        fmax=args.fmax, lmax=args.lmax)


if __name__ == "__main__":
    main()
