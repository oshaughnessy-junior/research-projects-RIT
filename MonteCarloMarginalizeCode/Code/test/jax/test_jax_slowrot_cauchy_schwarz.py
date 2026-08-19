"""test_jax_slowrot_cauchy_schwarz : the JAX rotation likelihood must be a real <d|h> - (1/2)<h|h>.

The JAX twin of ``RIFT/likelihood/test_slowrot_cauchy_schwarz.py``, which guards the numpy/cupy
NoLoop.  Read that file first -- the physics, the reason the arrival offset must be nonzero, and
the (A)/(B)/(C) ladder are documented there and are not repeated in full here.

WHY THIS FILE EXISTS SEPARATELY FROM test_jax_slowrot.py.
``test_jax_slowrot.py`` gate (a) checks that the JAX path AGREES with the NoLoop.  That is
necessary but NOT sufficient, and the difference is not academic: a likelihood that drops the
arrival-time post-phase from BOTH terms is perfectly self-consistent, satisfies Cauchy-Schwarz,
and was measured ~95 nats from the correct value.  Agreement pins the two implementations to each
other; only a bound and an independently constructed model pin the VALUE.

Three checks, in order (the later ones are worthless without the earlier ones):

  (A) TEETH.  With the modulation switched off (f_sidereal=0) against the SAME rotating data the
      deficit must be LARGE, or this configuration does not exercise rotation at all and (B),(C)
      would pass on an untested code path.
  (B) THE BOUND.  No sampled lnL(t) may exceed (1/2)<d|d>.  The data IS the exact model at the
      p_max under test (see data_for), so at the true arrival sample lnL sits ON the bound:
      maximum sensitivity, no slack.  Measured DEFICIT at the peak (0.5<d|d> - max(lnL); the
      assert gates the OPPOSITE sign, overshoot <= TOL_BOUND, so a positive deficit means the
      bound is respected -- see config_for): ~0 nats (p_max=0, roundoff either side of the
      bound) and ~+5e-10 out of 50908.118464 (p_max=1), i.e. the table below.  Quoted to ONE
      figure because it is not portable across CPUs: bit-identical on repeat runs of a given
      host, but 5.602487e-10 on the Intel Xeon E5-26xx v4 head nodes (ldas-pcdev11/13) and
      5.093170e-10 on the AMD EPYC 9475F (citlogin6) -- same code, same venv, same commit, a
      9.1% gap, i.e. a difference in the SECOND significant figure.  See PORTABILITY below.
      Two earlier revisions of this bullet got this wrong in turn: "not bit-reproducible run
      to run" (repeat runs agree to every digit -- it is the CPU), then "a difference in the
      THIRD significant figure" (5.6 vs 5.1 is the second).
      This bullet read "5.1e-04 out of 3.2e+05" from 41a7d6fb until 2026-08-19; neither figure
      survived the #142/#143 widening and the #163 Nyquist fix.
  (C) THE MECHANISM.  lnL(t) must equal a directly constructed <d|h> - (1/2)<h|h> for the model
      the likelihood implies, built explicitly in the time domain and contracted with the same
      band-limited, noise-weighted inner product.  (B) can only detect a violation; (C) pins the
      value.

  (D) is a bonus cross-check: the JAX lnL(t) against the numpy NoLoop lnL(t) on the same bank.

(C)'s tolerance is ABSOLUTE (1e-6 nats) OR RELATIVE to 0.5<h|h> (1e-6), whichever passes.  Both
rungs now clear it on the ABSOLUTE arm with room to spare; the relative arm is a backstop, not
slack bought to make p_max=1 go green.

PORTABILITY -- READ BEFORE FILING A FINDING ON ANY DIGIT IN THIS FILE.  Every number here is
bit-identical on repeat runs of one host (repeat runs, core count and call order were all
checked) but NOT portable between CPU families.  Unless a line says otherwise, numbers are
measured on the Intel Xeon E5-26xx v4 head nodes (ldas-pcdev11/13).  The AMD EPYC 9475F head
node (citlogin6) differs, and NOT only in low-order digits -- at p_max=0 the (C) residual
differs by 25%, in the leading digit.  Known divergent cells, Intel -> AMD:

    p_max=0  (B) bound deficit     +0.000000e+00 -> -7.275958e-12  (sign flips: on AMD the
                                                    peak sits 7.3e-12 ABOVE the bound)
    p_max=0  (C) vs explicit       5.821e-11     -> 7.276e-11      (1.14e-15 -> 1.43e-15 rel)
    p_max=1  (B) bound deficit     +5.602e-10    -> +5.093e-10
    p_max=1  (C) vs explicit       6.476e-10     -> 6.039e-10
    p_max=1  (D) vs numpy NoLoop   8.222e-10     -> 8.440e-10
    p_max=2  (D) at INFL=5400,f1700  1.854947e-06 -> 1.855078e-06  (straddles 3 s.f.:
                                                    rounds to 1.85e-06 vs 1.86e-06)
    p_max=2  worst|noloop-expl| at f512  6.35e-08 -> 6.26e-08

Nothing in this file is a tolerance, so the spread costs nothing and no gate is at risk.  Do
NOT "fix" a cell because your host differs, and do not derive an argument from a digit that
appears above.  Everything the DECISIONS rest on is portable at the precision used for it.

WHAT THE LADDER MEASURES, AS OF ISSUE #159 (Config below; ldas-pcdev11, CPU, float64):

                         p_max=0                      p_max=1
    bands / 0.5<d|d>     5 / 50960.387223             14 / 50908.118464
    (A) static deficit   4.9865 nats                  3.9234 nats     (gate: > 1.0)
    (B) bound deficit    +0.000000e+00                +5.602e-10      (gate: overshoot <= 1e-6)
    (C) vs explicit      5.821e-11 = 1.14e-15 rel     6.476e-10 = 1.27e-14 rel
    (D) vs numpy NoLoop  5.821e-11                    8.222e-10

(A) and (B) were scoped to p_max=0 for one release (#151) because the p_max=1 rung read
(A) 0.3907 / (B) -4.108e-03 / (C) 6.06e-07 relative, i.e. the bound was VIOLATED by more than
the reference could resolve.  That was diagnosed as the delay expansion diverging.  IT WAS NOT:
lowering fmax from 1700 to 64, which cuts max|2 pi f delta_tau| from 30.4 to 1.1, moved (C) not
much: (C) relative stays between 4.88e-07 and 6.06e-07 across fmax = 1700/1024/512/256/128 and
rises only at fmax=64, to 1.18e-06 (sweep in issue #159).  A flat band and then a 2x wander,
where the divergence hypothesis needed four orders.  Two separate defects were responsible,
both now fixed:

  1. The NYQUIST BIN of the FD derivative weight.  This packing carries +fNyq but not -fNyq, so
     an odd derivative weight cannot be consistent there, and conj(h^(p)) and (conj h)^(p) --
     the same function -- disagreed in that one bin by a SIGN.  U takes both factors from the
     same family and never noticed; V = <chi_a^*|chi_a'> pairs the two orders and did.  The
     sidereal modulation is a sub-bin shift applied as a time-domain phase, so it spread that
     one bin across the whole band.  |H(+fNyq)| is 0.02-0.14 of |H(100 Hz)| for these modes, so
     this was worth 1.5e-07 of the p_max=1 model norm -- a norm too SMALL, which is exactly how
     lnL got 4e-03 nats OVER the bound.  Fixed in flwr.time_derivative_weight; it is a defect in
     the shared precompute, not in this port, and the numpy NoLoop carried it identically.
  2. THE SHIFT CONVENTION of (C)'s own reference.  See _explicit_model_fd: the bank shifts the
     MODULATED template circularly and repairs the phase with rotation_post_phase, and the
     reference has to do the same.  Worth the rest: with defect 1 fixed but the reference
     still modulating on the unrolled grid, (C) reads 1.66e-02 nats = 3.26e-07 relative at
     INFL=1350 and 1.30e-01 nats = 2.55e-06 at the INFL=5400 this rung now ships.

With both fixed, (C) is at machine precision at p_max=1 and (B) sits ON the bound to 6e-10, so
both are asserted at both rungs.  Do not "fix" a future regression here by widening TOL_BOUND,
TOL_DIRECT_* or MIN_STATIC_DEFICIT -- every number above has four or more orders of margin.

TWO THINGS THIS LADDER DELIBERATELY DOES NOT CLAIM.

  * It does not claim the p-expansion CONVERGES here.  It does not: run_ladder prints
    max|2 pi f delta_tau| = 184.9 at the p_max=1 configuration (30.4 at the old INFL=1350).
    That is fine and is the point of building the data as the exact model at the p_max under
    test -- what is being validated is that the evaluator computes lnL for the model the bank
    implies, which is a statement about the code and holds at any Omega.  It is NOT a
    statement that the truncated model is close to a physical waveform.
  * It does not measure the gap between the bank's CIRCULARLY shifted model and a
    non-circularly (physically) modulated one.  That gap is real and is 1.30e-01 nats
    = 2.55e-06 relative at this configuration, because hY^(1) carries 5.9e-04 of its peak
    over the K_ARR samples the shift wraps (hY^(0) carries 1.2e-16, which is why Path A is
    immune).  It is a property of FFT-correlation banks generally, not of this port, and no
    assert here covers it.  A Path-B production analysis with a nonzero arrival offset
    inherits it.

The whole ladder runs at p_max=0 (Path A) AND p_max=1 (Path B).  Path B is a distinct code path
for this port, not a wider bank: several ``p`` then share a sidereal harmonic ``n``, so the
post-phase buckets ``m = n_a' - n_a`` collect (a,a') pairs from DIFFERENT p (4-20 pairs per bucket
at p_max=1 vs 1-5 at p_max=0) and the V-term reflection ``(p,n)->(p,-n)`` has to resolve within p.
p_max=2 is NOT run, and is NOT currently supported: after the #142/#143 widening it is a
27-band bank whose 729 U/V cross terms dominate the precompute, and it adds no new branch --
the same duplicate-m scatter-add and within-p reflection p_max=1 already exercises.  It is skipped
because it buys no coverage for 2-3x the runtime (two interleaved trials, one host: p_max=2 took
59.2 s then 55.0 s, p_max=1 27.8 s then 20.0 s -- the first-trial figures carry JIT warmup, and
the p_max=2 times are to the (D) failure below, not to a full pass), not because it is broken: at
INFL=5400 it passes (A), (B) and (C), and only (D) -- this file's bonus cross-check against the
numpy NoLoop -- exceeds its tolerance, and only on (D)'s ABSOLUTE arm (1.85e-06 vs TOL_NOLOOP
1e-8; (C), which has an `abs OR rel` gate, passes at 3.4e-11 relative).  Both evaluators sit far
from TOL_NOLOOP but close to each other in relative terms, and both sit at a comparable distance
from the independent explicit model.  The three |...-expl| and sign columns below are measured
over (C)'s 21-sample window about the peak; (D) is run_ladder's own whole-164-sample-scan max,
which is NOT the same window -- at row 2 (D)'s argmax is k=+17, outside the window's k in
[32,52], and restricting (D) to the window would read 1.12e-06 rather than 1.85e-06:

        p_max=2 (Intel; (D) row 2 and |noloop-expl| row 3 are host-split, see PORTABILITY)
        config             worst|jax-expl|   worst|noloop-expl|   signs agree   (D), full scan
        INFL=1350, f1700   3.50e-06          1.66e-06             21 of 21      3.45e-06
        INFL=5400, f1700   1.73e-06          1.42e-06             13 of 21      1.85e-06
        INFL=5400, f512    1.41e-07          6.35e-08             17 of 21      1.16e-07

DO NOT ATTACH A MECHANISM TO THIS TABLE WITHOUT MEASURING IT AT MORE THAN ONE ROW.  Two earlier
revisions of this paragraph did, and both were wrong in opposite directions: the first quoted
row 1's numbers under row 2's heading and concluded the two evaluators err "in the same
direction" (true at INFL=1350, where the signs agree 21 of 21 -- false at INFL=5400, where they
agree 13 of 21, which is chance); the second retracted those numbers as unattainable (they are
attainable, at row 1) and concluded the errors are INDEPENDENT and (D) therefore "lands near
their sum and cannot be driven to TOL_NOLOOP by any choice of rate" -- but (D) is nowhere near
either sum (5.16e-06 vs 3.45e-06; 3.15e-06 vs 1.85e-06), the sign structure differs between
rows, and (D) in fact FALLS 1.9x for a 4x rate change and 16x at the narrower band.  The honest
statement is the table.

The decision needs no mechanism.  (D) exceeds TOL_NOLOOP (1e-8, an ABSOLUTE-only gate) at every
configuration tried -- by roughly 350x, 185x and 12x for the three rows above, in that order
(approximate on purpose: the third row's inputs are host-split, see PORTABILITY).  Note the
third: at fmax=512 (D) is only ~12x over, so "the band is too wide" is the one reading this table
does NOT rule out, and someone will propose shipping p_max=2 there.  It still fails, by an order
of magnitude, on a gate with no relative arm -- so supporting the rung means giving (D) the
`abs OR rel` shape (C) already has, a change to what the test ASSERTS rather than a tolerance
bump, and that is a decision to take deliberately with its own measurements.  Until then
config_for() RAISES rather than quietly running at the Path-A rate.

THE ARRIVAL OFFSET MUST BE NONZERO.  The post-phase is exp(i n Omega (t - tref)); at t = tref it
is the identity and a broken implementation passes every check.  The data is therefore placed at
the detector's true geometric arrival time (+10.2 ms for H1 here, 42 samples).

MUTATION TEST (measured on the configuration above; both mutations applied to the post-phase in
jax_ile/core.py, and both rungs re-measured).
  * Drop the post-phase from BOTH terms (the pre-#131 code).  Self-consistent, so (B) does NOT
    fire -- it lands 0.057 nats (p_max=0) / 0.993 nats (p_max=1) UNDER the bound.  (C) catches
    it at 95.31 nats = 1.87e-03 of 0.5<h|h> (p_max=0) and 231.33 nats = 4.54e-03 (p_max=1),
    i.e. 1900-4500x the relative gate and 1e+11 x the absolute one; (D) at 163 / 379 nats.
    This is exactly why (C) and (D) exist and why NoLoop agreement alone is not enough --
    though test_jax_slowrot.py gate (a) does also fire.
  * Drop it from the model norm only (the asymmetric form).  (B) fires: 10.57 nats OVER the
    bound at p_max=0, 16.75 nats OVER at p_max=1.
Neither check subsumes the other; keep both.

(A) does NOT move under either mutation (3.9234 nats at p_max=1 in all three runs), and that is
correct rather than a gap: (A) compares the rotating evaluator against the SAME evaluator with
f_sidereal=0, so a change common to both cancels.  (A) is a guard on the CONFIGURATION -- it
fails when the chosen Omega leaves rotation worth less than MIN_STATIC_DEFICIT, which is what
retired the 90-minute rate for this rung -- not a guard on the post-phase.  (B), (C) and (D)
are what watch the evaluator.

Run: JAX_PLATFORMS=cpu PYTHONPATH=<tree>/MonteCarloMarginalizeCode/Code \\
     python test/jax/test_jax_slowrot_cauchy_schwarz.py
"""
from __future__ import print_function, division
import numpy as np

import jax
jax.config.update("jax_enable_x64", True)

import lal
import lalsimulation as lalsim
import RIFT.lalsimutils as lsu
import RIFT.likelihood.factored_likelihood as fl
import RIFT.likelihood.factored_likelihood_with_rotation as flwr
import RIFT.likelihood.slowrot_response as srr

from RIFT.likelihood.jax_ile.banded import build_rotation_data
# _accumulate_unit is the (private) kernel that produces the per-time-bin kappa and rho^2.
# The public entry points marginalize over t, which would smear exactly the arrival-time
# dependence this file is about; every sampled lnL_t below is a genuine lnL for ONE arrival
# time, which is what makes (B) tolerance-free.
from RIFT.likelihood.jax_ile.core import _accumulate_unit

fmin = 30.; event_time = 1e9; t_window = 0.1; Lmax = 2
deltaT = 1. / 4096.; seglen = 4.; deltaF = 1. / seglen
fNyq = 1. / 2. / deltaT; N = int(round(seglen / deltaT))
det = 'H1'
HARM = (-2, -1, 0, 1, 2)
psd_dict = {det: lalsim.SimNoisePSDaLIGOZeroDetHighPower}


def _harm_for(p_max):
    """The harmonic set the PRECOMPUTE will actually carry for this p_max.

    rotation_coefficients emits keys (p, n+m) with |m| <= 1, so the coefficient index widens
    by one per derivative order, and PrecomputeLikelihoodTermsWithRotation widens a too-narrow
    `harmonics` to |n| <= 2 + p_max rather than silently dropping bands (#142/#143).  Derive
    the set from that same helper: assuming HARM here instead would put the data, the bank and
    the explicit reference model on THREE different harmonic sets at p_max >= 1.
    """
    return flwr.widen_harmonics_for_p_max(HARM, p_max)[0]
RA, DEC, PSI, INCL, PHIREF = 1.0, 0.2, 0.5, 0.7, 0.9
DLOUD = fl.distMpcRef * 1e6 * lsu.lsu_PC / 30.      # loud, so lnL sits near the bound

# ---------------------------------------------------------------- per-rung configuration
# The two knobs the rung's conditioning turns on: the rotation rate (through INFL, the factor
# by which the sidereal rate is inflated so that Omega*T_segment matches a long signal) and the
# upper end of the band.  They are PER p_max because the p >= 1 rungs need a different balance
# from Path A -- see CONFIG below and the module docstring.
INFL_DEFAULT = 5400. / seglen      # Omega * T_segment as for a 90-minute signal
FMAX_DEFAULT = 1700.


class Config(object):
    """One rung's (INFL, fmax), plus everything derived from them.

    Everything that does NOT depend on these two knobs -- the waveform modes, hY_data, hY_ref
    and its FD derivatives -- stays at module level and is shared across configurations, so a
    sweep over (INFL, fmax) does not regenerate waveforms.
    """

    def __init__(self, infl=INFL_DEFAULT, fmax=FMAX_DEFAULT):
        self.infl = float(infl)
        self.fmax = float(fmax)
        # The 5-harmonic ANTENNA expansion is exact at any Omega, so inflating Omega costs no
        # accuracy at p_max=0.  The DELAY expansion is a Taylor series and does not share that
        # property: see _delay_expansion_ratio.
        self.omega = flwr.OMEGA_EARTH * self.infl
        self.fsid = self.omega / (2.0 * np.pi)
        self.ipc = lsu.ComplexIP(fmin, self.fmax, fNyq, deltaF, psd_dict[det], True, False, 0.)
        self._data_cache = {}

    def __repr__(self):
        return "Config(INFL=%.1f, fmax=%.0f, Omega*T_seg=%.3f rad)" % (
            self.infl, self.fmax, self.omega * seglen)


# The configuration each rung runs at.  An unlisted p_max >= 1 RAISES in config_for() below;
# the bare-Config() fallback is reachable only for p_max < 1 and not in CONFIG -- i.e.
# negative, or a non-integer below 1 -- since 0 is a key here.  Neither occurs in practice.
#
# Path B runs FASTER than Path A, at Omega*T_segment for a 6-hour signal rather than a
# 90-minute one, and that is (A)'s requirement, not (B)'s or (C)'s.  With the model
# non-truncated (#142/#143) the static approximation is good to 0.39 nats at the 90-minute
# rate -- below MIN_STATIC_DEFICIT, i.e. the rung would not be exercising rotation.  The
# deficit grows FASTER THAN LINEARLY but slower than Omega^2 over this range (measured:
# 0.0046 / 0.107 / 0.389 / 1.296 / 3.923 nats at INFL = 135 / 675 / 1350 / 2700 / 5400 --
# that is 10.1x for the last 4x, i.e. ~Omega^1.66, where Omega^2 would predict 16x; this
# comment said "like Omega^2" against that same list).  So 4x the rate buys 10x the teeth.
# Nothing else
# pays for it: (B) and (C) are at machine precision across that whole range once the two
# defects issue #159 turned up are fixed (see the module docstring).
CONFIG = {
    0: Config(),
    1: Config(infl=21600. / seglen),
}


def config_for(p_max):
    """Rotation rate for this rung.  REFUSES an unlisted p_max >= 1 rather than guessing.

    The old fallback handed any unlisted p_max the Path-A default (INFL=1350), which is the
    rate this file argues is too slow for p >= 1 -- so `run_ladder(p_max=2)` silently ran at
    a rate its own asserts reject.  Measured there (shipped code, CPU float64):

        p_max=2, INFL=1350   (A) 0.4022 -> fires   (B) +3.12e-06 > TOL_BOUND   (D) 3.45e-06
        p_max=2, INFL=5400                          (B) +3.69e-07 ok           (D) 1.85e-06
        p_max=2, INFL=5400, fmax=512                (B) +1.08e-07 ok           (D) 1.16e-07

    (B) here is the OVERSHOOT max(lnL) - 0.5<d|d> that the assert gates, so all three rows are
    positive: the bound is exceeded at every one, and rows 2 and 3 are "ok" only because they sit
    inside TOL_BOUND (1e-6).  Do not quote run_ladder's printed "deficit" in this column -- that
    is 0.5<d|d> - max(lnL), the opposite sign, and an earlier revision of this table mixed the
    two, so rows 2 and 3 read as "bound respected with margin" when they are violations 2.7x and
    9x inside tolerance.

    So p_max=2 is not merely un-tuned: (D) -- JAX vs the numpy NoLoop -- exceeds TOL_NOLOOP
    (1e-8) at every configuration tried, by 345x / 185x / 11.6x for the three rows above.
    DO NOT WRITE A MECHANISM FOR THAT HERE.  Three revisions of the module docstring tried to
    ("float64 cancellation in the 729-term U/V sums", "same direction at the same floor",
    "independent errors that cannot be driven to TOL_NOLOOP by any choice of rate") and all
    three were refuted by measuring a second row; this docstring carried the first of them
    verbatim for two commits after the module docstring retracted it.  The measurements live
    in the module docstring's p_max=2 table -- read that, not a story about it.  Note also
    that raising the rate DOES move (D) (3.45e-06 -> 1.85e-06 for 4x), so "not a rate that
    wants raising", which this docstring used to say, is false; it moves and does not arrive.

    Supporting the rung would mean giving (D) the `abs OR rel` shape (C) already has -- a
    change to what the test ASSERTS, not a tolerance bump, and not a loosening of TOL_NOLOOP.
    Give it a CONFIG entry only together with that change and its own measurements.
    """
    if p_max in CONFIG:
        return CONFIG[p_max]
    if p_max >= 1:
        raise ValueError(
            "no CONFIG entry for p_max=%r: this ladder's rate is chosen per rung, and the "
            "old fallback silently used the Path-A rate (INFL=1350), which p >= 1 asserts "
            "reject.  See this function's docstring for the p_max=2 measurements." % (p_max,))
    return Config()

TOL_BOUND = 1e-6           # nats above (1/2)<d|d> that we call a violation
TOL_DIRECT_ABS = 1e-6      # nats of disagreement with the explicit model
TOL_DIRECT_REL = 1e-6      # ... or, as a backstop, of 0.5<h|h> (see the module docstring)
TOL_NOLOOP = 1e-8          # nats of disagreement with the numpy NoLoop lnL(t)
MIN_STATIC_DEFICIT = 1.0   # (A): rotation must be worth at least this much here
NPTS_SCAN = 164            # +-20 ms
SCAN_HALF = 10             # (C) samples either side of the arrival sample

TVALS = -0.02 + np.arange(NPTS_SCAN) * deltaT


def _ifft_arr(hf):
    n = hf.data.length; dt = 1. / (n * hf.deltaF)
    ts = lal.CreateCOMPLEX16TimeSeries("h", hf.epoch, 0., dt, lal.DimensionlessUnit, n)
    lal.COMPLEX16FreqTimeFFT(ts, hf, lal.CreateReverseCOMPLEX16FFTPlan(n, 0))
    return np.array(ts.data.data)


def _to_fd(arr, epoch, dt, n):
    ts = lal.CreateCOMPLEX16TimeSeries("h", epoch, 0., dt, lal.DimensionlessUnit, n)
    ts.data.data[:] = arr[:n]
    hf = lal.CreateCOMPLEX16FrequencySeries("hf", epoch, 0., 1. / dt / n, lsu.lsu_HertzUnit, n)
    lal.COMPLEX16TimeFreqFFT(hf, ts, lal.CreateForwardCOMPLEX16FFTPlan(n, 0))
    return hf


Psig = lsu.ChooseWaveformParams(
    fmin=fmin, radec=True, incl=INCL, phiref=PHIREF, theta=DEC, phi=RA, psi=PSI,
    m1=30 * lal.MSUN_SI, m2=25 * lal.MSUN_SI, detector=det, dist=200e6 * lal.PC_SI,
    deltaT=deltaT, tref=event_time, deltaF=deltaF)

lald = lalsim.DetectorPrefixToLALDetector(det)
DELAY = float(lal.TimeDelayFromEarthCenter(np.asarray(lald.location), RA, DEC,
                                           lal.LIGOTimeGPS(event_time)))
K_ARR = int(round(DELAY / deltaT))       # arrival sample offset from tref
assert K_ARR > 0, ("this test needs the signal placed at a POSITIVE arrival offset (see the "
                   "module docstring): the post-phase is the identity at zero offset, and a "
                   "negative one wraps the inspiral onset.  Geometric delay here is %g s." % DELAY)

# ---------------------------------------------------------------- data: the exact Path-A model,
# placed at the detector's geometric arrival time.
Pm = Psig.manual_copy(); Pm.dist = DLOUD
hlms_d, _ = fl.internal_hlm_generator(Pm, Lmax, verbose=False, quiet=True)
lm0 = list(hlms_d.keys())[0]
epoch_intr = float(hlms_d[lm0].epoch)
u_grid = epoch_intr + np.arange(N) * deltaT          # data-grid intrinsic time = t' - tref
hY_data = np.zeros(N, dtype=complex)
for lm in hlms_d:
    hY_data += _ifft_arr(hlms_d[lm]) * lal.SpinWeightedSphericalHarmonic(INCL, -PHIREF, -2,
                                                                        lm[0], lm[1])
g_ev = float(lal.GreenwichMeanSiderealTime(lal.LIGOTimeGPS(event_time))) - RA
Atil = {n: v * np.exp(1j * n * g_ev)
        for n, v in srr.antenna_harmonics(lald.response, DEC, PSI).items()}
INV_DIST = fl.distMpcRef / (DLOUD / (lsu.lsu_PC * 1e6))


def _path_a_data(cfg):
    """The exact Path-A model F(u) * roll(hY, K_ARR) at this configuration's Omega."""
    F_of_u = sum(Atil[n] * np.exp(1j * n * cfg.omega * u_grid) for n in Atil)
    return _to_fd(np.real(F_of_u * np.roll(hY_data, K_ARR)),
                  lal.LIGOTimeGPS(epoch_intr + event_time), deltaT, N)


def delay_expansion_ratio(cfg):
    """max |2 pi f delta_tau| over the band: the p-expansion's convergence parameter.

    The p >= 1 bands are the Taylor series of h(t - delta_tau(t)) in the delay DRIFT
    delta_tau(t) = tau(t) - tau(tref), so the p-th band is smaller than the p-1'th by roughly
    this factor.  Above 1 the series diverges at the top of the band and every construction
    that reconstructs the model from it -- including (C)'s explicit reference -- inherits that.

    It is a max over the whole u_grid evaluated at fmax, so it is an UPPER BOUND at the band
    edge -- but do not read that as "therefore it overstates".  Measured at the rate this rung
    ships (INFL=5400, fmax=1700), where it prints 184.9:

        p_max      0          1          2          3
        bands      5          14         27         44
        0.5<d|d>   50732.00   50908.12   50807.14   2032018.46

    (0.5<d|d> varies with p_max because data_for() rebuilds the DATA as the p_max-truncated
    model -- see its docstring -- so this row is the norm of the reconstruction, not of a
    fixed dataset.  That is exactly what makes it a divergence meter.)

    The p <= 2 bands are perturbative there (<= 0.35%, and p = 2 is SMALLER than p = 1 at
    +0.148% vs +0.347%), and p = 3 is NOT: it blows up by a factor of 40.  So at this
    configuration the printed 184.9 is a CORRECT warning about p = 3, not an overstatement.

    IT IS NOT THE REASON THE RUNG STOPS AT p_max = 1.  This caption used to say it was; the
    table above refutes that, since p = 2 is the most perturbative order here.  p_max = 2 is
    unsupported for unrelated reasons -- runtime, and (D) against the numpy NoLoop -- which
    are config_for's business, not this metric's.  What this number licenses is refusing
    p >= 3.  Its trend across rates is the informative part; the single value is a band-edge
    bound and says nothing on its own about which p you can afford.
    """
    Bd = srr.delay_harmonics(lald.location, DEC)
    Btil = {m: Bd[m] * np.exp(1j * m * g_ev) for m in Bd}
    D = dict(Btil)
    D[0] = D[0] - np.real(sum(Btil.values()))
    dtau = sum(D[m] * np.exp(1j * m * cfg.omega * u_grid) for m in D)
    return 2.0 * np.pi * cfg.fmax * float(np.max(np.abs(np.real(dtau))))


def _Pv():
    Pv = Psig.manual_copy()
    for key, v in [('phi', RA), ('theta', DEC), ('incl', INCL), ('phiref', PHIREF),
                   ('psi', PSI), ('dist', DLOUD)]:
        setattr(Pv, key, np.ones(1) * v)
    Pv.tref = event_time; Pv.deltaT = deltaT
    return Pv


def rotation_lnL_t(f_sidereal, p_max, cfg):
    """(jax lnL(t), numpy NoLoop lnL(t), arrival sample offsets, a_list) on one shared bank."""
    P = Psig.manual_copy()
    data_dict = data_for(p_max, cfg)[1]
    bank = flwr.PrecomputeLikelihoodTermsWithRotation(
        event_time, t_window, P, data_dict, psd_dict, Lmax, cfg.fmax, harmonics=HARM,
        p_max=p_max, f_sidereal=f_sidereal, analyticPSD_Q=True, verbose=False, quiet=True,
        skip_interpolation=True)
    meta = bank[4]
    _harm = _harm_for(p_max)
    assert len(meta['a_list']) == (p_max + 1) * len(_harm), (
        "unexpected a_list size: %d bands for p_max=%d over %d harmonics"
        % (len(meta['a_list']), p_max, len(_harm)))
    lk, rho_b, U_b, V_b, epd = flwr.pack_rotation_arrays(meta, bank[3], bank[1], bank[2])
    Pv = _Pv()

    lnL_ref = flwr.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoopWithRotation(
        TVALS, Pv, meta, lk, rho_b, U_b, V_b, epd, Lmax=Lmax, array_output=True)[0]

    jdata = build_rotation_data(meta, lk, rho_b, U_b, V_b, epd, deltaT, TVALS)
    kappa, rho_sq = _accumulate_unit(
        jdata, Pv.phi, Pv.theta, Pv.psi, Pv.incl, Pv.phiref, "nearest", False)
    lnL_jax = np.asarray(kappa.real * INV_DIST - 0.5 * rho_sq * INV_DIST ** 2)[0]

    # Reproduce the shared indexing so we know which arrival sample each output is.
    off = float(Pv.tref - float(epd[det]))
    ifirst = int(np.round((off + DELAY + TVALS[0]) / deltaT))
    kvals = ifirst + np.arange(NPTS_SCAN) - int(round(off / deltaT))
    return lnL_jax, np.asarray(lnL_ref), kvals, list(meta['a_list'])


# ---------------------------------------------------------------- the explicit model for (C)
# The model the likelihood implies, built explicitly on the data grid:
#
#   h(u) = invDist * Re[ sum_a C~_a(t) chi_a(u - t) ],   chi_a(u) = e^{i n_a Omega u} hY^(p_a)(u)
#
# with C~_a = C_a e^{i n_a Omega k dt} the arrival-time post-phase at arrival sample k
# (rotation_post_phase).
#
# THE SHIFT IS APPLIED TO THE MODULATED TEMPLATE, and that is not interchangeable with the
# obvious-looking alternative.  Analytically the post-phase cancels the shift inside the
# modulation -- C~_{(p,n)} e^{i n Omega (u - k dt)} = C_{(p,n)} e^{i n Omega u} -- so one is
# tempted to modulate on the UNROLLED grid and write
#     h(u) = invDist Re[ sum_p G_p(u) roll(hY^(p), k) ],  G_p(u) = sum_n C_{(p,n)} e^{i n Omega u}.
# But the shift here is CIRCULAR, and e^{i n Omega u} is not periodic on the segment, so the
# two forms differ by e^{i n Omega T_seg} on exactly the k samples that wrap the boundary.
# At p_max=0 that costs nothing -- hY^(0) is machine zero over the last K_ARR samples
# (1.2e-16 of its peak) -- but hY^(1) is NOT: the FD derivative leaves 5.9e-04 of its peak
# there, and the wrapped mismatch then shows up as ~1e-02 nats of disagreement with the
# bank, which computes the shift by FFT correlation and is circular in exactly this sense.
# See issue #159.  The post-phase is still applied EXPLICITLY below, so (C) keeps its teeth
# against a dropped rotation_post_phase (see the mutation numbers in the module docstring).
#
# At p_max=0 the sum reduces to F(u)*roll(hY,k), the numpy twin's construction (G_0 == F),
# and data_for() asserts that equality at 1e-12.
#
# G_p reuses flwr.rotation_coefficients and the FD derivative weight rather than re-deriving
# them: what (C) is pinning is the arrival-time post-phase and the band contraction, not the
# response algebra (test_jax_slowrot_coeffs, 2e-16) or the FD derivative (test_slowrot_fd_ops).
Pref = Psig.manual_copy()
Pref.dist = fl.distMpcRef * 1e6 * lsu.lsu_PC
Pref.deltaF = deltaF
hlms_r, _ = fl.internal_hlm_generator(Pref, Lmax, verbose=False, quiet=True)
Ylm_r = fl.ComputeYlms(Lmax, INCL, -PHIREF, selected_modes=list(hlms_r.keys()))
hY_ref = np.zeros(N, dtype=complex)
for lm in hlms_r:
    hY_ref += Ylm_r[lm] * _ifft_arr(hlms_r[lm])
data_epoch = lal.LIGOTimeGPS(epoch_intr + event_time)
_hY_ref_fd = _to_fd(hY_ref, data_epoch, deltaT, N)
_FVALS = flwr.evaluate_fvals_from_length(N, _hY_ref_fd.deltaF)


def _hY_deriv(p):
    """p-th time derivative of hY_ref on the data grid (FD weight, RIFT fvals packing)."""
    if p == 0:
        return hY_ref
    hfp = lal.CreateCOMPLEX16FrequencySeries(
        "hfp", _hY_ref_fd.epoch, 0., _hY_ref_fd.deltaF, lsu.lsu_HertzUnit, N)
    hfp.data.data[:] = _hY_ref_fd.data.data * flwr.time_derivative_weight(_FVALS, p)
    return _ifft_arr(hfp)


def _explicit_model_fd(k, p_max, a_list, cfg):
    """FD of h(u) above, for arrival sample k, at fiducial distance scaled by INV_DIST.

    ``a_list`` is the bank's band list and the sum is RESTRICTED to it.  Since #142/#143 the
    precompute WIDENS a too-narrow harmonic set to |n| <= 2 + p_max, so for a bank built that
    way the restriction is a no-op and nothing is dropped -- keep it anyway, because it is what
    makes this reference track the bank rather than assume it, and a bank built with
    widen_harmonics=False genuinely is a truncated model that this sum must match.

    Historical note, because the number is instructive: before #142 the bank had no band for
    the |n| = 3 coefficients at p_max=1, both evaluators silently dropped them, and summing the
    full coefficient dict here instead of restricting to a_list disagreed by 2.2e+05 nats at
    this configuration -- the dropped bands were the same order as the ones kept, because at
    INFL=1350, the rate this rung then ran at, the first-order delay term dominates.
    """
    C = flwr.rotation_coefficients(det, RA, DEC, PSI, event_time, p_max)   # {(p,n): C_a}
    keep = set((int(p), int(n)) for (p, n) in a_list)
    h_td = np.zeros(N, dtype=complex)
    for p in range(p_max + 1):
        hp = _hY_deriv(p)
        for (pa, na), c in C.items():
            if pa != p or (pa, na) not in keep:
                continue
            chi_a = np.exp(1j * na * cfg.omega * u_grid) * hp          # chi_a(u)
            post = np.exp(1j * na * cfg.omega * k * deltaT)            # rotation_post_phase
            h_td = h_td + c * post * np.roll(chi_a, k)                 # C~_a chi_a(u - k dt)
    return _to_fd(np.real(h_td) * INV_DIST, data_epoch, deltaT, N)


def data_for(p_max, cfg):
    """(data, data_dict, 0.5<d|d>, a_list) with the data EQUAL to the exact model at this p_max.

    That is what makes (B) maximally tight: with the data equal to the model the likelihood can
    represent, lnL at the true arrival sample sits exactly ON (1/2)<d|d>, leaving no slack for an
    inconsistency to hide in.  A p_max=0 dataset used against a p_max=1 bank would instead leave
    the p>=1 bands fitting nothing, and (B) would pass with 1e5 nats of margin.

    p_max=0 uses the INDEPENDENT construction above (srr.antenna_harmonics -> F(u) -> Re[F*roll]),
    which shares nothing with rotation_coefficients; the assert below pins the two together at
    p_max=0 so the p>=1 datasets inherit that provenance.
    """
    if p_max not in cfg._data_cache:
        a_list = flwr._elementary_index_set(_harm_for(p_max), p_max)
        if p_max == 0:
            d = _path_a_data(cfg)
            chk = _explicit_model_fd(K_ARR, 0, a_list, cfg)
            dd = np.max(np.abs(chk.data.data - d.data.data))
            ref = np.max(np.abs(d.data.data))
            assert dd <= 1e-12 * ref, (
                "the explicit model and the independent antenna_harmonics data construction "
                "disagree at p_max=0 by %g (rel %g) -- (C)'s reference is not the Path-A model"
                % (dd, dd / ref))
        else:
            d = _explicit_model_fd(K_ARR, p_max, a_list, cfg)
        cfg._data_cache[p_max] = (d, {det: d}, 0.5 * cfg.ipc.ip(d, d).real, a_list)
    return cfg._data_cache[p_max]


def run_ladder(p_max=0, cfg=None, verbose=True):
    """The (A)-(D) ladder at one p_max.  Returns a dict of the measured numbers."""
    if cfg is None:
        cfg = config_for(p_max)
    tag = "Path %s, p_max=%d" % ("A" if p_max == 0 else "B", p_max)
    data, _dd, HALF_DD, _al = data_for(p_max, cfg)
    if verbose:
        print("\n=== JAX SLOWROT CAUCHY-SCHWARZ (%s, A=%d bands, 0.5<d|d>=%.6f) ==="
              % (tag, len(_al), HALF_DD))
        print("    %s  arrival offset %+d samples (%+.2f ms)  max|2 pi f dtau| = %.3f"
              % (cfg, K_ARR, 1e3 * K_ARR * deltaT, delay_expansion_ratio(cfg)))

    # ------------------------------------------------------------ (A) teeth
    lnL_static, _, _, _ = rotation_lnL_t(0.0, p_max, cfg)
    static_deficit = HALF_DD - float(np.max(lnL_static))
    print("(A) rotation OFF vs rotating data: deficit = %.4f nats" % static_deficit)
    assert static_deficit > MIN_STATIC_DEFICIT, (
        "this configuration does not exercise rotation (static deficit %g <= %g), so the "
        "bound and direct-model checks below would be vacuous"
        % (static_deficit, MIN_STATIC_DEFICIT))

    # ------------------------------------------------------------ (B) the bound
    lnL_rot, lnL_noloop, kvals, a_list = rotation_lnL_t(cfg.fsid, p_max, cfg)
    overshoot = float(np.max(lnL_rot)) - HALF_DD
    jpeak = int(np.argmax(lnL_rot))
    print("(B) rotation ON : max lnL = %.6f at k=%+d   deficit = %+.6e"
          % (np.max(lnL_rot), kvals[jpeak], HALF_DD - np.max(lnL_rot)))
    assert kvals[jpeak] == K_ARR, (
        "lnL peaks at arrival sample %d, not the %d the data was built at -- the test is no "
        "longer sitting on the bound and (B) has lost its teeth" % (kvals[jpeak], K_ARR))
    assert overshoot <= TOL_BOUND, (
        "Cauchy-Schwarz VIOLATED: max JAX lnL exceeds 0.5<d|d> by %g nats.  lnL = <d|h> - "
        "(1/2)<h|h> cannot exceed (1/2)<d|d> for any h, so term1 and term2 are being "
        "evaluated for different templates -- see rotation_post_phase() and "
        "core._accumulate_unit_banded." % overshoot)

    # ------------------------------------------------------------ (C) the mechanism
    # (C) scans only NON-NEGATIVE arrival offsets: a circular shift to earlier times wraps real
    # signal across the segment boundary, where the FFT correlation the precompute uses and an
    # explicit time-domain roll legitimately disagree.  See the numpy twin's docstring.
    worst = 0.0; worst_ref = 0.0; n_cmp = 0; scale = 0.0
    for j in range(max(0, jpeak - SCAN_HALF), min(NPTS_SCAN, jpeak + SCAN_HALF + 1)):
        k = int(kvals[j])
        if k < 0:
            continue
        hf = _explicit_model_fd(k, p_max, a_list, cfg)
        hh = cfg.ipc.ip(hf, hf).real
        lnL_direct = cfg.ipc.ip(hf, data).real - 0.5 * hh
        worst = max(worst, abs(lnL_direct - lnL_rot[j]))
        worst_ref = max(worst_ref, abs(lnL_direct - lnL_noloop[j]))
        scale = max(scale, 0.5 * hh); n_cmp += 1
    print("(C) vs explicit time-domain model over %d samples about the peak: max|d lnL| = %.3e"
          "  (rel to 0.5<h|h>=%.3e: %.2e; numpy NoLoop vs the same reference: %.3e)"
          % (n_cmp, worst, scale, worst / scale, worst_ref))

    # ------------------------------------------------------------ (D) vs the numpy NoLoop
    d_noloop = float(np.max(np.abs(lnL_rot - lnL_noloop)))
    print("(D) vs numpy NoLoop lnL(t) over the whole %d-sample scan: max|d lnL| = %.3e"
          % (NPTS_SCAN, d_noloop))

    assert n_cmp >= SCAN_HALF, "too few comparable samples (%d) for (C) to mean anything" % n_cmp
    assert worst < TOL_DIRECT_ABS or worst / scale < TOL_DIRECT_REL, (
        "JAX rotation likelihood disagrees with the explicit <d|h> - (1/2)<h|h> for the model "
        "it implies by %g nats (%.2e of 0.5<h|h>) at p_max=%d" % (worst, worst / scale, p_max))
    assert d_noloop < TOL_NOLOOP, "JAX vs NoLoop lnL(t) disagree by %g nats" % d_noloop

    return dict(p_max=p_max, infl=cfg.infl, fmax=cfg.fmax, half_dd=HALF_DD,
                static_deficit=static_deficit, max_lnL=float(np.max(lnL_rot)),
                overshoot=overshoot, direct=worst, direct_rel=worst / scale,
                noloop=d_noloop, expansion_ratio=delay_expansion_ratio(cfg))


# pytest collects these; running the file as a script executes the same thing (see __main__).
def test_cauchy_schwarz_path_a():
    run_ladder(p_max=0)


def test_cauchy_schwarz_path_b():
    run_ladder(p_max=1)


if __name__ == "__main__":
    run_ladder(p_max=0)
    run_ladder(p_max=1)
    print("\nALL JAX SLOWROT CAUCHY-SCHWARZ CHECKS PASSED")
