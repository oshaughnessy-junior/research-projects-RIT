"""Known-answer gate for RIFT.likelihood.gpu_waveform_xhm against LALSimulation.

Compares the IMRPhenomXHM modes (2,+-2), (2,+-1), (3,+-3), (3,+-2), (4,+-4), point
by point, with ``SimInspiralChooseFDModes(IMRPhenomXHM)`` and
``PhenomXHMThresholdMband = 0`` on 50 random aligned-spin binaries: 20 BBH
(Mtot 10-150 Msun, q 0.05-1, |chi| <= 0.95), 10 NSBH and 20 BNS, with f_min =
f_ref drawn from 10-20 Hz and durations 8-256 s at 4096 Hz.  Mismatches are
maximized over time and phase with the aLIGO zero-detuned high-power PSD, per
mode, on its own norm.  Run with ``-s`` to print the measured distributions.
The CuPy check runs only where a GPU is usable.
"""

import numpy as np
import pytest

lal = pytest.importorskip("lal")
lalsim = pytest.importorskip("lalsimulation")

from RIFT.likelihood import gpu_waveform_xhm as XHM

N_POINTS = 50
SRATE = 4096.0
F_MAX = SRATE / 2
DIST = 400.0e6 * 3.085677581491367e16
MODES = ((2, 2), (2, -2), (2, 1), (2, -1), (3, 3), (3, -3), (3, 2), (3, -2), (4, 4), (4, -4))

MISMATCH_TOL = 1e-10     # (2,2), (2,1), (3,3), (4,4); measured <= 9e-16 (2026-10-20)
REL_TOL = 1e-9          # measured <= 4.7e-10
# The (3,2) mode feeds LAL's spherical-harmonic ringdown through second
# differences with step 1e-7 (phase) and 1e-9 (amplitude), so a 1e-15 change in
# the (2,2) setup moves its intermediate phase by ~1e-4 rad.  LAL itself is not
# reproducible at that level.  Measured on 2026-10-20: mismatch <= 8.8e-7,
# relative error <= 5.9e-3; the bounds sit just above.
MISMATCH_TOL_32 = 2e-6
REL_TOL_32 = 1e-2


def _points(n, seed):
    rng = np.random.default_rng(seed)
    kind = np.array(["bbh"] * 20 + ["nsbh"] * 10 + ["bns"] * 20)[:n]
    m1 = np.empty(n)
    m2 = np.empty(n)
    c1 = np.empty(n)
    c2 = np.empty(n)
    for i, k in enumerate(kind):
        if k == "bbh":
            mtot = rng.uniform(10.0, 150.0)
            q = rng.uniform(0.05, 1.0)
            m1[i], m2[i] = mtot / (1 + q), mtot * q / (1 + q)
            c1[i], c2[i] = rng.uniform(-0.95, 0.95, 2)
        elif k == "nsbh":
            m1[i], m2[i] = rng.uniform(3.0, 20.0), rng.uniform(1.0, 2.2)
            c1[i], c2[i] = rng.uniform(-0.9, 0.9), rng.uniform(-0.05, 0.05)
        else:
            m1[i], m2[i] = rng.uniform(1.0, 2.5, 2)
            c1[i], c2[i] = rng.uniform(-0.05, 0.05, 2)
    flip = rng.uniform(size=n) < 0.25        # exercise LAL's relabelling
    m1, m2 = np.where(flip, m2, m1), np.where(flip, m1, m2)
    c1, c2 = np.where(flip, c2, c1), np.where(flip, c1, c2)
    fmin = rng.uniform(10.0, 20.0, n)
    T = rng.choice([8.0, 16.0, 32.0, 64.0, 128.0, 256.0], n)
    phi = rng.uniform(0.0, 2 * np.pi, n)
    return m1, m2, c1, c2, fmin, T, phi


def _lal_modes(m1, m2, c1, c2, fmin, df, phi, n_freq):
    d = lal.CreateDict()
    lalsim.SimInspiralWaveformParamsInsertPhenomXHMThresholdMband(d, 0.0)
    hlms = lalsim.SimInspiralChooseFDModes(m1 * lal.MSUN_SI, m2 * lal.MSUN_SI, 0.0, 0.0, c1, 0.0, 0.0, c2,
                                           df, fmin, F_MAX, fmin, phi, DIST, 0.0, d, lalsim.IMRPhenomXHM)
    out = []
    for ell, emm in MODES:
        h = lalsim.SphHarmFrequencySeriesGetMode(hlms, ell, emm).data.data
        c = len(h) // 2
        out.append(h[c - (n_freq - 1): c + n_freq])
    return np.array(out)


def _inv_psd(df, n_freq, fmin):
    f = np.arange(n_freq) * df
    w = np.zeros(n_freq)
    band = f >= fmin
    w[band] = 1.0 / np.array([lalsim.SimNoisePSDaLIGOZeroDetHighPower(x) for x in f[band]])
    return w


def _one_side(h, emm, n_freq):
    return h[n_freq - 1:] if emm < 0 else h[:n_freq][::-1]


def _mismatch(a, b, w):
    z = np.fft.ifft(np.conj(a) * b * w)
    norm = np.sqrt(np.sum(np.abs(a) ** 2 * w) * np.sum(np.abs(b) ** 2 * w))
    return 1.0 - np.max(np.abs(z)) * len(z) / norm


def _max_rel(a, b, inband):
    support = inband & (np.abs(a) > 1e-8 * np.max(np.abs(a)))
    return float(np.max(np.abs(a[support] - b[support]) / np.abs(a[support])))


@pytest.fixture(scope="module")
def measured():
    m1, m2, c1, c2, fmin, T, phi = _points(N_POINTS, 20261020)
    stats = {(l, abs(m)): ([], []) for l, m in MODES}
    zero_checked = 0
    for i in range(N_POINTS):
        df = 1.0 / T[i]
        n_freq = int(F_MAX / df) + 1
        ref = _lal_modes(m1[i], m2[i], c1[i], c2[i], fmin[i], df, phi[i], n_freq)
        got = XHM.imrphenomxhm_modes(m1[i:i + 1], m2[i:i + 1], c1[i:i + 1], c2[i:i + 1], DIST,
                                     phi[i:i + 1], fmin[i], fmin[i], df, n_freq, f_max=F_MAX,
                                     modes=MODES, xp=np)[0]
        assert got.shape == ref.shape
        w = _inv_psd(df, n_freq, fmin[i])
        inband = np.arange(n_freq) * df >= fmin[i]
        for k, (ell, emm) in enumerate(MODES):
            np.testing.assert_array_equal(np.abs(got[k]) > 0, np.abs(ref[k]) > 0)
            if np.max(np.abs(ref[k])) == 0.0:
                zero_checked += 1
                continue
            a = _one_side(ref[k], emm, n_freq)
            b = _one_side(got[k], emm, n_freq)
            stats[(ell, abs(emm))][0].append(_mismatch(a, b, w))
            stats[(ell, abs(emm))][1].append(_max_rel(a, b, inband))
    return stats


def _summary(mode, mism, rel):
    print("(%d,%d): mismatch median %.1e max %.1e | max rel error median %.1e max %.1e  [n=%d]"
          % (mode[0], mode[1], np.median(mism), np.max(mism), np.median(rel), np.max(rel), len(rel)))


@pytest.mark.parametrize("mode", [(2, 2), (2, 1), (3, 3), (4, 4)])
def test_modes_match_lal_xhm(measured, mode):
    mism, rel = measured[mode]
    _summary(mode, mism, rel)
    assert len(rel) >= N_POINTS
    assert max(mism) <= MISMATCH_TOL
    assert max(rel) <= REL_TOL


def test_mode_32_matches_lal_xhm(measured):
    mism, rel = measured[(3, 2)]
    _summary((3, 2), mism, rel)
    assert len(rel) >= N_POINTS
    assert max(mism) <= MISMATCH_TOL_32
    assert max(rel) <= REL_TOL_32


def test_equal_mass_equal_spin_odd_modes_vanish():
    df, n_freq = 0.25, int(F_MAX / 0.25) + 1
    ref = _lal_modes(20.0, 20.0, 0.3, 0.3, 20.0, df, 0.4, n_freq)
    got = XHM.imrphenomxhm_modes(20.0, 20.0, 0.3, 0.3, DIST, 0.4, 20.0, 20.0, df, n_freq,
                                 f_max=F_MAX, modes=MODES, xp=np)[0]
    for k, (ell, emm) in enumerate(MODES):
        if (ell, abs(emm)) in ((2, 1), (3, 3)):
            assert np.max(np.abs(ref[k])) == 0.0 and np.max(np.abs(got[k])) == 0.0
        else:
            sup = np.abs(ref[k]) > 1e-8 * np.max(np.abs(ref[k]))
            rel = np.max(np.abs(ref[k][sup] - got[k][sup]) / np.abs(ref[k][sup]))
            assert rel < (REL_TOL_32 if abs(emm) == 2 and ell == 3 else REL_TOL)


def test_batch_equals_single_point():
    m1, m2, c1, c2, fmin, T, phi = _points(6, 5)
    df, n_freq = 0.25, int(F_MAX / 0.25) + 1
    allp = XHM.imrphenomxhm_modes(m1, m2, c1, c2, DIST, phi, 15.0, 15.0, df, n_freq, f_max=F_MAX,
                                  modes=MODES, xp=np, two_sided=False)
    for i in (0, 3, 5):
        one = XHM.imrphenomxhm_modes(m1[i:i + 1], m2[i:i + 1], c1[i:i + 1], c2[i:i + 1], DIST,
                                     phi[i:i + 1], 15.0, 15.0, df, n_freq, f_max=F_MAX,
                                     modes=MODES, xp=np, two_sided=False)[0]
        np.testing.assert_allclose(one, allp[i], rtol=1e-13, atol=0)


def _usable_cupy():
    try:
        import cupy as cp
        if cp.cuda.runtime.getDeviceCount() < 1:
            return None
        cp.arange(3).sum()
        return cp
    except Exception:
        return None


def test_cupy_matches_numpy():
    cp = _usable_cupy()
    if cp is None:
        pytest.skip("no usable CuPy/GPU on this host")
    m1, m2, c1, c2, fmin, T, phi = _points(N_POINTS, 20261020)
    worst = {}
    for T0 in (8.0, 256.0):
        df = 1.0 / T0
        n_freq = int(F_MAX / df) + 1
        for sl in (slice(0, 20), slice(20, 30), slice(30, 50)):
            args = (m1[sl], m2[sl], c1[sl], c2[sl])
            ref = XHM.imrphenomxhm_modes(*args, DIST, phi[sl], 10.0, 10.0, df, n_freq, f_max=F_MAX,
                                         xp=np, two_sided=False)
            got = XHM.imrphenomxhm_modes(*[cp.asarray(a) for a in args], DIST, cp.asarray(phi[sl]),
                                         10.0, 10.0, df, n_freq, f_max=F_MAX, xp=cp,
                                         two_sided=False)
            got = cp.asnumpy(got)
            scale = np.max(np.abs(ref), axis=-1, keepdims=True)
            scale = np.where(scale > 0, scale, 1.0)
            err = np.max(np.abs(got - ref) / scale, axis=(0, 2))
            for k, mode in enumerate(XHM.XHM_MODES):
                worst[mode] = max(worst.get(mode, 0.0), float(err[k]))
            del got, ref
    print("\nCuPy vs NumPy, max |diff| / max |mode|: " +
          ", ".join("(%d,%d) %.1e" % (l, m, worst[(l, m)]) for l, m in XHM.XHM_MODES))
    assert max(worst.values()) <= 1e-10
