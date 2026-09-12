"""Fail-closed and Fourier-contract tests for the optional Ripple adapter."""

import unittest
from unittest import mock

import numpy as np

from RIFT.likelihood import gpu_waveform as gw


class TestFourierConvention(unittest.TestCase):
    def test_continuous_roundtrip(self):
        rng = np.random.default_rng(20260912)
        ht = rng.normal(size=32) + 1j * rng.normal(size=32)
        hf = gw._continuous_forward(ht, 1.0 / 16.0, np)
        recovered = gw._continuous_inverse(hf, 1.0 / 16.0, np)
        np.testing.assert_allclose(recovered, ht, rtol=2e-14, atol=2e-14)

    def test_forward_matches_lal_rift_packing(self):
        try:
            import lal
        except ImportError:
            self.skipTest("LAL is optional in lightweight unit environments")
        rng = np.random.default_rng(17)
        n, dt = 32, 1.0 / 16.0
        values = rng.normal(size=n) + 1j * rng.normal(size=n)
        ts = lal.CreateCOMPLEX16TimeSeries(
            "test", lal.LIGOTimeGPS(0), 0, dt, lal.DimensionlessUnit, n
        )
        ts.data.data[:] = values
        fs = lal.CreateCOMPLEX16FrequencySeries(
            "test", ts.epoch, 0, 1.0 / (n * dt), lal.DimensionlessUnit, n
        )
        lal.COMPLEX16TimeFreqFFT(fs, ts, lal.CreateForwardCOMPLEX16FFTPlan(n, 0))
        np.testing.assert_allclose(
            gw._continuous_forward(values, dt, np), fs.data.data,
            rtol=2e-14, atol=2e-14,
        )


class _Params:
    deltaT = 1.0 / 16.0
    deltaF = 0.5
    fmin = 2.0
    fmax = 8.0
    fref = 2.0
    phiref = 0.3
    m1 = 30.0 * 1.9884099021470416e30
    m2 = 25.0 * 1.9884099021470416e30
    s1x = s1y = s2x = s2y = 0.0
    s1z = 0.1
    s2z = -0.2
    dist = 200.0 * 3.085677581491367e22


class TestProviderGuards(unittest.TestCase):
    def setUp(self):
        try:
            import jax.numpy as jnp
            import lalsimulation as lalsim
        except ImportError:
            self.skipTest("JAX and LAL are required for provider contract tests")
        self.jnp = jnp
        self.P = _Params()
        self.P.approx = lalsim.IMRPhenomD

    def test_rift_conditioning_fails_closed(self):
        with self.assertRaisesRegex(gw.WaveformCompatibilityError, "not certified"):
            gw.generate_imrphenomd_fd(self.P, backend=self.jnp)

    def test_rejects_wrong_approximant_before_ripple_import(self):
        import lalsimulation as lalsim
        self.P.approx = lalsim.TaylorF2
        with self.assertRaisesRegex(gw.WaveformCompatibilityError, "only supports"):
            gw.generate_imrphenomd_fd(
                self.P, backend=self.jnp, conditioning="direct_fd"
            )

    def test_direct_fd_is_unconditioned_and_has_exact_symmetries(self):
        jnp = self.jnp

        class FakeRipple:
            @staticmethod
            def gen_IMRPhenomD(f, params, fref):
                return (1.0 + 0.25j) * f ** (-7.0 / 6.0)

        with mock.patch.object(gw, "_load_ripple", return_value=FakeRipple):
            bank = gw.generate_imrphenomd_fd(
                self.P, backend=jnp, conditioning="direct_fd"
            )
        self.assertFalse(bank.conditioned)
        self.assertEqual(bank.epoch, 0.0)
        n = bank.modes[(2, 2)].shape[0]
        reflection = (-np.arange(n)) % n
        h22 = np.asarray(bank.modes[(2, 2)])
        h2m2 = np.asarray(bank.modes[(2, -2)])
        np.testing.assert_allclose(h2m2, np.conj(h22[reflection]))
        for lm, mode in bank.modes.items():
            np.testing.assert_allclose(
                np.asarray(bank.conjugate_modes[lm]),
                np.conj(np.asarray(mode)[reflection]),
            )


if __name__ == "__main__":
    unittest.main()
