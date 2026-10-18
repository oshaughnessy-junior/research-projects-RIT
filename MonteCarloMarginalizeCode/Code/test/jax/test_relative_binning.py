"""Relative binning reproduces the exact Q/U/V reduction (ripple_modes.reduce_qvu)."""
import numpy as np
import jax.numpy as jnp

from RIFT.likelihood.jax_ile.relative_binning import RelativeBinning, phase_bins
from RIFT.likelihood.jax_ile.ripple_modes import reduce_qvu

N, DF = 2 ** 14, 0.25
F = -0.5 * N * DF + DF * np.arange(N)
BAND = (np.abs(F) >= 20) & (np.abs(F) <= 1000)


def _chirp(k, amp=1.0, dphi=0.0, dt0=0.0):
    a = np.where(BAND, np.abs(F + 1e-9) ** (-7 / 6), 0) * amp
    ph = (k + 1) * 3e4 * np.abs(F + 1e-9) ** (-5 / 3) + np.sign(F) * 0.3 * k + 2 * np.pi * F * dt0 + dphi
    return a * np.exp(1j * ph * np.sign(F + 1e-12))


def _setup():
    rng = np.random.default_rng(1)
    h0 = np.stack([_chirp(k) for k in range(3)])
    d = np.where(BAND, rng.normal(size=N) + 1j * rng.normal(size=N), 0) + 0.3 * h0.sum(0)
    w = np.where(BAND, 1.0, 0.0)
    dets = {"H1": dict(d=jnp.asarray(d), w=jnp.asarray(w), n_shift=1000)}
    return h0, dets


def _rel(a, b):
    return float(np.max(np.abs(np.asarray(a) - np.asarray(b))) / np.max(np.abs(np.asarray(b))))


def test_rb_exact_at_fiducial_and_converges():
    h0, dets = _setup()
    h1 = np.stack([_chirp(k, amp=1.05, dphi=0.2, dt0=2e-4) for k in range(3)])
    errs = []
    for eps in (1.0, 0.1):
        rb = RelativeBinning(F, dets, 200, DF, 1.0 / (N * DF), phase_bins(20, 1000, eps)).set_fiducial(h0)
        for h, store in ((h0, None), (h1, errs)):
            q, u, v = rb.qvu(jnp.asarray(h[:, rb.k_pos]), jnp.asarray(h[:, rb.k_neg]))["H1"]
            q0, u0, v0 = reduce_qvu(jnp.asarray(h), dets["H1"]["d"], dets["H1"]["w"], DF, 1000, 200)
            e = max(_rel(q, q0), _rel(u, u0), _rel(v, v0))
            if store is None:
                assert e < 1e-4, e          # exact up to the ratio regularization
            else:
                store.append(e)
    assert errs[1] < errs[0] and errs[1] < 1e-4, errs
