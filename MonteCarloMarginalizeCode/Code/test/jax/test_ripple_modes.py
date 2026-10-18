"""ripple_modes: the JAX Q/U/V reduction matches gpu_precompute; ripple backend smoke test."""
import numpy as np
import pytest
import jax.numpy as jnp

from RIFT.likelihood import gpu_precompute as gp
from RIFT.likelihood.jax_ile.ripple_modes import conj_modes, reduce_qvu


def test_reduce_qvu_matches_compound_precompute():
    rng = np.random.default_rng(0)
    K, N = 3, 512
    h = rng.normal(size=(K, N)) + 1j * rng.normal(size=(K, N))
    d = rng.normal(size=N) + 1j * rng.normal(size=N)
    w = rng.uniform(0.1, 1.0, size=N)
    hc = np.conj(h[:, (N - np.arange(N)) % N])
    q0, u0, v0 = gp.compound_precompute_arrays(h[None], hc[None], d, w, 0.25, 1.0 / (N * 0.25),
                                               17, 64, backend=np)
    q, u, v = reduce_qvu(jnp.asarray(h), jnp.asarray(d), jnp.asarray(w), 0.25, 17, 64)
    assert np.allclose(np.asarray(q), q0[0].T, atol=1e-10)
    assert np.allclose(np.asarray(u), u0[0, 0], atol=1e-10)
    assert np.allclose(np.asarray(v), v0[0, 0], atol=1e-10)
    assert np.allclose(np.asarray(conj_modes(jnp.asarray(h))), hc)


def test_ripple_modes_smoke():
    pytest.importorskip("ripplegw")
    from RIFT.likelihood.jax_ile.ripple_modes import RippleXPHMModes
    df, N = 0.25, 8192
    f = -0.5 * N * df + df * np.arange(N)
    m = RippleXPHMModes(f, 20.0, 800.0, 20.0, l_max=2)
    h = np.asarray(m(30.0, 20.0, [0.4, 0.1, 0.2], [0.0, 0.1, -0.1]))
    assert h.shape == (5, N) and np.all(np.isfinite(h))
    band = (np.abs(f) >= 20) & (np.abs(f) <= 800)
    assert np.all(h[:, ~band] == 0) and np.max(np.abs(h[:, band])) > 0
