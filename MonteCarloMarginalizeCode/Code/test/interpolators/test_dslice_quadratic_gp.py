"""Known-answer tests for the per-point quadratic-in-1/d distance-slice model."""
import numpy as np

from RIFT.interpolators.dslice_quadratic_gp import fit_dslice_quadratic_gp, fit_points


def _grid(n_pts=300, k=20, noise=0.05, seed=2):
    rng = np.random.default_rng(seed)
    xi = rng.uniform(-1, 1, (n_pts, 2))
    A = 30 - 0.5 * np.sum((xi / 0.5) ** 2, 1)
    u0 = 1.0 / (1000 * (1 + 0.3 * xi[:, 0]))
    s = 0.25 * u0
    rows = []
    for i in range(n_pts):
        u = u0[i] + s[i] * rng.normal(0, 1, k)
        d = 1 / u
        y = A[i] - 0.5 * ((u - u0[i]) / s[i]) ** 2 + rng.normal(0, noise, k)
        rows.append(np.column_stack([y, noise * np.ones(k), np.repeat(xi[i:i + 1], k, 0), d]))
    g = np.vstack(rows)
    return g, xi, A, u0, s


def test_fit_points_recovers_per_point_parameters():
    g, xi, A, u0, s = _grid()
    pts = fit_points(g[:, 2:4], 1 / g[:, 4], g[:, 0], g[:, 1])
    order = np.lexsort(xi.T[::-1])
    assert len(pts["A"]) == len(A) and sum(pts["rejected"].values()) == 0
    assert np.max(np.abs(pts["A"] - A[order])) < 0.1
    assert np.max(np.abs(pts["u0"] / u0[order] - 1)) < 0.02
    assert np.max(np.abs(np.exp(pts["log_s"]) / s[order] - 1)) < 0.1


def test_model_predicts_held_out_rows():
    g, *_ = _grid(n_pts=500)
    X = g[:, [2, 3, 4]]                         # fit coords: x0, x1, dist (dist_index 2)
    model, rec, _ = fit_dslice_quadratic_gp(X[:8000], g[:8000, 0], g[:8000, 1], 2, max_train_points=300,
                                            fit_backend="numpy")
    res = model.predict(X[8000:]) - g[8000:, 0]
    near = g[8000:, 0] > g[:, 0].max() - 5
    assert np.sqrt(np.mean(res[near] ** 2)) < 0.3, np.sqrt(np.mean(res[near] ** 2))
