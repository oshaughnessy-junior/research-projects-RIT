"""Known-answer tests for the averaged-amplitude distance model."""
import numpy as np
from scipy.integrate import quad

from RIFT.interpolators.dslice_amplitude_model import DistanceAmplitudeModel, fit_point, log_model


def test_closed_form_matches_quadrature_including_deep_tails():
    R, us, fm = 40.0, 1 / 3000.0, 0.4
    for d in [300, 500, 1500, 3000, 6000, 9000, 1e5, 1e7]:
        x = (1 / d) / us
        num = np.log(quad(lambda f: np.exp(R * (1 - (1 - f * x) ** 2)), fm, 1, epsabs=0, epsrel=1e-12)[0] / (1 - fm))
        assert abs(log_model(1 / d, R, us, fm, 0.0) - num) < 1e-6, d


def test_far_limit_is_zero_and_flat_top_is_R():
    R, us, fm = 30.0, 1 / 2000.0, 0.5
    assert abs(log_model(1e-9, R, us, fm, 0.0)) < 1e-4
    top = log_model(np.linspace(us * 1.1, us / fm * 0.9, 20), R, us, fm, 0.0)
    assert np.all(np.abs(top - R) < 1.5)      # approximately flat across the degenerate range


def test_point_fit_reproduces_the_conditional_distance_posterior():
    """Slices placed over the central part of the conditional (as the export does); the fitted model's
    distance posterior with a d^2 prior must match the true one, tails included."""
    R, us, fm = 40.0, 1 / 3000.0, 0.4
    rng = np.random.default_rng(0)
    d = np.linspace(1400, 3200, 50)
    y = log_model(1 / d, R, us, fm, 0.0) + rng.normal(0, 0.15, 50)
    p, rms = fit_point(1 / d, y, np.ones(50) / 0.15 ** 2, fix_C=True)
    assert rms < 0.2 and p[3] == 0.0
    grid = np.linspace(300, 10000, 20000)

    def quantiles(lnl):
        w = np.exp(lnl - lnl.max()) * grid ** 2
        c = np.cumsum(w) / w.sum()
        return np.interp([0.01, 0.05, 0.5, 0.95, 0.99], c, grid)

    qt, qf = quantiles(log_model(1 / grid, R, us, fm, 0.0)), quantiles(log_model(1 / grid, *p))
    err = np.abs(qf / qt - 1)
    # one point, 50 noisy slices: the 1/99% tails carry more fit noise than the 5-95% body
    assert np.max(err[1:4]) < 0.03 and np.max(err[[0, 4]]) < 0.06, (qt, qf)


def test_model_interpolates_fields_over_intrinsic_coordinates():
    rng = np.random.default_rng(1)
    rows = []
    for x0 in rng.uniform(-1, 1, 200):
        R, us = 30 + 5 * x0, 1 / (3000 * (1 + 0.2 * x0))
        d = rng.uniform(1500, 9000, 30)
        rows.append(np.column_stack([np.full(30, x0), d, log_model(1 / d, R, us, 0.4, 0.0) + rng.normal(0, 0.1, 30)]))
    g = np.vstack(rows)
    m = DistanceAmplitudeModel(dist_index=1, n_jobs=1).fit(g[:, :2], g[:, 2], 0.1 * np.ones(len(g)))
    assert m.report["points_fit"] == 200
    q = np.column_stack([np.zeros(5), np.array([2000, 3000, 4000, 6000, 8000.0])])
    truth = log_model(1 / q[:, 1], 30.0, 1 / 3000.0, 0.4, 0.0)
    assert np.max(np.abs(m.predict(q) - truth)) < 1.0
