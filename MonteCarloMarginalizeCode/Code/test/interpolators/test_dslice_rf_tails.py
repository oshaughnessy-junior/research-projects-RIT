"""RFDistanceTails: base fit kept inside each point's slices, physical continuation outside."""
import numpy as np

from RIFT.interpolators.dslice_amplitude_model import log_model
from RIFT.interpolators.dslice_rf_tails import RFDistanceTails


def _grid(seed=4):
    rng = np.random.default_rng(seed)
    rows = []
    for x0 in rng.uniform(-1, 1, 120):
        R, us = 30 + 5 * x0, 1 / (3000 * (1 + 0.2 * x0))
        d = np.linspace(1800, 4000, 30)
        rows.append(np.column_stack([np.full(30, x0), d, log_model(1 / d, R, us, 0.4, 0.0) + rng.normal(0, 0.05, 30)]))
    return np.vstack(rows)


def test_inside_is_base_outside_is_continuation():
    g = _grid()
    flat = lambda x: np.full(len(x), 7.0)          # a 'tree' that is flat everywhere
    m = RFDistanceTails(flat, dist_index=1, sides="both").fit(g[:, :2], g[:, 2], 0.05 * np.ones(len(g)))
    x0 = g[0, 0]
    inside = m(np.array([[x0, 2500.0]]))
    assert inside[0] == 7.0
    near, far = m(np.array([[x0, 600.0]])), m(np.array([[x0, 9000.0]]))
    assert near[0] < 7.0 - 1 and far[0] < 7.0 - 1      # both tails fall away from the flat edge value
    near_only = RFDistanceTails(flat, 1, sides="near").fit(g[:, :2], g[:, 2], 0.05 * np.ones(len(g)))
    assert near_only(np.array([[x0, 9000.0]]))[0] == 7.0 and near_only(np.array([[x0, 600.0]]))[0] < 6.0


def test_shared_fmin_is_reported():
    g = _grid()
    m = RFDistanceTails(lambda x: np.zeros(len(x)), 1).fit(g[:, :2], g[:, 2], 0.05 * np.ones(len(g)))
    assert abs(m.report["fmin_shared"] - 0.4) < 0.1
