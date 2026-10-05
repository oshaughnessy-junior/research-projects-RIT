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


def test_continuation_never_rises_above_the_edge():
    # slices only on the far side of the peak: the fitted shape rises toward small d beyond the data
    rng = np.random.default_rng(5)
    rows = []
    for x0 in rng.uniform(-1, 1, 80):
        d = np.linspace(4000, 7000, 30)
        rows.append(np.column_stack([np.full(30, x0), d, log_model(1 / d, 30.0, 1 / 2000.0, 0.5, 0.0) + rng.normal(0, 0.05, 30)]))
    g = np.vstack(rows)
    m = RFDistanceTails(lambda x: np.full(len(x), 5.0), 1, sides="both").fit(g[:, :2], g[:, 2], 0.05 * np.ones(len(g)))
    assert np.all(m(np.column_stack([np.full(5, g[0, 0]), [500.0, 1000.0, 2000.0, 9000.0, 20000.0]])) <= 5.0)


def test_point_fit_scipy_default_and_batched_option():
    g = _grid()
    m = RFDistanceTails(lambda x: np.zeros(len(x)), 1).fit(g[:, :2], g[:, 2], 0.05 * np.ones(len(g)))
    b = RFDistanceTails(lambda x: np.zeros(len(x)), 1, point_fit="batched").fit(g[:, :2], g[:, 2], 0.05 * np.ones(len(g)))
    assert m.report["point_fit"] == "scipy" and b.report["point_fit"] == "batched"
    _, inv = np.unique(np.round(g[:, :1], 10), axis=0, return_inverse=True)
    inv = inv.reshape(-1)
    cost = lambda P: np.sum((log_model(1 / g[:, 1], *P[inv].T[:3], 0.0) - g[:, 2]) ** 2)
    # two fitters (each with its own shared f_min): scipy reaches at most the batched cost
    assert cost(m.P) <= cost(b.P) * (1 + 1e-6)


def test_driver_refuses_tail_options_outside_rf(tmp_path):
    # parse-time refusals: tails need rf; point-fit options on rf need the tails
    import os
    import subprocess
    import sys
    code = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    driver = os.path.join(code, "bin", "util_ConstructEOSPosterior.py")
    env = dict(os.environ, PYTHONPATH=code, OMP_NUM_THREADS="1", MPLBACKEND="Agg", CUDA_VISIBLE_DEVICES="",
               MPLCONFIGDIR=str(tmp_path))
    base = [sys.executable, driver, "--fname", str(tmp_path / "missing.dat"), "--parameter", "xx"]
    for extra, msg in ((["--fit-method", "dslice-amp", "--rf-dslice-tails", "near"], "apply to --fit-method rf only"),
                       (["--fit-method", "rf", "--dslice-amp-point-fit-jobs", "2"], "apply to --fit-method dslice-amp only"),
                       (["--fit-method", "rf", "--rf-dslice-tails-fmin", "free"], "needs --rf-dslice-tails")):
        p = subprocess.run(base + extra, env=env, cwd=str(tmp_path), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           universal_newlines=True, timeout=300)
        assert p.returncode == 2 and msg in p.stdout, p.stdout[-2000:]
