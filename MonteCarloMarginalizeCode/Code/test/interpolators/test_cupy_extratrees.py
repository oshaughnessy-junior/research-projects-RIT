"""GPU ExtraTrees (RIFT.interpolators.cupy_extratrees) against its numpy reference and sklearn.
Needs cupy and a device.

1. Exact: grown from the same supplied uniforms, the GPU tree equals the numpy reference node for node
   (ties, a discrete feature and sample weights included).
2. Tie breaking: with a feature duplicated, sklearn's random feature order splits on each copy about
   half the time. The GPU forest must too; argmax tie breaking (the deliberately broken variant)
   always takes the first copy and fails.
3. Distribution: held-out error and tree size match sklearn's ExtraTreesRegressor.
"""
import numpy as np
import pytest

try:
    import cupy as cp
    cp.zeros(1)
except Exception as err:
    pytest.skip("needs cupy and a CUDA device: %s" % type(err).__name__, allow_module_level=True)

from sklearn.ensemble import ExtraTreesRegressor

import RIFT.interpolators.cupy_extratrees as ce


def _data(n=3000, seed=0):
    rng = np.random.default_rng(seed)
    X = rng.uniform(-1, 1, (n, 4))
    X[:, 3] = np.round(X[:, 3], 1)
    y = np.sin(3 * X[:, 0]) * X[:, 1] + rng.normal(0, 0.1, n)
    return X, y, rng.uniform(0.5, 2.0, n)


def _uniforms(level, tids, nids, F):
    return np.array([np.random.default_rng([level, int(t), int(n)]).uniform(size=F) for t, n in zip(tids, nids)])


def test_gpu_tree_equals_reference_for_shared_uniforms():
    X, y, w = _data()
    t = ce.CupyExtraTreesRegressor(n_estimators=1).fit(X, y, w, uniforms=_uniforms).estimators_[0].tree_
    left, right, feat, thr, val = ce._reference_tree(X, y, w, _uniforms)
    assert np.array_equal(t.children_left, left) and np.array_equal(t.children_right, right)
    assert np.array_equal(t.feature, feat) and np.array_equal(t.threshold, thr)
    assert np.max(np.abs(t.value[:, 0, 0] - val)) < 1e-12


def _split_share_first_copy(trees):
    f = np.concatenate([t.tree_.feature for t in trees])
    f = f[f >= 0]
    return np.mean(f[(f == 0) | (f == 1)] == 0)


@pytest.fixture(scope="module")
def duplicated():
    X, y, w = _data(n=2000, seed=1)
    return np.c_[X[:, 0], X], y, w


def test_tied_features_share_splits_like_sklearn(duplicated):
    X, y, w = duplicated
    sk = _split_share_first_copy(ExtraTreesRegressor(10, random_state=0, n_jobs=1).fit(X, y, sample_weight=w).estimators_)
    gpu = _split_share_first_copy(ce.CupyExtraTreesRegressor(10, random_state=0).fit(X, y, w).estimators_)
    assert abs(sk - 0.5) < 0.05 and abs(gpu - 0.5) < 0.05, (sk, gpu)


def test_argmax_tie_breaking_is_caught(duplicated, monkeypatch):
    X, y, w = duplicated
    monkeypatch.setattr(ce, "_pick", lambda xp, proxy, u: xp.argmax(proxy, axis=1).astype(xp.int32))
    gpu = _split_share_first_copy(ce.CupyExtraTreesRegressor(10, random_state=0).fit(X, y, w).estimators_)
    assert abs(gpu - 0.5) > 0.05


def test_heldout_error_and_size_match_sklearn():
    X, y, w = _data()
    rng = np.random.default_rng(9)
    Xh = rng.uniform(-1, 1, (20000, 4)); Xh[:, 3] = np.round(Xh[:, 3], 1)
    yh = np.sin(3 * Xh[:, 0]) * Xh[:, 1]
    e_gpu, e_sk = [], []
    for seed in range(4):
        g = ce.CupyExtraTreesRegressor(30, random_state=seed).fit(X, y, w)
        f = g.forest()
        s = ExtraTreesRegressor(30, random_state=seed, n_jobs=1).fit(X, y, sample_weight=w)
        e_gpu.append(np.mean((cp.asnumpy(f.predict(Xh)) - yh) ** 2))
        e_sk.append(np.mean((s.predict(Xh) - yh) ** 2))
        assert f.n_nodes == sum(e.tree_.node_count for e in s.estimators_)    # fully grown: 2n - 1 per tree
    assert abs(np.mean(e_gpu) / np.mean(e_sk) - 1) < 0.08, (e_gpu, e_sk)


def test_zero_weights_match_reference_and_sklearn():
    """Samples with weight 0: a split needs positive weight on both sides (sklearn's proxy is NaN
    otherwise). Every tree of a multi-tree group must equal the reference, and no leaf may be NaN."""
    X, y, w = _data(n=2000, seed=5)
    w[np.random.default_rng(6).random(len(w)) < 0.3] = 0.0
    g = ce.CupyExtraTreesRegressor(n_estimators=4, trees_per_group=4).fit(X, y, w, uniforms=_uniforms)
    for k, e in enumerate(g.estimators_):
        t = e.tree_
        left, right, feat, thr, val = ce._reference_tree(X, y, w, _uniforms, tree_id=k)
        assert np.array_equal(t.children_left, left) and np.array_equal(t.feature, feat), k
        assert np.array_equal(t.threshold, thr) and np.all(np.isfinite(t.value)), k
        assert np.max(np.abs(t.value[:, 0, 0] - val)) < 1e-12, k
    rng = np.random.default_rng(9)
    Xh = rng.uniform(-1, 1, (20000, 4)); Xh[:, 3] = np.round(Xh[:, 3], 1)
    yh = np.sin(3 * Xh[:, 0]) * Xh[:, 1]
    f = ce.CupyExtraTreesRegressor(20, random_state=1).fit(X, y, w).forest(release=True)
    p = cp.asnumpy(f.predict(Xh))
    s = ExtraTreesRegressor(20, random_state=1, n_jobs=1).fit(X, y, sample_weight=w)
    # sklearn grows on positive weights only: fully grown, 2 n_pos - 1 nodes per tree (distinct rows)
    assert f.n_nodes == sum(e.tree_.node_count for e in s.estimators_)
    assert np.all(np.isfinite(p))
    assert np.mean((p - yh) ** 2) < 1.3 * np.mean((s.predict(Xh) - yh) ** 2)
