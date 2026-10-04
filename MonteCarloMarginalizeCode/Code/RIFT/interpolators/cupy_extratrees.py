"""ExtraTrees regression forest grown on the GPU, with sklearn's ExtraTreesRegressor algorithm.

Reproduces ExtraTreesRegressor(criterion='squared_error', max_features=1.0, min_samples_split=2,
min_samples_leaf=1, bootstrap=False) with sample weights. At every node and for every feature that is
not constant on the node (max <= min + 1e-7, sklearn's FEATURE_THRESHOLD, in float32 X), the threshold
is drawn uniformly on [min, max) (a draw equal to max is moved to min, as in sklearn); the split with
the largest weighted-MSE proxy improvement S_L^2/W_L + S_R^2/W_R is kept, ties (equal partitions)
broken uniformly as sklearn's random feature order does (tolerance 1e-13 of the proxy). A node is a leaf when it
holds fewer than two samples, its weighted impurity is <= double epsilon, or every feature is constant
on it; its value is the weighted mean of y. Samples go left when float32(x) <= threshold.

The trees are grown level by level, a group of trees at a time, one CUDA thread per (tree, sample)
pair. The random stream is cupy's, so a forest equals an sklearn forest in distribution, not draw for
draw; ``_reference_tree`` below is the same algorithm in numpy, driven by supplied uniforms, against
which a GPU tree grown from the same uniforms is checked exactly. Prediction is by
RIFT.interpolators.cupy_forest.CupyForest.
"""
import numpy as np

_FEATURE_THRESHOLD = 1e-7
_EPS = np.finfo(np.float64).eps

_KERNELS = r"""
__device__ __forceinline__ int ord_f(float f) { int i = __float_as_int(f); return i >= 0 ? i : i ^ 0x7FFFFFFF; }

extern "C" __global__
void node_stats(const int* __restrict__ act, const long long n_act, const int N, const int F,
                const float* __restrict__ X, const double* __restrict__ y, const double* __restrict__ w,
                const int* __restrict__ slot, double* W, double* S1, double* S2, int* cnt, int* cntw,
                int* fmin, int* fmax)
{
    long long k = (long long)blockDim.x * blockIdx.x + threadIdx.x;
    if (k >= n_act) return;
    int p = act[k]; int i = p % N; int s = slot[p];
    double wi = w[i], yi = y[i];
    atomicAdd(W + s, wi); atomicAdd(S1 + s, wi * yi); atomicAdd(S2 + s, wi * yi * yi); atomicAdd(cnt + s, 1);
    if (wi > 0) atomicAdd(cntw + s, 1);
    const float* x = X + (long long)i * F;
    for (int f = 0; f < F; f++) {
        int v = ord_f(x[f]);
        atomicMin(fmin + (long long)s * F + f, v); atomicMax(fmax + (long long)s * F + f, v);
    }
}

extern "C" __global__
void split_stats(const int* __restrict__ act, const long long n_act, const int N, const int F,
                 const float* __restrict__ X, const double* __restrict__ y, const double* __restrict__ w,
                 const int* __restrict__ slot, const unsigned char* __restrict__ splitting,
                 const double* __restrict__ thr, const unsigned char* __restrict__ cand, double* LW, double* LS,
                 int* LCW)
{
    long long k = (long long)blockDim.x * blockIdx.x + threadIdx.x;
    if (k >= n_act) return;
    int p = act[k]; int s = slot[p];
    if (!splitting[s]) return;
    int i = p % N;
    double wi = w[i], wy = wi * y[i];
    const float* x = X + (long long)i * F;
    for (int f = 0; f < F; f++) {
        long long sf = (long long)s * F + f;
        if (cand[sf] && (double)x[f] <= thr[sf]) {
            atomicAdd(LW + sf, wi); atomicAdd(LS + sf, wy);
            if (wi > 0) atomicAdd(LCW + sf, 1);
        }
    }
}

extern "C" __global__
void route(const int* __restrict__ act, const long long n_act, const int N, const int F,
           const float* __restrict__ X, int* slot, const unsigned char* __restrict__ splitting,
           const int* __restrict__ bfeat, const double* __restrict__ bthr, const int* __restrict__ child0,
           unsigned char* alive)
{
    long long k = (long long)blockDim.x * blockIdx.x + threadIdx.x;
    if (k >= n_act) return;
    int p = act[k]; int s = slot[p];
    if (!splitting[s]) { alive[k] = 0; return; }
    int i = p % N;
    float xv = X[(long long)i * F + bfeat[s]];
    slot[p] = child0[s] + (((double)xv <= bthr[s]) ? 0 : 1);
    alive[k] = 1;
}
"""


def _pick(xp, proxy, u_tie):
    """Best feature per row. Features whose proxy is within 1e-13 (relative) of the best are tied --
    typically several features give the same partition of a small node -- and one is chosen uniformly
    with u_tie, as sklearn's random feature order does. A fixed rule (argmax: lowest index) would bias
    which feature carries the cut. The tolerance is relative to the whole proxy, which includes the node
    offset S^2/W, so it must sit above the proxy's roundoff (a few eps for the small nodes where ties
    occur); improvements closer than 1e-13 S^2/W also count as tied."""
    pmax = xp.max(proxy, axis=1, keepdims=True)
    tied = (proxy >= pmax - 1e-13 * xp.abs(pmax)) & xp.isfinite(proxy)
    n_tied = xp.sum(tied, axis=1)
    j = xp.minimum(xp.floor(u_tie * n_tied), xp.maximum(n_tied - 1, 0)).astype(xp.int64)
    rank = xp.cumsum(tied, axis=1) - 1
    return xp.argmax(tied & (rank == j[:, None]), axis=1).astype(xp.int32)


def _decode_ord(cp, a):
    a = a.astype(cp.int32)
    return cp.where(a >= 0, a, a ^ 0x7FFFFFFF).view(cp.float32)


class _Tree:
    """sklearn-like tree_ arrays (host numpy) for CupyForest."""

    def __init__(self, left, right, feat, thr, val):
        self.children_left, self.children_right = left, right
        self.feature, self.threshold = feat, thr
        self.value = val[:, None, None]
        self.node_count = len(left)


class _Est:
    def __init__(self, tree):
        self.tree_ = tree


class CupyExtraTreesRegressor:
    """GPU ExtraTreesRegressor (single output). ``fit(X, y, sample_weight)``; ``forest()`` returns a
    CupyForest for prediction. ``estimators_`` hold sklearn-like ``tree_`` arrays on the host only if
    ``keep_host=True``."""

    def __init__(self, n_estimators=100, trees_per_group=None, random_state=None, verbose=False):
        self.n_estimators = int(n_estimators)
        self.trees_per_group = trees_per_group
        self.random_state = random_state
        self.verbose = verbose

    def fit(self, X, y, sample_weight=None, uniforms=None):
        """uniforms: optional callable (level, tree_ids, node_ids, n_features) -> (K, F) array of U[0,1)
        draws, used instead of the RNG (for the exact reference check)."""
        import cupy as cp
        Xh = np.ascontiguousarray(np.asarray(X, dtype=np.float32))
        N, F = Xh.shape
        Xd = cp.asarray(Xh)
        yd = cp.asarray(np.asarray(y, dtype=np.float64))
        wd = cp.asarray(np.ones(N) if sample_weight is None else np.asarray(sample_weight, dtype=np.float64))
        mod = cp.RawModule(code=_KERNELS)
        k_stats, k_split, k_route = (mod.get_function(n) for n in ("node_stats", "split_stats", "route"))
        rng = cp.random.RandomState(self.random_state if self.random_state is not None
                                    else np.random.SeedSequence().generate_state(1)[0])
        if self.trees_per_group:
            tpg = int(self.trees_per_group)
        else:
            # ~(64 F + 40) bytes per (tree, sample) at the widest level (measured 13.3 GB for 20 trees,
            # 1.2M rows, F = 9); use at most half the free device memory
            free = cp.cuda.Device().mem_info[0] + cp.get_default_memory_pool().free_bytes()
            tpg = int(0.5 * free // ((64 * F + 40) * N))
            if tpg < 1:
                raise MemoryError("cupy ExtraTrees: %.1f GB free is too little for one tree of %d rows" % (free / 1e9, N))
            tpg = min(tpg, self.n_estimators, int(2 ** 31 // (2 * N)) - 1, 20)
        self.n_features_in_ = F
        self._trees = []          # per tree: dict of device arrays
        TH = 256
        for g0 in range(0, self.n_estimators, tpg):
            T = min(tpg, self.n_estimators - g0)
            cap = 2 * N                                   # nodes per tree are at most 2N - 1
            left = cp.full(T * cap, -1, dtype=cp.int32); right = cp.full(T * cap, -1, dtype=cp.int32)
            feat = cp.full(T * cap, -2, dtype=cp.int32); thr_out = cp.full(T * cap, -2.0, dtype=cp.float64)
            val = cp.zeros(T * cap, dtype=cp.float64)
            n_nodes = np.ones(T, dtype=np.int64)          # root of each tree exists
            # frontier: K nodes, each (tree, node id); pairs carry their frontier slot
            ftree = cp.arange(T, dtype=cp.int32); fid = cp.zeros(T, dtype=cp.int32)
            slot = cp.repeat(cp.arange(T, dtype=cp.int32), N)
            act = cp.arange(T * N, dtype=cp.int32)
            level = 0
            while len(act):
                K = len(ftree)
                n_act = np.int64(len(act))
                blocks = (int((n_act + TH - 1) // TH),)
                W = cp.zeros(K); S1 = cp.zeros(K); S2 = cp.zeros(K)
                cnt = cp.zeros(K, dtype=cp.int32); cntw = cp.zeros(K, dtype=cp.int32)
                fmin = cp.full((K, F), np.iinfo(np.int32).max, dtype=cp.int32)
                fmax = cp.full((K, F), np.iinfo(np.int32).min, dtype=cp.int32)
                k_stats(blocks, (TH,), (act, n_act, np.int32(N), np.int32(F), Xd, yd, wd, slot, W, S1, S2, cnt, cntw, fmin, fmax))
                lo = _decode_ord(cp, fmin).astype(cp.float64); hi = _decode_ord(cp, fmax).astype(cp.float64)
                imp = S2 / W - (S1 / W) ** 2
                cand = ~(hi.astype(cp.float32) <= (lo.astype(cp.float32) + np.float32(_FEATURE_THRESHOLD)))
                splitting = (cnt >= 2) & (imp > _EPS) & cp.any(cand, axis=1)
                if uniforms is None:
                    u = rng.uniform(0.0, 1.0, (K, F + 1))
                else:
                    u = cp.asarray(uniforms(level, cp.asnumpy(ftree) + g0, cp.asnumpy(fid), F + 1), dtype=cp.float64)
                thr = (hi - lo) * u[:, :F] + lo
                thr = cp.where(thr == hi, lo, thr)
                LW = cp.zeros((K, F)); LS = cp.zeros((K, F)); LCW = cp.zeros((K, F), dtype=cp.int32)
                k_split(blocks, (TH,), (act, n_act, np.int32(N), np.int32(F), Xd, yd, wd, slot,
                                        splitting.astype(cp.uint8), thr, cand.astype(cp.uint8), LW, LS, LCW))
                RW = W[:, None] - LW; RS = S1[:, None] - LS
                # each side needs positive weight (sklearn's proxy is NaN otherwise, so the split loses);
                # tested on counts of positive-weight samples, since W - LW leaves roundoff
                valid = cand & (LCW > 0) & (cntw[:, None] - LCW > 0)
                proxy = cp.where(valid, LS * LS / cp.where(valid, LW, 1.0) + RS * RS / cp.where(valid, RW, 1.0), -cp.inf)
                bf = _pick(cp, proxy, u[:, F])
                splitting &= cp.isfinite(cp.max(proxy, axis=1))
                sp8 = splitting.astype(cp.uint8)
                bthr = thr[cp.arange(K), bf]
                # node records
                gid = ftree.astype(cp.int64) * cap + fid
                val[gid] = S1 / W
                # children: consecutive ids per tree, in frontier (= tree) order
                spi = splitting.astype(cp.int64)
                ns = cp.asnumpy(cp.bincount(ftree, weights=spi, minlength=T)).astype(np.int64)
                csum = cp.cumsum(spi) - spi                                   # global rank among splits
                tree_first = cp.asarray(np.r_[0, np.cumsum(ns)[:-1]])          # rank of the tree's first split
                rank_in_tree = csum - tree_first[ftree]
                child_id = cp.asarray(n_nodes)[ftree] + 2 * rank_in_tree
                sgid = gid[splitting]
                left[sgid] = child_id[splitting].astype(cp.int32)
                right[sgid] = (child_id[splitting] + 1).astype(cp.int32)
                feat[sgid] = bf[splitting]
                thr_out[sgid] = bthr[splitting]
                n_nodes += 2 * ns
                # next frontier: two children per splitting node, slot = 2 * global rank (+1)
                child0 = (2 * csum).astype(cp.int32)
                ftree_s = ftree[splitting]
                ftree = cp.repeat(ftree_s, 2)
                fid = cp.stack([child_id[splitting], child_id[splitting] + 1], axis=1).reshape(-1).astype(cp.int32)
                alive = cp.empty(len(act), dtype=cp.uint8)
                k_route(blocks, (TH,), (act, n_act, np.int32(N), np.int32(F), Xd, slot, sp8, bf, bthr, child0, alive))
                act = act[alive.astype(bool)]
                level += 1
            for t in range(T):
                n = int(n_nodes[t]); o = t * cap
                self._trees.append(dict(left=left[o:o + n].copy(), right=right[o:o + n].copy(), feat=feat[o:o + n].copy(),
                                        thr=thr_out[o:o + n].copy(), val=val[o:o + n].copy()))
            if self.verbose:
                print(" cupy ExtraTrees: trees %d-%d grown, %d levels, nodes %s" % (g0, g0 + T - 1, level, n_nodes.tolist()))
            del left, right, feat, thr_out, val, slot, act
        return self

    @property
    def estimators_(self):
        out = []
        for t in self._trees:
            lf = t["left"].get(); rt = t["right"].get()
            out.append(_Est(_Tree(lf, rt, t["feat"].get(), t["thr"].get(), t["val"].get())))
        return out

    def forest(self, release=False):
        """CupyForest for prediction. release=True frees the per-tree arrays (estimators_ then
        unavailable), so the forest is the only device copy."""
        import cupy as cp
        from RIFT.interpolators.cupy_forest import CupyForest
        f = CupyForest.from_device_trees(self._trees, self.n_features_in_)
        if release:
            self._trees = None
            cp.get_default_memory_pool().free_all_blocks()
        return f


def _reference_tree(X, y, w, uniforms, tree_id=0):
    """Same algorithm in numpy, breadth first, driven by uniforms(level, tree_ids, node_ids, F) like
    CupyExtraTreesRegressor.fit. Returns (left, right, feature, threshold, value) with the same node
    numbering (children of a level's splitting nodes numbered consecutively in frontier order)."""
    X = np.asarray(X, dtype=np.float32); N, F = X.shape
    left, right, feat, thr_o, val = [-1], [-1], [-2], [-2.0], [0.0]
    frontier = [(0, np.arange(N))]
    level = 0
    while frontier:
        u = np.asarray(uniforms(level, np.full(len(frontier), tree_id), np.array([n for n, _ in frontier]), F + 1))
        nxt = []
        for k, (nid, idx) in enumerate(frontier):
            ww, yy, xx = w[idx], y[idx], X[idx]
            W, S1, S2 = ww.sum(), (ww * yy).sum(), (ww * yy * yy).sum()
            pos = ww > 0
            val[nid] = S1 / W
            lo, hi = xx.min(axis=0).astype(np.float64), xx.max(axis=0).astype(np.float64)
            cand = ~(hi.astype(np.float32) <= lo.astype(np.float32) + np.float32(_FEATURE_THRESHOLD))
            if len(idx) < 2 or not (S2 / W - (S1 / W) ** 2 > _EPS) or not cand.any():
                continue
            t = (hi - lo) * u[k, :F] + lo
            t = np.where(t == hi, lo, t)
            goes_left = xx.astype(np.float64) <= t[None, :]
            LW = (ww[:, None] * goes_left).sum(0); LS = ((ww * yy)[:, None] * goes_left).sum(0)
            RW, RS = W - LW, S1 - LS
            lcw = (goes_left & pos[:, None]).sum(0)
            valid = cand & (lcw > 0) & (pos.sum() - lcw > 0)
            with np.errstate(divide="ignore", invalid="ignore"):
                proxy = np.where(valid, LS ** 2 / LW + RS ** 2 / RW, -np.inf)
            if not np.isfinite(proxy.max()):
                continue
            f = int(_pick(np, proxy[None, :], u[k, F:F + 1])[0])
            c = len(left)
            left[nid], right[nid], feat[nid], thr_o[nid] = c, c + 1, f, t[f]
            left += [-1, -1]; right += [-1, -1]; feat += [-2, -2]; thr_o += [-2.0, -2.0]; val += [0.0, 0.0]
            m = goes_left[:, f]
            nxt += [(c, idx[m]), (c + 1, idx[~m])]
        frontier = nxt
        level += 1
    return (np.array(left), np.array(right), np.array(feat), np.array(thr_o), np.array(val))
