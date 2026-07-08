"""
test_td_inner_product.py:  Unit tests for the TD Toeplitz inner-product adapter
(RIFT/likelihood/td_inner_product.py, task T1 of RIFT_TD_INTEGRATION_PLAN.md).

Requires lal + RIFT to import.  Runnable two ways:
    pytest test_td_inner_product.py
    python  test_td_inner_product.py       (runs every test, prints the acceptance numbers)

Acceptance tests (plan Sec. 6, task T1):
  (i)   test_complexip_vs_lsu      : ToeplitzComplexIP.ip vs lsu.ComplexIP.ip, <= 0.5% rel.
  (ii)  test_complexoverlap_vs_lsu : ToeplitzComplexOverlap rhoTS vs lsu.ComplexOverlap,
                                     <= 1% in max|.|, argmax bin exact (0 samples).
  (iii) test_hermiticity_symmetry  : ip(a,b) = conj(ip(b,a)); U/V-style matrix symmetries.
  (iv)  test_dense_vs_iterative    : dense vs tdanalysis PCG covariance solve, 1e-8 rel
                                     (skips cleanly with a message if tdanalysis absent).
Plus:
  test_array_psd_input             : numpy-array PSD (post resample_psd_series) form works.

Which conda env ran these: recorded at the top of the T1 report -- `junior_rift`
(lal 7.7.0), with PYTHONPATH pointing at MonteCarloMarginalizeCode/Code (and, for test
(iv), at a tdanalysis checkout, e.g. ~/vaishak_p/tdanalysis).
"""

import numpy as np

import lal
import RIFT.lalsimutils as lsu

try:
    import RIFT.likelihood.td_inner_product as tdip
except Exception:  # allow running from the test/ directory
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "RIFT", "likelihood"))
    import td_inner_product as tdip


# --------------------------------------------------------------------------- config / helpers

FS = 4096.0
FNYQ = FS / 2.0
N = 4096                    # two-sided length (long segment for a good match)
DELTAF = FS / N            # = 1.0
DELTAT = 1.0 / FS
FLOW = 40.0
FMAX = 1000.0
LEN1 = N // 2 + 1


def analytic_psd(f):
    """Smooth, strictly positive analytic one-sided PSD (aLIGO-ish, arbitrary scale)."""
    f = np.atleast_1d(np.asarray(f, dtype=float))
    fsafe = np.maximum(np.abs(f), 1.0)
    out = 1e-46 * (1.0 + (20.0 / fsafe) ** 4 + (fsafe / 250.0) ** 2)
    return out if out.size > 1 else float(out[0])


def make_bandlimited_series(rng, flow=FLOW, fmax=FMAX):
    """A random band-limited two-sided COMPLEX16FrequencySeries in lal packing."""
    fvals_1s = np.arange(LEN1) * DELTAF
    spec1 = np.zeros(LEN1, dtype=complex)
    band = (fvals_1s >= flow) & (fvals_1s <= fmax)
    spec1[band] = rng.standard_normal(band.sum()) + 1j * rng.standard_normal(band.sum())
    # lal two-sided packing exactly as lsu.InnerProduct.weights2side does:
    h2 = np.zeros(N, dtype=complex)
    h2[:LEN1] = spec1[::-1]
    h2[LEN1 - 1:] = spec1[:-1]
    hf = lal.CreateCOMPLEX16FrequencySeries(
        "h", lal.LIGOTimeGPS(0.), 0., DELTAF, lsu.lsu_HertzUnit, N)
    hf.data.data = h2
    return hf


def make_context(analyticPSD_Q=True, psd=None):
    if psd is None:
        psd = analytic_psd
    return tdip.ToeplitzInnerProductContext(
        psd, DELTAF, FNYQ, FLOW, FMAX, analyticPSD_Q=analyticPSD_Q)


# --------------------------------------------------------------------------- (i) ComplexIP

def test_complexip_vs_lsu():
    rng = np.random.default_rng(3)
    IP = lsu.ComplexIP(FLOW, FMAX, FNYQ, DELTAF, analytic_psd, analyticPSD_Q=True)
    ctx = make_context()
    tip = tdip.ToeplitzComplexIP(ctx)

    print("[T1-i] derived ip_normalization = 2*deltaT = %.6e (2/fs = %.6e)"
          % (ctx.ip_normalization, 2.0 / FS))

    rels = []
    for _ in range(8):
        h1 = make_bandlimited_series(rng)
        h2 = make_bandlimited_series(rng)
        a = IP.ip(h1, h2)
        b = tip.ip(h1, h2)
        rels.append(abs(a - b) / abs(a))
    rmax = float(np.max(rels))
    rmed = float(np.median(rels))
    print("[T1-i] ToeplitzComplexIP vs lsu.ComplexIP: max rel = %.3e, median = %.3e"
          % (rmax, rmed))
    assert rmax < 5e-3     # plan AC (i): <= 0.5%


# --------------------------------------------------------------------------- (ii) ComplexOverlap

def test_complexoverlap_vs_lsu():
    rng = np.random.default_rng(11)
    CO = lsu.ComplexOverlap(FLOW, FMAX, FNYQ, DELTAF, analytic_psd,
                            analyticPSD_Q=True, full_output=True)
    ctx = make_context()
    tov = tdip.ToeplitzComplexOverlap(ctx)

    maxrels = []
    idx_mismatch = 0
    for _ in range(6):
        hlm = make_bandlimited_series(rng)
        data = make_bandlimited_series(rng)
        r0, ts0, i0, p0 = CO.ip(hlm, data)
        r1, ts1, i1, p1 = tov.ip(hlm, data)
        d0 = np.abs(ts0.data.data)
        d1 = np.abs(ts1.data.data)
        maxrels.append(np.max(np.abs(d1 - d0)) / np.max(d0))
        if i0 != i1:
            idx_mismatch += 1
        # epoch / length contract for the FL:920-929 drop-in
        assert ts1.data.length == ctx.len2side
        assert abs(ts1.deltaT - DELTAT) <= 1e-12
    mr = float(np.max(maxrels))
    print("[T1-ii] ToeplitzComplexOverlap rhoTS vs lsu: max|.| rel = %.3e ; "
          "argmax mismatches = %d/6" % (mr, idx_mismatch))
    assert mr < 1e-2       # plan AC (ii): <= 1% in max|.|
    assert idx_mismatch == 0   # argmax bin exact (0 samples)


# --------------------------------------------------------------------------- (iii) symmetry

def test_hermiticity_symmetry():
    rng = np.random.default_rng(21)
    IP = lsu.ComplexIP(FLOW, FMAX, FNYQ, DELTAF, analytic_psd, analyticPSD_Q=True)
    ctx = make_context()
    tip = tdip.ToeplitzComplexIP(ctx)

    # ip(a,b) = conj(ip(b,a)) for the Hermitian usage pattern (conj on first arg).
    rels = []
    for _ in range(8):
        a = make_bandlimited_series(rng)
        b = make_bandlimited_series(rng)
        ab = tip.ip(a, b)
        ba = tip.ip(b, a)
        rels.append(abs(ab - np.conj(ba)) / abs(ab))
    rmax = float(np.max(rels))
    print("[T1-iii] |ip(a,b) - conj(ip(b,a))| / |ip(a,b)| max = %.3e" % rmax)
    assert rmax < 1e-10

    # cross-consistency with lsu on the same identity (both are Hermitian in this sense).
    a = make_bandlimited_series(rng)
    b = make_bandlimited_series(rng)
    lsu_ab = IP.ip(a, b)
    print("[T1-iii] lsu.ComplexIP Hermiticity |ab - conj(ba)|/|ab| = %.3e"
          % (abs(lsu_ab - np.conj(IP.ip(b, a))) / abs(lsu_ab)))

    # U/V-style matrix symmetry: the cross-term matrix U_ij = ip(h_i, h_j) built from a
    # set of modes is Hermitian (U = U^dagger).
    modes = [make_bandlimited_series(rng) for _ in range(4)]
    U = np.array([[tip.ip(hi, hj) for hj in modes] for hi in modes])
    herm = np.max(np.abs(U - U.conj().T)) / np.max(np.abs(U))
    print("[T1-iii] U-matrix Hermiticity ||U - U^H|| / ||U|| = %.3e" % herm)
    assert herm < 1e-10


# --------------------------------------------------------------------------- (iv) dense vs iterative

def test_dense_vs_iterative():
    try:
        from tdanalysis.forms.pcg import PCGQuadraticForm  # noqa: F401
    except Exception as exc:
        import pytest
        pytest.skip("tdanalysis not importable ({0!r}); iterative-path test skipped. "
                    "Point PYTHONPATH at a tdanalysis checkout to exercise it.".format(exc))
        return

    rng = np.random.default_rng(7)
    ctx = make_context()
    d = rng.standard_normal(ctx.N)
    x_dense = ctx.solve_covariance(d, method="dense")
    x_pcg = ctx.solve_covariance(d, method="pcg")
    rel = np.linalg.norm(x_pcg - x_dense) / np.linalg.norm(x_dense)
    print("[T1-iv] dense vs tdanalysis-PCG covariance solve rel = %.3e" % rel)
    assert rel < 1e-8      # plan AC (iv): dense vs iterative to 1e-8


# --------------------------------------------------------------------------- array-PSD input

def test_array_psd_input():
    rng = np.random.default_rng(5)
    # numpy-array PSD sampled on the one-sided rfft grid (what ILE holds post-resample).
    f1s = np.arange(LEN1) * DELTAF
    S_arr = analytic_psd(f1s)
    IP = lsu.ComplexIP(FLOW, FMAX, FNYQ, DELTAF, S_arr, analyticPSD_Q=False)
    ctx = make_context(analyticPSD_Q=False, psd=S_arr)
    tip = tdip.ToeplitzComplexIP(ctx)
    rels = []
    for _ in range(6):
        h1 = make_bandlimited_series(rng)
        h2 = make_bandlimited_series(rng)
        rels.append(abs(IP.ip(h1, h2) - tip.ip(h1, h2)) / abs(IP.ip(h1, h2)))
    rmax = float(np.max(rels))
    print("[T1-array] array-PSD ToeplitzComplexIP vs lsu: max rel = %.3e" % rmax)
    assert rmax < 5e-3


# --------------------------------------------------------------------------- script entry

if __name__ == "__main__":
    import time
    tests = [
        ("(i) ComplexIP vs lsu", test_complexip_vs_lsu),
        ("(ii) ComplexOverlap vs lsu", test_complexoverlap_vs_lsu),
        ("(iii) Hermiticity/symmetry", test_hermiticity_symmetry),
        ("(iv) dense vs iterative", test_dense_vs_iterative),
        ("array-PSD input", test_array_psd_input),
    ]
    t0 = time.time()
    failures = 0
    skipped = 0
    for name, fn in tests:
        print("\n=== %s ===" % name)
        try:
            fn()
            print("PASS: %s" % name)
        except AssertionError as e:
            failures += 1
            print("FAIL: %s : %s" % (name, e))
        except BaseException as e:
            # pytest.skip raises Skipped (a BaseException subclass, not Exception);
            # treat any skip-like outcome as a skip, re-raise everything else.
            if e.__class__.__name__ in ("Skipped", "OutcomeException"):
                skipped += 1
                print("SKIP: %s : %s" % (name, e))
            else:
                raise
    print("\nTotal runtime: %.2f s ; %d/%d passed (%d skipped)"
          % (time.time() - t0, len(tests) - failures - skipped, len(tests), skipped))
    raise SystemExit(1 if failures else 0)
