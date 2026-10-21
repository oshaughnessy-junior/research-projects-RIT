"""
Time-indexed cross terms in DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop.

For the time-domain fixed-segment code path the template self term <h|h> depends on
the arrival time, so U and V are supplied per detector as (npts_full, n_lm, n_lm)
arrays on the rholm time grid.  These checks run on CPU (xpy=np) with synthetic
random Q/U/V and random extrinsic parameters:

  1. scalar (2-D) U path is bit-identical to the parent-commit code (stored
     hex-exact reference values, same deterministic inputs);
  2. time-indexed U that is constant in time gives exactly the scalar-U result;
  3. genuinely time-varying U against a brute-force per-sample, per-time loop,
     for nearest and cubic time_interp, with and without phase marginalization,
     with return_lnLt, with an elementwise custom loglikelihood (the distance
     marginalization hook), with mixed scalar/time-indexed detectors, and with
     a window running past the buffer edge (zero extension);
  4. refusals: calibration paths, the legacy array likelihoods, and bad shapes.

Run with:  pytest test/test_td_time_indexed_U.py
"""
import numpy as np
import pytest
import lal
import lalsimulation as lalsim

import RIFT.likelihood.factored_likelihood as fl
from RIFT.likelihood.vectorized_lal_tools import ComputeDetAMResponse, TimeDelayFromEarthCenter
from RIFT.likelihood.SphericalHarmonics_gpu import SphericalHarmonicsVectorized


class _PVec(object):
    pass


def _make_case(seed=7, dets=("H1", "L1", "V1"), modes=((2, 2), (2, -2), (2, 1)),
               npts_full=256, npts=12, npts_extrinsic=5, deltaT=1.0/4096,
               epoch_offset=-0.03, hermitian=True):
    """Synthetic inputs.  Returns a dict with everything the likelihood needs.

    U_t[det] is (npts_full, n_lm, n_lm): Hermitian at each time when
    hermitian=True (V_t symmetric), otherwise fully random.  U_s[det] is the
    (n_lm, n_lm) scalar cross term equal to U_t at time index 0 (used for the
    constant-in-time check).
    """
    rng = np.random.default_rng(seed)
    n_lm = len(modes)
    lookupNKDict = {det: np.array(modes, dtype=int) for det in dets}
    rholmsArrayDict = {}
    U_t, V_t, U_s, V_s, epochDict = {}, {}, {}, {}, {}
    P = _PVec()
    P.phi = rng.uniform(0, 2*np.pi, npts_extrinsic)
    P.theta = rng.uniform(0.2, np.pi-0.2, npts_extrinsic)
    P.psi = rng.uniform(0, np.pi, npts_extrinsic)
    P.incl = rng.uniform(0.2, np.pi-0.2, npts_extrinsic)
    P.phiref = rng.uniform(0, 2*np.pi, npts_extrinsic)
    P.dist = rng.uniform(300.0, 900.0, npts_extrinsic) * (lal.PC_SI*1e6)
    P.tref = 1000000000.0
    P.deltaT = deltaT
    for det in dets:
        rholmsArrayDict[det] = (rng.standard_normal((n_lm, npts_full))
                                + 1j*rng.standard_normal((n_lm, npts_full)))
        A = rng.standard_normal((npts_full, n_lm, n_lm)) + 1j*rng.standard_normal((npts_full, n_lm, n_lm))
        B = rng.standard_normal((npts_full, n_lm, n_lm)) + 1j*rng.standard_normal((npts_full, n_lm, n_lm))
        if hermitian:
            A = A + np.conj(np.swapaxes(A, 1, 2))
            B = B + np.swapaxes(B, 1, 2)
        U_t[det] = A
        V_t[det] = B
        U_s[det] = A[0].copy()
        V_s[det] = B[0].copy()
        epochDict[det] = P.tref + epoch_offset
    tvals = np.linspace(-npts//2*deltaT, npts//2*deltaT, npts)
    return dict(P=P, tvals=tvals, lookupNKDict=lookupNKDict, rholmsArrayDict=rholmsArrayDict,
                U_t=U_t, V_t=V_t, U_s=U_s, V_s=V_s, epochDict=epochDict, npts=npts,
                npts_extrinsic=npts_extrinsic, deltaT=deltaT, dets=list(dets))


def _call(case, U, V, **kw):
    return fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(
        case["tvals"], case["P"], case["lookupNKDict"], case["rholmsArrayDict"],
        U, V, case["epochDict"], Lmax=2, xpy=np, **kw)


def _cubic_weights(u):
    return (-u*(u-1.0)*(u-2.0)/6.0,
            (u+1.0)*(u-1.0)*(u-2.0)/2.0,
            -(u+1.0)*u*(u-2.0)/2.0,
            (u+1.0)*u*(u-1.0)/6.0)


def _brute_force(case, U, V, time_interp='nearest', phase_marginalization=False,
                 loglikelihood=None, return_lnLt=False):
    """Per-sample, per-time loop implementing
        lnL_k(e,t) = (D_ref/D) Re[sum_lm conj(F Y_lm) Q_lm(t)]
                     - (D_ref/D)^2/4 Re[ |F|^2 sum conj(Y_lm) Y_l'm' U + F^2 sum Y_lm Y_l'm' V ]
    with Q, U, V looked up at the same (nearest or cubic-interpolated) index.
    U[det], V[det] may be 2-D (time independent) or 3-D (time indexed).
    """
    P = case["P"]
    tvals = case["tvals"]
    npts = case["npts"]
    ne = case["npts_extrinsic"]
    deltaT = float(P.deltaT)
    distMpc = P.dist/(lal.PC_SI*1e6)
    invD = fl.distMpcRef/distMpc
    gmst = lal.GreenwichMeanSiderealTime(P.tref)
    kappa = np.zeros((ne, npts), dtype=complex)
    rho_sq = np.zeros((ne, npts))
    for det in case["dets"]:
        detector = lalsim.DetectorPrefixToLALDetector(det)
        lms = case["lookupNKDict"][det]
        n_lm = len(lms)
        Y = SphericalHarmonicsVectorized(lms, P.incl, -P.phiref, xpy=np, l_max=2)
        F = ComputeDetAMResponse(detector.response, P.phi, P.theta, P.psi, gmst, xpy=np)
        t_det = float(P.tref - float(case["epochDict"][det])) + TimeDelayFromEarthCenter(
            detector.location, P.phi, P.theta, float(gmst), xpy=np)
        sample_first = (t_det + tvals[0])/deltaT
        Q = case["rholmsArrayDict"][det].T          # (npts_full, n_lm)
        n_time = Q.shape[0]
        Ud = U[det]
        Vd = V[det]
        for e in range(ne):
            if time_interp == 'nearest':
                i0 = int(np.rint(sample_first[e]) + 0.5)
                stencil = [(0, 1.0)]
            else:
                i0 = int(np.floor(sample_first[e]))
                stencil = list(zip((-1, 0, 1, 2), _cubic_weights(sample_first[e] - np.floor(sample_first[e]))))
            for j in range(npts):
                Q_here = np.zeros(n_lm, dtype=complex)
                U_here = np.zeros((n_lm, n_lm), dtype=complex)
                V_here = np.zeros((n_lm, n_lm), dtype=complex)
                for off, w in stencil:
                    idx = i0 + j + off
                    if 0 <= idx < n_time:
                        Q_here += w*Q[idx]
                        U_here += w*(Ud[idx] if Ud.ndim == 3 else Ud)
                        V_here += w*(Vd[idx] if Vd.ndim == 3 else Vd)
                k = 0j
                for a in range(n_lm):
                    term = np.conj(F[e]*Y[e, a])*Q_here[a]
                    if phase_marginalization and a == 1:
                        # the whole (2,-2) contribution (F, Y and Q) is conjugated
                        term = np.conj(term)
                    k += term
                kappa[e, j] += invD[e]*k
                s = 0j
                for a in range(n_lm):
                    for b in range(n_lm):
                        s += (F[e]*np.conj(F[e]))*np.conj(Y[e, a])*Y[e, b]*U_here[a, b]
                        s += F[e]*F[e]*Y[e, a]*Y[e, b]*V_here[a, b]
                rho_sq[e, j] += 0.5*invD[e]**2*s.real
    if loglikelihood is None:
        loglikelihood = lambda kap, rho: kap - 0.5*rho
    k_in = np.abs(kappa) if phase_marginalization else kappa.real
    lnL_t = loglikelihood(k_in, rho_sq)
    if return_lnLt:
        return lnL_t
    # per-row offset, as the likelihood takes it (a single batch maximum underflows quiet rows)
    m = np.max(lnL_t, axis=-1, keepdims=True)
    L = fl.my_simps(np.exp(lnL_t - m), dx=deltaT, axis=-1)
    return m[:, 0] + np.log(L)


# --------------------------------------------------------------------------
# 1. scalar path bit-identical to the parent commit
# --------------------------------------------------------------------------
# Reference lnL (hex floats) from DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop
# at the parent commit (ddc2d99e), on _make_case(seed=7) with the scalar U_s/V_s,
# for (time_interp, phase_marginalization) in the order below.
_SCALAR_REFERENCE = {
    ('nearest', False): ['-0x1.8b78573b6077ep+2', '-0x1.69f6e851c2512p+2', '-0x1.81443504c9ec2p+2', '-0x1.6a71c1e4a2e94p+2', '-0x1.63cac0935a0c2p+2'],
    ('nearest', True): ['-0x1.629d5b63a02c0p+2', '-0x1.0fe1e4000425fp+2', '-0x1.590ec83cc02eep+2', '-0x1.51325ea047cc8p+2', '-0x1.3168217666e44p+2'],
    ('cubic', False): ['-0x1.8b4c87601802ep+2', '-0x1.6c3c22b249c90p+2', '-0x1.80dd0045f38f8p+2', '-0x1.6ac000166eb86p+2', '-0x1.6b7851ccaed10p+2'],
    ('cubic', True): ['-0x1.6a79ffc519bb2p+2', '-0x1.1328980112586p+2', '-0x1.61e85eed08db0p+2', '-0x1.579687f4bb618p+2', '-0x1.3bc0e01ddb03ep+2'],
}


@pytest.mark.parametrize("time_interp,phase_marg", sorted(_SCALAR_REFERENCE.keys()))
def test_scalar_path_bit_identical_to_parent(time_interp, phase_marg):
    case = _make_case(seed=7, modes=((2, 2), (2, -2)))
    lnL = _call(case, case["U_s"], case["V_s"], time_interp=time_interp, phase_marginalization=phase_marg)
    ref = np.array([float.fromhex(h) for h in _SCALAR_REFERENCE[(time_interp, phase_marg)]])
    assert ref.shape == lnL.shape
    # The stored values were computed on one machine; other numpy/BLAS builds differ at the
    # level of rounding (~1e-15 relative), so compare to 1e-12 rather than bit for bit.
    np.testing.assert_allclose(np.asarray(lnL), ref, rtol=1e-12, atol=0,
                               err_msg="scalar path changed")


def test_scalar_path_against_brute_force():
    case = _make_case(seed=11)
    for time_interp in ('nearest', 'cubic'):
        lnL = _call(case, case["U_s"], case["V_s"], time_interp=time_interp)
        ref = _brute_force(case, case["U_s"], case["V_s"], time_interp=time_interp)
        np.testing.assert_allclose(lnL, ref, rtol=1e-10, atol=1e-10)


# --------------------------------------------------------------------------
# 2. constant-in-time U reproduces the scalar result exactly
# --------------------------------------------------------------------------
@pytest.mark.parametrize("time_interp", ['nearest', 'cubic'])
@pytest.mark.parametrize("phase_marg", [False, True])
def test_constant_time_indexed_equals_scalar(time_interp, phase_marg):
    case = _make_case(seed=3, modes=((2, 2), (2, -2)))
    U_const = {det: np.broadcast_to(case["U_s"][det], (256, 2, 2)).copy() for det in case["dets"]}
    V_const = {det: np.broadcast_to(case["V_s"][det], (256, 2, 2)).copy() for det in case["dets"]}
    kw = dict(time_interp=time_interp, phase_marginalization=phase_marg)
    lnL_s = _call(case, case["U_s"], case["V_s"], **kw)
    lnL_t = _call(case, U_const, V_const, **kw)
    # Same quantities, summed in a different order: agreement to a few ulp.
    np.testing.assert_allclose(lnL_t, lnL_s, rtol=1e-13, atol=0)
    lnLt_s = _call(case, case["U_s"], case["V_s"], return_lnLt=True, **kw)
    lnLt_t = _call(case, U_const, V_const, return_lnLt=True, **kw)
    assert lnLt_t.shape == (case["npts_extrinsic"], case["npts"])
    np.testing.assert_allclose(lnLt_t, lnLt_s, rtol=1e-13, atol=1e-12)


def test_constant_time_indexed_self_term_exact():
    # The self term itself: the time-indexed code path gathers the same matrix at every
    # window index, so it must equal the scalar einsum to roundoff.
    case = _make_case(seed=5)
    det = "H1"
    n_lm = 3
    rng = np.random.default_rng(0)
    ne = 4
    F = rng.standard_normal(ne) + 1j*rng.standard_normal(ne)
    Y = rng.standard_normal((ne, n_lm)) + 1j*rng.standard_normal((ne, n_lm))
    U = case["U_s"][det]
    V = case["V_s"][det]
    ref = ((F*np.conj(F)).real*np.einsum("...i,...j,ij", np.conj(Y), Y, U).real
           + (np.square(F)*np.einsum("...i,...j,ij", Y, Y, V)).real)
    U3 = np.broadcast_to(U, (40, n_lm, n_lm)).copy()
    V3 = np.broadcast_to(V, (40, n_lm, n_lm)).copy()
    ifirst = np.array([3, 5, 0, 20], dtype=np.int32)
    got = fl._time_indexed_self_term(U3, V3, F, Y, ifirst, None, 8, 'nearest', xpy=np)
    assert got.shape == (ne, 8)
    np.testing.assert_allclose(got, ref[:, None]*np.ones((1, 8)), rtol=1e-13, atol=0)
    frac = np.array([0.1, 0.7, 0.0, 0.5])
    got_c = fl._time_indexed_self_term(U3, V3, F, Y, ifirst, frac, 8, 'cubic', xpy=np)
    np.testing.assert_allclose(got_c, ref[:, None]*np.ones((1, 8)), rtol=1e-12, atol=0)


# --------------------------------------------------------------------------
# 3. genuinely time-varying U against the brute-force loop
# --------------------------------------------------------------------------
@pytest.mark.parametrize("time_interp", ['nearest', 'cubic'])
@pytest.mark.parametrize("phase_marg", [False, True])
@pytest.mark.parametrize("hermitian", [True, False])
def test_time_varying_against_brute_force(time_interp, phase_marg, hermitian):
    case = _make_case(seed=23, modes=((2, 2), (2, -2)), hermitian=hermitian)
    kw = dict(time_interp=time_interp, phase_marginalization=phase_marg)
    lnL = _call(case, case["U_t"], case["V_t"], **kw)
    ref = _brute_force(case, case["U_t"], case["V_t"], **kw)
    np.testing.assert_allclose(lnL, ref, rtol=1e-10, atol=1e-10)
    lnLt = _call(case, case["U_t"], case["V_t"], return_lnLt=True, **kw)
    reft = _brute_force(case, case["U_t"], case["V_t"], return_lnLt=True, **kw)
    assert lnLt.shape == (case["npts_extrinsic"], case["npts"])
    np.testing.assert_allclose(lnLt, reft, rtol=1e-10, atol=1e-10)
    # the self term must vary along the time axis
    rho_only = _call(case, case["U_t"], case["V_t"], return_lnLt=True,
                     loglikelihood=lambda k, r: r, **kw)
    assert np.max(np.abs(np.diff(rho_only, axis=-1))) > 1e-3


@pytest.mark.parametrize("time_interp", ['nearest', 'cubic'])
def test_time_varying_three_modes(time_interp):
    case = _make_case(seed=29, modes=((2, 2), (2, -2), (2, 1)), hermitian=True)
    lnL = _call(case, case["U_t"], case["V_t"], time_interp=time_interp)
    ref = _brute_force(case, case["U_t"], case["V_t"], time_interp=time_interp)
    np.testing.assert_allclose(lnL, ref, rtol=1e-10, atol=1e-10)


@pytest.mark.parametrize("time_interp", ['nearest', 'cubic'])
def test_custom_loglikelihood_sees_time_dependent_rho_sq(time_interp):
    # Elementwise loglikelihood (the shape the distance-marginalization table
    # helper has) must receive rho_sq of shape (npts_extrinsic, npts).
    seen = {}

    def loglike(kappa_sq, rho_sq):
        seen['shape'] = rho_sq.shape
        seen['varies'] = float(np.max(np.abs(np.diff(rho_sq, axis=-1))))
        return kappa_sq - 0.5*rho_sq + 0.1*np.log1p(np.abs(rho_sq))

    case = _make_case(seed=31, modes=((2, 2), (2, -2)))
    lnL = _call(case, case["U_t"], case["V_t"], time_interp=time_interp, loglikelihood=loglike)
    assert seen['shape'] == (case["npts_extrinsic"], case["npts"])
    assert seen['varies'] > 1e-3
    ref = _brute_force(case, case["U_t"], case["V_t"], time_interp=time_interp, loglikelihood=loglike)
    np.testing.assert_allclose(lnL, ref, rtol=1e-10, atol=1e-10)


@pytest.mark.parametrize("time_interp", ['nearest', 'cubic'])
def test_mixed_scalar_and_time_indexed_detectors(time_interp):
    case = _make_case(seed=37, modes=((2, 2), (2, -2)))
    U = dict(case["U_t"]); V = dict(case["V_t"])
    U["L1"] = case["U_s"]["L1"]; V["L1"] = case["V_s"]["L1"]
    lnL = _call(case, U, V, time_interp=time_interp)
    ref = _brute_force(case, U, V, time_interp=time_interp)
    np.testing.assert_allclose(lnL, ref, rtol=1e-10, atol=1e-10)


@pytest.mark.parametrize("time_interp", ['nearest', 'cubic'])
def test_window_past_buffer_edge_zero_extension(time_interp):
    # epoch close to tref -> ifirst near 0, some windows start before the buffer
    case = _make_case(seed=41, modes=((2, 2), (2, -2)), epoch_offset=-0.0005, npts_full=64)
    lnL = _call(case, case["U_t"], case["V_t"], time_interp=time_interp, return_lnLt=True)
    ref = _brute_force(case, case["U_t"], case["V_t"], time_interp=time_interp, return_lnLt=True)
    np.testing.assert_allclose(lnL, ref, rtol=1e-10, atol=1e-10)


# --------------------------------------------------------------------------
# 4. refusals
# --------------------------------------------------------------------------
def test_refuse_calibration_paths():
    case = _make_case(seed=43, modes=((2, 2), (2, -2)))
    with pytest.raises(NotImplementedError):
        _call(case, case["U_t"], case["V_t"], n_cal=2)
    with pytest.raises(NotImplementedError):
        _call(case, case["U_t"], case["V_t"], n_cal=2, cal_method='fused')
    U_cal = {det: case["U_s"][det][None] for det in case["dets"]}
    V_cal = {det: case["V_s"][det][None] for det in case["dets"]}
    with pytest.raises(NotImplementedError):
        _call(case, case["U_t"], case["V_t"], ctUArrayDict_cal=U_cal, ctVArrayDict_cal=V_cal)


def test_refuse_options_not_implemented_for_time_indexed():
    # stencils, pregrids and quadratures the time-indexed self term does not implement
    case = _make_case(seed=47, modes=((2, 2), (2, -2)))
    dT = float(case["P"].deltaT)
    for kw in (dict(time_interp='sinc'),
               dict(time_quadrature='bandlimited'),
               dict(time_quadrature='peak-local'),
               dict(explicit_time_values=True),
               dict(time_interp='cubic', q_deltaT=dT / 2)):
        with pytest.raises(NotImplementedError):
            _call(case, case["U_t"], case["V_t"], **kw)
    # the fixed-grid trapezoid rule applies to any integrand
    _call(case, case["U_t"], case["V_t"], time_quadrature='trapezoid')


def test_refuse_bad_shapes():
    case = _make_case(seed=47, modes=((2, 2), (2, -2)))
    with pytest.raises(ValueError):            # U 3-D, V 2-D
        _call(case, case["U_t"], case["V_s"])
    U_short = {det: case["U_t"][det][:-1] for det in case["dets"]}
    V_short = {det: case["V_t"][det][:-1] for det in case["dets"]}
    with pytest.raises(ValueError):            # time grid mismatch
        _call(case, U_short, V_short)


def test_refuse_legacy_array_likelihoods():
    case = _make_case(seed=53, modes=((2, 2), (2, -2)))
    P = case["P"]
    with pytest.raises(NotImplementedError):
        fl.SingleDetectorLogLikelihoodModelViaArray(case["lookupNKDict"], case["U_t"], case["V_t"],
                                                    P.tref, P.phi[0], P.theta[0], P.incl[0], P.phiref[0],
                                                    P.psi[0], P.dist[0], "H1")
    Ps = _PVec()
    for name in ("phi", "theta", "psi", "incl", "phiref", "dist"):
        setattr(Ps, name, float(getattr(P, name)[0]))
    Ps.tref = P.tref
    Ps.deltaT = P.deltaT
    with pytest.raises(NotImplementedError):
        fl.DiscreteFactoredLogLikelihoodViaArray(case["tvals"], Ps, case["lookupNKDict"], case["rholmsArrayDict"],
                                                 case["U_t"], case["V_t"], case["epochDict"], Lmax=2)
    with pytest.raises(NotImplementedError):
        fl.DiscreteFactoredLogLikelihoodViaArrayVector(case["tvals"], P, case["lookupNKDict"], case["rholmsArrayDict"],
                                                       case["U_t"], case["V_t"], case["epochDict"], Lmax=2, xpy=np)
    with pytest.raises(NotImplementedError):
        fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoopOrig(case["tvals"], P, case["lookupNKDict"], case["rholmsArrayDict"],
                                                                 case["U_t"], case["V_t"], case["epochDict"], Lmax=2)


# --------------------------------------------------------------------------
# packing
# --------------------------------------------------------------------------
class _FakeSeries(object):
    def __init__(self, data, epoch):
        class _D(object):
            pass
        self.data = _D()
        self.data.data = data
        self.data.length = len(data)
        self.epoch = epoch


def test_pack_time_indexed_cross_terms():
    rng = np.random.default_rng(59)
    modes = [(2, 2), (2, -2), (2, 1), (2, -1)]
    npts = 20
    rholms = {m: _FakeSeries(rng.standard_normal(npts) + 0j, 1.0) for m in modes}
    ct = {(a, b): rng.standard_normal(npts) + 1j*rng.standard_normal(npts) for a in modes for b in modes}
    ctV = {(a, b): rng.standard_normal(npts) + 1j*rng.standard_normal(npts) for a in modes for b in modes}
    lNK, lKN, lconj, U, V, rA, rI, ep = fl.PackLikelihoodDataStructuresAsArrays(modes, None, rholms, ct, ctV)
    assert U.shape == (npts, 4, 4) and V.shape == (npts, 4, 4)
    for a in modes:
        for b in modes:
            np.testing.assert_array_equal(U[:, lKN[a], lKN[b]], ct[(a, b)])
            np.testing.assert_array_equal(V[:, lKN[a], lKN[b]], ctV[(a, b)])
    # scalar values still give (n_lm, n_lm), same lookup ordering
    ct_s = {k: v[0] for k, v in ct.items()}
    ctV_s = {k: v[0] for k, v in ctV.items()}
    lNK2, lKN2, _, U2, V2, _, _, _ = fl.PackLikelihoodDataStructuresAsArrays(modes, None, rholms, ct_s, ctV_s)
    assert U2.shape == (4, 4)
    assert lKN2 == lKN
    np.testing.assert_array_equal(U2, U[0])
    np.testing.assert_array_equal(V2, V[0])
    # wrong length refuses
    ct_bad = dict(ct)
    ct_bad[(modes[1], modes[2])] = ct_bad[(modes[1], modes[2])][:-1]
    with pytest.raises(ValueError):
        fl.PackLikelihoodDataStructuresAsArrays(modes, None, rholms, ct_bad, ctV)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
