"""Expanded source components retain every mixed U/V term in cubic NoLoop.

The waveform fixture is a general complex signal, not a reflection-symmetrized
mode set. The oracle projects in time, uses numpy rFFT and direct bin sums; it
does not call RIFT's likelihood or cross-term algebra.
"""
import copy
from types import SimpleNamespace

import lal
import lalsimulation as ls
import numpy as np
import pytest

from RIFT import lalsimutils as lsu
from RIFT.likelihood import factored_likelihood as fl

RATE, DURATION, EVENT = 4096, 4., 1e9
DT = 1. / RATE
FREQ = np.fft.rfftfreq(int(RATE * DURATION), DT)
PSD = 1e-46 * (1 + (FREQ / 200.)**2)
WEIGHT = ((FREQ >= 20) & (FREQ <= 400)) / PSD
LABELS = tuple((2, m, (2, sector)) for m in (-2, 2)
               for sector in ('S', 'A+', 'A-'))


def coefficients(labels, incl, phiref, xpy=np, l_max=2, P_vec=None):
    # This is a synthetic source coefficient contract, NOT an XPNR gauge fix.
    incl, phiref = xpy.asarray(incl), xpy.asarray(phiref)
    values = []
    for ell, m, (_, sector) in labels:
        # Independent scalar LAL harmonic, rather than the GPU harmonic helper.
        y = xpy.asarray([lal.SpinWeightedSphericalHarmonic(float(i), -float(p),
                          -2, ell, m) for i, p in zip(incl, phiref)])
        if sector == 'A+':
            y = y * xpy.exp(-1j * phiref)
        elif sector == 'A-':
            y = y * xpy.exp(1j * phiref)
        values.append(y)
    return xpy.stack(values, axis=-1)


def time_series(values, epoch):
    h = lal.CreateCOMPLEX16TimeSeries('source basis', lal.LIGOTimeGPS(epoch),
                                    0., DT, lal.DimensionlessUnit, len(values))
    h.data.data[:] = values
    return h


def fixture_modes():
    t = np.arange(int(RATE * DURATION)) * DT - DURATION / 2
    modes = {}
    for index, label in enumerate(LABELS):
        envelope = np.exp(-((t + .01 * index) / .23)**2)
        chirp = np.exp(2j*np.pi*((55 + index)*t + (8 + .1*index)*t*t) + .2j*index)
        values = (1.5e-22 / (index + 1)) * envelope * (chirp + .3*chirp.conj())
        if label[-1][-1] != 'S':
            spectrum = np.fft.fft(values)
            freq = np.fft.fftfreq(len(values), DT)
            spectrum[freq <= 0 if label[-1][-1] == 'A+' else freq >= 0] = 0
            values = np.fft.ifft(spectrum)
        modes[label] = values
    return modes


def physical_parameters():
    p = lsu.ChooseWaveformParams()
    p.phi, p.theta, p.psi, p.incl, p.phiref = 1.2, -.3, .4, 1.1, .7
    p.dist = fl.distMpcRef * 1e6 * lal.PC_SI
    p.tref, p.deltaT, p.deltaF = lal.LIGOTimeGPS(EVENT), DT, 1 / DURATION
    p.fmin = p.fref = 20.
    p.radec = True
    return p


def projected_fd(modes, p, det, offset=0.):
    # Direct LAL detector tensors and numpy Fourier bins are an outside oracle.
    detector = ls.DetectorPrefixToLALDetector(det)
    fp, fc = lal.ComputeDetAMResponse(detector.response, p.phi, p.theta, p.psi,
                                     lal.GreenwichMeanSiderealTime(p.tref))
    y = coefficients(tuple(modes), [p.incl], [p.phiref])[0]
    z = sum(value * modes[label] for label, value in zip(modes, y))
    strain = (fp * z.real - fc * z.imag) * (
        fl.distMpcRef * 1e6 * lal.PC_SI / p.dist)
    delay = lal.TimeDelayFromEarthCenter(detector.location, p.phi, p.theta, p.tref)
    return np.fft.rfft(strain) * DT * np.exp(-2j*np.pi*FREQ*(delay + offset))


@pytest.fixture(scope='module')
def problem():
    p, modes = physical_parameters(), fixture_modes()
    data_fd = {det: projected_fd(modes, p, det) for det in ('H1', 'L1')}
    data = {}
    for det, fd in data_fd.items():
        values = np.fft.irfft(fd / DT, n=int(RATE * DURATION))
        data[det] = lsu.DataFourier(time_series(values, EVENT - DURATION / 2))
    psd = lal.CreateREAL8FrequencySeries('PSD', lal.LIGOTimeGPS(0), 0.,
                                       1 / DURATION, lal.DimensionlessUnit, len(PSD))
    psd.data.data[:] = PSD

    def generator(source, lmax):
        assert source.dist == fl.distMpcRef * 1e6 * lal.PC_SI
        h = {k: lsu.DataFourier(time_series(v, -DURATION / 2)) for k, v in modes.items()}
        # These are ACTUAL conjugate time series, with no (-m) parity assumption.
        hc = {k: lsu.DataFourier(time_series(v.conj(), -DURATION / 2)) for k, v in modes.items()}
        return h, hc

    _, u, v, rho, _, _ = fl.PrecomputeLikelihoodTerms(
        EVENT, .08, p.manual_copy(), data, {det: psd for det in data}, 2, 400.,
        ignore_threshold=None, source_basis_generator=generator,
        skip_interpolation=True, verbose=False, quiet=True)
    packed = {det: fl.PackLikelihoodDataStructuresAsArrays(
        list(rho[det]), None, rho[det], u[det], v[det], source_basis=True) for det in rho}
    arrays = tuple({det: packed[det][i] for det in packed} for i in (0, 5, 3, 4, 7))
    return SimpleNamespace(p=p, modes=modes, data_fd=data_fd, arrays=arrays, u=u, v=v)


def evaluate(problem, samples, offsets, function=None, angular=coefficients):
    p = problem.p.manual_copy()
    for key in ('phi', 'theta', 'psi', 'incl', 'phiref', 'dist'):
        setattr(p, key, np.array([getattr(sample, key) for sample in samples]))
    function = function or fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop
    return function(np.asarray(offsets), p, *problem.arrays, Lmax=2, xpy=np,
                    time_interp='cubic', return_lnLt=True,
                    explicit_time_values=True, source_angular_basis=angular)


def test_expanded_cubic_noloop_matches_independent_direct_bins(problem):
    samples = []
    for incl, phase, psi, distance in [(1.1, .7, .4, 1.), (.4, 1.3, .9, 1.4),
                                       (2.2, 2.5, -.2, .7), (1.7, -.4, 1.2, 2.)]:
        p = problem.p.manual_copy()
        p.incl, p.phiref, p.psi = incl, phase, psi
        p.dist *= distance
        samples.append(p)
    offsets = np.array([-.00173, 0., .00217])
    got = evaluate(problem, samples, offsets)
    expected = np.zeros_like(got)
    for i, p in enumerate(samples):
        for j, offset in enumerate(offsets):
            for det, data in problem.data_fd.items():
                h = projected_fd(problem.modes, p, det, offset)
                expected[i, j] += 4 / DURATION * np.sum(
                    (data.conj()*h - .5*abs(h)**2).real * WEIGHT)
    # Fixed bound, well below the cross-code 0.1-nat likelihood requirement.
    np.testing.assert_allclose(got, expected, atol=.005, rtol=0.)
    # Mixed sectors matter; a block-diagonal approximation is not equivalent.
    modified = list(problem.arrays)
    modified[2] = {det: matrix.copy() for det, matrix in modified[2].items()}
    modified[3] = {det: matrix.copy() for det, matrix in modified[3].items()}
    mask = np.array([[a[2] != b[2] for b in LABELS] for a in LABELS])
    for matrices in modified[2:4]:
        for matrix in matrices.values():
            matrix[mask] = 0
    truncated = copy.copy(problem)
    truncated.arrays = tuple(modified)
    assert np.max(abs(evaluate(truncated, samples, offsets) - expected)) > .1


def test_source_coefficients_are_shared_across_detectors(problem):
    calls = []
    def angular(*args, **kwargs):
        calls.append(args[0])
        return coefficients(*args, **kwargs)
    evaluate(problem, [problem.p], [0.], angular=angular)
    assert len(calls) == 1


def test_opaque_hashable_labels_are_preserved_and_shared(problem):
    # Labels need not be iterable or physical (l,m) pairs. In particular a
    # string must remain one label, not be rewritten as a tuple of characters.
    labels=(91, 'AS positive', ('carrier',2), -7, 'negative', ('sym',4))
    packed={}
    for det in problem.arrays[0]:
        rho={label:time_series(problem.arrays[1][det][i],problem.arrays[4][det])
             for i,label in enumerate(labels)}
        uv=[{(a,b):matrix[i,j] for i,a in enumerate(labels) for j,b in enumerate(labels)}
            for matrix in (problem.arrays[2][det],problem.arrays[3][det])]
        packed[det]=fl.PackLikelihoodDataStructuresAsArrays(
            list(labels),None,rho,*uv,source_basis=True)
        assert packed[det][0]==labels
    opaque=copy.copy(problem)
    opaque.arrays=tuple({det:packed[det][i] for det in packed} for i in (0,5,3,4,7))
    with pytest.raises(ValueError,match='require source_angular_basis'):
        evaluate(opaque,[problem.p],[0.],angular=None)
    seen=[]
    def angular(received,incl,phase,**kwargs):
        assert received==labels
        seen.append(received)
        return coefficients(LABELS,incl,phase,**kwargs)
    actual=evaluate(opaque,[problem.p],[0.],angular=angular)
    expected=evaluate(problem,[problem.p],[0.])
    np.testing.assert_array_equal(actual,expected)
    assert len(seen)==1


@pytest.mark.parametrize('matrix_index,minimum_change', [(2, .05), (3, .01)], ids=['U', 'V'])
def test_each_mixed_matrix_is_required(problem, matrix_index, minimum_change):
    modified = copy.copy(problem)
    arrays = list(problem.arrays)
    arrays[matrix_index] = {det: matrix.copy() for det, matrix in arrays[matrix_index].items()}
    mask = np.array([[a[2] != b[2] for b in LABELS] for a in LABELS])
    for matrix in arrays[matrix_index].values():
        matrix[mask] = 0
    modified.arrays = tuple(arrays)
    full = evaluate(problem, [problem.p], [0.])
    truncated = evaluate(modified, [problem.p], [0.])
    assert np.max(abs(truncated-full)) > minimum_change


def test_source_basis_cannot_use_physical_mode_pruning():
    with pytest.raises(ValueError, match='ignore_threshold=None'):
        fl.PrecomputeLikelihoodTerms(0., .1, physical_parameters(), {}, {}, 2, 400.,
                                     source_basis_generator=lambda *args: ({}, {}))


def test_source_basis_cannot_silently_ignore_force_22_mode():
    with pytest.raises(ValueError, match='force_22_mode'):
        fl.PrecomputeLikelihoodTerms(0., .1, physical_parameters(), {}, {}, 2, 400.,
            source_basis_generator=lambda *args: ({}, {}), ignore_threshold=None,
            force_22_mode=True)


def test_component_labels_require_explicit_source_packing(problem):
    with pytest.raises(ValueError, match='source_basis=True'):
        fl.PackLikelihoodDataStructuresAsArrays(list(LABELS), None,
            {label: time_series(np.zeros(8), 0.) for label in LABELS}, {}, {})


@pytest.mark.parametrize('bad_grid', ['epoch', 'length'])
def test_source_components_must_share_the_packed_grid(bad_grid):
    good = lsu.DataFourier(time_series(np.zeros(32), 0.))
    bad = lsu.DataFourier(time_series(np.zeros(33 if bad_grid == 'length' else 32),
                                     1. if bad_grid == 'epoch' else 0.))
    bank = {LABELS[0]: good, LABELS[1]: bad}
    with pytest.raises(ValueError, match='share one frequency grid and epoch'):
        fl.PrecomputeLikelihoodTerms(EVENT, .1, physical_parameters(), {'H1': good},
            {'H1': None}, 2, 400., ignore_threshold=None,
            source_basis_generator=lambda *args: (bank, bank), quiet=True, verbose=False)


def test_common_shifted_source_frequency_grid_is_refused():
    data=lsu.DataFourier(time_series(np.zeros(32),0.))
    shifted=lsu.DataFourier(time_series(np.zeros(32),0.))
    shifted.f0=data.f0+data.deltaF
    bank={LABELS[0]:shifted,LABELS[1]:shifted}
    with pytest.raises(ValueError,match='data frequency grid'):
        fl.PrecomputeLikelihoodTerms(EVENT,.1,physical_parameters(),{'H1':data},
            {'H1':None},2,400.,ignore_threshold=None,
            source_basis_generator=lambda *args:(bank,bank),quiet=True,verbose=False)


def test_full_source_parameter_vector_reaches_coefficients(problem):
    seen = []
    def angular(labels, incl, phase, *, P_vec, **kwargs):
        seen.append(P_vec)
        assert P_vec.deltaT == DT
        np.testing.assert_array_equal(P_vec.phiref, phase)
        return coefficients(labels, incl, phase, **kwargs)
    evaluate(problem, [problem.p], [0.], angular=angular)
    assert len(seen) == 1


def test_expanded_basis_rejects_unsupported_analytic_phase_marginalization(problem):
    p = problem.p.manual_copy()
    for name in ('phi', 'theta', 'psi', 'incl', 'phiref', 'dist'):
        setattr(p, name, np.asarray([getattr(p, name)]))
    with pytest.raises(NotImplementedError, match='phase marginalization'):
        fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(
            np.asarray([0.]), p, *problem.arrays, source_angular_basis=coefficients,
            phase_marginalization=True, return_lnLt=True, time_interp='cubic')


def test_expanded_labels_cannot_silently_use_ordinary_harmonics(problem):
    with pytest.raises(ValueError, match='require source_angular_basis'):
        evaluate(problem, [problem.p], [0.], angular=None)


def test_angular_callback_shape_is_checked(problem):
    with pytest.raises(ValueError, match='shape'):
        evaluate(problem, [problem.p], [0.], angular=lambda *a, **k: np.ones((1, 2)))


@pytest.mark.parametrize('nonfinite', [np.nan, np.inf])
def test_nonfinite_source_coefficients_are_refused(problem, nonfinite):
    with pytest.raises(ValueError, match='finite'):
        evaluate(problem, [problem.p], [0.],
                 angular=lambda *a, **k: np.full((1, len(LABELS)), nonfinite))


def test_source_callback_arrays_are_not_implicitly_converted(problem):
    with pytest.raises(TypeError, match='selected xpy backend'):
        evaluate(problem, [problem.p], [0.], angular=lambda *a, **k: [[1.] * len(LABELS)])


def test_integer_source_coefficients_are_refused(problem):
    with pytest.raises(TypeError, match='floating or complex dtype'):
        evaluate(problem, [problem.p], [0.],
                 angular=lambda *a, **k: np.ones((1, len(LABELS)), dtype=int))


def legacy_problem(problem):
    """Two physical modes, with ordinary legacy ndarray lookup tables."""
    result = copy.copy(problem)
    selected = [i for i, label in enumerate(LABELS) if label[2][1] == 'S']
    labels = np.asarray([LABELS[i][:2] for i in selected])
    lookup = {det: labels.copy() for det in problem.arrays[0]}
    rho = {det: matrix[selected].copy() for det, matrix in problem.arrays[1].items()}
    uv = [{det: matrix[np.ix_(selected, selected)].copy() for det, matrix in d.items()}
          for d in problem.arrays[2:4]]
    result.arrays = (lookup, rho, *uv, problem.arrays[4])
    return result


def test_ordinary_harmonic_hook_is_bitwise_equal_to_absent_hook(problem):
    legacy = legacy_problem(problem)
    def angular(labels, incl, phase, xpy=np, l_max=2, P_vec=None):
        return fl.SphericalHarmonicsVectorized(labels, incl, -phase, xpy=xpy, l_max=l_max)
    default = evaluate(legacy, [problem.p], [-.00173, 0., .00217], angular=None)
    callback = evaluate(legacy, [problem.p], [-.00173, 0., .00217], angular=angular)
    np.testing.assert_array_equal(default, callback)


def vector_coefficients(labels, incl, phase, xpy=np, l_max=2, P_vec=None):
    """Analytic spin -2, l=2, m=+/-2 harmonics, including synthetic sectors."""
    values = []
    for ell, m, (_, sector) in labels:
        y = xpy.sqrt(5. / (64 * xpy.pi)) * (1 + (m / 2)*xpy.cos(incl))**2
        y = y * xpy.exp(-1j*m*phase)
        if sector == 'A+':
            y = y * xpy.exp(-1j*phase)
        elif sector == 'A-':
            y = y * xpy.exp(1j*phase)
        values.append(y)
    return xpy.stack(values, axis=-1)


def test_vector_coefficients_match_independent_lal_harmonics():
    incl, phase = np.asarray([.3, 1.1, 2.6]), np.asarray([1.2, -.7, 2.])
    np.testing.assert_allclose(vector_coefficients(LABELS, incl, phase),
                               coefficients(LABELS, incl, phase), atol=1e-15, rtol=1e-15)


def test_source_basis_gpu_matches_cpu(problem):
    cupy = pytest.importorskip('cupy')
    try:
        cupy.zeros(1)
    except Exception as error:
        pytest.skip('A CUDA device is required: ' + str(error))
    p = problem.p.manual_copy()
    for key in ('phi', 'theta', 'psi', 'incl', 'phiref', 'dist'):
        setattr(p, key, cupy.asarray([getattr(p, key)]))
    arrays = (problem.arrays[0], *({det: cupy.asarray(value) for det, value in d.items()}
              for d in problem.arrays[1:4]), problem.arrays[4])
    actual = fl.DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop(
        cupy.asarray([-.00173, 0., .00217]), p, *arrays, Lmax=2, xpy=cupy,
        time_interp='cubic', return_lnLt=True, explicit_time_values=True,
        source_angular_basis=vector_coefficients).get()
    expected = evaluate(problem, [problem.p], [-.00173, 0., .00217],
                        angular=vector_coefficients)
    np.testing.assert_allclose(actual, expected, atol=1e-8, rtol=1e-11)
