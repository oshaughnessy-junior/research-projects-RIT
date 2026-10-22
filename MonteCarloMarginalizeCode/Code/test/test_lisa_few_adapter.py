"""Parameter contracts plus opt-in actual FEW waveform checks."""
from dataclasses import replace
import os

import numpy as np
import pytest

from RIFT.LISA import few_adapter as fa


def parameters():
    return fa.EMRIParameters(1e6, 10., 0., 12., 0.2, 1., 1.,
                             1., 2., 0.7, 0.3, 0.2, 0., 0.4)


def config():
    return fa.FEWConfig(4096, 10., 0., 'test mission epoch, seconds')


def test_parameter_and_config_roundtrip():
    p, c = parameters(), config()
    assert fa.EMRIParameters.from_dict(p.to_dict()) == p
    assert fa.FEWConfig.from_dict(c.to_dict()) == c
    assert p.qK != p.xI0


@pytest.mark.parametrize('field,value', [('a', .1), ('xI0', .9), ('p0', 9),
    ('p0', 17), ('e0', 0), ('e0', .7), ('m2_msun', -1), ('distance_gpc', 0),
    ('Phi_theta0', 1), ('qS', -1), ('phiK', 7), ('m1_msun', np.nan), ('e0', True)])
def test_restricted_domain(field, value):
    with pytest.raises(ValueError):
        replace(parameters(), **{field: value})


@pytest.mark.parametrize('field,value', [('n_samples', 1), ('n_samples', 2.5),
    ('n_samples', True), ('dt_seconds', 0), ('epoch_start_seconds', np.nan),
    ('epoch_reference', ''), ('eps', 0)])
def test_config_domain(field, value):
    with pytest.raises(ValueError):
        replace(config(), **{field: value})


def test_api_units_phase_order_and_output_contract(monkeypatch):
    calls = {}
    def factory(model, **kwargs):
        calls['construction'] = (model, kwargs)
        def wave(*args, **kw):
            calls['evaluation'] = (args, kw)
            return [np.ones(4096), 2*np.ones(4096)]
        from types import SimpleNamespace
        phase_coefficients = np.zeros((1,3,8))
        phase_coefficients[0,0,1] = 2*np.pi*.001*40950
        wave.waveform_generator = SimpleNamespace(ls=[2], ms=[2], ks=[0], ns=[1],
            inspiral_generator=SimpleNamespace(integrator_spline_t=np.array([0.,40950.]),
                integrator_spline_phase_coeff=phase_coefficients),
            end_time=40950., create_waveform=SimpleNamespace(num_pts_pad=0))
        return wave
    monkeypatch.setattr(fa, '_load_few', lambda: (factory, 100., '2.2.0'))
    out = fa.FewWaveformAdapter(config()).generate(parameters())
    args, kw = calls['evaluation']
    assert args == (1e6, 10., 0., 12., .2, 1., 1., 1., 2., .7, .3, .2, 0., .4)
    assert kw == {'T': 409.5, 'dt': 10., 'mode_selection_threshold': 1e-5,
                  'mode_selection': 'threshold', 'include_minus_mkn': True, 'batch_size': -1}
    assert calls['construction'][1]['frame'] == 'detector'
    assert out.polarizations.shape == (2, 4096)
    assert out.n_samples == 4096
    assert out.epoch_reference == config().epoch_reference
    assert out.metadata['carrier_nyquist_ratio'] < 1
    monkeypatch.setattr(fa, '_carrier_frequency_bound', lambda *args: 0.05)
    with pytest.raises(ValueError, match='reaches Nyquist'):
        fa.FewWaveformAdapter(config()).generate(parameters())


@pytest.mark.skipif(os.environ.get('RIFT_RUN_FEW_REAL') != '1',
                    reason='requires prefetched FEW2.2.0 model data; explicit opt-in')
def test_actual_few_polarizations_distance_and_phases(monkeypatch):
    adapter = fa.FewWaveformAdapter(config())
    p = parameters()
    selector_type = type(adapter.waveform.waveform_generator.mode_selector)
    original_selector_call = selector_type.__call__
    recorded_thresholds = []
    def selector_call(self, *args, **kwargs):
        recorded_thresholds.append(kwargs.get('mode_selection_threshold'))
        return original_selector_call(self, *args, **kwargs)
    monkeypatch.setattr(selector_type, '__call__', selector_call)
    out = adapter(p)
    doubled_distance = adapter(replace(p, distance_gpc=2*p.distance_gpc))
    np.testing.assert_allclose(doubled_distance.polarizations,
                               .5*out.polarizations, rtol=1e-11, atol=0)
    assert np.linalg.norm(out.polarizations) > 0
    for field in ('Phi_phi0', 'Phi_r0'):
        shifted = adapter(replace(p, **{field: getattr(p, field)+.3}))
        difference = np.linalg.norm(shifted.polarizations-out.polarizations)
        assert difference > 1e-5*np.linalg.norm(out.polarizations)
    assert out.metadata['backend'] == 'cpu'
    assert out.metadata['few_version'] == '2.2.0'
    assert len(out.metadata['retained_lmkn']) > 0
    assert recorded_thresholds and all(v == config().eps for v in recorded_thresholds)
    loose = fa.FewWaveformAdapter(replace(config(), eps=0.1))(p)
    assert recorded_thresholds[-1] == 0.1
    assert len(loose.metadata['retained_lmkn']) < len(out.metadata['retained_lmkn'])
    assert not np.array_equal(loose.polarizations, out.polarizations)


@pytest.mark.parametrize('output', [np.zeros((2, 4095)), np.full((2, 4096), np.nan),
                                    np.zeros((2, 4096), dtype=complex)])
def test_invalid_backend_output_is_refused(monkeypatch, output):
    monkeypatch.setattr(fa, '_load_few', lambda: (
        lambda *args, **kw: lambda *args, **kw: output, 100., '2.2.0'))
    with pytest.raises(ValueError):
        fa.FewWaveformAdapter(config()).generate(parameters())


def test_unreviewed_few_version_is_refused(monkeypatch):
    monkeypatch.setattr(fa, 'version', lambda distribution: '2.1.0')
    with pytest.raises(ImportError, match='requires fastemriwaveforms==2.2.0'):
        fa.FewWaveformAdapter(config())


def test_missing_prefetched_data_refused_without_download():
    class Manager:
        def try_get_local_file(self, name):
            return None
        def get_file(self, name):
            raise AssertionError('downloading API must not be called')
    with pytest.raises(FileNotFoundError, match='Prefetch required'):
        fa._verify_prefetched_files(Manager())


def test_prefetched_data_checksum_validation(tmp_path, monkeypatch):
    import hashlib
    path = tmp_path / 'data'
    path.write_bytes(b'model')
    class Manager:
        def try_get_local_file(self, name):
            return path
    monkeypatch.setattr(fa, 'MODEL_DATA_SHA256', {'data': hashlib.sha256(b'model').hexdigest()})
    fa._verify_prefetched_files(Manager())
    path.write_bytes(b'changed')
    with pytest.raises(ValueError, match='SHA256 mismatch'):
        fa._verify_prefetched_files(Manager())


@pytest.mark.parametrize('n', [2, 127, 128, 4096, 13108, 13109])
@pytest.mark.parametrize('dt', [0.1, 4., 10.])
def test_endpoint_inclusive_duration(n, dt):
    # Pinned SummationBase floors the requested steps and adds the initial point.
    year = 31558149.7635456
    duration = fa._few_duration_years(n, dt, year)
    assert int(duration*year/dt)+1 == n
    assert duration*year == pytest.approx((n-1)*dt)


@pytest.mark.parametrize('padding,end_time', [(1, 40950.), (0, 40000.), (0, np.nan)])
def test_physical_termination_guard(padding, end_time):
    from types import SimpleNamespace
    generator = SimpleNamespace(end_time=end_time,
                    create_waveform=SimpleNamespace(num_pts_pad=padding))
    with pytest.raises(ValueError, match='zero padding is refused'):
        fa._termination_metadata(generator, config())


def test_physical_termination_allows_endpoint_roundoff():
    from types import SimpleNamespace
    generator = SimpleNamespace(end_time=np.nextafter(40950., 0),
                    create_waveform=SimpleNamespace(num_pts_pad=0))
    assert fa._termination_metadata(generator, config())['zero_padding_samples'] == 0



def test_carrier_bound_for_linear_independent_phases():
    phases = np.zeros((2, 3, 8))
    widths = np.array([2., 3.])
    angular_rates = np.array([.3, .2, .1])
    phases[:, :, 1] = widths[:, None]*angular_rates
    modes = np.array([[2, 2, 0, 1], [3, -1, 2, -3]])
    bound = fa._carrier_frequency_bound([0., 2., 5.], phases, modes, 4.)
    expected = max(abs(modes[:, 1:]@angular_rates))/(2*np.pi)
    assert bound == pytest.approx(expected, rel=1e-12)


def test_carrier_bound_encloses_dense_curved_phase_derivative():
    rng = np.random.default_rng(73)
    phases = rng.normal(size=(1, 3, 8))
    modes = np.array([[2, 2, 0, -3]])
    bound = fa._carrier_frequency_bound([0., 2.], phases, modes, 2.)
    # Independent derivative of the nested FEW phase polynomial via Polynomial.
    from numpy.polynomial import Polynomial as P
    s, one = P([0, 1]), P([1])
    carrier = modes[0, 1:]@phases[0]
    r1,r2,r3,r4,r5,r6,r7,r8 = carrier
    phase = r1+s*(r2+(one-s)*(r3+s*(r4+(one-s)*(r5+s*(r6+(one-s)*(r7+s*r8))))))
    sampled = np.abs(phase.deriv()(np.linspace(0,1,10001)))/(2*2*np.pi)
    assert sampled.max() <= bound


@pytest.mark.parametrize('times,shape', [([0.,0.], (1,3,8)), ([1.,2.], (1,3,8)),
    ([0.,1.], (1,3,8)), ([0.,2.], (2,3,8))])
def test_carrier_bound_rejects_missing_coverage_and_bad_shapes(times, shape):
    with pytest.raises(ValueError):
        fa._carrier_frequency_bound(times, np.zeros(shape), np.array([[2,2,0,0]]), 2.)


def test_carrier_bound_catches_frequency_peak_between_knots():
    # rcont5-only phase=s^2(1-s)^2 has zero frequency at both knots.
    phases = np.zeros((1, 3, 8))
    phases[0, 0, 4] = 1.
    bound = fa._carrier_frequency_bound([0., 1.], phases, np.array([[2,1,0,0]]), 1.)
    # Max |2*s-6*s^2+4*s^3| = sqrt(3)/9 in the open interval.
    exact_peak_hz = np.sqrt(3)/(18*np.pi)
    assert bound >= exact_peak_hz
    assert bound > 0
