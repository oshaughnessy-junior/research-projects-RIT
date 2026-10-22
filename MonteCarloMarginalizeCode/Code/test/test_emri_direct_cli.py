"""Independent FFT/white-noise checks of the direct fixture calculation."""

import numpy as np
import pytest

from RIFT.LISA.emri_direct_cli import evaluate_channel_trials, channel_digest


@pytest.mark.parametrize('n', [127, 128])
def test_target_snr_and_time_domain_residual(n):
    t = np.arange(n)
    data = np.asarray([np.cos(2*np.pi*k*t/n) for k in (2, 4, 7)])
    levels = np.array([2., 3., 5.])
    result = evaluate_channel_trials(
        data, [('same', data), ('half', data/2), ('null', data*0)],
        dt=2., psd_levels=levels, target_snr=12., fmin=0, fmax=.25,
    )
    assert result['snr_squared'] == pytest.approx(144.)
    assert result['injection_log_likelihood'] == 0
    assert result['null_log_likelihood'] == pytest.approx(-72.)
    assert [r['log_likelihood'] for r in result['trials']] == pytest.approx([0, -18., -72.])
    # Independently derived discrete white-noise variance S/(2*dt).
    variance = np.asarray(result['noise']['one_sided_psd']) / 4.
    expected = -.5*np.sum((data-data/2)**2/variance[:, None])
    assert result['trials'][1]['log_likelihood'] == pytest.approx(expected)
    assert result['selected_bins'] == (n-1)//2


def test_noise_is_fixed_across_trials_and_changes_have_effect():
    n = 96
    t = np.arange(n)
    data = np.asarray([np.cos(2*np.pi*k*t/n) for k in (2, 4, 7)])
    kwargs = dict(dt=1., psd_levels=[1, 1, 1], target_snr=10, fmin=0, fmax=.5)
    a = evaluate_channel_trials(data, [('offset', data*.9)], **kwargs)
    b = evaluate_channel_trials(data, [('offset', data*1.1)], **kwargs)
    assert a['noise'] == b['noise']
    assert a['trials'][0]['log_likelihood'] == pytest.approx(-.5)
    assert b['trials'][0]['log_likelihood'] == pytest.approx(-.5)
    assert channel_digest(data) != channel_digest(data+.1)


@pytest.mark.parametrize('change', [
    {'dt': 0}, {'dt': np.nan}, {'target_snr': 0}, {'target_snr': np.inf},
    {'psd_levels': [1, -1, 1]}, {'psd_levels': [1, 1]},
    {'fmin': .2, 'fmax': .1}, {'fmin': 10, 'fmax': 11},
])
def test_invalid_noise_and_sampling(change):
    rng = np.random.default_rng(19)
    data = rng.normal(size=(3, 64))
    kwargs = dict(dt=1., psd_levels=[1, 1, 1], target_snr=10, fmin=0, fmax=.5)
    kwargs.update(change)
    with pytest.raises(ValueError):
        evaluate_channel_trials(data, [('same', data)], **kwargs)


def test_invalid_channels_and_no_band_power():
    kwargs = dict(dt=1., psd_levels=[1, 1, 1], target_snr=10, fmin=0, fmax=.5)
    for data in (np.ones((2, 64)), np.ones((3, 64), dtype=complex),
                 np.zeros((3, 64)), np.full((3, 64), np.nan)):
        with pytest.raises(ValueError):
            evaluate_channel_trials(data, [], **kwargs)
    rng = np.random.default_rng(41)
    data = rng.normal(size=(3, 64))
    with pytest.raises(ValueError):
        evaluate_channel_trials(data, [('bad', data[:, :-1])], **kwargs)


def test_absolute_psd_is_frozen_across_fixture_changes():
    n, dt = 128, 2.
    t = np.arange(n)
    data = np.asarray([np.cos(2*np.pi*k*t/n) for k in (2, 4, 7)])
    kwargs = dict(dt=dt, psd_levels=[2, 3, 5], target_snr=None, fmin=0, fmax=.25)
    base = evaluate_channel_trials(data, [('offset', data/2)], **kwargs)
    changed = evaluate_channel_trials(data*2, [('offset', data)], **kwargs)
    assert base['noise']['normalization'] == 'fixed_supplied_psd'
    assert base['noise']['one_sided_psd'] == [2., 3., 5.]
    assert changed['noise'] == base['noise']
    assert changed['snr_squared'] == pytest.approx(4*base['snr_squared'])
    assert changed['trials'][0]['log_likelihood'] == pytest.approx(
        4*base['trials'][0]['log_likelihood'])
