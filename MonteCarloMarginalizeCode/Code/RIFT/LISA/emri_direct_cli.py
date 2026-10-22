"""Bounded fixed-nuisance EMRI self-injection with synthetic independent noise."""

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import time

import numpy as np

from .direct_likelihood import aet_inner_product, aet_log_likelihood


def channel_digest(channels):
    """Hash shape, dtype and contiguous channel bytes for the evidence manifest."""
    value = np.ascontiguousarray(channels)
    digest = hashlib.sha256()
    digest.update(str(value.shape).encode())
    digest.update(value.dtype.str.encode())
    digest.update(value.tobytes())
    return digest.hexdigest()


def evaluate_channel_trials(data, models, *, dt, psd_levels, target_snr,
                            fmin, fmax):
    """Evaluate real A/E/T arrays using fixed synthetic white channel noise.

    This constructs an artificial diagonal noise model in the response output
    observable. It makes no statement about physical LISA noise covariance.
    DC and any Nyquist endpoint are excluded; all trials use the same mask/PSD.
    """
    data = np.asarray(data)
    if data.ndim != 2 or data.shape[0] != 3 or data.shape[1] < 4:
        raise ValueError('data must have shape (3, n_time), with n_time >= 4')
    if not np.isrealobj(data) or not np.all(np.isfinite(data)):
        raise ValueError('channel data must be finite and real')
    if not np.isscalar(dt) or not np.isfinite(dt) or dt <= 0:
        raise ValueError('dt must be finite and positive')
    levels = np.asarray(psd_levels, dtype=float)
    if levels.shape != (3,) or not np.all(np.isfinite(levels)) or np.any(levels <= 0):
        raise ValueError('psd_levels must contain three finite positive values')
    if not np.isscalar(target_snr) or not np.isfinite(target_snr) or target_snr <= 0:
        raise ValueError('target_snr must be finite and positive')
    if not np.isfinite(fmin) or not np.isfinite(fmax) or not 0 <= fmin < fmax:
        raise ValueError('frequency limits must be finite, ordered and nonnegative')
    n = data.shape[1]
    frequencies = np.fft.rfftfreq(n, dt)
    mask = (frequencies > 0) & (frequencies >= fmin) & (frequencies <= fmax)
    if n % 2 == 0:
        mask[-1] = False
    if not np.any(mask):
        raise ValueError('frequency cuts select no interior positive bins')
    df = 1.0 / (n * dt)
    fd = dt * np.fft.rfft(data, axis=1)
    psd = np.broadcast_to(levels[:, None], fd.shape).copy()
    raw_norm = aet_inner_product(fd, fd, psd, df, mask=mask)
    if not np.isfinite(raw_norm) or raw_norm <= 0:
        raise ValueError('injection has no nonzero power in the selected band')
    psd *= raw_norm / target_snr**2
    snr_squared = aet_inner_product(fd, fd, psd, df, mask=mask)
    records = []
    for trial_id, model in models:
        model = np.asarray(model)
        if model.shape != data.shape or not np.isrealobj(model):
            raise ValueError('trial channels must match the real injection shape')
        if not np.all(np.isfinite(model)):
            raise ValueError('trial channels must be finite')
        fm = dt * np.fft.rfft(model, axis=1)
        log_likelihood = aet_log_likelihood(fd, fm, psd, df, mask=mask)
        records.append({'id': trial_id, 'log_likelihood': log_likelihood,
                        'channel_sha256': channel_digest(model)})
    return {'noise': {'kind': 'synthetic_independent_white',
                      'one_sided_psd': psd[:, 0].tolist(),
                      'normalization': 'fixed_from_injection_target_snr'},
            'snr_squared': snr_squared,
            'null_log_likelihood': aet_log_likelihood(fd, 0*fd, psd, df, mask=mask),
            'injection_log_likelihood': aet_log_likelihood(fd, fd, psd, df, mask=mask),
            'n_samples': n, 'dt_seconds': float(dt), 'delta_f_hz': df,
            'selected_bins': int(mask.sum()),
            'first_frequency_hz': float(frequencies[mask][0]),
            'last_frequency_hz': float(frequencies[mask][-1]),
            'injection_sha256': channel_digest(data), 'trials': records}


def run_fixture(config, *, max_points=8):
    """Generate the real FEW/response path; reject unsupported fixture fields."""
    started = time.monotonic()
    from .few_adapter import EMRIParameters, FEWConfig, FewWaveformAdapter
    from .emri_response import ResponseConfig, equal_arm_aet

    required = {'schema_version', 'waveform', 'response', 'injection', 'trials', 'noise'}
    if not isinstance(config, dict) or set(config) != required or config['schema_version'] != 1:
        raise ValueError('fixture must have exactly the schema_version=1 fields')
    trials = config['trials']
    if not isinstance(trials, list) or not 1 <= len(trials) <= max_points:
        raise ValueError('trial count must be between one and max_points')
    ids = []
    for trial in trials:
        if not isinstance(trial, dict) or set(trial) != {'id', 'changes'}:
            raise ValueError('each trial requires id and changes')
        if not isinstance(trial['id'], str) or not trial['id']:
            raise ValueError('trial id must be a nonempty string')
        if not isinstance(trial['changes'], dict):
            raise ValueError('trial changes must be an object')
        ids.append(trial['id'])
    if len(set(ids)) != len(ids):
        raise ValueError('trial ids must be unique')
    noise = config['noise']
    noise_fields = {'kind', 'psd_levels', 'target_snr', 'fmin_hz', 'fmax_hz'}
    if not isinstance(noise, dict) or set(noise) != noise_fields:
        raise ValueError('noise requires the complete synthetic white noise fields')
    if noise['kind'] != 'synthetic_independent_white':
        raise ValueError('only explicitly synthetic independent white noise is supported')
    wave_config = FEWConfig.from_dict(config['waveform'])
    if wave_config.epoch_reference != 'pinned equal-arm orbit t=0':
        raise ValueError('fixture epoch_reference must be pinned equal-arm orbit t=0')
    if wave_config.n_samples > 16384:
        raise ValueError('initial direct fixture is limited to 16384 waveform samples')
    response_config = ResponseConfig.from_dict(config['response'])
    injection = EMRIParameters.from_dict(config['injection'])
    trial_parameters = []
    for trial in trials:
        values = injection.to_dict()
        unknown = set(trial['changes']) - set(values)
        if unknown:
            raise ValueError(f'unknown trial parameters: {sorted(unknown)}')
        values.update(trial['changes'])
        trial_parameters.append(EMRIParameters.from_dict(values))
    adapter = FewWaveformAdapter(wave_config)
    setup_finished = time.monotonic()
    wave_metadata = []
    response_metadata = []

    def generate(parameters):
        polarizations = adapter.generate(parameters)
        channels = equal_arm_aet(
            *polarizations.polarizations, dt=polarizations.dt_seconds,
            sky_colatitude=parameters.qS, sky_longitude=parameters.phiS,
            epoch=polarizations.epoch_start_seconds, config=response_config,
        )
        wave_metadata.append(getattr(polarizations, 'metadata', {}))
        response_metadata.append(channels.metadata)
        return channels

    data = generate(injection)

    def models():
        for trial, parameters in zip(trials, trial_parameters):
            result = generate(parameters)
            if result.dt != data.dt or result.epoch_seconds != data.epoch_seconds:
                raise ValueError('trial response sampling/epoch differs from injection')
            yield trial['id'], result.channels

    result = evaluate_channel_trials(
        data.channels, models(), dt=data.dt, psd_levels=noise['psd_levels'],
        target_snr=noise['target_snr'], fmin=noise['fmin_hz'], fmax=noise['fmax_hz'],
    )
    versions = {}
    for package in ('numpy', 'fastemriwaveforms', 'fastlisaresponse', 'lisaanalysistools'):
        versions[package] = importlib.metadata.version(package)
    result.update({'schema_version': 1, 'status': 'COMPLETED',
                   'backend': 'few_cpu_equal_arm_direct_fixed_nuisance',
                   'marginalized_parameters': [], 'fixture': config,
                   'versions': versions, 'response_epoch_seconds': data.epoch_seconds,
                   'window': 'rectangular; identical retained segment for every trial',
                   'module_sha256': {
                       name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
                       for name in ('emri_direct_cli.py', 'direct_likelihood.py',
                                    'few_adapter.py', 'emri_response.py')},
                   'waveform_metadata': wave_metadata,
                   'response_metadata': response_metadata,
                   'setup_seconds': setup_finished - started,
                   'evaluation_seconds': time.monotonic() - setup_finished,
                   'elapsed_seconds': time.monotonic() - started})
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--max-points', type=int, default=8)
    args = parser.parse_args(argv)
    if not 1 <= args.max_points <= 64:
        parser.error('--max-points must be between 1 and 64')
    if args.output.exists():
        parser.error('output already exists; preserve previous evidence')
    raw = args.fixture.read_bytes()
    result = run_fixture(json.loads(raw), max_points=args.max_points)
    result['fixture_sha256'] = hashlib.sha256(raw).hexdigest()
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
