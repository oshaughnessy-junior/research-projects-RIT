"""Run reproducible CLI-level multirate controls on an allocated CIT GPU.

Usage: python run_ile_matrix.py CODE FIXTURES OUTPUT
Fixtures: acceptance-4 grid, PSD, frame cache and schedules from the paper repository.
Every case is a fresh process; two intrinsic points exercise per-job cache reuse.
"""
import json
from pathlib import Path
import subprocess
import sys
import time
import numpy as np
from igwn_ligolw import ligolw, lsctables, utils

code, fixtures, out = map(lambda p: Path(p).resolve(), sys.argv[1:])
out.mkdir(parents=True, exist_ok=True)

class Handler(ligolw.LIGOLWContentHandler):
    pass

lsctables.use_in(Handler)

def samples(path):
    rows = lsctables.SimInspiralTable.get_table(utils.load_filename(str(path), contenthandler=Handler))
    cols = ('longitude', 'latitude', 'polarization', 'coa_phase', 'inclination', 'distance',
            'geocent_end_time', 'geocent_end_time_ns')
    return {tuple(getattr(row, col) for col in cols): float(row.alpha1) for row in rows}

results = {}
for tag, kind, backend in [('cupy', 'two_rate', 'cupy'), ('numpy', 'two_rate', 'numpy'),
                            ('reference', 'two_rate', 'reference'), ('full', 'one_segment', 'cupy'),
                            ('xphm_cupy', 'two_rate', 'cupy'), ('xphm_reference', 'two_rate', 'reference'),
                            ('trunc_cupy', 'two_rate', 'cupy'), ('trunc_numpy', 'two_rate', 'numpy')]:
    dest = out / tag
    dest.mkdir(exist_ok=True)
    cmd = [sys.executable, str(code / 'bin/integrate_likelihood_extrinsic_batchmode'),
           '--early-time-multirate', str(fixtures / ('schedule_%s.json' % kind)),
           '--early-time-multirate-backend', backend,
           '--time-marginalization', '--vectorized', '--gpu', '--force-gpu-only',
           '--sampler-method', 'AV', '--internal-sky-network-coordinates', '--internal-rotate-phase',
           '--sim-xml', str(fixtures / 'grid.xml.gz'), '--n-events-to-analyze', '2',
           '--event-time', '1000000000', '--data-start-time', '999999512', '--data-end-time', '1000000024',
           '--cache-file', str(fixtures / 'zero_noise.cache'),
           '--channel-name', 'H1=FAKE-STRAIN', '--channel-name', 'L1=FAKE-STRAIN',
           '--psd-file', 'H1=' + str(fixtures / 'psd_O4.xml.gz'),
           '--psd-file', 'L1=' + str(fixtures / 'psd_O4.xml.gz'),
           '--fmin-template', '20', '--reference-freq', '20', '--fmax', '1700', '--srate', '4096',
           '--inv-spec-trunc-time', '2' if tag.startswith('trunc') else '0',
           '--l-max', '2' if tag.startswith('trunc') else '3' if tag.startswith('xphm') else '4',
           '--approximant', 'IMRPhenomXPHM' if tag.startswith('xphm') else 'IMRPhenomXHM', '--d-max', '300',
           '--n-max', '4096', '--n-eff', '1000000', '--n-chunk', '2048', '--seed', '19',
           '--save-samples', '--output-file', str(dest / 'output')]
    (dest / 'command.json').write_text(json.dumps(cmd, indent=2))
    start = time.monotonic()
    with (dest / 'ile.log').open('w') as log:
        proc = subprocess.run(cmd, cwd=dest, stdout=log, stderr=subprocess.STDOUT, timeout=1800)
    text = (dest / 'ile.log').read_text()
    assert proc.returncode == 0, (tag, proc.returncode, text[-6000:])
    if kind == 'two_rate':
        assert 'early-time multirate: schedule' in text, tag
        assert ('early-time multirate: backend ' + backend) in text, tag
    assert 'gpu=True' in text, tag
    dat = sorted(dest.glob('output_*_.dat'))
    assert len(dat) == 2, (tag, dat)
    results[tag] = {'seconds': time.monotonic() - start,
                    'points': [np.loadtxt(f).tolist() for f in dat]}
    print(tag, results[tag], flush=True)

for tag, control in [('numpy', 'cupy'), ('reference', 'cupy'), ('full', 'cupy'),
                     ('xphm_reference', 'xphm_cupy'), ('trunc_numpy', 'trunc_cupy')]:
    matched = []
    for i in range(2):
        a = samples(out / control / ('output_%d_.xml.gz' % i))
        b = samples(out / tag / ('output_%d_.xml.gz' % i))
        keys = a.keys() & b.keys()
        assert len(keys) >= 10, (tag, i, len(keys))
        diffs = [abs(a[k] - b[k]) for k in keys if np.isfinite(a[k]) and np.isfinite(b[k])]
        assert diffs, (tag, i)
        assert not any(np.isfinite(a[k]) != np.isfinite(b[k]) for k in keys)
        # Samples use real_4 alpha1. GPU arithmetic parity is separately tested at 1e-10.
        limit = 1e-4 if tag != 'full' else 0.01
        peak = max(v for v in b.values() if np.isfinite(v))
        top = [abs(a[k] - b[k]) for k in keys if np.isfinite(a[k]) and np.isfinite(b[k]) and b[k] >= peak - 50.]
        assert top and max(top if tag == 'full' else diffs) < limit, (tag, i, max(diffs))
        matched.append({'n': len(keys), 'n_within50': len(top), 'max_abs_dlnL': max(diffs),
                        'max_abs_dlnL_within50': max(top)})
    results[tag]['matched_cupy'] = matched
(out / 'results.json').write_text(json.dumps(results, indent=2))
print('ILE_MATRIX_PASS', flush=True)
