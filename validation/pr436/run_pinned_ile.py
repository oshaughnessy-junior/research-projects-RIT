"""Converged conditional-distance ILE control, alongside the broad-prior smoke matrix.

Usage: python run_pinned_ile.py ROOT BAND. Reuses the matrix's recorded CuPy command.
Pins sky, orientation and phase to the known injection, leaving distance sampled
and arrival time marginalized. This is not a full extrinsic posterior test.
"""
import json
from pathlib import Path
import subprocess
import sys
import time
import numpy as np

root, band = Path(sys.argv[1]), sys.argv[2]
base = json.loads((root / ('final_' + band) / 'ile/cupy/command.json').read_text())
for flag in ('--internal-sky-network-coordinates', '--internal-rotate-phase'):
    base.remove(flag)
for flag, value in [('--right-ascension', '1.0'), ('--declination', '0.4'), ('--inclination', '0.6'),
                    ('--psi', '0.4'), ('--phi-orb', '0.3')]:
    base.extend([flag, value])

def setarg(cmd, key, value):
    cmd[cmd.index(key) + 1] = str(value)

results = {}
for tag, kind, backend in [('cupy', 'two_rate', 'cupy'), ('full', 'one_segment', 'numpy')]:
    out = root / ('final_' + band) / 'pinned' / tag
    out.mkdir(parents=True, exist_ok=True)
    cmd = list(base)
    schedule = Path(cmd[cmd.index('--early-time-multirate') + 1])
    setarg(cmd, '--early-time-multirate', schedule.with_name('schedule_%s.json' % kind))
    setarg(cmd, '--early-time-multirate-backend', backend)
    setarg(cmd, '--n-eff', 50)
    setarg(cmd, '--n-max', 100000)
    setarg(cmd, '--output-file', out / 'output')
    (out / 'command.json').write_text(json.dumps(cmd, indent=2))
    start = time.monotonic()
    with (out / 'ile.log').open('w') as f:
        p = subprocess.run(cmd, cwd=out, stdout=f, stderr=subprocess.STDOUT, timeout=1800)
    assert p.returncode == 0, (tag, p.returncode)
    rows = [np.loadtxt(f).tolist() for f in sorted(out.glob('output_*_.dat'))]
    assert len(rows) == 2, (tag, rows)
    assert all(np.isfinite(r[-4]) and r[-1] >= 50 for r in rows), (tag, rows)
    results[tag] = {'seconds': time.monotonic() - start, 'rows': rows}
    print(tag, results[tag], flush=True)
diffs = [abs(a[-4] - b[-4]) for a, b in zip(results['cupy']['rows'], results['full']['rows'])]
assert max(diffs) < 0.01, diffs
results['abs_dlnZ'] = diffs
(root / ('final_' + band) / 'pinned/results.json').write_text(json.dumps(results, indent=2))
print('PINNED_ILE_PASS', flush=True)
