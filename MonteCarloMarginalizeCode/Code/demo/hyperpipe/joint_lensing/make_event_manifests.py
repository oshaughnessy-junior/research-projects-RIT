#!/usr/bin/env python
"""Write explicit 17D source+lens and matching 15D null prior manifests.

Requires an existing prepared.json/npz data bundle in the owning prototype.
These are demonstration priors, not published-event priors.
"""
import argparse
import json
from pathlib import Path
import math

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--data-directory', required=True)
p.add_argument('--outdir', required=True)
a = p.parse_args()
data = Path(a.data_directory).resolve()
meta = json.loads((data / 'prepared.json').read_text())
seed = meta['seeds']['UL'][0]
out = Path(a.outdir).resolve()
out.mkdir(parents=True, exist_ok=False)

def prior(lo, hi, kind='uniform'):
    return dict(min=lo, max=hi, prior=kind)

coordinates = dict(
    chirp_mass=prior(60, 180, 'log_uniform'), mass_ratio=prior(.1, 1),
    a_1=prior(0, .99), a_2=prior(0, .99),
    tilt_1=prior(0, math.pi, 'isotropic'), tilt_2=prior(0, math.pi, 'isotropic'),
    phi_12=prior(0, 2*math.pi), phi_jl=prior(0, 2*math.pi),
    theta_jn=prior(0, math.pi, 'isotropic'), phase=prior(0, 2*math.pi),
    psi=prior(0, math.pi), ra=prior(0, 2*math.pi),
    dec=prior(-math.pi/2, math.pi/2),
    amplitude=prior(.05, 5, 'log_uniform'), time_shift=prior(-.1, .1))
manifest = dict(factory='RIFT.hyperpipe.drivers.joint_lensing:static_network_factory',
                factory_config=dict(data_directory=str(data), clean=True, maximum_frequency=448),
                fixed=dict(geocent_time=seed['geocent_time']), coordinates=coordinates)
for model in ('unlensed', 'lensed'):
    m = dict(manifest, model=model, coordinates=dict(coordinates))
    if model == 'lensed':
        m['coordinates'].update(MLz=prior(10, 10000, 'log_uniform'), y=prior(.05, 3))
    (out / (model + '.json')).write_text(json.dumps(m, indent=2) + '\n')
