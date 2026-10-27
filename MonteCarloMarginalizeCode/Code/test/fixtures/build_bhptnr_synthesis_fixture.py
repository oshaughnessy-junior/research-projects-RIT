"""Extract native upstream mode_sum checkpoints; never generate a waveform.

Usage: python build_bhptnr_synthesis_fixture.py FULL_NATIVE_NPZ GEOMETRY_PROTOCOL OUTPUT
The output must not exist. Inputs are the preserved Paper5 geometry-v3 artifacts.
"""
import hashlib
import json
from pathlib import Path
import sys

import numpy as np


def build(native_path, protocol_path, output):
    native_sha = '0d017221e4de10e7a8bcd71cfa5e845909f94e4c0ae4b0c1436554c24d2ed713'
    protocol_sha = '06e244f2421ef22c0b8a35fbea2d8f36122bb7d1d337888331b79484835dabae'
    for path, expected in [(native_path,native_sha),(protocol_path,protocol_sha)]:
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
            raise ValueError('archived source artifact hash mismatch')
    p=json.loads(Path(protocol_path).read_text())
    with np.load(native_path,allow_pickle=False) as f:
        references=[f[f'surrogate_reference_{i}'] for i in range(6)]
        indices=sorted({0,1000,50000,100000,152494,len(f['surrogate_time'])-1,
                        *(int(np.argmax(np.abs(r))) for r in references)})
        def encode(a):return np.stack([a.real,a.imag],axis=-1).tolist()
        result=dict(schema_version=1, source_native_npz_sha256=native_sha,
            source_protocol_sha256=protocol_sha, source_revision=p['surrogate_source_revision'],
            data_sha256='fb8d8dc92bb0a242968e08dc8ab499a73c83737bb5eedfd127df7cb209fbb9a9',
            binary=p['binary'],calibrated=False,source_views=p['source_views'],
            original_n_samples=len(f['surrogate_time']),selected_indices=indices,
            times_seconds=f['surrogate_time'][indices].tolist(),
            positive_modes={f'{l},{m}':encode(f[f'surrogate_h_{l}_{m}'][indices]) for l,m in p['positive_modes']},
            upstream_mode_sum_references=[encode(r[indices]) for r in references],
            reference_full_array_peaks=[float(np.max(np.abs(r))) for r in references],
            relative_error_max=p['surrogate_relative_error_max'],
            scope='selected historical upstream mode_sum samples; finite synthesis only; no runtime or physical-fidelity qualification')
    with Path(output).open('x') as stream:json.dump(result,stream,indent=2);stream.write('\n')


if __name__=='__main__':build(*sys.argv[1:])
