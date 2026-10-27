"""Optional locked BHPTNRSur1dq1e4 native positive modes in seconds and strain.

No synthesis, observation epoch, interpolation, response, or inference is implied.
Use one adapter per fresh process: upstream caches HDF5 data in module globals.
"""
from dataclasses import asdict, dataclass
from copy import deepcopy
import hashlib
import importlib
from importlib import metadata
import json
import math
from numbers import Real
import os
from pathlib import Path
import platform
import sys
from types import MappingProxyType
from unittest.mock import patch
import warnings

import numpy as np

SOURCE_REVISION = "403875df67ef4d18fad1da785d7436e7b933ccd1"
DATA_NAME = "BHPTNRSur1dq1e4.h5"
DATA_SIZE = 915165368
DATA_SHA256 = "fb8d8dc92bb0a242968e08dc8ab499a73c83737bb5eedfd127df7cb209fbb9a9"
MAX_SAMPLES = 262144
POSITIVE_MODES = ((2,2),(2,1),(3,1),(3,2),(3,3),(4,2),(4,3),(4,4),
                  (5,3),(5,4),(5,5),(6,4),(6,5),(6,6),(7,5),(7,6),(7,7),
                  (8,6),(8,7),(8,8),(9,7),(9,8),(9,9),(10,8),(10,9))
SOURCE_FILES = {'BHPTNRSurrogate/__init__.py': '1c1108e33a8877fd595a461a53f723fd7ef48bd2ab6894acea469420f9a4044a',
 'BHPTNRSurrogate/_data.py': '55b458eb861603567d95e9678ed383fb87263cd4906668e20b315984c96c5017',
 'BHPTNRSurrogate/surrogates/BHPTNRSur1dq1e4.py': 'da8c079bf7cbb1ee16217c6538a5f3283b4c0cdaf48afabf422964ae1c7da892',
 'BHPTNRSurrogate/surrogates/BHPTNRSur2dq1e3.py': '08ec7e6bad974209ecda64dc7c85f2a350206bf628130bd6eea8a64caf344528',
 'BHPTNRSurrogate/surrogates/__init__.py': 'fc5be47e1b2cbc8d0096205e36c52f97d2fc978d941391944dd58e014c665862',
 'BHPTNRSurrogate/surrogates/common_utils/__init__.py': '15ce30fb77fe9cb14bb6da71ce991330a395b9ea01087e84c07ce3c794806cc0',
 'BHPTNRSurrogate/surrogates/common_utils/check_inputs.py': '4db3acbf24040d465768ea986c5a082d5b05c73f72357ec4a7af7b4ad446ad0e',
 'BHPTNRSurrogate/surrogates/common_utils/doc_string.py': 'f5f817d81b27f0b340c486dafd3a75e6a7243c188e6aaa9a35c5be621655b679',
 'BHPTNRSurrogate/surrogates/common_utils/filehash.py': '3acad4c5c9517247f39ed90fa445f7b13d16ebd06feaacb1b1044054fd73a656',
 'BHPTNRSurrogate/surrogates/common_utils/fits.py': '579a669b68592fbe7f5bcaf2579ad11e2be682855a6e4e6d9aa25a9721caf7ff',
 'BHPTNRSurrogate/surrogates/common_utils/load_GPRs.py': 'd64a4f530e2eb58a4c5ccaf2d43bf45554094d13873db75292dd163868e91aa4',
 'BHPTNRSurrogate/surrogates/common_utils/load_splines.py': '37e98721ae24653e87c78bb1f186a9581087abcf2886cd2f4f7f3604cf491d8d',
 'BHPTNRSurrogate/surrogates/common_utils/nr_calibration.py': '6a67a0362cfa106c031d3f0e070884b3839b8b72d61ab3163f771e2e6058e08e',
 'BHPTNRSurrogate/surrogates/common_utils/utils.py': '211353a58ae75b9175ab177acc1c94de48d757864d337cc27a5d99d766352359',
 'BHPTNRSurrogate/surrogates/model_utils/__init__.py': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
 'BHPTNRSurrogate/surrogates/model_utils/eval_surrogates.py': 'd5451907855df7d911e9cbf670c8b1befbe2d21eeae34ca730b4259233f9bfe9',
 'BHPTNRSurrogate/surrogates/model_utils/load_surrogates.py': '21f646f6671c2749f765edea93bc7acb966c62a6f523d27fa6d511d6b4637ee5'}


def _sha256(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            result.update(block)
    return result.hexdigest()


def _real(value, name):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, Real):
        raise ValueError(name+' must be a finite real scalar')
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(name+' must be a finite real scalar')
    return value


@dataclass(frozen=True)
class BHPTNRParameters:
    primary_msun: float
    secondary_msun: float
    distance_gpc: float

    def __post_init__(self):
        for name in self.__dataclass_fields__:
            object.__setattr__(self, name, _real(getattr(self, name), name))
        if not self.primary_msun >= self.secondary_msun > 0:
            raise ValueError('require primary_msun >= secondary_msun > 0')
        if not 2.5 <= self.primary_msun/self.secondary_msun <= 10000:
            raise ValueError('BHPTNRSur1dq1e4 requires 2.5 <= q <= 10000')
        if self.distance_gpc <= 0:
            raise ValueError('distance_gpc must be positive')
        _real(self.primary_msun+self.secondary_msun, 'total mass')
        _real(1000*self.distance_gpc, 'distance in Mpc')


@dataclass(frozen=True)
class BHPTNRModes:
    time_seconds: np.ndarray
    positive_modes: MappingProxyType
    metadata: dict



def _verify_dependency_origins():
    """Refuse loaded execution submodules outside their installed distribution."""
    receipt = {}
    hashes = {}
    for name in ('numpy', 'scipy', 'h5py', 'gwtools'):
        distribution = metadata.distribution(name)
        if distribution.files is None:
            raise ValueError('dependency distribution lacks a file roster: '+name)
        owned = {Path(distribution.locate_file(item)).resolve() for item in distribution.files}
        modules = {}
        for module_name, module in tuple(sys.modules.items()):
            if module_name != name and not module_name.startswith(name+'.'):
                continue
            location = getattr(module, '__file__', None)
            if location is None:
                continue
            path = Path(location).resolve(strict=True)
            if path not in owned:
                raise ValueError('loaded dependency module is outside its distribution: '+module_name)
            if path not in hashes:
                hashes[path] = _sha256(path)
            modules[module_name] = {'path': str(path), 'sha256': hashes[path]}
        receipt[name] = {'version': distribution.version, 'loaded_modules': modules}
    return receipt


def _preflight(source_root, data_directory, runtime_lock):
    source_root = Path(source_root).resolve(strict=True)
    data_directory = Path(data_directory).resolve(strict=True)
    lock_path = Path(runtime_lock).resolve(strict=True)
    lock = json.loads(lock_path.read_text())
    fields = {'schema_version', 'source_revision', 'python_abi', 'system', 'architecture', 'versions'}
    if not isinstance(lock, dict) or set(lock) != fields:
        raise ValueError('runtime lock has unsupported or missing fields')
    if type(lock['schema_version']) is not int or lock['schema_version'] != 1:
        raise ValueError('runtime lock requires integer schema_version=1')
    if lock['source_revision'] != SOURCE_REVISION:
        raise ValueError('unsupported BHPTNR source revision')
    if (lock['python_abi'] != sys.implementation.cache_tag or
            lock['system'] != platform.system() or lock['architecture'] != platform.machine()):
        raise ValueError('runtime lock platform or Python ABI mismatch')
    if not isinstance(lock['versions'], dict) or set(lock['versions']) != {'numpy','scipy','h5py','gwtools'}:
        raise ValueError('runtime lock requires numpy, scipy, h5py and gwtools versions')
    versions = {name: metadata.version(name) for name in lock['versions']}
    if versions != lock['versions']:
        raise ValueError('runtime dependency versions differ from lock')
    _verify_dependency_origins()
    actual = {str(path.relative_to(source_root)) for path in (source_root/'BHPTNRSurrogate').rglob('*.py')}
    if actual != set(SOURCE_FILES):
        raise ValueError('BHPTNR Python source roster mismatch')
    for relative, expected in SOURCE_FILES.items():
        if _sha256(source_root/relative) != expected:
            raise ValueError('BHPTNR source mismatch: '+relative)
    dataset = data_directory/DATA_NAME
    if dataset.stat().st_size != DATA_SIZE or _sha256(dataset) != DATA_SHA256:
        raise ValueError('BHPTNR model data size or SHA256 mismatch')
    if any(name == 'BHPTNRSurrogate' or name.startswith('BHPTNRSurrogate.') for name in sys.modules):
        raise ValueError('BHPTNR is already imported; use a fresh process to establish cache provenance')
    return source_root, data_directory, {
        'source_revision': SOURCE_REVISION, 'source_files': dict(SOURCE_FILES),
        'data_path': str(dataset), 'data_sha256': DATA_SHA256,
        'runtime_lock_sha256': _sha256(lock_path), 'runtime': lock,
    }


def _no_download(*args, **kwargs):
    raise RuntimeError('BHPTNR adapter forbids implicit model-data downloads')


class BHPTNRAdapter:
    """Verify prefetched artifacts before loading the optional model.

    The pinned upstream module caches one dataset globally. Construct and use
    this adapter in a dedicated process; concurrent initialization is unsupported.
    """

    def __init__(self, source_root, data_directory, runtime_lock, *, calibrated):
        if type(calibrated) is not bool:
            raise ValueError('calibrated must be an explicit bool')
        source, data, self._receipt = _preflight(source_root, data_directory, runtime_lock)
        self.calibrated = calibrated
        old_path = list(sys.path)
        try:
            sys.path.insert(0, str(source))
            with patch.dict(os.environ, {'BHPTNR_SURROGATE_DATA_DIR': str(data)}), \
                    patch('urllib.request.urlretrieve', _no_download):
                self._model = importlib.import_module('BHPTNRSurrogate.surrogates.BHPTNRSur1dq1e4')
            for name, module in tuple(sys.modules.items()):
                if name == 'BHPTNRSurrogate' or name.startswith('BHPTNRSurrogate.'):
                    path = Path(module.__file__).resolve(strict=True)
                    relative = str(path.relative_to(source))
                    if relative not in SOURCE_FILES or _sha256(path) != SOURCE_FILES[relative]:
                        raise ValueError('loaded BHPTNR module differs from pinned source: '+name)
            if Path(self._model.h5_data_dir).resolve() != data or self._model._surrogate_data:
                raise ValueError('BHPTNR data directory or initial cache mismatch')
        finally:
            sys.path[:] = old_path
        self._receipt['model_module_path'] = str(Path(self._model.__file__).resolve())
        self._receipt['loaded_dependencies'] = _verify_dependency_origins()

    def generate(self, parameters):
        if not isinstance(parameters, BHPTNRParameters):
            raise TypeError('parameters must be BHPTNRParameters')
        p = parameters
        with patch('urllib.request.urlretrieve', _no_download), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            times, modes = self._model.generate_surrogate(
                p.primary_msun/p.secondary_msun, modes=list(POSITIVE_MODES),
                M_tot=p.primary_msun+p.secondary_msun, dist_mpc=1000*p.distance_gpc,
                calibrated=self.calibrated, mass_scale='M', lmax=10,
                neg_modes=False, mode_sum=False)
        times = np.asarray(times)
        if times.dtype.kind not in 'fi':
            raise ValueError('backend must return real native times')
        with np.errstate(over='ignore', invalid='ignore'):
            times = np.array(times, dtype=float, copy=True)
        if (times.ndim != 1 or not 2 <= len(times) <= MAX_SAMPLES or times.dtype.kind not in 'fi'
                or not np.all(np.isfinite(times)) or not np.all(np.diff(times) > 0)):
            raise ValueError('backend must return finite increasing native times within the sample cap')
        if not isinstance(modes, dict) or set(modes) != set(POSITIVE_MODES):
            raise ValueError('backend must return all 25 positive modes without reflected partners')
        result = {}
        for mode in POSITIVE_MODES:
            value = np.asarray(modes[mode])
            if value.shape != times.shape or value.dtype.kind not in 'fci' or not np.all(np.isfinite(value)):
                raise ValueError('backend returned an invalid native mode: '+repr(mode))
            with np.errstate(over='ignore', invalid='ignore'):
                result[mode] = np.array(value, dtype=complex, copy=True)
            if not np.all(np.isfinite(result[mode])):
                raise ValueError('native mode is not finite at output precision: '+repr(mode))
            result[mode].setflags(write=False)
        times.setflags(write=False)
        receipt = deepcopy(self._receipt)
        receipt.update(model='BHPTNRSur1dq1e4', parameters=asdict(p), calibrated=self.calibrated,
                       mass_scale='M', time_units='seconds', mode_units='strain',
                       time_origin='native surrogate time origin; no observation epoch assigned',
                       positive_modes=[list(mode) for mode in POSITIVE_MODES], neg_modes=False, mode_sum=False,
                       n_samples=len(times), first_time_seconds=float(times[0]), last_time_seconds=float(times[-1]),
                       warnings=[str(item.message) for item in caught],
                       scope='finite native positive modes only; no response or physical-fidelity qualification')
        return BHPTNRModes(times, MappingProxyType(result), receipt)
