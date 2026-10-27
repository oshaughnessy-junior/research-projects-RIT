"""Locked BHPTNR artifact checks and native-mode API wiring."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import platform
import sys
from types import SimpleNamespace

import numpy as np
import pytest

from RIFT.LISA import bhptnr_adapter as ba


def parameters():
    return ba.BHPTNRParameters(1e6, 200, 1)


@pytest.mark.parametrize('field,value', [('primary_msun',True),('primary_msun',np.inf),
    ('secondary_msun',0),('secondary_msun',1),('secondary_msun',5e5),
    ('distance_gpc',0),('distance_gpc','1'),('distance_gpc',np.nan)])
def test_parameters_refuse_unsupported_values(field, value):
    with pytest.raises(ValueError):
        replace(parameters(), **{field:value})


@pytest.mark.parametrize('q', [2.5,10000])
def test_inclusive_q_boundaries(q):
    assert ba.BHPTNRParameters(q,1,1).primary_msun == q


def artifacts(tmp_path, monkeypatch):
    source = tmp_path/'source'
    package = source/'BHPTNRSurrogate'
    package.mkdir(parents=True)
    (package/'__init__.py').write_text('# fixture\n')
    monkeypatch.setattr(ba, 'SOURCE_FILES', {'BHPTNRSurrogate/__init__.py':ba._sha256(package/'__init__.py')})
    data = tmp_path/'data'
    data.mkdir()
    (data/ba.DATA_NAME).write_bytes(b'fixture-data')
    monkeypatch.setattr(ba, 'DATA_SIZE', 12)
    monkeypatch.setattr(ba, 'DATA_SHA256', hashlib.sha256(b'fixture-data').hexdigest())
    monkeypatch.setattr(ba.metadata, 'version', lambda name:'fixture-version')
    monkeypatch.setattr(ba, '_verify_dependency_origins', lambda:{'fixture':True})
    lock = {'schema_version':1,'source_revision':ba.SOURCE_REVISION,
            'python_abi':sys.implementation.cache_tag,'system':platform.system(),
            'architecture':platform.machine(),
            'versions':dict.fromkeys(['numpy','scipy','h5py','gwtools'],'fixture-version')}
    lock_path = tmp_path/'runtime.json'
    lock_path.write_text(json.dumps(lock))
    return source, data, lock_path, lock


def test_preflight_does_not_import_model(tmp_path, monkeypatch):
    source,data,path,_ = artifacts(tmp_path, monkeypatch)
    monkeypatch.setattr(ba.importlib, 'import_module', lambda name:pytest.fail('unexpected model import'))
    _,_,receipt = ba._preflight(source,data,path)
    assert receipt['data_sha256'] == ba.DATA_SHA256
    assert receipt['runtime_lock_sha256'] == ba._sha256(path)


@pytest.mark.parametrize('field,value', [('schema_version',True),('source_revision','other'),
    ('python_abi','other'),('system','other'),('architecture','other'),('versions',{}),('extra',1)])
def test_lock_mismatch_precedes_backend_import(tmp_path, monkeypatch, field, value):
    source,data,path,lock = artifacts(tmp_path,monkeypatch)
    lock[field] = value
    path.write_text(json.dumps(lock))
    monkeypatch.setattr(ba.importlib,'import_module',lambda name:pytest.fail('unexpected model import'))
    with pytest.raises(ValueError):
        ba.BHPTNRAdapter(source,data,path,calibrated=False)


@pytest.mark.parametrize('mutation', ['missing-data','data-bytes','source-bytes','source-extra','preloaded','version'])
def test_artifact_failures_precede_backend_import(tmp_path, monkeypatch, mutation):
    source,data,path,_ = artifacts(tmp_path,monkeypatch)
    if mutation == 'missing-data':
        (data/ba.DATA_NAME).unlink()
    elif mutation == 'data-bytes':
        (data/ba.DATA_NAME).write_bytes(b'wrong-data!!')
    elif mutation == 'source-bytes':
        (source/'BHPTNRSurrogate/__init__.py').write_text('# changed\n')
    elif mutation == 'source-extra':
        (source/'BHPTNRSurrogate/extra.py').write_text('')
    elif mutation == 'preloaded':
        monkeypatch.setitem(sys.modules, 'BHPTNRSurrogate', SimpleNamespace())
    else:
        monkeypatch.setattr(ba.metadata,'version',lambda name:'wrong')
    monkeypatch.setattr(ba.importlib,'import_module',lambda name:pytest.fail('unexpected model import'))
    with pytest.raises((ValueError,FileNotFoundError)):
        ba.BHPTNRAdapter(source,data,path,calibrated=False)


def fake_adapter(monkeypatch, *, output=None, calibrated=False):
    calls = []
    def generate(*args, **kwargs):
        calls.append((args,kwargs))
        if output is not None:
            return output
        return np.array([-3.,-1.,2.]), {mode:np.array([1+2j,2+3j,3+4j]) for mode in ba.POSITIVE_MODES}
    adapter = object.__new__(ba.BHPTNRAdapter)
    adapter.calibrated = calibrated
    adapter._receipt = {'fixture':True}
    adapter._model = SimpleNamespace(generate_surrogate=generate)
    return adapter, calls


@pytest.mark.parametrize('calibrated',[False,True])
def test_generate_wires_full_native_modes_without_sampling(monkeypatch, calibrated):
    adapter,calls = fake_adapter(monkeypatch,calibrated=calibrated)
    result = adapter.generate(parameters())
    args,kwargs = calls[0]
    assert args == (5000.,)
    assert kwargs == dict(modes=list(ba.POSITIVE_MODES),M_tot=1000200.,dist_mpc=1000.,
                          calibrated=calibrated,mass_scale='M',lmax=10,neg_modes=False,mode_sum=False)
    assert np.array_equal(result.time_seconds,[-3.,-1.,2.])
    assert set(result.positive_modes) == set(ba.POSITIVE_MODES)
    assert len(result.positive_modes) == 25
    assert result.metadata['calibrated'] is calibrated
    assert not result.time_seconds.flags.writeable
    assert all(not value.flags.writeable for value in result.positive_modes.values())
    with pytest.raises(TypeError):
        result.positive_modes[(2,-2)] = np.ones(3)
    json.dumps(result.metadata,allow_nan=False)


@pytest.mark.parametrize('times', [np.array([0]),np.array([0,0]),np.array([1,0]),
    np.array([0,np.nan]),np.array([0,1j]),np.zeros((1,2)), np.arange(ba.MAX_SAMPLES+1),
    np.array([2**53,2**53+1],dtype=np.int64)])
def test_invalid_native_times_refused(monkeypatch,times):
    adapter,_ = fake_adapter(monkeypatch,output=(times,{}))
    with pytest.raises(ValueError,match='native times'):
        adapter.generate(parameters())


@pytest.mark.parametrize('mutation', ['missing','negative','shape','nonfinite','object'])
def test_invalid_mode_outputs_refused(monkeypatch,mutation):
    modes = {mode:np.ones(3,dtype=complex) for mode in ba.POSITIVE_MODES}
    if mutation == 'missing': del modes[(2,2)]
    elif mutation == 'negative': modes[(2,-2)]=np.ones(3)
    elif mutation == 'shape': modes[(2,2)]=np.ones(2)
    elif mutation == 'nonfinite': modes[(2,2)]=np.array([1,np.nan,2])
    else: modes[(2,2)]=np.array([1,2,3],dtype=object)
    adapter,_ = fake_adapter(monkeypatch,output=(np.arange(3.),modes))
    with pytest.raises(ValueError):
        adapter.generate(parameters())


def test_wrong_parameter_type_never_calls_backend(monkeypatch):
    adapter,calls = fake_adapter(monkeypatch)
    with pytest.raises(TypeError): adapter.generate({})
    assert calls == []


def test_download_is_refused():
    with pytest.raises(RuntimeError,match='downloads'): ba._no_download('unused')


@pytest.mark.parametrize('calibrated',[None,0,'false',np.bool_(False)])
def test_calibration_choice_is_explicit(tmp_path,monkeypatch,calibrated):
    monkeypatch.setattr(ba,'_preflight',lambda *args:pytest.fail('unexpected preflight'))
    with pytest.raises(ValueError,match='explicit bool'):
        ba.BHPTNRAdapter(tmp_path,tmp_path,tmp_path,calibrated=calibrated)


def test_constructor_checks_import_identity_and_restores_environment(tmp_path,monkeypatch):
    source,data,path,_ = artifacts(tmp_path,monkeypatch)
    model = SimpleNamespace(__file__=str(tmp_path/'unverified.py'),h5_data_dir=str(data),_surrogate_data={})
    monkeypatch.setenv('BHPTNR_SURROGATE_DATA_DIR','original')
    before_path = list(sys.path)
    original_import = ba.importlib.import_module
    def imported(name):
        if not name.startswith('BHPTNRSurrogate'):
            return original_import(name)
        assert ba.os.environ['BHPTNR_SURROGATE_DATA_DIR'] == str(data)
        assert sys.path[0] == str(source)
        monkeypatch.setitem(sys.modules,'BHPTNRSurrogate.surrogates.BHPTNRSur1dq1e4',model)
        return model
    monkeypatch.setattr(ba.importlib,'import_module',imported)
    with pytest.raises((ValueError,FileNotFoundError)):
        ba.BHPTNRAdapter(source,data,path,calibrated=False)
    assert sys.path == before_path
    assert ba.os.environ['BHPTNR_SURROGATE_DATA_DIR'] == 'original'


def test_generation_does_not_expose_receipt_for_mutation(monkeypatch):
    adapter,_ = fake_adapter(monkeypatch)
    adapter._receipt = {'runtime':{'versions':{'fixture':'1'}}}
    first = adapter.generate(parameters())
    first.metadata['runtime']['versions']['fixture'] = 'changed'
    assert adapter.generate(parameters()).metadata['runtime']['versions']['fixture'] == '1'


@pytest.mark.parametrize('foreign',[False,True])
def test_dependency_identity_covers_execution_submodules(tmp_path,monkeypatch,foreign):
    package = tmp_path/'gwtools'
    package.mkdir()
    for filename in ('__init__.py','gwtools.py'):
        (package/filename).write_text('# fixture\n')
    distribution = SimpleNamespace(files=['gwtools/__init__.py','gwtools/gwtools.py'],
        locate_file=lambda item:tmp_path/item,version='fixture')
    other = SimpleNamespace(files=[],locate_file=lambda item:tmp_path/item,version='fixture')
    monkeypatch.setattr(ba.metadata,'distribution',lambda name:distribution if name=='gwtools' else other)
    for name in tuple(sys.modules):
        if any(name==root or name.startswith(root+'.') for root in ('numpy','scipy','h5py','gwtools')):
            monkeypatch.delitem(sys.modules,name)
    monkeypatch.setitem(sys.modules,'gwtools',SimpleNamespace(__file__=str(package/'__init__.py')))
    location = tmp_path/'outside.py' if foreign else package/'gwtools.py'
    if foreign: location.write_text('# wrong execution module\n')
    monkeypatch.setitem(sys.modules,'gwtools.gwtools',SimpleNamespace(__file__=str(location)))
    if foreign:
        with pytest.raises(ValueError,match='gwtools.gwtools'):
            ba._verify_dependency_origins()
    else:
        receipt=ba._verify_dependency_origins()
        assert 'gwtools.gwtools' in receipt['gwtools']['loaded_modules']
