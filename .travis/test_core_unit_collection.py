"""Bounded accounting tests; these are separate from the 105-file core census."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

import core_unit_collection as collection


@pytest.fixture
def manifest(tmp_path):
    files=['test_a.py','sub/test_b.py']
    (tmp_path/'sub').mkdir()
    for name in files:(tmp_path/name).write_text('')
    return files


def test_exact_files_classes_parameters_and_shortened_rootpath(tmp_path,manifest):
    nodeids=['test_a.py::TestClass::test_method[param::value]',
             r'test_a.py::test_regex[\n]', 'sub/test_b.py::test_plain']
    assert collection.count_nodeids(manifest,nodeids,tmp_path,tmp_path)=={
        'test_a.py':2,'sub/test_b.py':1}
    assert collection.count_nodeids([manifest[1]],['test_b.py::test_plain'],tmp_path/'sub',tmp_path)=={manifest[1]:1}


def test_absent_file_is_counted_zero_not_omitted(tmp_path,manifest):
    assert collection.count_nodeids(manifest,['test_a.py::test_one'],tmp_path,tmp_path)=={
        'test_a.py':1,'sub/test_b.py':0}


@pytest.mark.parametrize('nodeids',[
    [],['test_a.py'],['test_a.py::'],['::test'],['../test_a.py::test'],
    ['/test_a.py::test'],['test_a.py::test\nspoof'],['test_a.py::test','test_a.py::test'],
    ['sub\\test_b.py::test'],[None],['unlisted.py::test'],
])
def test_malformed_outside_empty_and_duplicate_nodeids_fail_closed(tmp_path,manifest,nodeids):
    (tmp_path/'unlisted.py').write_text('')
    with pytest.raises((ValueError,OSError)):
        collection.count_nodeids(manifest,nodeids,tmp_path,tmp_path)


def test_duplicate_manifest_alias_empty_or_unsafe_manifest_refuses(tmp_path,manifest):
    for files in [[],[manifest[0],str(tmp_path/manifest[0])],[manifest[0]+'\t']]:
        with pytest.raises(ValueError):
            collection.count_nodeids(files,['test_a.py::test'],tmp_path,tmp_path)


def invoke(tmp_path,files):
    output=tmp_path/'counts.tsv'
    env=dict(os.environ,PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',PYTHONDONTWRITEBYTECODE='1')
    result=subprocess.run([sys.executable,str(Path(collection.__file__).resolve()),str(output),*files],
                          cwd=tmp_path,env=env,text=True,capture_output=True,timeout=20)
    return result,output


def test_one_joint_real_collection_counts_items_not_warning_stdout(tmp_path):
    (tmp_path/'test_a.py').write_text('''import pytest, warnings
warnings.warn("test_b.py::warning_is_not_a_test")
@pytest.mark.parametrize("x",[1,2])
def test_parameter(x): pass
class TestClass:
 def test_method(self): pass
''')
    (tmp_path/'project_helper.py').write_text('VALUE=42\n')
    (tmp_path/'test_b.py').write_text('import project_helper\ndef test_one(): assert project_helper.VALUE==42\n')
    result,output=invoke(tmp_path,['test_a.py','test_b.py'])
    assert result.returncode==0,result.stdout+result.stderr
    assert 'warning_is_not_a_test' in result.stdout
    assert output.read_text()=='test_a.py\t3\ntest_b.py\t1\n'


@pytest.mark.parametrize('content',[
    'def broken(\n', 'import no_such_core_collection_dependency_123\n',
    'raise RuntimeError("collection failed")\n', '',
])
def test_actual_collection_errors_or_no_tests_never_publish_counts(tmp_path,content):
    (tmp_path/'test_bad.py').write_text(content)
    result,output=invoke(tmp_path,['test_bad.py'])
    assert result.returncode!=0 and not output.exists()


def test_import_skipped_file_remains_zero_for_shell_floor(tmp_path):
    (tmp_path/'test_ok.py').write_text('def test_one(): pass\n')
    (tmp_path/'test_skip.py').write_text('import pytest\npytest.skip("missing dependency",allow_module_level=True)\n')
    result,output=invoke(tmp_path,['test_ok.py','test_skip.py'])
    assert result.returncode==0,result.stdout+result.stderr
    assert output.read_text()=='test_ok.py\t1\ntest_skip.py\t0\n'


@pytest.mark.parametrize("known_skip,new_skip,invalid_metadata,expected_rc", [
    (False, False, None, 0), (True, False, None, 0),
    (False, True, None, 1), (True, True, None, 1),
    (True, False, "reason", 1), (True, False, "classname", 1),
])
def test_outcome_allowance_requires_named_actual_skip(
        tmp_path, known_skip, new_skip, invalid_metadata, expected_rc):
    """Synthetic accounting replay: subtest passes cannot fund an ordinary skip.

    The scientific suites are not executed. The real shell gate consumes synthetic
    collection counts and JUnit, while its own XML parser and every guard run.
    """
    import re
    import xml.etree.ElementTree as ET

    script = Path(__file__).with_name("test-core-units.sh").resolve()
    source = script.read_text()
    value = lambda name: int(re.search(r"^"+name+r"=(\d+)", source, re.MULTILINE)[1])
    tests, base_skips = value("EXPECTED_TESTS"), value("MAX_SKIPPED")
    total = max(tests, value("EXPECTED_PASSED") + base_skips + int(known_skip)) + 3
    xml = tmp_path/"synthetic-outcomes.xml"
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite", tests=str(total), failures="0", errors="0",
                          skipped=str(base_skips+int(known_skip)+int(new_skip)))
    if known_skip:
        case = ET.SubElement(suite, "testcase",
            classname="MonteCarloMarginalizeCode.Code.test.test_eos_posterior_tempering_kwarg",
            name="test_the_argparse_spelling_is_inert_at_the_sampler")
        if invalid_metadata == "classname":
            case.set("classname", "synthetic.not_test_eos_posterior_tempering_kwarg")
        reason = "synthetic wrong reason" if invalid_metadata == "reason" else (
            "cupy importable: run is not bit-reproducible under numpy seeding")
        ET.SubElement(case, "skipped", type="pytest.skip", message=reason)
    ET.ElementTree(root).write(xml)
    stub = tmp_path/"accounting-python"
    stub.write_text("#!"+sys.executable+"\n"+r'''
import os,sys,shutil
from pathlib import Path
args=sys.argv[1:]
if args[0]=='-c':sys.exit(0)
if args[0]=='-':os.execv(sys.executable,[sys.executable,'-I','-S',*args])
if args[0].endswith('core_unit_collection.py'):
    files=args[2:]
    floors={'test_lalsim_eos_compat.py':49,'test_waveform_sample_rejection.py':19,
            'test_posterior_compression.py':41,'test_raw_distance_contract.py':44,
            'test_auxiliary_grid_transfers.py':18,'test_gmm_sample_matches_score.py':17,
            'test_eos_affine_central.py':49,'test_nmb_sequence_branches.py':16}
    counts=[floors.get(Path(name).name,1) for name in files]
    counts[-1]+=int(os.environ['SYNTHETIC_TEST_COUNT'])-sum(counts)
    Path(args[1]).write_text(''.join(name+'\t'+str(n)+'\n' for name,n in zip(files,counts)))
    sys.exit(0)
if args[:2]==['-m','pytest']:
    path=next(a.split('=',1)[1] for a in args if a.startswith('--junit-xml='))
    shutil.copyfile(os.environ['SYNTHETIC_JUNIT'],path)
    sys.exit(0)
raise RuntimeError('unexpected accounting invocation')
''')
    stub.chmod(0o755)
    env = dict(os.environ, RIFT_COREUNIT_PYTHON=str(stub), RIFT_COREUNIT_WORKERS="1",
               SYNTHETIC_JUNIT=str(xml), SYNTHETIC_TEST_COUNT=str(tests),
               PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(["bash", str(script)], env=env, capture_output=True,
                            text=True, timeout=120)
    assert result.returncode == expected_rc, result.stdout+result.stderr
    if expected_rc:
        assert "SKIPPED, at most" in result.stderr
    else:
        assert "core unit gate: PASS" in result.stdout
