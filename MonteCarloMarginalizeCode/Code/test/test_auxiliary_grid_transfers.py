"""Drive CEPP with the writer's real transfer-list mutations for every auxiliary grid."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

CODE = Path(__file__).resolve().parents[1]
BUILDER = CODE / 'bin' / 'create_event_parameter_pipeline_BasicIteration'

@pytest.mark.parametrize('frame_mode', ['staged', 'cvmfs'])
@pytest.mark.parametrize('schedule', ['ordinary', 'Z', 'G'])
@pytest.mark.parametrize('seeded', [True, False])
def test_auxiliary_grid_transfers_are_independent(tmp_path, frame_mode, schedule, seeded):
    lal = pytest.importorskip('lal')
    from RIFT import lalsimutils
    p = lalsimutils.ChooseWaveformParams()
    p.m1, p.m2 = 30 * lal.MSUN_SI, 20 * lal.MSUN_SI
    lalsimutils.ChooseWaveformParams_array_to_xml([p], fname=str(tmp_path/'input'))
    for name, text in [('ile', 'X --n-eff 20 --time-marginalization --vectorized --gpu --force-xpy'),
                       ('cip', 'X --no-plots --fit-method rf --parameter mc --parameter eta'),
                       ('seed', 'X'), ('puff', 'X --parameter mc'), ('fetch', 'X')]:
        (tmp_path/(name+'.args')).write_text(text+'\n')
    (tmp_path/'psd.xml.gz').touch()
    (tmp_path/'transfer.txt').write_text(str(tmp_path/'psd.xml.gz')+'\n')
    (tmp_path/'frames').mkdir()
    (tmp_path/'local.cache').touch()
    env = dict(os.environ, SINGULARITY_RIFT_IMAGE='/unused-image.sif',
               SINGULARITY_BASE_EXE_DIR='/usr/local/bin', OMP_NUM_THREADS='1',
               OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    env['PATH'] = str(CODE/'bin')+os.pathsep+str(Path(sys.executable).parent)+os.pathsep+env.get('PATH','')
    env['PYTHONPATH'] = str(CODE)+os.pathsep+env.get('PYTHONPATH','')
    cmd = [sys.executable, os.environ.get('RIFT_CEPP_UNDER_TEST',str(BUILDER)),
           '--working-directory', str(tmp_path), '--input-grid', str(tmp_path/'input.xml.gz'),
           '--ile-args', str(tmp_path/'ile.args'), '--cip-args', str(tmp_path/'cip.args'),
           '--seed-climb-exe', '/bin/true', '--seed-climb-args', str(tmp_path/'seed.args'),
           '--seed-climb-mode', 'composite', '--seed-climb-n-max', '2',
           '--puff-args', str(tmp_path/'puff.args'), '--puff-cadence', '1',
           '--fetch-ext-grid-exe', '/bin/true', '--fetch-ext-grid-args', str(tmp_path/'fetch.args'),
           '--transfer-file-list', str(tmp_path/'transfer.txt'),
           '--n-iterations', '2', '--n-samples-per-job', '2', '--use-singularity',
           '--last-iteration-extrinsic', '--last-iteration-extrinsic-nsamples', '2']
    if schedule != 'ordinary':
        (tmp_path/'cip.list').write_text('1 --no-plots --fit-method rf --parameter mc --parameter eta\n'+('Z' if schedule=='Z' else 'G1')+' --no-plots --fit-method rf --parameter mc --parameter eta\n')
        cmd += ['--cip-args-list', str(tmp_path/'cip.list'), '--n-iterations-subdag-max', '1']
    if not seeded:
        for key in ['--seed-climb-exe', '--seed-climb-args', '--seed-climb-mode', '--seed-climb-n-max']:
            index=cmd.index(key);del cmd[index:index+2]
    if frame_mode == 'staged':
        cmd += ['--frames-dir', str(tmp_path/'frames')]
    else:
        cmd += ['--cache-file', str(tmp_path/'local.cache'), '--use-cvmfs-frames']
    proc = subprocess.run(cmd,cwd=tmp_path,env=env,capture_output=True,text=True,timeout=90)
    assert proc.returncode == 0, proc.stdout+proc.stderr
    for tag,grid in [('ILE_puff','puffball'),('ILE_fetch','fetch'),('ILE_extr','overlap-grid')]+([('ILE_seed','seed')] if seeded else []):
        sub=(tmp_path/(tag+'.sub')).read_text()
        line=next(l for l in sub.splitlines() if l.startswith('transfer_input_files ='))
        if tag != 'ILE_extr':
            assert 'overlap-grid-' not in line, (tag,line)
        assert grid+'-$(macroiteration).xml.gz' in line
        assert 'psd.xml.gz' in line
        assert line.count('ile_pre.sh') <= 1
        assert line.count('ile_dispatch.sh') <= 1

    # Every final extrinsic worker must consume the grid emitted by the final fit.
    import re
    dag=(tmp_path/'marginalize_intrinsic_parameters_BasicIterationWorkflow.dag').read_text()
    extr_nodes=re.findall(r'^JOB (\S+) .*ILE_extr.sub$',dag,re.M)
    assert extr_nodes
    for node in extr_nodes:
        variables=re.search(r'^VARS '+re.escape(node)+r' (.*)$',dag,re.M).group(1)
        assert 'macroiteration="2"' in variables, variables
    assert (tmp_path/'iteration_2_ile/logs').is_dir()
