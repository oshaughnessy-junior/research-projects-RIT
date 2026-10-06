"""Render the actual profile and template; no DAG submission or likelihoods."""
from pathlib import Path
import copy
import pytest
from test_asimov_rift_template_contract import _base_meta,_render

def merge(target,overlay):
    for key,value in overlay.items():
        if isinstance(value,dict) and isinstance(target.get(key),dict):
            merge(target[key],value)
        else:target[key]=copy.deepcopy(value)


def test_bootstrapped_xphm_profile_renders_requested_settings():
    yaml=pytest.importorskip('yaml')
    p=Path(__file__).with_name('blueprints')/'analysis_rift_XPHM_lossless_q.yaml'
    overlay=yaml.safe_load(p.read_text())
    meta=_base_meta();merge(meta,overlay)
    _,ini=_render(meta);section='rift-pseudo-pipe'
    assert ini.get('engine','srate').strip()=='4096'
    assert ini.get('engine','approx').strip()=='IMRPhenomXPHM'
    assert ini.get('engine','fref').strip()=='20'
    assert ini.get(section,'cip-fit-method').strip('"')=='rf'
    assert ini.get(section,'rf-transverse-spin-coordinates').strip('"')=='lossless-q'
    assert ini.get(section,'internal-ile-interpolate-time').strip("'")=='cubic'
    # scheduler.pipeline settings reach pseudo_pipe on its CLI, not through INI.
    import ast
    from types import SimpleNamespace
    source=Path(__file__).resolve().parents[2]/'RIFT/asimov/rift.py'
    tree=ast.parse(source.read_text())
    build=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='build_dag')
    block=next(n for n in build.body if isinstance(n,ast.If)
        and ast.unparse(n.test)=='"pipeline" in self.production.meta["scheduler"]'.replace('"', "'"))
    namespace={'self':SimpleNamespace(production=SimpleNamespace(meta=meta)),'command':[]}
    exec(compile(ast.Module(body=[block],type_ignores=[]),'pipeline-passthrough','exec'),namespace)
    cli=namespace['command']
    assert '--internal-ile-sky-network-coordinates' in cli
    assert '--internal-ile-srate-time-resampling=4096' in cli
    assert '--internal-truncate-cip-arg-list=2' in cli
    assert ini.get(section,'internal-propose-converge-last-stage').lower()=='true'
    assert ini.get(section,'calibration-reweighting').lower()=='false'
    assert ini.get(section,'add-extrinsic').lower()=='true'
    assert ini.get(section,'add-extrinsic-time-resampling').lower()=='true'
    assert '--av-stop-metric kish' in ini.get(section,'manual-extra-cip-args')
    assert overlay['scheduler']['bootstrap size']==10000
    assert overlay['scheduler']['bootstrap reuse existing'] is False
    # Reviewed event physical priors survive the overlay exactly.
    assert meta['priors']==_base_meta()['priors']


def test_profile_requests_internal_subdag():
    yaml=pytest.importorskip("yaml")
    p=Path(__file__).with_name("blueprints")/"analysis_rift_XPHM_lossless_q.yaml"
    assert yaml.safe_load(p.read_text())["scheduler"]["pipeline"]["use-subdags"] is True


def test_staged_frames_skip_truncation_and_unused_oauth():
    meta=_base_meta()
    meta['scheduler']['osg']=True
    meta['scheduler']['frames directory']='/verified/frames'
    meta['scheduler']['oauth service']='none'
    _,ini=_render(meta)
    section='rift-pseudo-pipe'
    assert ini.get(section,'internal-staged-frames-directory').strip("'")== '/verified/frames'
    assert ini.get(section,'internal-truncate-files-for-osg-file-transfer').lower()=='false'
    assert not ini.has_option(section,'internal-use-oauth-files')
