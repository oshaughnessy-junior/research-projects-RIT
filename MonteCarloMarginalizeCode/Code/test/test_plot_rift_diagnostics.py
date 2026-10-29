"""Adversarial tests of PR249's actual diagnostic functions, without running a campaign."""
import ast
import base64
from pathlib import Path
import html
import io
import json
import os
import re
import time
from types import SimpleNamespace
import glob
import numpy as np
import pytest
from RIFT import lalsimutils as lsu

CODE = Path(__file__).resolve().parents[1]
SCRIPT = CODE/'bin/plot_RIFT.py'


@pytest.fixture
def plot():
    # Execute the shipped helper definitions without parsing CLI or generating plots.
    tree = ast.parse(SCRIPT.read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) or
             isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and
             t.id in ('HTML_TEMPLATE','PUFFING_FOLDER','PUFFING_THRESHOLDS','PUFFING_MAX_ITERATIONS') for t in n.targets)]
    env = dict(np=np, Path=Path, html=html, io=io, json=json, os=os, re=re,
               time=time, base64=base64, glob=glob, lsu=lsu,
               run_diagnostics={}, JSD_threshold=.005, LISA=False,
               eccentricity=True, precessing=False, non_spinning=False,
               puffing_file_cache={})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT), 'exec'), env)
    return SimpleNamespace(**env), env


def test_aligned_eccentric_matching_and_missing_labels(plot, tmp_path):
    p, env = plot
    env['precessing'] = False
    data = np.zeros((2, 15)); data[:,1:3] = [20,15]
    data[:,5] = [-.3,-.2]; data[:,8] = -.1
    data[:,9] = .1; data[:,10] = [.01,6.27]
    data[:,11] = [100,200]; data[:,12] = .01
    path = tmp_path/'consolidated_0.composite'; np.savetxt(path,data)
    params = {name: np.array([data[1,col]]) for name,col in p.composite_column_layout().items() if name not in ('lnL','err')}
    result = p.match_to_composite(params,str(path))
    assert result['matched_fraction'] == 1
    assert result['lnL'][0] == 200
    params['s1z'][:] = .7
    assert np.isnan(p.match_to_composite(params,str(path))['lnL'][0])


def test_unmatched_or_missing_labels_stay_unknown(plot, tmp_path):
    p, _ = plot
    result = p.find_lnL_of_points({}, str(tmp_path/'missing'), 'composite')
    assert result['lnL'] is None
    assert 'not found' in result['note']


def test_html_embeds_actual_diagnostic_images(plot, tmp_path):
    p, env = plot
    from PIL import Image
    plots = tmp_path/'plots'; (plots/'histograms').mkdir(parents=True)
    image = plots/'histograms/histogram_Main_mc.png'
    Image.new('RGB',(20,10),'white').save(image)
    (plots/'Diagnostics.txt').write_text('Run summary\n'+'='*80+'\nfixture\n\nILE diagnostics\n'+'='*80+'\nILE status: GOOD\n')
    p.write_html_diagnostics(str(tmp_path))
    text = (plots/'Diagnostics.html').read_text()
    assert 'data:image/png;base64,' in text
    assert 'Puffing health' in text
    assert 'histogram_Main_mc.png' not in text


def test_gap_center_handles_two_lobes_across_seam(plot):
    p, _ = plot
    summary, unwrapped = p.circular_summary(np.array([.01,.02,2*np.pi-.02,2*np.pi-.01]))
    assert np.ptp(unwrapped) == pytest.approx(.04)
    assert summary['largest_empty_arc'] > 6
