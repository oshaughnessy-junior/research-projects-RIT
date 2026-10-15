"""Reconstruct omitted failed batch using saved likelihoods, zero true dispatch.

This is a verified deterministic replay, not an original failed-batch capture.
"""
import os
os.sched_setaffinity(0,{0})
import json,hashlib
from pathlib import Path
import numpy as np
from run_five_arm import factory,cube,PARAMS,mixture_draw
from igwn_ligolw import utils,ligolw,lsctables
from RIFT.likelihood.priors_utils import get_astropy_cosmology,norm_and_inverse_via_grid_interp
from astropy.cosmology import z_at_value
from astropy import units as u
ROOT=Path('/scratch/richard.oshaughnessy/codex_learned_wtf_20261015')
out=ROOT/'stage4/run3';saved=[]
for f in sorted(out.glob('cache_[0-9]*.npz')):
    if 'validation' in f.name:continue
    d=np.load(f);saved.append((f.name,d['x'],d['ll']+d['lp']))
class Capture(Exception):pass
index=0

def replay(x,phase):
    global index
    if index==len(saved):
        assert phase=='evidence';np.savez_compressed(out/'cache_failed_batch_reconstructed.npz',x=x)
        raise Capture()
    name,want,value=saved[index]
    assert phase in name
    np.testing.assert_array_equal(x,want)
    index+=1;return value

np.random.seed(261051);rng=np.random.RandomState(261051)
meta=json.loads((ROOT/'final_seed261015/result.json').read_text())['data'];rows=[]
for source in meta['source_archives'][:3]:
    path=ROOT/'final_seed261015'/f"input-{source['seed']}.xml.gz"
    doc=utils.load_filename(str(path),contenthandler=ligolw.LIGOLWContentHandler)
    rows.append(np.array([[q.longitude,q.latitude,q.polarization,q.inclination,q.coa_phase,q.distance] for q in lsctables.SimInspiralTable.get_table(doc)]))
train=np.vstack(rows);trng=np.random.RandomState(261050);x=cube(train[trng.choice(len(train),100,replace=True)])
av,gm=factory();replay(x,'training');av.bootstrap_from_samples(x,loglkl=None,cover_frac=0,dilate=1,inflate=1,seed=261050)
gm.update_sampling_prior(np.zeros(100),n_history=100,external_rvs={p:x[:,i] for i,p in enumerate(PARAMS)},tempering_exp=.1,log_scale_weights=True)
points=[];weights=[];adapt=0
try:
    for chunk in range(10):
        x,q,labels=mixture_draw(av,gm,100,rng,[.2,.4,.4]);lt=replay(x,'evidence');points.append(x);weights.append(lt-np.log(q))
        if chunk<9:
            needed=int(np.sum(av.ninbin))
            if needed<=894-adapt:
                def callback(*cols):
                    global adapt
                    v=np.column_stack(cols);adapt+=len(v);return replay(v,'adaptation')
                av.update_sampling_prior_selfish(callback,*PARAMS,xpy=np)
            ex=np.vstack(points);lw=np.concatenate(weights)
            gm.update_sampling_prior(lw.copy(),n_history=len(ex),external_rvs={p:ex[:,i] for i,p in enumerate(PARAMS)},tempering_exp=.1,log_scale_weights=True)
except Capture:
    c=get_astropy_cosmology('Planck15');zlo=float(z_at_value(c.luminosity_distance,1*u.Mpc));zhi=float(z_at_value(c.luminosity_distance,10000*u.Mpc))
    pdf,_,_=norm_and_inverse_via_grid_interp(lambda z:c.differential_comoving_volume(z).value/(1+z),[zlo,zhi],y_of_x=lambda z:c.luminosity_distance(z).value,vectorized=True)
    D=1+9999*x[:,5];p=np.asarray(pdf(D));bad=~np.isfinite(p)|(p<=0)
    r=dict(status='reconstructed_after_exact_saved_prefix_match',prefix_batches_matched=index,
        failed_batch_rows=len(x),true_likelihood_attempts=0,cache_target_replay_rows=sum(len(v[1]) for v in saved),
        nonpositive_or_nonfinite_prior_rows=int(bad.sum()),distance_min=float(D.min()),distance_max=float(D.max()),
        prior_interpolation_domain=[float(pdf.x[0]),float(pdf.x[-1])],boundary_rows=int(np.any((x==0)|(x==1),axis=1).sum()),
        failure_capture='reconstruction; original failing batch was not saved')
    (out/'cache-replay.json').write_text(json.dumps(r,indent=2));print(json.dumps(r,indent=2))
