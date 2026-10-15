"""Existing arrays/source only: no RIFT, likelihood, model, fit, draw or inference run."""
import os,time,resource,json,hashlib,argparse,threading
from pathlib import Path
os.environ.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
os.sched_setaffinity(0,{0});resource.setrlimit(resource.RLIMIT_CPU,(24,25))
def guard():
 while True:
  rss=int(next(x.split()[1] for x in Path('/proc/self/status').read_text().splitlines() if x.startswith('VmRSS:')))
  if rss>8*1024**2:os._exit(137)
  time.sleep(.1)
threading.Thread(target=guard,daemon=True).start()
import numpy as np
from scipy.special import logsumexp
from igwn_ligolw import utils,ligolw,lsctables
p=argparse.ArgumentParser()
for k in ['pilot','training','provenance','output']:p.add_argument('--'+k,required=True)
a=p.parse_args();pilot=Path(a.pilot);sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();prov=json.loads(Path(a.provenance).read_text())
r=dict(status='existing_artifact_diagnosis_complete',new_physical_calls=0,training_updates=0,new_proposal_draws=0,inference_runs=0,artifacts={},arms={})
def load(path):
 key='run3/'+path.name;assert sha(path)==prov['artifacts'][key]['sha256'],'pilot array changed: '+key
 r['artifacts'][str(path)]=sha(path);return np.load(path)
def quant(x):return dict(zip(['min','p05','median','p95','max'],[float(v) for v in np.quantile(x,[0,.05,.5,.95,1])]))
summary=json.loads((pilot/'summary.json').read_text());records={}
for arm in ['av','av_gmm','cache','pretrained','tuned']:
 records[arm]={}
 for phase in ['training','evidence','adaptation','validation']:
  files=sorted(pilot.glob(arm+'_[0-9][0-9]_'+phase+'.npz'))
  arrays=[load(f) for f in files]
  records[arm][phase]={k:np.concatenate([d[k] for d in arrays],axis=0) for k in ['x','ll','lp']} if arrays else None
cache_ll=records['cache']['training']['ll'];threshold=float(np.median(cache_ll)-10)
r['diagnostic_only_high_likelihood_threshold']=dict(value=threshold,definition='median of old-target scored same-cache training rows minus10; posterior-mass/accuracy claim forbidden')
for arm,phases in records.items():
 ar=summary['arms'][arm];training=phases['training'];ev=phases['evidence'];norm=ar['normalization_checks']
 info=dict(status=ar['status'],training_ll=quant(training['ll']),training_fraction_above_cache_proxy=float(np.mean(training['ll']>=threshold)),training_cosi_positive_mass=float(np.mean(training['x'][:,3]>.5)),initial_av_volume=norm[0]['av']['volume'],final_av_volume=norm[-1]['av']['volume'],max_av_normalization_error=max(n['av']['max_density_error'] for n in norm),max_gmm_group_normalization_error=max([n['gmm']['max_group_mass_error'] for n in norm if n['gmm']]+[0]),saved_evidence_rows=len(ev['x']),saved_evidence_ll=quant(ev['ll']),evidence_rows_above_cache_proxy=int(np.sum(ev['ll']>=threshold)))
 if ar['status']=='complete':
  d=load(pilot/(arm+'_evidence.npz'));assert np.array_equal(d['x'],ev['x']);ll=ev['ll'];lp=ev['lp'];lw=d['lw'];logq=ll+lp-lw;assert np.isfinite(logq).all()
  ranks=np.argsort(lw)[::-1];top=ranks[:2];relative=np.exp(lw-logsumexp(lw));winner=top[0];labels=d['labels']
  info.update(weight_log_gap_first_second=float(lw[top[0]]-lw[top[1]]),likelihood_gap_for_weight_top_two=float(ll[top[0]]-ll[top[1]]),top_two=dict(indices=top.tolist(),ll=ll[top].tolist(),lp=lp[top].tolist(),logq=logq[top].tolist(),labels=labels[top].tolist()),logq=quant(logq),logprior=quant(lp),loglikelihood_range=float(np.ptp(ll)),logq_range=float(np.ptp(logq)),logprior_range=float(np.ptp(lp)),minimum_q=float(np.exp(logq.min())),max_weight=float(relative.max()),ess=float(1/(relative@relative)),winner_batch=int(winner//100),winner_source=int(labels[winner]),proposal_source_counts={str(v):int(np.sum(labels==v)) for v in np.unique(labels)},per_batch=[dict(batch=i,maximum_ll=float(ll[i*100:(i+1)*100].max()),rows_above_proxy=int(np.sum(ll[i*100:(i+1)*100]>=threshold))) for i in range(10)])
  # Target-score-only concentration is a diagnostic of dynamic range, not IS.
  wt=np.exp(ll+lp-logsumexp(ll+lp));info['target_score_only_effective_count_not_posterior_ess']=float(1/(wt@wt))
 r['arms'][arm]=info
meta=json.loads((Path(a.training)/'result.json').read_text())['data'];rows=[]
for entry in meta['source_archives']:
 path=Path(a.training)/f"input-{entry['seed']}.xml.gz";assert sha(path)==entry['xml_sha256'];r['artifacts'][str(path)]=sha(path)
 doc=utils.load_filename(str(path),contenthandler=ligolw.LIGOLWContentHandler)
 rows.append(np.array([[q.longitude,q.latitude,q.polarization,q.inclination,q.coa_phase,q.distance] for q in lsctables.SimInspiralTable.get_table(doc)]))
train=np.vstack(rows[:3]);held=rows[3]
r['cache_coordinate_diagnostics']=dict(training_rows=len(train),heldout_rows=len(held),training_cosi_positive_mass=float(np.mean(np.cos(train[:,3])>0)),heldout_cosi_positive_mass=float(np.mean(np.cos(held[:,3])>0)),training_distance=quant(train[:,5]),heldout_distance=quant(held[:,5]))
def raw(q):
 angular=np.column_stack([q[:,0],q[:,4],2*q[:,2]]);center=np.arctan2(np.sin(angular).mean(0),np.cos(angular).mean(0));angular=np.arctan2(np.sin(angular-center),np.cos(angular-center))
 return np.column_stack([angular,np.arctanh(np.sin(q[:,1])),np.arctanh(np.cos(q[:,3])),np.log(q[:,5])])
# Within each inclination branch, inspect correlation lost by 3 grouped proposal factors.
group=np.array([0,1,1,0,2,2]);r['within_cache_branch_correlations']={}
for sign in [False,True]:
 chosen=train[(np.cos(train[:,3])>0)==sign];coords=raw(chosen);corr=np.corrcoef(coords,rowvar=False);eigs=np.linalg.eigvalsh(corr);cross=abs(corr)*(group[:,None]!=group[None,:])
 r['within_cache_branch_correlations'][str(sign)]=dict(rows=len(chosen),max_omitted_cross_group_correlation=float(cross.max()),correlation_eigenvalues=eigs.tolist(),minimum_to_maximum_eigenvalue_ratio=float(eigs.min()/eigs.max()),correlation_matrix=corr.tolist())
# Recover stored teacher scores only if the immutable acquisition NPZ contains them.
path=Path(meta['path']);assert sha(path)==meta['sha256'];d=np.load(path,allow_pickle=False);r['teacher_npz_fields']=list(d.files)
for k in d.files:
 if any(s in k.lower() for s in ['log','lnl','lnz','weight']):
  try:v=d[k];r.setdefault('teacher_score_fields',{})[k]=dict(shape=list(v.shape),dtype=str(v.dtype),quantiles=quant(v) if np.issubdtype(v.dtype,np.number) else None)
  except ValueError:r.setdefault('teacher_score_fields',{})[k]=dict(object_field_not_loaded=True)
Path(a.output).write_text(json.dumps(r,indent=2,allow_nan=False)+'\n');print(json.dumps({k:{n:v[n] for n in ['training_fraction_above_cache_proxy','evidence_rows_above_cache_proxy','weight_log_gap_first_second','logq_range'] if n in v} for k,v in r['arms'].items()},indent=2))
