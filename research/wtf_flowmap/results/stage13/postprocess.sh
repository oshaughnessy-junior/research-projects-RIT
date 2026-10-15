set -eu
/cvmfs/software.igwn.org/conda/envs/igwn/bin/python - <<'PY'
from pathlib import Path
import json,hashlib
root=Path('/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage13');p=root/'run1';r=json.loads((p/'summary.json').read_text());sha=lambda x:hashlib.sha256(Path(x).read_bytes()).hexdigest()
files=list(p.glob('*.npz'))+[p/'summary.json',root/'benchmark.time',root/'source/run_retention_benchmark.py',root/'source/journal.json']
m=dict(files={str(x.relative_to(root)):sha(x) for x in files},candidate_sha256=sha(root.parent/'stage11/run1/q8_smoke_2updates.pt'),native_sources={})
code=root.parent/'stage3a/source/MonteCarloMarginalizeCode/Code/RIFT/integrators'
for filename in ['mcsamplerAdaptiveVolume.py','mcsamplerEnsemble.py','gaussian_mixture_model.py','MonteCarloEnsemble.py','unreliable_oracle/resampling.py']:m['native_sources'][filename]=sha(code/filename)
(root/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
PY
/usr/bin/time -v -o /scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage13/components.time /cvmfs/software.igwn.org/conda/envs/igwn/bin/python - <<'PY'
import os,resource,json,hashlib
os.environ.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1');os.sched_setaffinity(0,{0});resource.setrlimit(resource.RLIMIT_CPU,(10,11))
from pathlib import Path
import numpy as np
root=Path('/scratch/richard.oshaughnessy/codex_learned_wtf_20261015/stage13');p=root/'run1';r=json.loads((p/'summary.json').read_text());manifest=json.loads((root/'manifest.json').read_text());threshold=r['shared_reference']['threshold'];result=dict(new_physical_calls=0,new_draws=0,training_updates=0,interpretation='unweighted proposal components, not posterior moments; separate intentional defensive-uniform broadening',components={})
for arm in r['seeds'][0]['arms']:
 f=p/f'seed261071_{arm}_diagnostic.npz';assert hashlib.sha256(f.read_bytes()).hexdigest()==manifest['files'][str(f.relative_to(root))];d=np.load(f)
 result['components'][arm]={}
 for label,name in [(0,'uniform'),(1,'AV'),(2,'GMM')]:
  ix=d['labels']==label;ll=d['postfit_ll'][ix];x=d['postfit_x'][ix]
  result['components'][arm][name]=dict(n=int(ix.sum()),high_score_hits=int(np.sum(ll>=threshold)),maximum_ll=float(ll.max()) if len(ll) else None,mean_ll=float(ll.mean()) if len(ll) else None,mean_distance_mpc=float((1+9999*x[:,5]).mean()) if len(x) else None)
(root/'components.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
PY
