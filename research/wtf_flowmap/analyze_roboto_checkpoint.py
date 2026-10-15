"""Pure checkpoint/cached-target analysis: zero likelihood evaluations."""
import os,time,resource,json,hashlib,argparse,threading
from pathlib import Path
os.environ.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
os.sched_setaffinity(0,{0});resource.setrlimit(resource.RLIMIT_CPU,(15,16))
def guard():
 while True:
  rss=int(next(x.split()[1] for x in Path('/proc/self/status').read_text().splitlines() if x.startswith('VmRSS:')))
  if rss>8*1024**2:os._exit(137)
  time.sleep(.1)
threading.Thread(target=guard,daemon=True).start()
import torch
from periodic_model import PeriodicVelocity
p=argparse.ArgumentParser()
for k in ['checkpoint','tuned','previous','output']:p.add_argument('--'+k,required=True)
a=p.parse_args();torch.set_num_threads(1);torch.set_num_interop_threads(1)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert sha(a.checkpoint)=='a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea'
assert sha(a.tuned)=='71be3106af24277b3f78882b4ddf849fc748be9075055b627da24ec0e93c13e1'
ck=torch.load(a.checkpoint,map_location='cpu',weights_only=True);tuned=torch.load(a.tuned,map_location='cpu',weights_only=True)
student=PeriodicVelocity(two_time=True,width=tuned['map_width']).double();student.load_state_dict(tuned['flowmap']);student.requires_grad_(False)
base=PeriodicVelocity(width=ck['drift_width']).double();base.load_state_dict(ck['drift']);base.requires_grad_(False)
assert all(torch.equal(v,ck['drift'][k]) for k,v in tuned['drift'].items())
r=dict(status='pure_checkpoint_cached_target_analysis_passed',physical_evaluations=0,optimizer_steps=0,checkpoint_writes=0,checkpoints=dict(pretrained_sha256=sha(a.checkpoint),tuned_sha256=sha(a.tuned)),metadata={k:tuned[k] for k in ['updates_completed','reward_scale','beta','adam_lr','implementation_validation_only','source_checkpoint_sha256']},student_change_from_pretrained_l2=float(sum((v-ck['flowmap'][k]).square().sum() for k,v in tuned['flowmap'].items()).sqrt()),base_unchanged=True,cases=[])
for c in json.loads(Path(a.previous).read_text())['cases']:
 x=torch.tensor(c['x'],dtype=torch.float64);t=torch.tensor(c['t'],dtype=torch.float64)
 with torch.no_grad():control=student(x,t,t)-base(x,t)
 old=torch.tensor(c['old_full_gradient'],dtype=torch.float64);new=torch.tensor(c['full_gradient'],dtype=torch.float64)
 r['cases'].append(dict(name=c['name'],raw_reward_shift_nat=c['old_to_new_raw_value'],full_target_delta_l2=float((new-old).norm()),full_target_relative_delta_l2=float((new-old).norm()/old.norm()),student_diagonal_loss_old_cached_target=float((control-old).square().sum()),student_diagonal_loss_q8_cached_target=float((control-new).square().sum()),scope='two cached targets only, not training trajectory or posterior quality'))
Path(a.output).write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r,indent=2))
