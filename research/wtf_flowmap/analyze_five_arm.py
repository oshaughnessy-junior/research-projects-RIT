"""Read-only diagnostics; no target dispatch and no posterior-truth assertion."""
import argparse,json,time,resource,math
from pathlib import Path
import numpy as np
from scipy.special import logsumexp
from igwn_ligolw import utils,ligolw,lsctables


def physical(x):
    return np.column_stack((2*np.pi*x[:,0],np.arcsin(2*x[:,1]-1),np.pi*x[:,2],
        np.arccos(2*x[:,3]-1),2*np.pi*x[:,4],1+9999*x[:,5]))
def values(q,tail):
    D=q[:,5];ci=np.cos(q[:,3]);v={'mean_distance':D,'second_distance_moment':D*D,
        'mean_sindec':np.sin(q[:,1]),'mean_cosi':ci,'positive_cosi_mass':ci>0,
        'distance_tail_mass':(D<tail[0])|(D>tail[1]),'polar_tail_mass':np.abs(ci)>.9}
    for i,name,period in [(0,'ra',2*np.pi),(2,'psi',np.pi),(4,'phase',2*np.pi)]:
        a=2*np.pi*q[:,i]/period;v[name+'_cos']=np.cos(a);v[name+'_sin']=np.sin(a)
    for phase in range(2):
        for psi in range(2):v[f'phase_psi_quadrant_{phase}_{psi}']=((q[:,4]%(2*np.pi))>=np.pi)==bool(phase)
    # Replace each marginal phase predicate by the intended joint quadrant.
    for phase in range(2):
        for psi in range(2):v[f'phase_psi_quadrant_{phase}_{psi}'] &= (((q[:,2]%np.pi)>=np.pi/2)==bool(psi))
    return {k:np.asarray(a,dtype=float) for k,a in v.items()}
def estimate(lw):
    z=logsumexp(lw);w=np.exp(lw-z);scaled=np.exp(lw-np.max(lw))
    return dict(lnZ=float(z-np.log(len(lw))),relative_se=float(scaled.std(ddof=1)/math.sqrt(len(lw))/scaled.mean()),
        ess=float(1/(w@w)),max_weight=float(w.max()))

def main(a):
    start=time.perf_counter();cpu=time.process_time();out=Path(a.output)
    r=json.loads((out/'summary.json').read_text());meta=json.loads((Path(a.training)/'result.json').read_text())['data']
    rows=[]
    for entry in meta['source_archives']:
        doc=utils.load_filename(str(Path(a.training)/f"input-{entry['seed']}.xml.gz"),contenthandler=ligolw.LIGOLWContentHandler)
        rows.append(np.array([[q.longitude,q.latitude,q.polarization,q.inclination,q.coa_phase,q.distance] for q in lsctables.SimInspiralTable.get_table(doc)]))
    train=np.vstack(rows[:3]);held=rows[3];tail=np.quantile(train[:,5],[.05,.95]);hv=values(held,tail)
    result=dict(interpretation='single seed, single training anchor; held-out finite-ESS fairdraw cache is a diagnostic reference, not exact posterior truth',
        absolute_evidence_error=None,true_mode_mass_error=None,
        uncertainty='nominal conditional-importance sample variance and SNIS standard errors; adaptive dependence and teacher correlation are not fully calibrated',
        time_prior_conversion='lnZ_uniform_closed_time = lnI_API - log(613/4096); API integral is the primary target',arms={})
    for name,ar in r['arms'].items():
        if ar['status']!='complete':result['arms'][name]=dict(status=ar['status'],error=ar.get('error'));continue
        d=np.load(out/(name+'_evidence.npz'));lw=d['lw'];w=d['weights'];q=physical(d['x']);metrics={}
        for k,v in values(q,tail).items():
            mean=float(w@v);ref=float(hv[k].mean())
            metrics[k]=dict(value=mean,nominal_snis_se=float(np.sqrt(np.sum(w*w*(v-mean)**2))),
                heldout_reference=ref,heldout_naive_sem=float(hv[k].std(ddof=1)/np.sqrt(len(held))),
                diagnostic_discrepancy=mean-ref)
        full=estimate(lw);first=estimate(lw[:500]);second=estimate(lw[500:]);delta=abs(first['lnZ']-second['lnZ'])
        split_bound=max(.2,3*math.hypot(first['relative_se'],second['relative_se']))
        converged=bool(full['ess']>=100 and full['max_weight']<=.05 and full['relative_se']<=.1 and delta<=split_bound)
        # Explicit equal-share amortization of common setup and earlier failed gates.
        # GNU time totals for gate1/run1/run2: 3.30 +5.99 +14.42 CPU s;
        # wall totals: 4.80 +9.95 +16.99 s. This is a scenario allocation,
        # not five separately measured cold jobs.
        sharecpu=(r['shared_setup_cpu_seconds']+3.30+5.99+14.42)/5
        sharewall=(r['shared_setup_wall_seconds']+4.80+9.95+16.99)/5
        result['arms'][name]=dict(status='complete',**full,metrics=metrics,first_half=first,second_half=second,
            split_lnZ_difference=delta,split_bound=split_bound,predeclared_convergence=converged,
            total_attempts=ar['attempts'],physical_rows=ar['physical_rows'],phases=ar['phases'],
            neural_forward_rows=100 if name in ['pretrained','tuned'] else 0,neural_gradient_rows=0,
            cold_ess_per_true_row_upper_bound=full['ess']/(15984560+ar['attempts']) if name in ['cache','pretrained','tuned'] else None,
            arm_cpu_seconds=ar['cpu_seconds'],arm_wall_seconds=ar['wall_seconds'],
            shared_setup_cpu_share=sharecpu,shared_setup_wall_share=sharewall,
            ess_per_total_attempt=full['ess']/ar['attempts'],
            ess_per_reuse_cpu_second=full['ess']/(ar['cpu_seconds']+sharecpu),
            ess_per_reuse_wall_second=full['ess']/(ar['wall_seconds']+sharewall),
            lnZ_uniform_closed_time=full['lnZ']-math.log(613/4096))
    result['analysis_cpu_seconds']=time.process_time()-cpu;result['analysis_wall_seconds']=time.perf_counter()-start
    result['max_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    (out/'diagnostics.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps({k:{n:v[n] for n in ['lnZ','ess','relative_se','predeclared_convergence'] if n in v} for k,v in result['arms'].items()},indent=2))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--training',required=True);main(p.parse_args())
