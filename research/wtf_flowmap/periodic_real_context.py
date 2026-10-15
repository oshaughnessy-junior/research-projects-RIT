"""Pinned repaired model, data and full-mode bank setup; no likelihood dispatch.

Extracted from parity_periodic.py at06a6c67e. It retains exact source/hash and
coordinate checks; callbacks and their physical-row ledger are caller-owned.
"""
import os,json,hashlib,math
from pathlib import Path
def load_context(args,r,save):
    import numpy as np
    import torch
    import jax
    import jax.numpy as jnp
    from igwn_ligolw import utils,ligolw,lsctables
    from periodic_model import PeriodicVelocity,base_noise,decode,support_mask
    from periodic_target_bank import build_bank
    import RIFT.likelihood.factored_likelihood as fl
    from RIFT.likelihood.jax_ile import wrapper,core
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    assert sha(args.checkpoint)=='a63d634bf014ed43c3c46be3318f2ad3459e9c943975320a3960a79fc73ef4ea'
    expected=[(fl.__file__,'7b87a19e7133155cb1bb67a790a58ab7204f605d6d12cd78471edc34e044c974'),
        (wrapper.__file__,'0db282efa8db272e404cc2d649485feaba3c0339998a36ddde293b7ac4166005'),
        (core.__file__,'0f499791e796558b7b2460027ef8459302925dcf792801883664ceaf56336ab8')]
    for path,digest in expected:assert sha(path)==digest,'unreviewed likelihood source change'
    r['likelihood_sources']={path:sha(path) for path,_ in expected}
    driver=Path(fl.__file__).parents[2]/'bin/integrate_likelihood_extrinsic_batchmode'
    driver_text=driver.read_text()
    assert 'get_astropy_cosmology("Planck15")' in driver_text
    assert 'psi_prior_pdf = mcsampler.ret_uniform_samp_vector_alt(psi_prior_range[0], psi_prior_range[1])' in driver_text
    assert 'prior_pdf = mcsampler.ret_uniform_samp_vector_alt(phi_orb_prior_range[0], phi_orb_prior_range[1])' in driver_text
    r['current_normalized_driver_sha256']=sha(driver)
    inventory=json.loads(Path(args.inventory).read_text())
    for entry in inventory['inputs']:
        if entry['path'].endswith(('.gwf','-psd.xml.gz','single-intrinsic.xml.gz')):
            assert sha(entry['path'])==entry['sha256']
    data_record=Path(args.training)/'result.json'
    assert sha(data_record)=='e809627592936cd16e2e55fe9abece6ea476d25323d550f47b91162b097166a9'
    provenance=json.loads(data_record.read_text())['data']
    theta=[]
    for entry in provenance['source_archives']:
        path=Path(args.training)/f"input-{entry['seed']}.xml.gz"
        assert sha(path)==entry['xml_sha256']
        doc=utils.load_filename(str(path),contenthandler=ligolw.LIGOLWContentHandler)
        q=lsctables.SimInspiralTable.get_table(doc)[0]
        theta.append([q.longitude,q.latitude,q.polarization%math.pi,q.inclination,q.coa_phase,q.distance])
    theta=np.asarray(theta);ck=torch.load(args.checkpoint,map_location='cpu',weights_only=True)
    assert ck['geometry']=='T3_R3_v1' and ck['gates_passed']
    model=PeriodicVelocity(two_time=True,width=ck['map_width']).double();model.load_state_dict(ck['flowmap']);model.requires_grad_(False)
    center=torch.tensor(ck['center'],dtype=torch.float64);scale=torch.tensor(ck['scale'],dtype=torch.float64)
    def encode(q):
        raw=np.array([np.arctanh(np.sin(q[1])),np.arctanh(np.cos(q[3])),np.log(q[5])])
        return np.r_[q[0],q[4],2*q[2],(raw-center.numpy())/scale.numpy()]
    states=np.asarray([encode(q) for q in theta])
    def torch_decode(x):return decode(x,center,scale)
    def jax_decode(x):
        raw=x[3:]*jnp.asarray(scale.numpy())+jnp.asarray(center.numpy())
        return jnp.stack([x[0],jnp.arcsin(jnp.tanh(raw[0])),x[2]/2,
            jnp.arccos(jnp.tanh(raw[1])),x[1],jnp.exp(raw[2])])
    for i in [0,3]:
        x=torch.tensor(states[i],dtype=torch.float64)
        np.testing.assert_allclose(torch_decode(x).numpy(),theta[i],rtol=1e-12,atol=1e-10)
        jt=torch.autograd.functional.jacobian(torch_decode,x).numpy()
        jj=np.asarray(jax.jacfwd(jax_decode)(states[i]));np.testing.assert_allclose(jt,jj,atol=1e-10,rtol=1e-10)
        h=1e-6
        for d in range(6):
            e=np.eye(6)[d]*h
            fd=(torch_decode(x+torch.tensor(e)).numpy()-torch_decode(x-torch.tensor(e)).numpy())/(2*h)
            np.testing.assert_allclose(fd,jt[:,d],atol=1e-5,rtol=1e-6)
        assert jt[2,2]==.5 and jt[3,4]<0 and np.isfinite(jt).all()
    r['coordinate_contracts']='two-row roundtrip, Torch/JAX/full FD Jacobian passed'
    r['normalization_convention']=dict(reward='physical log time-integral API only, no prior or Jacobian added',
        angular_prior='unit mass on RA[0,2pi),phase[0,2pi),psi[0,pi)',distance_prior='current normalized Planck15 cosmo_sourceframe on [1,10000]',
        historical_angular_prior_mass=8,historical_lnZ_offset=math.log(8),likelihood_is_not_evidence=True)
    r['runtime']=dict(host=os.uname().nodename,affinity=sorted(os.sched_getaffinity(0)),pid=os.getpid(),
        numpy=np.__version__,torch=torch.__version__,jax=jax.__version__)
    bank=build_bank(args,r,save);like=bank['like'];reference=bank['reference'];save()
    return dict(checkpoint=ck,model=model,center=center,scale=scale,bank=bank,decode=torch_decode,theta=theta)
