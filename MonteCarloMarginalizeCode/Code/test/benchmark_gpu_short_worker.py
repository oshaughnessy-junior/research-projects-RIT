#!/usr/bin/env python3
"""Short physical BBH, matched CPU/GPU precompute and repeated worker timings.

No BNS input is opened. Synthetic signal/noise products exist only in memory.
This tests precompute and point likelihoods, not posterior convergence.
"""
import argparse
import json
import time

import numpy as np
import lal
import lalsimulation as lalsim
import RIFT.lalsimutils as lsu
from RIFT.likelihood import gpu_precompute as gpu
from RIFT.likelihood import factored_likelihood_rotating_freqresponse as fr


def emit(**record):
    print(json.dumps(record, sort_keys=True), flush=True)


def sync(xp):
    if xp is not np:
        xp.cuda.Stream.null.synchronize()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--intrinsics', type=int, default=5)
    parser.add_argument('--detectors', default='H1,L1,V1')
    parser.add_argument('--qmax', type=int, default=1)
    parser.add_argument('--pmax', type=int, default=1)
    parser.add_argument('--backend', choices=['cupy','numpy'], default='cupy',
                        help='numpy is harness validation only, not GPU performance')
    parser.add_argument('--compare-numpy', action='store_true',
                        help='also time the batched NumPy algorithm on this same worker')
    args = parser.parse_args()
    if args.backend == 'cupy':
        import cupy as xp
    else:
        xp = np
    started = time.perf_counter()
    detectors = args.detectors.split(',')
    P0 = lsu.ChooseWaveformParams(
        m1=30*lal.MSUN_SI, m2=25*lal.MSUN_SI, fmin=30., fref=100.,
        deltaT=1/1024., deltaF=0.5, approx=lalsim.IMRPhenomD,
        radec=True, phi=1.2, theta=0.3, incl=0.7, psi=0.5, phiref=0.4,
        tref=1e9, dist=200e6*lal.PC_SI, detector=detectors[0])
    data, psds = {}, {}
    for det in detectors:
        Pd = P0.manual_copy()
        Pd.detector = det
        data[det] = lsu.non_herm_hoff(Pd)
        n = data[det].data.length
        psd = lal.CreateREAL8FrequencySeries(
            det, lal.LIGOTimeGPS(0), 0., P0.deltaF, lal.SecondUnit, n//2+1)
        psd.data.data[:] = [lalsim.SimNoisePSDaLIGOZeroDetHighPower(max(10.,f))
                           for f in np.arange(n//2+1)*P0.deltaF]
        psds[det] = psd
    sync(xp)
    context = gpu.GPUPrecomputeContext(xp)
    numpy_context = gpu.GPUPrecomputeContext(np) if args.compare_numpy else None
    emit(stage='worker_setup', seconds=time.perf_counter()-started,
         bins=n, masses_msun=[30,25], delta_f=P0.deltaF,
         backend=args.backend,
         gpu=None if xp is np else xp.cuda.runtime.getDeviceProperties(0)['name'].decode())
    for index in range(args.intrinsics):
        P = P0.manual_copy()
        P.m1 += index*0.01*lal.MSUN_SI
        common = dict(event_time_geo=1e9, t_window=0.15, P=P, data_dict=data,
                      psd_dict=psds, Lmax=2, fMax=512., Qmax=args.qmax,
                      p_max=args.pmax, L_arm=40000., skip_interpolation=True,
                      quiet=True, verbose=False)
        t0 = time.perf_counter()
        cpu = fr.PrecomputeLikelihoodTermsRotatingFreqResponse(**common)
        emit(stage='cpu_precompute', intrinsic=index, seconds=time.perf_counter()-t0)
        if numpy_context is not None:
            t0 = time.perf_counter()
            numpy_bank = gpu.PrecomputeLikelihoodTermsRotatingFreqResponseGPU(
                **common,backend=np,context=numpy_context)
            emit(stage='batched_numpy_precompute',intrinsic=index,
                 seconds=time.perf_counter()-t0)
            del numpy_bank
        sync(xp)
        t0 = time.perf_counter()
        bank = gpu.PrecomputeLikelihoodTermsRotatingFreqResponseGPU(
            **common, context=context, backend=xp,
            timing_callback=lambda stage, elapsed, details: emit(
                stage=stage, seconds=elapsed, intrinsic=index, **details))
        sync(xp)
        emit(stage='candidate_precompute',backend=args.backend, intrinsic=index, seconds=time.perf_counter()-t0,
             context_uploads=context.uploads, context_hits=context.cache_hits,
             context_entries=len(context._arrays),
             device_pool_used_bytes=0 if xp is np else xp.get_default_memory_pool().used_bytes())
        cpu_pack = fr.pack_rotating_freqresponse_arrays(cpu[4], cpu[3], cpu[1], cpu[2])
        got_pack = fr.pack_rotating_freqresponse_arrays(bank[4], bank[3], bank[1], bank[2])
        errors = dict(Q=0., U=0., V=0.)
        for det in detectors:
            for ai in bank[4]['a_list']:
                a, b = got_pack[1][det][ai], cpu_pack[1][det][ai]
                np.testing.assert_allclose(a, b, rtol=2e-9, atol=1e-8)
                errors['Q'] = max(errors['Q'],float(np.max(np.abs(a-b))))
            for name, slot in [('U',2),('V',3)]:
                a, b = got_pack[slot][det], cpu_pack[slot][det]
                np.testing.assert_allclose(a, b, rtol=2e-9, atol=1e-7)
                errors[name] = max(errors[name],float(np.max(np.abs(a-b))))
            assert got_pack[4][det] == cpu_pack[4][det]
        Pv = P0.manual_copy()
        for name in ['phi','theta','incl','psi','phiref','dist']:
            setattr(Pv,name,np.full(4,getattr(P0,name)))
        Pv.phi += np.array([0.,1e-4,-1e-4,0.01])
        tvals = np.array([-1/1024.,0.,1/1024.])
        cpu_lnl = fr.DiscreteFactoredLogLikelihoodRotatingFreqResponseNoLoop(
            tvals,Pv,cpu[4],*cpu_pack,Lmax=2,time_interp='nearest',xpy=np,array_output=True)
        got_lnl = fr.DiscreteFactoredLogLikelihoodRotatingFreqResponseNoLoop(
            tvals,Pv,bank[4],*got_pack,Lmax=2,time_interp='nearest',xpy=np,array_output=True)
        np.testing.assert_allclose(got_lnl,cpu_lnl,rtol=2e-9,atol=1e-8)
        if not np.all(np.isfinite(got_lnl)):
            raise RuntimeError('nonfinite downstream likelihood')
        emit(stage='cpu_gpu_parity',intrinsic=index,max_abs_error=errors,
             max_abs_lnl_error=float(np.max(np.abs(cpu_lnl-got_lnl))))
        del bank,cpu,cpu_pack,got_pack
    assert context.uploads == 3*len(detectors), 'detector inputs were re-uploaded'
    emit(stage='worker_total',seconds=time.perf_counter()-started,
         timing_scope='host process; container and queue excluded')


if __name__ == '__main__':
    main()
