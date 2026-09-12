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


_TOP_LEVEL_TIMING_STAGES = {
    'waveform', 'input_prep', 'basis', 'Q_U', 'V',
    'device_export', 'host_export',
}


def timing_recorder(records, intrinsic, algorithm):
    """Return a callback that retains non-overlapping stage wall times."""
    def callback(stage, elapsed, details):
        elapsed = float(elapsed)
        if stage in _TOP_LEVEL_TIMING_STAGES:
            records[stage] = records.get(stage, 0.0) + elapsed
        emit(stage=stage, seconds=elapsed, intrinsic=intrinsic,
             algorithm=algorithm, **details)
    return callback


def host_legacy_pack(packed, meta, xp, require_gpu):
    """Pack first, then copy device arrays only for the post-timing oracle."""
    lookup, rho, U, V, epoch = gpu.pack_device_precompute(
        packed, meta, require_gpu=require_gpu)

    def host(value):
        return np.asarray(value) if xp is np else xp.asnumpy(value)

    return (lookup,
            {det: {a: host(value) for a, value in rows.items()}
             for det, rows in rho.items()},
            {det: host(value) for det, value in U.items()},
            {det: host(value) for det, value in V.items()},
            epoch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--intrinsics', type=int, default=5)
    parser.add_argument('--detectors', default='H1,L1,V1')
    parser.add_argument('--qmax', type=int, default=1)
    parser.add_argument('--pmax', type=int, default=1)
    parser.add_argument('--lmax', type=int, default=2)
    parser.add_argument('--approximant', default='IMRPhenomD')
    parser.add_argument('--delta-t', type=float, default=1/1024.)
    parser.add_argument('--delta-f', type=float, default=0.5)
    parser.add_argument('--fmin', type=float, default=30.)
    parser.add_argument('--fref', type=float, default=100.)
    parser.add_argument('--fmax', type=float, default=512.)
    parser.add_argument('--t-window', type=float, default=0.15)
    parser.add_argument('--mass1', type=float, default=30.)
    parser.add_argument('--mass2', type=float, default=25.)
    parser.add_argument('--spin1x', type=float, default=0.)
    parser.add_argument('--spin1y', type=float, default=0.)
    parser.add_argument('--spin1z', type=float, default=0.)
    parser.add_argument('--spin2x', type=float, default=0.)
    parser.add_argument('--spin2y', type=float, default=0.)
    parser.add_argument('--spin2z', type=float, default=0.)
    parser.add_argument('--arm-length', type=float, default=40000.)
    parser.add_argument('--backend', choices=['cupy','numpy'], default='cupy',
                        help='numpy is harness validation only, not GPU performance')
    parser.add_argument('--compare-numpy', action='store_true',
                        help='also time the batched NumPy algorithm on this same worker')
    args = parser.parse_args()
    if args.intrinsics < 1 or args.lmax < 2 or args.qmax < 0 or args.pmax < 0:
        parser.error('intrinsics must be positive; lmax>=2 and qmax,pmax>=0')
    if min(args.delta_t, args.delta_f, args.fmin, args.fref,
           args.fmax, args.t_window, args.mass1, args.mass2,
           args.arm_length) <= 0:
        parser.error('waveform grid, frequencies, masses, window, and arm length must be positive')
    if args.fmax > 0.5/args.delta_t:
        parser.error('fmax exceeds the Nyquist frequency implied by delta-t')
    if args.backend == 'cupy':
        import cupy as xp
    else:
        xp = np
    started = time.perf_counter()
    detectors = [det.strip() for det in args.detectors.split(',') if det.strip()]
    if not detectors:
        parser.error('at least one detector is required')
    try:
        approximant = lalsim.GetApproximantFromString(args.approximant)
    except Exception as exc:
        parser.error('unknown approximant %r: %s' % (args.approximant, exc))
    P0 = lsu.ChooseWaveformParams(
        m1=args.mass1*lal.MSUN_SI, m2=args.mass2*lal.MSUN_SI,
        s1x=args.spin1x, s1y=args.spin1y, s1z=args.spin1z,
        s2x=args.spin2x, s2y=args.spin2y, s2z=args.spin2z,
        fmin=args.fmin, fref=args.fref,
        deltaT=args.delta_t, deltaF=args.delta_f, approx=approximant,
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
    # Injection angles belong only to the synthetic detector data.  The mode
    # bank follows the ILE convention and carries no extrinsic angles; this is
    # essential for XPHM as well as aligned-spin approximants.
    P_template = P0.manual_copy()
    P_template.phiref = P_template.psi = P_template.incl = 0.0
    sync(xp)
    context = gpu.GPUPrecomputeContext(xp)
    numpy_context = gpu.GPUPrecomputeContext(np) if args.compare_numpy else None
    emit(stage='worker_setup', seconds=time.perf_counter()-started,
         bins=n, masses_msun=[args.mass1,args.mass2],
         spins=[[args.spin1x,args.spin1y,args.spin1z],
                [args.spin2x,args.spin2y,args.spin2z]],
         approximant=args.approximant, lmax=args.lmax,
         delta_t=P0.deltaT, delta_f=P0.deltaF, fmin=args.fmin,
         fmax=args.fmax, t_window=args.t_window,
         qmax=args.qmax, pmax=args.pmax,
         backend=args.backend,
         gpu=None if xp is np else xp.cuda.runtime.getDeviceProperties(0)['name'].decode())
    for index in range(args.intrinsics):
        P = P_template.manual_copy()
        P.m1 += index*0.01*lal.MSUN_SI
        common = dict(event_time_geo=1e9, t_window=args.t_window, P=P, data_dict=data,
                      psd_dict=psds, Lmax=args.lmax, fMax=args.fmax, Qmax=args.qmax,
                      p_max=args.pmax, L_arm=args.arm_length, skip_interpolation=True,
                      quiet=True, verbose=False)
        t0 = time.perf_counter()
        cpu = fr.PrecomputeLikelihoodTermsRotatingFreqResponse(**common)
        emit(stage='cpu_precompute', intrinsic=index, seconds=time.perf_counter()-t0,
             timing_role='numerical_reference_only')
        actual_modes = list(cpu[4]['modes'])
        actual_a = list(cpu[4]['a_list'])
        mode_count, a_count = len(actual_modes), len(actual_a)
        q_bytes = sum(
            np.asarray(cpu[3][det][a][mode].data.data).nbytes
            for det in detectors for a in actual_a for mode in actual_modes)
        basis_bytes_per_detector = (a_count*mode_count*n*
                                    np.dtype(np.complex128).itemsize)
        uv_bytes = (len(detectors)*2*a_count*a_count*mode_count*mode_count*
                    np.dtype(np.complex128).itemsize)
        emit(stage='bank_geometry', intrinsic=index, actual_mode_count=mode_count,
             actual_modes=actual_modes, exact_compound_count=a_count,
             exact_q_bytes=q_bytes, exact_uv_bytes=uv_bytes,
             exact_retained_quv_bytes=q_bytes+uv_bytes,
             primary_basis_bytes_per_detector=basis_bytes_per_detector,
             full_fft_bins=n)
        numpy_seconds = None
        if numpy_context is not None:
            numpy_timings = {}
            t0 = time.perf_counter()
            numpy_bank = gpu.PrecomputeLikelihoodTermsRotatingFreqResponseGPU(
                **common, backend=np, context=numpy_context,
                return_device=True,
                timing_callback=timing_recorder(
                    numpy_timings, index, 'batched_numpy'))
            numpy_seconds = time.perf_counter()-t0
            emit(stage='batched_numpy_precompute',intrinsic=index,
                 seconds=numpy_seconds,
                 stage_seconds=numpy_timings,
                 context_uploads=numpy_context.uploads,
                 context_hits=numpy_context.cache_hits,
                 timing_scope='host wall through resident bank return')
            # Exercise the exact classic handoff contract after the timed region.
            host_legacy_pack(numpy_bank[0], numpy_bank[1], np, require_gpu=False)
            del numpy_bank
        sync(xp)
        candidate_timings = {}
        t0 = time.perf_counter()
        bank = gpu.PrecomputeLikelihoodTermsRotatingFreqResponseGPU(
            **common, context=context, backend=xp,
            return_device=True,
            timing_callback=timing_recorder(candidate_timings, index, args.backend))
        sync(xp)
        candidate_seconds = time.perf_counter()-t0
        emit(stage='candidate_precompute',backend=args.backend, intrinsic=index,
             seconds=candidate_seconds, stage_seconds=candidate_timings,
             context_uploads=context.uploads, context_hits=context.cache_hits,
             context_entries=len(context._arrays),
             device_pool_used_bytes=0 if xp is np else xp.get_default_memory_pool().used_bytes(),
             timing_scope='host wall through resident bank return')
        if numpy_seconds is not None and xp is not np:
            emit(stage='batched_algorithm_comparison', intrinsic=index,
                 numpy_seconds=numpy_seconds, gpu_seconds=candidate_seconds,
                 end_to_end_speedup=numpy_seconds/candidate_seconds,
                 numpy_stage_seconds=numpy_timings,
                 gpu_stage_seconds=candidate_timings,
                 timing_scope=('same-process host wall through resident bank return; '
                               'container staging, scheduler queue, and later oracle copies excluded'))
        cpu_pack = fr.pack_rotating_freqresponse_arrays(cpu[4], cpu[3], cpu[1], cpu[2])
        # This is deliberately after candidate_seconds: production hands these
        # buffers directly to JAX/classic GPU ILE.  Host copies exist only so
        # the small numerical oracle below can use its NumPy implementation.
        got_pack = host_legacy_pack(
            bank[0], bank[1], xp, require_gpu=(xp is not np))
        errors = dict(Q=0., U=0., V=0.)
        for det in detectors:
            for ai in bank[1]['a_list']:
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
        tvals = np.array([-P0.deltaT,0.,P0.deltaT])
        cpu_lnl = fr.DiscreteFactoredLogLikelihoodRotatingFreqResponseNoLoop(
            tvals,Pv,cpu[4],*cpu_pack,Lmax=args.lmax,time_interp='nearest',xpy=np,array_output=True)
        got_lnl = fr.DiscreteFactoredLogLikelihoodRotatingFreqResponseNoLoop(
            tvals,Pv,bank[1],*got_pack,Lmax=args.lmax,time_interp='nearest',xpy=np,array_output=True)
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
