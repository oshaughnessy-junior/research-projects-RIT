"""Early-time multirate precompute by data-side weighting (DSWc). Opt-in; nothing calls it by default.

Design, evidence and acceptance tests: DESIGN_early_time_multirate.md (this directory).

Conventions follow ComputeModeIPTimeSeries (factored_likelihood.py): lal FFTs carry dt and df, and
two-sided frequency arrays are in centred order.  With dbar = IFFT(d~ * weights2side), the overlap
time series that function returns is

    Q(n dt) = 2 dt sum_s dbar[s] conj(h[(s - n) mod N]),

so the data-side weighting can be applied once per job and the template enters raw.
"""
import numpy as np
import lal

import RIFT.lalsimutils as lsu

__all__ = ["data_side_weighted", "overlap_series_dsw", "ComputeModeIPTimeSeriesDSW"]


def data_side_weighted(data, psd, fmin, fMax, fNyq, analyticPSD_Q=False, inv_spec_trunc_Q=False, T_spec=0.):
    """dbar(t) = IFFT(d~ * weights2side): the data weighted by RIFT's own inner-product weights.

    `data` is the two-sided COMPLEX16FrequencySeries ILE loads; the weights are those of the
    ComplexOverlap that ComputeModeIPTimeSeries builds, so truncation settings carry over.
    Returns a complex numpy array of length N on the data's time grid (starts at data.epoch).
    """
    IP = lsu.ComplexOverlap(fmin, fMax, fNyq, data.deltaF, psd, analyticPSD_Q, inv_spec_trunc_Q, T_spec,
                            full_output=True)
    assert data.data.length == IP.len2side
    w = lal.CreateCOMPLEX16FrequencySeries("dbar(f)", data.epoch, data.f0, data.deltaF,
                                           lsu.lsu_HertzUnit, data.data.length)
    w.data.data = data.data.data * IP.weights2side
    return np.array(lsu.DataInverseFourier(w).data.data)


def overlap_series_dsw(dbar, h, dt, lags):
    """2 dt sum_s dbar[s] conj(h[(s - n) mod N]) at integer lags n (any sign), circular as in RIFT."""
    N = len(dbar)
    assert len(h) == N
    c = np.fft.ifft(np.fft.fft(dbar) * np.conj(np.fft.fft(h)))
    return 2. * dt * c[np.asarray(lags) % N]


def ComputeModeIPTimeSeriesDSW(hlms, dbar, data_epoch, deltaT, N_shift, N_window):
    """One-segment, full-rate counterpart of ComputeModeIPTimeSeries from precomputed dbar.

    Returns {mode: COMPLEX16TimeSeries} with the same samples and epoch as ComputeModeIPTimeSeries
    (acceptance test 1). `hlms` are the two-sided FD modes PrecomputeLikelihoodTerms passes.
    """
    rholms = {}
    lags = N_shift + np.arange(N_window)
    for pair, hf in hlms.items():
        h = np.array(lsu.DataInverseFourier(hf).data.data)
        q = overlap_series_dsw(dbar, h, deltaT, lags)
        ts = lal.CreateCOMPLEX16TimeSeries("rho", data_epoch - hf.epoch, 0., deltaT,
                                           lsu.lsu_DimensionlessUnit, N_window)
        ts.epoch += N_shift * deltaT
        ts.data.data = q
        rholms[pair] = ts
    return rholms
