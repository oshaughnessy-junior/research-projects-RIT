"""hlmoft_IMRPv2_dict must not inherit garbage from its uninitialized mode buffers (#488).

lal.CreateCOMPLEX16TimeSeries does not zero memory.  A NaN left there survived the old
`data.data *= 0` clear and produced an intermittent non-finite (2,-2) mode.  The test fills
every new buffer with NaN and requires the same, finite modes as an unpoisoned call.
"""
import types

import lal
import lalsimulation as lalsim
import numpy as np

from RIFT import lalsimutils as lsu


def _params():
    P = lsu.ChooseWaveformParams()
    P.m1, P.m2 = 35 * lal.MSUN_SI, 30 * lal.MSUN_SI
    P.s1x, P.s1z, P.s2z = 0.3, 0.1, -0.2
    P.fmin = P.fref = 30.0
    P.deltaT = 1.0 / 2048
    P.approx = lalsim.IMRPhenomPv2
    return P


def _modes():
    return {k: np.array(v.data.data) for k, v in lsu.hlmoft_IMRPv2_dict(_params()).items()}


def test_poisoned_buffers_give_clean_modes(monkeypatch):
    clean = _modes()
    assert all(np.all(np.isfinite(v)) for v in clean.values())

    def poisoned_create(*args):
        ts = lal.CreateCOMPLEX16TimeSeries(*args)
        ts.data.data = np.full(ts.data.length, np.nan, dtype=complex)
        return ts

    proxy = types.SimpleNamespace(**vars(lal))
    proxy.CreateCOMPLEX16TimeSeries = poisoned_create
    monkeypatch.setattr(lsu, "lal", proxy)
    poisoned = _modes()

    assert sorted(poisoned) == sorted(clean)
    for k in clean:
        np.testing.assert_array_equal(poisoned[k], clean[k], err_msg=str(k))
