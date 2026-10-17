"""The native FD conditioning operator preserves independently labelled bases."""
import lal
import numpy as np
import pytest

from RIFT import lalsimutils as lsu


def parameters():
    p = lsu.ChooseWaveformParams()
    p.m1, p.m2 = 60 * lal.MSUN_SI, 30 * lal.MSUN_SI
    p.s1z, p.s2z = .3, -.2
    p.fmin = p.fref = 8.
    p.deltaT, p.deltaF = 1 / 64., 1 / 8.
    return p


def frequency_series(values, p, epoch=-.123456789):
    h = lal.CreateCOMPLEX16FrequencySeries("native component", lal.LIGOTimeGPS(epoch),
            0., p.deltaF, lal.SecondUnit, len(values))
    h.data.data[:] = values
    return h


def components(p):
    n = int(1 / (p.deltaT * p.deltaF))
    f = p.deltaF * (np.arange(n + 1) - n // 2)
    a = np.exp(-((f - 17) / 4)**2) * np.exp(.03j * f*f)
    b = .4 * np.exp(-((f + 12) / 7)**2) * np.exp(-.07j * f)
    return a, b, f


def test_unconditioned_transform_has_explicit_native_sign_and_nyquist_policy():
    p = parameters()
    a, _, _ = components(p)
    # Both Nyquist endpoints are removed together on the N+1 -> N resize.
    a[0] = a[-1] = 12 + 3j
    actual = lsu.condition_fd_mode_components(
        p, {"A+": frequency_series(a, p)}, global_sign=1,
        no_condition=True, fd_centering_factor=1.)["A+"]
    raw = a[:-1].copy()
    raw[0] = 0.
    expected = np.fft.ifft(np.fft.ifftshift(raw)) * len(raw) * p.deltaF
    np.testing.assert_allclose(actual.data.data, expected, atol=1e-13, rtol=1e-13)
    assert actual.epoch == lal.LIGOTimeGPS(-8.)
    assert actual.deltaT == p.deltaT
    negative = lsu.condition_fd_mode_components(
        p, {"A+": frequency_series(a, p)}, global_sign=-1,
        no_condition=True, fd_centering_factor=1.)["A+"]
    np.testing.assert_array_equal(negative.data.data, -actual.data.data)


def test_shared_operator_commutes_with_arbitrary_complex_angular_weights():
    p = parameters()
    a, b, _ = components(p)
    alpha, beta = .2 + 1.1j, -.7 + .3j
    kwargs = dict(global_sign=1, fd_standoff_factor=.9, fd_centering_factor=.8)
    keys = [(2, 1, ("S", 1, 1)), (2, 2, ("A", 2, -1))]
    output = lsu.condition_fd_mode_components(
        p, {keys[0]: frequency_series(a, p), keys[1]: frequency_series(b, p)}, **kwargs)
    expected = lsu.condition_fd_mode_components(
        p, {"sum": frequency_series(alpha*a + beta*b, p)}, **kwargs)["sum"]
    actual = alpha*output[keys[0]].data.data + beta*output[keys[1]].data.data
    np.testing.assert_allclose(actual, expected.data.data, atol=1e-13, rtol=1e-12)
    assert all(h.epoch == expected.epoch and h.deltaT == expected.deltaT
               and h.data.length == expected.data.length for h in output.values())


def test_positive_negative_support_must_be_split_before_td_tapers():
    p = parameters()
    _, _, f = components(p)
    # Steady sinusoids deliberately reach both time-window edges, making the
    # forbidden post-conditioning support split a numerically visible change.
    raw = np.zeros(len(f), dtype=complex)
    raw[f == 12.] = 1. + .3j
    raw[f == -13.] = -.6 + .2j
    positive, negative = raw.copy(), raw.copy()
    positive[f <= 0] = 0.
    negative[f > 0] = 0.
    kwargs = dict(global_sign=1, fd_standoff_factor=.9, fd_centering_factor=.8)
    output = lsu.condition_fd_mode_components(
        p, {"A+": frequency_series(positive, p), "A-": frequency_series(negative, p)}, **kwargs)
    full = lsu.condition_fd_mode_components(
        p, {"full": frequency_series(raw, p)}, **kwargs)["full"]
    np.testing.assert_allclose(output["A+"].data.data + output["A-"].data.data,
                               full.data.data, atol=1e-13, rtol=1e-12)
    # The conditioned + component acquires -f support from the TD window.
    # Splitting the conditioned spectrum afterwards would destroy that part.
    transformed = lsu.DataFourier(output["A+"])
    frequencies = lsu.evaluate_fvals(transformed, lal_convention=True)
    assert (np.linalg.norm(transformed.data.data[frequencies < 0])
            / np.linalg.norm(transformed.data.data)) > 1e-5
    assert not np.allclose(output["A+"].data.data,
                            output["A-"].data.data.conj(), atol=1e-10, rtol=1e-10)


def test_copying_and_component_order_leave_source_state_unchanged():
    p = parameters()
    a, b, _ = components(p)
    raw = {"S": frequency_series(a, p), "A": frequency_series(b, p)}
    before = {k: (h.data.data.copy(), str(h.epoch), h.data.length)
              for k, h in raw.items()}
    state = (p.fmin, p.fref, p.deltaT, p.deltaF)
    first = lsu.condition_fd_mode_components(p, raw, global_sign=1)
    second = lsu.condition_fd_mode_components(p, dict(reversed(list(raw.items()))), global_sign=1)
    assert (p.fmin, p.fref, p.deltaT, p.deltaF) == state
    for key, h in raw.items():
        np.testing.assert_array_equal(h.data.data, before[key][0])
        assert (str(h.epoch), h.data.length) == before[key][1:]
        np.testing.assert_array_equal(first[key].data.data, second[key].data.data)


@pytest.mark.parametrize("field", ["deltaF", "f0", "epoch", "length"])
def test_inconsistent_component_grids_fail_closed(field):
    p = parameters()
    a, b, _ = components(p)
    bad = frequency_series(b[:-1] if field == "length" else b, p)
    if field == "deltaF":
        bad.deltaF *= 2
    elif field == "f0":
        bad.f0 = 1.
    elif field == "epoch":
        bad.epoch = lal.LIGOTimeGPS(0.)
    with pytest.raises(ValueError, match="share one native frequency grid"):
        lsu.condition_fd_mode_components(
            p, {"valid": frequency_series(a, p), "bad": bad}, global_sign=1)


def test_global_sign_is_required_and_cannot_be_a_fitted_phase():
    p = parameters()
    a, _, _ = components(p)
    raw = {"S": frequency_series(a, p)}
    with pytest.raises(TypeError, match="global_sign"):
        lsu.condition_fd_mode_components(p, raw)
    with pytest.raises(ValueError, match="explicitly"):
        lsu.condition_fd_mode_components(p, raw, global_sign=np.exp(.1j))
