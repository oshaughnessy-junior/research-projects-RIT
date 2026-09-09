# Simultaneous slow rotation and finite-arm response

## Scope

`integrate_likelihood_extrinsic_batchmode --rotation-slow --freqresponse` now selects
one compound response model. It is intended for long, loud BNS-like signals, including
strongly precessing systems and higher modes. The response operators act on each full
inertial-frame mode, so the implementation makes no per-mode stationary-phase or
time-frequency-track approximation.

This is not the economical path for short eccentric BBH mergers. Use `--freqresponse`
alone there unless Earth rotation is independently relevant.

## Factorization

The finite-arm response already has the form

```text
F(f,t) = sum_b beta_b(t) W_b(f).
```

`beta_0` is the exact LAL long-wavelength response and `beta_(1+q)` contains an
arm-projection polynomial of order `q`. Under Earth rotation these coefficients have
finite sidereal half-widths 2 and `q+2`, respectively. A small exact DFT recovers their
Fourier coefficients. Composing the slow-delay expansion gives elementary templates

```text
chi_(b,p,n) = M_n d_t^p [W_b h_lm].
```

The coefficient half-width is `width(beta_b)+p`. Conjugation reflects only the sidereal
index, `(b,p,n) -> (b,p,-n)`, because each `W_b` is Hermitian.

## Cost and controls

The number of compound elements is

```text
sum_b sum_(p=0)^pmax [2 (width(beta_b)+p) + 1].
```

At the current defaults (`Qmax=4`, `pmax=0`) this is 50 elements and 2500 ordered U/V
pairs per detector. At `pmax=1` it is 112 elements and 12544 pairs. The driver prints
both counts before integration. Start with `--rotation-p-max 0` and the smallest
`--freqresponse-qmax` justified by a response-convergence check.

## JAX status

At the time this conventional implementation was added, the JAX library had separately
tagged banded implementations for `rotation` and `freqresponse`, but no compound tag or
`(b,p,n)` coefficient/reflection policy. The production driver also treated both command
line flags as compatibility options. See the follow-on JAX implementation and profiling
changes on this branch for the current status.

## Validation

`test_slowrot_rotating_freqresponse.py` checks the exact compound basis roster, sidereal
reconstruction of every finite-response coefficient, delay-order band support, and the
full precompute/NoLoop reduction to the existing finite-response likelihood at zero
sidereal rate, plus the Cauchy--Schwarz bound at zero and physical sidereal rates. The
existing `test_slowrot_noloop.py` remains the regression for the shared rotation
contraction.
