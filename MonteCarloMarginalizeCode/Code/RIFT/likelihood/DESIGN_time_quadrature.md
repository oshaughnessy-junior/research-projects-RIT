# The time-marginalization quadrature: two rules, and which one is right

`DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop` integrates `exp(lnL(t))` over
the marginalization window.  Until now it picked the rule from the *backend*:

```python
if (xpy is np) or (optimized_gpu_tools is None):
    simps = my_simps                   # scipy.integrate.simpson
else:
    simps = optimized_gpu_tools.simps  # a vendored copy of pre-1.11 scipy simps
```

`optimized_gpu_tools.simps` is a cupy transliteration of scipy's old `simps`,
which handled an even number of samples with `even='avg'`.  scipy changed that
convention in 1.11 (the Cartwright correction; the igwn environment ships
1.13.1).  So the CPU and GPU paths run **different quadrature rules**, and the
same inputs give different `lnL`.  The JAX driver builds its weights from scipy
(`jax_ile.core._simpson_weights`), so it sits with the CPU path: three code
paths, two conventions.

Measured 2026-08-27/28 on `rift_O4d`; scripts and the full sweeps are in the PR
that added this file.

## What actually differs

Both rules are linear in the samples, so each *is* a weight vector, recovered
exactly by applying it to the identity (`w = rule(np.eye(npts), dx=deltaT)` --
the same trick the fused-calmarg kernel and `jax_ile` already use).  Doing that
settles the whole question:

| npts | scipy (CPU/JAX) interior | vendored `even='avg'` (GPU) interior |
|------|--------------------------|--------------------------------------|
| odd  | `4/3, 2/3, 4/3, ...`     | `4/3, 2/3, 4/3, ...` -- **bit-identical** |
| even | `4/3, 2/3, ...` from the left, tail `[5/4, 1, 5/12]` | **`1, 1, 1, ...`**, ends `[5/12, 13/12]` |

Averaging Simpson's two panel alignments -- which is what `even='avg'` does --
gives every interior sample weight `(4/3 + 2/3)/2 = 1`.  So for even `npts` the
GPU path is not running Simpson's rule at all: **it is running the trapezoidal
rule** with slightly modified end weights.  Both rules integrate a constant
exactly (weights sum to `(npts-1)*deltaT`), which is why the disagreement never
showed up in a normalization check.

For a non-negative integrand the two answers are bounded by the extreme weight
ratios, attained when a single sample dominates:

```
ln(I_scipy / I_avg)  in  [ ln(2/3), ln(4/3) ]  =  [-0.4055, +0.2877] nats
```

Measured through the shipped likelihood on real GPU hardware (`ldas-pcdev13`,
srate 4096, npts 614, 3 IFOs, identical inputs, numpy vs cupy): max per-sample
`|dlnL|` **0.3947 nats** -- the bound, saturated -- with `dlnZ = +0.28 nats` over
the extrinsic Monte Carlo.  It does **not** average out.  With a well-resolved
peak the same test gives `2e-8` nats.

`marginalization_time_grid` returns `npts = int(2*iwh/deltaT)`, i.e. at
`iwh=0.075 s`: 153 / 307 / **614** / **1228** / 2457 at srate 1024 / 2048 / 4096
/ 8192 / 16384.  **Only 4096 and 8192 are even** -- so the two commonest rates
are exactly the affected ones, and 16384 is not affected at all.

## Which rule is more accurate

Do not assume the newer scipy is better.  It is not, for this integrand.

Gaussian bump `exp(-(t-t0)^2/2 sigma^2)` on a 614-sample window, reference the
exact `erf`, worst case over 32 sub-sample offsets of the peak, error in nats:

| sigma / deltaT | scipy Simpson | `even='avg'` / trapezoid |
|-----|----------|----------|
| 0.5 | 2.0e-1   | 1.5e-2   |
| 1.0 | 4.8e-3   | 5.4e-9   |
| 1.5 | 1.0e-5   | 2.2e-16  |
| 2.0 | 1.8e-9   | 2.2e-16  |

The trapezoidal rule wins by six orders of magnitude at `sigma = deltaT`, and is
exact to machine precision from `1.5 deltaT` up.  This is Euler-Maclaurin: when
every derivative of the integrand vanishes at both ends of the window, all the
trapezoid's boundary correction terms vanish and only the aliasing error is
left, which for a smooth bump falls off *exponentially* in `sigma/deltaT`.
Simpson is `(4/3) T(h) - (1/3) T(2h)`; the `T(2h)` piece carries an
exponentially larger aliasing error, so mixing it in makes the answer worse, not
better.  `jax_ile.core.make_distance_gh` already relies on the same argument for
the distance quadrature.

The precondition is that the peak sits comfortably inside the window.  Where it
does not, Simpson's higher formal order wins (Gaussian, `sigma = 2 deltaT`):

| peak position | scipy Simpson | trapezoid |
|---------------|---------------|-----------|
| centre        | 1.8e-9        | 2.2e-16   |
| 1 sigma from the edge | 2.8e-4 | 6.2e-3   |
| 1 sigma outside       | 1.2e-3 | 4.4e-2   |

so the ordering reverses only in a regime where the time marginalization is
already mis-centred.

## The bigger error both rules share

The `lnL(t)` peak has width `sigma_t ~ 1/(2 pi f_c rho)` -- it **narrows as the
SNR grows**.  At srate 4096 with a band centroid near 400 Hz, `sigma_t/deltaT`
is 0.41 at `rho=4` and 0.08 at `rho=20`; on a band-limited-`kappa` model with an
exact spectral reference, *both* rules are then wrong by 0.4 to 69 nats, and
their mutual difference saturates at `ln(4/3)`.  At srate 16384 the same model
gives `sigma_t/deltaT = 1.7` at `rho=4`, `npts` is odd, and the CPU/GPU gap is
4e-16.

So the CPU/GPU divergence is a **symptom of under-resolution, and is always
smaller than the error both rules already share**.  Making the backends agree is
worth doing -- an answer that depends on which device ran it is indefensible --
but it is not the same as making the answer right.  Adequate `--srate-internal`
is what makes it right, and the opt-in band-limited quadrature (PR #203), which
refines the grid before integrating, removes the ambiguity where it applies.

On the extrinsic Monte Carlo -- 400 samples, each with its own sub-sample time
delay so the grid phase varies sample to sample as it does in production, srate
4096, exact spectral reference -- the error is a **bias on `lnZ`, not noise that
averages out**, and it reweights the extrinsic samples:

| `rho_net` | `dlnZ` scipy | `dlnZ` `even='avg'` | TV(w, w_exact) scipy / avg |
|-----|--------|--------|-------------|
| 6   | +0.13  | +0.16  | 0.20 / 0.14 |
| 10  | +0.23  | +0.42  | 0.32 / 0.25 |
| 15  | -0.50  | -0.18  | 0.47 / 0.44 |
| 25  | -3.92  | -3.52  | 0.60 / 0.60 |
| 40  | -12.9  | -12.5  | 0.74 / 0.74 |

Both rules, not one; the CPU-vs-GPU part is the difference between the two
columns (0.03 to 0.32 nats here) and the shared part is much larger.  This is a
stress case -- the model puts the band centroid near 400 Hz at srate 4096, i.e.
an internal rate deliberately too low for the signal -- and it is the same
mechanism behind the standing advice to set `--srate-internal` well above the
data rate for low-mass sources.

## What is shipped here, and what is not

`TIME_QUADRATURE` (env `RIFT_TIME_QUADRATURE`) selects the rule:

- `'auto'` -- **the default, and unchanged**: the historical backend-dependent
  choice, returned verbatim, so production numbers do not move.
- `'simpson'` -- scipy's convention on both backends.
- `'trapezoid'` -- trapezoid on both backends; no even/odd ambiguity at all.

**No default was changed.**  Both rules are the shipped default on their own
path, so either choice moves production numbers on one of them, and a change of
this class needs end-to-end known-answer runs, which have not been done.

The proposal, for sign-off: make `'trapezoid'` the default on **both** backends.
It is measurably the more accurate rule on the design integrand (a peak inside
the window) at both odd and even `npts`; it has no even/odd ambiguity, so the
divergence cannot come back; and it is what the GPU path already effectively
runs at even `npts`, so it moves the GPU numbers least.  It does move the CPU,
JAX and odd-`npts` numbers -- towards the truth, per the table above, but they
do move.
