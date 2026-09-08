# Band-limited time quadrature for the distance-marginalized JAX likelihood

RECORD, 2026-09-08. Method and measured numbers for
`time_quadrature="bandlimited"` on `JAXDistanceMarginalizedLikelihood`. The code
carries the tested constants and a pointer here. It carries no numbers.

## What changed

`fused_log_likelihood_distmarg` used to reduce over distance on the data time
grid and hand the reduced `lnL(t)` to `_time_marginalize_terminal`, which
refuses `bandlimited`. Interpolating an already-reduced nonlinear field can
converge to the wrong function. The kernel now gathers the guarded complex
primitive, refines it, and applies the same distance quadrature at every refined
node.

Order of operations under `bandlimited`:

1. `_accumulate_unit(..., guard=g)` with `g = bandlimited_time_guard(npts)[1]`,
   the certified guard.
2. Per row, inside `lax.map`: raised-cosine support pad, then even-extension FFT
   refinement of `kappa(t)` at a curvature-derived power-of-two factor.
3. Crop to the integrated window.
4. `log sum_g exp(K x_g - R x_g^2 / 2 + log w_g)` at every remaining node, in
   blocks sized from the refined row length.
5. Stable trapezoid over the original closed window.

Step 3 comes before step 4. `reduce_fn` is pointwise in the node, so the value
is unchanged, and the distance reduction then runs on `(npts-1)*f+1` nodes
instead of the guard-padded `2*(npts+2g-1)*f`. Reducing first cost 6.50 GB for
one scalar `value_and_grad`; cropping first costs 2.56 GB.

Certificates are the fixed-distance ones applied to the distance-marginalized
field: factor doubling to 1e-3 nat, remeasured peak-width resolution, a 15-nat
endpoint gap, and agreement between the certified guard and half of it. A row
that fails any of them returns NaN and the driver refuses the run.

`bandlimited_time_guard` is new and is the single definition of the
`(initial, certified)` guard pair. The fixed-distance kernel, the
distance-marginalized kernel and the driver's storage-window sizing all read it.

The curvature probe moved inside the row-local `lax.map`. It used to be computed
for the whole batch, which for the distance path would have carried an
`(S, 2*n_guarded, block)` temporary.

`JAX_ILE_DISTMARG_GH` is not read by `fused_log_likelihood_distmarg` on either
quadrature. The per-sample Gauss-Hermite placement is implemented only in the
phi/psi-marginalized kernels, which refuse `bandlimited`. Both branches of the
distance-marginalized kernel call one helper, so a future GH branch there lands
on the refined grid as well as the coarse one.

## Scope, and why the other wrappers still refuse

- `JAXDistPhiMargLikelihood` and `fused_log_likelihood_distphimarg`: the phi_ref
  grid sum streams one primitive per grid point through a `lax.scan` carrying
  only `(S, npts)`. Primitive-first requires `(nphi, n_fine)` per row. At the
  shipped `nphi = 32` and the certified factor cap that is about 200 times the
  row-local budget. This is a cost limit, not a correctness gap.
- `JAXDistPsiMargLikelihood`, `JAXDistPhiPsiMargLikelihood`, the exact-angle and
  Laplace schemes: these reach `anglemarg.py` coefficient-table kernels that
  return an already-reduced `lnL(t)`. There is no primitive at that seam to
  refine without an adapter. `time_first_peaklocal.py` prototypes one.

## Measurement setup

Zero-noise synthetic injection through the production `PrecomputeLikelihoodTerms`:
35 + 30 Msun, `IMRPhenomD`, H1L1, `fmin = fref = 40 Hz`, `fmax = 300 Hz`,
`deltaF = 0.25 Hz`, srate 1024 Hz, integration half-window 75 ms (npts 153,
guard 128 initial / 256 certified), distance grid 512 uniform nodes over
[50, 4000] Mpc with the Euclidean prior, evaluated at the injected angles
(1.2, -0.4, 0.7, 0.9, 2.1). Host `ldas-grid`, CPU, float64,
`~/.cache/jaxci_venv/bin/python`.

Amplitude is set by the injected distance: 390 Mpc gives rho 19.92, and
48.5 Mpc gives rho 160.18.

## Agreement with an independent reference

The reference reconstructs the primitive with a plain periodic zero-padded FFT,
reduces over distance with a numpy log-sum-exp, and integrates with a numpy
trapezoid. Its extension is periodic where the shipped one is an even
reflection, and its guard taper ramps on `(k+1)/(g+1)` where the shipped one
ramps on `k/g`.

| rho | shipped bandlimited | reference (guard 512, factor 512) | difference (nat) |
|---|---|---|---|
| 19.92 | 589.877456830 | 589.877457058 | -2.3e-07 |
| 160.18 | 39193.075978776 | 39193.075993296 | -1.5e-05 |

Reference convergence, tapered. Successive differences along each ladder:

| ladder | rho 19.92 | rho 160.18 |
|---|---|---|
| factor 64→128→256→512→1024 at guard 512 | 0, 0, 0, 0 | -5.6e-02, +1.2e-03, +7e-12, 0 |
| guard 128→256→512→1024 at factor 512 | +7.4e-07, +1.8e-07, +4.5e-08 | +4.7e-05, +1.2e-05, +2.9e-06 |

The taper is required. An untapered periodic reconstruction leaves a step at the
periodic seam, and its Gibbs ringing decays like 1/guard:

| untapered reference, factor 128 | rho 19.92 | rho 160.18 |
|---|---|---|
| guard 128 | 589.929584 | 39196.403097 |
| guard 256 | 589.897169 | 39194.333390 |
| guard 512 | 589.883333 | 39193.449964 |
| guard 1024 | 589.878738 | 39193.156559 |

Each doubling halves the residual instead of removing it. At guard 1024 the
untapered reference is still 1.3e-03 nat (rho 20) and 8.1e-02 nat (rho 160) from
the shipped value, so it certifies nothing at the tolerance this work asserts.

The shipped value is not sitting on its own stopping tolerance. Forcing
`_TIME_ADAPTIVE_RTOL` to 1e-4 selects the same factor and returns the same
number, 39193.075978776. At 1e-5 and 1e-6 the row returns NaN, because the
doubling cannot be met within `_TIME_ADAPTIVE_FACTOR_MAX`.

## The option changes the answer

| rho | native Simpson | bandlimited | gap (nat) |
|---|---|---|---|
| 19.92 | 541.265029 | 589.877457 | 48.6 |
| 160.18 | 35923.702300 | 39193.075979 | 3269.4 |

Reduce-then-refine, built explicitly in the test with the same numpy pieces,
lands 3.31 nat (rho 20) and 99.35 nat (rho 160) from the shipped value.

Mutating `at_factor` to reduce on the coarse grid and refine the reduced field
makes the rho 20 agreement test fail by -3.366 nat, about 3400 times its
tolerance, and makes the rho 160 row return NaN. The resolution and doubling
certificates reject the wrong-order field on their own.

## Memory

Peak RSS, one JAX process, same data. `vmap 8` is eight chains of
`value_and_grad`, which is the shape flowMC's MALA proposal builds.

| path | scalar | vmap 8 |
|---|---|---|
| fixed-distance 6-D bandlimited (shipped) | 1.15 GB | 3.25 GB |
| distance-marginalized, reduce before crop | 6.50 GB | - |
| distance-marginalized, crop before reduce | 2.56 GB | 9.96 GB |

The remaining factor over the fixed-distance path is the distance grid itself.
`_BANDLIMITED_GRID_ELEMENTS` sits near the minimum of a two-sided trade-off, so
lowering it makes things worse. At `1<<22` the `vmap 8` peak is 9.97 GB; at
`1<<18` it is 22.77 GB, because a smaller block multiplies the scan carries the
reverse pass retains, and smaller values exceed the 25 GiB per-user cgroup.

Driver runs of `--mode flowmc --distance-marginalization`, integration
half-window 20 ms, 32 distance nodes, one training and one production loop:

| steps (local = global) | quadrature | peak RSS | wall | result row |
|---|---|---|---|---|
| 20 | simpson | 1.43 GB | 0:21 | yes |
| 20 | bandlimited | 23.4 GB | 4:50 | yes |
| 4 | bandlimited | 6.36 GB | 2:03 | yes |
| 2 | bandlimited | 4.46 GB | 2:09 | yes |

flowMC unrolls its per-step proposal, so the compiled graph is multiplied by the
step count. The eleven refinement branches make that graph large, and 20 steps
exceeds the 25 GiB cgroup on `ldas-grid`. The gated driver test uses 2 steps and
brackets the peak with `--n-prior-pilot` instead.

## Files

- `core.py`: `bandlimited_time_guard`, `_time_marginalize_reflected_primitive`
  (`reduce_fn`, row-local probe, crop before reduce),
  `_logsumexp_grid_scanned`, `fused_log_likelihood_distmarg`.
- `wrapper.py`: `JAXDistanceMarginalizedLikelihood` accepts the option and
  publishes `time_guard_initial` and `time_guard_certified`.
- `test/jax/test_jax_bandlimited_distmarg.py`: 14 tests, 181 s on `ldas-grid`.
