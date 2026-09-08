# JAX angle-marginalization memory model

These are logical array-size and lifetime models for the JAX-only
angle-marginalization kernels. Except for the historical XLA allocation request
identified below, they are not measurements of CUDA allocator peak memory.
They must not be read as the footprint of conventional production ILE.

The evaluation cap in `samplers.py` protects only callers using `eval_lnL*`.
Direct `log_likelihood` calls and scalar value/gradient/Hessian entry points
bypass it, and a fraction of reported device memory does not bound the sum of
live buffers, allocator reservations, or reverse-mode residuals.

Let `S` be batch size, `T=data.npts`, `F` a phi chunk, `D` a distance block,
`Q=16` the Laplace u chunk, `E` the exact dense-angle chunk, `G` the exact
distance block, and `P` the rolled sample-time point block. Float64 and
complex128 occupy 8 and 16 bytes.

## Common storage

For source mode bound `m`, the coefficient tables have shapes
`(m+1,3,S,T)` and `(2m+1,5,S,T)` complex128. Together they contain

```
16 S T [3(m+1) + 5(2m+1)] bytes.
```

At `m=2` this is `544 S T` bytes: 2.42 GiB at `S=4000,T=1193`.
Their angle-sample loop is rolled, but coefficient construction is not yet
tiled over the evaluation sample/time axes. These tables persist across the
phi scan; the quoted number is their logical payload, not an allocator peak.

## Exact

The dense angle grid is scanned in `E=8` chunks and distance in `G=32`
blocks. The dominant exponent slab is `(E S,T,G)` float64, or
`8 E G S T = 2048 S T` bytes (9.10 GiB at `4000 x 1193`). Grid length is
bounded; sample and time still multiply the slab. Exact therefore remains
under the conservative outer cap pending point-axis tiling.

## Laplace

Before this patch, one step of the u scan formed the logical f64 result
`blk` with shape

```
(Q,D,F,S,T) float64 = 8 Q D F S T = 8192 S T bytes
```

at shipped `Q=16,D=4,F=16`. It is reduced over `Q` immediately; the distance
and phi scans do not keep all of their blocks simultaneously. The complex128
products used to form `blk` have the same shape but are eligible for compiler
fusion. At `S=4000,T=1193`, the f64 `blk` alone is 36.407 GiB. Commit
`c5b81dd6` records that XLA requested this single allocation during a pre-cap
SNR-40 JAX acceptance run against a 25-GiB cgroup. This investigation does not
have the original allocator log, did not reproduce that run, and did not
measure a 36-GiB current-production footprint.

The other source-visible live values include the persistent coefficient tables,
five phi fields (`64 F S T` bytes: A0/B0 real, A1/B1/B2 complex), distance-scan
carries and, for differentiated calls, residuals selected by XLA/AD. Their
simultaneous physical lifetime cannot be obtained by summing source-level
shapes and requires an allocator profile.

Laplace now flattens the independent `(S,T)` axes, edge-pads only the last
tile, and maps distance/psi marginalization over fixed tiles. Its expensive
slab is bounded by

```
8 Q D F min(S T,P),  P=LAPLACE_POINT_BLOCK=4096,
```

or 32 MiB with shipped inner blocks for a direct call whose only batched axes
are the explicit `S,T` axes. Padding repeats a finite edge point and is discarded
before the phi reduction. Every real bin retains the same distance nodes, psi
quadrature, per-bin reduction order, phi reduction, and Simpson time
marginalization. The map body is checkpointed for reverse AD. Coefficient tables
and phi fields remain `O(S T)`, so this is neither a claim that total memory is
32 MiB nor a bound on an arbitrary transformed caller.

In particular, `flowMC` applies an outer `vmap` over its chains to the scalar AD
target. The scalar wrapper has explicit `S=1`, so its `pblk` calculation cannot
see that mapped chain axis. For the usual 20-chain driver call at `T=1193`, the
corresponding logical primal slab is at most about 186 MiB before accounting for
AD residuals, not 36.41 GiB, but it is also not covered by the 32-MiB statement.

## Production call paths

Conventional `integrate_likelihood_extrinsic_batchmode` does not call this JAX
kernel. Its maintained GPU NoLoop path samples distance, phi and psi and carries
primarily `(S,T)` arrays (`kappa_sq` complex128 and `rho_sq` float64); it has no
`Q*D*F` angle-quadrature multiplier. Operation on 4-GB cards therefore does not
contradict the JAX shape above.

The separate `integrate_likelihood_extrinsic_jax` reaches this kernel only for
the distance+phi+psi-marginalized mode with a resolved Laplace scheme. Its host
pilot/reweight evaluations call `angle_marg_eval_chunk`; the sampler helpers do
the same. At `T=1193`, the 4-GiB fallback target caps the old model at `S=439`,
so the current production call path does not submit `S=4000`. Scalar
value/gradient/Hessian calls use explicit `S=1`; flowMC normally maps those over
20 chains.

When a device limit is known, `_angle_marg_buffer_target()` now always applies
the configured fraction: a reported 4-GiB card therefore gets a 2-GiB target at
the default fraction. The historical 4-GiB value is reserved for the
unknown-device fallback. If the modeled payload for one sample exceeds the
target, the evaluation helper raises a resource preflight error instead of
returning a fictitious chunk size of one. This remains a source-level working-set
model, not a bound on total allocator use; direct `log_likelihood` calls bypass
the helper altogether.

## psi_local_phi_dense (`--angle-marg-scheme peak-local`)

Kernel id `psi_local_phi_dense` (`RIFT/likelihood/peak_local_names.py`). This
section is about that kernel and not about `four_axis_local`, the four-axis
controller branch, which has no dense angle axis and whose workspace is
`O(local_order**4)` per mode.

The u-node axis is already streamed with `U_live<=8`, and phi with `F=16`.
The node body per sample-time point is `8 F N_x 4 U_live` bytes: 1 MiB at
`N_x=256`. The phi scan also returns every step before reducing it, so its
stacked `(n_phi,N_x)` f64 result adds `8 n_phi N_x` bytes per sample-time point.
The outer evaluation cap budgets the sum and refuses a call when even one sample
does not fit. For example, at `T=1193,N_x=256,m_max=2`, `A=450` gives
`n_phi=352` and a 1.966-GiB one-sample model, while `A=12500` gives
`n_phi=1792` and a 5.242-GiB model.

This does not fix hidden transformed axes. Nested `vmap(vmap(_one))` still
multiplies the body and scan result by explicit `S T`, and flowMC applies an
additional outer chain `vmap` to the scalar likelihood that this preflight
cannot see. A follow-up must roll those axes around `_one` and GPU-profile a
suitably smaller point tile before peak-local can claim a total-memory bound.

## The buffer allowance, and how to set it per card

`angle_marg_eval_chunk` compares the per-sample model above against an allowance.
`_angle_marg_buffer_target` derives it by one of four paths, and records which one at
the return site. The refusal prints that record, so a reader is never left with a
number and no account of it.

| path | allowance | when |
|---|---|---|
| `RIFT_ANGLEMARG_BUFFER_BYTES` | the value, absolute | whenever it is set |
| device probe | `RIFT_ANGLEMARG_BUFFER_FRACTION` (default 0.5) times readable free memory | a GPU reports a usable free-memory key |
| on-demand allocator | 4 GiB, bounded by `bytes_limit - bytes_in_use` | small pool beside a much larger limit |
| blind fallback | 4 GiB | no GPU, no readable key, or the probe raised |

Two operating-point errors here produce messages that read like code faults.

**The absolute override does not track the card.** It wins over every probe, so a
value copied between hosts is wrong on the second one; a runner carrying a hardcoded
6 GiB on a 24 GiB card was found on 2026-09-08. Size it as half of what `nvidia-smi`
shows free to you: 12 GiB on a 24 GiB card you hold alone. Set it only when the probe
cannot read the device.

**A zero allowance now means what it says.** Before #285, `largest_free_block_bytes`
reading 0 on jax 0.9.2 was taken for a full card, so the preflight refused the default
`exact` scheme on an idle 24 GiB device. That is fixed. A zero surviving the probe
allocation is a consumed pool, so check `nvidia-smi` for who holds the card.

**A large allowance does not make every call fit.** `psi_local_phi_dense` sizes
`n_phi` from the amplitude, so its per-sample buffer grows as `sqrt(A)`. Evaluating
the model above at `T=1193`, `N_x=256`, `m_max=2`:

| rho | n_phi | per-sample model |
|---:|---:|---:|
| 40.8 | 464 | 2.22 GiB |
| 81.5 | 928 | 3.28 GiB |
| 163.1 | 1856 | 5.39 GiB |
| 326.2 | 3696 | 9.58 GiB |
| 652.3 | 7392 | 17.99 GiB |

A 24 GiB card at the default fraction allows about 12 GiB, so the top two rows fit
at no chunk size: the sample axis is the only one this cap divides. Shorten the time
window, shrink the distance grid, or run another kernel.

These are MODEL values from `samplers._peaklocal_bytes_per_sample_pt`, reproducible
from this file. A 2026-09-08 sampler-arms probe recorded an observed refusal of
19.99 GiB, but its configuration survives in neither repository, and that figure
implies `rho ~ 731` at the dimensions above. An earlier revision attached it to
rungs 160 and 640, which the model contradicts by 3.7x at rung 160. Quote the model,
or quote a run whose dimensions you have.

## Validation boundary

Checkpointing the exact/Laplace phi scans and peak-local phi/u scans bounds
saved loop residuals, but does not by itself shrink primal `S*T`
vectorization. Tests inspect the traced Laplace kernel-input shape and compare
tiled versus one-block values and gradients, including a padded tail.

CPU tests cannot establish CUDA allocator peaks, GPU XLA fusion, or the
throughput-optimal `P`. Before relaxing `angle_marg_eval_chunk`, profile all
three schemes on a production CUDA host at `T≈1193`, batches spanning the
current cap and nominal 1000/4000, and exercise value, gradient, and
Fisher/Hessian calls while recording allocator peak statistics. Profile the
flowMC outer-vmap path separately: explicit point tiling does not bound that
hidden chain axis.

## 2026-09-08: jax 0.9.2 never populates `largest_free_block_bytes`

On jax 0.9.2 (ldas-pcdev11, idle 24 GiB card) `largest_free_block_bytes` and
`pool_bytes` both read 0 before the first allocation, so `_device_available_bytes`
now treats a bare 0 in either field as "not reported" and falls through, rather
than as a full device.

Adversarial review of that fix found it reachable on a *busy* card too, before
this process's own first allocation, where 0/0 read the same as idle but the 4 GiB
fallback it falls through to is not safe. `_angle_marg_buffer_target` now forces
one tiny allocation with `_probe_allocate` before reading `memory_stats()`, so the
pool signal exists to read; if the resulting pool is small next to `bytes_limit`
(the on-demand-allocator shape, where the pool only grows to fit what has been
requested so far), availability is bounded by `bytes_limit - bytes_in_use` rather
than trusted. A missing `bytes_in_use` key is now read as unknown, not 0.
