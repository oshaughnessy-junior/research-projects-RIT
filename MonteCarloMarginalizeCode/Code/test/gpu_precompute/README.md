# GPU precompute validation

The unit suite compares every returned array against an independent NumPy oracle and
also compares a full `Re<h|d> - <h|h>/2` likelihood against the `Q/U` contraction.
Inputs are complex and non-Hermitian, both frequency halves are populated, PSD weights
are asymmetric, and one context is reused across multiple intrinsic points.

Run the CPU gate from `MonteCarloMarginalizeCode/Code`:

```bash
PYTHONPATH=. python -m pytest -q test/gpu_precompute
```

Run the mandatory device gate in the CUDA container:

```bash
RIFT_REQUIRE_GPU_PRECOMPUTE=1 PYTHONPATH=. python -m pytest -q \
  test/gpu_precompute --require-gpu
```

The benchmark starts its clock inside the already-running worker, synchronizes the
device before and after each call, and reports several sequential intrinsic points:

```bash
PYTHONPATH=. python test/gpu_precompute/benchmark_gpu_precompute.py \
  --backend cupy --bins 2048 --basis 40 --modes 1 --window 256 \
  --intrinsics 3
```

The short physical three-detector benchmark compares CPU and GPU precompute and processes
five nearby intrinsic points in one worker:

```bash
PYTHONPATH=. python test/benchmark_gpu_short_worker.py --intrinsics 5
```

The AV integration gate generates its own short frames and PSDs, evaluates two intrinsic
points with an n-eff target of 20, and limits saved samples to a 200-row fairdraw:

```bash
PYTHONPATH=. python test/gpu_precompute/run_short_av_ile.py --keep
```

Container transfer and queue latency are outside all reported timers.  Long BNS-scale
arrays are deferred until the short correctness and memory gates pass.
