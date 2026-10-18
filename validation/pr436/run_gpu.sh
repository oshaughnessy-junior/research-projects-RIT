#!/bin/bash
set -euo pipefail
root=/scratch/richard.oshaughnessy/pr436_adversarial_20261018
band=$1
case "$band" in
 pascal) sif=/scratch/richard.oshaughnessy/containers/rift_o4d_cc60-90_cuda118_20260717.sif ;;
 ampere) sif=/scratch/richard.oshaughnessy/containers/rift_o4d_cc60-90_cuda118_20260717.sif ;;
 blackwell) sif=/scratch/richard.oshaughnessy/containers/rift_o4d_cc90-120_cuda128_20260717.sif ;;
 *) exit 2 ;;
esac
mkdir -p "$root/final_$band/cupy_cache"
cd "$root/final_$band"
nvidia-smi > nvidia-smi.txt
export APPTAINERENV_PYTHONPATH="$root/final_code:$root/final_code/test:/scratch/richard.oshaughnessy/early_time_compression/xpytest/pylib"
export APPTAINERENV_GW_SURROGATE=''
export APPTAINERENV_OMP_NUM_THREADS=1 APPTAINERENV_OPENBLAS_NUM_THREADS=1 APPTAINERENV_MKL_NUM_THREADS=1
export APPTAINERENV_CUPY_CACHE_DIR="$root/final_$band/cupy_cache"
export APPTAINERENV_RIFT_CI_REQUIRE_GPU=1
apptainer exec --nv --bind /scratch "$sif" python -c 'import cupy; print(cupy.__version__); print(cupy.cuda.runtime.getDeviceProperties(0)); print(cupy.arange(4).sum().get())' > device.txt
apptainer exec --nv --bind /scratch "$sif" python -m pytest -v -rs -p no:cacheprovider \
 "$root/final_code/test/test_multirate_xpy.py" "$root/final_code/test/test_multirate_precompute.py" \
 "$root/final_code/test/test_multirate_two_rate.py" "$root/final_code/test/test_multirate_dsw_onesegment.py" > pytest.txt 2>&1
apptainer exec --nv --bind /scratch "$sif" python "$root/run_ile_matrix.py" "$root/final_code" \
 /scratch/richard.oshaughnessy/early_time_compression/a4 "$root/final_$band/ile" > ile_matrix.txt 2>&1
touch PASS
