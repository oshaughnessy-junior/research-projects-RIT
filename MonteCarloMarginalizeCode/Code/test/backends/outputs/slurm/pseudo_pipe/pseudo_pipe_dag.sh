#!/bin/bash
# Slurm driver script for workflow
# Submits each job via sbatch and chains them with --dependency=afterok
set -euo pipefail

JOBID_0=$(sbatch /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/helper.sub | awk '{print $4}')
echo "Submitted helper_LDG_Events_py_68 as ${JOBID_0}"
JOBID_1=$(sbatch --dependency=afterok:${JOBID_0} --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/PSD.sub | awk '{print $4}')
echo "Submitted BayesWave_69 as ${JOBID_1}"
# Sub-DAG: /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/marginalize_intrinsic_parameters.dag
echo "Slurm backend: SUBDAG nodes are not supported natively; driving sub-script /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/marginalize_intrinsic_parameters.dag via bash" >&2
JOBID_2=$(sbatch --dependency=afterok:${JOBID_1} --wrap="bash /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/marginalize_intrinsic_parameters.dag" | awk '{print $4}')
JOBID_3=$(sbatch --dependency=afterok:${JOBID_2} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/CalReweight.sub | awk '{print $4}')
echo "Submitted util_CalibrationReweight_py_80 as ${JOBID_3}"
JOBID_4=$(sbatch --dependency=afterok:${JOBID_3} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/PLOT.sub | awk '{print $4}')
echo "Submitted plot_posterior_corner_py_81 as ${JOBID_4}"
