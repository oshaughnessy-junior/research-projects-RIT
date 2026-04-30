#!/bin/bash
# Slurm driver script for workflow
# Submits each job via sbatch and chains them with --dependency=afterok
set -euo pipefail

JOBID_0=$(sbatch --export=ALL,SLURM_VAR_IDX=1 --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/eos_posterior/MARG-event-1.sub | awk '{print $4}')
echo "Submitted util_ConstructIntrinsicPosterior_GenericCoordinates_py_65 as ${JOBID_0}"
JOBID_1=$(sbatch --export=ALL,SLURM_VAR_IDX=2 --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/eos_posterior/MARG-event-2.sub | awk '{print $4}')
echo "Submitted util_ConstructIntrinsicPosterior_GenericCoordinates_py_66 as ${JOBID_1}"
JOBID_2=$(sbatch --dependency=afterok:${JOBID_0}:${JOBID_1} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/eos_posterior/EOSPost.sub | awk '{print $4}')
echo "Submitted util_EOSPosterior_py_64 as ${JOBID_2}"
JOBID_3=$(sbatch --dependency=afterok:${JOBID_2} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/eos_posterior/TEST.sub | awk '{print $4}')
echo "Submitted convergence_test_samples_67 as ${JOBID_3}"
