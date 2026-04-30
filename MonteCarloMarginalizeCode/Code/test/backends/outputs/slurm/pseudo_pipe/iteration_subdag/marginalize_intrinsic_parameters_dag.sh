#!/bin/bash
# Slurm driver script for workflow
# Submits each job via sbatch and chains them with --dependency=afterok
set -euo pipefail

JOBID_0=$(sbatch /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/init.sub | awk '{print $4}')
echo "Submitted cp_70 as ${JOBID_0}"
JOBID_1=$(sbatch --dependency=afterok:${JOBID_0} --export=ALL,SLURM_VAR_EVENT=0 --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/ILE-0.sub | awk '{print $4}')
echo "Submitted integrate_likelihood_extrinsic_batchmode_71 as ${JOBID_1}"
JOBID_2=$(sbatch --dependency=afterok:${JOBID_1} --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/CIP-0.sub | awk '{print $4}')
echo "Submitted util_ConstructIntrinsicPosterior_GenericCoordinates_py_72 as ${JOBID_2}"
JOBID_3=$(sbatch --dependency=afterok:${JOBID_2} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/TEST-0.sub | awk '{print $4}')
echo "Submitted convergence_test_samples_73 as ${JOBID_3}"
JOBID_4=$(sbatch --dependency=afterok:${JOBID_3} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/PUFF-0.sub | awk '{print $4}')
echo "Submitted util_ParameterPuffball_py_74 as ${JOBID_4}"
JOBID_5=$(sbatch --dependency=afterok:${JOBID_4} --export=ALL,SLURM_VAR_EVENT=1 --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/ILE-1.sub | awk '{print $4}')
echo "Submitted integrate_likelihood_extrinsic_batchmode_75 as ${JOBID_5}"
JOBID_6=$(sbatch --dependency=afterok:${JOBID_5} --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/CIP-1.sub | awk '{print $4}')
echo "Submitted util_ConstructIntrinsicPosterior_GenericCoordinates_py_76 as ${JOBID_6}"
JOBID_7=$(sbatch --dependency=afterok:${JOBID_6} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/TEST-1.sub | awk '{print $4}')
echo "Submitted convergence_test_samples_77 as ${JOBID_7}"
JOBID_8=$(sbatch --dependency=afterok:${JOBID_7} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/pseudo_pipe/iteration_subdag/PUFF-1.sub | awk '{print $4}')
echo "Submitted util_ParameterPuffball_py_78 as ${JOBID_8}"
