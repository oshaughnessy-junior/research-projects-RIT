#!/bin/bash
# Slurm driver script for workflow
# Submits each job via sbatch and chains them with --dependency=afterok
set -euo pipefail

JOBID_0=$(sbatch /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/init.sub | awk '{print $4}')
echo "Submitted cp_55 as ${JOBID_0}"
JOBID_1=$(sbatch --dependency=afterok:${JOBID_0} --export=ALL,SLURM_VAR_EVENT=0 --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/ILE-0.sub | awk '{print $4}')
echo "Submitted integrate_likelihood_extrinsic_batchmode_56 as ${JOBID_1}"
JOBID_2=$(sbatch --dependency=afterok:${JOBID_1} --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/CIP-0.sub | awk '{print $4}')
echo "Submitted util_ConstructIntrinsicPosterior_GenericCoordinates_py_57 as ${JOBID_2}"
JOBID_3=$(sbatch --dependency=afterok:${JOBID_2} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/TEST-0.sub | awk '{print $4}')
echo "Submitted convergence_test_samples_58 as ${JOBID_3}"
JOBID_4=$(sbatch --dependency=afterok:${JOBID_3} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/PUFF-0.sub | awk '{print $4}')
echo "Submitted util_ParameterPuffball_py_59 as ${JOBID_4}"
JOBID_5=$(sbatch --dependency=afterok:${JOBID_4} --export=ALL,SLURM_VAR_EVENT=1 --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/ILE-1.sub | awk '{print $4}')
echo "Submitted integrate_likelihood_extrinsic_batchmode_60 as ${JOBID_5}"
JOBID_6=$(sbatch --dependency=afterok:${JOBID_5} --requeue /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/CIP-1.sub | awk '{print $4}')
echo "Submitted util_ConstructIntrinsicPosterior_GenericCoordinates_py_61 as ${JOBID_6}"
JOBID_7=$(sbatch --dependency=afterok:${JOBID_6} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/TEST-1.sub | awk '{print $4}')
echo "Submitted convergence_test_samples_62 as ${JOBID_7}"
JOBID_8=$(sbatch --dependency=afterok:${JOBID_7} /Users/rossma/RIFT_ralph/MonteCarloMarginalizeCode/Code/test/backends/outputs/slurm/cepp_basic_iteration/PUFF-1.sub | awk '{print $4}')
echo "Submitted util_ParameterPuffball_py_63 as ${JOBID_8}"
