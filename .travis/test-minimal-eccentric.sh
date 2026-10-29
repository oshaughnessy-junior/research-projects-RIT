#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD/MonteCarloMarginalizeCode/Code${PYTHONPATH:+:$PYTHONPATH}"
python -m pytest -q \
  MonteCarloMarginalizeCode/Code/test/test_assign_param_chieff_aligned.py \
  MonteCarloMarginalizeCode/Code/test/test_av_backport_adversarial.py \
  MonteCarloMarginalizeCode/Code/test/test_av_coverage_guard.py \
  MonteCarloMarginalizeCode/Code/test/test_av_empty_live_volume.py \
  MonteCarloMarginalizeCode/Code/test/test_av_retained_density.py \
  MonteCarloMarginalizeCode/Code/test/test_cip_lnL_annotation.py \
  MonteCarloMarginalizeCode/Code/test/test_cip_pipeline.py \
  MonteCarloMarginalizeCode/Code/test/test_cip_priors.py \
  MonteCarloMarginalizeCode/Code/test/test_convert_coordinates_row_independence.py \
  MonteCarloMarginalizeCode/Code/test/test_convert_coordinates_source_redshift.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_helper_defaults.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_phase.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_phase_auto.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_phase_models.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_phase_policy.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_phase_puff.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_phase_sampling.py \
  MonteCarloMarginalizeCode/Code/test/test_eccentric_prior_schedule.py \
  MonteCarloMarginalizeCode/Code/test/test_gwsignal_teob_mode_sign.py \
  MonteCarloMarginalizeCode/Code/test/test_mc_error.py \
  MonteCarloMarginalizeCode/Code/test/test_puffball_transverse_reflect.py \
  MonteCarloMarginalizeCode/Code/test/test_time_marginalization_grid_labels.py \
  MonteCarloMarginalizeCode/Code/test/test_time_marginalization_perrow_offset.py
