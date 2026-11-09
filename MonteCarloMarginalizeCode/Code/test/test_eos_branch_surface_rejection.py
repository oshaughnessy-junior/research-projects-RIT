"""Stable support and sample-local branch-interface failures."""

import numpy as np
import pytest

from RIFT.physics import eos_affine


class NarrowStableEOS:
    mMaxMsun_all = 2.0

    def branches_for_m(self, mass):
        return [0] if 1.96 <= mass <= 2.0 else []

    def lambda_from_m(self, mass, branch_id=None):
        return 100.0


def test_probe_does_not_infer_empty_support_from_point_grid():
    eos = NarrowStableEOS()
    assert eos_affine.require_branch_surface(eos) == 2.0
    assert eos_affine.select_branch(eos, 1.98, 0.5, 2.0) == (100.0, 0, 1)


def test_probe_uses_published_narrow_branch_bounds():
    class WithBounds(NarrowStableEOS):
        number_of_branches = 1
        branch_minimum_mass_msun = staticmethod(lambda branch: 1.96)
        branch_maximum_mass_msun = staticmethod(lambda branch: 2.0)

        def branches_for_m(self, mass):
            self.probed.append(mass)
            return super().branches_for_m(mass)

    eos = WithBounds()
    eos.probed = []
    assert eos_affine.require_branch_surface(eos) == 2.0
    assert 1.98 in eos.probed


def test_declared_empty_sequence_is_rejected():
    eos = NarrowStableEOS()
    eos.number_of_branches = 0
    with pytest.raises(eos_affine.BranchProbeFailure, match="no stable branches"):
        eos_affine.require_branch_surface(eos)


@pytest.mark.parametrize("bad_ids", [[np.nan], [0, 0], [-1], [True], [0.5]])
def test_bad_runtime_branch_ids_reject_only_their_samples(bad_ids):
    class IntermittentEOS(NarrowStableEOS):
        def branches_for_m(self, mass):
            return bad_ids if mass == 1.75 else [0]

    eos = IntermittentEOS()
    assert eos_affine.require_branch_surface(eos) == 2.0
    x, weights, ids = eos_affine.convert_waveform_coordinates_with_eos_central(
        [[1.7, 1.7], [1.75, 1.7], [1.8, 1.7]],
        [[0.5, 0.5]] * 3, coord_names=["lambda1", "lambda2"],
        low_level_coord_names=["m1", "m2"], eos_class=eos)
    assert np.isfinite(x[[0, 2]]).all()
    assert np.isneginf(x[1]).all()
    np.testing.assert_array_equal(ids[1], eos_affine.NO_BRANCH)
    np.testing.assert_array_equal(weights, 0.0)


def test_duplicate_ids_at_startup_are_configuration_error():
    eos = NarrowStableEOS()
    eos.branches_for_m = lambda mass: [0, 0]
    with pytest.raises(eos_affine.BranchSurfaceError, match="unique"):
        eos_affine.require_branch_surface(eos)
