"""Opt-in gate against an actual reviewed LALSimulation build.

This is intentionally separate from the fake-backed compatibility tests. It
skips ordinary CI unless RIFT_REVIEWED_LALSIM_MANIFEST names a build-generated
manifest and fails closed once the gate is enabled.
"""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re

import numpy as np
import pytest


MANIFEST_ENV = "RIFT_REVIEWED_LALSIM_MANIFEST"
REQUIRED_SYMBOLS = (
    "SimulationVCSInfo",
    "SimNeutronStarEOSFromFilePhaseTransition",
    "CreateSimNeutronStarFamilyPT",
    "SimNeutronStarFamNumberOfBranches",
    "SimNeutronStarFamBranchMinMass",
    "SimNeutronStarFamBranchMaxMass",
    "SimNeutronStarFamBranchRadius",
    "SimNeutronStarFamBranchLoveNumberK2",
    "SimNeutronStarFamBranchCentralPressure",
    "SimNeutronStarEOSMultiPartsPseudoEnthalpyOfPressure",
    "SimNeutronStarEOSMultiPartsSpeedOfSoundOfPseudoEnthalpy",
)


def _sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_manifest():
    manifest_name = os.environ.get(MANIFEST_ENV)
    if not manifest_name:
        pytest.skip(
            "real reviewed-LALSimulation gate disabled; set {}".format(
                MANIFEST_ENV
            )
        )
    manifest_path = Path(manifest_name).resolve()
    with manifest_path.open() as stream:
        manifest = json.load(stream)
    ref = manifest.get("lalsuite_ref", "")
    assert re.fullmatch(r"[0-9a-f]{40}", ref), (
        "lalsuite_ref must be the exact 40-character commit built for this job"
    )
    return manifest_path, manifest


def _load_adapter_module():
    """Load the pure adapter without importing RIFT's heavyweight __init__."""
    source = (
        Path(__file__).resolve().parents[1]
        / "RIFT" / "physics" / "lalsim_eos_compat.py"
    )
    spec = importlib.util.spec_from_file_location(
        "rift_lalsim_eos_compat_gate", str(source)
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_actual_reviewed_lalsimulation_tables(record_property):
    import lal
    import lalsimulation as lalsim
    adapter_module = _load_adapter_module()
    AmbiguousFamilyBranchError = adapter_module.AmbiguousFamilyBranchError
    LALSimNeutronStarFamilyAdapter = (
        adapter_module.LALSimNeutronStarFamilyAdapter
    )

    manifest_path, manifest = _load_manifest()
    missing = [name for name in REQUIRED_SYMBOLS if not hasattr(lalsim, name)]
    assert not missing, "reviewed LALSimulation symbols missing: {}".format(missing)
    vcs_info = lalsim.SimulationVCSInfo
    assert vcs_info.vcsId == manifest["lalsuite_ref"], (
        "manifest ref {} does not match imported LALSimulation build {}".format(
            manifest["lalsuite_ref"], vcs_info.vcsId
        )
    )
    assert vcs_info.vcsClean == "CLEAN", (
        "reviewed LALSimulation build has uncommitted source modifications: {}"
        .format(vcs_info.vcsStatus)
    )
    record_property("lalsuite_ref", manifest["lalsuite_ref"])
    record_property(
        "lalsimulation_version",
        getattr(lalsim, "LALSIMULATION_VERSION", "unknown"),
    )
    record_property("lalsimulation_vcs_status", vcs_info.vcsStatus)
    record_property("lalsimulation_vcs_tag", vcs_info.vcsTag)
    fixtures = manifest.get("fixtures", {})
    assert set(fixtures) == {"two_column", "nine_column", "twin_star"}
    loaded = {}
    expected_columns = {"two_column": 2, "nine_column": 9, "twin_star": None}
    for name in ("two_column", "nine_column", "twin_star"):
        fixture = fixtures[name]
        path = (manifest_path.parent / fixture["path"]).resolve()
        assert path.is_file(), "missing {} fixture: {}".format(name, path)
        assert _sha256(path) == fixture["sha256"]
        data = np.loadtxt(str(path))
        columns = 1 if data.ndim == 1 else data.shape[1]
        if expected_columns[name] is not None:
            assert columns == expected_columns[name]
        multipart_eos = lalsim.SimNeutronStarEOSFromFilePhaseTransition(
            str(path)
        )
        loaded[name] = LALSimNeutronStarFamilyAdapter(
            multipart_eos, minimal=True, multipart=True,
            lalsim_module=lalsim,
        )
        assert loaded[name].number_of_branches >= 1

    # Exercise the reviewed CreateFamilyPT min_fam argument in both modes.
    nine_path = (manifest_path.parent / fixtures["nine_column"]["path"]).resolve()
    nine_eos = lalsim.SimNeutronStarEOSFromFilePhaseTransition(str(nine_path))
    nine_extended = LALSimNeutronStarFamilyAdapter(
        nine_eos, minimal=False, multipart=True, lalsim_module=lalsim
    )
    assert nine_extended.number_of_branches >= 1

    family = loaded["twin_star"]
    assert family.number_of_branches >= 2
    overlaps = []
    for left in range(family.number_of_branches):
        for right in range(left + 1, family.number_of_branches):
            lower = max(family.minimum_mass(left), family.minimum_mass(right))
            upper = min(family.maximum_mass(left), family.maximum_mass(right))
            if lower < upper:
                overlaps.append((left, right, 0.5 * (lower + upper)))
    assert overlaps, "twin_star fixture has no overlapping stable mass branches"
    left, right, mass = overlaps[0]
    with pytest.raises(AmbiguousFamilyBranchError):
        family.radius(mass)
    with pytest.raises(ValueError, match="branch_id .* outside"):
        family.radius(mass, branch_id=family.number_of_branches)
    outside_left = np.nextafter(family.maximum_mass(left), np.inf)
    with pytest.raises(ValueError, match="outside stable branch"):
        family.radius(outside_left, branch_id=left)
    radii = [family.radius(mass, branch_id=branch) for branch in (left, right)]
    love = [
        family.love_number_k2(mass, branch_id=branch)
        for branch in (left, right)
    ]
    pressure = [
        family.central_pressure(mass, branch_id=branch)
        for branch in (left, right)
    ]
    pseudo_enthalpy = [
        lalsim.SimNeutronStarEOSMultiPartsPseudoEnthalpyOfPressure(
            pressure_here, family.eos
        )
        for pressure_here in pressure
    ]
    sound_speed = [
        lalsim.SimNeutronStarEOSMultiPartsSpeedOfSoundOfPseudoEnthalpy(
            enthalpy_here, family.eos
        )
        for enthalpy_here in pseudo_enthalpy
    ]
    sound_speed_over_c = [value / lal.C_SI for value in sound_speed]
    tidal_lambda = [
        (2.0 / 3.0) * love_here * (radius_here * lal.C_SI**2 /
                                   (mass * lal.G_SI))**5
        for love_here, radius_here in zip(love, radii)
    ]
    assert all(
        np.isfinite(value) and value > 0
        for value in radii + love + pressure + pseudo_enthalpy
        + sound_speed + sound_speed_over_c + tidal_lambda
    )
    assert all(value > 1.0 for value in sound_speed), (
        "multipart sound-speed accessor must return SI m/s"
    )
    assert all(value < 1.1 for value in sound_speed_over_c)
    assert not np.isclose(radii[0], radii[1], rtol=1e-10, atol=0)
    assert not np.isclose(love[0], love[1], rtol=1e-10, atol=0)
    assert not np.isclose(pressure[0], pressure[1], rtol=1e-10, atol=0)
    assert not np.isclose(tidal_lambda[0], tidal_lambda[1], rtol=1e-10, atol=0)
