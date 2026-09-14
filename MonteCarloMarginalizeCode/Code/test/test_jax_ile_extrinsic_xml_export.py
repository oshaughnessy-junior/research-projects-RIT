"""The JAX ILE driver must export sim_inspiral XML, not only the .dat sidecar.

create_event_parameter_pipeline_BasicIteration's --last-iteration-extrinsic
stage hands its convert_extr job ``EXTR_out-<event>.xml_<k>_.xml.gz``.  The
driver wrote only ``<output>_<k>_samples.dat``, so a DAG driven by it built
cleanly, ran its terminal stage and collected nothing.  These tests pin the
XML's existence, its row count against the ``.dat``, and the round trip of every
column both files carry.

The executable parses options at import, so the writers are extracted by AST --
the same technique as test_jax_template_finalization.py, and for the same
reason: it exercises the real implementation rather than a copy of it.
"""
import ast
import math
from pathlib import Path
import sys
from types import SimpleNamespace

import lal
import numpy as np
import pytest

from igwn_ligolw import ligolw, lsctables, utils as ligolw_utils

from RIFT.likelihood import factored_likelihood


DRIVER = (Path(__file__).resolve().parents[1] /
          "bin" / "integrate_likelihood_extrinsic_jax")

# The export path and everything it calls, plus the module constants those read.
# Named explicitly rather than taken wholesale: the driver has functions whose
# DEFAULT ARGUMENTS reference module state, and a default is evaluated at `def`
# time, so exec-ing the whole file's functions fails on names this test has no
# reason to supply.
_WANTED_FUNCS = ("dat_path", "samples_path", "xml_path", "_remove_stale_artifact",
                 "fairdraw_indices", "fairdraw_size", "_target_ess_was_given",
                 "was_supplied", "_xml_extrinsic_columns", "write_samples_xml",
                 "write_samples")
_WANTED_ASSIGNS = ("_TEMPERED_MODES", "_FAIRDRAW_MODES", "_FAIRDRAW_N_MAX_DEFAULT")


def _driver_namespace():
    """The driver's real export functions, in a namespace that can run them."""
    source = DRIVER.read_text()
    tree = ast.parse(source, filename=str(DRIVER))
    body = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in _WANTED_FUNCS:
            body.append(node)
        elif isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id in _WANTED_ASSIGNS
                for t in node.targets):
            body.append(node)
    found = {n.name for n in body if isinstance(n, ast.FunctionDef)}
    missing = set(_WANTED_FUNCS) - found
    assert not missing, (
        "the driver no longer defines %s; this test would silently exercise "
        "less than it claims" % sorted(missing))
    module = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {"np": np, "sys": sys, "os": __import__("os"),
                 "MSUN": lal.MSUN_SI, "PC": lal.PC_SI}
    exec(compile(module, str(DRIVER), "exec"), namespace)
    return namespace


NS = _driver_namespace()


def _P(m1=1.5, m2=1.3):
    """A minimal template with the intrinsic attributes the export reads."""
    return SimpleNamespace(
        m1=m1 * lal.MSUN_SI, m2=m2 * lal.MSUN_SI,
        s1x=0.0, s1y=0.0, s1z=0.11, s2x=0.0, s2y=0.0, s2z=-0.07,
        lambda1=300.0, lambda2=250.0, eccentricity=0.0, meanPerAno=0.0,
        a6c=0.0, E0=0.0, p_phi0=0.0)


def _opts(tmp_path, mode="laplace-is", **updates):
    values = dict(
        output_file=str(tmp_path / "EXTR_out-0.xml"), save_samples=True,
        mode=mode, seed=7, phase_marginalization=False,
        fairdraw_extrinsic_output=False, fairdraw_extrinsic_output_n_max=None,
        n_fairdraw_extrinsic_samples=None, n_eff=100,
        target_export_ess_frac=None, _supplied_options=set())
    values.update(updates)
    return SimpleNamespace(**values)


def _theta(n, ndim, rng):
    """A physical extrinsic cloud in the driver's column order."""
    cols = [rng.uniform(0.0, 2 * np.pi, n),              # ra
            np.arcsin(rng.uniform(-1.0, 1.0, n)),        # dec
            rng.uniform(0.0, np.pi, n),                  # psi
            np.arccos(rng.uniform(-1.0, 1.0, n)),        # incl
            rng.uniform(0.0, 2 * np.pi, n),              # phiref
            rng.uniform(50.0, 500.0, n)]                 # distance
    if ndim == 3:        # flowmc-phipsimarg: ra, dec, incl
        return np.column_stack([cols[0], cols[1], cols[3]])
    if ndim == 4:        # phimarg: ra, dec, psi, incl -- dpsimarg reorders below
        return np.column_stack(cols[:4])
    return np.column_stack(cols[:ndim])


def _run(opts, theta, lnL, with_distance, epoch=1126259462.0,
         logZ=41.25, sigma_lnL=0.031, neff=88.5, ntotal=4096):
    NS["write_samples"](opts, 0, theta, lnL, with_distance, P=_P(),
                        fiducial_epoch=epoch, logZ=logZ, sigma_lnL=sigma_lnL,
                        neff=neff, ntotal=ntotal)
    return (Path(NS["samples_path"](opts, 0)), Path(NS["xml_path"](opts, 0)))


def _load(xml):
    return ligolw_utils.load_filename(
        str(xml), contenthandler=ligolw.LIGOLWContentHandler)


def _sim_rows(xml):
    return lsctables.SimInspiralTable.get_table(_load(xml))


def _dat(path):
    return np.atleast_1d(np.genfromtxt(str(path), names=True))


# ---------------------------------------------------------------------------
# The file the pipeline reads
# ---------------------------------------------------------------------------
def test_xml_name_is_the_one_convert_extr_opens(tmp_path):
    opts = _opts(tmp_path)
    assert NS["xml_path"](opts, 3).endswith("EXTR_out-0.xml_3_.xml.gz")


def test_one_xml_per_event_alongside_the_unchanged_dat(tmp_path):
    rng = np.random.default_rng(0)
    opts = _opts(tmp_path)
    theta = _theta(64, 6, rng)
    lnL = rng.normal(40.0, 1.0, 64)
    dat, xml = _run(opts, theta, lnL, with_distance=True)
    assert dat.is_file() and xml.is_file()
    # The sidecar keeps its exact columns: nothing that reads it may change.
    assert list(_dat(dat).dtype.names) == [
        "right_ascension", "declination", "distance", "inclination", "psi",
        "phi_orb", "loglikelihood"]


def test_xml_row_count_matches_the_dat(tmp_path):
    rng = np.random.default_rng(1)
    opts = _opts(tmp_path)
    theta = _theta(37, 6, rng)
    lnL = rng.normal(40.0, 1.0, 37)
    dat, xml = _run(opts, theta, lnL, with_distance=True)
    assert len(_sim_rows(xml)) == len(_dat(dat)) == 37


def test_nonfinite_lnL_rows_are_dropped_from_both_products(tmp_path):
    rng = np.random.default_rng(2)
    opts = _opts(tmp_path)
    theta = _theta(20, 6, rng)
    lnL = rng.normal(40.0, 1.0, 20)
    lnL[[3, 11]] = np.nan
    dat, xml = _run(opts, theta, lnL, with_distance=True)
    assert len(_sim_rows(xml)) == len(_dat(dat)) == 18


def test_no_xml_without_save_samples(tmp_path):
    rng = np.random.default_rng(3)
    opts = _opts(tmp_path, save_samples=False)
    NS["write_samples"](opts, 0, _theta(8, 6, rng), np.zeros(8), True, P=_P(),
                        fiducial_epoch=0.0, logZ=1.0, sigma_lnL=0.1)
    assert not Path(NS["xml_path"](opts, 0)).exists()
    assert not Path(NS["samples_path"](opts, 0)).exists()


# ---------------------------------------------------------------------------
# Round trip: the XML columns must be the .dat columns
# ---------------------------------------------------------------------------
def test_angles_and_distance_round_trip_to_the_dat(tmp_path):
    rng = np.random.default_rng(4)
    opts = _opts(tmp_path)
    theta = _theta(50, 6, rng)
    lnL = rng.normal(40.0, 2.0, 50)
    dat, xml = _run(opts, theta, lnL, with_distance=True)
    tab, ref = _sim_rows(xml), _dat(dat)
    got = {name: np.array([getattr(r, name) for r in tab])
           for name in ("longitude", "latitude", "inclination", "polarization",
                        "coa_phase", "distance", "alpha1")}
    np.testing.assert_allclose(got["longitude"], ref["right_ascension"], rtol=0, atol=1e-6)
    np.testing.assert_allclose(got["latitude"], ref["declination"], rtol=0, atol=1e-6)
    np.testing.assert_allclose(got["inclination"], ref["inclination"], rtol=0, atol=1e-6)
    np.testing.assert_allclose(got["polarization"], ref["psi"], rtol=0, atol=1e-6)
    np.testing.assert_allclose(got["coa_phase"], ref["phi_orb"], rtol=0, atol=1e-6)
    np.testing.assert_allclose(got["distance"], ref["distance"], rtol=0, atol=1e-4)
    # lnL rides alpha1, as it does in batchmode's export.
    np.testing.assert_allclose(got["alpha1"], ref["loglikelihood"], rtol=0, atol=1e-5)


def test_time_is_the_fiducial_epoch_on_every_row(tmp_path):
    rng = np.random.default_rng(5)
    opts = _opts(tmp_path)
    epoch = 1187008882.43
    _, xml = _run(opts, _theta(12, 6, rng), np.full(12, 30.0), True, epoch=epoch)
    for row in _sim_rows(xml):
        t = row.geocent_end_time + 1e-9 * row.geocent_end_time_ns
        assert math.isclose(t, epoch, rel_tol=0, abs_tol=1e-6)


def test_intrinsic_columns_and_evidence_row(tmp_path):
    rng = np.random.default_rng(6)
    opts = _opts(tmp_path)
    _, xml = _run(opts, _theta(9, 6, rng), np.full(9, 12.0), True,
                  logZ=41.25, sigma_lnL=0.031, neff=88.5, ntotal=4096)
    doc = _load(xml)
    for row in lsctables.SimInspiralTable.get_table(doc):
        assert math.isclose(row.mass1, 1.5, rel_tol=1e-9)
        assert math.isclose(row.mass2, 1.3, rel_tol=1e-9)
        assert math.isclose(row.spin1z, 0.11, rel_tol=1e-9)
        assert math.isclose(row.alpha5, 300.0, rel_tol=1e-9)
    sngl = lsctables.SnglInspiralTable.get_table(doc)
    assert len(sngl) == 1
    # append_likelihood_result_to_xmldoc: snr=logZ, tau0=neff, tau3=converged.
    assert math.isclose(sngl[0].snr, 41.25, rel_tol=1e-9)
    assert math.isclose(sngl[0].tau0, 88.5, rel_tol=1e-9)
    assert math.isclose(sngl[0].mass1, 1.5, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# Every --mode layout, or an explicit refusal
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("mode,ndim", [
    ("flowmc-phimarg", 4),
    ("nuts-phimarg", 4),
    ("flowmc-dpsimarg", 4),
    ("flowmc-phipsimarg", 3),
    ("laplace-is", 5),
])
def test_marginalized_layouts_export_and_agree_with_their_dat(tmp_path, mode, ndim):
    rng = np.random.default_rng(7)
    opts = _opts(tmp_path, mode=mode)
    theta = _theta(16, ndim, rng)
    if mode == "flowmc-dpsimarg":       # ra, dec, phiref, incl
        theta = np.column_stack([theta[:, 0], theta[:, 1], theta[:, 3], theta[:, 2]])
    lnL = rng.normal(20.0, 1.0, 16)
    dat, xml = _run(opts, theta, lnL, with_distance=False)
    tab, ref = _sim_rows(xml), _dat(dat)
    assert len(tab) == len(ref) == 16
    np.testing.assert_allclose([r.longitude for r in tab],
                               ref["right_ascension"], rtol=0, atol=1e-6)
    np.testing.assert_allclose([r.inclination for r in tab],
                               ref["inclination"], rtol=0, atol=1e-6)
    # Distance was marginalized analytically: no per-sample draw exists, so the
    # reference distance the templates were built at is written, exactly as
    # batchmode does.  A silent per-sample value here would be fabricated.
    np.testing.assert_allclose([r.distance for r in tab],
                               factored_likelihood.distMpcRef, rtol=0, atol=1e-9)
    if "psi" in (ref.dtype.names or ()):
        np.testing.assert_allclose([r.polarization for r in tab],
                                   ref["psi"], rtol=0, atol=1e-6)
    else:
        # psi was integrated out: NaN, never a fiducial 0.0 that would read as a
        # polarization measurement downstream.
        assert all(math.isnan(r.polarization) for r in tab)


def test_six_d_distance_is_the_per_sample_draw_not_the_reference(tmp_path):
    rng = np.random.default_rng(8)
    opts = _opts(tmp_path)
    theta = _theta(24, 6, rng)
    _, xml = _run(opts, theta, np.full(24, 15.0), True)
    got = np.array([r.distance for r in _sim_rows(xml)])
    np.testing.assert_allclose(np.sort(got), np.sort(theta[:, 5]), rtol=0, atol=1e-4)
    assert got.std() > 1.0                      # a real posterior, not a constant
    assert not np.allclose(got, factored_likelihood.distMpcRef)


def test_phase_marginalized_writes_the_reference_phase(tmp_path):
    rng = np.random.default_rng(9)
    opts = _opts(tmp_path, phase_marginalization=True)
    theta = _theta(10, 6, rng)
    dat, xml = _run(opts, theta, np.full(10, 15.0), True)
    assert "phi_orb" not in (_dat(dat).dtype.names or ())
    assert all(r.coa_phase == 0.0 for r in _sim_rows(xml))


def test_unmappable_layout_refuses_rather_than_guessing(tmp_path):
    opts = _opts(tmp_path, mode="laplace-is")
    with pytest.raises(RuntimeError, match="no sim_inspiral column mapping"):
        NS["_xml_extrinsic_columns"](opts, np.zeros((4, 7)), False)
    with pytest.raises(RuntimeError, match="not 6"):
        NS["_xml_extrinsic_columns"](opts, np.zeros((4, 5)), True)


def test_missing_provenance_arguments_refuse_rather_than_skip_the_xml(tmp_path):
    rng = np.random.default_rng(10)
    opts = _opts(tmp_path)
    with pytest.raises(RuntimeError, match="sim_inspiral XML"):
        NS["write_samples"](opts, 0, _theta(4, 6, rng), np.zeros(4), True)
    assert not Path(NS["xml_path"](opts, 0)).exists()


def test_prior_columns_are_unity_so_the_resampler_gets_finite_weights(tmp_path):
    """alpha2/alpha3 reach util_ResampleILEOutputWithExtrinsic.py as p and ps.

    convert_output_format_ile2inference copies them into the 'p'/'ps' columns and
    the resampler forms exp(lnL - lnLmax) * (p/ps) / Npts, so zeros -- batchmode's
    placeholder, which its sampler record overwrites through the joint_prior /
    joint_s_prior CMAP keys -- would give every row a nan weight.  1.0/1.0 is what
    util_ConvertJAXILEFairdraws.py already writes for these same rows.
    """
    rng = np.random.default_rng(11)
    opts = _opts(tmp_path)
    _, xml = _run(opts, _theta(15, 6, rng), rng.normal(30.0, 1.0, 15), True)
    tab = _sim_rows(xml)
    p = np.array([r.alpha2 for r in tab])
    ps = np.array([r.alpha3 for r in tab])
    np.testing.assert_allclose(p, 1.0, rtol=0, atol=0)
    np.testing.assert_allclose(ps, 1.0, rtol=0, atol=0)
    like = np.array([r.alpha1 for r in tab])
    assert np.all(np.isfinite(np.exp(like - like.max()) * (p / ps)))
