"""Known-answer mixtures, hostile contracts and the existing-run CLI."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import numpy as np
import pytest
from scipy.special import logsumexp

CODE = Path(__file__).resolve().parents[1]
# Keep numerical tests independent of RIFT's optional waveform imports.
spec = importlib.util.spec_from_file_location("compression", CODE/"RIFT/compression/__init__.py", submodule_search_locations=[str(CODE/"RIFT/compression")])
c = importlib.util.module_from_spec(spec)
sys.modules["compression"] = c
spec.loader.exec_module(c)
from compression.config import resolve
from compression.cli import run
from compression.model import CompressedFit, fit_posterior


def configuration(tmp_path, distance="Euclidean", cosmology="Planck15"):
    ini = tmp_path/"production.ini"
    ini.write_text(f"""[engine]
chirpmass-min = 10
chirpmass-max = 60
q-min = .1
q-max = 1
distance-min = 100
distance-max = 2500
cosmology = {cosmology}
[rift-pseudo-pipe]
assume-precessing = True
ile-distance-prior = {distance}
spin-magnitude-prior = volumetric
[rift-auto-fit]
enabled = true
targets = aligned_distance, precessing_source_redshift
component_counts = 1,2
weight_column = posterior_weight
group_column = intrinsic_group
validation_ks = .08
""")
    (tmp_path/"args_ile.txt").write_text(f"X --d-min 100 --d-max 2500 --d-prior {distance} --reference-freq 20")
    (tmp_path/"args_cip_list.txt").write_text("2 X --parameter mc --parameter eta --parameter s1x --parameter s1y --parameter s1z --parameter s2x --parameter s2y --parameter s2z --mc-range [10,60] --eta-range [.08,.25] --chi-max .99")
    return ini


def known_likelihood(n=18000, dimension=9, seed=91):
    """Draw the known likelihood proposal, not the fitted posterior model."""
    rng = np.random.default_rng(seed)
    mean = np.array([30., .5, .12, -.1, .2, -.1, .15, -.15, 1000.])
    widths = np.array([1.5, .025, .04, .04, .05, .04, .04, .05, 80.])
    if dimension == 5:
        ix = [0, 1, 4, 7, 8]
        mean, widths = mean[ix], widths[ix]
    d = len(mean)
    correlation = np.eye(d)
    correlation[0, -1] = correlation[-1, 0] = .45
    correlation[1, 2] = correlation[2, 1] = -.25
    cov = correlation*widths[:, None]*widths[None, :]
    means = np.stack([mean.copy(), mean.copy()])
    means[1, 0] += 5.
    means[1, -1] += 160.
    means[1, 2] -= .12
    labels = rng.choice(2, n, p=[.65, .35])
    chart = means[labels]+rng.multivariate_normal(np.zeros(d), cov, n)
    return chart, means, cov


def truth_logpdf(x, means, cov):
    from scipy.stats import multivariate_normal
    return logsumexp(np.stack([np.log(w)+multivariate_normal.logpdf(x, m, cov) for w, m in zip([.65, .35], means)]), axis=0)


@pytest.mark.parametrize("dimension,distance_prior", [(5, "Euclidean"), (9, "Euclidean"), (5, "cosmo_sourceframe")])
def test_known_gmm_likelihood_and_source_frames(tmp_path, dimension, distance_prior):
    ini = configuration(tmp_path, distance_prior)
    _, contract, _ = resolve(ini, tmp_path)
    target = "aligned_distance" if dimension == 5 else "precessing_source_redshift"
    x, means, cov = known_likelihood(dimension=dimension)
    template = dict(schema="rift-posterior-compression-v1", target=target, contract=contract,
        cosmology_yaml=c.CosmologyContext().serialized,
        mixture=dict(weights=[1.], means=[means[0].tolist()], covariances=[cov.tolist()]))
    prior = CompressedFit(template)
    w = np.exp(prior.log_native_prior_chart(x)-np.max(prior.log_native_prior_chart(x)))
    points = x.copy()
    points[:, 1] = (1-x[:, 1]**2)/4
    model, report = fit_posterior(points, w, np.arange(len(w)), contract, target, components=[1, 2], seed=33, maxiter=300)
    assert report["status"] == "pass_native_recovery", report
    probes, _, _ = known_likelihood(4000, dimension, seed=123456)
    physical = probes.copy()
    physical[:, 1] = (1-probes[:, 1]**2)/4
    estimated = model.log_likelihood(physical, mass_frame="detector", radial_coordinate="luminosity_distance")
    exact = truth_logpdf(probes, means, cov)
    residual = estimated-exact
    residual -= np.median(residual)
    assert np.sqrt(np.mean(residual**2)) < .25
    for cosmology in ("Planck15", "Planck18"):
        ctx = c.CosmologyContext(cosmology, 2500)
        source = physical.copy()
        z = ctx.redshift(source[:, -1])
        source[:, 0] /= 1+z
        source[:, -1] = z
        np.testing.assert_allclose(model.log_likelihood(source, mass_frame="source", radial_coordinate="redshift", cosmology=ctx), estimated, atol=1e-6)
    # Counterexample: fitting one broad component must fail the declared shape gate.
    _, bad_report = fit_posterior(points, w, np.arange(len(w)), contract, target, components=[1], seed=33, tolerance=.025)
    assert bad_report["status"] == "failed_validation"


def test_inheritance_and_conflicts(tmp_path):
    ini = configuration(tmp_path, "cosmo_sourceframe")
    _, contract, _ = resolve(ini, tmp_path)
    assert contract["cosmology"] == "Planck15"
    assert contract["distance_prior"] == "cosmo_sourceframe"
    ini.write_text(ini.read_text()+"distance_prior = Euclidean\n")
    with pytest.raises(ValueError, match="conflicts"):
        resolve(ini, tmp_path)


def test_wrong_cosmology_and_custom_priors_refused(tmp_path):
    ini = configuration(tmp_path, "cosmo_sourceframe", "Planck18")
    with pytest.raises(ValueError, match="Planck15"):
        resolve(ini, tmp_path)


@pytest.mark.parametrize("coordinates,extra,prior", [
    ("--parameter s1z --parameter s2z", "", "uniform_aligned"),
    ("--parameter s1z --parameter s2z", "--aligned-prior alignedspin-zprior", "zprior_aligned"),
    ("--parameter chi1 --parameter chi2 --parameter cos_theta1 --parameter cos_theta2 --parameter phi1 --parameter phi2", "", "uniform_mag_prec"),
    ("--parameter s1z_bar --parameter s2z_bar --parameter chi1_perp_u --parameter chi2_perp_u --parameter phi1 --parameter phi2", "--prior-in-integrand-correction uniform_over_rbar_singular", "uniform_mag_prec"),
])
def test_actual_sampling_spin_priors(tmp_path, coordinates, extra, prior):
    ini = configuration(tmp_path)
    (tmp_path/"args_cip_list.txt").write_text("1 X --parameter mc --parameter eta "+coordinates+" "+extra)
    requested = {"uniform_aligned":"volumetric", "zprior_aligned":"zprior_aligned", "uniform_mag_prec":"uniform_mag_prec"}[prior]
    ini.write_text(ini.read_text().replace("spin-magnitude-prior = volumetric", 'spin-magnitude-prior = "'+requested+'"'))
    if prior in ("uniform_aligned", "zprior_aligned"):
        ini.write_text(ini.read_text().replace("assume-precessing = True", "assume-precessing = False"))
    _, contract, _ = resolve(ini, tmp_path)
    assert contract["spin_prior"] == prior


def test_native_mass_support_and_missing_records(tmp_path):
    ini = configuration(tmp_path)
    ini.write_text(ini.read_text().replace("assume-precessing = True", "assume-precessing = False"))
    (tmp_path/"args_cip_list.txt").write_text("1 X --parameter mtot --parameter q --mtot-range [40,80] --parameter s1z --parameter s2z")
    _, contract, _ = resolve(ini, tmp_path)
    assert contract["cuts"]["mtot"] == [40, 80]
    (tmp_path/"args_cip_list.txt").write_text("1 X --parameter m1 --parameter m2 --parameter s1z --parameter s2z")
    assert resolve(ini, tmp_path)[1]["cuts"]["m2"] == [.9,1000]
    (tmp_path/"args_ile.txt").unlink()
    with pytest.raises(ValueError, match="executed"):
        resolve(ini, tmp_path)


def test_real_ile_pin_and_reference_frequency(tmp_path):
    ini = configuration(tmp_path)
    with (tmp_path/"args_ile.txt").open("a") as stream:
        stream.write(" --reference-freq 43")
    assert resolve(ini, tmp_path)[1]["fref"] == 43
    with (tmp_path/"args_ile.txt").open("a") as stream:
        stream.write(" --pin-distance-to-sim")
    with pytest.raises(ValueError, match="Pinned"):
        resolve(ini, tmp_path)
    ini = configuration(tmp_path)
    with (tmp_path/"args_cip_list.txt").open("a") as f:
        f.write(" --supplementary-prior-code unknown")
    with pytest.raises(ValueError, match="Unsupported retained-prior"):
        resolve(ini, tmp_path)


def test_nonflat_cosmology_derivative():
    from astropy.cosmology import LambdaCDM
    ctx = c.CosmologyContext(LambdaCDM(H0=71, Om0=.25, Ode0=.6, Tcmb0=2.7), 15000)
    z = np.linspace(.01, 1, 25)
    numerical = (ctx.cosmology.luminosity_distance(z+1e-5).value-ctx.cosmology.luminosity_distance(z-1e-5).value)/2e-5
    np.testing.assert_allclose(ctx.distance_derivative(z), numerical, rtol=2e-6)
    np.testing.assert_allclose(ctx.redshift(ctx.luminosity_distance(z)), z, atol=1e-8)


def test_native_distance_prior_matches_rift(tmp_path):
    env = dict(os.environ, PYTHONPATH=str(CODE)+os.pathsep+os.environ.get("PYTHONPATH", ""))
    script = """
import numpy as np
import importlib.util
from pathlib import Path
import RIFT
from RIFT.compression import CosmologyContext
spec = importlib.util.spec_from_file_location('distance_prior_reference', Path(RIFT.__file__).parent/'likelihood/jax_ile/distance_prior.py')
reference_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reference_module)
cosmo_distance_prior = reference_module.cosmo_distance_prior
from RIFT.likelihood.priors_utils import dist_prior_pseudo_cosmo
context = CosmologyContext('Planck15', 10000)
distance = np.geomspace(1., 9000., 100)
for kind in ('cosmo', 'cosmo_sourceframe'):
    actual = context.distance_logprior(distance, kind)
    reference = cosmo_distance_prior(kind).log_density_unnormalized(distance)
    np.testing.assert_allclose(actual-actual[0], reference-reference[0], atol=2e-5)
actual = context.distance_logprior(distance, 'pseudo_cosmo')
reference = np.log(dist_prior_pseudo_cosmo(distance))
np.testing.assert_allclose(actual-actual[0], reference-reference[0], atol=1e-12)
"""
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env, timeout=120)
    assert proc.returncode == 0, proc.stdout+proc.stderr


def test_fake_ile_existing_run_cli(tmp_path):
    ini = configuration(tmp_path)
    fake = CODE/"demo/auto_fit/fake_ile.py"
    proc = subprocess.run([sys.executable, str(fake), "--output", str(tmp_path/"extrinsic_posterior_samples.dat"), "--rows", "18000"], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    result = run(ini, tmp_path)
    assert result["status"] == "complete", result
    for target in ("aligned_distance", "precessing_source_redshift"):
        loaded = CompressedFit.load(tmp_path/"auto_fit"/target/"model.json")
        assert loaded.dimension == (5 if target == "aligned_distance" else 9)
    probes, means, covariance = known_likelihood(4000, 9, seed=37372)
    points = probes.copy()
    points[:, 1] = (1-probes[:, 1]**2)/4
    recovered = loaded.log_likelihood(points, mass_frame="detector", radial_coordinate="luminosity_distance")
    residual = recovered-truth_logpdf(probes, means, covariance)
    residual -= np.median(residual)
    rms = float(np.sqrt(np.mean(residual**2)))
    assert rms < .3
    (tmp_path/"known_answer_cli.json").write_text(json.dumps(dict(log_likelihood_shape_rms=rms,
        maximum_ks={k:v["maximum_ks"] for k,v in result["reports"].items()})))
    env = dict(os.environ, PYTHONPATH=str(CODE)+os.pathsep+os.environ.get("PYTHONPATH", ""))
    proc = subprocess.run([sys.executable, str(CODE/"bin/util_RIFT_compress_posterior.py"),
        "--config", str(ini), "--rundir", str(tmp_path), "--output", "standalone"],
        capture_output=True, text=True, env=env, timeout=120)
    assert proc.returncode == 0, proc.stdout+proc.stderr
    assert json.loads((tmp_path/"standalone/manifest.json").read_text())["status"] == "complete"
    with pytest.raises(FileExistsError):
        run(ini, tmp_path)
    # An already exported redshift from a different cosmology must not be trusted.
    table = tmp_path/"extrinsic_posterior_samples.dat"
    text = table.read_text().splitlines()
    table.write_text(text[0]+" redshift\n"+"\n".join(t+" 2.0" for t in text[1:])+"\n")
    with pytest.raises(ValueError, match="cosmology"):
        run(ini, tmp_path, output="wrong_cosmology")
    assert json.loads((tmp_path/"wrong_cosmology/manifest.json").read_text())["status"] == "failed"


def test_group_information_cannot_be_manufactured(tmp_path):
    ini = configuration(tmp_path)
    _, contract, _ = resolve(ini, tmp_path)
    x, _, _ = known_likelihood(2000, 5)
    x[:, 1] = (1-x[:, 1]**2)/4
    with pytest.raises(ValueError, match="100 original"):
        fit_posterior(x, np.ones(len(x)), np.zeros(len(x)), contract, "aligned_distance")


def test_last_cip_binding_rejects_stale_or_incomplete_pairs(tmp_path):
    from compression.cli import read_posterior, bind_final_cip
    fake = CODE/"demo/auto_fit/fake_ile.py"
    subprocess.run([sys.executable, str(fake), "--output", str(tmp_path/"paired.dat"), "--rows", "3000"], check=True)
    aligned, full, weights, _, _ = read_posterior(tmp_path/"paired.dat")
    final = tmp_path/"posterior_samples-1.dat"
    adjusted, groups, binding = bind_final_cip(aligned, full, weights, final)
    assert np.isclose(adjusted.sum(), 1)
    assert binding["rows"] == 3000
    stale = full.copy()
    stale[:, 0] += 10
    with pytest.raises(ValueError, match="final CIP"):
        bind_final_cip(aligned, stale, weights, final)
    with pytest.raises(ValueError, match="lack paired"):
        bind_final_cip(aligned[:10], full[:10], weights[:10], final)


@pytest.mark.parametrize("extra", ["--no-downselect", "--trust-sample-parameter-box"])
def test_unresolved_native_support_refused(tmp_path, extra):
    ini = configuration(tmp_path)
    with (tmp_path/"args_cip_list.txt").open("a") as stream:
        stream.write(" "+extra)
    with pytest.raises(ValueError, match="support"):
        resolve(ini, tmp_path)


def test_inactive_ranges_and_partial_spins(tmp_path):
    ini = configuration(tmp_path)
    ini.write_text(ini.read_text().replace("assume-precessing = True", "assume-precessing = False"))
    cip = tmp_path/"args_cip_list.txt"
    cip.write_text("1 X --parameter m1 --parameter m2 --mc-range [10, 60] --parameter s1z --parameter s2z")
    contract = resolve(ini, tmp_path)[1]
    assert contract["bounds"]["mc"][1] > 800
    assert contract["bounds"]["eta"][0] < .01
    cip.write_text("1 X --parameter mtot --parameter q --mtot-range [40, 800] --parameter s1z --parameter s2z")
    contract = resolve(ini, tmp_path)[1]
    assert contract["bounds"]["mc"][1] > 300
    cip.write_text("1 X --parameter mc --parameter eta --parameter s1x --parameter s1y --parameter s1z --parameter s2z")
    with pytest.raises(ValueError, match="incomplete"):
        resolve(ini, tmp_path)


def test_alternate_sampling_flag_alone_does_not_change_prior(tmp_path):
    ini = configuration(tmp_path)
    with (tmp_path/"args_cip_list.txt").open("a") as stream:
        stream.write(" --pseudo-uniform-magnitude-prior-alternate-sampling")
    assert resolve(ini, tmp_path)[1]["spin_prior"] == "volumetric"


def test_framework_default_boolean_and_overwritten_cuts(tmp_path):
    ini = configuration(tmp_path)
    ini.write_text(ini.read_text().replace("spin-magnitude-prior = volumetric", 'spin-magnitude-prior = "default"').replace("assume-precessing = True", "assume-precessing ="))
    with (tmp_path/"args_cip_list.txt").open("a") as stream:
        stream.write(" --downselect-parameter s1z --downselect-parameter-range [-.1,.1]")
    contract = resolve(ini, tmp_path)[1]
    assert contract["spin_prior"] == "volumetric"
    assert contract["cuts"]["s1z"][1] > .9
    ini.write_text(ini.read_text().replace("assume-precessing =\n", "assume-precessing = False\n"))
    (tmp_path/"args_cip_list.txt").write_text("1 X --parameter mc --parameter eta --parameter s1z --parameter s2z")
    assert resolve(ini, tmp_path)[1]["spin_prior"] == "uniform_aligned"


def test_reverse_spin_caps_respect_native_cartesian_clip(tmp_path):
    ini = configuration(tmp_path)
    with (tmp_path/"args_cip_list.txt").open("a") as stream:
        stream.write(" --chi-max .5 --chi-small-max .9")
    with pytest.raises(ValueError, match="anisotropic"):
        resolve(ini, tmp_path)
    ini.write_text(ini.read_text().replace("assume-precessing = True", "assume-precessing = False"))
    (tmp_path/"args_cip_list.txt").write_text("1 X --parameter mc --parameter eta --parameter s1z --parameter s2z --chi-max .5 --chi-small-max .9")
    assert resolve(ini, tmp_path)[1]["cuts"]["s2z"] == [-.5,.5]


def test_real_pseudo_pipe_terminal_job(tmp_path):
    pytest.importorskip("lal")
    from test_rf_transverse_pipeline_build import _build, _opt_in_ini
    ini = _opt_in_ini(tmp_path)
    ini.write_text(ini.read_text()+"\n[rift-auto-fit]\nenabled = true\ncomponent_counts = 1,2\n")
    proc, rundir = _build(tmp_path, ini)
    assert proc.returncode == 0, proc.stdout[-6000:]
    submit = (rundir/"AUTO_FIT.sub").read_text()
    assert "universe = local" in submit.lower()
    assert "auto_fit.sh" in submit
    assert (rundir/"auto_fit.production.ini").read_text() == ini.read_text()
    wrapper = (rundir/"auto_fit.sh").read_text()
    assert "--intrinsic-posterior overlap-grid-6.xml.gz" in wrapper
    from compression.cli import read_posterior
    intrinsic, full, _, _, _ = read_posterior(rundir/"target_params.xml.gz", intrinsic_only=True)
    assert len(intrinsic) > 0 and full is not None
    dag = (rundir/"marginalize_intrinsic_parameters_BasicIterationWorkflow.dag").read_text()
    assert "AUTO_FIT.sub" in dag
    lines = dag.splitlines()
    job = next(line.split()[1] for line in lines if line.startswith("JOB ") and "AUTO_FIT.sub" in line)
    assert any(line.startswith("PARENT ") and " CHILD "+job in line for line in lines)
