"""Joint source/lens likelihood rows for native Hyperpipe.

Every sampled coordinate is a unit-prior quantile.  Thus EOS_POST's unit
prior on [0,1]^d is normalized, including when the physical prior is not
uniform. Factories return log likelihood *ratios* with one common noise
reference for lensed and unlensed runs. No maximization is performed here.
"""
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
import re
import shlex

import numpy as np

from .base import MargDriverBase


def validate_manifest(manifest):
    if manifest.get("model") not in ("lensed", "unlensed"):
        raise ValueError("model must be lensed or unlensed")
    if not re.fullmatch(r"[\w.]+:[\w]+", manifest.get("factory", "")):
        raise ValueError("factory must be importable module:function")
    coordinates = manifest.get("coordinates", {})
    if not coordinates:
        raise ValueError("at least one sampled coordinate is required")
    for name, spec in coordinates.items():
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
            raise ValueError("invalid coordinate name: " + name)
        lo, hi = float(spec["min"]), float(spec["max"])
        prior = spec.get("prior", "uniform")
        if not np.isfinite([lo, hi]).all() or hi <= lo:
            raise ValueError("finite increasing prior bounds required: " + name)
        if prior not in ("uniform", "log_uniform", "isotropic"):
            raise ValueError("unsupported prior: " + prior)
        if prior == "log_uniform" and lo <= 0:
            raise ValueError("positive log_uniform bounds required")
        if prior == "isotropic" and not (0 <= lo < hi <= np.pi):
            raise ValueError("isotropic angle bounds must lie in [0,pi]")
    if set(coordinates) & set(manifest.get("fixed", {})):
        raise ValueError("fixed and sampled coordinates must be disjoint")
    return manifest


def physical_parameters(quantiles, manifest):
    expected = {"u_" + name for name in manifest["coordinates"]}
    if set(quantiles) != expected:
        raise ValueError("grid columns must exactly match prior quantiles: " + repr(sorted(expected)))
    parameters = dict(manifest.get("fixed", {}))
    for name, spec in manifest["coordinates"].items():
        u = float(quantiles["u_" + name])
        if not np.isfinite(u) or not 0 <= u <= 1:
            raise ValueError("quantile outside [0,1]: " + name)
        lo, hi = float(spec["min"]), float(spec["max"])
        prior = spec.get("prior", "uniform")
        if prior == "uniform":
            value = lo + u * (hi - lo)
        elif prior == "log_uniform":
            value = np.exp(np.log(lo) + u * np.log(hi / lo))
        else:
            value = np.arccos(np.clip(np.cos(lo) - u * (np.cos(lo) - np.cos(hi)), -1, 1))
        parameters[name] = float(value)
    return parameters


class JointLensingDriver(MargDriverBase):
    description = __doc__

    def add_arguments(self, parser):
        parser.add_argument("--joint-config", required=True)

    def log_likelihood(self, row_values, column_names, opts):
        path = str(Path(opts.joint_config).resolve())
        if getattr(self, "_config_path", None) != path:
            manifest = validate_manifest(json.loads(Path(path).read_text()))
            module, function = manifest["factory"].split(":")
            self._likelihood = getattr(importlib.import_module(module), function)(manifest.get("factory_config", {}))
            self._manifest = manifest
            self._config_path = path
        p = physical_parameters(dict(zip(column_names, map(float, row_values))), self._manifest)
        value = float(self._likelihood(p, self._manifest["model"]))
        if not np.isfinite(value):
            raise ValueError("factory returned nonfinite log likelihood ratio")
        return value, 0.0


def static_network_factory(config):
    """Optional physical adapter to the owning lensing_prototype package.

    Uses the negative-Fourier point lens transfer at every retained frequency;
    amplitude and time_shift are sampled, never profiled. This direct physical
    polarization adapter does not invoke ILE or claim a NoLoop evaluation.
    """
    from lensing_prototype.static_event import StaticNetwork
    from lensing_prototype.point_lens import point_lens_transfer
    network = StaticNetwork(config["data_directory"], clean=config.get("clean", True),
                            maximum_frequency=config.get("maximum_frequency", 448))

    def likelihood(parameters, model):
        p = dict(parameters)
        if "luminosity_distance" in p:
            raise ValueError("sample amplitude, not ignored luminosity_distance at the fixed 1000-Mpc reference")
        amplitude = p.pop("amplitude")
        time_shift = p.pop("time_shift")
        mass = p.pop("MLz", None)
        y = p.pop("y", None)
        if amplitude < 0:
            raise ValueError("amplitude must be nonnegative")
        transfer = None
        if model == "lensed":
            if mass is None or y is None or mass <= 0 or y <= 0:
                raise ValueError("lensed model requires positive MLz and y")
            transfer = np.ones(len(network.f), complex)
            # Frequencies outside the likelihood mask have zero weight.
            transfer[network.mask] = point_lens_transfer(network.f[network.mask], mass, y)
        return network.evaluate(network.strain(p), transfer=transfer,
                                amplitude=amplitude, time_shift=time_shift)
    return likelihood


def build_workflow(manifest, outdir, n_seed=1024, seed=1234):
    """Create (without submitting) native YAML, unit-prior grid and manifest.

    Output directories must be new. Absolute paths support Condor's changed cwd;
    factory package and prepared data must be available on every execute node.
    """
    validate_manifest(manifest)
    if n_seed < 2:
        raise ValueError("n_seed must be at least two")
    outdir = Path(outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=False)
    config_path = outdir / "joint.json"
    config_path.write_text(json.dumps(manifest, indent=2) + "\n")
    names = ["u_" + n for n in manifest["coordinates"]]
    grid = np.column_stack([np.zeros((n_seed, 2)), np.random.default_rng(seed).random((n_seed, len(names)))])
    np.savetxt(outdir / "seed.dat", grid, header="lnL sigma_lnL " + " ".join(names))
    # JSON is valid YAML and avoids an extra serialization dependency.
    config = {
        "arch": {"method": "default", "n-iterations": 4, "n-samples-per-job": n_seed, "explode-marg-jobs": 1, "explode-marg-jobs-last": 1},
        "post": {"extra-args": "--internal-use-lnL", "coords-fit": " ".join(names), "coords-sample": " ".join(n + ":[0,1]" for n in names),
                 "settings": {"n-max": 1000000, "n-eff": 2000, "fit-method": "rf", "sampler-method": "AV"}},
        "marg-list": [{"name": "joint_" + manifest["model"], "exe": "util_HyperpipeJointLensing.py",
                       "args": "--joint-config " + shlex.quote(str(config_path)) + " --conforming-output-name", "n-chunk": 32}],
        "puff": {"puff-factor": 0.5, "force-away": 0.03},
        "init": {"file": str(outdir / "seed.dat")},
        "general": {"rundir": str(outdir / "run"), "request-memory": 4096}}
    yaml_path = outdir / "hyperpipe.yaml"
    yaml_path.write_text(json.dumps(config, indent=2) + "\n")
    return yaml_path



def collect_result(workflow_path):
    """Collect a completed final single-worker EOS_POST product, fail closed.

    This verifies artifacts, not interpolation/convergence accuracy. Native
    JOIN_POST joins samples only and must never be used as an evidence file.
    """
    workflow = json.loads(Path(workflow_path).read_text())
    arch = workflow["arch"]
    if arch.get("explode-marg-jobs", 1) != 1 or arch.get("explode-marg-jobs-last", 1) != 1:
        raise ValueError("evidence collection supports exactly one final worker")
    n = int(arch["n-iterations"])
    if n < 1:
        raise ValueError("positive expected n-iterations required")
    # The scoped builder samples the complete normalized unit box. Refuse a
    # different coordinate measure rather than report an unnormalized result.
    tokens = workflow["post"]["coords-sample"].split()
    matches = [re.fullmatch(r"([A-Za-z][A-Za-z0-9_]*):\[0(?:\.0*)?,1(?:\.0*)?\]", t) for t in tokens]
    if not tokens or not all(matches):
        raise ValueError("collector requires explicit unit-box sampling coordinates")
    names = [m.group(1) for m in matches]
    if len(names) != len(set(names)):
        raise ValueError("duplicate sampling coordinates")
    expected_columns = ["lnL", "sigma_lnL"] + names
    run = Path(workflow["general"]["rundir"])
    files = [p for p in (run / ("iteration_%d_post" % (n-1))).glob("output-%d-*" % n)
             if p.is_file() and p.suffix == ""]
    if len(files) != 1:
        raise ValueError("expected exactly one final worker integral, got " + str(files))
    integral = files[0]
    value = np.loadtxt(integral, ndmin=1)
    if value.size != 1 or not np.isfinite(value).all():
        raise ValueError("final evidence must contain one finite absolute lnZ")
    samples = Path(str(integral) + ".dat")
    grid = run / ("grid-%d.dat" % n)
    for p in (samples, grid, run / "all.marg_net"):
        with p.open() as f:
            header = f.readline().strip()
        if not header.startswith("#") or header.lstrip("#").split() != expected_columns:
            raise ValueError("unexpected joint coordinate schema: " + str(p))
        rows = np.loadtxt(p, ndmin=2)
        if (not rows.size or rows.shape[1] != len(expected_columns)
                or not np.isfinite(rows).all()):
            raise ValueError("missing, empty or nonfinite joint samples: " + str(p))
        if np.any(rows[:, 2:] < 0) or np.any(rows[:, 2:] > 1):
            raise ValueError("joint prior quantile outside [0,1]: " + str(p))

    return dict(log_evidence_ratio=float(value[0]), evidence_file=str(integral),
                posterior_file=str(samples), joined_posterior_file=str(grid),
                iteration=n-1, statistical_convergence_validated=False)


def main(argv=None):
    import sys
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "collect":
        parser = argparse.ArgumentParser(description="Collect final single-worker native evidence")
        parser.add_argument("--workflow", required=True)
        opts = parser.parse_args(argv[1:])
        print(json.dumps(collect_result(opts.workflow), indent=2))
    elif argv and argv[0] == "build":
        parser = argparse.ArgumentParser(description="Build a native joint-lensing Hyperpipe workflow")
        parser.add_argument("--joint-config", required=True)
        parser.add_argument("--outdir", required=True)
        parser.add_argument("--n-seed", type=int, default=1024)
        parser.add_argument("--seed", type=int, default=1234)
        opts = parser.parse_args(argv[1:])
        print(build_workflow(json.loads(Path(opts.joint_config).read_text()), opts.outdir, opts.n_seed, opts.seed))
    else:
        JointLensingDriver().run(argv)


if __name__ == "__main__":
    main()
