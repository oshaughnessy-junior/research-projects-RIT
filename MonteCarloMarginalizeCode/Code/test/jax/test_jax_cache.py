import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import textwrap
import threading
import zipfile
from pathlib import Path

import pytest

from RIFT import jax_cache as cache


COMPAT = {
    "python": "3.11.9", "jax": "0.4.35", "jaxlib": "0.4.35",
    "accelerator_plugins": {"jax-cuda12-plugin": "0.4.35"},
    "backend": "gpu", "platform_version": "CUDA 12.4",
    "device_kind": "NVIDIA A30", "compute_capability": "8.0",
}


class _Config:
    jax_persistent_cache_enable_xla_caches = None

    def __init__(self):
        self.updates = []

    def update(self, name, value):
        self.updates.append((name, value))


class _Jax:
    config = _Config()


def test_configure_uses_compatibility_namespace(tmp_path, monkeypatch):
    monkeypatch.delenv("JAX_COMPILATION_CACHE_DIR", raising=False)
    monkeypatch.setattr(cache, "runtime_compatibility", lambda unused: COMPAT)
    fake = _Jax()
    selected = cache.configure_persistent_cache(fake, ["--jax-cache-dir", str(tmp_path)])
    assert selected == (tmp_path / cache.compatibility_key(COMPAT)).resolve()
    assert os.environ["JAX_COMPILATION_CACHE_DIR"] == str(selected)
    manifest = json.loads((selected / cache.MANIFEST_NAME).read_text())
    assert manifest["compatibility"] == COMPAT
    assert ("jax_persistent_cache_enable_xla_caches", "") in fake.config.updates
    assert ("jax_enable_compilation_cache", True) in fake.config.updates


def test_configure_supports_jax_before_auxiliary_xla_caches(tmp_path, monkeypatch):
    """JAX 0.4.24 has executable caching but not the path-valued XLA option."""
    class LegacyConfig:
        def __init__(self):
            self.updates = []

        def update(self, name, value):
            if name == "jax_persistent_cache_enable_xla_caches":
                raise AttributeError("Unrecognized config option")
            self.updates.append((name, value))

    class LegacyJax:
        config = LegacyConfig()

    monkeypatch.delenv("JAX_COMPILATION_CACHE_DIR", raising=False)
    monkeypatch.setattr(cache, "runtime_compatibility", lambda unused: COMPAT)
    selected = cache.configure_persistent_cache(
        LegacyJax(), ["--jax-cache-dir", str(tmp_path)])
    assert selected == (tmp_path / cache.compatibility_key(COMPAT)).resolve()
    assert ("jax_enable_compilation_cache", True) in LegacyJax.config.updates


def test_disable_does_not_create_cache(tmp_path, monkeypatch):
    monkeypatch.delenv("JAX_COMPILATION_CACHE_DIR", raising=False)
    fake = _Jax()
    assert cache.configure_persistent_cache(fake, ["--no-jax-persistent-cache"]) is None
    assert ("jax_enable_compilation_cache", False) in fake.config.updates
    assert not list(tmp_path.iterdir())


def test_condor_scratch_is_the_default_root(tmp_path, monkeypatch):
    monkeypatch.delenv("RIFT_JAX_CACHE_ROOT", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setenv("_CONDOR_SCRATCH_DIR", str(tmp_path))
    assert cache.default_cache_root() == tmp_path / ".rift_cache" / "jax"


def test_bundle_option_scan_uses_last_cli_value():
    assert cache.argv_option(["--jax-cache-bundle", "old.zip",
                              "--jax-cache-bundle=new.zip"],
                             "--jax-cache-bundle") == "new.zip"


def test_cache_cli_help_does_not_require_optional_jax():
    script = Path(__file__).resolve().parents[2] / "bin" / "rift_jax_cache"
    code = textwrap.dedent("""
        import runpy
        import sys
        sys.modules["jax"] = None
        sys.argv = ["rift_jax_cache", "--help"]
        runpy.run_path(%r, run_name="__main__")
    """ % str(script))
    completed = subprocess.run([sys.executable, "-c", code], check=False,
                               capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stderr
    assert "Inspect, export, and safely import" in completed.stdout


def test_unwritable_cache_disables_without_failing(monkeypatch, capsys):
    monkeypatch.delenv("JAX_COMPILATION_CACHE_DIR", raising=False)
    monkeypatch.setattr(cache, "runtime_compatibility", lambda unused: COMPAT)
    monkeypatch.setattr(Path, "mkdir", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("read only")))
    fake = _Jax()
    assert cache.configure_persistent_cache(fake, ["--jax-cache-dir", "/unwritable"]) is None
    assert "disabling JAX persistent cache" in capsys.readouterr().err
    assert ("jax_enable_compilation_cache", False) in fake.config.updates


def test_manifest_updates_use_unique_atomic_temporary_files(tmp_path, monkeypatch):
    sources = []
    lock = threading.Lock()
    real_replace = os.replace

    def recording_replace(source, target):
        with lock:
            sources.append(str(source))
        real_replace(source, target)

    monkeypatch.setattr(cache.os, "replace", recording_replace)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: cache._write_manifest(tmp_path, COMPAT, {"writer": i}),
                      range(24)))
    assert len(sources) == 24
    assert len(set(sources)) == 24
    assert json.loads((tmp_path / cache.MANIFEST_NAME).read_text())["writer"] in range(24)


def test_runtime_fingerprint_records_current_accelerator_plugins(monkeypatch):
    class Client:
        platform_version = "PJRT CUDA 13"

    class Device:
        client = Client()
        device_kind = "Future GPU"
        compute_capability = (13, 0)

    class Jax:
        __version__ = "1.0"

        @staticmethod
        def default_backend():
            return "gpu"

        @staticmethod
        def devices(backend):
            assert backend == "gpu"
            return [Device()]

    versions = {"jaxlib": "1.0", "jax-cuda13-plugin": "1.0"}
    monkeypatch.setattr(cache, "_package_version", versions.get)
    identity = cache.runtime_compatibility(Jax)
    assert identity["accelerator_plugins"] == {"jax-cuda13-plugin": "1.0"}
    assert identity["compute_capability"] == "13.0"


def test_bundle_round_trip_and_profile_guard(tmp_path):
    source = tmp_path / "source"
    (source / "nested").mkdir(parents=True)
    (source / "nested" / "compiled-entry").write_bytes(b"compiled")
    bundle = tmp_path / "warm.zip"
    cache.export_bundle(source, bundle, COMPAT, "o4-laplace", {"n_chunk": 8000})
    destination = cache.import_bundle(bundle, tmp_path / "target", COMPAT, "o4-laplace")
    assert (destination / "nested" / "compiled-entry").read_bytes() == b"compiled"
    records = sorted(destination.glob(cache.IMPORT_MANIFEST_PREFIX + "*.json"))
    assert len(records) == 1
    manifest = json.loads(records[0].read_text())
    assert manifest["static_shapes"] == {"n_chunk": 8000}
    cache._write_manifest(destination, COMPAT)
    assert json.loads(records[0].read_text()) == manifest

    # Compatible bundles merge compiler entries, so provenance must retain
    # every contributor rather than silently replacing the previous profile.
    (source / "nested" / "second-entry").write_bytes(b"second")
    second_bundle = tmp_path / "second.zip"
    cache.export_bundle(source, second_bundle, COMPAT, "o4-exact",
                        {"n_chunk": 1000})
    cache.import_bundle(second_bundle, tmp_path / "target", COMPAT,
                        "o4-exact")
    records = sorted(destination.glob(cache.IMPORT_MANIFEST_PREFIX + "*.json"))
    assert len(records) == 2
    imported_profiles = {
        json.loads(path.read_text())["imported_profile"] for path in records}
    assert imported_profiles == {"o4-laplace", "o4-exact"}
    cache.import_bundle(bundle, tmp_path / "target", COMPAT, "o4-laplace")
    assert len(list(destination.glob(
        cache.IMPORT_MANIFEST_PREFIX + "*.json"))) == 2

    # A cache warmed by an older PR may still contain the former singular
    # import record. Neither legacy nor current provenance is compiler data.
    (destination / cache.IMPORT_MANIFEST_NAME).write_text("{}\n")
    reexport = tmp_path / "reexport.zip"
    reexport_manifest = cache.export_bundle(destination, reexport, COMPAT)
    exported_names = {Path(rel).name for rel in reexport_manifest["files"]}
    assert cache.MANIFEST_NAME not in exported_names
    assert cache.IMPORT_MANIFEST_NAME not in exported_names
    assert not any(name.startswith(cache.IMPORT_MANIFEST_PREFIX)
                   for name in exported_names)
    with pytest.raises(ValueError, match="profile"):
        cache.import_bundle(bundle, tmp_path / "wrong-profile", COMPAT, "other")

    exact = tmp_path / "standard-jax-exact-dir"
    imported = cache.import_bundle(bundle, tmp_path / "ignored-root", COMPAT,
                                   destination=exact)
    assert imported == exact
    assert (exact / "nested" / "compiled-entry").read_bytes() == b"compiled"


def test_import_publishes_cache_entries_atomically(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "entry").write_bytes(b"compiled")
    bundle = tmp_path / "warm.zip"
    cache.export_bundle(source, bundle, COMPAT)
    replacements = []
    real_replace = os.replace

    def recording_replace(temporary, target):
        replacements.append((Path(temporary), Path(target)))
        real_replace(temporary, target)

    monkeypatch.setattr(cache.os, "replace", recording_replace)
    destination = cache.import_bundle(bundle, tmp_path / "target", COMPAT)
    entry_publications = [(temporary, target) for temporary, target in replacements
                          if target == destination / "entry"]
    assert len(entry_publications) == 1
    temporary, target = entry_publications[0]
    assert temporary.parent == target.parent
    assert temporary != target


def test_bundle_rejects_runtime_mismatch_and_tampering(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "entry").write_bytes(b"one")
    bundle = tmp_path / "warm.zip"
    cache.export_bundle(source, bundle, COMPAT)
    mismatch = dict(COMPAT, jaxlib="0.5.0")
    with pytest.raises(ValueError, match="incompatible"):
        cache.import_bundle(bundle, tmp_path / "mismatch", mismatch)

    tampered = tmp_path / "tampered.zip"
    with zipfile.ZipFile(bundle) as old, zipfile.ZipFile(tampered, "w") as new:
        for name in old.namelist():
            new.writestr(name, b"two" if name == "cache/entry" else old.read(name))
    with pytest.raises(ValueError, match="checksum"):
        cache.import_bundle(tampered, tmp_path / "tampered", COMPAT)

    unexpected = tmp_path / "unexpected.zip"
    with zipfile.ZipFile(bundle) as old, zipfile.ZipFile(unexpected, "w") as new:
        for name in old.namelist():
            new.writestr(name, old.read(name))
        new.writestr("unrelated", b"surprise")
    with pytest.raises(ValueError, match="unexpected"):
        cache.import_bundle(unexpected, tmp_path / "unexpected", COMPAT)


def test_bundle_rejects_oversized_or_overcompressed_members(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "entry").write_bytes(b"0" * 10_000)
    bundle = tmp_path / "warm.zip"
    cache.export_bundle(source, bundle, COMPAT)

    monkeypatch.setattr(cache, "MAX_BUNDLE_MEMBER_BYTES", 100)
    with pytest.raises(ValueError, match="size limit"):
        cache.import_bundle(bundle, tmp_path / "oversized", COMPAT)
    monkeypatch.setattr(cache, "MAX_BUNDLE_MEMBER_BYTES", 20_000)
    monkeypatch.setattr(cache, "MAX_BUNDLE_COMPRESSION_RATIO", 2)
    with pytest.raises(ValueError, match="compression-ratio"):
        cache.import_bundle(bundle, tmp_path / "overcompressed", COMPAT)


def test_real_jax_cache_reused_across_fresh_processes(tmp_path):
    pytest.importorskip("jax")
    code = textwrap.dedent("""
        import jax
        import jax.numpy as jnp
        from RIFT.jax_cache import configure_persistent_cache
        configure_persistent_cache(jax, ["--jax-cache-dir", r"%s"])
        @jax.jit
        def work(x):
            for _ in range(8):
                x = jnp.sin(x @ x + 0.01)
            return x.sum()
        print(float(work(jnp.eye(64)).block_until_ready()))
    """ % tmp_path)
    env = os.environ.copy()
    env.update({
        "JAX_PLATFORMS": "cpu",
        "JAX_PERSISTENT_CACHE_MIN_COMPILE_TIME_SECS": "0",
        "JAX_PERSISTENT_CACHE_MIN_ENTRY_SIZE_BYTES": "0",
        "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1",
        "TF_NUM_INTRAOP_THREADS": "1", "TF_NUM_INTEROP_THREADS": "1",
        "XLA_FLAGS": "--xla_cpu_multi_thread_eigen=false --xla_force_host_platform_device_count=1",
    })

    def run():
        subprocess.run([sys.executable, "-c", code], env=env, check=True,
                       capture_output=True, text=True, timeout=120)

    def entries():
        return {
            str(path.relative_to(tmp_path)): (hashlib.sha256(path.read_bytes()).hexdigest(),
                                              path.stat().st_mtime_ns)
            for path in tmp_path.rglob("*")
            if path.is_file() and path.name != cache.MANIFEST_NAME
            and not path.name.endswith(".tmp")
        }

    run()
    first = entries()
    assert first, "the first fresh process did not populate JAX's persistent cache"
    run()
    assert entries() == first, "the second fresh process recompiled or rewrote cache entries"


def test_real_jax_cache_bundle_reused_from_different_absolute_root(tmp_path):
    """A transferred executable must not be keyed by its original cache path."""
    jax = pytest.importorskip("jax")
    source_root = tmp_path / "producer" / "cache"
    target_root = tmp_path / "consumer-at-a-different-path" / "cache"
    code = textwrap.dedent("""
        import jax
        import jax.numpy as jnp
        from RIFT.jax_cache import configure_persistent_cache
        configure_persistent_cache(jax, ["--jax-cache-dir", r"%s"])
        @jax.jit
        def transferred_work(x):
            for _ in range(8):
                x = jnp.sin(x @ x + 0.01)
            return x.sum()
        print(float(transferred_work(jnp.eye(64)).block_until_ready()))
    """)
    env = os.environ.copy()
    env.pop("JAX_COMPILATION_CACHE_DIR", None)
    env.update({
        "JAX_PLATFORMS": "cpu",
        "JAX_PERSISTENT_CACHE_MIN_COMPILE_TIME_SECS": "0",
        "JAX_PERSISTENT_CACHE_MIN_ENTRY_SIZE_BYTES": "0",
        "JAX_DEBUG_LOG_MODULES": "jax._src.compiler,jax._src.compilation_cache",
        "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1",
        "TF_NUM_INTRAOP_THREADS": "1", "TF_NUM_INTEROP_THREADS": "1",
        "XLA_FLAGS": "--xla_cpu_multi_thread_eigen=false --xla_force_host_platform_device_count=1",
    })

    producer = subprocess.run(
        [sys.executable, "-c", code % source_root], env=env, check=True,
        capture_output=True, text=True, timeout=120)
    compatibility = cache.runtime_compatibility(jax)
    source = source_root / cache.compatibility_key(compatibility)
    bundle = tmp_path / "portable.zip"
    cache.export_bundle(source, bundle, compatibility, "different-root-test")
    target = cache.import_bundle(bundle, target_root, compatibility,
                                 "different-root-test")

    def entries():
        return {
            str(path.relative_to(target)): (hashlib.sha256(path.read_bytes()).hexdigest(),
                                            path.stat().st_mtime_ns)
            for path in target.rglob("*")
            if path.is_file() and not cache._is_provenance_file(path)
            and not path.name.endswith(".tmp")
        }

    imported = entries()
    assert imported, "producer did not create any persistent executable entry"
    consumer = subprocess.run(
        [sys.executable, "-c", code % target_root], env=env, check=True,
        capture_output=True, text=True, timeout=120)
    assert consumer.stdout == producer.stdout
    # JAX publishes an executable cache entry atomically after compilation.  A
    # fresh compile would therefore replace it and change its mtime; preserving
    # every imported byte and mtime pins an actual persistent-cache load without
    # depending on JAX's version-specific debug-log formatting.
    assert entries() == imported, "consumer recompiled after cache-root transfer"


@pytest.mark.parametrize("scheme", ["exact", "laplace"])
def test_angle_batched_kernel_persists_without_host_effects(tmp_path, scheme):
    """Pin both real anglemarg graphs, not a toy matmul cache entry.

    JAX refuses to persist any graph containing debug callbacks.  This test
    executes the shipped exact coefficient/reconstruction/scan kernel in two
    fresh processes and requires its named cache entry to survive unchanged;
    reintroducing the former amplitude callback therefore fails behaviorally.
    """
    pytest.importorskip("jax")
    test_dir = Path(__file__).resolve().parent
    code = textwrap.dedent("""
        import sys
        sys.path.insert(0, r"%s")
        import jax
        import jax.numpy as jnp
        from RIFT.jax_cache import configure_persistent_cache
        configure_persistent_cache(jax, ["--jax-cache-dir", r"%s"])
        from test_angle_marg_exact import make_synth, _dist_grid, RA, DEC, INCL, INTERP
        from RIFT.likelihood.jax_ile import anglemarg as AM
        data = make_synth(npts=16)
        xg, lwg = _dist_grid(data, n=16)
        if %r == "exact":
            @jax.jit
            def persisted_work(ra, dec, incl):
                return AM.fused_log_likelihood_distphipsimarg_exact(
                    data, ra, dec, incl, xg, lwg, interp=INTERP,
                    amp_sizing=AM.ANGLE_MARG_CROSSOVER_AMPLITUDE,
                    dense_chunk=8, grid_block=8, return_amp=True)
        else:
            @jax.jit
            def persisted_work(ra, dec, incl):
                return AM.fused_log_likelihood_distphipsimarg_laplace(
                    data, ra, dec, incl, xg, lwg, interp=INTERP,
                    amp_sizing=AM.ANGLE_MARG_CROSSOVER_AMPLITUDE,
                    phi_chunk=8, dist_block=8, return_amp=True)
        value, amp = persisted_work(jnp.asarray(RA), jnp.asarray(DEC), jnp.asarray(INCL))
        print(float(value.block_until_ready()[0]), float(amp.block_until_ready()))
    """ % (test_dir, tmp_path, scheme))
    env = os.environ.copy()
    env.update({
        "JAX_PLATFORMS": "cpu",
        "JAX_PERSISTENT_CACHE_MIN_COMPILE_TIME_SECS": "0",
        "JAX_PERSISTENT_CACHE_MIN_ENTRY_SIZE_BYTES": "0",
        "JAX_DEBUG_LOG_MODULES": "jax._src.compiler,jax._src.compilation_cache",
        "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1",
        "TF_NUM_INTRAOP_THREADS": "1", "TF_NUM_INTEROP_THREADS": "1",
        "XLA_FLAGS": "--xla_cpu_multi_thread_eigen=false --xla_force_host_platform_device_count=1",
    })

    def run():
        return subprocess.run([sys.executable, "-c", code], env=env, check=True,
                              capture_output=True, text=True, timeout=180)

    def persisted_entries():
        return {
            str(path.relative_to(tmp_path)): (hashlib.sha256(path.read_bytes()).hexdigest(),
                                              path.stat().st_mtime_ns)
            for path in tmp_path.rglob("*")
            if path.is_file() and "jit_persisted_work-" in path.name
        }

    first_run = run()
    assert "because it uses host callbacks" not in first_run.stderr
    first = persisted_entries()
    assert first, "the shipped %s-angle batch graph was not persisted" % scheme
    second_run = run()
    assert "because it uses host callbacks" not in second_run.stderr
    assert persisted_entries() == first, (
        "fresh-process %s kernel cache entry changed" % scheme)
