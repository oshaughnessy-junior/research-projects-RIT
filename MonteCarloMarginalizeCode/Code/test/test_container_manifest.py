"""
Tests for container family manifest parsing and the expression-valued
SingularityImage / selective-transfer / require_gpus wiring.

These run without a real HTCondor pool: the parser + expression builders are
pure, and the integration test inspects the generated ``condor_cmds`` on the
job object returned by ``write_ILE_sub_simple`` (no .sub file or condor needed).

Run directly:  python test/test_container_manifest.py
Or via pytest: pytest test/test_container_manifest.py
"""

import os
import shutil
import stat
import subprocess
import sys
import textwrap

import pytest

yaml = pytest.importorskip("yaml")  # manifest parsing requires PyYAML

import RIFT.misc.container_manifest as cm


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

MIXED_MANIFEST = textwrap.dedent(
    """
    version: 1
    fallback: ancient
    containers:
      - label: ancient
        image: /cvmfs/sw/rift_ancient_cuda11.sif
        cuda_capability_min: 3.0
        cuda_capability_max: 7.0
      - label: modern
        image: osdf:///igwn/rift_modern_cuda12.sif
        cuda_capability_min: 7.0
    """
)

ALL_CVMFS_MANIFEST = textwrap.dedent(
    """
    version: 1
    fallback: ancient
    containers:
      - label: ancient
        image: /cvmfs/sw/rift_ancient.sif
        cuda_capability_min: 3.0
      - label: modern
        image: /cvmfs/sw/rift_modern.sif
        cuda_capability_min: 7.0
    """
)


def _write(tmp_path, text, name="fam.yaml"):
    p = tmp_path / name
    p.write_text(text)
    return str(p)


# ---------------------------------------------------------------------------
# 1. parser
# ---------------------------------------------------------------------------

def test_parser_sorts_and_resolves_fallback(tmp_path):
    m = cm.load_container_manifest(_write(tmp_path, MIXED_MANIFEST))
    # sorted by capability descending
    assert [c["label"] for c in m["containers"]] == ["modern", "ancient"]
    assert m["fallback"] == "ancient"
    assert m["capability_attr"] == cm.DEFAULT_CAPABILITY_ATTR


def test_parser_default_fallback_is_lowest(tmp_path):
    # no explicit fallback -> most-compatible (lowest-min) container
    text = MIXED_MANIFEST.replace("fallback: ancient\n", "")
    m = cm.load_container_manifest(_write(tmp_path, text))
    assert m["fallback"] == "ancient"


def test_parser_rejects_unknown_fallback(tmp_path):
    text = MIXED_MANIFEST.replace("fallback: ancient", "fallback: nope")
    with pytest.raises(cm.ContainerManifestError):
        cm.load_container_manifest(_write(tmp_path, text))


def test_parser_rejects_empty(tmp_path):
    with pytest.raises(cm.ContainerManifestError):
        cm.load_container_manifest(_write(tmp_path, "version: 1\ncontainers: []\n"))


def test_parser_rejects_missing_image(tmp_path):
    text = "containers:\n  - label: x\n    cuda_capability_min: 5.0\n"
    with pytest.raises(cm.ContainerManifestError):
        cm.load_container_manifest(_write(tmp_path, text))


# ---------------------------------------------------------------------------
# 2. expressions
# ---------------------------------------------------------------------------

def test_image_expression(tmp_path):
    m = cm.load_container_manifest(_write(tmp_path, MIXED_MANIFEST))
    expr = cm.build_singularity_image_expr(m)
    assert expr == (
        'ifThenElse(TARGET.GPUs_Capability >= 7.0, '
        '"./rift_modern_cuda12.sif", "/cvmfs/sw/rift_ancient_cuda11.sif")'
    )
    # an expression must NOT be a quoted string literal
    assert not expr.startswith('"')


def test_transfer_expression_is_comma_free_ternary(tmp_path):
    m = cm.load_container_manifest(_write(tmp_path, MIXED_MANIFEST))
    expr = cm.build_transfer_input_expr(m)
    assert expr == (
        '$$([ (TARGET.GPUs_Capability >= 7.0 ? '
        '"osdf:///igwn/rift_modern_cuda12.sif" : "") ])'
    )
    # the token sits inside a comma-separated transfer_input_files list, so it
    # must contain no commas of its own
    assert "," not in expr


def test_transfer_expression_none_when_all_in_place(tmp_path):
    m = cm.load_container_manifest(_write(tmp_path, ALL_CVMFS_MANIFEST))
    assert cm.build_transfer_input_expr(m) is None


def test_require_gpus_floor(tmp_path):
    m = cm.load_container_manifest(_write(tmp_path, MIXED_MANIFEST))
    assert cm.build_require_gpus_floor(m) == "Capability >= 3.0"


def test_capability_attr_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("RIFT_GPU_CAPABILITY_ATTR", "CUDACapability")
    m = cm.load_container_manifest(_write(tmp_path, MIXED_MANIFEST))
    assert "TARGET.CUDACapability >=" in cm.build_singularity_image_expr(m)


# ---------------------------------------------------------------------------
# 3-5. integration with write_ILE_sub_simple (inspect generated condor_cmds)
# ---------------------------------------------------------------------------

def _make_ile_job(tmp_path, monkeypatch, singularity_image):
    """Call write_ILE_sub_simple in an isolated cwd; return its condor_cmds dict.

    Skips if the dag_utils_generic backend cannot be imported in this env.
    """
    dag = pytest.importorskip("RIFT.misc.dag_utils_generic")
    monkeypatch.chdir(tmp_path)
    job, _ = dag.write_ILE_sub_simple(
        tag="ILE",
        log_dir=str(tmp_path) + "/",
        exe="/usr/bin/true",
        arg_str="--foo bar",
        transfer_files=["../all.net"],
        use_singularity=True,
        singularity_image=singularity_image,
        request_gpu=True,
        cache_file="local.cache",
    )
    return dict(job.condor_cmds)


def test_integration_family_mixed(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "RIFT_REQUIRE_GPUS", '(DeviceName=!="Tesla K10.G1.8GB")'
    )
    cmds = _make_ile_job(tmp_path, monkeypatch, _write(tmp_path, MIXED_MANIFEST))

    img = cmds["MY.SingularityImage"]
    assert img.startswith("ifThenElse(")          # expression, not a literal
    assert not img.startswith('"')

    # selective transfer: exactly one $$() token, whole family NOT dumped
    tif = cmds["transfer_input_files"]
    assert tif.count("$$([") == 1
    assert "/cvmfs/sw/rift_ancient_cuda11.sif" not in tif  # cvmfs image not transferred
    assert tif.count("osdf:///igwn/rift_modern_cuda12.sif") == 1

    # floor composed with (not replacing) the user's RIFT_REQUIRE_GPUS
    rg = cmds["require_gpus"]
    assert "Capability >= 3.0" in rg
    assert 'DeviceName=!="Tesla K10.G1.8GB"' in rg
    assert "&&" in rg


def test_integration_all_cvmfs_no_transfer_token(tmp_path, monkeypatch):
    cmds = _make_ile_job(tmp_path, monkeypatch, _write(tmp_path, ALL_CVMFS_MANIFEST))
    assert "$$([" not in cmds.get("transfer_input_files", "")
    # still an expression-valued image + a capability floor
    assert cmds["MY.SingularityImage"].startswith("ifThenElse(")
    assert "Capability >= 3.0" in cmds["require_gpus"]


def test_backward_compat_single_sif(tmp_path, monkeypatch):
    monkeypatch.delenv("RIFT_REQUIRE_GPUS", raising=False)
    cmds = _make_ile_job(tmp_path, monkeypatch, "./foo.sif")
    # byte-identical legacy behavior: quoted literal, no $$() token, no floor
    assert cmds["MY.SingularityImage"] == '"./foo.sif"'
    assert "$$([" not in cmds.get("transfer_input_files", "")
    assert "require_gpus" not in cmds


# ---------------------------------------------------------------------------
# 6. OSG-safe runtime-selection wrapper
# ---------------------------------------------------------------------------

def test_runtime_wrapper_text_contents(tmp_path):
    m = cm.load_container_manifest(_write(tmp_path, MIXED_MANIFEST))
    text = cm.build_runtime_selection_wrapper(m, inner_command="./ile_pre.sh")
    assert text.startswith("#!/bin/bash")
    # cap->image table baked from the manifest (osdf image -> ./basename; cvmfs
    # image -> verbatim in-place path; the osdf URL is the single-fetch source)
    assert '"./rift_modern_cuda12.sif"' in text                 # osdf runtime path
    assert '"/cvmfs/sw/rift_ancient_cuda11.sif"' in text        # cvmfs verbatim
    assert '"osdf:///igwn/rift_modern_cuda12.sif"' in text      # fetch URL
    assert 'FALLBACK_LABEL="ancient"' in text
    assert 'INNER_COMMAND="./ile_pre.sh"' in text
    # an in-place-only image must have an empty fetch URL (never fetched)
    bash = shutil.which("bash")
    if bash:
        r = subprocess.run([bash, "-n", "-c", text], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr


@pytest.mark.skipif(not shutil.which("bash") or not shutil.which("awk"),
                    reason="needs bash + awk to exercise the wrapper")
def test_runtime_wrapper_selects_by_capability(tmp_path):
    # in-place image paths that exist, so acquisition needs no network fetch
    anc = tmp_path / "anc.sif"; anc.write_text("x")
    mod = tmp_path / "mod.sif"; mod.write_text("x")
    manifest_text = textwrap.dedent(
        """
        version: 1
        fallback: ancient
        containers:
          - label: ancient
            image: {anc}
            cuda_capability_min: 3.0
            cuda_capability_max: 7.0
          - label: modern
            image: {mod}
            cuda_capability_min: 7.0
        """
    ).format(anc=anc, mod=mod)
    m = cm.load_container_manifest(_write(tmp_path, manifest_text))
    wrapper = tmp_path / "select.sh"
    wrapper.write_text(cm.build_runtime_selection_wrapper(m, inner_command="/bin/true"))
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)

    # fake `apptainer` on PATH so the final exec succeeds without a real runtime
    fakebin = tmp_path / "bin"; fakebin.mkdir()
    fake = fakebin / "apptainer"
    fake.write_text('#!/bin/bash\necho "APPTAINER $*"\n')
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ, PATH="{}:{}".format(fakebin, os.environ["PATH"]))

    def run(cap):
        env2 = dict(env, RIFT_CONTAINER_FORCE_CAP=cap)
        return subprocess.run([str(wrapper)], capture_output=True, text=True, env=env2)

    r = run("12.0")   # >= 7.0 -> modern
    assert r.returncode == 0, r.stderr
    assert "selected: modern" in r.stderr
    r = run("5.0")    # in [3.0,7.0) -> ancient
    assert "selected: ancient" in r.stderr
    r = run("2.0")    # below everything -> fallback (ancient)
    assert "fallback" in r.stderr and "selected: ancient" in r.stderr


def test_integration_runtime_select(tmp_path, monkeypatch):
    # Opt-in OSG-safe mode: no expression-valued MY.SingularityImage, no $$()
    # transfer token; a wrapper executable is emitted instead, and the
    # require_gpus floor is still applied.
    monkeypatch.setenv("RIFT_CONTAINER_RUNTIME_SELECT", "1")
    monkeypatch.delenv("RIFT_REQUIRE_GPUS", raising=False)
    # pin the in-container exe dir so the inner command is deterministic
    monkeypatch.setenv("SINGULARITY_BASE_EXE_DIR", "/opt/rift/bin/")
    cmds = _make_ile_job(tmp_path, monkeypatch, _write(tmp_path, MIXED_MANIFEST))

    assert "MY.SingularityImage" not in cmds          # the OSG-breaking attr is gone
    assert "MY.SingularityBindCVMFS" not in cmds
    assert "$$([" not in cmds.get("transfer_input_files", "")  # wrapper self-fetches
    assert "Capability >= 3.0" in cmds["require_gpus"]         # floor still steers GPUs

    # the wrapper was written and execs the in-container exe (no frames -> not ile_pre.sh)
    wrapper = tmp_path / "rift_container_select.sh"
    assert wrapper.exists()
    body = wrapper.read_text()
    assert body.startswith("#!/bin/bash")
    assert 'INNER_COMMAND="/opt/rift/bin/true"' in body    # SINGULARITY_BASE_EXE_DIR + exe basename


if __name__ == "__main__":
    sys.exit(pytest.main([os.path.abspath(__file__), "-v"]))
