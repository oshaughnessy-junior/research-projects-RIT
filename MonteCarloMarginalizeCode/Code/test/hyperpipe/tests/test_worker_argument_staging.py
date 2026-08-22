import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[3]
    / "RIFT"
    / "misc"
    / "hyperpipeline_io.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("hyperpipeline_io_test", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stage_file_rewrites_submit_path_to_worker_basename(tmp_path):
    module = _load_module()
    source_dir = tmp_path / "production-inputs"
    source_dir.mkdir()
    source = source_dir / "H1-psd.xml.gz"
    source.write_bytes(b"representative psd")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    args_file = run_dir / "args_ile.txt"
    args_file.write_text("X --psd-file H1={} --n-eff 2\n".format(source))

    destination = module.stage_file_for_worker_arguments(
        str(source), str(run_dir), [str(args_file)]
    )

    assert Path(destination).read_bytes() == source.read_bytes()
    assert args_file.read_text() == "X --psd-file H1=H1-psd.xml.gz --n-eff 2\n"


def test_prepared_relative_frame_cache_is_transferred_without_precommand(tmp_path):
    module = _load_module()
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    (frames / "H1-TEST-100-4.gwf").write_bytes(b"frame")
    cache = tmp_path / "local.cache"
    cache.write_text("H TEST 100 4 frames_dir/H1-TEST-100-4.gwf\n")

    frames_arg, cache_arg, transfers = module.stage_prepared_frame_cache(
        str(frames), str(cache), ["psd.xml.gz"])

    assert frames_arg is None
    assert cache_arg == str(cache)
    assert transfers == ["psd.xml.gz", str(frames), str(cache)]


def test_submit_host_frame_cache_retains_legacy_precommand_fallback(tmp_path):
    module = _load_module()
    frames = tmp_path / "frames_dir"
    frames.mkdir()
    cache = tmp_path / "local.cache"
    cache.write_text("H TEST 100 4 file://localhost/data/H1.gwf\n")

    frames_arg, cache_arg, transfers = module.stage_prepared_frame_cache(
        str(frames), str(cache), [])

    assert frames_arg == str(frames)
    assert cache_arg is None
    assert transfers == []
