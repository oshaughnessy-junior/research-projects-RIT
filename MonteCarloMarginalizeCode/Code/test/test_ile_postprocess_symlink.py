"""Preserved rescue products consolidate identically through a directory alias."""
import os
from pathlib import Path
import subprocess


POSTPROCESS = Path(__file__).resolve().parents[1] / "bin" / "util_ILEdagPostprocess.sh"


def test_legacy_join_follows_only_the_command_line_directory_symlink(tmp_path):
    # Two unique rows, in different directories: collection must remain recursive.
    products = tmp_path / "preserved ILE products"
    products.mkdir()
    nested = products / "batch"
    nested.mkdir()
    (products / "CME_0.dat").write_text("0 1 10 .1 100 20\n")
    (nested / "CME_1.dat").write_text("1 2 12 .1 200 30\n")
    (products / "command-single.sh").write_text("# preserved command\n")
    (products / "example-psd.xml.gz").write_text("preserved PSD\n")
    alias = tmp_path / "iteration_0_ile"
    alias.symlink_to(products, target_is_directory=True)
    # -H must not become -L: internal aliases could duplicate archived rows.
    (products / "duplicate_batch").symlink_to(nested, target_is_directory=True)
    shim = tmp_path / "shim"
    shim.mkdir()
    cleaner = shim / "util_CleanILE.py"
    cleaner.write_text('#!/bin/bash\ncat "$1"\n')
    cleaner.chmod(0o755)
    env = dict(os.environ, PATH=str(shim) + os.pathsep + os.environ["PATH"],
               RIFT_HYPERPIPELINE_FORMAT="0")
    outputs = []
    for name, root in (("direct", products), ("alias", alias)):
        result = subprocess.run(["bash", str(POSTPROCESS), str(root), name,
                                 "--intrinsic-digits", "12"], cwd=tmp_path, env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=30)
        assert result.returncode == 0, result.stderr
        output = (tmp_path / (name + ".composite")).read_text()
        assert len(output.splitlines()) == 2, result.stdout + result.stderr
        outputs.append(output)
    assert outputs[0] == outputs[1]
    assert outputs[0].splitlines()[0].startswith("1 2 12")
