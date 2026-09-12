"""Keep paired smoke tests independent of differing executable defaults."""
import ast
from pathlib import Path


def test_paired_smokes_pin_the_same_reference_frequency():
    for name in ("run_short_av_ile.py", "run_short_jax_av_ile.py"):
        path = Path(__file__).with_name(name)
        tree = ast.parse(path.read_text(), filename=str(path))
        values = [
            ast.literal_eval(call.args[2])
            for call in ast.walk(tree)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == "set_option"
            and len(call.args) == 3
            and isinstance(call.args[1], ast.Str)
            and call.args[1].s == "--reference-freq"
        ]
        assert values == [100.0], name
