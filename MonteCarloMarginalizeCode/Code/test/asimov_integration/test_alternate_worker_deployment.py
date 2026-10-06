"""Deployment contracts for the internal-subdag builder; no job submission."""
import ast
from pathlib import Path
from types import SimpleNamespace

SOURCE = Path(__file__).resolve().parents[2] / 'bin/create_event_parameter_pipeline_AlternateIteration'


def test_worker_calls_forward_disk_and_authentication():
    tree = ast.parse(SOURCE.read_text())
    cip = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
           and isinstance(n.func, ast.Attribute) and n.func.attr == 'write_CIP_sub']
    ile = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
           and isinstance(n.func, ast.Attribute) and n.func.attr == 'write_ILE_sub_simple']
    assert len(cip) == 4 and len(ile) == 4
    for call in cip:
        values = {k.arg: ast.unparse(k.value) for k in call.keywords}
        assert values['request_disk'] == 'opts.cip_request_disk'
        assert values['use_oauth_files'] == 'opts.use_oauth_files'
    for call in ile:
        values = {k.arg: ast.unparse(k.value) for k in call.keywords}
        for key in ('request_disk', 'use_oauth_files', 'use_singularity',
                    'singularity_image', 'use_osg', 'frames_dir', 'cache_file'):
            assert key in values


def test_requested_final_time_resampling_exports_fair_draws():
    tree = ast.parse(SOURCE.read_text())
    block = next(n for n in ast.walk(tree) if isinstance(n, ast.If)
                 and ast.unparse(n.test) == 'opts.last_iteration_extrinsic_time_resampling')
    for enabled in (False, True):
        namespace = {'opts': SimpleNamespace(last_iteration_extrinsic_time_resampling=enabled),
                     'ile_args_extr': '--time-marginalization --distance-marginalization ',
                     'n_points_per_ILE': 5}
        exec(compile(ast.Module(body=[block], type_ignores=[]), str(SOURCE), 'exec'), namespace)
        args = namespace['ile_args_extr']
        assert ('--resample-time-marginalization' in args) == enabled
        assert ('--fairdraw-extrinsic-output-n-max 5' in args) == enabled
        assert ('--distance-marginalization' in args) != enabled


def test_truncated_schedule_counts_only_retained_groups():
    source=SOURCE.with_name('util_RIFT_pseudo_pipe.py')
    tree=ast.parse(source.read_text())
    block=next(n for n in ast.walk(tree) if isinstance(n,ast.If)
               and ast.unparse(n.test)=='not opts.internal_truncate_cip_arg_list is None')
    for groups,count in [(['2 early','2 middle','Z internal','1 final'],2),
                         (['2 early','G3 grid','2 final'],5)]:
        namespace={'opts':SimpleNamespace(internal_truncate_cip_arg_list=2),
                   'lines':groups,'n_iterations':6}
        exec(compile(ast.Module(body=[block],type_ignores=[]),str(source),'exec'),namespace)
        assert namespace['n_iterations']==count
        assert namespace['lines']==groups[-2:]


def test_puff_subdag_waits_for_grid_producer():
    tree=ast.parse(SOURCE.read_text())
    calls=[ast.unparse(n) for n in ast.walk(tree) if isinstance(n,ast.Call)]
    assert 'subdag_puff_node.add_parent(parent_puff_node)' in calls
