"""Opt-in row accounting at the actual evaluator boundary, including failed calls.

Wrap EVERY true evaluator (also callbacks saved by optimizers/oracles) with one
ledger. Counts are attempted rows, not successful outputs or sampler.ntotal.
No claim is made about unwrapped callbacks, physical cost per row, or GPU timing.
"""
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from time import perf_counter

_phase = ContextVar("rift_evaluation_phase", default="unclassified")


@contextmanager
def cost_phase(name, ledger=None):
    start = perf_counter()
    token = _phase.set(name)
    try:
        yield
    finally:
        _phase.reset(token)
        if ledger is not None:
            ledger.phase_seconds[name] += perf_counter() - start


class EvaluationLedger:
    def __init__(self):
        self.phase_seconds = defaultdict(float)
        self.records = defaultdict(lambda: dict(rows=0, calls=0, failures=0, seconds=0.0))

    def wrap(self, function, kind="true", layout="columns"):
        """columns: positional/keyword coordinate arrays; matrix: first arg (N,D).

        Row counting inspects shapes only, so device arrays need no host copy.
        Scalar optimizer calls count one. Broadcasting uses the largest column;
        grouped tuple coordinates should be wrapped at a flattened boundary.
        """
        if layout not in ("columns", "matrix"):
            raise ValueError("layout must be columns or matrix")

        @wraps(function)
        def counted(*args, **kwargs):
            values = args if args else tuple(kwargs.values())
            if layout == "matrix":
                shape = getattr(values[0], "shape", ())
                rows = int(shape[0]) if len(shape) > 1 else 1
            else:
                rows = max((int(v.shape[-1]) if getattr(v, "ndim", 0) else 1
                            for v in values), default=1)
            record = self.records[(kind, _phase.get())]
            record["rows"] += rows
            record["calls"] += 1
            start = perf_counter()
            try:
                return function(*args, **kwargs)
            except BaseException:
                record["failures"] += 1
                raise
            finally:
                record["seconds"] += perf_counter() - start
        counted.evaluation_ledger = self
        return counted

    def snapshot(self):
        records = {kind + ":" + phase: dict(values)
                   for (kind, phase), values in sorted(self.records.items())}
        return dict(records=records, phase_seconds=dict(self.phase_seconds),
                    true_rows=sum(v["rows"] for (k, _), v in self.records.items() if k == "true"),
                    surrogate_rows=sum(v["rows"] for (k, _), v in self.records.items() if k == "surrogate"))
