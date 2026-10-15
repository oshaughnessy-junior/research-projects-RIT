"""Run focused regressions while metering synthetic sampler likelihood callbacks.

The audit counts attempted rows in sampler/optimizer callbacks, including tests
that intentionally raise. Analytic truth calculations and mocked bookkeeping-only
unit tests are separate diagnostics, not inference likelihood calls.
"""
import argparse
from functools import wraps
import json
from pathlib import Path
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--json', required=True)
    args = parser.parse_args()
    started = time.perf_counter()
    from RIFT.integrators.evaluation_cost import EvaluationLedger
    from RIFT.integrators import mcsamplerPortfolio, mcsamplerGPU, mcsamplerAdaptiveVolume, mcsamplerEnsemble
    from RIFT.integrators.unreliable_oracle.hill_climber import ClimbingOracle
    import pytest
    ledger = EvaluationLedger()

    def meter(fn):
        if getattr(fn, '_transport_validation_meter', None) is ledger:
            return fn
        wrapped = ledger.wrap(fn)
        wrapped._transport_validation_meter = ledger
        return wrapped

    def intercept(method):
        @wraps(method)
        def run(self, fn, *args, **kwargs):
            return method(self, meter(fn), *args, **kwargs)
        return run

    for module in [mcsamplerPortfolio, mcsamplerGPU, mcsamplerAdaptiveVolume, mcsamplerEnsemble]:
        for name in ['integrate', 'integrate_log']:
            method = getattr(module.MCSampler, name, None)
            if method is not None:
                setattr(module.MCSampler, name, intercept(method))
    original_setup = ClimbingOracle.setup
    def setup(self, lnL=None, **kwargs):
        return original_setup(self, lnL=meter(lnL) if lnL else None, **kwargs)
    ClimbingOracle.setup = setup
    here = Path(__file__).resolve().parent
    status = pytest.main(['-q'] + [str(here/name) for name in [
        'test_transport_cost.py', 'test_portfolio_restrict_and_warm.py', 'test_portfolio_member_density.py']])
    with open(args.json, 'w') as f:
        json.dump(dict(exit_code=int(status), sampler_callback_cost=ledger.snapshot(),
                       wall_seconds=time.perf_counter()-started), f, indent=2)
    raise SystemExit(status)


if __name__ == '__main__':
    main()
