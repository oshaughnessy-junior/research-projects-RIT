"""Build-time guards for a bootstrap retaining the original likelihood module."""
from RIFT.likelihood.time_marginalization_quadrature import find_time_quadrature_in_ile_args
NOLOOP_ONLY_TIME_QUADRATURES = ('peak-local',)


def refuse_jax_unsupported_time_quadrature(ile_args, where):
    found = [q for q in find_time_quadrature_in_ile_args(ile_args) if q is not None]
    if found and found[-1] in NOLOOP_ONLY_TIME_QUADRATURES:
        raise ValueError('JAX does not implement {} in {}'.format(found[-1], where))
