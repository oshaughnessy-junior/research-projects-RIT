"""
pytest fixtures + path wiring for the in-tree RIFT.hyperpipe test suite.

Resolves ``RIFT_ROOT`` in two ways, in order:
  1. the ``$RIFT_ROOT`` environment variable, if set (e.g. by pixi's
     activation block);
  2. walking up from this file's location --- with the canonical layout
     ``$RIFT_ROOT/MonteCarloMarginalizeCode/Code/test/hyperpipe/tests/conftest.py``,
     ``parents[5]`` of ``__file__`` is ``$RIFT_ROOT``.

This means the suite works from any RIFT clone with no user-specific
paths to edit.
"""
from __future__ import annotations

import importlib
import os
import sys
import types
from pathlib import Path

import pytest


_HERE = Path(__file__).resolve()


def _is_rift_root(p: Path) -> bool:
    return (p / "MonteCarloMarginalizeCode" / "Code" / "RIFT" / "hyperpipe").exists()


def _rift_root() -> Path:
    # 1. honor explicit env var if it points at a real RIFT clone
    env = os.environ.get("RIFT_ROOT")
    if env:
        p = Path(env).resolve()
        if _is_rift_root(p):
            return p
    # 2. canonical in-tree location: parents[5] of this file
    if len(_HERE.parents) > 5:
        candidate = _HERE.parents[5]
        if _is_rift_root(candidate):
            return candidate
    # 3. give up
    pytest.skip(
        "Could not locate RIFT root. Either set $RIFT_ROOT or run this "
        "suite from its canonical location at "
        "$RIFT_ROOT/MonteCarloMarginalizeCode/Code/test/hyperpipe/tests/."
    )


@pytest.fixture(scope="session")
def rift_root() -> Path:
    return _rift_root()


@pytest.fixture(scope="session")
def hyperpipe_dir(rift_root: Path) -> Path:
    return rift_root / "MonteCarloMarginalizeCode" / "Code" / "RIFT" / "hyperpipe"


@pytest.fixture(scope="session")
def rift_bin(rift_root: Path) -> Path:
    return rift_root / "MonteCarloMarginalizeCode" / "Code" / "bin"


@pytest.fixture(scope="session")
def rift_py(rift_root: Path) -> Path:
    return rift_root / "MonteCarloMarginalizeCode" / "Code"


@pytest.fixture(scope="session")
def hp_modules(rift_py: Path):
    """Import the lightweight hyperpipe modules and expose them on a namespace."""
    if str(rift_py) not in sys.path:
        sys.path.insert(0, str(rift_py))
    # These modules do not use lalsimutils. Bypass RIFT/__init__.py so the
    # pipeline-build unit tests stay runnable in a small numpy/pytest env.
    if "RIFT" not in sys.modules:
        fake_rift = types.ModuleType("RIFT")
        fake_rift.__path__ = [str(rift_py / "RIFT")]
        sys.modules["RIFT"] = fake_rift
    coords = importlib.import_module("RIFT.hyperpipe.coords")
    config = importlib.import_module("RIFT.hyperpipe.config")
    marg_list = importlib.import_module("RIFT.hyperpipe.marg_list")
    marg_contract = importlib.import_module("RIFT.hyperpipe.marg_contract")
    execution_contract = importlib.import_module(
        "RIFT.hyperpipe.execution_contract")
    cip_pipeline = importlib.import_module("RIFT.misc.cip_pipeline")
    dag_utils_generic = importlib.import_module("RIFT.misc.dag_utils_generic")
    drivers_base = importlib.import_module("RIFT.hyperpipe.drivers.base")
    return types.SimpleNamespace(
        coords=coords,
        config=config,
        marg_list=marg_list,
        marg_contract=marg_contract,
        execution_contract=execution_contract,
        cip_pipeline=cip_pipeline,
        dag_utils_generic=dag_utils_generic,
        drivers_base=drivers_base,
    )
