"""Offline, dependency-light contract drift detection.

This package intentionally does not import :mod:`RIFT` or any scientific stack.
Repository resolution, credentials, persistence, scheduling, and notifications are
runner responsibilities.
"""

from .engine import evaluate
from .model import CORE_VERSION, Registry, ResolvedInputs, SentinelInputError, load_registry, load_resolved_inputs
from .report import render_human, render_machine

__all__ = [
    "Registry",
    "ResolvedInputs",
    "SentinelInputError",
    "evaluate",
    "load_registry",
    "load_resolved_inputs",
    "render_human",
    "render_machine",
]

__version__ = CORE_VERSION
