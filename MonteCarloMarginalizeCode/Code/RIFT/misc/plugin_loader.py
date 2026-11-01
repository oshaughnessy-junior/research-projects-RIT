"""
RIFT.misc.plugin_loader
=======================

The shared spec resolver and ini parser for RIFT's ``--supplementary-*`` plugin family.

Extracted from :mod:`RIFT.misc.coordinate_plugin` (which re-exports thin wrappers with its
historical error text) so that every plugin-taking option resolves a ``--...-code SPEC`` the
same way:

  * SPEC looks like a file path (ends in ``.py``, contains a path separator, or names an
    existing file) -> loaded with ``importlib.util.spec_from_file_location`` under the
    module name ``<module_prefix><stem>``.  This is the form to use on a file-transfer
    worker, where the job directory is not reliably on ``sys.path``.
  * otherwise -> ``importlib.import_module(SPEC)`` (a dotted name on ``PYTHONPATH``).

The ini parser preserves key case (``optionxform = str``), matching
``--supplementary-likelihood-factor-ini``.
"""

import configparser
import hashlib
import importlib
import importlib.util
import os
import sys


def spec_looks_like_path(spec):
    """True if *spec* is treated as a file path rather than a dotted module name."""
    return spec.endswith(".py") or os.path.sep in spec or os.path.isfile(spec)


def load_module_from_spec(spec, flag="--supplementary-*-code", module_prefix="_rift_plugin_"):
    """Resolve *spec* (file path or dotted name) to an imported module.

    *flag* is used only in error text, so the user sees the option they actually passed.
    """
    if not spec:
        raise ValueError("{}: empty plugin spec".format(flag))
    if spec_looks_like_path(spec):
        if not os.path.isfile(spec):
            raise FileNotFoundError("{}: file not found: {!r}".format(flag, spec))
        module_name = module_prefix + os.path.splitext(os.path.basename(spec))[0]
        loader_spec = importlib.util.spec_from_file_location(module_name, spec)
        if loader_spec is None or loader_spec.loader is None:
            raise ImportError("Could not build import spec for {!r}".format(spec))
        module = importlib.util.module_from_spec(loader_spec)
        sys.modules[module_name] = module
        loader_spec.loader.exec_module(module)
        return module
    return importlib.import_module(spec)


def parse_ini(ini_path, flag="--supplementary-*-ini"):
    """Parse an optional ini file, preserving key case.  ``None``/empty -> ``None``."""
    if not ini_path:
        return None
    if not os.path.isfile(ini_path):
        raise FileNotFoundError("{}: file not found: {!r}".format(flag, ini_path))
    cfg = configparser.ConfigParser()
    cfg.optionxform = str  # preserve case
    cfg.read(ini_path)
    return cfg


def module_provenance(module):
    """(path, sha256) of a loaded module's source file, for a startup banner.

    Either may be ``None`` (built-in or namespace module, unreadable file)."""
    path = getattr(module, "__file__", None)
    if not path or not os.path.isfile(path):
        return path, None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return path, h.hexdigest()
