"""Simulation archive database — schema and queue interfaces.

This module defines the v2 archive layout described in DESIGN.md:
manifest.json + index.jsonl + per-sim status.json/summary.json under
sims/<name>/, with frozen code under code/, and an explicit split
between RequestQueue (decides what to submit) and RunQueue (actually
runs the generator).

The existing classes in BaseManager.py / CondorManager.py /
SlurmManager.py keep working unchanged; this module is additive. Real
condor/slurm queue implementations should land here as small
subclasses of RequestQueue / RunQueue.

Status:
    - Manifest, Index, StatusRecord, code-freeze: implemented.
    - LocalRequestQueue + LocalRunQueue: implemented (no schedd; runs
      the frozen generator inline). Used by examples/sim_database_hello.py
      and as a smoke-test fixture.
    - CondorRequestQueue / SlurmRunQueue / CondorRunQueue: stubs that
      raise NotImplementedError. Flesh out by porting the existing
      CondorManager logic onto these interfaces.
"""

from __future__ import annotations

import datetime
import inspect
import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

# Re-export the FSM constants so callers don't have to import BaseManager.
from .BaseManager import QUEUE_STATES  # noqa: F401

SCHEMA_VERSION = 1
DEFAULT_GENERATOR_FILE = "generator.py"
DEFAULT_SUMMARIZER_FILE = "summarizer.py"
DEFAULT_SAME_Q_FILE = "same_q.py"
DEFAULT_LOOKUP_KEY_FILE = "lookup_key.py"


# Sentinel singletons used inside dedup buckets when a parameter set is
# unhashable (lookup_key returns e.g. a dict). We fall back to the
# string repr in that case.
def _safe_hashable(x: Any) -> Any:
    try:
        hash(x)
        return x
    except TypeError:
        return ("__unhashable__", repr(x))


def _default_same_q(a: Any, b: Any) -> bool:
    return a == b


def _default_lookup_key(p: Any) -> Any:
    return str(p)


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

class Manifest:
    """archive-level metadata stored at <base>/manifest.json."""

    FILENAME = "manifest.json"

    def __init__(self, data: Dict[str, Any]):
        self.data = data

    @classmethod
    def new(cls, name: str,
            request_queue_kind: str,
            run_queue_kind: str,
            generator_entrypoint: str = "generator:run",
            summarizer_entrypoint: Optional[str] = None,
            same_q_entrypoint: Optional[str] = None,
            lookup_key_entrypoint: Optional[str] = None,
            params_schema: Optional[Dict[str, Any]] = None,
            summary_schema: Optional[Dict[str, Any]] = None,
            rift_version: Optional[str] = None,
            request_queue_extra: Optional[Dict[str, Any]] = None,
            run_queue_extra: Optional[Dict[str, Any]] = None,
            ) -> "Manifest":
        data: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "name": name,
            "created_at": _now(),
            "rift_version": rift_version or _detect_rift_version(),
            "code": {
                "generator": "code/" + DEFAULT_GENERATOR_FILE,
                "generator_entrypoint": generator_entrypoint,
            },
            "request_queue": {"kind": request_queue_kind,
                              "extra": request_queue_extra or {}},
            "run_queue":     {"kind": run_queue_kind,
                              "extra": run_queue_extra or {}},
        }
        if summarizer_entrypoint is not None:
            data["code"]["summarizer"] = "code/" + DEFAULT_SUMMARIZER_FILE
            data["code"]["summarizer_entrypoint"] = summarizer_entrypoint
        if same_q_entrypoint is not None:
            data["code"]["same_q"] = "code/" + DEFAULT_SAME_Q_FILE
            data["code"]["same_q_entrypoint"] = same_q_entrypoint
        if lookup_key_entrypoint is not None:
            data["code"]["lookup_key"] = "code/" + DEFAULT_LOOKUP_KEY_FILE
            data["code"]["lookup_key_entrypoint"] = lookup_key_entrypoint
        if params_schema is not None:
            data["params_schema"] = params_schema
        if summary_schema is not None:
            data["summary_schema"] = summary_schema
        return cls(data)

    def write(self, base: Union[str, Path]) -> None:
        path = Path(base) / self.FILENAME
        path.write_text(json.dumps(self.data, indent=2, sort_keys=True) + "\n")

    @classmethod
    def read(cls, base: Union[str, Path]) -> "Manifest":
        path = Path(base) / cls.FILENAME
        return cls(json.loads(path.read_text()))


def _detect_rift_version() -> Optional[str]:
    try:
        from importlib.metadata import version as _v
        return _v("RIFT")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Index
# ---------------------------------------------------------------------------

class Index:
    """One JSON object per line, one line per sim. The canonical, cheap
    view of the archive. Status updates and registrations rewrite the
    file under a simple file lock (not implemented yet — single-writer
    assumption is documented for now)."""

    FILENAME = "index.jsonl"

    def __init__(self, base: Union[str, Path]):
        self.base = Path(base)

    @property
    def path(self) -> Path:
        return self.base / self.FILENAME

    def all(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text().splitlines() if line]

    def by_name(self, name: str) -> Optional[Dict[str, Any]]:
        for row in self.all():
            if row.get("name") == name:
                return row
        return None

    def with_status(self, status: str) -> List[Dict[str, Any]]:
        return [r for r in self.all() if r.get("status") == status]

    def upsert(self, row: Dict[str, Any]) -> None:
        rows = self.all()
        for i, existing in enumerate(rows):
            if existing.get("name") == row["name"]:
                rows[i] = row
                break
        else:
            rows.append(row)
        self._write_all(rows)

    def remove(self, name: str) -> None:
        self._write_all([r for r in self.all() if r.get("name") != name])

    def _write_all(self, rows: Iterable[Dict[str, Any]]) -> None:
        tmp = self.path.with_suffix(".jsonl.tmp")
        with open(tmp, "w") as f:
            for row in rows:
                f.write(json.dumps(row, sort_keys=True) + "\n")
        os.replace(tmp, self.path)   # atomic on POSIX


# ---------------------------------------------------------------------------
# Per-sim status
# ---------------------------------------------------------------------------

class StatusRecord:
    """sims/<name>/status.json. The full per-sim story; the index is a
    projection of this. Mutations go through transition() so the history
    log and the FSM stay consistent."""

    FILENAME = "status.json"

    def __init__(self, data: Dict[str, Any]):
        self.data = data

    @classmethod
    def new(cls, name: str, params: Any, target_level: int = 1) -> "StatusRecord":
        ts = _now()
        return cls({
            "name": name,
            "params": params,
            "status": "ready",
            "target_level": int(target_level),
            "current_level": 0,
            "levels": [],   # list of {"level": int, "output_path": str, "completed_at": str}
            "history": [{"status": "ready", "ts": ts}],
            "request_queue": None,
            "run_queue": None,
            "started_at": None,
            "completed_at": None,
        })

    def transition(self, new_status: str, **fields: Any) -> None:
        if new_status not in QUEUE_STATES:
            raise ValueError("Unknown status %r; expected one of %s" %
                             (new_status, QUEUE_STATES))
        self.data["status"] = new_status
        self.data["history"].append({"status": new_status, "ts": _now()})
        if new_status == "running" and self.data["started_at"] is None:
            self.data["started_at"] = _now()
        if new_status == "complete" and self.data["completed_at"] is None:
            self.data["completed_at"] = _now()
        for k, v in fields.items():
            self.data[k] = v

    def append_level(self, level: int, output_path: str) -> None:
        """Record a successful level computation. Updates current_level
        and the levels[] list. Caller should subsequently transition
        to 'complete' or 'refine_ready' as appropriate."""
        self.data["levels"].append({
            "level": int(level),
            "output_path": output_path,
            "completed_at": _now(),
        })
        self.data["current_level"] = max(self.data.get("current_level", 0), int(level))

    def bump_target(self, target_level: int) -> bool:
        """Raise target_level if `target_level` is higher than the current
        target. Returns True iff a bump occurred."""
        cur = self.data.get("target_level", 0)
        if int(target_level) > cur:
            self.data["target_level"] = int(target_level)
            return True
        return False

    def needs_more_work(self) -> bool:
        return self.data.get("current_level", 0) < self.data.get("target_level", 0)

    def write(self, sim_dir: Union[str, Path]) -> None:
        Path(sim_dir, self.FILENAME).write_text(
            json.dumps(self.data, indent=2, sort_keys=True) + "\n")

    @classmethod
    def read(cls, sim_dir: Union[str, Path]) -> "StatusRecord":
        return cls(json.loads(Path(sim_dir, cls.FILENAME).read_text()))


# ---------------------------------------------------------------------------
# Code freezing
# ---------------------------------------------------------------------------

CodeSpec = Union[Callable[..., Any], str, os.PathLike, Dict[str, Any]]


def freeze_code(spec: CodeSpec, code_dir: Union[str, Path],
                target_filename: str = DEFAULT_GENERATOR_FILE,
                ) -> str:
    """Snapshot the generator (or summarizer) source into <code_dir>/.

    Returns the entrypoint string ("module:callable") that should be
    stored in the manifest. Three input shapes are accepted:

      * callable: inspect.getsource() is captured into a single-file
        module named after `target_filename`. The function must be
        self-contained (no closure captures, no module-relative imports
        beyond the standard library and explicitly-listed deps).
      * path: a .py file is copied verbatim.
      * dict: {"module_path": ..., "entrypoint": "mod:fn",
               "extra_files": [...]}. All listed files are copied; the
        named module becomes the canonical generator.
    """
    code_dir = Path(code_dir)
    code_dir.mkdir(parents=True, exist_ok=True)

    if callable(spec):
        src = inspect.getsource(spec)
        # Dedent so `def` starts at column 0 (matters when capturing a
        # local function defined inside a test).
        src = textwrap.dedent(src)
        out = code_dir / target_filename
        out.write_text(src)
        module_name = Path(target_filename).stem
        return "{}:{}".format(module_name, spec.__name__)

    if isinstance(spec, (str, os.PathLike)):
        src_path = Path(spec)
        if not src_path.is_file():
            raise FileNotFoundError(src_path)
        shutil.copy(src_path, code_dir / target_filename)
        # Caller is responsible for naming the entrypoint.
        return "{}:run".format(Path(target_filename).stem)

    if isinstance(spec, dict):
        module_path = Path(spec["module_path"])
        shutil.copy(module_path, code_dir / target_filename)
        for extra in spec.get("extra_files", []):
            shutil.copy(extra, code_dir / Path(extra).name)
        return spec.get("entrypoint", "{}:run".format(Path(target_filename).stem))

    raise TypeError("spec must be a callable, a path, or a dict; got %r" %
                    type(spec))


def load_entrypoint(code_dir: Union[str, Path], entrypoint: str) -> Callable[..., Any]:
    """Resolve a 'module:callable' entrypoint against <code_dir>/. Used
    by workers (and by LocalRunQueue for the no-schedd path)."""
    module_name, _, attr = entrypoint.partition(":")
    if not attr:
        raise ValueError("entrypoint must be 'module:callable'; got %r" % entrypoint)
    code_dir = str(Path(code_dir).resolve())
    if code_dir not in sys.path:
        sys.path.insert(0, code_dir)
    import importlib
    if module_name in sys.modules:
        importlib.reload(sys.modules[module_name])
    mod = importlib.import_module(module_name)
    return getattr(mod, attr)


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------

class Archive:
    """Thin facade over the on-disk layout. Owns the FSM; queues report
    state, this class applies it. Construction either creates a new
    archive (when `manifest` is given) or rehydrates an existing one
    (when only `base_location` is given)."""

    def __init__(self, base_location: Union[str, Path],
                 manifest: Optional[Manifest] = None,
                 request_queue: Optional["RequestQueue"] = None,
                 run_queue: Optional["RunQueue"] = None,
                 generator_spec: Optional[CodeSpec] = None,
                 summarizer_spec: Optional[CodeSpec] = None,
                 same_q_spec: Optional[CodeSpec] = None,
                 lookup_key_spec: Optional[CodeSpec] = None):
        self.base = Path(base_location)
        self.request_queue = request_queue
        self.run_queue = run_queue
        if manifest is not None:
            self._initialize_new(manifest, generator_spec, summarizer_spec,
                                 same_q_spec, lookup_key_spec)
        else:
            self.manifest = Manifest.read(self.base)
        self.index = Index(self.base)
        # Resolve dedup callables (frozen versions if available, else defaults).
        self._same_q: Callable[[Any, Any], bool] = self._resolve_same_q()
        self._lookup_key: Callable[[Any], Any] = self._resolve_lookup_key()
        # Build the in-memory dedup index from index.jsonl. Buckets are
        # {hashable_lookup_key: [sim_name, ...]}; values are kept in
        # registration order so the first match wins under same_q.
        # For legacy rows missing a stored lookup_key, compute it now.
        self._dedup_buckets: Dict[Any, List[str]] = {}
        for row in self.index.all():
            if "lookup_key" in row:
                key = row["lookup_key"]
            else:
                try:
                    key = self._lookup_key(row.get("params"))
                except Exception:
                    key = row.get("name")
            self._dedup_buckets.setdefault(_safe_hashable(key), []).append(row["name"])

    # ---- bootstrap / rehydrate -------------------------------------------
    def _initialize_new(self, manifest: Manifest,
                        generator_spec: Optional[CodeSpec],
                        summarizer_spec: Optional[CodeSpec],
                        same_q_spec: Optional[CodeSpec],
                        lookup_key_spec: Optional[CodeSpec]) -> None:
        self.base.mkdir(parents=True, exist_ok=True)
        for sub in ("code", "sims", "request_queue", "run_queue"):
            (self.base / sub).mkdir(exist_ok=True)
        if generator_spec is None:
            raise ValueError("generator_spec is required when creating a new archive")
        gen_entry = freeze_code(generator_spec, self.base / "code",
                                target_filename=DEFAULT_GENERATOR_FILE)
        manifest.data["code"]["generator_entrypoint"] = gen_entry
        if summarizer_spec is not None:
            sum_entry = freeze_code(summarizer_spec, self.base / "code",
                                    target_filename=DEFAULT_SUMMARIZER_FILE)
            manifest.data["code"]["summarizer"] = "code/" + DEFAULT_SUMMARIZER_FILE
            manifest.data["code"]["summarizer_entrypoint"] = sum_entry
        if same_q_spec is not None:
            sq_entry = freeze_code(same_q_spec, self.base / "code",
                                   target_filename=DEFAULT_SAME_Q_FILE)
            manifest.data["code"]["same_q"] = "code/" + DEFAULT_SAME_Q_FILE
            manifest.data["code"]["same_q_entrypoint"] = sq_entry
        if lookup_key_spec is not None:
            lk_entry = freeze_code(lookup_key_spec, self.base / "code",
                                   target_filename=DEFAULT_LOOKUP_KEY_FILE)
            manifest.data["code"]["lookup_key"] = "code/" + DEFAULT_LOOKUP_KEY_FILE
            manifest.data["code"]["lookup_key_entrypoint"] = lk_entry
        manifest.write(self.base)
        self.manifest = manifest

    # ---- callable resolution ---------------------------------------------
    def _resolve_same_q(self) -> Callable[[Any, Any], bool]:
        ep = self.manifest.data.get("code", {}).get("same_q_entrypoint")
        if not ep:
            return _default_same_q
        return load_entrypoint(self.base / "code", ep)

    def _resolve_lookup_key(self) -> Callable[[Any], Any]:
        ep = self.manifest.data.get("code", {}).get("lookup_key_entrypoint")
        if not ep:
            return _default_lookup_key
        return load_entrypoint(self.base / "code", ep)

    # ---- registration -----------------------------------------------------
    def sim_dir(self, name: str) -> Path:
        return self.base / "sims" / name

    def find_existing(self, params: Any) -> Optional[str]:
        """Return the name of an existing sim whose params satisfy
        same_q(params, existing.params), else None. O(1) average via
        the lookup_key bucket; O(K) worst case where K is the bucket
        size (typically 1)."""
        key = _safe_hashable(self._lookup_key(params))
        for cand_name in self._dedup_buckets.get(key, []):
            cand = self.index.by_name(cand_name)
            if cand is None:
                continue
            try:
                if self._same_q(params, cand["params"]):
                    return cand_name
            except Exception:
                continue
        return None

    def register(self, params: Any, target_level: int = 1,
                 name: Optional[str] = None) -> str:
        """Idempotent under same_q + level. Behavior:

          * sim does not exist:    register fresh with `target_level=N`
          * sim exists, current_level >= N:  return name, no work
          * sim exists, current_level < N:   bump target_level to
            max(existing, N), transition to 'refine_ready', return name

        Returns the (existing or newly allocated) sim_name in all cases.
        """
        target_level = int(target_level)
        existing = self.find_existing(params)
        if existing is not None:
            self._maybe_bump_target(existing, target_level)
            return existing
        if name is None:
            name = str(len(list((self.base / "sims").iterdir())) + 1)
        sd = self.sim_dir(name)
        sd.mkdir(parents=True, exist_ok=True)
        (sd / "logs").mkdir(exist_ok=True)
        (sd / "params.json").write_text(json.dumps(params) + "\n")
        rec = StatusRecord.new(name, params, target_level=target_level)
        rec.write(sd)
        lk = self._lookup_key(params)
        self.index.upsert({"name": name, "params": params,
                           "status": "ready", "summary": None,
                           "lookup_key": lk,
                           "target_level": target_level,
                           "current_level": 0})
        self._dedup_buckets.setdefault(_safe_hashable(lk), []).append(name)
        return name

    def refine(self, name: str, target_level: int) -> bool:
        """Explicit refinement request. Bumps the target_level if needed
        and (when the sim already had output) transitions it to
        'refine_ready'. Returns True iff a bump occurred."""
        return self._maybe_bump_target(name, int(target_level))

    def _maybe_bump_target(self, name: str, target_level: int) -> bool:
        rec = StatusRecord.read(self.sim_dir(name))
        if not rec.bump_target(target_level):
            return False
        # Decide the new status. If the sim already produced output for
        # at least one level, it's now 'refine_ready'. If it had no
        # levels yet, leave it in whatever pre-run state it was in
        # (typically 'ready' or 'submit_ready').
        new_status = rec.data["status"]
        if rec.data["current_level"] >= 1 and rec.data["status"] == "complete":
            new_status = "refine_ready"
        rec.transition(new_status)  # records the history line even if status unchanged
        rec.data["status"] = new_status
        rec.write(self.sim_dir(name))
        row = self.index.by_name(name) or {"name": name}
        row["status"] = new_status
        row["target_level"] = rec.data["target_level"]
        row["current_level"] = rec.data["current_level"]
        self.index.upsert(row)
        return True

    # ---- transitions ------------------------------------------------------
    def transition(self, name: str, new_status: str, **fields: Any) -> None:
        rec = StatusRecord.read(self.sim_dir(name))
        rec.transition(new_status, **fields)
        rec.write(self.sim_dir(name))
        row = self.index.by_name(name) or {"name": name}
        row["status"] = new_status
        self.index.upsert(row)

    def update_summary(self, name: str, summary: Dict[str, Any]) -> None:
        sd = self.sim_dir(name)
        (sd / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n")
        row = self.index.by_name(name) or {"name": name}
        row["summary"] = summary
        self.index.upsert(row)

    # ---- introspection ----------------------------------------------------
    def with_status(self, status: str) -> List[str]:
        return [r["name"] for r in self.index.with_status(status)]

    def get_status(self, name: str) -> Optional[str]:
        r = self.index.by_name(name)
        return r["status"] if r else None

    def load_generator(self) -> Callable[..., Any]:
        return load_entrypoint(self.base / "code",
                               self.manifest.data["code"]["generator_entrypoint"])

    def load_summarizer(self) -> Optional[Callable[..., Any]]:
        ep = self.manifest.data.get("code", {}).get("summarizer_entrypoint")
        if not ep:
            return None
        return load_entrypoint(self.base / "code", ep)

    # ---- file-transfer helpers (for run pools without shared FS) ---------
    def transfer_input_files_for(self, sim_name: str, level: int) -> List[str]:
        """Files a worker needs on the execute host to compute (sim, level).

        Suitable as the value of condor's `transfer_input_files`. Paths
        are returned as filesystem paths on the submit side; condor
        flattens basenames into the worker sandbox cwd.

        Includes:
          * <archive>/code   — the whole frozen code directory
          * <archive>/sims/<name>/params.json
          * <archive>/sims/<name>/level_1.json ... level_<level-1>.json
            (whichever are present on disk)
        """
        sd = self.sim_dir(sim_name)
        files: List[str] = [str(self.base / "code")]
        params_path = sd / "params.json"
        if params_path.exists():
            files.append(str(params_path))
        for i in range(1, int(level)):
            p = sd / "level_{}.json".format(i)
            if p.exists():
                files.append(str(p))
        return files

    def expected_output(self, sim_name: str, level: int) -> Tuple[str, str]:
        """Return (output_basename, absolute_remap_target) for a (sim, level)
        worker. Use the basename in `transfer_output_files` and the pair
        in `transfer_output_remaps` so condor places the worker's output
        at the canonical archive location on the submit node."""
        basename = "level_{}.json".format(int(level))
        target = str(self.sim_dir(sim_name) / basename)
        return basename, target

    def worker_bootstrap_script(self) -> str:
        """Return a self-contained Python bootstrap script (as a string)
        that the run pool's executable should be set to. The script
        expects argv: --sim-name <n> --level <N> --prev-levels FILE [...].
        Standard input file layout: code/ in sandbox, params.json in
        sandbox, prev-level files in sandbox by basename. Output is
        written to cwd as level_<N>.json."""
        ep = self.manifest.data["code"]["generator_entrypoint"]
        module_name, _, fn_name = ep.partition(":")
        return textwrap.dedent('''\
            #!/usr/bin/env python3
            """Auto-generated worker bootstrap for the simulation_manager
            v2 archive. Loads the frozen generator and runs one level."""
            import argparse, json, os, sys

            ap = argparse.ArgumentParser()
            ap.add_argument("--sim-name", required=True)
            ap.add_argument("--level", type=int, required=True)
            ap.add_argument("--prev-levels", nargs="*", default=[])
            args = ap.parse_args()

            sys.path.insert(0, "code")
            from {module_name} import {fn_name} as _gen

            with open("params.json") as f:
                params = json.load(f)

            prev = [os.path.abspath(p) for p in args.prev_levels]
            _gen(params, sim_dir=os.getcwd(), level=args.level, prev_levels=prev)
        ''').format(module_name=module_name, fn_name=fn_name)


# ---------------------------------------------------------------------------
# Queue interfaces
# ---------------------------------------------------------------------------

class RequestQueue:
    """Decides which sims should next be sent to the run queue, and
    tracks their state in the request system. Subclasses set `kind`."""
    kind: str = "abstract"

    def submit_pending(self, archive: Archive) -> List[str]:
        raise NotImplementedError

    def poll(self, archive: Archive) -> Dict[str, str]:
        raise NotImplementedError


class RunQueue:
    """Actually runs sims (writes their output)."""
    kind: str = "abstract"

    def build_worker(self, archive: Archive, sim_name: str) -> str:
        raise NotImplementedError

    def submit(self, archive: Archive, sim_names: Iterable[str]
               ) -> List[Tuple[str, str]]:
        raise NotImplementedError

    def poll(self, archive: Archive, sim_names: Iterable[str]
             ) -> Dict[str, str]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Local queues (no schedd) — used for tests and worked examples
# ---------------------------------------------------------------------------

class LocalRequestQueue(RequestQueue):
    """Trivial pass-through: every sim that needs work ('ready' or
    'refine_ready') is immediately handed to the run queue."""
    kind = "local"

    def __init__(self, run_queue: "LocalRunQueue"):
        self.run_queue = run_queue

    def submit_pending(self, archive: Archive) -> List[str]:
        pending = (archive.with_status("ready")
                   + archive.with_status("refine_ready"))
        for n in pending:
            archive.transition(n, "submit_ready")
        if pending:
            self.run_queue.submit(archive, pending)
        return pending

    def poll(self, archive: Archive) -> Dict[str, str]:
        return {n: archive.get_status(n) for n in archive.with_status("running")}


class LocalRunQueue(RunQueue):
    """Runs the frozen generator inline in the current process. Computes
    every missing level (current_level + 1 ... target_level) for each
    submitted sim. Useful for end-to-end tests without a cluster."""
    kind = "local"

    def build_worker(self, archive: Archive, sim_name: str) -> str:
        # No external script needed: we call the generator in-process.
        return ""

    def submit(self, archive: Archive, sim_names: Iterable[str]
               ) -> List[Tuple[str, str]]:
        gen = archive.load_generator()
        summarizer = archive.load_summarizer()
        results: List[Tuple[str, str]] = []
        for name in sim_names:
            sd = archive.sim_dir(name)
            rec = StatusRecord.read(sd)
            archive.transition(name, "running",
                               run_queue={"kind": self.kind, "job_id": name})
            params = json.loads((sd / "params.json").read_text())
            target = rec.data["target_level"]
            current = rec.data["current_level"]
            stuck = False
            for lvl in range(current + 1, target + 1):
                prev_levels = [str(sd / "level_{}.json".format(i))
                               for i in range(1, lvl)]
                try:
                    gen(params, sim_dir=str(sd), level=lvl,
                        prev_levels=prev_levels)
                except Exception as exc:
                    rec = StatusRecord.read(sd)
                    rec.transition("stuck",
                                   run_queue={"kind": self.kind,
                                              "job_id": name,
                                              "error": str(exc)})
                    rec.write(sd)
                    archive.index.upsert({**(archive.index.by_name(name) or {"name": name}),
                                          "status": "stuck"})
                    stuck = True
                    break
                level_output = sd / "level_{}.json".format(lvl)
                # Generator may write to a different filename; record
                # whatever it produced for this level.
                if not level_output.exists():
                    candidates = sorted(sd.glob("level_{}*".format(lvl)))
                    if candidates:
                        level_output = candidates[0]
                rec = StatusRecord.read(sd)
                rec.append_level(lvl, str(level_output.relative_to(archive.base))
                                 if level_output.exists() else "")
                rec.write(sd)
            if stuck:
                results.append((name, "stuck"))
                continue
            archive.transition(name, "complete")
            # Re-read to attach final level info on the index row.
            rec = StatusRecord.read(sd)
            row = archive.index.by_name(name) or {"name": name}
            row["current_level"] = rec.data["current_level"]
            row["target_level"] = rec.data["target_level"]
            row["status"] = "complete"
            archive.index.upsert(row)
            if summarizer is not None:
                level_paths = [str(archive.base / l["output_path"])
                               for l in rec.data["levels"] if l["output_path"]]
                try:
                    summary = summarizer(sim_dir=str(sd), params=params,
                                         levels=level_paths)
                except TypeError:
                    # Summarizer may have the simpler (sim_dir, params) signature.
                    summary = summarizer(sim_dir=str(sd), params=params)
                except Exception:
                    summary = None
                if summary is not None:
                    archive.update_summary(name, summary)
            results.append((name, "complete"))
        return results

    def poll(self, archive: Archive, sim_names: Iterable[str]
             ) -> Dict[str, str]:
        # Local runs are synchronous; a poll after submit always shows complete.
        return {n: archive.get_status(n) for n in sim_names}


# ---------------------------------------------------------------------------
# Dual-condor queue stubs
# ---------------------------------------------------------------------------
#
# Topology:
#
#   request pool (e.g. CIT submit host)         run pool (e.g. OSG / remote)
#   ┌──────────────────────────────┐            ┌──────────────────────────────┐
#   │ planner DAG                  │            │ per-sim, per-level workers   │
#   │  - scout: pick what to do    │            │  - read frozen code/         │
#   │  - DualCondorRequestQueue    │  submit   │  - run gen(params, sd, lvl)  │
#   │    builds & submits sub-DAG  │ ────────> │  - write level_<N>.json      │
#   │    via condor_submit_dag     │            │                              │
#   │    -name <run_pool_schedd>   │            │ shared FS: <base>/sims/...   │
#   │  - polls run-pool schedd via │ <──────── │                              │
#   │    htcondor.Schedd(run_pool) │  output    │                              │
#   └──────────────────────────────┘            └──────────────────────────────┘
#
# Both pools mount the same archive; communication is the filesystem.
# The classes below sketch the interfaces that need fleshing out;
# detailed pseudocode lives in the docstrings.

class DualCondorRequestQueue(RequestQueue):
    """Runs on the *request* condor pool. Selects pending work from the
    archive and submits per-sim, per-level work to the *run* pool via
    `condor_submit_dag -name <run_pool_schedd>`.

    Configuration in the manifest's request_queue.extra:
        request_pool : str   # accounting/scheduling for the planner DAG
        run_pool     : str   # value to pass as condor_submit_dag -name
        run_collector: str   # optional: collector host for htcondor.Schedd lookups
        dag_template : str   # path to a base submit description; we
                             # substitute per-sim macros at build time
        accounting_group / accounting_group_user : pass-through

    Pseudocode for submit_pending:

        ready_or_refine = archive.with_status('ready') + archive.with_status('refine_ready')
        if not ready_or_refine: return []
        # Build a sub-DAG with one node per (sim, missing-level).
        nodes = []
        for sim_name in ready_or_refine:
            rec = StatusRecord.read(archive.sim_dir(sim_name))
            for lvl in range(rec.data['current_level']+1,
                             rec.data['target_level']+1):
                nodes.append((sim_name, lvl))
        sub_dag = build_subdag_file(archive, nodes,
                                     submit_to_pool=self.run_pool)
        # condor_submit_dag against the RUN pool's schedd
        result = subprocess.run(
            ['condor_submit_dag', '-name', self.run_pool, '-f', sub_dag],
            check=True, capture_output=True, text=True)
        cluster_id = parse_cluster_id(result.stdout)
        # mark all submitted sims 'submit_ready' (running once the
        # run pool actually picks them up, observed via poll())
        for sim_name, _lvl in nodes:
            archive.transition(sim_name, 'submit_ready',
                               request_queue={'kind': 'condor',
                                              'pool': self.run_pool,
                                              'cluster_id': cluster_id})
        return list({n for n, _ in nodes})

    Pseudocode for poll:

        # Use the cached _htcondor_module from CondorManager; the same
        # bindings can talk to a remote schedd via Schedd(<collector>).
        from RIFT.simulation_manager.CondorManager import _htcondor_module
        schedd = _htcondor_module.Schedd(self.run_pool_collector)
        live = schedd.query(constraint='DAGManJobId == {}'.format(self.cluster_id),
                            projection=['ProcId', 'Args', 'JobStatus'])
        # match Args -> sim_name + level macros, update statuses.
    """
    kind = "condor"

    def __init__(self, request_pool: Optional[str] = None,
                 run_pool: Optional[str] = None,
                 run_collector: Optional[str] = None,
                 dag_template: Optional[str] = None,
                 **submit_kwargs: Any):
        self.request_pool = request_pool
        self.run_pool = run_pool
        self.run_collector = run_collector
        self.dag_template = dag_template
        self.submit_kwargs = submit_kwargs
        self.cluster_id: Optional[int] = None

    def submit_pending(self, archive: Archive) -> List[str]:
        raise NotImplementedError(
            "DualCondorRequestQueue.submit_pending: see docstring. Port "
            "the DAG-building logic from "
            "CondorManager.SimulationArchiveOnLocalDiskIntegratedCondorQueue."
            "generate_dag_for_all_ready_simulations and adjust the "
            "submit step to target the run pool's schedd via "
            "condor_submit_dag -name <run_pool>.")

    def poll(self, archive: Archive) -> Dict[str, str]:
        raise NotImplementedError(
            "DualCondorRequestQueue.poll: query the run pool's schedd "
            "via htcondor.Schedd(<run_collector>); reuse the "
            "_htcondor_module caching from CondorManager.")


class DualCondorRunQueue(RunQueue):
    """Runs on the *run* condor pool (or is targeted from the request
    pool via -name). Builds per-(sim, level) submit descriptions whose
    executable is a small bootstrap that loads the frozen generator
    and calls it with (params, sim_dir, level, prev_levels).

    Configuration in the manifest's run_queue.extra:
        run_pool         : str        # schedd name (matches DualCondorRequestQueue.run_pool)
        accounting_group / accounting_group_user
        request_memory   : int (MB)
        request_disk     : str (e.g. '4G')
        use_singularity  : bool
        singularity_image: str
        transfer_input_files : list[str]   # archive code/ + manifest
        getenv           : str        # RIFT_GETENV / RIFT_GETENV_OSG style

    Pseudocode for build_worker(sim_name, level):

        # The execute host has NO view of the archive directory. Use
        # condor file transfer to ferry the per-(sim, level) inputs in
        # and the level output back. All paths in the worker are
        # sandbox-relative; condor flattens basenames into cwd.

        sd = archive.sim_dir(sim_name)

        # 1) Bootstrap script — written once per archive, reused for
        #    every (sim, level) job. It expects --sim-name --level
        #    --prev-levels and reads ./params.json + ./level_<i>.json
        #    from the sandbox.
        worker_path = '<base>/run_queue/workers/bootstrap.py'
        write(worker_path, archive.worker_bootstrap_script())

        # 2) Per-(sim, level) condor submit description.
        prev_basenames = ['level_{}.json'.format(i) for i in range(1, level)
                          if (sd / 'level_{}.json'.format(i)).exists()]
        out_base, out_target = archive.expected_output(sim_name, level)
        sub = build_submit_description(
            universe='vanilla',
            executable=worker_path,
            arguments=['--sim-name', sim_name, '--level', level,
                       '--prev-levels'] + prev_basenames,
            transfer_input_files=archive.transfer_input_files_for(sim_name, level),
            transfer_output_files=[out_base],
            transfer_output_remaps='"{}={}"'.format(out_base, out_target),
            should_transfer_files='YES',
            when_to_transfer_output='ON_EXIT',
            request_memory=self.request_memory,
            request_disk=self.request_disk,
            getenv=os.environ.get('RIFT_GETENV', 'True'),
            log_dir='<base>/run_queue/logs',
            ...)
        sub_path = '<base>/run_queue/submit_files/{}_lvl{}.sub'.format(sim_name, level)
        write(sub_path, sub)
        return sub_path

    Pseudocode for submit(sim_names):

        # Build per-(sim, level) submit and assemble a DAG. Return the
        # mapping sim_name -> condor cluster id.
        nodes = []
        for sim in sim_names:
            rec = StatusRecord.read(archive.sim_dir(sim))
            for lvl in range(rec.data['current_level']+1, rec.data['target_level']+1):
                sub = self.build_worker(archive, sim, level=lvl)
                nodes.append((sim, lvl, sub))
        dag = write_dag('<base>/run_queue/dags/run_<batch>.dag', nodes)
        result = subprocess.run(['condor_submit_dag', dag], check=True, ...)
        return [(sim, parse_cluster_id(result.stdout)) for sim, _, _ in nodes]

    Pseudocode for poll(sim_names):

        from RIFT.simulation_manager.CondorManager import _htcondor_module
        schedd = _htcondor_module.Schedd()  # local; the run pool itself
        # Map cluster ids back to sim names via the manifest of submitted
        # work; missing from queue + level_<N>.json present on disk =>
        # 'complete' (or 'refine_ready' if more levels remain).
    """
    kind = "condor"

    def __init__(self, run_pool: Optional[str] = None,
                 request_memory: int = 4096,
                 request_disk: str = "4G",
                 use_singularity: bool = False,
                 singularity_image: Optional[str] = None,
                 transfer_input_files: Optional[List[str]] = None,
                 **submit_kwargs: Any):
        self.run_pool = run_pool
        self.request_memory = request_memory
        self.request_disk = request_disk
        self.use_singularity = use_singularity
        self.singularity_image = singularity_image
        self.transfer_input_files = transfer_input_files or []
        self.submit_kwargs = submit_kwargs

    def build_worker(self, archive: Archive, sim_name: str,
                     level: int = 1) -> str:
        raise NotImplementedError("DualCondorRunQueue.build_worker: see docstring")

    def submit(self, archive: Archive, sim_names: Iterable[str]
               ) -> List[Tuple[str, str]]:
        raise NotImplementedError("DualCondorRunQueue.submit: see docstring")

    def poll(self, archive: Archive, sim_names: Iterable[str]
             ) -> Dict[str, str]:
        raise NotImplementedError("DualCondorRunQueue.poll: see docstring")
