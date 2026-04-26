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
    def new(cls, name: str, params: Any) -> "StatusRecord":
        ts = _now()
        return cls({
            "name": name,
            "params": params,
            "status": "ready",
            "history": [{"status": "ready", "ts": ts}],
            "request_queue": None,
            "run_queue": None,
            "output": {"path": None, "size": None, "sha256": None},
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

    def register(self, params: Any, name: Optional[str] = None) -> str:
        """Idempotent under same_q: if an existing sim matches `params`,
        return its name unchanged. Otherwise allocate a new sim slot,
        write params/status, and append to the index."""
        existing = self.find_existing(params)
        if existing is not None:
            return existing
        if name is None:
            name = str(len(list((self.base / "sims").iterdir())) + 1)
        sd = self.sim_dir(name)
        sd.mkdir(parents=True, exist_ok=True)
        (sd / "logs").mkdir(exist_ok=True)
        (sd / "params.json").write_text(json.dumps(params) + "\n")
        rec = StatusRecord.new(name, params)
        rec.write(sd)
        lk = self._lookup_key(params)
        self.index.upsert({"name": name, "params": params,
                           "status": "ready", "summary": None,
                           "lookup_key": lk})
        self._dedup_buckets.setdefault(_safe_hashable(lk), []).append(name)
        return name

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
    """Trivial pass-through: every 'ready' sim is immediately handed to
    the run queue. The run queue does the actual work."""
    kind = "local"

    def __init__(self, run_queue: "LocalRunQueue"):
        self.run_queue = run_queue

    def submit_pending(self, archive: Archive) -> List[str]:
        ready = archive.with_status("ready")
        for n in ready:
            archive.transition(n, "submit_ready")
        if ready:
            self.run_queue.submit(archive, ready)
        return ready

    def poll(self, archive: Archive) -> Dict[str, str]:
        return {n: archive.get_status(n) for n in archive.with_status("running")}


class LocalRunQueue(RunQueue):
    """Runs the frozen generator inline in the current process. Useful
    for testing the archive layout end-to-end without any cluster."""
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
            archive.transition(name, "running",
                               run_queue={"kind": self.kind, "job_id": name})
            params = json.loads((sd / "params.json").read_text())
            try:
                gen(params, sim_dir=str(sd))
            except Exception as exc:
                archive.transition(name, "stuck",
                                   run_queue={"kind": self.kind,
                                              "job_id": name,
                                              "error": str(exc)})
                results.append((name, "stuck"))
                continue
            output_path = sd / "output"
            # Generators may pick any extension; record the first matching file.
            output_files = sorted(p for p in sd.iterdir()
                                  if p.name.startswith("output"))
            output_record = {
                "path": (str(output_files[0].relative_to(archive.base))
                         if output_files else None),
                "size": output_files[0].stat().st_size if output_files else None,
                "sha256": None,
            }
            archive.transition(name, "complete", output=output_record)
            if summarizer is not None:
                try:
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
# Cluster queue stubs — to be fleshed out
# ---------------------------------------------------------------------------

class CondorRequestQueue(RequestQueue):
    """Build a condor DAG over all 'ready' sims, condor_submit_dag,
    track via DAGManJobId. Port the existing CondorManager logic onto
    this interface."""
    kind = "condor"

    def __init__(self, **submit_kwargs: Any):
        self.submit_kwargs = submit_kwargs

    def submit_pending(self, archive: Archive) -> List[str]:
        raise NotImplementedError(
            "CondorRequestQueue.submit_pending: port logic from "
            "CondorManager.SimulationArchiveOnLocalDiskIntegratedCondorQueue."
            "generate_dag_for_all_ready_simulations + submit_dag.")

    def poll(self, archive: Archive) -> Dict[str, str]:
        raise NotImplementedError(
            "CondorRequestQueue.poll: port logic from "
            "CondorManager.refresh_status_from_condor.")


class SlurmRunQueue(RunQueue):
    """Per-sim worker scripts written under run_queue/workers/, sbatched
    against a configured partition. Build on simple_slurm or pyslurmutils
    (already optionally imported in SlurmManager)."""
    kind = "slurm"

    def __init__(self, partition: str, **slurm_kwargs: Any):
        self.partition = partition
        self.slurm_kwargs = slurm_kwargs

    def build_worker(self, archive: Archive, sim_name: str) -> str:
        raise NotImplementedError("SlurmRunQueue.build_worker: stub")

    def submit(self, archive: Archive, sim_names: Iterable[str]
               ) -> List[Tuple[str, str]]:
        raise NotImplementedError("SlurmRunQueue.submit: stub")

    def poll(self, archive: Archive, sim_names: Iterable[str]
             ) -> Dict[str, str]:
        raise NotImplementedError("SlurmRunQueue.poll: stub")


class CondorRunQueue(RunQueue):
    """Per-sim worker as a vanilla-universe condor job. Reuse most of
    the existing CondorManager submit-file logic."""
    kind = "condor"

    def build_worker(self, archive: Archive, sim_name: str) -> str:
        raise NotImplementedError("CondorRunQueue.build_worker: stub")

    def submit(self, archive: Archive, sim_names: Iterable[str]
               ) -> List[Tuple[str, str]]:
        raise NotImplementedError("CondorRunQueue.submit: stub")

    def poll(self, archive: Archive, sim_names: Iterable[str]
             ) -> Dict[str, str]:
        raise NotImplementedError("CondorRunQueue.poll: stub")
