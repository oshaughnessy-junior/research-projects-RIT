"""The in-process transport: one supervisor drives a ledger run on one host.

Loop: one controller tick; if it asks for rows, one engine process per lease, side by
side.  The supervisor returns when the run stops, when its time budget ends, or when an
engine log shows the likelihood left the NoLoop path.  Started again on the same
directory it resumes, because the directory holds the whole state.

After an engine exits, by what its shard and its in-flight file hold:
  killed by this supervisor        nothing is recorded; the next lease covers the missing rows
  every lease row has a line       nothing to do (the engine reports its own failed points)
  died on a key, engine known good a strike against that key; two strikes make it FAILED (L = 0)
  anything else with rows missing  the engine is not working: no row is marked

A launch is one round of engines, one per lease.  Three launches in a row in which no
engine finished its lease or earned a strike end the supervisor with exit status 6.

A point is marked failed only on evidence about that point.  The engine's own line is
evidence.  A death while a key was in flight is evidence only if the engine wrote a row
in the same launch, which shows that it works.  A key on which a launch died without
that is moved to the end of the next lease, so the launch after shows whether the engine
works on other points.  When only such keys are left, the controller puts a key that
already has a row first in the lease, so that launch can still show the engine works.
An engine that cannot start, has lost its GPU, or ends before its last point therefore
never turns points into zeros: a lost device earns the key in flight one strike at most.
An engine that writes no in-flight file gets no strikes.

Engine logs are read for lines off the NoLoop path after each lease, and all of them
again at every start, compressed ones included: a supervisor ended in mid-lease never
read its last logs.

`chaos` kills engines on purpose, for tests: `engine=N` engine processes on alternate
launches, each after a random delay between `lo` and `hi` seconds, or, with `rows=K`,
once its shard holds K lines.  Every kill is logged
in ctl/chaos.jsonl.
"""
import fcntl
import glob
import gzip
import json
import os
import shlex
import signal
import socket
import subprocess
import time

import numpy as np

from . import controller

EXIT_DONE, EXIT_BUDGET, EXIT_FAILED, EXIT_REDLINE, EXIT_ENGINE = 0, 3, 4, 5, 6
BARREN_LIMIT = 3      # consecutive launches in which no engine finished its lease or earned a strike
# Lines that mean an engine was not on the NoLoop likelihood (RIFT AGENTS.md, REDLINE).
# `Override --gpu` is expected on a host without cupy once --force-xpy is given, which the engine requires.
OFF_NOLOOP = ("this is the old vectorized code path not the xpy path", "NOT APPLIED -- this configuration cannot honour")


def _log(run_dir, name, rec):
    os.makedirs(os.path.join(run_dir, "ctl"), exist_ok=True)
    with open(os.path.join(run_dir, "ctl", name), "a") as f:
        f.write(json.dumps(dict(rec, t=time.time())) + "\n")


def _shard_keys(path):
    keys = set()
    if os.path.exists(path):
        for line in open(path):
            try:
                r = json.loads(line)
                keys.add((r["s"], r["i"]))
            except ValueError:
                pass
    return keys


def _off_noloop(logf):
    try:
        with (gzip.open if logf.endswith(".gz") else open)(logf, "rt", errors="replace") as f:
            text = f.read()
    except (OSError, EOFError):
        return 0
    return sum(text.count(s) for s in OFF_NOLOOP)


def _stop_off_noloop(run_dir, off):
    """Rows from an engine off the NoLoop path must not enter a fit or an estimate: end the run."""
    r = controller.Run(run_dir)
    r.set_head(**dict(r.head() or {}, state="FAILED", reason="an engine left the NoLoop likelihood path"))
    print("ledger run: %d engine log lines show the likelihood off the NoLoop path; stopping" % off, flush=True)
    return EXIT_REDLINE


def lease_record(lease, base, logf, status, killed, wall, cleanup=None):
    """What one engine launch left, read where the engine ran: rows written, rows missing,
    the key in flight when it ended, and log lines off the NoLoop path.  Removes the
    in-flight file and, by the glob template `cleanup`, the engine's own output files."""
    have = _shard_keys(lease["shard"])
    keys = [tuple(k) for k in json.load(open(lease["keys"]))["keys"]]
    rec = dict(lease=base, shard=lease["shard"], rows=len(have), missing=sum(k not in have for k in keys), status=status,
               killed=bool(killed), wall=wall, off_noloop=_off_noloop(logf), in_flight=None)
    if cleanup:
        for f in glob.glob(cleanup.format(base=base)):
            try:
                os.remove(f)
            except OSError:
                pass
    flight = lease["shard"] + ".inflight"
    if os.path.exists(flight):
        try:
            key = tuple(json.load(open(flight))["key"])
            if (key[1], key[2]) not in have:          # else it ended after that key's line was written
                rec["in_flight"] = list(key)
        except (ValueError, KeyError, TypeError, IndexError):
            pass
        try:
            os.remove(flight)
        except OSError:
            pass
    return rec


def settle(state, rec):
    """Apply one launch's record to the supervisor state, by the rules in the module text.
    Returns "worked" (the lease was finished, or the launch earned a strike), "idle" (rows
    missing and no evidence against one point) or None (the launch was stopped by its
    supervisor, which is evidence of nothing).  Two strikes write the key's FAILED line."""
    if rec["killed"]:
        return None
    if not rec["missing"]:
        return "worked"
    key = rec.get("in_flight")
    if key is not None and rec["rows"]:               # it wrote a row first, so this engine works
        name = "%d/%d/%d" % tuple(key)
        state["strikes"][name] = state["strikes"].get(name, 0) + 1
        rec["strike"] = name
        if state["strikes"][name] >= 2:
            with open(rec["shard"][:-6] + "-ctl.jsonl", "a") as f:
                f.write(json.dumps(dict(v=key[0], s=key[1], i=key[2], status="FAILED", lnL=None,
                                        reason="the engine died twice on this point")) + "\n")
            rec["recorded_failed"] = 1
        return "worked"
    rec["not_working"] = True
    if key is not None and list(key) not in state.setdefault("defer", []):
        state["defer"].append(list(key))
    return "idle"


def _chaos_kills(run_dir):
    """Engine kills made on purpose so far.  ctl/chaos.jsonl is their one record, so a
    supervisor that is itself killed cannot lose count of them."""
    path = os.path.join(run_dir, "ctl", "chaos.jsonl")
    return sum('"engine"' in line for line in open(path)) if os.path.exists(path) else 0


def _reap_orphans(run_dir):
    """Kill engines a previous supervisor left running on this host, identified by the
    lease path in the command line of their process-group leader."""
    path = os.path.join(run_dir, "ctl", "engines.json")
    if not os.path.exists(path):
        return
    for rec in json.load(open(path)):
        if rec.get("host") != socket.gethostname():
            continue
        try:
            args = subprocess.run(["ps", "-p", str(rec["pgid"]), "-o", "args="], capture_output=True, text=True).stdout
        except OSError:
            continue
        if rec["lease"] in args:
            try:
                os.killpg(rec["pgid"], signal.SIGKILL)
            except OSError:
                pass
    os.remove(path)


def run(run_dir, engine_command, cwd=None, engines=1, gpus=None, budget=float("inf"), chaos="", min_left=30.0,
        cleanup=None):
    """Drive the run.  engine_command is a shell template with {lease} {keys} {shard} {n} {log} {base}.
    cleanup is a glob template in {base}: files an engine leaves that are removed after its lease."""
    os.makedirs(os.path.join(run_dir, "ctl"), exist_ok=True)
    os.makedirs(os.path.join(run_dir, "logs"), exist_ok=True)
    lock = open(os.path.join(run_dir, "ctl", "lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise SystemExit("ledger run: another supervisor holds %s" % lock.name)
    _reap_orphans(run_dir)
    off = sum(_off_noloop(f) for f in glob.glob(os.path.join(run_dir, "logs", "*.log*")))
    if off and os.path.exists(os.path.join(run_dir, "HEAD.json")):
        return _stop_off_noloop(run_dir, off)
    t_end = time.time() + budget
    spec = dict(engine=0, seed=0, lo=60.0, hi=400.0, rows=0)
    spec.update({k: float(v) for k, v in (kv.split("=") for kv in chaos.split(",") if kv)})
    state_file = os.path.join(run_dir, "ctl", "supervisor_state.json")
    state = json.load(open(state_file)) if os.path.exists(state_file) else dict(launches=0, strikes={})
    barren = 0
    rng = np.random.default_rng([int(spec["seed"]), state["launches"]])
    live, ours = [], set()        # running engines; pids this supervisor killed

    def stop_engines(signum=None, frame=None):
        for eng in live:
            ours.add(eng.pid)
            try:
                os.killpg(eng.pid, signal.SIGKILL)
            except OSError:
                pass
        if signum is not None:
            raise SystemExit(EXIT_BUDGET)

    old = signal.signal(signal.SIGTERM, stop_engines)
    try:
        while True:
            act = controller.tick(controller.Run(run_dir), engines, defer=state.get("defer", []))
            _log(run_dir, "ticks.jsonl", {k: v for k, v in act.items() if k != "leases"})
            if act["action"] in ("done", "failed"):
                print("ledger run %s" % act["action"], flush=True)
                return EXIT_DONE if act["action"] == "done" else EXIT_FAILED
            if act["action"] == "again":
                continue
            left = t_end - time.time()
            if left < min_left:
                print("ledger run: budget spent; the run resumes on the next start", flush=True)
                return EXIT_BUDGET
            state["launches"] += 1
            kill_at, kill_rows = None, None
            if _chaos_kills(run_dir) < spec["engine"] and state["launches"] % 2 == 1:
                if spec["rows"]:
                    kill_rows = int(spec["rows"])
                else:
                    kill_at = float(rng.uniform(spec["lo"], spec["hi"]))
            t0 = time.time()
            jobs = []
            for slot, lease in enumerate(act["leases"]):
                base = os.path.basename(lease["lease"])[:-4]
                logf = os.path.join(run_dir, "logs", base + ".log")
                cmd = engine_command.format(lease=shlex.quote(lease["lease"]), keys=shlex.quote(lease["keys"]),
                                            shard=shlex.quote(lease["shard"]), n=lease["n"], log=shlex.quote(logf),
                                            base=base)
                env = dict(os.environ)
                if gpus:
                    env["CUDA_VISIBLE_DEVICES"] = str(gpus[slot % len(gpus)])
                lf = open(logf, "ab")
                eng = subprocess.Popen(cmd, shell=True, stdout=lf, stderr=subprocess.STDOUT, start_new_session=True,
                                       cwd=cwd, env=env)
                live.append(eng)
                jobs.append((lease, base, logf, lf, eng))
            controller.write_atomic(os.path.join(run_dir, "ctl", "engines.json"), json.dumps(
                [dict(host=socket.gethostname(), pgid=eng.pid, lease=lease["lease"]) for lease, _, _, _, eng in jobs]))
            why = None
            deadline = t0 + (min(left, kill_at) if kill_at is not None else left)
            while any(eng.poll() is None for _, _, _, _, eng in jobs):
                by_rows = (kill_rows is not None and jobs[0][4].poll() is None
                           and len(_shard_keys(jobs[0][0]["shard"])) >= kill_rows)
                if by_rows or time.time() >= deadline:
                    why = "chaos" if by_rows or (kill_at is not None and kill_at < left) else "budget"
                    # a chaos kill takes one engine; the budget takes all
                    for _, _, _, _, eng in (jobs[:1] if why == "chaos" else jobs):
                        if eng.poll() is None:
                            ours.add(eng.pid)
                        try:
                            os.killpg(eng.pid, signal.SIGKILL)
                        except OSError:
                            pass
                    if why == "chaos":
                        kill_at, kill_rows, deadline = None, None, t0 + left
                        lease, base = jobs[0][0], jobs[0][1]
                        jobs[0][4].wait()
                        have = _shard_keys(lease["shard"])
                        miss = [k for k in (tuple(k) for k in json.load(open(lease["keys"]))["keys"]) if k not in have]
                        _log(run_dir, "chaos.jsonl", dict(kill="engine", lease=base, rows_kept=len(have),
                                                           in_flight=list(miss[0]) if miss else None))
                        continue
                    break
                time.sleep(0.05)
            off, worked, idle = 0, False, False
            for slot, (lease, base, logf, lf, eng) in enumerate(jobs):
                eng.wait()
                lf.close()
                live.remove(eng)
                rec = lease_record(lease, base, logf, eng.returncode, eng.pid in ours, time.time() - t0, cleanup)
                off += rec["off_noloop"]
                outcome = settle(state, rec)
                worked, idle = worked or outcome == "worked", idle or outcome == "idle"
                _log(run_dir, "leases.jsonl", rec)
            barren = 0 if worked else barren + idle
            controller.write_atomic(state_file, json.dumps(state))
            if os.path.exists(os.path.join(run_dir, "ctl", "engines.json")):
                os.remove(os.path.join(run_dir, "ctl", "engines.json"))
            if off:
                return _stop_off_noloop(run_dir, off)
            if barren >= BARREN_LIMIT:
                print("ledger run: in %d launches in a row no engine finished its lease or gave evidence "
                      "against one point; the engine is not working, see %s.  These launches marked no point "
                      "failed." % (barren, os.path.join(run_dir, "logs")), flush=True)
                return EXIT_ENGINE
            if why == "budget":
                print("ledger run: budget spent; the run resumes on the next start", flush=True)
                return EXIT_BUDGET
    finally:
        stop_engines()
        signal.signal(signal.SIGTERM, old)
