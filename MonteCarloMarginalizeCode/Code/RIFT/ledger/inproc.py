"""The in-process transport: one supervisor drives a ledger run on one host.

Loop: one controller tick; if it asks for rows, one engine process per lease, side by
side.  The supervisor returns when the run stops, when its time budget ends, or when an
engine log shows the likelihood left the NoLoop path.  Started again on the same
directory it resumes, because the directory holds the whole state.

After an engine exits:
  exit 0 with lease rows missing   the engine skipped those points; each becomes a FAILED row (L = 0)
  its own non-zero exit            a strike against the first missing key; two strikes make it FAILED
  killed (budget or chaos)         nothing is recorded; the next lease covers the missing rows

`chaos` kills engines on purpose, for tests: `engine=N` engine processes, each after a
random delay between `lo` and `hi` seconds, on alternate launches.  Every kill is logged
in ctl/chaos.jsonl.
"""
import fcntl
import json
import os
import signal
import socket
import subprocess
import time

import numpy as np

from . import controller

EXIT_DONE, EXIT_BUDGET, EXIT_FAILED, EXIT_REDLINE = 0, 3, 4, 5
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
        text = open(logf, errors="replace").read()
    except OSError:
        return 0
    return sum(text.count(s) for s in OFF_NOLOOP)


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


def run(run_dir, engine_command, cwd=None, engines=1, gpus=None, budget=float("inf"), chaos="", min_left=30.0):
    """Drive the run.  engine_command is a shell template with {lease} {keys} {shard} {n} {log} {base}."""
    os.makedirs(os.path.join(run_dir, "ctl"), exist_ok=True)
    os.makedirs(os.path.join(run_dir, "logs"), exist_ok=True)
    lock = open(os.path.join(run_dir, "ctl", "lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise SystemExit("ledger run: another supervisor holds %s" % lock.name)
    _reap_orphans(run_dir)
    t_end = time.time() + budget
    spec = dict(engine=0, seed=0, lo=60.0, hi=400.0)
    spec.update({k: float(v) for k, v in (kv.split("=") for kv in chaos.split(",") if kv)})
    state_file = os.path.join(run_dir, "ctl", "supervisor_state.json")
    state = json.load(open(state_file)) if os.path.exists(state_file) else dict(engine_kills=0, launches=0, strikes={})
    rng = np.random.default_rng([int(spec["seed"]), state["launches"]])
    live = []

    def stop_engines(signum=None, frame=None):
        for eng in live:
            try:
                os.killpg(eng.pid, signal.SIGKILL)
            except OSError:
                pass
        if signum is not None:
            raise SystemExit(EXIT_BUDGET)

    old = signal.signal(signal.SIGTERM, stop_engines)
    try:
        while True:
            act = controller.tick(controller.Run(run_dir), engines)
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
            kill_at = None
            if state["engine_kills"] < spec["engine"] and state["launches"] % 2 == 1:
                kill_at = float(rng.uniform(spec["lo"], spec["hi"]))
            t0 = time.time()
            jobs = []
            for slot, lease in enumerate(act["leases"]):
                base = os.path.basename(lease["lease"])[:-4]
                logf = os.path.join(run_dir, "logs", base + ".log")
                cmd = engine_command.format(lease=lease["lease"], keys=lease["keys"], shard=lease["shard"],
                                            n=lease["n"], log=logf, base=base)
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
                if time.time() >= deadline:
                    why = "chaos" if kill_at is not None and kill_at < left else "budget"
                    # a chaos kill takes one engine; the budget takes all
                    for _, _, _, _, eng in (jobs[:1] if why == "chaos" else jobs):
                        try:
                            os.killpg(eng.pid, signal.SIGKILL)
                        except OSError:
                            pass
                    if why == "chaos":
                        kill_at, deadline = None, t0 + left
                        state["engine_kills"] += 1
                        lease, base = jobs[0][0], jobs[0][1]
                        jobs[0][4].wait()
                        have = _shard_keys(lease["shard"])
                        miss = [k for k in (tuple(k) for k in json.load(open(lease["keys"]))["keys"]) if k not in have]
                        _log(run_dir, "chaos.jsonl", dict(kill="engine", lease=base, rows_kept=len(have),
                                                           in_flight=list(miss[0]) if miss else None))
                        continue
                    break
                time.sleep(0.05)
            off = 0
            for slot, (lease, base, logf, lf, eng) in enumerate(jobs):
                eng.wait()
                lf.close()
                live.remove(eng)
                have = _shard_keys(lease["shard"])
                keys = [tuple(k) for k in json.load(open(lease["keys"]))["keys"]]
                missing = [k for k in keys if k not in have]
                killed = eng.returncode is not None and eng.returncode < 0
                rec = dict(lease=base, rows=len(have), missing=len(missing), status=eng.returncode,
                           killed=bool(killed), wall=time.time() - t0, off_noloop=_off_noloop(logf))
                off += rec["off_noloop"]
                failed = []
                if not killed and missing:
                    if eng.returncode == 0:
                        failed = missing
                    else:
                        k = "%d/%d/%d" % ((lease["version"],) + missing[0])
                        state["strikes"][k] = state["strikes"].get(k, 0) + 1
                        if state["strikes"][k] >= 2:
                            failed = [missing[0]]
                if failed:
                    with open(lease["shard"][:-6] + "-ctl.jsonl", "a") as f:
                        for s, i in failed:
                            f.write(json.dumps(dict(v=lease["version"], s=s, i=i, status="FAILED", lnL=None)) + "\n")
                    rec["recorded_failed"] = len(failed)
                _log(run_dir, "leases.jsonl", rec)
            controller.write_atomic(state_file, json.dumps(state))
            if os.path.exists(os.path.join(run_dir, "ctl", "engines.json")):
                os.remove(os.path.join(run_dir, "ctl", "engines.json"))
            if off:
                # rows from such an engine must not enter a fit or an estimate: the run ends here
                r = controller.Run(run_dir)
                r.set_head(**dict(r.head(), state="FAILED", reason="an engine left the NoLoop likelihood path"))
                print("ledger run: %d engine log lines show the likelihood off the NoLoop path; stopping" % off, flush=True)
                return EXIT_REDLINE
            if why == "budget":
                print("ledger run: budget spent; the run resumes on the next start", flush=True)
                return EXIT_BUDGET
    finally:
        stop_engines()
        signal.signal(signal.SIGTERM, old)
