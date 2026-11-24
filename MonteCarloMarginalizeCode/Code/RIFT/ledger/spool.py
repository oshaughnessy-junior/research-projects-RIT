"""The spool transport: pilots on a filesystem that several hosts share.

A run is N copies of one command, `rift-run --transport spool`, each in its own scheduler
job.  There is no other process.  Each copy is a pilot with `engines` slots, and loops:

    the run is over (HEAD)          stop its engines and exit with the run's status
    an open lease and a free slot   claim the lease, start an engine on it
    an engine ended                 write what it left to spool/done/
    a free slot and no open lease   take the tick lock: settle new records, close leases whose
                                    pilot is gone, run the controller tick, write tickets

Files, all under the run directory:

    spool/open/<lease>.json             a ticket: the lease the controller wrote
    spool/claimed/<lease>.<pilot>.json  the same ticket after one pilot renamed it; that pilot
                                        keeps its modification time fresh while the engine runs
    spool/done/<lease>.json             the record of the launch (inproc.lease_record)
    spool/pilots/<pilot>.json           host and slots; its modification time is the heartbeat
    ctl/tick.lock, start.lock           exclusive-create locks, broken when stale
    ctl/pilots/<pilot>.jsonl            what one pilot did: start, claims, engine ends, locks broken, exit

Rules.  A claim is one rename, and only its winner holds the new name.  A pilot records and
never judges: the pilot that holds the tick lock applies the one-host transport's rules
(inproc.settle) to each record once.  A pilot the scheduler stops (SIGTERM) or whose budget
ends stops its engines and says so in the record, which is evidence of nothing.  A pilot that
dies without a record is found by its heartbeat: after `stale_after` seconds its lease is
closed as stopped, its rows stay, and a record it writes later is ignored.  The tick gives
no second lease to a row a running lease will write, and closes no version while a claimed
lease has no record: the record carries what the engine's log said about the NoLoop path.
A pilot whose leases end three times
in a row with rows missing and no evidence against one point leaves with status 6, so a
node with a bad device takes three leases at most.

Nothing here uses flock, a process id of another host, or a signal to another pilot.  Ages
are differences of modification times of files on the shared filesystem.
"""
import glob
import json
import os
import random
import shlex
import signal
import socket
import subprocess
import time

from . import controller
from .inproc import (BARREN_LIMIT, EXIT_BUDGET, EXIT_DONE, EXIT_ENGINE, EXIT_FAILED, EXIT_REDLINE, _log, _off_noloop,
                     _stop_off_noloop, lease_record, settle)

LOCK_STALE = 120.0        # seconds; a tick takes a few


def create_exclusive(path, text):
    """Create `path` holding `text` unless it exists.  True if this call made it."""
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    return True


def read_json(path):
    """A JSON file another host may be writing or removing: None until it is whole."""
    try:
        return json.load(open(path))
    except (OSError, ValueError):
        return None


class Lock:
    """An exclusive-create lock file.  One older than `stale` seconds, by the filesystem's
    own times, is taken to belong to a dead holder and is removed by whoever renames it."""

    def __init__(self, path, me, stale=LOCK_STALE):
        self.path, self.me, self.stale = path, me, stale
        self.broken = []          # stale locks this holder removed

    def age(self):
        probe = "%s.probe.%s" % (self.path, self.me)
        try:
            with open(probe, "w"):
                pass
            return os.stat(probe).st_mtime - os.stat(self.path).st_mtime
        except OSError:
            return None
        finally:
            try:
                os.remove(probe)
            except OSError:
                pass

    def take(self):
        if create_exclusive(self.path, self.me):
            return True
        age = self.age()
        if age is not None and age > self.stale:
            grave = "%s.stale.%s" % (self.path, self.me)
            try:
                os.rename(self.path, grave)
                os.remove(grave)
            except OSError:
                return False
            self.broken.append(dict(lock=os.path.basename(self.path), age=age))
            return create_exclusive(self.path, self.me)
        return False

    def wait(self, timeout=600.0, pause=0.5):
        t_end = time.time() + timeout
        while not self.take():
            if time.time() > t_end:
                raise SystemExit("ledger run: could not take %s in %d s" % (self.path, timeout))
            time.sleep(pause)
        return self

    def release(self):
        try:
            if open(self.path).read() == self.me:
                os.remove(self.path)
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()


def pilot_id():
    return "%s-%d-%06x" % (socket.gethostname().split(".")[0], os.getpid(), random.SystemRandom().randrange(16 ** 6))


def start_lock(run_dir, me=None):
    """Held around the first start of a run, so that N pilots starting at once create one."""
    os.makedirs(run_dir, exist_ok=True)
    return Lock(os.path.join(run_dir, "start.lock"), me or pilot_id()).wait()


class Spool:
    def __init__(self, run_dir, me, stale_after):
        self.run_dir, self.me, self.stale_after = run_dir, me, stale_after
        for d in ("open", "claimed", "done", "pilots"):
            os.makedirs(self.p(d), exist_ok=True)
        os.makedirs(os.path.join(run_dir, "ctl", "pilots"), exist_ok=True)
        os.makedirs(os.path.join(run_dir, "logs"), exist_ok=True)
        self.card = self.p("pilots", me + ".json")
        self.lock = Lock(os.path.join(run_dir, "ctl", "tick.lock"), me)

    def p(self, *parts):
        return os.path.join(self.run_dir, "spool", *parts)

    def event(self, what, **fields):
        """One line in this pilot's own log."""
        while self.lock.broken:
            _log(self.run_dir, os.path.join("pilots", self.me + ".jsonl"), dict(self.lock.broken.pop(), event="lock broken"))
        _log(self.run_dir, os.path.join("pilots", self.me + ".jsonl"), dict(fields, event=what))

    def beat(self, paths=()):
        for path in (self.card,) + tuple(paths):
            try:
                os.utime(path, None)
            except OSError:
                pass

    def now(self):
        """The filesystem's time: the modification time of a file this pilot has just touched."""
        self.beat()
        return os.stat(self.card).st_mtime

    def names(self, d):
        return sorted(n for n in os.listdir(self.p(d)) if n.endswith(".json"))

    def claim(self):
        """Take one open ticket.  Returns (ticket, its path under claimed/) or None."""
        for name in self.names("open"):
            src, dst = self.p("open", name), self.p("claimed", "%s.%s.json" % (name[:-5], self.me))
            try:
                os.rename(src, dst)
            except OSError:
                # Another pilot took it; or the rename happened and its reply was lost, which
                # the new name shows, since only this pilot could have made it.
                if not os.path.exists(dst):
                    continue
            ticket = read_json(dst)
            if ticket is None:
                continue
            self.beat([dst])               # a rename keeps the ticket's old time; the heartbeat starts now
            return ticket, dst
        return None

    def outstanding(self):
        """Tickets open, and tickets claimed with no record yet: {lease base: (path, pilot)}."""
        done = set(n[:-5] for n in self.names("done"))
        out = {n[:-5]: (self.p("open", n), None) for n in self.names("open")}
        for n in self.names("claimed"):
            base, pilot = n[:-5].rsplit(".", 1)
            if base not in done:
                out[base] = (self.p("claimed", n), pilot)
        return out, done

    def worth_a_tick(self, settled):
        """Without the lock and without reading the ledger: is there anything for a tick to do?"""
        out, done = self.outstanding()
        if done - settled or not out:
            return True
        now = self.now()
        for path, pilot in out.values():
            try:
                if pilot is not None and now - os.stat(path).st_mtime > self.stale_after:
                    return True
            except OSError:
                return True
        return False

    def tick(self, fleet, min_lease_rows, cleanup=None):
        """One tick under the lock.  Returns the controller's action, "over" after an engine log
        off the NoLoop path, or None when there was nothing to do or another pilot holds the lock."""
        state_file = os.path.join(self.run_dir, "ctl", "supervisor_state.json")
        state = read_json(state_file) or {}
        if not self.worth_a_tick(set(state.get("settled", []))) or not self.lock.take():
            return None
        try:
            state = read_json(state_file) or dict(launches=0, strikes={})
            state.setdefault("strikes", {})
            settled = state.setdefault("settled", [])
            out, done = self.outstanding()
            now = self.now()
            # leases whose pilot stopped beating: closed as stopped, which is evidence of nothing
            for base, (path, pilot) in sorted(out.items()):
                if pilot is None:
                    continue
                try:
                    age = now - os.stat(path).st_mtime
                except OSError:
                    continue
                ticket = read_json(path)
                if age > self.stale_after and ticket is not None:
                    rec = lease_record(ticket, base, os.path.join(self.run_dir, "logs", base + ".log"), None, True, age, cleanup)
                    rec.update(pilot=pilot, stale=True)
                    create_exclusive(self.p("done", base + ".json"), json.dumps(rec))
            # records not yet settled, each once
            off = 0
            for name in self.names("done"):
                base = name[:-5]
                rec = read_json(self.p("done", name))
                if base in settled or rec is None:
                    continue
                off += rec.get("off_noloop", 0)
                settle(state, rec)
                settled.append(base)
                _log(self.run_dir, "leases.jsonl", rec)
                for path in glob.glob(self.p("claimed", base + ".*.json")):
                    try:
                        os.remove(path)
                    except OSError:
                        pass
            controller.write_atomic(state_file, json.dumps(state))
            if off:
                _stop_off_noloop(self.run_dir, off)
                return "over"
            # rows a lease still open or running will write get no second lease
            out, _ = self.outstanding()
            busy, running = [], 0
            for base, (path, pilot) in out.items():
                ticket = read_json(path)
                meta = read_json(ticket["keys"]) if ticket else None
                if meta is None:
                    continue
                running += pilot is not None
                busy.extend((ticket["version"], s, i) for s, i in meta["keys"])
            slots = 0
            for name in self.names("pilots"):
                card = read_json(self.p("pilots", name))
                try:
                    if card and now - os.stat(self.p("pilots", name)).st_mtime <= self.stale_after:
                        slots += int(card.get("slots", 1))
                except OSError:
                    pass
            parts = max(int(fleet), slots - running, 1)
            stale_tickets = [path for path, pilot in out.values() if pilot is None]
            while True:
                act = controller.tick(controller.Run(self.run_dir), parts, defer=state.get("defer", []), busy=busy,
                                      min_lease_rows=min_lease_rows, hold=running > 0)
                _log(self.run_dir, "ticks.jsonl", dict({k: v for k, v in act.items() if k != "leases"}, pilot=self.me))
                if act["action"] in ("again", "done", "failed"):
                    # the version closed with every row in: a ticket still open would only repeat rows
                    for path in stale_tickets:
                        try:
                            os.remove(path)
                        except OSError:
                            pass
                    stale_tickets, busy = [], []
                if act["action"] != "again":
                    break
            for lease in act.get("leases", []):
                base = os.path.basename(lease["lease"])[:-4]
                controller.write_atomic(self.p("open", base + ".json"), json.dumps(dict(lease, base=base)))
            return act["action"]
        finally:
            self.lock.release()


def run(run_dir, engine_command, cwd=None, engines=1, gpus=None, budget=float("inf"), fleet=1, stale_after=180.0,
        min_lease_rows=32, poll=2.0, min_left=30.0, cleanup=None):
    """One pilot.  engine_command and cleanup are as in inproc.run.  `fleet` is the number of
    engine slots expected over all pilots: a version's rows are cut into at least that many
    leases, and a pilot that arrives late finds the ones not yet claimed."""
    me = pilot_id()
    sp = Spool(run_dir, me, stale_after)
    controller.write_atomic(sp.card, json.dumps(dict(host=socket.gethostname(), slots=int(engines), pid=os.getpid())))
    sp.event("start", host=socket.gethostname(), slots=int(engines), fleet=int(fleet), stale_after=stale_after)

    def leave(status):
        sp.event("exit", status=status)
        return status
    off = sum(_off_noloop(f) for f in glob.glob(os.path.join(run_dir, "logs", "*.log*")))
    if off and os.path.exists(os.path.join(run_dir, "HEAD.json")):
        return _stop_off_noloop(run_dir, off)
    t_end = time.time() + budget
    slots = {}                # slot -> dict(ticket, path, base, logf, lf, eng, t0)
    stopping, barren, last_beat = [False], 0, 0.0

    def on_term(signum, frame):
        stopping[0] = True

    def finish(slot, stopped):
        nonlocal barren
        job = slots.pop(slot)
        if stopped and job["eng"].poll() is None:
            try:
                os.killpg(job["eng"].pid, signal.SIGKILL)
            except OSError:
                pass
        else:
            stopped = False                       # it had ended by itself before this pilot stopped it
        job["eng"].wait()
        job["lf"].close()
        rec = lease_record(job["ticket"], job["base"], job["logf"], job["eng"].returncode, stopped,
                           time.time() - job["t0"], cleanup)
        rec.update(pilot=me, host=socket.gethostname())
        kept = create_exclusive(sp.p("done", job["base"] + ".json"), json.dumps(rec))   # a record of a lease closed as stale stays
        sp.event("engine ended", lease=job["base"], status=rec["status"], stopped=bool(stopped), rows=rec["rows"],
                 missing=rec["missing"], recorded=kept)
        if not stopped:
            evidence = rec["in_flight"] is not None and rec["rows"]
            barren = barren + 1 if rec["missing"] and not evidence else 0

    old = signal.signal(signal.SIGTERM, on_term)
    try:
        while True:
            if time.time() - last_beat > stale_after / 6.0:
                sp.beat([job["path"] for job in slots.values()])
                last_beat = time.time()
            for slot in [s for s, job in slots.items() if job["eng"].poll() is not None]:
                finish(slot, False)
            head = read_json(os.path.join(run_dir, "HEAD.json"))
            over = head is not None and head["state"] in controller.STATES_OVER
            if over or stopping[0] or t_end - time.time() < min_left:
                for slot in list(slots):
                    finish(slot, True)
                if over:
                    print("ledger run %s" % head["state"].lower(), flush=True)
                    if head["state"] == "DONE":
                        return leave(EXIT_DONE)
                    return leave(EXIT_REDLINE if "NoLoop" in head.get("reason", "") else EXIT_FAILED)
                print("ledger run: this pilot's budget is spent or it was told to stop; the run resumes "
                      "with any pilot", flush=True)
                return leave(EXIT_BUDGET)
            if barren >= BARREN_LIMIT:
                if not slots:
                    print("ledger run: %d leases in a row ended on this pilot with rows missing and no evidence "
                          "against one point; its engine is not working, see %s.  It marked no point failed." % (
                              barren, os.path.join(run_dir, "logs")), flush=True)
                    return leave(EXIT_ENGINE)
                time.sleep(poll)
                continue
            busy = False
            while len(slots) < engines:
                got = sp.claim()
                if got is None:
                    break
                ticket, path = got
                slot = min(set(range(int(engines))) - set(slots))
                base = ticket["base"]
                logf = os.path.join(run_dir, "logs", base + ".log")
                cmd = engine_command.format(lease=shlex.quote(ticket["lease"]), keys=shlex.quote(ticket["keys"]),
                                            shard=shlex.quote(ticket["shard"]), n=ticket["n"], log=shlex.quote(logf),
                                            base=base)
                env = dict(os.environ)
                if gpus:
                    env["CUDA_VISIBLE_DEVICES"] = str(gpus[slot % len(gpus)])
                lf = open(logf, "ab")
                eng = subprocess.Popen(cmd, shell=True, stdout=lf, stderr=subprocess.STDOUT, start_new_session=True,
                                       cwd=cwd, env=env)
                slots[slot] = dict(ticket=ticket, path=path, base=base, logf=logf, lf=lf, eng=eng, t0=time.time())
                sp.event("claim", lease=base, rows=ticket["n"], pid=eng.pid)
                busy = True
            if len(slots) < engines and sp.tick(fleet, min_lease_rows, cleanup) in ("run", "again", "done", "failed", "over"):
                busy = True
            if not busy:
                time.sleep(poll)
    finally:
        for job in slots.values():                # an exception in this pilot: its engines do not outlive it
            try:
                os.killpg(job["eng"].pid, signal.SIGKILL)
            except OSError:
                pass
        signal.signal(signal.SIGTERM, old)
        try:
            os.remove(sp.card)
        except OSError:
            pass
