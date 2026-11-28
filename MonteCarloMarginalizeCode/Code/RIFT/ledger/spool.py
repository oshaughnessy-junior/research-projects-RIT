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
    spool/claimed/<lease>.<pilot>.json  the same ticket after one pilot renamed it
    spool/done/<lease>.json             the record of the launch (inproc.lease_record)
    spool/pilots/<pilot>.json           the pilot's card: host, slots, the leases its engines
                                        hold, and a counter it raises while it lives
    ctl/tick.lock, start.lock           exclusive-create locks, broken when stale
    <lock>.gate                         held for the instant in which a lock is broken or released
    ctl/pilots/<pilot>.jsonl            what one pilot did: start, claims, engine ends, locks broken, exit

Rules.  A claim is one rename, and only its winner holds the new name.  A pilot records and
never judges: the pilot that holds the tick lock applies the one-host transport's rules
(inproc.settle) to each record once.  A pilot the scheduler stops (SIGTERM) or whose budget
ends stops its engines and says so in the record, which is evidence of nothing.  A pilot that
dies without a record is found by its card.  A claimed lease is live while its pilot's card
names it under a counter that advances.  A pilot that has watched that stay the same for
`stale_after` seconds of its own clock closes the lease as stopped; its rows stay, and a
record written later is ignored.  A pilot that has just started therefore closes nothing
in its first `stale_after` seconds.  The tick gives
no second lease to a row a running lease will write, and closes no version while a claimed
lease has no record or a record is unread: the record carries what the engine's log said
about the NoLoop path.  Every tick counts the lines off that path over every record, and
stops the run before it writes anything else, so the stop does not depend on the tick that
first read the record.  A record, like a lock, is written under another name and linked, so
its name never shows without its content.  A launch that ended on SIGTERM was stopped,
whether the signal reached the pilot first or its engine.
A pilot whose leases end three times
in a row with rows missing and no evidence against one point leaves with status 6, so a
node with a bad device takes three leases at most.

Nothing here uses flock, a process id of another host, a signal to another pilot, or a
clock of another host.  Nor does it read a file time that another pilot set: an NFS 4.2
client can keep the times it sets on a file it created to itself, and at CIT on 2026-11-26
no other node saw them, while the content of the file was current at every read.  The two
locks compare times of creation, which every client sees.  Nor does it ask by name whether a
file exists that another pilot may have made: a client there that asked for a name before
the file existed did not find it for most of a minute afterwards, while a listing of the
directory was current.  HEAD.json and the state file are looked for in a listing, and one
that is listed and cannot be read is never taken as absent.
"""
import errno
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
IO_ERRORS = 10            # passes of a pilot's loop in a row that may end on an OSError


def create_exclusive(path, text):
    """Create `path` holding `text` unless it exists.  True if this call made it.  The text is
    written under another name and linked, so `path` is never seen without its content: a
    pilot killed in this call leaves no empty record and no lock without a holder's name."""
    tmp = "%s.new.%s.%d" % (path, socket.gethostname(), os.getpid())
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.link(tmp, path)
        return True
    except FileExistsError:
        return False
    except OSError as err:
        if err.errno in (errno.EPERM, errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP):
            raise SystemExit("ledger run: %s is on a filesystem with no hard links, which the spool "
                             "transport needs" % path)
        return os.stat(tmp).st_nlink == 2         # the link was made and its reply was lost
    finally:
        os.remove(tmp)


def read_json(path):
    """A JSON file another host may be writing or removing: None until it is whole."""
    try:
        return json.load(open(path))
    except (OSError, ValueError):
        return None


def listed_json(path):
    """(listed, content) of a JSON file another host may have made.  Whether it exists is read
    from a listing of its directory (see the module text).  (True, None) is a file this host
    cannot read yet; it is not an absent one."""
    d, name = os.path.split(path)
    try:
        if name not in os.listdir(d):
            return False, None
    except FileNotFoundError:
        return False, None
    return True, read_json(path)


class Lock:
    """An exclusive-create lock file.  One older than `stale` seconds, by the filesystem's
    own times, is taken to belong to a dead holder and is removed.  A lock is broken, and
    released, only by the pilot that holds its gate, `<lock>.gate`, and a breaker measures the
    age again there.  So of two pilots that found one lock stale, the second finds the
    first's new lock and leaves it; and a holder whose lock was broken while it ran does not
    remove the lock that replaced it.  A gate left by a pilot that died in that instant is
    removed when it is stale in its turn."""

    def __init__(self, path, me, stale=LOCK_STALE):
        self.path, self.me, self.stale = path, me, stale
        self.broken = []          # stale locks this holder removed

    def age(self, path=None):
        probe = "%s.probe.%s" % (self.path, self.me)
        try:
            with open(probe, "w"):
                pass
            return os.stat(probe).st_mtime - os.stat(path or self.path).st_mtime
        except OSError:
            return None
        finally:
            try:
                os.remove(probe)
            except OSError:
                pass

    def gate(self):
        """Take the gate.  Returns its path, or None if another pilot holds it."""
        gate = self.path + ".gate"
        if create_exclusive(gate, self.me):
            return gate
        age = self.age(gate)
        if age is not None and age > self.stale:
            try:
                os.remove(gate)                   # its holder died; the next call takes it
            except OSError:
                pass
        return None

    def ungate(self, gate):
        if gate is not None:
            try:
                os.remove(gate)
            except OSError:
                pass

    def take(self):
        if create_exclusive(self.path, self.me):
            return True
        age = self.age()
        if age is None or age <= self.stale:
            return False
        gate = self.gate()
        if gate is None:
            return False
        try:
            age = self.age()                      # again: by now it may be the lock of the pilot that broke it
            if age is None or age <= self.stale:
                return False
            os.remove(self.path)
            self.broken.append(dict(lock=os.path.basename(self.path), age=age))
            return create_exclusive(self.path, self.me)
        except OSError:
            return False
        finally:
            self.ungate(gate)

    def held(self):
        """Does the lock still name this holder?  It does not once another pilot broke it."""
        try:
            return open(self.path).read() == self.me
        except OSError:
            return False

    def wait(self, timeout=600.0, pause=0.5):
        t_end = time.time() + timeout
        while not self.take():
            if time.time() > t_end:
                raise SystemExit("ledger run: could not take %s in %d s" % (self.path, timeout))
            time.sleep(pause)
        return self

    def release(self):
        gate = None
        for _ in range(40):                       # two seconds at most; then without the gate
            gate = self.gate()
            if gate is not None:
                break
            time.sleep(0.05)
        try:
            if self.held():
                os.remove(self.path)
        except OSError:
            pass
        finally:
            self.ungate(gate)

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
    def __init__(self, run_dir, me, stale_after, card=None):
        self.run_dir, self.me, self.stale_after = run_dir, me, stale_after
        self.about, self.count = dict(card or {}), 0     # what this pilot's card says of it; its counter
        self.seen, self.clock = {}, time.monotonic       # {what was watched: (its value, when first seen)}
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

    def beat(self, leases=()):
        """Write this pilot's card again, naming the leases its engines hold, with the counter one higher."""
        self.count += 1
        try:
            controller.write_atomic(self.card, json.dumps(dict(self.about, leases=sorted(leases), beat=self.count)))
        except OSError:
            pass

    def unchanged(self, what, value):
        """Seconds of this pilot's own clock for which it has seen `what` hold `value`."""
        now = self.clock()
        if what not in self.seen or self.seen[what][0] != value:
            self.seen[what] = (value, now)
        return now - self.seen[what][1]

    def idle(self, out):
        """{lease: seconds} for the claimed tickets of `out` (see outstanding): for how long this
        pilot has seen no sign of life for the lease.  The sign is the counter of the card of
        the pilot that claimed it, while that card names the lease."""
        cards, ages = {}, {}
        for base, (path, pilot) in sorted(out.items()):
            if pilot is None:
                continue
            if pilot not in cards:
                cards[pilot] = read_json(self.p("pilots", pilot + ".json")) or {}
            card = cards[pilot]
            ages[base] = self.unchanged(("lease", base, pilot), card.get("beat") if base in card.get("leases", ()) else None)
        for what in [w for w in self.seen if w[0] == "lease" and w[1] not in ages]:
            del self.seen[what]
        return ages

    def live_slots(self):
        """Engine slots of the pilots whose counter this pilot has seen advance within stale_after."""
        slots, names = 0, self.names("pilots")
        for name in names:
            card = read_json(self.p("pilots", name))
            if card and self.unchanged(("card", name), card.get("beat")) <= self.stale_after:
                slots += int(card.get("slots", 1))
        for what in [w for w in self.seen if w[0] == "card" and w[1] not in names]:
            del self.seen[what]
        return slots

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
        return any(age > self.stale_after for age in self.idle(out).values())

    def tick(self, fleet, min_lease_rows, cleanup=None):
        """One tick under the lock.  Returns the controller's action, "over" after an engine log
        off the NoLoop path, or None when there was nothing to do or another pilot holds the lock."""
        state_file = os.path.join(self.run_dir, "ctl", "supervisor_state.json")
        head_file = os.path.join(self.run_dir, "HEAD.json")
        listed, state = listed_json(state_file)
        if listed and state is None:
            return None
        if not self.worth_a_tick(set((state or {}).get("settled", []))) or not self.lock.take():
            return None
        try:
            listed, state = listed_json(state_file)
            if (listed and state is None) or listed_json(head_file) == (True, None):
                return None                       # a file this host cannot read yet is not an absent one
            state = state or dict(launches=0, strikes={})
            state.setdefault("strikes", {})
            settled = state.setdefault("settled", [])
            out, done = self.outstanding()
            # leases with no sign of life: closed as stopped, which is evidence of nothing
            for base, age in sorted(self.idle(out).items()):
                path, pilot = out[base]
                ticket = read_json(path)
                if age > self.stale_after and ticket is not None:
                    rec = lease_record(ticket, base, os.path.join(self.run_dir, "logs", base + ".log"), None, True, age, cleanup)
                    rec.update(pilot=pilot, stale=True)
                    create_exclusive(self.p("done", base + ".json"), json.dumps(rec))
            # records not yet settled, each once; lines off the NoLoop path over every record
            off = 0
            for name in self.names("done"):
                base = name[:-5]
                rec = read_json(self.p("done", name))
                if rec is None:
                    continue
                off += rec.get("off_noloop", 0)
                if base in settled:
                    continue
                settle(state, rec)
                settled.append(base)
                _log(self.run_dir, "leases.jsonl", rec)
                for path in glob.glob(self.p("claimed", base + ".*.json")):
                    try:
                        os.remove(path)
                    except OSError:
                        pass
            if not self.lock.held():
                self.event("lock lost")
                return None
            if off:
                _stop_off_noloop(self.run_dir, off)       # first: a ticker that dies after this line leaves the run stopped
            controller.write_atomic(state_file, json.dumps(state))
            if off:
                return "over"
            # rows a lease still open or running will write get no second lease
            out, done = self.outstanding()
            unread = done - set(settled)                  # a record that landed during this tick, or could not be read
            busy, running = [], 0
            for base, (path, pilot) in out.items():
                ticket = read_json(path)
                meta = read_json(ticket["keys"]) if ticket else None
                if meta is None:
                    continue
                running += pilot is not None
                busy.extend((ticket["version"], s, i) for s, i in meta["keys"])
            parts = max(int(fleet), self.live_slots() - running, 1)
            stale_tickets = [path for path, pilot in out.values() if pilot is None]
            while True:
                act = controller.tick(controller.Run(self.run_dir), parts, defer=state.get("defer", []), busy=busy,
                                      min_lease_rows=min_lease_rows, hold=running > 0 or bool(unread))
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
            if act.get("leases") and not self.lock.held():
                self.event("lock lost")
                return None                               # leases with no ticket: their rows are leased again
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
    sp = Spool(run_dir, me, stale_after, card=dict(host=socket.gethostname(), slots=int(engines), pid=os.getpid()))
    sp.beat()
    sp.event("start", host=socket.gethostname(), slots=int(engines), fleet=int(fleet), stale_after=stale_after)

    def leave(status):
        sp.event("exit", status=status)
        return status
    off = sum(_off_noloop(f) for f in glob.glob(os.path.join(run_dir, "logs", "*.log*")))
    if off and listed_json(os.path.join(run_dir, "HEAD.json"))[0]:
        return _stop_off_noloop(run_dir, off)
    t_end = time.time() + budget
    slots = {}                # slot -> dict(ticket, path, base, logf, lf, eng, t0)
    stopping, barren, last_beat, errors = [False], 0, None, 0

    def on_term(signum, frame):
        stopping[0] = True

    def finish(slot, stopped):
        nonlocal barren
        job = slots[slot]                         # it leaves `slots` once its record is written
        if stopped and job["eng"].poll() is None:
            job["stopped"] = True
            try:
                os.killpg(job["eng"].pid, signal.SIGKILL)
            except OSError:
                pass
        job["eng"].wait()
        # Stopped by this pilot, or by the SIGTERM that stops a whole job: a scheduler may signal
        # the engine as well as the pilot, and either may notice first.  Any other end is the engine's own.
        stopped = job.get("stopped", False) or job["eng"].returncode in (-signal.SIGTERM, 128 + signal.SIGTERM)
        job["lf"].close()
        rec = lease_record(job["ticket"], job["base"], job["logf"], job["eng"].returncode, stopped,
                           time.time() - job["t0"], cleanup)
        rec.update(pilot=me, host=socket.gethostname())
        kept = create_exclusive(sp.p("done", job["base"] + ".json"), json.dumps(rec))   # a record of a lease closed as stale stays
        del slots[slot]
        sp.event("engine ended", lease=job["base"], status=rec["status"], stopped=bool(stopped), rows=rec["rows"],
                 missing=rec["missing"], recorded=kept)
        if not stopped:
            evidence = rec["in_flight"] is not None and rec["rows"]
            barren = barren + 1 if rec["missing"] and not evidence else 0

    old = signal.signal(signal.SIGTERM, on_term)
    try:
        while True:
            try:
                if last_beat is None or time.monotonic() - last_beat > stale_after / 6.0:
                    sp.beat(job["base"] for job in slots.values())
                    sp.idle(sp.outstanding()[0])      # a pilot with no free slot watches the others too
                    last_beat = time.monotonic()
                for slot in [s for s, job in slots.items() if job["eng"].poll() is not None]:
                    finish(slot, False)
                head = listed_json(os.path.join(run_dir, "HEAD.json"))[1]
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
                    try:
                        lf = open(logf, "ab")
                        eng = subprocess.Popen(cmd, shell=True, stdout=lf, stderr=subprocess.STDOUT,
                                               start_new_session=True, cwd=cwd, env=env)
                    except OSError as err:        # not a pass to make again: each would take one more lease
                        raise SystemExit("ledger run: this pilot cannot start an engine: %s" % err)
                    slots[slot] = dict(ticket=ticket, path=path, base=base, logf=logf, lf=lf, eng=eng, t0=time.time())
                    sp.beat(job["base"] for job in slots.values())      # the card names the lease from now on
                    sp.event("claim", lease=base, rows=ticket["n"], pid=eng.pid)
                    busy = True
                if len(slots) < engines and sp.tick(fleet, min_lease_rows, cleanup) in ("run", "again", "done", "failed", "over"):
                    busy = True
                if not busy:
                    time.sleep(poll)
                errors = 0
            except OSError as err:
                # A listing or a read that failed: nothing was read this pass.  Each step of a pass
                # can be made again, so the pilot keeps its engines and looks again.
                errors += 1
                try:
                    sp.event("io error", error=repr(err), passes=errors)
                except OSError:
                    pass
                if errors >= IO_ERRORS:
                    raise
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
