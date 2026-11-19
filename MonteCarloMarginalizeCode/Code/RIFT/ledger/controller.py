"""The controller of one ledger run: a stateless tick over a directory.

A run directory holds

    config.json                              seed, sizes, stop rule, prior box, grid header
    artifacts/proposal-vNN-<sha16>.json      one proposal per version, never rewritten
    plans/plan-vNN-iAAAAAA-iBBBBBB.dat       draws [A, B) of every stream of version NN
    HEAD.json                                state, current version, draws planned per stream
    leases/lease-vNN-nKKKK.{dat,json}        points for one engine process, and their keys
    ledger/shard-vNN-nKKKK*.jsonl            rows, one line per finished key
    certificate.json                         the reported estimate, written once at the stop
    posterior_weighted.dat, posterior_samples.dat

A key is (version, stream, index).  Its point is draw `index` of stream `stream` from the
version's proposal.  The plan file written when the draws are first made is the record
of the points and of their weights ln p - ln q: later ticks read it and never draw a
planned key again.  A tick reads the whole ledger and decides from the files alone:

    learning version complete   fit the next proposal; it is frozen when the freeze rule holds
    frozen version complete     estimate over its rows; stop, or plan more draws
    rows missing                write leases for them

Version 1 is the prior on the box.  No earlier fit and no other run's product enters.
"""
import glob
import hashlib
import json
import os

import numpy as np

from . import adapt, estimator
from .proposal import PriorProposal, draw_plan, proposal_from_json
from .space import GRID_COLUMNS, PARAMS, Box, from_grid

DEFAULTS = dict(seed=20261119, n_streams=4, learn_per_stream=375, frozen_per_stream=375, max_learn_versions=12,
                target_n_eff=1000.0, lnz_tol=0.05, k_max=0.7, chi2_max=11.3, max_frozen_per_stream=2000,
                min_extension=64, extension_margin=1.1, extension_factor=1.5, lnl_offset=0.0,
                n_max_samples=400000)
GRID_HEADER = ["# RIFT_HYPERPIPELINE_V1", "# lnL sigma_lnL " + " ".join(GRID_COLUMNS)]
PLAN_COLUMNS = ("stream", "index", "ok", "ln_pq", "mc", "delta", "s1z", "s2z") + GRID_COLUMNS
STATES_OVER = ("DONE", "FAILED")


def write_atomic(path, text):
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, path)


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def sha256_file(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def init(path, box, grid_header=None, **overrides):
    """Write config.json.  Refuses to replace one."""
    unknown = sorted(set(overrides) - set(DEFAULTS))
    if unknown:
        raise ValueError("unknown configuration keys: %s" % unknown)
    cfg = dict(DEFAULTS)
    cfg.update({k: type(DEFAULTS[k])(v) for k, v in overrides.items()})
    cfg["box"] = box.to_json()
    cfg["grid_header"] = list(grid_header or GRID_HEADER)
    if cfg["grid_header"][-1].split()[1:] != ["lnL", "sigma_lnL"] + list(GRID_COLUMNS):
        raise ValueError("unsupported grid columns: %s" % cfg["grid_header"][-1])
    os.makedirs(path, exist_ok=True)
    if os.path.exists(os.path.join(path, "config.json")):
        raise FileExistsError("config.json exists in %s" % path)
    write_atomic(os.path.join(path, "config.json"), json.dumps(cfg, indent=1, sort_keys=True))
    return cfg


class Run:
    def __init__(self, path):
        self.path = path
        self.cfg = json.load(open(os.path.join(path, "config.json")))
        self.box = Box.from_json(self.cfg["box"])

    def p(self, *parts):
        return os.path.join(self.path, *parts)

    # ---- proposals -------------------------------------------------------------

    def artifacts(self):
        out = [json.load(open(f)) for f in sorted(glob.glob(self.p("artifacts", "proposal-v??-*.json")))]
        if [a["version"] for a in out] != list(range(1, len(out) + 1)):
            raise SystemExit("artifact versions are not 1..n: %s" % [a["version"] for a in out])
        return out

    def publish(self, version, kind, prop, info):
        text = canonical(dict(version=version, kind=kind, proposal=prop.to_json(), fit=info))
        sha = hashlib.sha256(text.encode()).hexdigest()
        write_atomic(self.p("artifacts", "proposal-v%02d-%s.json" % (version, sha[:16])), text)
        return sha

    def head(self):
        return json.load(open(self.p("HEAD.json"))) if os.path.exists(self.p("HEAD.json")) else None

    def set_head(self, **kw):
        write_atomic(self.p("HEAD.json"), canonical(kw))
        return kw

    # ---- plans -----------------------------------------------------------------

    def plan_files(self, version):
        files = sorted(glob.glob(self.p("plans", "plan-v%02d-i*-i*.dat" % version)))
        spans = [tuple(int(x[1:]) for x in os.path.basename(f)[:-4].split("-")[2:4]) for f in files]
        at = 0
        for lo, hi in spans:
            if lo != at:
                raise SystemExit("plan files of version %d do not tile the indices: %s" % (version, spans))
            at = hi
        return files, at

    def plan(self, version, prop=None, per_stream=None):
        """The recorded plan of a version.  With a proposal and a size, draws not yet
        planned are made and recorded first."""
        files, covered = self.plan_files(version)
        if prop is not None and per_stream is not None and covered < per_stream:
            pl = draw_plan(prop, self.box, self.cfg["n_streams"], covered, per_stream)
            lines = ["# RIFT ledger plan, version %d, indices [%d, %d) of %d streams"
                     % (version, covered, per_stream, self.cfg["n_streams"]), "# " + " ".join(PLAN_COLUMNS)]
            for j in range(len(pl["ok"])):
                vals = [pl["ln_pq"][j]] + list(pl["a"][j]) + list(pl["grid"][j])
                lines.append("%d %d %d " % (pl["stream"][j], pl["index"][j], pl["ok"][j])
                             + " ".join(repr(float(v)) for v in vals))
            name = self.p("plans", "plan-v%02d-i%06d-i%06d.dat" % (version, covered, per_stream))
            write_atomic(name, "\n".join(lines) + "\n")
            files.append(name)
        if not files:
            raise SystemExit("version %d has no plan" % version)
        t = np.vstack([np.loadtxt(f, comments="#", ndmin=2) for f in files])
        t = t[np.lexsort((t[:, 1], t[:, 0]))]
        return dict(stream=t[:, 0].astype(int), index=t[:, 1].astype(int), ok=t[:, 2] > 0.5, ln_pq=t[:, 3],
                    a=t[:, 4:8], grid=t[:, 8:16])

    # ---- ledger ----------------------------------------------------------------

    def ledger(self):
        """Canonical row per key: the first one in shard-name order.  A shard may end in a
        partial line (its engine was killed); a bad line anywhere else is an error."""
        rows, stat = {}, dict(shards=0, rows=0, repeats=0, repeat_mismatch=0, partial_lines=0)
        for f in sorted(glob.glob(self.p("ledger", "shard-*.jsonl"))):
            stat["shards"] += 1
            lines = open(f).read().split("\n")
            for n, line in enumerate(lines):
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    if n != len(lines) - 1:
                        raise SystemExit("bad line %d in %s" % (n + 1, f))
                    stat["partial_lines"] += 1
                    continue
                key = (r["v"], r["s"], r["i"])
                if key in rows:
                    stat["repeats"] += 1
                    stat["repeat_mismatch"] += rows[key]["lnL"] != r["lnL"]
                else:
                    rows[key] = r
                    stat["rows"] += 1
        return rows, stat

    def lnl(self, rows, version, pl):
        """ln L minus the offset, aligned with a plan: nan where missing, -inf where failed."""
        out = np.full(len(pl["ok"]), np.nan)
        for j in np.flatnonzero(pl["ok"]):
            r = rows.get((version, int(pl["stream"][j]), int(pl["index"][j])))
            if r is None:
                continue
            if r["status"] != "OK" or r["lnL"] is None:
                out[j] = -np.inf
                continue
            if "m1" in r and abs(r["m1"] / r["m2"] - pl["grid"][j, 0] / pl["grid"][j, 1]) > 1e-9 * r["m1"] / r["m2"]:
                raise SystemExit("row %s does not carry its key's point" % (r,))
            out[j] = r["lnL"]
        return out

    def lease_text(self, grid_rows):
        return "\n".join(self.cfg["grid_header"]) + "\n" + "".join(
            " ".join("%.18e" % v for v in [0.0, 0.0] + list(g)) + "\n" for g in grid_rows)

    def write_leases(self, version, pl, missing, parts=1):
        """One lease per engine: the missing rows, split into `parts` contiguous pieces."""
        out = []
        k = len(glob.glob(self.p("leases", "lease-*.json")))
        for j in np.array_split(np.flatnonzero(missing), max(1, min(int(parts), int(missing.sum())))):
            stem = "v%02d-n%04d" % (version, k)
            k += 1
            text = self.lease_text(pl["grid"][j])
            write_atomic(self.p("leases", "lease-%s.dat" % stem), text)
            write_atomic(self.p("leases", "lease-%s.json" % stem), canonical(dict(
                version=version, seed=self.cfg["seed"], n_max_samples=self.cfg["n_max_samples"],
                keys=[[int(pl["stream"][i]), int(pl["index"][i])] for i in j],
                grid_sha256=hashlib.sha256(text.encode()).hexdigest())))
            out.append(dict(lease=self.p("leases", "lease-%s.dat" % stem), keys=self.p("leases", "lease-%s.json" % stem),
                            shard=self.p("ledger", "shard-%s.jsonl" % stem), n=int(len(j)), version=version))
        return out


def load_history(run, arts, rows, head=None):
    """Every version with its proposal, recorded plan and rows.  With HEAD, the current
    version is planned up to HEAD's size first."""
    hist = []
    for a in arts:
        prop = proposal_from_json(a["proposal"], run.box)
        grow = head is not None and a["version"] == head["version"]
        pl = run.plan(a["version"], prop if grow else None, head["per_stream"] if grow else None)
        hist.append(dict(prop=prop, plan=pl, lnl=run.lnl(rows, a["version"], pl), kind=a["kind"], fit=a["fit"]))
    return hist


def fit_next(run, hist):
    """hist: closed learning versions, oldest first."""
    a, ln_pl = [], []
    for h in hist:
        sel = h["plan"]["ok"] & np.isfinite(h["lnl"])
        a.append(h["plan"]["a"][sel])
        ln_pl.append(run.box.ln_prior_A(h["plan"]["a"][sel]) + h["lnl"][sel])
    props = [h["prop"] for h in hist]
    return adapt.next_version(np.vstack(a), np.concatenate(ln_pl), props, run.cfg["seed"] + 1000 * len(props))


def tick(run, engines=1):
    """One tick.  Returns the next action: again, run (with leases), done or failed."""
    cfg = run.cfg
    for d in ("artifacts", "plans", "leases", "ledger"):
        os.makedirs(run.p(d), exist_ok=True)
    arts = run.artifacts()
    if not arts:
        sha = run.publish(1, "learn", PriorProposal(run.box, cfg["seed"]), None)
        run.set_head(state="LEARN", version=1, per_stream=cfg["learn_per_stream"], proposal_sha=sha)
        return dict(action="again", note="published version 1")
    head = run.head()
    v = len(arts)
    if head is None or head["version"] != v:        # the tick died between the artifact and HEAD
        kind = arts[-1]["kind"]
        head = run.set_head(state="LEARN" if kind == "learn" else "FROZEN", version=v, proposal_sha=None,
                            per_stream=cfg["learn_per_stream"] if kind == "learn" else cfg["frozen_per_stream"])
    if head["state"] in STATES_OVER:
        return dict(action=head["state"].lower())
    rows, stat = run.ledger()
    hist = load_history(run, arts, rows, head)
    cur = hist[-1]
    missing = cur["plan"]["ok"] & np.isnan(cur["lnl"])
    if missing.any():
        return dict(action="run", ledger=stat, leases=run.write_leases(v, cur["plan"], missing, engines))
    if cur["kind"] == "learn":
        if v >= cfg["max_learn_versions"]:
            run.set_head(**dict(head, state="FAILED", reason="no freeze by version %d" % v))
            return dict(action="failed")
        prop, info = fit_next(run, hist)
        is_frozen = adapt.frozen([h["fit"] for h in hist if h["fit"]] + [info])
        info["frozen"] = bool(is_frozen)
        sha = run.publish(v + 1, "frozen" if is_frozen else "learn", prop, info)
        run.set_head(state="FROZEN" if is_frozen else "LEARN", version=v + 1, proposal_sha=sha,
                     per_stream=cfg["frozen_per_stream"] if is_frozen else cfg["learn_per_stream"])
        return dict(action="again", note="published version %d (%s)" % (v + 1, "frozen" if is_frozen else "learn"), fit=info)
    res = estimator.frozen_estimate(cfg, cur["plan"], cur["lnl"])
    if res["stop"] or head["per_stream"] >= cfg["max_frozen_per_stream"]:
        res.update(version=v, per_stream=head["per_stream"], draws=int(len(cur["plan"]["ok"])), ledger=stat,
                   open_indices=int(missing.sum()), certified=res["stop"],
                   proposals=[os.path.basename(f) for f in sorted(glob.glob(run.p("artifacts", "proposal-v??-*.json")))],
                   plans={os.path.basename(f): sha256_file(f) for f in run.plan_files(v)[0]},
                   fits=[h["fit"] for h in hist if h["fit"]], config=cfg,
                   integrator=dict(n_max_samples=cfg["n_max_samples"], note=(
                       "each ln L is one Monte Carlo estimate at this sample count; ln Z and its "
                       "error do not include that estimate's bias")))
        write_products(run, cur, rows, v, res)
        write_atomic(run.p("certificate.json"), json.dumps(res, indent=1, sort_keys=True))
        run.set_head(**dict(head, state="DONE" if res["stop"] else "FAILED",
                            reason="stop rule met" if res["stop"] else "draw limit reached with a check failing"))
        return dict(action="done" if res["stop"] else "failed", n_eff=res["estimate"]["n_eff"])
    per = estimator.extend(cfg, head["per_stream"], res["forecast_more"])
    run.set_head(**dict(head, per_stream=per))
    return dict(action="again", note="frozen round extended to %d per stream" % per, n_eff=res["estimate"]["n_eff"],
                checks=res["checks"], efficiency=res["efficiency"])


def write_products(run, cur, rows, version, res):
    """The frozen round as tables in the hyperpipeline grid format: every weighted row,
    and equal-weight draws from them."""
    pl, lnl = cur["plan"], cur["lnl"]
    sel = np.flatnonzero(pl["ok"] & np.isfinite(lnl))
    ln_w = lnl[sel] + pl["ln_pq"][sel]
    sigma = np.array([rows[(version, int(pl["stream"][j]), int(pl["index"][j]))].get("sigma") or 0.0 for j in sel])
    table = np.column_stack([lnl[sel] + run.cfg["lnl_offset"], sigma, pl["grid"][sel]])
    head = run.cfg["grid_header"]

    def dump(name, body, extra=""):
        write_atomic(run.p(name), "\n".join(head[:-1] + [head[-1] + extra]) + "\n" + "".join(
            " ".join("%.18e" % v for v in row) + "\n" for row in body))

    dump("posterior_weighted.dat", np.column_stack([table, ln_w - ln_w.max()]), " ln_weight")
    w = np.exp(ln_w - ln_w.max())
    rng = np.random.default_rng([run.cfg["seed"], 909])
    n_out = max(1, int(res["estimate"]["n_eff"]))
    dump("posterior_samples.dat", table[rng.choice(len(sel), size=n_out, p=w / w.sum())])
    res["products"] = dict(posterior_weighted=int(len(sel)), posterior_samples=n_out)


def status(run):
    arts, head = run.artifacts(), run.head()
    rows, stat = run.ledger()
    out = dict(head=head, versions=len(arts), ledger=stat, fits=[a["fit"] for a in arts if a["fit"]])
    by_v = {}
    for (v, _, _), _r in rows.items():
        by_v[v] = by_v.get(v, 0) + 1
    out["rows_by_version"] = by_v
    if os.path.exists(run.p("certificate.json")):
        c = json.load(open(run.p("certificate.json")))
        out["certificate"] = dict(estimate=c["estimate"], checks=c["checks"], group_chi2=c["group_chi2"],
                                  certified=c["certified"])
    return out


def verify(run):
    """Refit each proposal from the ledger and the recorded plans; compare with the
    published artifact.  Also recompute the certificate's estimate from the same files."""
    arts, rows = run.artifacts(), run.ledger()[0]
    hist = load_history(run, arts, rows)
    out = dict(fits=[], certificate=None)
    for n in range(1, len(arts)):
        prop, info = fit_next(run, hist[:n])
        info["frozen"] = bool(adapt.frozen([h["fit"] for h in hist[:n] if h["fit"]] + [info]))
        text = canonical(dict(version=n + 1, kind=arts[n]["kind"], proposal=prop.to_json(), fit=info))
        out["fits"].append(dict(version=n + 1, same_bytes=canonical(arts[n]) == text))
    if os.path.exists(run.p("certificate.json")):
        cert = json.load(open(run.p("certificate.json")))
        res = estimator.frozen_estimate(run.cfg, hist[-1]["plan"], hist[-1]["lnl"])
        out["certificate"] = dict(
            lnZ_difference=res["estimate"]["lnZ"] - cert["estimate"]["lnZ"],
            n_eff_difference=res["estimate"]["n_eff"] - cert["estimate"]["n_eff"],
            plans_match={os.path.basename(f): sha256_file(f) == cert["plans"].get(os.path.basename(f))
                         for f in run.plan_files(len(arts))[0]})
    return out


def replay_lease(run, n):
    """A lease that repeats n ledger keys: the key in flight at each recorded engine kill,
    the last row kept before it, and a seeded random choice of the rest.  Rows go to replay/."""
    arts, (rows, _) = run.artifacts(), run.ledger()
    hist = load_history(run, arts, rows)
    want = []
    chaos = run.p("ctl", "chaos.jsonl")
    for rec in (json.loads(x) for x in open(chaos)) if os.path.exists(chaos) else []:
        if rec["kill"] != "engine":
            continue
        v = int(rec["lease"].split("-")[1][1:])
        if rec["in_flight"]:
            want.append((v,) + tuple(rec["in_flight"]))
        shard = run.p("ledger", "shard-%s.jsonl" % rec["lease"][6:])
        kept = [json.loads(x) for x in open(shard).read().split("\n")[:-1]] if os.path.exists(shard) else []
        if kept:
            want.append((v, kept[-1]["s"], kept[-1]["i"]))
    ok = sorted(k for k, r in rows.items() if r["status"] == "OK")
    rng = np.random.default_rng([run.cfg["seed"], 404])
    for j in rng.permutation(len(ok)):
        if len(want) >= n:
            break
        if ok[j] not in want:
            want.append(ok[j])
    want = [k for k in dict.fromkeys(want) if k in rows and rows[k]["status"] == "OK"]
    grid = []
    for v, s, i in want:
        pl = hist[v - 1]["plan"]
        grid.append(pl["grid"][int(np.flatnonzero((pl["stream"] == s) & (pl["index"] == i))[0])])
    os.makedirs(run.p("replay"), exist_ok=True)
    write_atomic(run.p("replay", "lease.dat"), run.lease_text(grid))
    write_atomic(run.p("replay", "lease.json"), canonical(dict(
        version=0, seed=run.cfg["seed"], n_max_samples=run.cfg["n_max_samples"],
        keys=[[s, i] for _, s, i in want], versions=[v for v, _, _ in want])))
    return dict(lease=run.p("replay", "lease.dat"), keys=run.p("replay", "lease.json"),
                shard=run.p("replay", "rows.jsonl"), n=len(want), version=0)


def replay_check(run):
    rows = run.ledger()[0]
    again = [json.loads(x) for x in open(run.p("replay", "rows.jsonl")) if x.strip()]
    same = [rows[(r["v"], r["s"], r["i"])]["lnL"] == r["lnL"] for r in again]
    diff = [abs(rows[(r["v"], r["s"], r["i"])]["lnL"] - r["lnL"]) for r in again if r["lnL"] is not None]
    out = dict(keys=len(again), identical=int(sum(same)), largest_difference=float(max(diff)) if diff else None,
               versions=sorted(set(r["v"] for r in again)))
    write_atomic(run.p("replay", "result.json"), json.dumps(out, indent=1))
    return out


__all__ = ["DEFAULTS", "PARAMS", "Run", "init", "tick", "status", "verify", "replay_lease", "replay_check",
           "from_grid"]
