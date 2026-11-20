"""The ledger engine: production ILE in one process per lease, writing keyed rows.

    python -m RIFT.ledger.engine --lease-keys LEASE.json --shard SHARD.jsonl -- ILE_SCRIPT ILE_ARGS...

ILE_SCRIPT is `integrate_likelihood_extrinsic_batchmode`, run unmodified with
`--sim-grid LEASE.dat --n-events-to-analyze N --event=0` among its arguments.  Data
reading, waveform generation, precompute and the NoLoop likelihood are the script's
own code.  The engine replaces only what happens around the per-point call of the AV
sampler:

  * the point's key (version, stream, index) is read from the lease;
  * every random stream is reseeded from (run seed, key), so the row does not depend
    on which process evaluates it or on what that process did before;
  * the sampler runs to the lease's sample count, not to an n_eff target;
  * one line is appended to the shard and synced before the script continues.

Every lease row gets a line.  ILE catches an exception in one point and goes on to the
next; the engine sees the gap and writes that row with status FAILED.  While a point is
in the sampler its key is in SHARD.inflight, so a supervisor can tell a crash on one
point from an engine that never reached a point.

The four NoLoop flags and `--sampler-method AV` are required, and options known to call
the sampler more than once per point or to carry state between points are refused,
before ILE starts.  A second sampler call for one point that gets through anyway writes
an INVALID line for the key and ends the process at once.  ILE's own output files are
not used: the shard is the record.
"""
import json
import os
import runpy
import sys
import time

import numpy as np

REDLINE_FLAGS = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")
# ILE options the engine cannot honour.  optparse accepts any unambiguous prefix of an
# option name, so arguments are resolved against the script's own option list first.
ILE_REFUSED = (
    "--auto-logarithm-offset",            # the ln L offset must be one number for the run
    "--zero-likelihood", "--rom-integrate-intrinsic", "--pin-distance-to-sim", "--use-eccentricity",
    "--export-eos-index",
    "--calibration-burn-in-neff", "--sampler-warmstart-retry-neff", "--mc-error-replicas",   # a second sampler call
    "--sampler-sequential-warmstart", "--no-adapt-after-first", "--sampler-load-state",      # state from another point
    "--sampler-save-state", "--sampler-warmstart-samples",
    "--internal-soft-fail-on-cuda-error", "--internal-make-empty-file-on-error",             # failures reported as success
)
EXIT_DOUBLE_CALL = 70


def option_names(script):
    """Option names an ILE script declares, read from its text; None if it cannot be read."""
    import re
    try:
        text = open(script, errors="replace").read()
    except (OSError, TypeError):
        return None
    names = set()
    for line in text.split("\n"):
        if "add_option(" in line:
            names.update(re.findall(r'["\'](--[A-Za-z0-9_-]+)["\']', line.split("help=")[0]))
    return (names | set(REDLINE_FLAGS) | {"--sampler-method"}) if names else None


def check_ile_arguments(argv, script=None):
    """Refuse an ILE command line the engine cannot honour.  Returns {option: [values]},
    with each option under the full name optparse would resolve it to when `script` is
    the ILE script.  Without the script, any prefix of a refused option is refused."""
    known = option_names(script)
    opts, last = {}, None
    for t in argv:
        if t.startswith("--"):
            name = t.split("=", 1)[0]
            if known and name not in known:
                full = [k for k in known if k.startswith(name)]
                name = full[0] if len(full) == 1 else name       # optparse rejects the ambiguous and the unknown itself
            last = name
            opts[last] = [t.split("=", 1)[1]] if "=" in t else []
        elif last is not None:
            opts[last].append(t)
    missing = [f for f in REDLINE_FLAGS if f not in opts]
    if missing:
        raise SystemExit("ledger engine: the ILE arguments lack %s; every ILE command must carry all of %s"
                         % (" ".join(missing), " ".join(REDLINE_FLAGS)))
    bad = sorted(o for o in opts if o.startswith("--calibration") or o in ILE_REFUSED
                 or (not known and any(r.startswith(o) for r in ILE_REFUSED)))
    if bad:
        raise SystemExit("ledger engine: ILE options not supported: %s" % ", ".join(bad))
    if opts.get("--sampler-method") != ["AV"]:
        raise SystemExit("ledger engine: --sampler-method AV is required, got %r" % (opts.get("--sampler-method"),))
    return opts


class Engine:
    def __init__(self, lease_keys, shard):
        self.lease = json.load(open(lease_keys))
        self.shard = shard
        self.out = open(shard, "a")
        self.done = set()
        self.t_last = time.time()
        self.orig = None

    def key(self, idx):
        stream, index = self.lease["keys"][idx]
        version = self.lease["versions"][idx] if "versions" in self.lease else self.lease["version"]
        return int(version), int(stream), int(index)

    def write(self, idx, **fields):
        version, stream, index = self.key(idx)
        self.out.write(json.dumps(dict(dict(v=version, s=stream, i=index, lease_row=idx), **fields)) + "\n")
        self.out.flush()
        os.fsync(self.out.fileno())
        self.done.add(idx)

    def mark_skipped(self, upto):
        """Lease rows below `upto` that ILE passed over without reaching the sampler."""
        for idx in range(upto):
            if idx not in self.done:
                self.write(idx, status="FAILED", lnL=None, reason="ILE skipped the point before the sampler")

    def inflight(self, idx=None):
        path = self.shard + ".inflight"
        if idx is None:
            if os.path.exists(path):
                os.remove(path)
            return
        with open(path, "w") as f:
            json.dump(list(self.key(idx)), f)
            f.flush()
            os.fsync(f.fileno())

    def integrate(self, sampler, func, *args, **kwargs):
        from RIFT.integrators.seeding import seed_everything
        frame = sys._getframe(1)
        while frame is not None and frame.f_code.co_name != "analyze_event":
            frame = frame.f_back
        if frame is None:
            raise RuntimeError("the ledger engine needs ILE's analyze_event on the stack")
        idx, P = int(frame.f_locals["indx_event"]), frame.f_locals["P"]
        if idx in self.done:
            # ILE would catch an exception here and carry on with a row that holds only the
            # first call.  Void the key and stop.
            self.write(idx, status="INVALID", lnL=None, reason="the sampler was called twice for this point")
            sys.stderr.write("ledger engine: ILE called the sampler twice for lease row %d; this option "
                             "combination is not supported\n" % idx)
            sys.stderr.flush()
            os._exit(EXIT_DOUBLE_CALL)
        self.mark_skipped(idx)
        version, stream, index = self.key(idx)
        seed = int(np.random.SeedSequence([int(self.lease["seed"]), version, stream, index]).generate_state(1)[0])
        seed_everything(seed, verbose=False)
        t_pre = time.time() - self.t_last
        acc = [0.0, 0]
        xp = getattr(sampler, "xpy", None)

        def timed(*a, **k):
            t0 = time.time()
            val = func(*a, **k)
            if xp is not None and xp is not np:
                try:
                    xp.cuda.Stream.null.synchronize()
                except Exception:
                    pass
            acc[0] += time.time() - t0
            acc[1] += 1
            return val

        self.inflight(idx)
        t0 = time.time()
        out = self.orig(sampler, timed, *args, **dict(kwargs, neff=float("inf"), nmax=int(self.lease["n_max_samples"])))
        row = dict(status="OK", lnL=None, key_seed=seed, point=point_of(P), ntotal=int(sampler.ntotal),
                   t_int=time.time() - t0, t_like=acc[0], t_pre=t_pre)
        if out[0] is None or not np.isfinite(float(out[0])):
            row.update(status="FAILED", reason="the sampler returned no finite integral")
        else:
            row.update(lnL=float(out[0]), sigma=float(np.exp(out[1] / 2 - out[0])), effsamp=float(out[2]))
        self.write(idx, **row)
        self.inflight(None)
        self.t_last = time.time()
        return out


def point_of(P):
    """The evaluated point as a grid row: masses in solar masses, Cartesian spins."""
    import lal
    return [float(P.m1) / lal.MSUN_SI, float(P.m2) / lal.MSUN_SI, float(P.s1x), float(P.s1y), float(P.s1z),
            float(P.s2x), float(P.s2y), float(P.s2z)]


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--" not in argv:
        print(__doc__)
        return 0 if ("-h" in argv or "--help" in argv) else 2
    own, ile = argv[:argv.index("--")], argv[argv.index("--") + 1:]
    opts = dict(zip(own[::2], own[1::2]))
    if set(opts) != {"--lease-keys", "--shard"} or not ile:
        raise SystemExit(__doc__)
    check_ile_arguments(ile[1:], ile[0])
    os.environ.setdefault("RIFT_NO_GWSIGNAL", "True")
    import RIFT.integrators.mcsamplerAdaptiveVolume as av
    eng = Engine(opts["--lease-keys"], opts["--shard"])
    eng.orig = av.MCSampler.integrate
    eng.inflight(None)

    def integrate(self, func, *args, **kwargs):
        return eng.integrate(self, func, *args, **kwargs)

    av.MCSampler.integrate = integrate
    print(" [ledger engine] lease %s: %d keys, %d samples per point" % (
        opts["--lease-keys"], len(eng.lease["keys"]), eng.lease["n_max_samples"]), flush=True)
    sys.argv = list(ile)
    try:
        runpy.run_path(ile[0], run_name="__main__")
    except SystemExit as exc:
        if exc.code not in (0, None):
            raise
    # ILE came to its normal end: a row it never reached the sampler for was skipped
    eng.mark_skipped(len(eng.lease["keys"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
