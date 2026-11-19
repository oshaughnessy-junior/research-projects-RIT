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

The four NoLoop flags and `--sampler-method AV` are required and checked here.  ILE's
own output files are not used: the shard is the record.
"""
import json
import os
import runpy
import sys
import time

import numpy as np

REDLINE_FLAGS = ("--time-marginalization", "--vectorized", "--gpu", "--force-xpy")


class Engine:
    def __init__(self, lease_keys, shard):
        self.lease = json.load(open(lease_keys))
        self.out = open(shard, "a")
        self.done = set()
        self.t_last = time.time()
        self.orig = None

    def key(self, idx):
        stream, index = self.lease["keys"][idx]
        version = self.lease["versions"][idx] if "versions" in self.lease else self.lease["version"]
        return int(version), int(stream), int(index)

    def integrate(self, sampler, func, *args, **kwargs):
        from RIFT.integrators.seeding import seed_everything
        frame = sys._getframe(1)
        while frame is not None and frame.f_code.co_name != "analyze_event":
            frame = frame.f_back
        if frame is None:
            raise RuntimeError("the ledger engine needs ILE's analyze_event on the stack")
        idx, P = int(frame.f_locals["indx_event"]), frame.f_locals["P"]
        if idx in self.done:
            raise RuntimeError("ILE called the sampler twice for lease row %d; the ledger engine does not "
                               "support the option that does so (burn-in, warm-start retry, replicas)" % idx)
        self.done.add(idx)
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

        t0 = time.time()
        out = self.orig(sampler, timed, *args, **dict(kwargs, neff=float("inf"), nmax=int(self.lease["n_max_samples"])))
        row = dict(v=version, s=stream, i=index, status="OK", lnL=None, key_seed=seed, m1=float(P.m1), m2=float(P.m2),
                   ntotal=int(sampler.ntotal), t_int=time.time() - t0, t_like=acc[0], t_pre=t_pre, lease_row=idx)
        if out[0] is None or not np.isfinite(float(out[0])):
            row["status"] = "FAILED"
        else:
            row.update(lnL=float(out[0]), sigma=float(np.exp(out[1] / 2 - out[0])), effsamp=float(out[2]))
        self.out.write(json.dumps(row) + "\n")
        self.out.flush()
        os.fsync(self.out.fileno())
        self.t_last = time.time()
        return out


def check_ile_arguments(argv):
    """Refuse an ILE command line that would not reach the NoLoop likelihood under AV."""
    missing = [f for f in REDLINE_FLAGS if f not in argv]
    if missing:
        raise SystemExit("ledger engine: the ILE arguments lack %s; every ILE command must carry %s"
                         % (" ".join(missing), " ".join(REDLINE_FLAGS)))
    method = None
    for n, a in enumerate(argv):
        if a == "--sampler-method" and n + 1 < len(argv):
            method = argv[n + 1]
        elif a.startswith("--sampler-method="):
            method = a.split("=", 1)[1]
    if method != "AV":
        raise SystemExit("ledger engine: --sampler-method AV is required, got %r" % (method,))


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--" not in argv or "-h" in argv[:argv.index("--")] or "--help" in argv[:argv.index("--")]:
        print(__doc__)
        return 0 if ("-h" in argv or "--help" in argv) else 2
    own, ile = argv[:argv.index("--")], argv[argv.index("--") + 1:]
    opts = dict(zip(own[::2], own[1::2]))
    if set(opts) != {"--lease-keys", "--shard"} or not ile:
        raise SystemExit(__doc__)
    check_ile_arguments(ile[1:])
    os.environ.setdefault("RIFT_NO_GWSIGNAL", "True")
    import RIFT.integrators.mcsamplerAdaptiveVolume as av
    eng = Engine(opts["--lease-keys"], opts["--shard"])
    eng.orig = av.MCSampler.integrate

    def integrate(self, func, *args, **kwargs):
        return eng.integrate(self, func, *args, **kwargs)

    av.MCSampler.integrate = integrate
    print(" [ledger engine] lease %s: %d keys, %d samples per point" % (
        opts["--lease-keys"], len(eng.lease["keys"]), eng.lease["n_max_samples"]), flush=True)
    sys.argv = list(ile)
    runpy.run_path(ile[0], run_name="__main__")
    return 0


if __name__ == "__main__":
    sys.exit(main())
