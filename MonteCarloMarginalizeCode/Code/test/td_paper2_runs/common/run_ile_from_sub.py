#!/usr/bin/env python
r"""run_ile_from_sub.py -- extract the ILE command from a generated condor ILE.sub and run ONE
point locally (the paper-2 'validate' smoke). Faithful to the DAG: it takes the exact
arguments string the pipeline emitted, substitutes the condor macros for a single evaluation,
and executes integrate_likelihood_extrinsic_batchmode -- so a finite lnL here means the
production DAG carries a working td-toeplitz + glitch-basis + window + distance-marg
configuration.

Macro substitution for one local point:
  $(macroiteration) -> --iteration (default 0; grid overlap-grid-0.xml.gz must exist)
  $(macroevent)     -> --event     (default 0)
  $(macrongroup)    -> --ngroup    (default 1 = analyze one grid point)
  $(cluster),$(process) -> 0
The condor arguments use the "old" (single-token) quoting; we join it into one string, macro-
substitute, then shlex.split into an argv list and exec directly (no bash re-quoting), dropping
condor's empty/stray quote tokens. --force-xpy is added for CPU-only smoke hosts (numpy Ylm
fallback; ILE keeps --gpu/--vectorized but runs on the CPU xpy path).

Usage:
  run_ile_from_sub.py --sub runs/MP/ILE.sub --n-max 20000 --n-eff 10 --force-xpy
"""
import argparse
import os
import re
import shlex
import subprocess
import sys


def extract_arguments(sub_path):
    txt = open(sub_path).read()
    m = re.search(r'^\s*arguments\s*=\s*"(.*)"\s*$', txt, re.MULTILINE | re.DOTALL)
    if not m:
        sys.exit("run_ile_from_sub: no arguments = \"...\" line in %s" % sub_path)
    return m.group(1)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sub", required=True, help="generated ILE.sub")
    ap.add_argument("--iteration", type=int, default=0)
    ap.add_argument("--event", type=int, default=0)
    ap.add_argument("--ngroup", type=int, default=1)
    ap.add_argument("--n-max", type=int, default=None, help="override --n-max for a fast smoke")
    ap.add_argument("--n-eff", type=int, default=None, help="override --n-eff for a fast smoke")
    ap.add_argument("--force-xpy", action="store_true", help="add --force-xpy (CPU numpy path)")
    ap.add_argument("--out-prefix", default=None, help="override --output-file prefix")
    ap.add_argument("--exe", default=None,
                    help="override the .sub executable (use the BRANCH bin for local validation; "
                         "the .sub's `which` resolves to the installed RIFT, which may predate "
                         "the td-toeplitz/glitch flags)")
    ap.add_argument("--dry-run", action="store_true", help="print the argv and exit")
    args = ap.parse_args()

    sub_dir = os.path.dirname(os.path.abspath(args.sub))
    raw = extract_arguments(args.sub)
    subs = {r"$(macroiteration)": str(args.iteration), r"$(macroevent)": str(args.event),
            r"$(macrongroup)": str(args.ngroup), r"$(cluster)": "0", r"$(process)": "0"}
    for k, v in subs.items():
        raw = raw.replace(k, v)
    # condor escapes a literal double-quote as "" ; collapse and drop stray empty quote tokens
    raw = raw.replace('""', ' ')
    argv = [t for t in shlex.split(raw) if t not in ("", "'")]

    # relative grid / lookup / output paths in the .sub are resolved against the ILE initialdir
    # (the rundir); run from there so overlap-grid-<it>.xml.gz and the distmarg lookup resolve.
    def fix(tok):
        if tok.startswith("--sim-xml"):
            return tok
        return tok
    argv = [fix(t) for t in argv]

    if args.n_max is not None:
        argv = _override(argv, "--n-max", str(args.n_max))
    if args.n_eff is not None:
        argv = _override(argv, "--n-eff", str(args.n_eff))
    if args.force_xpy and "--force-xpy" not in argv:
        argv.append("--force-xpy")
    if args.out_prefix:
        argv = [a for a in argv if not a.startswith("--output-file")]
        argv.append("--output-file=%s" % args.out_prefix)

    # the executable is the .sub's executable line
    exe = args.exe
    if exe is None:
        for line in open(args.sub):
            if line.strip().startswith("executable"):
                exe = line.split("=", 1)[1].strip()
                break
    if exe is None:
        exe = "integrate_likelihood_extrinsic_batchmode"

    print("[validate] rundir=%s" % sub_dir)
    print("[validate] exe=%s" % exe)
    print("[validate] argv:\n  " + " ".join(shlex.quote(a) for a in argv))
    if args.dry_run:
        return
    cwd = os.getcwd()
    os.chdir(sub_dir)
    try:
        rc = subprocess.call([sys.executable, exe] + argv)
    finally:
        os.chdir(cwd)
    sys.exit(rc)


def _override(argv, flag, val):
    out, i, done = [], 0, False
    while i < len(argv):
        if argv[i] == flag and i + 1 < len(argv):
            out += [flag, val]; i += 2; done = True
        elif argv[i].startswith(flag + "="):
            out += ["%s=%s" % (flag, val)]; i += 1; done = True
        else:
            out.append(argv[i]); i += 1
    if not done:
        out += [flag, val]
    return out


if __name__ == "__main__":
    main()
