#!/usr/bin/env python3
"""Check that each roster entry's STATUS is still true, not merely present.

WHY THIS EXISTS.  .travis/test-ci-roster.py enforces that every ungated test file carries a
reason.  It cannot tell whether the reason is CORRECT, and that gap is not theoretical: the
roster asserted "skips cleanly without them" for two jax_gp files while one of them ERRORED
without jax and the other had no guard at all (PR #248).  A reason nobody re-checks is prose,
and prose is what this whole census exists to stop being mistaken for coverage.

So each status carries a FALSIFIABLE, direction-checked predicate, and this job runs it:

  LEGACY     must FAIL to import.  If it collects, the pre-package module names it supposedly
             needs are resolving, and the file is a candidate for real gating.
  HANDRUN    must collect NO tests.  If it collects some, it is a pytest suite wearing the
             wrong label -- and one that no job runs.
  EXPENSIVE  must collect tests AND pass none of them without RIFT_RUN_EXPENSIVE.  Catches
             both a suite that stopped collecting and an opt-in guard that stopped guarding.
  OPTDEP     must declare its dependencies as `needs:<mod>[,<mod>]` or `needs:env:VAR`, none of
             which may appear in requirements.txt -- if CI installs it, it is not optional.  The
             behavioural half is keyed off whether those deps are ACTUALLY present in the running
             environment, because that differs between CIT and a runner: with one absent the file
             must not collect-and-fully-pass; with all present it may.  An earlier version of this
             check simply flagged "collects and all pass here", which fired on CIT purely because
             CIT has jax -- a check that reported the environment rather than the claim.

DELIBERATELY NOT CHECKED: which dependency an OPTDEP file wants, and whether a HANDRUN study's
internal gate still holds.  Both need the missing stack or a long run; claiming to check them
would be the same overreach this file exists to catch.  The predicates above are the part that
is decidable HERE, and the docstrings say so.

This is a SEPARATE job from ci-roster-check on purpose: that one is stdlib-only with no
`needs: install`, and must stay that way so it reports even when the install matrix is broken.
This one needs RIFT importable.
"""

import os
import re
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROSTER = os.path.join(".travis", "ci_roster.txt")
CODE = os.path.join("MonteCarloMarginalizeCode", "Code")
TIMEOUT = 300


def _read_roster():
    out = []
    for n, raw in enumerate(open(ROSTER, errors="replace"), 1):
        if raw.lstrip().startswith("#") or not raw.strip():
            continue
        bits = raw.rstrip("\n").split(None, 2)
        if len(bits) >= 3:
            out.append((n, bits[0], bits[1], bits[2].strip()))
    return out



def _declared_deps(reason):
    """Modules / env vars an OPTDEP entry claims, from `needs:a,b` or `needs:env:VAR`."""
    m = re.search(r"needs:([A-Za-z0-9_.,:]+)", reason)
    return [d for d in m.group(1).split(",") if d] if m else []


def _in_requirements(mod):
    """True if requirements.txt installs this module -- in which case it is not optional."""
    try:
        req = open(os.path.join(REPO, "requirements.txt"), errors="replace").read()
    except OSError:
        return False
    want = mod.lower().replace("-", "_")
    for line in req.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        name = re.split(r"[<>=\[]", line)[0].strip().lower().replace("-", "_")
        if name == want:
            return True
    return False


def _dep_present(dep):
    """Is this declared dependency actually available in the environment running the check?"""
    if dep.startswith("env:"):
        return bool(os.environ.get(dep[4:]))
    pr = subprocess.run([sys.executable, "-c", "import %s" % dep],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return pr.returncode == 0


def _pytest(path, extra_env=None, collect_only=True):
    """Return (rc, n_collected, n_passed).  n_passed is -1 when not run."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.join(REPO, CODE) + os.pathsep + env.get("PYTHONPATH", "")
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("MPLBACKEND", "Agg")
    env.update(extra_env or {})
    cmd = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"]
    if collect_only:
        cmd.append("--collect-only")
    cmd.append(path)
    try:
        pr = subprocess.run(cmd, env=env, cwd=REPO, timeout=TIMEOUT,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.TimeoutExpired:
        return None, None, None
    text = pr.stdout.decode("utf-8", "replace")
    if collect_only:
        return pr.returncode, len(re.findall(r"::", text)), -1
    m = re.search(r"(\d+) passed", text)
    c = re.search(r"(\d+) (?:passed|failed|skipped|error)", text)
    return pr.returncode, (1 if c else 0), (int(m.group(1)) if m else 0)


def main():
    os.chdir(REPO)
    errs, checked = [], {}
    for lineno, path, status, reason in _read_roster():
        if not os.path.exists(path):
            continue                      # ci-roster-check owns that error
        if status == "LEGACY":
            rc, n, _ = _pytest(path)
            if rc is None:
                errs.append("%s:%d: %s timed out during collection." % (ROSTER, lineno, path))
            elif n > 0:
                errs.append("%s:%d: %s is LEGACY (\"cannot be imported\") but COLLECTS %d "
                            "tests.\n    Whatever it needed now resolves. Re-check the reason: "
                            "it is probably gateable." % (ROSTER, lineno, path, n))
        elif status == "HANDRUN":
            rc, n, _ = _pytest(path)
            if rc is None:
                errs.append("%s:%d: %s timed out during collection." % (ROSTER, lineno, path))
            elif n > 0:
                errs.append("%s:%d: %s is HANDRUN (\"not a pytest target\") but COLLECTS %d "
                            "tests.\n    It is a real suite that no job runs -- gate it, or "
                            "correct the status." % (ROSTER, lineno, path, n))
        elif status == "EXPENSIVE":
            rc, n, _ = _pytest(path)
            if rc is None or n == 0:
                errs.append("%s:%d: %s is EXPENSIVE but collects nothing.\n    The opt-in "
                            "suite is gone or stopped importing." % (ROSTER, lineno, path))
            else:
                rc2, _, passed = _pytest(path, collect_only=False)
                if passed > 0:
                    errs.append("%s:%d: %s is EXPENSIVE (\"skips unless RIFT_RUN_EXPENSIVE=1\") "
                                "but %d test(s) PASSED without it.\n    The opt-in guard stopped "
                                "guarding." % (ROSTER, lineno, path, passed))
        elif status == "OPTDEP":
            deps = _declared_deps(reason)
            if not deps:
                errs.append("%s:%d: %s is OPTDEP but names no dependency.\n"
                            "    Write `needs:<module>[,<module>]` or `needs:env:VAR` in the "
                            "reason so the claim can be checked instead of believed.  Two entries "
                            "carrying only prose here turned out to collect and pass completely, "
                            "and belonged in a job." % (ROSTER, lineno, path))
            for d in deps:
                if not d.startswith("env:") and _in_requirements(d):
                    errs.append("%s:%d: %s is OPTDEP on %r, which requirements.txt DOES install.\n"
                                "    Then it is not optional -- gate the file."
                                % (ROSTER, lineno, path, d))
            missing = [d for d in deps if not _dep_present(d)]
            if missing:
                rc, n, _ = _pytest(path)
                if rc is not None and n > 0:
                    rc2, _, passed = _pytest(path, collect_only=False)
                    if rc2 == 0 and passed == n:
                        errs.append("%s:%d: %s is OPTDEP on missing %s, yet collects %d tests and "
                                    "ALL PASS.\n    It does not actually need what it claims; gate "
                                    "it, or correct the reason."
                                    % (ROSTER, lineno, path, ",".join(missing), n))
        else:
            continue
        checked[status] = checked.get(status, 0) + 1

    print("test-roster-verify: predicates checked per status")
    for s in sorted(checked):
        print("  %-10s %3d" % (s, checked[s]))
    if errs:
        print("\ntest-roster-verify: FAIL", file=sys.stderr)
        for e in errs:
            print("  " + e, file=sys.stderr)
        return 1
    print("test-roster-verify: PASS -- every checkable roster reason still holds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
