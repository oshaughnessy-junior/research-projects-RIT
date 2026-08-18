"""DualCondorRunQueue's catch-and-release policy for memory holds.

`auto_release_on_oom` bumps `request_memory` and releases a job held on
HoldReasonCode 26/34, up to `oom_max_retries` times. The budget is
counted in *holds*, not in *starts*.

That distinction is the whole point of this module. `NumJobStarts`
counts every execution attempt: preemption, eviction and checkpoint
restarts all increment it, and none of them are memory events. On an
opportunistic pool they are the common case, so counting starts both
exhausts the retry budget for jobs that were never OOM-held and inflates
the memory bump by the number of times the job happened to be kicked.
Observed on the OSPool ap41 access point: a job at NumJobStarts=5 with
NumHolds=2, and a pool job at NumJobStarts=11 with NumHolds=3.

The expressions are evaluated here, not string-matched, so the tests
describe scheduler behaviour rather than the text that encodes it.
Evaluation needs the HTCondor python bindings; the shape tests do not
and stay live without them.

Run with the RIFT-importable interpreter, e.g.:

    PYTHONPATH=<...>/MonteCarloMarginalizeCode/Code \
      python -m pytest -q .../tests/test_condor_oom_release.py
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from RIFT.simulation_manager.database import (
    Archive, DualCondorRunQueue, Manifest,
)

try:                                                 # pragma: no cover
    import classad2 as classad
except ImportError:                                  # pragma: no cover
    try:
        import classad
    except ImportError:
        classad = None

needs_classad = pytest.mark.skipif(
    classad is None, reason="HTCondor python bindings not importable")


def _generator_src():
    return (
        "import json, os\n"
        "def run(params, sim_dir, level, prev_levels):\n"
        "    p = os.path.join(sim_dir, 'level_%d.json' % level)\n"
        "    with open(p, 'w') as f:\n"
        "        json.dump({'level': level}, f)\n"
        "    return p\n"
    )


@pytest.fixture
def archive(tmp_path):
    code = tmp_path / "src"
    code.mkdir()
    (code / "generator.py").write_text(_generator_src())
    manifest = Manifest.new(name="oom_release",
                            request_queue_kind="condor",
                            run_queue_kind="condor")
    return Archive(
        base_location=tmp_path / "arch", manifest=manifest,
        generator_spec={"module_path": str(code / "generator.py"),
                        "entrypoint": "generator:run"},
    )


def _build(archive, queue, level=1):
    name = archive.register({"x": 1}, target_level=level)
    return open(queue.build_worker(archive, name, level)).read()


def _command(sub_text, key):
    """The right-hand side of a single submit command."""
    hits = [l for l in sub_text.splitlines()
            if l.split("=")[0].strip().lower() == key]
    assert len(hits) == 1, hits
    return hits[0].split("=", 1)[1].strip()


def _eval(expr, **job_ad):
    """Evaluate a submit expression against a synthetic job ad.

    `MY.` is stripped first. It is submit-language scope syntax that
    condor resolves against the job ad at evaluation time -- confirmed
    by `test_the_submit_description_is_accepted_by_condor` below, which
    shows condor_submit materialising InitialRequestMemory and keeping
    the prefixed reference verbatim -- but the python bindings evaluate
    a lone ad and return Undefined for it. Stripping keeps the rest of
    the real emitted text under test."""
    got = classad.ExprTree(expr.replace("MY.", "")).eval(
        classad.ClassAd(dict(job_ad)))
    # classad.Value is an IntEnum, so Undefined and Error are truthy
    # ints: `assert _eval(...)` would pass on either, and every
    # behavioural test here would be vacuous. Refuse them at the door.
    if isinstance(got, classad.Value):
        raise AssertionError(
            "expression evaluated to {!r}, not a value: {}".format(got, expr))
    return got


# --------------------------------------------------------------------
# what the policy is counting
# --------------------------------------------------------------------

def test_the_retry_budget_is_not_spent_by_starts(archive):
    """Neither expression may consult NumJobStarts.

    Kept as a shape assertion as well as the behavioural ones below,
    because a future edit could reintroduce the attribute in a position
    the synthetic ads happen not to exercise."""
    sub = _build(archive, DualCondorRunQueue(auto_release_on_oom=True))
    assert "NumJobStarts" not in _command(sub, "periodic_release")
    assert "NumJobStarts" not in _command(sub, "request_memory")


@needs_classad
def test_preemption_does_not_consume_the_oom_budget(archive):
    """The failing case. A job kicked off its slot seven times and held
    for memory once is on its *first* OOM retry and must be released."""
    q = DualCondorRunQueue(auto_release_on_oom=True, oom_max_retries=5)
    release = _command(_build(archive, q), "periodic_release")
    assert _eval(release, HoldReasonCode=34, NumJobStarts=7, NumHolds=1)


@needs_classad
def test_the_budget_still_runs_out(archive):
    """The cap must still bite, or a genuinely too-large job cycles
    forever instead of falling through to stuck-detection."""
    q = DualCondorRunQueue(auto_release_on_oom=True, oom_max_retries=5)
    release = _command(_build(archive, q), "periodic_release")
    assert _eval(release, HoldReasonCode=34, NumJobStarts=1, NumHolds=5) is False
    assert _eval(release, HoldReasonCode=34, NumJobStarts=1, NumHolds=4)


@needs_classad
@pytest.mark.parametrize("code", [26, 34])
def test_only_memory_holds_are_released(archive, code):
    q = DualCondorRunQueue(auto_release_on_oom=True)
    release = _command(_build(archive, q), "periodic_release")
    assert _eval(release, HoldReasonCode=code, NumHolds=1)
    assert _eval(release, HoldReasonCode=13, NumHolds=1) is False


@needs_classad
def test_the_memory_bump_tracks_holds(archive):
    """Two memory holds, not the eleven times the job was restarted."""
    q = DualCondorRunQueue(auto_release_on_oom=True, oom_memory_factor=1.5)
    mem = _command(_build(archive, q), "request_memory")
    got = _eval(mem, LastHoldReasonCode=34, NumJobStarts=11, NumHolds=2,
                MemoryUsage=1000, InitialRequestMemory=4096)
    assert got == 3000


@needs_classad
def test_memory_is_untouched_when_the_hold_was_not_about_memory(archive):
    q = DualCondorRunQueue(auto_release_on_oom=True, request_memory=4096)
    mem = _command(_build(archive, q), "request_memory")
    got = _eval(mem, LastHoldReasonCode=13, NumHolds=3, MemoryUsage=1000,
                InitialRequestMemory=4096)
    assert got == 4096


# --------------------------------------------------------------------
# NumHolds is undefined until the first hold
# --------------------------------------------------------------------

@needs_classad
def test_an_unheld_job_still_gets_a_usable_memory_request(archive):
    """An undefined request_memory matches no slot and the job sits idle
    with nothing in its log to say why -- a worse failure than a wrong
    number, so the expression must not propagate undefined."""
    q = DualCondorRunQueue(auto_release_on_oom=True)
    mem = _command(_build(archive, q), "request_memory")
    got = _eval(mem, LastHoldReasonCode=34, MemoryUsage=1000,
                InitialRequestMemory=4096)          # no NumHolds
    assert got == 1500                              # 1.5 * 1 hold * 1000


@needs_classad
def test_a_first_memory_hold_is_released_even_before_the_counter_exists(archive):
    q = DualCondorRunQueue(auto_release_on_oom=True)
    release = _command(_build(archive, q), "periodic_release")
    assert _eval(release, HoldReasonCode=34) is True   # no NumHolds


# --------------------------------------------------------------------
# the policy can be turned off
# --------------------------------------------------------------------

def test_disabling_the_policy_leaves_a_plain_memory_request(archive):
    sub = _build(archive, DualCondorRunQueue(auto_release_on_oom=False,
                                             request_memory=4096))
    assert _command(sub, "request_memory") == "4096M"
    assert "periodic_release" not in sub


# --------------------------------------------------------------------
# the scheduler's own opinion
# --------------------------------------------------------------------

def test_the_submit_description_is_accepted_by_condor(archive, tmp_path):
    """No expression evaluator substitutes for condor parsing it.

    Skipped where condor_submit is absent; -dry-run contacts no schedd
    and queues nothing."""
    condor_submit = shutil.which("condor_submit")
    if condor_submit is None:
        pytest.skip("condor_submit not on PATH")
    sub = _build(archive, DualCondorRunQueue(auto_release_on_oom=True))
    path = tmp_path / "oom.sub"
    path.write_text(sub)
    out = tmp_path / "oom.dry"
    proc = subprocess.run([condor_submit, "-dry-run", str(out), str(path)],
                          capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    # The materialised ad carries a NumJobStarts=0 counter of its own,
    # so look at the two policy attributes rather than the whole file.
    policy = [l for l in out.read_text().splitlines()
              if l.split("=")[0].strip().lower()
              in ("requestmemory", "periodicrelease")]
    assert len(policy) == 2, policy
    for line in policy:
        assert "NumHolds" in line
        assert "NumJobStarts" not in line
