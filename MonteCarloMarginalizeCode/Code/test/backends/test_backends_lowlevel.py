"""Low-level test suite for ``RIFT.misc.dag_utils_generic``.

This file exercises the *backend-neutral data model* (`_GenericJob`,
`_GenericNode`, `_GenericDAG`, `_GenericManJob`) and the three bundled
:class:`WorkflowBackend` implementations (htcondor, glue, slurm) without
requiring the rest of the RIFT science stack (``lalsuite``, ``scipy``, ...).

Run::

    cd MonteCarloMarginalizeCode/Code/test/backends
    python3 test_backends_lowlevel.py

or via pytest::

    pytest test_backends_lowlevel.py -v

The test deliberately stubs out ``htcondor`` and ``glue.pipeline`` if neither
is installed, so the suite is runnable on a developer laptop with nothing
more than a stock python interpreter.
"""

import os
import re
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
import importlib
import importlib.util  # required: importlib.util is a submodule, not auto-loaded

# ---------------------------------------------------------------------------
# Locate the module under test without importing the rest of RIFT (which
# pulls in lalsimulation/scipy).
# ---------------------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
DAG_PATH = os.path.normpath(os.path.join(
    HERE, "..", "..", "RIFT", "misc", "dag_utils_generic.py"
))


# ---------------------------------------------------------------------------
# Stub modules so the htcondor / glue backends can be exercised without the
# real packages installed.  The tests fall back to these stubs only if the
# real package is not importable.
# ---------------------------------------------------------------------------

def _strip_comments(script):
    """Drop comment lines that are not sbatch directives.

    The Slurm backend deliberately preserves the original HTCondor submit
    commands as comments, and those contain ``$(macro...)`` text.  Auditing
    them would report every preserved comment as a defect.
    """
    kept = []
    for line in script.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") and not stripped.startswith("#SBATCH"):
            continue
        kept.append(line)
    return "\n".join(kept)


def _has_classad(text, name, value):
    """True if *text* declares custom ClassAd *name* under either spelling.

    ``+Name = v`` (submit-file syntax) and ``MY.Name = v`` (the modern
    equivalent the htcondor python bindings emit) are the same thing.
    """
    return ("+{} = {}".format(name, value) in text
            or "MY.{} = {}".format(name, value) in text)


def _install_htcondor_stub():
    """Provide a tiny `htcondor` shim if the real package isn't available."""
    try:
        import htcondor  # noqa: F401
        return False  # already real
    except ImportError:
        pass
    stub = types.ModuleType("htcondor")

    class _Submit(object):
        def __init__(self, d):
            self._d = dict(d)

        def __str__(self):
            return "\n".join("{} = {}".format(k, v) for k, v in self._d.items())

    stub.Submit = _Submit
    sys.modules["htcondor"] = stub
    return True


def _install_glue_stub():
    """Provide a tiny `glue.pipeline` shim if the real package isn't available."""
    try:
        from glue import pipeline  # noqa: F401
        return False
    except ImportError:
        pass
    pipeline_src = textwrap.dedent('''
        class CondorDAGJob(object):
            def __init__(self, universe="vanilla", executable=None):
                self.universe = universe; self.executable = executable
                self.opts=[]; self.short_opts=[]; self.file_opts=[]; self.var_opts=[]
                self.args=[]; self.condor_cmds=[]
                self.sub_file=None; self.log_file=None; self.stdout=None; self.stderr=None
                self._CondorJob__queue = 1
            def set_sub_file(self, f): self.sub_file=f
            def get_sub_file(self): return self.sub_file
            def set_log_file(self, f): self.log_file=f
            def set_stdout_file(self, f): self.stdout=f
            def set_stderr_file(self, f): self.stderr=f
            def add_opt(self,n,v=None): self.opts.append((n,v))
            def add_short_opt(self,n,v): self.short_opts.append((n,v))
            def add_var_opt(self,n): self.var_opts.append(n)
            def add_file_opt(self,n,v): self.file_opts.append((n,v))
            def add_arg(self,a): self.args.append(a)
            def add_condor_cmd(self,k,v): self.condor_cmds.append((k,v))
            def write_sub_file(self):
                with open(self.sub_file,"w") as fh:
                    fh.write("# (glue.pipeline stub)\\n")
                    fh.write("universe = {}\\n".format(self.universe))
                    fh.write("executable = {}\\n".format(self.executable))
                    parts=["--{}={}".format(k,v) if v is not None else "--{}".format(k) for k,v in self.opts]
                    parts+=["-{} {}".format(k,v) for k,v in self.short_opts]
                    parts+=["--{}={}".format(k,v) for k,v in self.file_opts]
                    parts+=["--{}=$(macro{})".format(n,n) for n in self.var_opts]
                    parts+=[str(a) for a in self.args]
                    if parts: fh.write("arguments = " + " ".join(parts) + "\\n")
                    if self.log_file: fh.write("log = {}\\n".format(self.log_file))
                    if self.stdout: fh.write("output = {}\\n".format(self.stdout))
                    if self.stderr: fh.write("error = {}\\n".format(self.stderr))
                    for k,v in self.condor_cmds:
                        fh.write("{} = {}\\n".format(k,v))
                    fh.write("queue {}\\n".format(self._CondorJob__queue))

        class CondorDAG(object):
            def __init__(self, log=None): self.nodes=[]; self.dag_file=None; self.log=log
            def add_node(self,n): self.nodes.append(n)
            def set_dag_file(self,f): self.dag_file=f
            def write_concrete_dag(self):
                path = self.dag_file if self.dag_file.endswith(".dag") else (self.dag_file + ".dag")
                with open(path,"w") as fh:
                    for n in self.nodes:
                        fh.write("# (glue stub) JOB {} {}\\n".format(n.name, getattr(n.job, "sub_file", "<subdag>")))
                        for k,v in n.macros.items():
                            fh.write("VARS {} {}=\\"{}\\"\\n".format(n.name,k,v))
                    for n in self.nodes:
                        for p in n.parents:
                            fh.write("PARENT {} CHILD {}\\n".format(p.name, n.name))

        class CondorDAGNode(object):
            _ctr=0
            def __init__(self,job):
                CondorDAGNode._ctr += 1
                self.name = "g_{}".format(CondorDAGNode._ctr)
                self.job=job; self.macros={}; self.parents=[]; self.cat=None; self.retry=0
            def add_macro(self,k,v): self.macros[k]=v
            def set_category(self,c): self.cat=c
            def set_retry(self,n): self.retry=n
            def add_parent(self,p): self.parents.append(p)

        class CondorDAGManJob(object):
            def __init__(self, f): self.f = f
            def create_node(self):
                n = CondorDAGNode.__new__(CondorDAGNode)
                CondorDAGNode._ctr += 1
                n.name = "g_{}".format(CondorDAGNode._ctr)
                n.job=None; n.macros={}; n.parents=[]; n.cat=None; n.retry=0
                return n
        ''')
    pkg = types.ModuleType("glue")
    pkg.__path__ = []
    pipeline_mod = types.ModuleType("glue.pipeline")
    exec(pipeline_src, pipeline_mod.__dict__)
    pkg.pipeline = pipeline_mod
    sys.modules["glue"] = pkg
    sys.modules["glue.pipeline"] = pipeline_mod
    return True


# ---------------------------------------------------------------------------
# Loader: load the module under test in isolation.
# ---------------------------------------------------------------------------

def _load_module():
    spec = importlib.util.spec_from_file_location("dag_utils_generic", DAG_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not locate dag_utils_generic.py at " + DAG_PATH)
    sys.modules.pop("dag_utils_generic", None)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_with_stubs():
    """Install stubs (if necessary) and return a freshly-loaded module."""
    _install_htcondor_stub()
    _install_glue_stub()
    return _load_module()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class GenericJobApiTests(unittest.TestCase):
    """Tests for the backend-neutral data model itself."""

    def setUp(self):
        self.m = _load_with_stubs()

    def test_facade_records_state(self):
        job = self.m.CondorDAGJob(universe="vanilla", executable="/bin/true")
        job.set_sub_file("/tmp/t.sub")
        job.set_log_file("/tmp/t.log")
        job.set_stdout_file("/tmp/t.out")
        job.set_stderr_file("/tmp/t.err")
        job.add_opt("output-file", "x.dat")
        job.add_opt("flag", None)
        job.add_short_opt("v", "3")
        job.add_arg("positional")
        job.add_var_opt("event")
        job.add_condor_cmd("request_memory", "4096M")
        job.add_condor_cmd("request_disk", "1024M")
        job.add_condor_cmd("getenv", "True")
        job.add_condor_cmd("MY.CustomThing", '"hello"')
        job.add_backend_cmd("default", "site_policy", "required")
        job.add_backend_cmd("slurm", "constraint", "x86_64")
        job._CondorJob__queue = 5

        d = job.to_dict()
        self.assertEqual(d["universe"], "vanilla")
        self.assertEqual(d["executable"], "/bin/true")
        self.assertEqual(d["sub_file"], "/tmp/t.sub")
        self.assertEqual(d["queue_count"], 5)
        self.assertIn(("output-file", "x.dat"), d["opts"])
        self.assertIn(("flag", None), d["opts"])
        self.assertIn(("v", "3"), d["short_opts"])
        self.assertIn("event", d["var_opts"])
        self.assertIn("positional", d["arguments"])
        # Semantic resource extraction
        self.assertEqual(d["resources"]["memory"], "4096M")
        self.assertEqual(d["resources"]["disk"], "1024M")
        # getenv -> inherit_environment
        self.assertTrue(d["inherit_environment"])
        # Custom (unknown) condor_cmd preserved verbatim
        self.assertIn(("MY.CustomThing", '"hello"'), d["condor_cmds"])
        self.assertEqual(d["backend_cmds"]["default"],
                         [("site_policy", "required")])
        self.assertEqual(d["backend_cmds"]["slurm"],
                         [("constraint", "x86_64")])

    def test_legacy_private_attributes(self):
        """Code that does ``job._CondorJob__arguments.remove(...)`` must work."""
        job = self.m.CondorDAGJob(universe="vanilla", executable="/bin/echo")
        job.add_arg("a")
        job.add_arg("b")
        job.add_arg("c")
        self.assertEqual(job._CondorJob__arguments, ["a", "b", "c"])
        job._CondorJob__arguments.remove("b")
        self.assertEqual(job.arguments, ["a", "c"])
        # Setter
        job._CondorJob__queue = 9
        self.assertEqual(job.queue_count, 9)

    def test_dag_node_factory_chain(self):
        job = self.m.CondorDAGJob(executable="/bin/echo")
        job.set_sub_file("/tmp/x.sub")
        n = self.m.CondorDAGNode(job)
        n.add_macro("event", "7")
        n.set_retry(3)
        self.assertEqual(n.retry, 3)
        self.assertEqual(n.macros["event"], "7")
        # Node name is "<exe-basename>-<md5>" per the glue.pipeline format
        # (so workflows from independent runs can be safely combined without
        # name collisions).
        self.assertTrue(n.name.startswith("echo-"),
                        "name was {!r}".format(n.name))
        # Legacy private attribute used by RIFT consumers writing custom
        # POST items must mirror node.name exactly.
        self.assertEqual(n._CondorDAGNode__md5name, n.name)
        # And must be unique across nodes
        n2 = self.m.CondorDAGNode(job)
        self.assertNotEqual(n.name, n2.name)

    def test_subdag_node(self):
        manjob = self.m.CondorDAGManJob("/path/to/inner.dag")
        node = manjob.create_node()
        self.assertTrue(isinstance(node, self.m._GenericSubdagNode))
        self.assertEqual(node.subdag_file, "/path/to/inner.dag")


class BackendRegistryTests(unittest.TestCase):

    def setUp(self):
        self.m = _load_with_stubs()

    def test_three_backends_registered(self):
        names = sorted(self.m._BACKENDS.keys())
        self.assertIn("htcondor", names)
        self.assertIn("glue", names)
        self.assertIn("slurm", names)

    def test_set_backend_unknown_raises(self):
        with self.assertRaises(KeyError):
            self.m.set_backend("nonexistent")

    def test_register_custom_backend(self):
        class MyBackend(self.m.WorkflowBackend):
            name = "mytest"
            def emit_job(self, job, path):
                with open(path, "w") as fh:
                    fh.write("# my backend\n")
            def emit_dag(self, dag, path):
                with open(path, "w") as fh:
                    fh.write("# my dag\n")

        self.m.register_backend(MyBackend())
        self.m.set_backend("mytest")
        self.assertEqual(self.m.current_backend_name(), "mytest")
        with tempfile.TemporaryDirectory() as td:
            sub = os.path.join(td, "x.sub")
            j = self.m.CondorDAGJob(executable="/bin/true")
            j.set_sub_file(sub)
            j.write_sub_file()
            with open(sub) as fh:
                self.assertIn("# my backend", fh.read())


class HTCondorBackendTests(unittest.TestCase):

    def setUp(self):
        self.m = _load_with_stubs()
        self.m.set_backend("htcondor")

    def test_emit_job_writes_submit_file(self):
        with tempfile.TemporaryDirectory() as td:
            job = self.m.CondorDAGJob(universe="vanilla", executable="/bin/echo")
            job.set_sub_file(os.path.join(td, "j.sub"))
            job.set_log_file(os.path.join(td, "j.log"))
            job.set_stdout_file(os.path.join(td, "j.out"))
            job.set_stderr_file(os.path.join(td, "j.err"))
            job.add_opt("foo", "bar")
            job.add_arg("hello")
            job.add_var_opt("event")
            job.add_condor_cmd("request_memory", "2048M")
            job.add_backend_cmd("default", "+Campaign", '"GWTC5"')
            job.add_backend_cmd("htcondor", "+PreCmd", '"ile_pre.sh"')
            job.add_backend_cmd("slurm", "constraint", "not-for-condor")
            job._CondorJob__queue = 3
            job.write_sub_file()

            with open(os.path.join(td, "j.sub")) as _fh:
                text = _fh.read()
            self.assertIn("universe = vanilla", text)
            self.assertIn("executable = /bin/echo", text)
            self.assertIn("--foo=bar", text)
            self.assertIn("--event=$(macroevent)", text)
            self.assertIn("hello", text)
            self.assertIn("request_memory = 2048M", text)
            # HTCondor accepts a custom ClassAd as either `+Name` or `MY.Name`,
            # and the real python bindings normalize the former to the latter.
            # This suite runs against a *stub* htcondor whenever the real
            # package is absent (which is the case in CI), so asserting one
            # spelling passes in CI and fails on every host that actually has
            # HTCondor installed.  Accept either.
            self.assertTrue(_has_classad(text, "Campaign", '"GWTC5"'), text)
            self.assertTrue(_has_classad(text, "PreCmd", '"ile_pre.sh"'), text)
            self.assertNotIn("not-for-condor", text)
            self.assertIn("queue 3", text)

    def test_emit_dag_writes_dagman_file(self):
        with tempfile.TemporaryDirectory() as td:
            j = self.m.CondorDAGJob(executable="/bin/echo")
            j.set_sub_file(os.path.join(td, "a.sub"))
            j.write_sub_file()
            n1 = self.m.CondorDAGNode(j); n1.add_macro("event", "7"); n1.set_retry(2)
            n2 = self.m.CondorDAGNode(j); n2.add_parent(n1)
            dag = self.m.CondorDAG()
            dag.set_environment("RIFT_HYPERPIPELINE_FORMAT", "1")
            dag.add_node(n1); dag.add_node(n2)
            dag.set_dag_file(os.path.join(td, "wf.dag"))
            dag.write_concrete_dag()
            with open(os.path.join(td, "wf.dag")) as _fh:
                text = _fh.read()
            self.assertIn("JOB " + n1.name + " " + j.get_sub_file(), text)
            self.assertIn("ENV SET RIFT_HYPERPIPELINE_FORMAT=1", text)
            self.assertEqual(
                text.count("ENV SET RIFT_HYPERPIPELINE_FORMAT=1"), 1)
            self.assertIn('VARS {} event="7"'.format(n1.name), text)
            self.assertIn("RETRY {} 2".format(n1.name), text)
            self.assertIn("PARENT {} CHILD {}".format(n1.name, n2.name), text)


class GlueBackendTests(unittest.TestCase):

    def setUp(self):
        self.m = _load_with_stubs()
        self.m.set_backend("glue")

    def test_emit_job_round_trips_through_glue(self):
        with tempfile.TemporaryDirectory() as td:
            job = self.m.CondorDAGJob(universe="vanilla", executable="/bin/cat")
            job.set_sub_file(os.path.join(td, "g.sub"))
            job.add_opt("input", "x.txt")
            job.add_arg("positional_arg")
            job.add_condor_cmd("request_memory", "1024")
            job.write_sub_file()
            with open(os.path.join(td, "g.sub")) as _fh:
                text = _fh.read()
            # glue stub writes a comment header and the universe line
            self.assertIn("universe = vanilla", text)
            self.assertIn("executable = /bin/cat", text)
            # glue.pipeline emits "--input x.txt" (space-separated); the
            # htcondor backend uses "--input=x.txt".  Accept either.
            self.assertTrue(
                "--input=x.txt" in text or "--input x.txt" in text,
                "expected --input/x.txt in: {!r}".format(text),
            )
            self.assertIn("positional_arg", text)
            self.assertIn("request_memory = 1024", text)

    def test_emit_dag_through_glue(self):
        with tempfile.TemporaryDirectory() as td:
            j = self.m.CondorDAGJob(executable="/bin/echo")
            j.set_sub_file(os.path.join(td, "a.sub"))
            j.write_sub_file()
            n1 = self.m.CondorDAGNode(j)
            n2 = self.m.CondorDAGNode(j); n2.add_parent(n1)
            dag = self.m.CondorDAG()
            dag.set_environment("RIFT_HYPERPIPELINE_FORMAT", "1")
            dag.add_node(n1); dag.add_node(n2)
            dag.set_dag_file(os.path.join(td, "wf.dag"))
            dag.write_concrete_dag()
            with open(os.path.join(td, "wf.dag")) as _fh:
                text = _fh.read()
            self.assertIn("PARENT", text)
            self.assertIn("ENV SET RIFT_HYPERPIPELINE_FORMAT=1", text)


class LocalBackendTests(unittest.TestCase):
    """The local backend RUNS things, so these tests run them.

    Every case here is a way a workflow can fail while still reporting
    success -- the failure mode this backend exists to eliminate, and the one
    a static inspection of the emitted scripts cannot see.
    """

    def setUp(self):
        self.m = _load_with_stubs()
        self.m.set_backend("local")

    def _script_job(self, td, name, body, retry=0):
        """A node whose payload is a real script file.

        Deliberately NOT `/bin/bash -c "<body>"`: HTCondor's argument syntax
        splits on whitespace unless a group is single-quoted, so a body passed
        as one add_arg would arrive as several argv elements -- and the test
        would exercise the quoting rules rather than the thing under test.
        """
        payload = os.path.join(td, name + "_payload.sh")
        with open(payload, "w") as fh:
            fh.write("#!/bin/bash\n" + body + "\n")
        os.chmod(payload, 0o755)
        job = self.m.CondorDAGJob(executable=payload)
        job.set_sub_file(os.path.join(td, name + ".sh"))
        job.write_sub_file()
        node = self.m.CondorDAGNode(job)
        node.set_retry(retry)
        return job, node

    def _run(self, td, dag):
        dag.set_dag_file(os.path.join(td, "wf.dag"))
        dag.write_concrete_dag()
        return subprocess.run(["bash", dag.dag_file], cwd=td, text=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=120)

    def test_failing_subdag_stops_the_workflow(self):
        """A bare `bash child.sh` in a driver without -e ignores the failure."""
        with tempfile.TemporaryDirectory() as td:
            child = os.path.join(td, "child_local.sh")
            with open(child, "w") as fh:
                fh.write("#!/bin/bash\nexit 3\n")
            _job, after = self._script_job(
                td, "after", "echo AFTER-SUBDAG > " + os.path.join(td, "after.txt"))
            subdag = self.m.CondorDAGManJob(os.path.join(td, "child.dag"))
            sub_node = subdag.create_node()
            after.add_parent(sub_node)
            dag = self.m.CondorDAG()
            dag.add_node(sub_node)
            dag.add_node(after)
            result = self._run(td, dag)
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertFalse(os.path.exists(os.path.join(td, "after.txt")),
                             "a downstream node ran after a failed sub-workflow")

    def test_abort_dag_on_ends_the_workflow_with_its_return_value(self):
        """RIFT's convergence test exits 1 to mean "converged, stop".

        Without ABORT-DAG-ON that is an ordinary failure, and the run ends
        with no posterior -- success turned into failure by the driver.
        """
        with tempfile.TemporaryDirectory() as td:
            _job, converge = self._script_job(td, "converge", "exit 1")
            _job2, after = self._script_job(
                td, "after", "echo RAN > " + os.path.join(td, "after.txt"))
            after.add_parent(converge)
            dag = self.m.CondorDAG()
            dag.add_node(converge)
            dag.add_node(after)
            dag.set_dag_file(os.path.join(td, "wf.dag"))
            dag.write_concrete_dag()
            # Added AFTER the write, exactly as BasicIteration does it.
            dag.add_abort_on(converge, 1, 0)
            result = subprocess.run(["bash", dag.dag_file], cwd=td, text=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=120)
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertIn("ABORT-DAG-ON", result.stdout)
            self.assertFalse(os.path.exists(os.path.join(td, "after.txt")))

    def test_control_logic_added_after_writing_is_not_pasted_verbatim(self):
        """DAGMan text appended to a bash driver is a syntax error, or worse.

        BasicIteration calls write_concrete_dag() and only then adds its
        SCRIPT POST guards.  If the backend does not re-emit, those guards
        vanish silently and the driver ends up with `SCRIPT POST ...` lines
        that bash tries to execute.
        """
        with tempfile.TemporaryDirectory() as td:
            guard = os.path.join(td, "guard.sh")
            with open(guard, "w") as fh:
                fh.write("#!/bin/bash\ntouch " + os.path.join(td, "guard.txt") + "\n")
            os.chmod(guard, 0o755)
            _job, node = self._script_job(td, "work", "true")
            dag = self.m.CondorDAG()
            dag.add_node(node)
            dag.set_dag_file(os.path.join(td, "wf.dag"))
            dag.write_concrete_dag()
            dag.add_script_post(node, guard)
            dag.set_dot_file("vis.dot")

            text = open(dag.dag_file).read()
            for directive in ("SCRIPT POST ", "DOT ", "ABORT-DAG-ON "):
                for line in text.splitlines():
                    self.assertFalse(
                        line.startswith(directive),
                        "DAGMan directive pasted into a bash driver: " + line)
            result = subprocess.run(["bash", dag.dag_file], cwd=td, text=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=120)
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertTrue(os.path.exists(os.path.join(td, "guard.txt")),
                            "the SCRIPT POST guard was dropped")

    def test_failing_post_script_fails_the_workflow(self):
        """Belt and braces, and deliberately so.

        Two independent mechanisms stop the workflow here: run_step checks the
        hook's status, and the driver runs under `set -e`.  That means this
        test is NOT lethal to removing either one alone -- I checked -- so it
        is a behaviour assertion rather than a guard on one line of emission.
        The guard on the hook actually being emitted is
        test_control_logic_added_after_writing_is_not_pasted_verbatim.
        """
        with tempfile.TemporaryDirectory() as td:
            guard = os.path.join(td, "guard.sh")
            with open(guard, "w") as fh:
                fh.write("#!/bin/bash\nexit 4\n")
            os.chmod(guard, 0o755)
            _job, node = self._script_job(td, "work", "true")
            _job2, after = self._script_job(
                td, "after", "echo RAN > " + os.path.join(td, "after.txt"))
            after.add_parent(node)
            dag = self.m.CondorDAG()
            dag.add_node(node)
            dag.add_node(after)
            dag.add_script_post(node, guard)
            result = self._run(td, dag)
            self.assertNotEqual(result.returncode, 0, result.stdout)
            # The status alone is weak: an unchecked failing command can leave
            # a nonzero status behind while the workflow carries on.  What
            # must not happen is the next node running.
            self.assertFalse(os.path.exists(os.path.join(td, "after.txt")),
                             "a downstream node ran after a failed POST script")

    def test_queue_count_runs_every_process(self):
        """`queue N` is ILE's --n-copies; running it once under-produces."""
        with tempfile.TemporaryDirectory() as td:
            out = os.path.join(td, "copies.txt")
            job, node = self._script_job(
                td, "copies", "echo COPY >> " + out)
            job._CondorJob__queue = 5
            job.write_sub_file()
            dag = self.m.CondorDAG()
            dag.add_node(node)
            result = self._run(td, dag)
            self.assertEqual(result.returncode, 0, result.stdout)
            with open(out) as fh:
                self.assertEqual(len(fh.read().split()), 5)

    def test_script_hooks_get_the_argv_the_caller_wrote(self):
        """PRE/POST hook arguments must survive to the script's argv intact.

        Three separate ways this was wrong, all silent -- the hook ran and
        received different arguments than the caller passed:

        * `$(JOBID)` / `$(RETURN)` were pasted as raw text, so bash treated
          them as command substitution: "JOBID: command not found", and every
          later argument shifted down two positions;
        * an argument containing a space split into two, because the API
          `" ".join`ed the `*args` it was given and threw the boundaries away;
        * a `$(macro)` reference was not substituted at all.

        The production call site is the ILE post script, which is passed
        exactly `$(JOBID) $(RETURN) <iteration> <target>`.
        """
        with tempfile.TemporaryDirectory() as td:
            out = os.path.join(td, "argv.txt")
            hook = os.path.join(td, "post.sh")
            with open(hook, "w") as fh:
                fh.write("#!/bin/bash\nprintf '[%s]' \"$@\" > " + out + "\n")
            os.chmod(hook, 0o755)
            spacey = os.path.join(td, "a dir", "file.txt")
            os.makedirs(os.path.dirname(spacey))
            job, node = self._script_job(td, "work", "true")
            job.write_sub_file()
            node.add_macro("macroiteration", 7)
            dag = self.m.CondorDAG()
            dag.add_node(node)
            dag.add_script_post(node, hook, "$(JOBID)", "$(RETURN)",
                                "$(macroiteration)", spacey, "plain")
            result = self._run(td, dag)
            self.assertEqual(result.returncode, 0, result.stdout)
            with open(out) as fh:
                argv = fh.read()
            self.assertNotIn("command not found", result.stdout)
            self.assertIn("[7]", argv)                 # macro substituted
            self.assertIn("[" + spacey + "]", argv)    # one arg, not two
            self.assertIn("[plain]", argv)
            self.assertIn("[0]", argv)                 # $(RETURN) of a success

    def test_a_failing_POST_script_is_retried_with_the_node(self):
        """DAGMan retries PRE+JOB+POST as one unit; the POST decides success.

        RIFT depends on it: the cip-explode design pairs RETRY 1000 with a POST
        script whose own comment says it "will USUALLY FAIL and get retried A
        LARGE NUMBER OF TIMES, until we complete work".  A driver that exits on
        the first POST failure makes that adaptive batching inert -- the
        workflow dies on attempt one instead of converging.
        """
        with tempfile.TemporaryDirectory() as td:
            counter = os.path.join(td, "count")
            hook = os.path.join(td, "post.sh")
            with open(hook, "w") as fh:
                # Fails twice, then succeeds -- exactly the shape RIFT relies on.
                fh.write(
                    "#!/bin/bash\n"
                    "n=$(cat {0} 2>/dev/null || echo 0); n=$((n+1));"
                    " echo $n > {0}\n"
                    "[ \"$n\" -ge 3 ]\n".format(counter))
            os.chmod(hook, 0o755)
            job, node = self._script_job(td, "work", "true")
            job.write_sub_file()
            node.set_retry(5)
            dag = self.m.CondorDAG()
            dag.add_node(node)
            dag.add_script_post(node, hook)
            result = self._run(td, dag)
            self.assertEqual(result.returncode, 0, result.stdout)
            with open(counter) as fh:
                self.assertEqual(fh.read().strip(), "3")

    def test_a_macro_that_is_not_named_macroSomething_still_resolves(self):
        """`add_macro` takes any key, and RIFT uses `ifo`.

        The macro layer used to recognise only `macro*`/`cluster`/`process`, so
        `initialdir = <dir>/$(ifo)` was emitted literally and every detector
        shared one directory NAMED `$(ifo)`.  `mkdir -p` made that succeed, so
        neither `set -u` nor `set -e` could see it -- the silent wrong-directory
        failure the strict rendering exists to prevent, walking past the
        strictness because the token never reached `macro_ref`.
        """
        with tempfile.TemporaryDirectory() as td:
            job, node = self._script_job(td, "work", "pwd > out.txt")
            job.add_condor_cmd("initialdir", os.path.join(td, "$(ifo)"))
            job.write_sub_file()
            node.add_macro("ifo", "H1")
            dag = self.m.CondorDAG()
            dag.add_node(node)
            result = self._run(td, dag)
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertTrue(os.path.isdir(os.path.join(td, "H1")),
                            "expected a per-IFO directory; got "
                            + repr(sorted(os.listdir(td))))
            self.assertFalse(os.path.exists(os.path.join(td, "$(ifo)")))

    def test_an_unassigned_macro_in_a_LOG_PATH_is_not_fatal(self):
        """HTCondor is lenient here, and RIFT depends on it.

        BasicIteration's `convert_extr` node names its log
        `batchconvert-$(macroevent).err` and never assigns `macroevent`;
        HTCondor expands that to nothing and the job runs.  A shell backend
        that is strict everywhere turns it into a hard failure -- which is a
        behaviour difference, not a stricter check.  Found by running the
        legacy builder's extrinsic path, where it killed both arms of a
        comparison.
        """
        with tempfile.TemporaryDirectory() as td:
            out = os.path.join(td, "ran.txt")
            job, node = self._script_job(td, "work", "echo ok > " + out)
            job.set_stdout_file(os.path.join(
                td, "logs", "work-$(macroevent).out"))
            job.set_stderr_file(os.path.join(
                td, "logs", "work-$(macroevent).err"))
            job.write_sub_file()
            node.add_macro("macroiteration", 0)   # but NOT macroevent
            dag = self.m.CondorDAG()
            dag.add_node(node)
            result = self._run(td, dag)
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertTrue(os.path.exists(out),
                            "the job did not run:\n" + result.stdout)

    def test_an_unassigned_macro_is_fatal_not_empty(self):
        """The defect this backend exists to expose must not be survivable.

        A job that reads a macro its node never assigned would, without
        `set -u`, run with an empty argument and exit zero.  Note the contrast
        with the log-path case above: strictness belongs where an empty
        expansion changes WHAT RUNS, and not where it only changes a filename.
        """
        with tempfile.TemporaryDirectory() as td:
            job = self.m.CondorDAGJob(executable="/bin/echo")
            job.add_opt("event", "$(macroevent)")
            job.set_sub_file(os.path.join(td, "j.sh"))
            job.write_sub_file()
            node = self.m.CondorDAGNode(job)   # no add_macro at all
            dag = self.m.CondorDAG()
            dag.add_node(node)
            result = self._run(td, dag)
            self.assertNotEqual(
                result.returncode, 0,
                "a job read an unassigned macro and still succeeded:\n"
                + result.stdout)


class SlurmBackendTests(unittest.TestCase):

    def setUp(self):
        self.m = _load_with_stubs()
        self.m.set_backend("slurm")

    def test_emit_job_writes_sbatch(self):
        with tempfile.TemporaryDirectory() as td:
            job = self.m.CondorDAGJob(universe="vanilla", executable="/bin/echo")
            job.set_sub_file(os.path.join(td, "j.sbatch"))
            job.set_stdout_file("/tmp/j.out")
            job.set_stderr_file("/tmp/j.err")
            job.add_arg("hello")
            job.add_condor_cmd("request_memory", "4096M")
            job.add_condor_cmd("request_disk", "2048M")
            job.add_condor_cmd("request_cpus", "4")
            job.add_condor_cmd("request_gpus", "1")
            job.add_condor_cmd("getenv", "True")
            job.add_condor_cmd("accounting_group", "ligo.test")
            job.add_condor_cmd("+SlurmPartition", '"compute"')
            job.add_backend_cmd("default", "exclusive", None)
            job.add_backend_cmd("slurm", "constraint", "x86_64")
            job.add_backend_cmd("htcondor", "+PreCmd", '"ile_pre.sh"')
            job._CondorJob__queue = 4
            job.write_sub_file()
            with open(os.path.join(td, "j.sbatch")) as _fh:
                text = _fh.read()
            self.assertTrue(text.startswith("#!/bin/bash"))
            self.assertIn("#SBATCH --mem=4096M", text)
            self.assertIn("#SBATCH --tmp=2048M", text)
            self.assertIn("#SBATCH --cpus-per-task=4", text)
            self.assertIn("#SBATCH --gres=gpu:1", text)
            self.assertIn("#SBATCH --array=0-3", text)
            self.assertIn("#SBATCH --output=/tmp/j.out", text)
            self.assertIn("#SBATCH --error=/tmp/j.err", text)
            self.assertIn("#SBATCH --account=ligo.test", text)
            self.assertIn("#SBATCH --partition=compute", text)
            self.assertIn("#SBATCH --exclusive", text)
            self.assertIn("#SBATCH --constraint=x86_64", text)
            self.assertNotIn("ile_pre.sh", text)
            self.assertIn("#SBATCH --export=ALL", text)
            # Each argv element is a separate double-quoted bash word: the
            # shell must not glob or word-split what HTCondor would have
            # passed to exec() verbatim.
            self.assertIn('exec "/bin/echo" "hello"', text)
            # Original Condor commands should be preserved as comments.
            self.assertIn("# Original HTCondor submit-file commands", text)

    # ------------------------------------------------------------------
    # Per-node parameterization
    # ------------------------------------------------------------------
    # These are round-trip tests on purpose.  Asserting that a particular
    # string appears in the sbatch script cannot catch the failure mode that
    # matters here: a reference the script makes and the driver never
    # satisfies.  Bash expands an unset variable to the empty string, so such
    # a job runs to a zero exit status with an argument silently missing.

    @staticmethod
    def _shell_references(text):
        """Every ``${NAME}`` / ``$NAME`` the script depends on."""
        refs = set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-[^}]*)?\}", text))
        refs |= set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", text))
        return refs

    #: Supplied by Slurm itself, never by the workflow.
    SLURM_BUILTINS = {
        "SLURM_JOB_ID", "SLURM_ARRAY_TASK_ID", "SLURM_ARRAY_JOB_ID",
        "SLURM_PROCID", "SLURM_NTASKS", "SLURM_SUBMIT_DIR", "HOME", "USER",
        "PATH", "TMPDIR",
    }

    def _build_parameterized_workflow(self, td):
        """A job whose per-node parameters appear in every field that matters.

        This mirrors what RIFT's own writers do: macros are baked into option
        values, log paths and the working directory, *not* only into the
        var-opt list.
        """
        job = self.m.CondorDAGJob(universe="vanilla", executable="/bin/echo")
        job.set_sub_file(os.path.join(td, "marg.sbatch"))
        job.set_stdout_file(os.path.join(
            td, "iteration_$(macroiteration)_marg/logs",
            "marg-$(macroevent)-$(cluster)-$(process).out"))
        job.set_stderr_file(os.path.join(
            td, "iteration_$(macroiteration)_marg/logs",
            "marg-$(macroevent)-$(cluster)-$(process).err"))
        job.add_opt("sim-grid", os.path.join(td, "grid-$(macroiteration).dat"))
        job.add_opt("output-file", "MARG-$(macroevent)-$(cluster)-$(process).dat")
        job.add_var_opt("event")
        job.add_condor_cmd("initialdir", os.path.join(
            td, "iteration_$(macroiteration)_marg/event_$(macroid)"))
        job.add_condor_cmd("getenv", "True")
        job.write_sub_file()

        node = self.m.CondorDAGNode(job)
        for key, value in (("macroiteration", 0), ("macroevent", 3),
                           ("macroid", 0), ("macrongroup", 3)):
            node.add_macro(key, value)
        dag = self.m.CondorDAG()
        dag.add_node(node)
        dag.set_dag_file(os.path.join(td, "wf.dag"))
        dag.write_concrete_dag()
        with open(os.path.join(td, "marg.sbatch")) as _fh:
            script = _fh.read()
        with open(os.path.join(td, "wf_dag.sh")) as _fh:
            driver = _fh.read()
        return script, driver

    def test_sbatch_leaves_no_unrendered_condor_macro(self):
        with tempfile.TemporaryDirectory() as td:
            script, _ = self._build_parameterized_workflow(td)
        leftovers = sorted(set(re.findall(r"\$\((macro[A-Za-z0-9_]*|cluster|process)\)",
                                          _strip_comments(script))))
        self.assertEqual(leftovers, [], (
            "HTCondor macro syntax survived into an sbatch script; under bash "
            "$(...) is command substitution, not a variable reference: "
            "{}".format(leftovers)))

    def test_every_sbatch_reference_is_supplied(self):
        with tempfile.TemporaryDirectory() as td:
            script, driver = self._build_parameterized_workflow(td)
        exported = set(re.findall(r"(SLURM_VAR_[A-Za-z0-9_]+)=", driver))
        self.assertTrue(exported, "driver exported no per-node variables")
        referenced = self._shell_references(_strip_comments(script))
        unsatisfied = sorted(
            name for name in referenced
            if name.startswith("SLURM_VAR_") and name not in exported)
        self.assertEqual(unsatisfied, [], (
            "sbatch script reads variables the driver never exports; bash "
            "expands these to the empty string rather than failing: "
            "{}\nexported: {}".format(unsatisfied, sorted(exported))))
        stray = sorted(
            name for name in referenced
            if not name.startswith("SLURM_VAR_")
            and name not in self.SLURM_BUILTINS)
        self.assertEqual(stray, [], "unexpected shell references: {}".format(stray))

    def test_job_enters_its_initialdir(self):
        """HTCondor's initialdir is the job's cwd; Slurm has no equivalent.

        RIFT puts per-iteration outputs under it, so a script that does not cd
        writes them where the next stage will not look -- and still exits 0.
        """
        with tempfile.TemporaryDirectory() as td:
            script, _ = self._build_parameterized_workflow(td)
        body = _strip_comments(script)
        self.assertRegex(body, r"(?m)^cd \S*iteration_.*_marg/event_",
                         "sbatch script never enters its initialdir")
        self.assertRegex(body, r"(?m)^mkdir -p \S*iteration_",
                         "sbatch script does not create its initialdir")
        # The cd must precede the payload, or it does nothing useful.
        self.assertLess(body.index("\ncd "), body.index("\nexec "))

    def test_emit_dag_writes_shell_driver(self):
        with tempfile.TemporaryDirectory() as td:
            ja = self.m.CondorDAGJob(executable="/bin/echo")
            ja.set_sub_file(os.path.join(td, "A.sbatch"))
            ja.write_sub_file()
            jb = self.m.CondorDAGJob(executable="/bin/cat")
            jb.set_sub_file(os.path.join(td, "B.sbatch"))
            jb.write_sub_file()
            na = self.m.CondorDAGNode(ja); na.add_macro("event", "7")
            nb = self.m.CondorDAGNode(jb); nb.add_parent(na); nb.set_retry(2)
            dag = self.m.CondorDAG()
            dag.set_environment("RIFT_HYPERPIPELINE_FORMAT", "1")
            dag.add_node(na); dag.add_node(nb)
            dag.set_dag_file(os.path.join(td, "wf.dag"))
            dag.write_concrete_dag()
            # Slurm rewrites .dag → _dag.sh
            driver = os.path.join(td, "wf_dag.sh")
            self.assertTrue(os.path.exists(driver))
            with open(driver) as _fh:
                text = _fh.read()
            self.assertTrue(text.startswith("#!/bin/bash"))
            self.assertIn("set -euo pipefail", text)
            self.assertIn("export RIFT_HYPERPIPELINE_FORMAT=1", text)
            self.assertIn("JOBID_0=$(sbatch", text)
            self.assertIn("--export=ALL,SLURM_VAR_EVENT=7", text)
            self.assertIn("JOBID_1=$(sbatch --dependency=afterok:${JOBID_0}", text)
            self.assertIn("--requeue", text)

    def test_topological_sort_handles_deep_chains(self):
        with tempfile.TemporaryDirectory() as td:
            jobs = []
            nodes = []
            for i in range(5):
                j = self.m.CondorDAGJob(executable="/bin/echo")
                j.set_sub_file(os.path.join(td, "j_{}.sbatch".format(i)))
                j.write_sub_file()
                jobs.append(j)
                n = self.m.CondorDAGNode(j)
                if nodes:
                    n.add_parent(nodes[-1])
                nodes.append(n)
            dag = self.m.CondorDAG()
            for n in reversed(nodes):  # add in reverse to test topo sort
                dag.add_node(n)
            dag.set_dag_file(os.path.join(td, "deep.dag"))
            dag.write_concrete_dag()
            with open(os.path.join(td, "deep_dag.sh")) as _fh:
                text = _fh.read()
            # Each successive sbatch line must depend on the previous JOBID_X
            ids = re.findall(r"JOBID_(\d+)=\$\(sbatch", text)
            self.assertEqual(ids, ["0", "1", "2", "3", "4"])
            for i in range(1, 5):
                expect = "JOBID_{}=$(sbatch --dependency=afterok:${{JOBID_{}}}".format(i, i - 1)
                self.assertIn(expect, text)


class CrossBackendInvarianceTests(unittest.TestCase):
    """Same workflow → three different artefacts.  We don't compare them
    line-by-line (they're expected to differ), but every backend must
    produce a *non-empty* artefact and the artefact must reference the
    same job's executable / queue / dependency structure."""

    def setUp(self):
        self.m = _load_with_stubs()

    def _build_workflow(self, td):
        ja = self.m.CondorDAGJob(executable="/bin/echo")
        ja.set_sub_file(os.path.join(td, "A.sub"))
        ja.add_arg("first")
        ja.add_condor_cmd("request_memory", "1024")
        jb = self.m.CondorDAGJob(executable="/bin/cat")
        jb.set_sub_file(os.path.join(td, "B.sub"))
        jb.add_arg("/etc/hostname")
        jb.add_condor_cmd("request_memory", "2048")
        na = self.m.CondorDAGNode(ja); na.add_macro("idx", "0")
        nb = self.m.CondorDAGNode(jb); nb.add_parent(na); nb.set_retry(1)
        dag = self.m.CondorDAG()
        dag.add_node(na); dag.add_node(nb)
        return ja, jb, dag

    def test_each_backend_produces_artefacts(self):
        for backend in ("htcondor", "glue", "slurm"):
            with self.subTest(backend=backend):
                with tempfile.TemporaryDirectory() as td:
                    self.m.set_backend(backend)
                    ja, jb, dag = self._build_workflow(td)
                    ja.write_sub_file()
                    jb.write_sub_file()
                    dag.set_dag_file(os.path.join(td, "wf.dag"))
                    dag.write_concrete_dag()

                    # Submit files exist and reference the executable
                    with open(os.path.join(td, "A.sub")) as _fh:
                        text_a = _fh.read()
                    self.assertIn("/bin/echo", text_a)
                    with open(os.path.join(td, "B.sub")) as _fh:
                        text_b = _fh.read()
                    self.assertIn("/bin/cat", text_b)

                    # Workflow artefact exists.  HTCondor/glue write to .dag,
                    # slurm rewrites the suffix to _dag.sh.
                    if backend == "slurm":
                        wf_path = os.path.join(td, "wf_dag.sh")
                    else:
                        wf_path = os.path.join(td, "wf.dag")
                    self.assertTrue(os.path.exists(wf_path),
                                    "{}: missing {}".format(backend, wf_path))
                    self.assertGreater(os.path.getsize(wf_path), 0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
