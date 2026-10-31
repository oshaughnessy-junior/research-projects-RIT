"""Terminal DAG job consuming the selected final paired posterior."""
from pathlib import Path
import shlex
import shutil
from .config import enabled, read_ini, resolve


def preflight(config, rundir, *, extrinsic, distance_reweighting=False):
    if not config or not enabled(config):
        return None
    if not extrinsic:
        raise ValueError("auto-fit requires final paired extrinsic posterior export")
    if distance_reweighting:
        raise ValueError("auto-fit with comoving-distance post-reweighting requires a resolved final-prior adapter; use the original posterior standalone")
    cfg, contract, settings = resolve(config, rundir)
    return cfg, contract, settings


def attach(dag, pipeline, config, rundir, parent, posterior, intrinsic_posterior, runtime_prefix=""):
    """The local-universe job uses products already returned to the submit host."""
    run = Path(rundir).resolve()
    source = Path(config).resolve()
    copy = run/"auto_fit.production.ini"
    if copy.exists() and copy.read_bytes() != source.read_bytes():
        raise ValueError("Existing auto-fit INI snapshot differs; preserve the previous workflow")
    if source != copy:
        shutil.copyfile(source, copy)
    cfg = read_ini(copy)
    settings = cfg["rift-auto-fit"]
    executable = shutil.which("util_RIFT_compress_posterior.py")
    if executable is None:
        raise ValueError("util_RIFT_compress_posterior.py is not installed/on PATH")
    wrapper = run/"auto_fit.sh"
    wrapper.write_text("#!/bin/bash\nset -eu\n"+runtime_prefix+"\nexec "+shlex.quote(executable)+
        " --config "+shlex.quote(str(copy))+" --rundir "+shlex.quote(str(run))+
        " --posterior "+shlex.quote(str(posterior))+
        " --intrinsic-posterior "+shlex.quote(str(intrinsic_posterior))+"\n")
    wrapper.chmod(0o755)
    job = pipeline.CondorDAGJob(universe="local", executable=str(wrapper))
    job.set_sub_file(str(run/"AUTO_FIT.sub"))
    job.set_log_file(str(run/"auto_fit.log"))
    job.set_stdout_file(str(run/"auto_fit.out"))
    job.set_stderr_file(str(run/"auto_fit.err"))
    job.add_condor_cmd("initialdir", str(run))
    job.add_condor_cmd("getenv", "True")
    for option, default in (("request_cpus", 1), ("request_memory", 4096), ("request_disk", 2048)):
        key = option+"_mb" if option != "request_cpus" else option
        value = settings.getint(key, fallback=default)
        if value <= 0 or (option == "request_cpus" and value != 1):
            raise ValueError("auto-fit currently requires one CPU and positive memory/disk budgets")
        job.add_condor_cmd(option, str(value))
    seconds = settings.getint("max_runtime_seconds", fallback=1800)
    if seconds <= 0:
        raise ValueError("max_runtime_seconds must be positive")
    job.add_condor_cmd("environment", '"OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1"')
    job.add_condor_cmd("periodic_remove", "JobStatus == 2 && (CurrentTime-JobStartDate) > "+str(seconds))
    job.write_sub_file()
    node = pipeline.CondorDAGNode(job)
    node.set_category("AUTO_FIT")
    node.set_retry(0)
    node.add_parent(parent)
    dag.add_node(node)
    return node
