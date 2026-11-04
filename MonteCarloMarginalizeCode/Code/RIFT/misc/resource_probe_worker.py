#!/usr/bin/env python3
"""Run one bounded ILE group on one GPU and record its memory.  Standard library only.

emit_probes copies this file into a job.  It runs in the job's container, reads
probe.json and writes results/receipt.json, which collect_probes reads.
"""
import ctypes
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

GPU_PROBE = """import cupy as c,json
assert c.cuda.runtime.getDeviceCount()==1,'a probe needs exactly one visible GPU'
p=c.cuda.runtime.getDeviceProperties(0)
pci=c.cuda.runtime.deviceGetPCIBusId(0);pci=pci.decode() if isinstance(pci,bytes) else pci
c.asnumpy(c.fft.fft(c.arange(64,dtype=c.float64)))
print(json.dumps(dict(pci_bus_id=pci,name=p['name'].decode(),sm=10*p['major']+p['minor'],vram_bytes=p['totalGlobalMem'],
 driver=c.cuda.runtime.driverGetVersion(),runtime=c.cuda.runtime.runtimeGetVersion(),cupy=c.__version__)))"""
OOM = re.compile(r"OutOfMemoryError|cudaErrorMemoryAllocation|CUDA_ERROR_OUT_OF_MEMORY|out of memory|"
                 r"CUFFT_ALLOC_FAILED|CUBLAS_STATUS_ALLOC_FAILED", re.I)


class NvmlMemory(ctypes.Structure):
    _fields_ = [("total", ctypes.c_ulonglong), ("free", ctypes.c_ulonglong), ("used", ctypes.c_ulonglong)]


def device_used_mb(bus_id):
    nvml = ctypes.CDLL("libnvidia-ml.so.1")
    if nvml.nvmlInit_v2():
        raise OSError("nvmlInit failed")
    handle = ctypes.c_void_p()
    if nvml.nvmlDeviceGetHandleByPciBusId_v2(bus_id.encode(), ctypes.byref(handle)):
        raise OSError("no NVML handle for " + bus_id)
    memory = NvmlMemory()
    if nvml.nvmlDeviceGetMemoryInfo(handle, ctypes.byref(memory)):
        raise OSError("nvmlDeviceGetMemoryInfo failed")
    return memory.used / 1048576.0


def set_option(arguments, key, value):
    out, i = [], 0
    while i < len(arguments):
        if arguments[i] == key:
            i += 2
        else:
            out.append(arguments[i])
            i += 1
    return out + [key, str(value)]


def monitor(child, bus_id, sampled, stop, series):
    while not stop.is_set():
        rss = used = None
        try:
            status = Path("/proc/%d/status" % child.pid).read_text()
            peak = re.search(r"^VmHWM:\s+(\d+)", status, re.M)
            if peak:
                sampled["host_peak_rss_kb"] = max(sampled["host_peak_rss_kb"], int(peak.group(1)))
            now = re.search(r"^VmRSS:\s+(\d+)", status, re.M)
            rss = int(now.group(1)) if now else None
        except OSError:
            pass
        try:
            used = device_used_mb(bus_id)
            sampled["gpu_peak_used_mb"] = max(sampled["gpu_peak_used_mb"], used)
        except OSError as error:
            sampled["gpu_sampler_error"] = str(error)
        series.write(json.dumps({"t": time.time(), "rss_kb": rss, "gpu_mb": used}) + "\n")
        series.flush()
        stop.wait(5)


def read_points(directory):
    """Finished points: ILE writes one .dat per point, ending ... lnL sigma ntotal neff."""
    points, finite = [], True
    for path in directory.glob("CME*_.dat"):      # not the _cal.dat companions
        for line in path.read_text().splitlines():
            try:
                values = [float(x) for x in line.split()]
            except ValueError:
                continue
            if len(values) >= 13:
                finite = finite and all(math.isfinite(x) for x in values)
                points.append({"file": path.name, "mtime": path.stat().st_mtime, "draws": values[-2]})
    return sorted(points, key=lambda p: p["mtime"]), finite


def find_gpu():
    """(device record, error text) from a child process, so this one holds no CUDA context."""
    found = subprocess.run([sys.executable, "-c", GPU_PROBE], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True)
    if found.returncode:
        return None, found.stderr[-2000:]
    return json.loads(found.stdout.strip().splitlines()[-1]), None


def main():
    root = Path.cwd()
    results = root / "results"
    results.mkdir(exist_ok=True)
    probe = json.loads((root / "probe.json").read_text())
    receipt = {"started": time.time(), "probe": probe}

    def save():
        (results / "receipt.json").write_text(json.dumps(receipt, indent=1) + "\n")

    save()
    os.environ.pop("PYTHONPATH", None)
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ[name] = "1"
    assigned = os.environ.get("_CONDOR_AssignedGPUs", "")
    if assigned.startswith("GPU-") and "," not in assigned:
        os.environ["CUDA_VISIBLE_DEVICES"] = assigned
    os.environ["RIFT_ILE_GPU_FANOUT"] = "1"
    os.environ["PYTHONFAULTHANDLER"] = "1"
    os.environ["CUPY_CACHE_DIR"] = str(root / "cupy-cache")
    os.environ["CUDA_CACHE_PATH"] = str(root / "cuda-cache")
    gpu, error = find_gpu()
    if error:
        receipt["gpu_probe_error"] = error
        save()
        return 3
    receipt["gpu"] = gpu
    save()

    arguments = list(probe["arguments"])
    directory = results / "ile"
    directory.mkdir()
    for key, value in (("--n-chunk", probe["n chunk"]), ("--n-eff", probe["n eff"]), ("--n-max", probe["n max"]),
                       ("--event", probe["event"]), ("--n-events-to-analyze", probe["points"]),
                       ("--output-file", directory / "CME.xml")):
        if value is not None:
            arguments = set_option(arguments, key, value)
    executable = shutil.which(probe["ile"]) or str(Path(sys.executable).parent / probe["ile"])
    sampled = {"host_peak_rss_kb": 0, "gpu_peak_used_mb": 0}
    start = time.monotonic()
    stop = threading.Event()
    with (directory / "stdout.log").open("w") as out, (directory / "stderr.log").open("w") as err, \
            (directory / "memory.jsonl").open("w") as series:
        child = subprocess.Popen([sys.executable, executable] + arguments, cwd=root / "case", stdout=out, stderr=err)
        thread = threading.Thread(target=monitor, args=(child, gpu["pci_bus_id"], sampled, stop, series), daemon=True)
        thread.start()
        try:
            code = child.wait(timeout=probe["timeout"])
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
            code = 124
        finally:
            stop.set()
            thread.join(timeout=8)
    points, finite = read_points(directory)
    text = (directory / "stderr.log").read_text()[-12000:] + (directory / "stdout.log").read_text()[-5000:]
    receipt["run"] = {"arguments": arguments, "returncode": code, "wall_seconds": time.monotonic() - start,
                      "points": points, "all_finite": finite, "gpu_oom": bool(OOM.search(text)),
                      "sampled_memory": sampled}
    receipt["finished"] = time.time()
    save()
    return 0 if code == 0 and finite and len(points) == probe["points"] else 4


if __name__ == "__main__":
    sys.exit(main())
