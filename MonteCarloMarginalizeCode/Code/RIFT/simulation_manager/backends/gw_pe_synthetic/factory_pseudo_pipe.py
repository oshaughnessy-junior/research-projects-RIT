"""Real `subdag_factory` for the GW PE synthetic-targeted backend.

Produces a per-(sim, level) sub-DAG by:

    1. Rendering a synthetic coinc.xml from the archive's stored params
    2. Staging frames: `gwpy`-generated Gaussian noise OR user-supplied
       pre-existing noise frames; signal generated via lalsimutils.hoft
       and added in-memory using gwpy. We do NOT shell out to
       `test/pp/add_frames.py` — that script lives outside PATH and
       round-trips through pycbc, which is fragile on synthetic frames
       (raises "invalid time" on epoch-rounding mismatches). Doing the
       sum natively in gwpy avoids both issues.
    3. Staging a PSD file (canonical aLIGO design or user-supplied)
    4. Rendering an ini, with level-dependent knobs (more iterations /
       higher n-eff target as level rises)
    5. Invoking `util_RIFT_pseudo_pipe.py --use-ini ... --use-coinc ...
       --use-rundir ... --use-online-psd-file ...`
    6. Returning the path of the .dag pseudo_pipe wrote

Imports are deferred so this module is importable without
lalsuite/gwpy; the deferred imports raise at call time on hosts
without those packages. This lets the archive be opened (and its
metadata inspected) on any submit node, even one that won't run PE.

Per BACKENDS.md: this never re-implements PE pipeline construction.
It only OWNS frame writing + ini/coinc rendering + the shell-out to
pseudo_pipe.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def subdag_factory(archive, sim_name: str, level: int) -> str:
    """Build the per-(sim, level) PE sub-DAG and return its path.

    Imports lalsuite and gwpy lazily; raises ImportError with a
    helpful message if they're not installed."""
    try:
        import RIFT.lalsimutils as lalsimutils  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "factory_pseudo_pipe requires lalsuite (RIFT.lalsimutils). "
            "Install lalsuite + the rest of the RIFT requirements, or use "
            "factory_stub.subdag_factory for development."
        ) from exc

    sd = archive.sim_dir(sim_name)
    rundir = sd / "level_{}".format(level)
    rundir.mkdir(parents=True, exist_ok=True)

    # Pull params from the archive's status record.
    from RIFT.simulation_manager.database import StatusRecord
    rec = StatusRecord.read(sd)
    params: Dict[str, Any] = rec.data.get("params") or {}

    # 1. Render coinc.xml.
    coinc_path = _render_coinc_xml(params, rundir / "coinc.xml")

    # 2. Stage frames (gwpy noise OR user-provided + injection).
    cache_path = _stage_frames(params, rundir / "frames", rundir / "data.cache")

    # 3. Stage PSD.
    psd_path = _stage_psd(params, rundir / "psd.xml.gz")

    # 4. Render ini, with level-dependent knobs.
    ini_path = _render_ini(params, level, rundir / "config.ini")

    # 5. Invoke pseudo_pipe.
    cmd = ["util_RIFT_pseudo_pipe.py",
           "--use-ini",              str(ini_path),
           "--use-coinc",            str(coinc_path),
           "--use-rundir",           str(rundir),
           "--use-online-psd-file",  str(psd_path)]
    extra = params.get("pseudo_pipe_extra_args") or []
    cmd.extend(extra)
    logger.info("running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(rundir))

    # 6. Locate the .dag pseudo_pipe wrote. Convention: it lives in the
    # rundir; pseudo_pipe writes a single top-level .dag plus several
    # iteration sub-DAGs. We return the top-level one.
    dag_paths = sorted(rundir.glob("*.dag"))
    if not dag_paths:
        raise RuntimeError(
            "util_RIFT_pseudo_pipe.py did not produce a .dag in {}".format(rundir))
    # Heuristic: pick the shallowest one (the one DAGman drives).
    top_dag = min(dag_paths, key=lambda p: len(p.parents))
    return str(top_dag)


# ---------------------------------------------------------------------------
# Helpers (deferred imports of lalsuite / gwpy / igwn-ligolw)
# ---------------------------------------------------------------------------

def _render_coinc_xml(params: Dict[str, Any], out_path: Path) -> Path:
    """Build a one-row sim_inspiral table representing the synthetic
    event and write it as a coinc.xml file consumable by pseudo_pipe.
    Modeled on `demo/populations/write_mdc.py`."""
    import RIFT.lalsimutils as lalsimutils
    import lal
    P = lalsimutils.ChooseWaveformParams()
    P.m1 = float(_mass1(params)) * lal.MSUN_SI
    P.m2 = float(_mass2(params)) * lal.MSUN_SI
    P.dist = float(params.get("distance", 100.0)) * 1e6 * lal.PC_SI
    P.theta = float(params.get("dec", 0.0))
    P.phi = float(params.get("ra", 0.0))
    P.psi = float(params.get("polarization", 0.0))
    P.incl = float(params.get("inclination", 0.0))
    P.tref = float(params.get("geocent_time", 0.0))
    P.s1z = float(params.get("s1z", 0.0))
    P.s2z = float(params.get("s2z", 0.0))
    if "eccentricity" in params:
        P.eccentricity = float(params["eccentricity"])
    if "approximant" in params:
        import lalsimulation as lalsim
        P.approx = lalsim.GetApproximantFromString(str(params["approximant"]))
    lalsimutils.ChooseWaveformParams_array_to_xml([P], str(out_path).removesuffix(".xml"))
    # ChooseWaveformParams_array_to_xml writes <prefix>.xml.gz; rename
    # to plain .xml if needed for pseudo_pipe.
    written = Path(str(out_path).removesuffix(".xml") + ".xml.gz")
    if written.exists() and not out_path.exists():
        shutil.move(str(written), str(out_path))
    return out_path


def _stage_frames(params: Dict[str, Any], frames_dir: Path,
                  cache_path: Path) -> Path:
    """Stage one combined-noise+signal `.gwf` per IFO, write a `.cache`
    file pointing at them, and return the cache path. All frame I/O
    goes through gwpy; we never shell out to `add_frames.py`."""
    frames_dir.mkdir(parents=True, exist_ok=True)
    ifos = params.get("ifos") or ["H1", "L1", "V1"]
    channel_per_ifo = params.get("channel") or {ifo: "FAKE-STRAIN" for ifo in ifos}
    seglen = float(params.get("seglen", 8.0))
    srate = int(params.get("srate", 4096))
    geocent_time = float(params.get("geocent_time", 0.0))

    # GPS window centered on the event. Integer-aligned to keep frame
    # epochs on the second so downstream readers don't choke on
    # fractional-epoch frames.
    start = int(geocent_time - seglen + 2)
    duration = int(seglen)

    cache_lines: List[str] = []
    for ifo in ifos:
        channel = channel_per_ifo[ifo]
        frame_path = frames_dir / "{}-COMBINED-{}-{}.gwf".format(ifo, start, duration)
        _stage_combined_frame_for_ifo(
            ifo=ifo, channel=channel, params=params,
            frame_path=frame_path,
            srate=srate, start=start, duration=duration)
        cache_lines.append("{ifo} {tag} {start} {dur} file://{path}".format(
            ifo=ifo[0], tag=ifo, start=start, dur=duration, path=frame_path))

    cache_path.write_text("\n".join(cache_lines) + "\n")
    return cache_path


def _stage_combined_frame_for_ifo(*, ifo: str, channel: str,
                                  params: Dict[str, Any],
                                  frame_path: Path,
                                  srate: int, start: int, duration: int) -> None:
    """Build a single `.gwf` for `ifo` containing noise + injected signal,
    using gwpy throughout. No pycbc, no `add_frames.py`, no intermediate
    files.

    Steps:
      1. Build the noise gwpy.TimeSeries (gwpy-generated Gaussian, OR
         a slice of a user-supplied frame).
      2. Generate the IFO-projected signal via `RIFT.lalsimutils.hoft`
         and convert to a gwpy.TimeSeries (matched sample rate).
      3. Add signal into the noise array at the correct sample-index
         offset — the signal occupies only a small window inside
         `[start, start+duration]`; samples outside that window stay
         as pure noise.
      4. Write the combined TimeSeries as a single .gwf.
    """
    try:
        from gwpy.timeseries import TimeSeries
    except ImportError as exc:
        raise ImportError(
            "factory_pseudo_pipe requires gwpy for frame staging. "
            "Install gwpy or skip this factory."
        ) from exc
    import numpy as np

    # ---- 1. Noise --------------------------------------------------------
    noise_source = params.get("noise_source", "gwpy")
    if noise_source == "user":
        user_frames = params.get("user_noise_frames") or {}
        src = user_frames.get(ifo)
        if not src:
            raise ValueError(
                "noise_source='user' but no user_noise_frames[{}]".format(ifo))
        try:
            full = TimeSeries.read(src, channel=channel)
        except Exception as exc:
            raise RuntimeError(
                "Failed to read user noise frame {!r}: {}".format(src, exc)) from exc
        try:
            noise = full.crop(start, start + duration)
        except Exception as exc:
            raise RuntimeError(
                "User noise frame {!r} does not cover [{}, {}]: {}".format(
                    src, start, start + duration, exc)) from exc
    else:
        n = duration * srate
        rng = np.random.default_rng(
            seed=hash((ifo, channel, start)) & 0xffffffff)
        # Scale roughly to design-aLIGO strain RMS so the synthetic SNR
        # is in a sensible range; exact PSD shaping is deferred to a
        # follow-up (lalsimulation.SimNoisePSDaLIGO*).
        data = rng.standard_normal(n).astype(np.float64) * 1e-23
        noise = TimeSeries(data, sample_rate=srate, t0=start,
                           name=channel, channel="{}:{}".format(ifo, channel))

    # ---- 2. Signal -------------------------------------------------------
    import RIFT.lalsimutils as lalsimutils
    import lal
    P = lalsimutils.ChooseWaveformParams()
    P.m1 = float(_mass1(params)) * lal.MSUN_SI
    P.m2 = float(_mass2(params)) * lal.MSUN_SI
    P.dist = float(params.get("distance", 100.0)) * 1e6 * lal.PC_SI
    P.theta = float(params.get("dec", 0.0))
    P.phi = float(params.get("ra", 0.0))
    P.psi = float(params.get("polarization", 0.0))
    P.incl = float(params.get("inclination", 0.0))
    P.tref = float(params.get("geocent_time", 0.0))
    P.s1z = float(params.get("s1z", 0.0))
    P.s2z = float(params.get("s2z", 0.0))
    P.detector = ifo
    P.deltaT = 1.0 / srate
    if "approximant" in params:
        import lalsimulation as lalsim
        P.approx = lalsim.GetApproximantFromString(str(params["approximant"]))
    htoft = lalsimutils.hoft(P)
    sig_array = np.asarray(htoft.data.data, dtype=np.float64)
    sig_dt = float(htoft.deltaT)
    sig_t0 = float(htoft.epoch)   # GPS seconds

    # Sanity: sample rates must agree. If they don't (lalsimutils may
    # adjust deltaT in some edge cases), resample the signal to the
    # noise grid.
    noise_dt = float(noise.dt.value)
    if abs(sig_dt - noise_dt) > 1e-9 * noise_dt:
        from gwpy.timeseries import TimeSeries as _TS
        sig_ts = _TS(sig_array, sample_rate=1.0 / sig_dt, t0=sig_t0,
                     name=channel, channel="{}:{}".format(ifo, channel))
        sig_ts = sig_ts.resample(noise.sample_rate.value)
        sig_array = np.asarray(sig_ts.value, dtype=np.float64)
        sig_t0 = float(sig_ts.t0.value)
        sig_dt = float(sig_ts.dt.value)

    # ---- 3. In-memory addition ------------------------------------------
    combined_arr = np.array(noise.value, copy=True)
    noise_t0 = float(noise.t0.value)
    i0 = int(round((sig_t0 - noise_t0) / noise_dt))
    i_start = max(0, i0)
    i_end = min(len(combined_arr), i0 + len(sig_array))
    if i_end > i_start:
        s_start = i_start - i0
        s_end = s_start + (i_end - i_start)
        combined_arr[i_start:i_end] += sig_array[s_start:s_end]
    else:
        logger.warning("signal for %s falls entirely outside the noise "
                       "window [%s, %s); writing pure noise frame",
                       ifo, start, start + duration)

    # ---- 4. Write combined frame ----------------------------------------
    combined = TimeSeries(combined_arr, sample_rate=noise.sample_rate,
                          t0=noise.t0, name=channel,
                          channel="{}:{}".format(ifo, channel))
    combined.write(str(frame_path), format="gwf")


def _stage_psd(params: Dict[str, Any], out_path: Path) -> Path:
    """Either copy a user-supplied psd.xml.gz or generate the canonical
    aLIGO design PSD. The latter follows the helper script in test/pp/."""
    psd_source = params.get("psd_source", "design_aligo")
    if psd_source == "user":
        src = params.get("user_psd_path")
        if not src:
            raise ValueError("psd_source='user' but no user_psd_path")
        shutil.copy(src, out_path)
        return out_path
    # design_aligo path. Generate a standard PSD via lalsimulation +
    # ligolw. For the prototype we shell out to gen_psd if available;
    # otherwise we leave a placeholder and pseudo_pipe falls back to
    # its own internal default.
    if shutil.which("util_WriteFakePSD.py"):
        cmd = ["util_WriteFakePSD.py",
               "--ifo", "H1", "--ifo", "L1", "--ifo", "V1",
               "--inj-snr", "20.0",
               "--out-psd-xml", str(out_path)]
        subprocess.run(cmd, check=True)
        return out_path
    logger.warning("util_WriteFakePSD.py not found; pseudo_pipe will fall back "
                   "to its built-in PSD. Write a psd.xml.gz manually for "
                   "more control.")
    out_path.write_text("")  # empty placeholder
    return out_path


def _render_ini(params: Dict[str, Any], level: int, out_path: Path) -> Path:
    """Render the ini RIFT will read. Per-level scaling: more iterations
    and higher n-eff target as level rises. Falls back to a base ini if
    `params` carries `base_ini_path`."""
    import configparser
    cfg = configparser.ConfigParser()
    base_ini = params.get("base_ini_path")
    if base_ini:
        cfg.read(base_ini)
    else:
        # Minimal default. Real users almost always pass `base_ini_path`
        # to inherit their site/instrument configuration.
        cfg["analysis"] = {
            "ifos":            "['H1','L1','V1']",
            "engine":          "rift",
        }
        cfg["lalinference"] = {"flow": "20"}
    if "rift-pseudo-pipe" not in cfg:
        cfg["rift-pseudo-pipe"] = {}
    iterations = max(2, 2 * int(level))
    n_eff = int(500 * level ** 2)
    cfg["rift-pseudo-pipe"]["internal-iterations"] = str(iterations)
    cfg["rift-pseudo-pipe"]["n-eff"] = str(n_eff)
    with open(out_path, "w") as f:
        cfg.write(f)
    return out_path


def _mass1(params):
    """If user supplies mc/eta, return m1; if user supplies m1/m2 directly,
    pass through. Mass-ratio convention: m1 >= m2."""
    if "m1" in params:
        return float(params["m1"])
    mc = float(params["mc"])
    eta = float(params["eta"])
    mtot = mc / (eta ** 0.6)
    return 0.5 * mtot * (1.0 + (1.0 - 4.0 * eta) ** 0.5)


def _mass2(params):
    if "m2" in params:
        return float(params["m2"])
    mc = float(params["mc"])
    eta = float(params["eta"])
    mtot = mc / (eta ** 0.6)
    return 0.5 * mtot * (1.0 - (1.0 - 4.0 * eta) ** 0.5)
