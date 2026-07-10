#!/usr/bin/env python
r"""make_event_ini.py -- emit a util_RIFT_pseudo_pipe.py ini for one event + one config.

Single source of truth = the per-event event.env (shell KEY=VALUE, also sourced by the
fetch/build scripts). Produces the four paper-2 configurations on IDENTICAL data (Table 1 of
paper2.tex): they differ ONLY in the glitch model, so the comparison is clean.

  U   unmitigated              td-toeplitz, NO glitch basis.  (For a full, un-truncated segment
                               the TD Toeplitz kernel == the classic fd-diagonal likelihood to
                               1e-13 -- the paper-1 T4 gate -- so U is the standard analysis; we
                               run it on the td engine so U/MF/MP share an identical code path
                               and the only difference is the glitch model.)
  MF  marginalized, flat       td-toeplitz + --glitch-basis-file <flat npz> --glitch-sigma profile
                               (paper-1 default: basis/lambda0/t0/fs only, no population keys).
  MP  marginalized, population  td-toeplitz + --glitch-basis-file <pop npz> (adds mu/sigma_shape).
  D   catalog-deglitched       classic fd-diagonal on the public BayesWave-subtracted frames
                               (different cache + channel; no glitch model). Only where a public
                               deglitched frame set exists (GW191109, GW200129: Zenodo 5546679).

The glitch-basis .npz auto-rides the condor transfer list and the ILE flag is basename-rewritten
on OSG (util_RIFT_pseudo_pipe.py:1304-1335). Window is inherited from the pipeline
(--data-tukey-window-time default -> --window-shape) -- never bypassed.

Usage:
  make_event_ini.py --env event.env --config MP \
      --basis-flat work/glitch_basis_L1.npz --basis-pop work/glitch_basis_L1_pop_full.npz \
      --out ini/GW250114_MP.ini
"""
import argparse
import os


def load_env(path):
    env = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.split(" #")[0].strip().strip('"').strip("'")
            env[k.strip()] = v
    return env


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, help="event.env")
    ap.add_argument("--config", required=True, choices=["U", "MF", "MP", "D"])
    ap.add_argument("--basis-flat", default=None, help="flat basis npz (MF)")
    ap.add_argument("--basis-pop", default=None, help="population basis npz (MP)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    e = load_env(args.env)

    ifos = [x.strip() for x in e["IFOS"].split(",") if x.strip()]
    srate = e.get("SRATE", "2048")
    seglen = e.get("ILE_SEGLEN", "8")
    fmin = e.get("FMIN", "20")
    fmax = e.get("FMAX", "1000")
    fref = e.get("FREF", "20")
    approx = e.get("APPROX", "IMRPhenomXPHM")
    lmax = e.get("LMAX", "4")
    dmax = e.get("DMAX", "10000")
    gps = e["GPS"]
    win_time = e.get("DATA_TUKEY_WINDOW_TIME", "0.4")
    assume_nospin = e.get("ASSUME_NOSPIN", "False")

    # data channel per config
    if args.config == "D":
        chan_suffix = e.get("DEGLITCH_CHANNEL", "DEGLITCHED-STRAIN")
        if not e.get("DEGLITCH_CACHE", "").strip():
            raise SystemExit("config D requested but DEGLITCH_CACHE is empty in %s "
                             "(no public deglitched frames for this event)" % args.env)
    else:
        chan_suffix = e.get("CHANNEL_SUFFIX", "GWOSC-STRAIN")
    channels = ",".join("'%s': '%s:%s'" % (ifo, ifo, chan_suffix) for ifo in ifos)
    flow = ", ".join("'%s': %s" % (ifo, fmin) for ifo in ifos)
    fhigh = ", ".join("'%s': %s" % (ifo, fmax) for ifo in ifos)

    lines = []
    lines += ["[analysis]",
              "ifos=[%s]" % ",".join("'%s'" % i for i in ifos),
              "singularity=False", "osg=False", "",
              "[condor]",
              "accounting_group=%s" % e.get("ACCOUNTING_GROUP", "ligo.dev.o4.cbc.pe.rift"),
              "accounting_group_user=%s" % e.get("ACCOUNTING_USER", os.environ.get("USER", "albert.einstein")),
              "",
              "[data]",
              "channels = {%s}" % channels, "",
              "[lalinference]",
              "flow = {%s}" % flow,
              "fhigh = {%s}" % fhigh, "",
              "[engine]",
              "fref=%s" % fref,
              "approx=%s" % approx,
              "amporder = -1",
              "seglen = %s" % seglen,
              "srate = %s" % srate,
              "distance-max = %s" % dmax,
              "comp-min = %s" % e.get("COMP_MIN", "1"),
              "comp-max = %s" % e.get("COMP_MAX", "1000"), "",
              "[rift-pseudo-pipe]",
              'approx="%s"' % approx,
              "l-max=%s" % lmax,
              "fmin-template=%s" % fmin,
              "event-time=%s" % gps,
              "assume-nospin=%s" % assume_nospin,
              "internal-ile-data-tukey-window-time=%s" % win_time,
              "",
              "# distance marginalization (production)",
              "internal-marginalize-distance=True",
              "internal-distance-max=%s" % dmax,
              "",
              "# sampler / CIP knobs",
              'cip-fit-method="rf"',
              'cip-sampler-method="GMM"',
              'ile-sampler-method="%s"' % e.get("ILE_SAMPLER", "adaptive_cartesian_gpu"),
              "ile-n-eff=%s" % e.get("ILE_NEFF", "10"),
              "internal-ile-n-max=%s" % e.get("ILE_NMAX", "4000000"),
              "force-initial-grid-size=%s" % e.get("GRID_SIZE", "500"),
              "use-online-psd=False",
              "n-output-samples=%s" % e.get("N_OUTPUT_SAMPLES", "5000")]
    if e.get("FORCE_ETA_RANGE", "").strip():
        lines += ['force-eta-range="%s"' % e["FORCE_ETA_RANGE"]]

    if args.config in ("MF", "MP"):
        gifo = e["GLITCH_IFO"]
        basis = args.basis_flat if args.config == "MF" else args.basis_pop
        if not basis:
            raise SystemExit("config %s needs --basis-%s"
                             % (args.config, "flat" if args.config == "MF" else "pop"))
        basis = os.path.abspath(basis)
        lines += ["",
                  "# ---- TIME-DOMAIN + GLITCH MARGINALIZATION (%s) ----" % args.config,
                  'internal-ile-inner-product="td-toeplitz"',
                  'internal-ile-td-solver="auto"',
                  'internal-ile-glitch-basis-file="%s:%s"' % (gifo, basis),
                  'internal-ile-glitch-sigma="profile"']
        if args.config == "MP":
            # paper-2 Sec. 3.2 detection-conditioned prior.  The OPERATING RECOMMENDATION for
            # triggered events is the exact two-component sign mixture (signmix); the M2
            # moment-matched Gaussian is its cheap zero-mean stand-in -- flip GLITCH_PRIOR_MODE
            # to m2 in event.env to switch.  Presence conditioning: GLITCH_SIGMA_MIN floors
            # sigma_g (a detected glitch is not small), a per-event knob (default unset = code
            # default bounds).
            prior_mode = e.get("GLITCH_PRIOR_MODE", "signmix")
            lines += ['internal-ile-glitch-prior-mode="%s"' % prior_mode]
            if e.get("GLITCH_SIGMA_MIN", "").strip():
                lines += ["internal-ile-glitch-sigma-min=%s" % e["GLITCH_SIGMA_MIN"].strip()]
    elif args.config == "U":
        lines += ["",
                  "# ---- UNMITIGATED baseline (td engine, no glitch model) ----",
                  'internal-ile-inner-product="td-toeplitz"',
                  'internal-ile-td-solver="auto"']
    elif args.config == "D":
        lines += ["",
                  "# ---- CATALOG-DEGLITCHED comparison (classic fd-diagonal) ----",
                  "# analyze the public BayesWave-subtracted frames (%s)"
                  % e.get("DEGLITCH_SOURCE", "Zenodo 5546679"),
                  "# DEGLITCH_CACHE=%s" % e.get("DEGLITCH_CACHE", "")]

    with open(args.out, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print("[ini] wrote %s (config %s)" % (args.out, args.config))


if __name__ == "__main__":
    main()
