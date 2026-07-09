# td_paper2_runs — per-event production setup for the paper-2 glitch-marginalization demos

Launch-ready RIFT workspaces for the paper-2 real-data glitch-marginalization demonstrations
(NEXT.md "Paper 2"; branch `tdlike_paper2`, local only). Each event gets its own directory with
a `fetch_data.sh`, `build_basis.sh`, `Makefile`, and `README.md`; shared tooling lives in
`common/`. This follows the roboto-paper production culture (one workspace per event; Makefile
`build`/`validate`/`submit` targets; regenerable data via scripts, not commits).

**LOCAL ONLY. Never `git push`. `make submit` is GUARDED — it prints the `condor_submit_dag`
line, it does not submit. Submission is a deliberate later step with the run owner.**

## Events and the run matrix (paper2.tex Table 1)

| dir | event | glitch (IFO, class) | configs | job |
|---|---|---|---|---|
| `GW250114/` | GW250114_082203 | L1, weak low-freq (Blip basis reuse) | U MF MP | antiglitch h2h (loudest); paper-1 infra reuse |
| `GW191109/` | GW191109_010717 | L1(+H1), slow scattered light | U MF MP D | GPBilby h2h + spin claim |
| `GW200129/` | GW200129_065458 | L1, scattered light 20-50 Hz | U MF MP D | subtraction schools h2h |
| `EV4/`      | GW230731_215307 | H1, weak/inferred (f_low-raise only) | U MF MP | the weak-glitch robustness objective |

Configurations (identical data, differing ONLY in the glitch model):
- **U** unmitigated — td-toeplitz, no glitch basis (== classic fd-diagonal to 1e-13, the T4 gate).
- **MF** marginalized, flat prior — `--glitch-basis-file <flat npz>` (paper-1 default).
- **MP** marginalized, population prior — `--glitch-basis-file <pop npz>` (mu/sigma_shape; §3).
- **D** catalog-deglitched — classic fd on the public BayesWave-subtracted frames (GW191109 /
  GW200129 only; Zenodo 5546679). Guarded `make build-D`, needs `DEGLITCH_CACHE` in `event.env`.

EV-4 identity: **GW230731_215307**, the weakest defensible case from GWTC-4.0 DQ Table 8
(arXiv:2508.18082) — mitigated by an f_low raise 20->40 Hz in H1 alone, no BayesWave. Residual
uncertainty (no published Gravity Spy class / TF window; O4 corpus not in Zenodo 5649212) is
documented in `EV4/event.env` and `EV4/README.md`.

## Per-event workflow

```sh
cd GW250114
make fetch      # open strain + off-source median-Welch PSDs (+ combined PSD xml) + coinc  (network)
make basis      # per-class corpus -> PPCA -> strain basis colored to the REAL event PSD + pop prior
make build      # generate a condor DAG per config via util_RIFT_pseudo_pipe.py  (NO submit)
make validate   # run ONE ILE point extracted from the generated MP ILE.sub  (go/no-go)
make submit     # GUARDED: print the condor_submit_dag lines (does NOT submit)
```

Everything derives from the per-event `event.env` (single source of truth; sourced by the shell
scripts and parsed by `common/make_event_ini.py`). Approximant is IMRPhenomXPHM (precessing,
l-max 4) to match the catalog + GPBilby comparisons; seglen/srate/fmin are per-event.

## `common/` tooling

| script | role |
|---|---|
| `fetch_event_data.py` | open strain + off-source median-Welch PSD + analysis frames + cache (T7c recipe, event-parameterized) |
| `fetch_class.py`      | per-CLASS glitch corpus from Gravity Spy (Zenodo 5649212) — generalizes `td_glitch_poc/fetch_blips.py` to any class/IFO/window (Blip or Scattered_Light) |
| `color_basis.py`      | color a whitened PCA basis to strain at the REAL event PSD + place at the glitch time (generalizes `make_scene.py` coloring) |
| `merge_psd_xml.py`    | combine per-IFO PSD XMLs into one multi-instrument file (so a single `--use-online-psd-file` carries true per-IFO PSDs) |
| `make_coinc.py`       | synthesize `sim.xml` + `coinc.xml` for a real event from catalog masses/time/SNR |
| `make_event_ini.py`   | emit a pseudo_pipe ini for one event + one config (U/MF/MP/D) |
| `run_ile_from_sub.py` | extract + run ONE ILE point from a generated condor `ILE.sub` (the `validate` smoke) |

The PCA (`build_pca.py`) and population fit (`fit_population.py`) are reused as-is from
`../td_glitch_poc/` (paper-1 tooling).

## Container

The ILE + glitch path is self-contained on stock RIFT (numpy/scipy; `tdanalysis` only for the
optional `--td-solver pcg` branch). Build the 2-image CIT family from the kit at
`~/rift_cit_build_container_family/tdlike_paper2/` on a CIT build host, copy the `.sif` to OSDF
staging, and `export SINGULARITY_RIFT_IMAGE=<...tdlike_paper2.cit.yaml>` before submitting on OSG
(deployer step; see `analyses/paper2_wiring/REPORT.md`).

## Committed vs regenerable

Committed: scripts, `event.env`, Makefiles, READMEs, and the small basis `.npz` (<1 MB each, under
each event's `basis/`). Regenerable + gitignored: `data/`, `psd/`, `work/`, `runs/`, `ini/`,
`*.log`, `sim.xml.gz`, `coinc.xml`.
