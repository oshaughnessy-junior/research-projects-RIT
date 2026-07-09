# GW191109_010717 -- paper-2 glitch-marg production-run setup (U/MF/MP + D).
# Targets: fetch -> basis -> build (DAG per config, NO submit) -> validate -> submit (GUARDED).
# LOCAL ONLY. Never `git push`. Submission is a deliberate later step with the run owner.
# NOTE: MF/MP need a SCATTERED_LIGHT corpus (build_basis.sh fetches it from Gravity Spy L1 O3b;
# ~216 MB CSV + GWOSC snippet fetches -- heavier than a blip corpus).  D needs the public
# BayesWave-deglitched frames (Zenodo 5546679); set DEGLITCH_CACHE in event.env first.
include event.env

HERE    := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
CODE    := $(abspath $(HERE)/../../..)
COMMON  := $(HERE)/../common
PY      ?= $(HOME)/.conda/envs/junior_rift/bin/python
PSEUDO  := $(PY) $(CODE)/bin/util_RIFT_pseudo_pipe.py
export PYTHONPATH := $(CODE):$(PYTHONPATH)
export PATH := $(dir $(PY)):$(CODE)/bin:$(PATH)

TAG_    := $(strip $(TAG))
GIFO    := $(strip $(GLITCH_IFO))
CONFIGS := U MF MP        # D is a separate target (needs downloaded deglitched frames)
FLAT_BASIS := $(HERE)/basis/glitch_basis_$(GIFO).npz
POP_BASIS  := $(HERE)/basis/glitch_basis_$(GIFO)_pop.npz
CONTAINER_KIT := $(HOME)/rift_cit_build_container_family/tdlike_paper2

CACHE   := $(HERE)/data/frames.cache
PSDCOMB := $(HERE)/psd/combined-psd.xml.gz
COINC   := $(HERE)/coinc.xml
VALIDATE_NMAX ?= 40000
VALIDATE_NEFF ?= 10

.PHONY: help fetch basis build build-D validate submit clean inis
help:
	@echo "GW191109 paper-2 setup.  Configs: $(CONFIGS) (+ D, guarded)"
	@echo "  make fetch / basis / build / validate / submit / clean  (see td_paper2_runs/README.md)"
	@echo "  make build-D  requires DEGLITCH_CACHE set in event.env (Zenodo 5546679 frames)"

fetch: $(CACHE)
$(CACHE):
	bash $(HERE)/fetch_data.sh

basis: $(POP_BASIS)
$(POP_BASIS): $(CACHE)
	bash $(HERE)/build_basis.sh

inis: basis
	@mkdir -p $(HERE)/ini
	$(PY) $(COMMON)/make_event_ini.py --env event.env --config U  --out $(HERE)/ini/$(TAG_)_U.ini
	$(PY) $(COMMON)/make_event_ini.py --env event.env --config MF --basis-flat $(FLAT_BASIS) --out $(HERE)/ini/$(TAG_)_MF.ini
	$(PY) $(COMMON)/make_event_ini.py --env event.env --config MP --basis-pop  $(POP_BASIS)  --out $(HERE)/ini/$(TAG_)_MP.ini

build: $(addprefix build-,$(CONFIGS))
build-%: inis
	@mkdir -p $(HERE)/runs/$*
	cd $(HERE)/runs/$* && $(PSEUDO) \
	  --use-ini $(HERE)/ini/$(TAG_)_$*.ini --use-coinc $(COINC) \
	  --use-rundir $(HERE)/runs/$*/run --fake-data-cache $(CACHE) \
	  --use-online-psd-file $(PSDCOMB) --condor-nogrid-nonworker
	@echo "[build-$*] DAG generated under $(HERE)/runs/$*/run (NOT submitted)"

# D config: classic fd-diagonal on the PUBLIC BayesWave-deglitched frames (different cache/channel)
build-D: fetch
	@test -n "$(strip $(DEGLITCH_CACHE))" || { echo "ERROR: set DEGLITCH_CACHE in event.env (Zenodo 5546679)"; exit 1; }
	@mkdir -p $(HERE)/runs/D $(HERE)/ini
	$(PY) $(COMMON)/make_event_ini.py --env event.env --config D --out $(HERE)/ini/$(TAG_)_D.ini
	cd $(HERE)/runs/D && $(PSEUDO) \
	  --use-ini $(HERE)/ini/$(TAG_)_D.ini --use-coinc $(COINC) \
	  --use-rundir $(HERE)/runs/D/run --fake-data-cache $(strip $(DEGLITCH_CACHE)) \
	  --use-online-psd-file $(PSDCOMB) --condor-nogrid-nonworker
	@echo "[build-D] deglitched-frame DAG generated (NOT submitted)"

validate: build-MP
	@echo "==== [$(TAG_)] validate: flags in generated MP ILE.sub ===="
	@grep -oE "\-\-internal-ile-inner-product [a-z-]+|\-\-glitch-basis-file [^ ]+|\-\-window-shape [0-9.]+" $(HERE)/runs/MP/run/ILE.sub | sort -u
	@grep -oE "transfer_input_files = .*" $(HERE)/runs/MP/run/ILE.sub
	$(PY) $(COMMON)/run_ile_from_sub.py --sub $(HERE)/runs/MP/run/ILE.sub \
	  --exe $(CODE)/bin/integrate_likelihood_extrinsic_batchmode \
	  --n-max $(VALIDATE_NMAX) --n-eff $(VALIDATE_NEFF) --force-xpy \
	  --out-prefix $(HERE)/runs/MP/validate_pt 2>&1 | tee $(HERE)/runs/MP/validate.log
	@grep -E "\[TD\]|\[G3\]|\[T6\]|sigma_g|Delta_lnZ" $(HERE)/runs/MP/validate.log | sort -u || true

submit:
	@echo "==== GUARDED SUBMIT (paper-2 P2-setup: DO NOT auto-submit) ===="
	@for c in $(CONFIGS) D; do \
	  dag=$$(ls $(HERE)/runs/$$c/run/*.dag 2>/dev/null | head -1); \
	  if [ -n "$$dag" ]; then echo "  # config $$c:"; echo "  cd $(HERE)/runs/$$c/run && condor_submit_dag $$dag"; \
	  else echo "  config $$c: no DAG yet (run 'make build-$$c')"; fi; \
	done
	@echo "Container: build $(CONTAINER_KIT) on a CIT host + set SINGULARITY_RIFT_IMAGE first (README)."

clean:
	rm -rf $(HERE)/runs $(HERE)/ini
