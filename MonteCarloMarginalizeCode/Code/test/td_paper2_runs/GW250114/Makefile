# GW250114_082203 -- paper-2 glitch-marg production-run setup.
# Targets: fetch -> basis -> build (DAG per config, NO submit) -> validate (one ILE point) ->
#          submit (GUARDED: prints condor_submit_dag, does NOT execute).
# LOCAL ONLY. Never `git push`. Submission is a deliberate later step with the run owner.
include event.env

HERE    := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
CODE    := $(abspath $(HERE)/../../..)
COMMON  := $(HERE)/../common
PY      ?= $(HOME)/.conda/envs/junior_rift/bin/python
PSEUDO  := $(PY) $(CODE)/bin/util_RIFT_pseudo_pipe.py
export PYTHONPATH := $(CODE):$(PYTHONPATH)
export PATH := $(dir $(PY)):$(CODE)/bin:$(PATH)     # so #!/usr/bin/env python shebangs resolve

TAG_    := $(strip $(TAG))
GIFO    := $(strip $(GLITCH_IFO))
CONFIGS := U MF MP        # GW250114: no public deglitched frames, so no D config
FLAT_BASIS := $(HERE)/basis/glitch_basis_$(GIFO).npz
POP_BASIS  := $(HERE)/basis/glitch_basis_$(GIFO)_pop.npz

# Container family kit (build + deploy is the deployer's step; see README).
CONTAINER_KIT := $(HOME)/rift_cit_build_container_family/tdlike_paper2

CACHE   := $(HERE)/data/frames.cache
PSDCOMB := $(HERE)/psd/combined-psd.xml.gz
COINC   := $(HERE)/coinc.xml

.PHONY: help fetch basis build validate submit clean inis
help:
	@echo "GW250114 paper-2 setup.  Configs: $(CONFIGS)"
	@echo "  make fetch      fetch strain + off-source PSDs + coinc  (network)"
	@echo "  make basis      corpus -> PCA -> strain basis + population prior"
	@echo "  make build      generate a condor DAG per config (NO submit)"
	@echo "  make validate   run ONE ILE point from the generated MP ILE.sub (go/no-go)"
	@echo "  make submit     GUARDED: print the condor_submit_dag line (does NOT submit)"
	@echo "  make clean      remove runs/ and ini/ (keeps data/, psd/, basis/)"

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
	@grep -q "internal-ile-inner-product td-toeplitz" $(HERE)/runs/$*/run/ILE.sub && \
	  echo "[build-$*] ILE.sub carries td-toeplitz" || echo "[build-$*] NOTE: U config (no glitch flags expected)"

# go/no-go: run ONE ILE point from the generated MP ILE.sub on the REAL data.
validate: build-MP
	@echo "==== [$(TAG_)] validate: flags in generated MP ILE.sub ===="
	@grep -oE "\-\-internal-ile-inner-product [a-z-]+|\-\-glitch-basis-file [^ ]+|\-\-window-shape [0-9.]+" $(HERE)/runs/MP/run/ILE.sub | sort -u
	@grep -oE "transfer_input_files = .*" $(HERE)/runs/MP/run/ILE.sub
	@echo "==== [$(TAG_)] validate: run ONE ILE point (CPU, reduced n-max) ===="
	$(PY) $(COMMON)/run_ile_from_sub.py --sub $(HERE)/runs/MP/run/ILE.sub \
	  --exe $(CODE)/bin/integrate_likelihood_extrinsic_batchmode \
	  --n-max $(VALIDATE_NMAX) --n-eff $(VALIDATE_NEFF) --force-xpy \
	  --out-prefix $(HERE)/runs/MP/validate_pt 2>&1 | tee $(HERE)/runs/MP/validate.log
	@echo "==== [$(TAG_)] validate banners ===="
	@grep -E "\[TD\]|\[G3\]|\[T6\]|guess_snr|sigma_g|Delta_lnZ" $(HERE)/runs/MP/validate.log | sort -u || true

VALIDATE_NMAX ?= 40000
VALIDATE_NEFF ?= 10

submit:
	@echo "==== GUARDED SUBMIT (paper-2 P2-setup: DO NOT auto-submit) ===="
	@for c in $(CONFIGS); do \
	  dag=$$(ls $(HERE)/runs/$$c/run/*.dag 2>/dev/null | head -1); \
	  if [ -n "$$dag" ]; then \
	    echo "  # config $$c:"; \
	    echo "  cd $(HERE)/runs/$$c/run && condor_submit_dag $$dag"; \
	  else echo "  config $$c: no DAG yet (run 'make build-$$c')"; fi; \
	done
	@echo "Container: build $(CONTAINER_KIT) on a CIT host and set SINGULARITY_RIFT_IMAGE first (README)."

clean:
	rm -rf $(HERE)/runs $(HERE)/ini
