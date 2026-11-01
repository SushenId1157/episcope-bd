# EpiScope - one-command reproducibility (ENG-1)
#
# From a clean clone:
#   pip install -r requirements.txt
#   cp .env.example .env     # then set EPISCOPE_PATIENT_SALT
#   make all
#
# Every target writes stage timings into docs/experiments/, so the
# scalability table in the report is a record of measured runs rather than a
# claim. SF can be overridden on any target: make all SF=0.25

PY      ?= python
SF      ?= 1.0
SEED    ?= 20260101
DATA    ?= ./episcope_data
PUBLIC  ?= ./episcope_public
DROP    ?=

.PHONY: all setup public generate pipeline analytics serve test clean \
        incremental dashboard scaling help

help:
	@echo "EpiScope targets:"
	@echo "  make all           full rebuild: public data, generator, lake, analytics, serving"
	@echo "  make all SF=0.25   the same at quarter scale (about 10 minutes)"
	@echo "  make public        fetch S1 boundaries, S2 population, S3 weather"
	@echo "  make generate      synthetic S4-S6 and the ground truth"
	@echo "  make pipeline      bronze, silver, gold"
	@echo "  make analytics     detection, forecast, optimiser, calendar"
	@echo "  make serve         export the suppressed JSON the dashboard reads"
	@echo "  make dashboard     serve the dashboard at localhost:8000"
	@echo "  make incremental DROP=/path/to/drop   ingest a 14-day drop"
	@echo "  make scaling       run SF=0.25, 0.5 and 1.0 and record timings"
	@echo "  make test          the pytest suite"
	@echo "  make clean         remove the lake, keeping the public sources"

all: public generate pipeline analytics serve
	@echo ""
	@echo "Rebuild complete. Stage timings: docs/experiments/pipeline_sf$(SF).json"
	@echo "Dashboard: make dashboard, then open http://localhost:8000"

setup:
	$(PY) -m pip install -r requirements.txt
	@test -f .env || (cp .env.example .env && \
	  echo "Created .env - set EPISCOPE_PATIENT_SALT before running")

# Public sources are fetched once and cached. S3 in particular is 64 API
# calls, so re-fetching on every run would be rude to the provider and slow.
public:
	$(PY) -m orchestration.run_public --public $(PUBLIC)

generate:
	$(PY) -m orchestration.run_stage --stage generator \
	  --sf $(SF) --seed $(SEED) --data $(DATA) --public $(PUBLIC)

pipeline:
	$(PY) -m orchestration.run_stage --stage bronze --sf $(SF) --data $(DATA) --public $(PUBLIC)
	$(PY) -m orchestration.run_stage --stage silver --sf $(SF) --data $(DATA) --public $(PUBLIC)
	$(PY) -m orchestration.run_stage --stage gold   --sf $(SF) --data $(DATA) --public $(PUBLIC)

analytics:
	$(PY) -m orchestration.run_analytics --data $(DATA) --public $(PUBLIC)

serve:
	$(PY) -m orchestration.run_serving --data $(DATA) --public $(PUBLIC)

# The hidden test drop. No date, file name or row count is hard-coded: the
# loader hashes the drop, skips it if that content was already ingested, and
# recomputes only the epi-years the drop actually touches.
incremental:
	@test -n "$(DROP)" || (echo "Usage: make incremental DROP=/path/to/drop" && exit 1)
	$(PY) -m orchestration.run_incremental --drop $(DROP) \
	  --data $(DATA) --public $(PUBLIC)

scaling:
	@for sf in 0.25 0.5 1.0; do \
	  echo "=== SF=$$sf ==="; \
	  $(MAKE) generate pipeline SF=$$sf; \
	done
	$(PY) -m orchestration.report_scaling

dashboard:
	@echo "Serving at http://localhost:8000 (Ctrl-C to stop)"
	cd dashboard && $(PY) -m http.server 8000

test:
	$(PY) -m pytest tests/ -v

clean:
	rm -rf $(DATA)/lake $(DATA)/raw_synthetic
	@echo "Lake removed. Public sources in $(PUBLIC) kept."
