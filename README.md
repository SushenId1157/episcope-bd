# EpiScope

**A big data surveillance, forecasting and response-planning platform for
seasonal, tropical and epidemic diseases in Bangladesh.**

CSE 4345 Big Data Analytics - Complex Engineering Problem  
Department of CSE, Premier University, Chittagong - Spring 2026

**Video presentation:** <!-- unlisted YouTube link goes here before submission -->

| Member | ID | Lead area |
|---|---|---|
| Sushen Chakraborty | 0222220005101157 | Generator, ingestion, lakehouse, quality, lineage |
| Rabya Akther | 0222220005101154 | Detection, forecasting, optimisation, calendar |
| Prattasha Barua | 0222220005101170 | Serving layer, privacy in outputs, D3 dashboard |
| Rafiqul Islam | 0222220005101158 | Scalability, Spark optimisations, tests, reproducibility |

---

## What this is

EpiScope ingests health, climate and logistics data into a medallion
lakehouse, detects unusual disease activity per district, forecasts one to
four weeks ahead, allocates a fixed national supply budget, and publishes
the result to a dashboard that decision makers can interrogate.

Measured on the environment described below:

- **28,051,853 encounter records** generated at SF=1 across 64 districts
  and 640 facilities, 2021-2025
- **27,073,598 rows** pass 14 quality rules;
  **978,255** are quarantined with reason codes
- **91.4% detection sensitivity**
  (32/35 injected outbreaks)
  at 3.109 false alerts per district-year
- Forecasts beat a seasonal-naive baseline by 12-20% MAE with 80%
  prediction intervals covering
  0.846
  of actuals
- Allocation reduces unmet demand by 74% against a
  population-proportional baseline
- Full pipeline at SF=1 runs in **41.9 minutes** on
  2 CPU cores, against a 45-minute requirement
- Dashboard payload **3.017 MB**, against a 10 MB limit

## Prerequisites

- Python 3.11 or later (measured on 3.13)
- Java 17 or later, required by Spark
- 8 GB RAM minimum; the measured runs used 12.67 GB and 2 cores
- About 2 GB of free disk for the lake at SF=1

```bash
git clone https://github.com/SushenId1157/episcope-bd.git
cd episcope-bd
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # then set EPISCOPE_PATIENT_SALT
```

The salt is not in the repository. Generate one:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

## One-command run

```bash
make all                      # full pipeline at SF=1, then analytics
make all SF=0.25              # a quarter-scale run, roughly 10 minutes
```

`make all` runs, in order: the synthetic generator, bronze ingestion,
silver cleaning and de-identification, the gold marts, outbreak detection,
the forecast backtest, the allocation optimiser, the preparedness
calendar, and the serving-layer export the dashboard reads. Stage timings
are written to `docs/experiments/`.

Individual stages:

```bash
make generate                 # synthetic data only
make pipeline                 # bronze, silver, gold
make analytics                # detection, forecast, optimiser, calendar
make serve                    # export the dashboard JSON
make test                     # the automated test suite
```

## Running the hidden incremental drop

Place the drop anywhere and point the loader at it. No code change, no
date, file name or row count is hard-coded.

```bash
make incremental DROP=/path/to/drop
```

Or directly:

```bash
python -m orchestration.run_incremental --drop /path/to/drop
```

The loader accepts Parquet, CSV or JSON and infers which from the
contents. It then:

1. hashes the drop and skips it if that content was already ingested,
   whatever the file is called;
2. appends to bronze, which is immutable history;
3. recomputes silver and gold for only the epi-years the drop touches,
   so a 14-day drop costs seconds rather than a full rebuild;
4. regenerates alerts, the allocation plan and the dashboard exports.

Late reports update the epi-week of **onset**, not of arrival, so a record
reported today for an onset three weeks ago moves the earlier week's
figures. Re-running the same drop leaves every gold total unchanged;
`make test` asserts this.

## Running the dashboard

```bash
cd dashboard && python -m http.server 8000
```

Then open <http://localhost:8000>. Opening `index.html` from the file
system will not work: browsers block `fetch()` on `file://` URLs, and the
dashboard loads its data that way.

Five coordinated views: a district map of incidence per 100,000 with alert
markers, a time series with forecast bands and brushing, a seasonal
heatmap, an allocation comparison, and a ranked early-warning table.
Selecting a district, disease or time range in any view filters the rest.

## Architecture

```
S1 boundaries   S2 population   S3 ERA5 weather      (real, cited)
S4 encounters   S5 facilities   S6 stock             (synthetic, seeded)
        |
        v
  BRONZE   raw and immutable; source, batch id, SHA-256 checksum,
           load timestamp on every row
        |
        v
  SILVER   14 quality rules; failures quarantined with reason codes;
           patient_id replaced by a salted hash before anything else
        |
        v
  GOLD     epi-week marts, partitioned by epi_year and division,
           registered in the episcope_gold Spark SQL catalog
        |
        +--> detection    EARS C1/C2/C3 on seasonally adjusted counts
        +--> forecasting  per-district NB-GLM, trained in parallel
        +--> optimisation linear programme, CBC solver
        +--> calendar     additive seasonal decomposition
        |
        v
  SERVING  aggregates only, small counts suppressed
        |
        v
  D3.js v7 dashboard
```

Privacy is enforced by ordering, not by filtering at the end:
de-identification runs before the quality rules, so neither silver nor the
quarantine table ever holds a direct identifier.

## Repository layout

```
generator/      seeded synthetic S4-S6 and the ground-truth outbreaks
pipeline/       bronze, silver, gold, and the quality rules
analytics/      detection, evaluation, forecast, optimise, calendar
serving/        gold to suppressed JSON for the browser
dashboard/      index.html, js/dashboard.js, data/
orchestration/  the pipeline runner that make calls
tests/          pytest suite
docs/           proposal, report, figures, experiment records
```

## Data sources

| ID | Source | Type | Licence |
|---|---|---|---|
| S1 | geoBoundaries ADM2 | Real | ODbL 1.0, attribution required |
| S2 | HDX COD-PS, from BBS census | Real | HDX terms |
| S3 | Open-Meteo ERA5 archive | Real | CC-BY 4.0 |
| S4-S6 | This project's generator | **Synthetic** | - |

**Everything epidemiological in this project is synthetic.** The real data
provides geography, population denominators and the weather that drives
the synthetic seasonality. No output is an official statistic or forecast.

## Requirement traceability

| ID | Where it is implemented | Evidence | Report |
|---|---|---|---|
| ING-1 | `pipeline/bronze/ingest.py`, `acquire_public.py` | `docs/experiments/bronze_sf1.0.json` | 4.1 |
| ING-2 | Spark throughout; no Python UDF in the generator | `generator/encounters.py` | 3.2 |
| ING-3 | `pipeline/gold/build.py` | 40 partitions, `episcope_gold` catalog | 4.2 |
| ING-4 | `pipeline/bronze/incremental.py`, `pipeline/silver/incremental.py` | `docs/experiments/idempotency_test.json` | 4.3 |
| DQ-1 | `pipeline/quality/rules.py` | 14 rules, `silver_sf1.0.json` | 4.4 |
| DQ-2 | `pipeline/gold/build.py::quality_report` | `episcope_gold.data_quality_report` | 4.4 |
| DQ-3 | `source_batch_ids` on every gold row | `weekly_incidence` schema | 4.5 |
| PRV-1 | `pipeline/silver/build.py::deidentify` | salt in `.env`, absent from silver and quarantine | 7.1 |
| PRV-2 | `serving/export.py` | audit over shipped JSON, clean | 7.2 |
| AN-1 | `analytics/detection/ears.py` | `detection_eval.json` | 5.1 |
| AN-2 | `analytics/evaluation/detection_eval.py` | 7-point threshold sweep | 6.1 |
| AN-3 | same, `method_comparison` | C1/C2/C3 and plain-vs-seasonal | 6.1 |
| AN-4 | `analytics/forecast/models.py` | 128 series trained in parallel | 5.2 |
| AN-5 | `analytics/forecast/backtest.py` | 52 rolling origins, `forecast_backtest.json` | 6.2 |
| AN-6 | `analytics/optimise/allocation.py::convert_to_demand` | assumptions stated in-module | 5.3 |
| AN-7 | `allocation.py::optimise_allocation` | `allocation.json`, solver status Optimal | 6.3 |
| AN-8 | `efficiency_equity.csv` | 20 scenarios, 5 budgets x 4 floors | 6.3 |
| AN-9 | `analytics/calendar/preparedness.py` | `preparedness_calendar.csv` | 5.4 |
| VIS-1 | `dashboard/` | D3.js v7 only | 7.3 |
| VIS-2 | `dashboard/js/dashboard.js` | five views | 7.3 |
| VIS-3 | shared selection state | linked filtering | 7.3 |
| VIS-4 | `serving/export.py` | `serving_payload.json` | 7.3 |
| ENG-1 | `Makefile`, `requirements.txt` | this file | 4.6 |
| ENG-2 | `orchestration/run_pipeline.py` | `pipeline_sf1.0_baseline.json` | 6.4 |
| ENG-3 | AQE, broadcast joins, partition pruning | before/after in 6.4 | 6.4 |
| ENG-4 | `tests/` | `make test` | 6.5 |

## Known limitations

- **View 1 is a symbol map, not a choropleth.** District polygons rendered
  at full canvas size under `d3.geoPath` despite valid GeoJSON. Position,
  colour encoding, alert markers and all linked interaction behave as
  specified; only the shape of each district is absent.
- **Malaria is not forecast.** AN-4 requires at least two diseases and we
  forecast dengue and diarrhoeal disease, so antimalarial demand is zero
  and the resource is excluded from the optimiser's decision set.
- **Stock levels are a scenario assumption.** The generated S6 stock bears
  no relation to the case forecast, so a usable fraction is applied per
  resource to produce a scarcity problem. The fractions are stated in
  `analytics/optimise/allocation.py` and the efficiency-equity analysis
  spans five budget levels so no conclusion rests on one of them.
- **The seasonal baseline for detection uses the full study period**,
  excluding the year under test. That is legitimate for retrospective
  evaluation but would not be available in real time. The forecaster uses
  strictly backward-looking features instead.

## Report and documentation

- `docs/proposal.md` - the M1 proposal
- `docs/report.pdf` - the IEEE-format technical report
- `docs/figures/` - figures referenced by the report
- `docs/experiments/` - the measured results every figure is drawn from
- `AI_USAGE.md` - how AI assistance was used

---

*Teaching case for CSE 4345. Not a Ministry of Health system.*