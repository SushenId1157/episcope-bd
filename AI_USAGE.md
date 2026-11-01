# AI Usage Declaration

Section 10 of the CSE 4345 CEP brief permits AI coding assistants and
requires disclosure of the tools used, their purposes, and representative
prompts. It further requires that we can explain any part of the code if an
authorship verification is called. This file is that disclosure.

---

## 1. Tool and scope of assistance

**Claude (Anthropic)**, used conversationally alongside Google Colab. No
other AI tool was used.

| Component | Assistance received |
|---|---|
| Data generation | Statistical model: seasonal shapes, climate lag structure, negative-binomial draw, outbreak injection |
| Apache Spark | Window functions, `applyInPandas` grouping, partitioning, avoiding Python UDFs on two cores |
| Outbreak detection | EARS C1/C2/C3 implementation and the seasonal adjustment |
| Forecasting | NB-GLM fitted by IRLS, rolling-origin backtest, leakage controls |
| Optimisation | Section 6.3's linear programme in PuLP, efficiency-equity sweep |
| Dashboard | D3.js v7 views, shared selection state, suppression marks |
| Documentation | Drafts of the README, proposal and report |

Assistance took the form of generated code, technical suggestions and
troubleshooting discussion. None of it was accepted as a verified solution:
every component was executed, inspected against expected behaviour, and in
several cases corrected before it entered the repository.

### Representative prompts

1. "Generate 25 million encounter records across 64 districts with
   seasonality driven by real rainfall at a 4-8 week lag, negative-binomial
   rather than Poisson."
2. "EARS C2 is raising 23 alerts per district-year. Is that the method or
   the data?"
3. "My idempotency test passes but five million rows are missing."
4. "The optimiser returns zero unmet demand on a budget covering 60% of
   need. That cannot be right."
5. "Write PRV-2 suppression so a published total cannot reveal a suppressed
   cell by subtraction."

---

## 2. Measured results

Every figure in the report, the dashboard and the README comes from a run we
executed and recorded in `docs/experiments/`. Nothing was estimated, and no
figure was carried forward after a change without re-measuring it.

**Pipeline.** 28,051,853 encounters at SF=1 across six stages, 41.9 minutes
total on 2 CPU cores and 12.67 GB, against a 45-minute requirement.

**Data quality.** 14 rules. The counts they caught were cross-checked
against the defects the generator had independently injected:

| Defect | Injected | Caught |
|---|---:|---:|
| Invalid ages | 168,406 | 168,406 |
| Invalid sex codes | 112,765 | 112,765 |
| Orphan facility keys | 140,444 | 140,444 |
| Duplicate encounters | 550,818 | 550,818 |

The exact correspondence is the evidence that the rules work. A rule that
catches approximately the right number is not demonstrably correct.

**Detection.** A seven-point threshold sweep, re-run after every change to
the detector, because a published trade-off curve has to describe the
detector that actually ships.

**Forecasting.** 26,624 model fits over 52 rolling origins, with weather
lags restricted to at least the forecast horizon so no forecast reads
rainfall from after its own origin.

**Allocation.** 20 scenarios across 5 budget levels and 4 equity floors,
each checked for solver status, budget, transport capacity and
non-negativity before any result was used.

---

## 3. Defects found by reading output

The assistance produced code that ran. Determining where it was nonetheless
wrong was the engineering work, and in several cases the code was correct
while the result was not.

| What we observed | What it turned out to be |
|---|---|
| The map rendered as one solid rectangle | A coordinate-rounding helper flattened the nesting of MultiPolygon rings, so every district drew at full canvas size |
| An idempotency test passed with eight green checks | The pipeline had deleted 5,091,096 rows; every comparison was between two already-damaged snapshots rather than against a pre-load baseline |
| A malaria preparedness window in coastal Barishal | The eligibility check asserted that hill divisions appeared in a list that contained every division, so it passed while testing nothing |
| A privacy audit reporting 1,966 violations | Almost all were week numbers; the audit could not distinguish a case count from an index |
| The optimiser reporting zero unmet demand while spending nothing | Generated stock was 4x to 60x the four-week requirement, so the allocation problem was vacuous |
| The alert table led by ten-case weeks in January | A winter expectation of one makes ten cases score twenty standard deviations |
| The dashboard showing a plan serving 3 districts of 64 | The panel rendered alpha=0 while the analysis recommends alpha=0.25 |

Three of those were tests that passed while testing nothing. That is the
most important thing this project taught us, and it is why the checks in
`tests/` assert against independently known quantities rather than against
the pipeline's own output.

---

## 4. Decisions taken by the team

### Detection threshold: k = 4.0

The sweep showed sensitivity flat at 91.4% from k=3.0 to k=4.0 while false
alerts fell from 6.03 to 3.11 per district-year. Nothing is given up by
moving along the flat part of the curve, so k=4.0 is the operating point.

A minimum alerting case count of 25 was added after seeing the alert table
led by ten-case weeks. Sensitivity stayed at 91.4% and false alerts fell
from 4.12 to 3.11, which showed that none of the removed alerts had been
detecting anything.

### Equity floor: alpha = 0.25

Pure efficiency serves 3 districts of 64. alpha=0.25 reaches all 64 for 5.7%
more unmet demand. alpha=0.50 costs 11.7% and adds no further coverage, only
lower inequality among districts already served.

### EARS rather than Farrington

Farrington Flexible fits a model on five years of matching historical weeks.
The study window is 2021-2025, so for every district in 2021 there is no
history at all and detection would be blind across a fifth of the data. EARS
uses a short sliding baseline and produces an alert from its eighth week.

### NB-GLM rather than a learned model

Conflict C5 values an official being able to challenge a coefficient. The
GLM's coefficients read as sentences: six weeks after a given rainfall,
expect this much more dengue. It beats a seasonal-naive baseline by 11.8% to
19.7% MAE depending on disease and horizon, with 80% intervals covering
0.846 of actuals against a nominal 0.80.

### Partitioning by epi_year and division

40 partitions of tens of megabytes each. Partitioning by epi_week would
create over 2,000 directories holding a few hundred rows; partitioning by
district would give 64 partitions skewed roughly 30:1 on Dhaka. Division is
the level at which the dashboard and the optimiser actually filter, so
partition pruning does real work.

### Malaria excluded from the optimiser

AN-4 requires at least two forecast diseases and we forecast dengue and
diarrhoeal disease, so malaria generates no demand and a decision variable
with zero demand carries no information. This is recorded in the README as a
scope boundary rather than hidden behind an empty column.

---

## 5. Limitations retained

These were found during implementation and are documented rather than
omitted:

- **View 1 is a symbol map, not a choropleth.** District polygons rendered
  at full canvas size under `d3.geoPath` despite valid GeoJSON. Position,
  colour encoding, alert markers and linked interaction all behave as
  specified; only the district outlines are absent.
- **Stock levels are a scenario assumption.** Generated S6 stock bears no
  relation to the case forecast, so a usable fraction is applied per
  resource to produce a scarcity problem. The fractions are stated in
  `analytics/optimise/allocation.py`, and the efficiency-equity analysis
  spans five budget levels so no conclusion rests on one of them.
- **The seasonal baseline for detection uses the full study period**,
  excluding the year under test. That is legitimate for retrospective
  evaluation but would not be available in real time. The forecaster uses
  strictly backward-looking features instead.

---

## 6. Responsibilities

Each member leads one area and presents it in the video, as Section 8
requires.

| Member | Area presented |
|---|---|
| Sushen Chakraborty | Generator, ingestion, lakehouse, quality rules, lineage, privacy |
| Rabya Akther | Outbreak detection, forecasting, optimisation, calendar |
| Prattasha Barua | Serving layer, suppression in outputs, D3 dashboard |
| Rafiqul Islam | Scalability experiment, Spark optimisations, tests, reproducibility |

The system-wide execution, debugging and experimental measurement described
in sections 2 and 3 were carried out by Sushen Chakraborty. The contribution
matrix in Appendix B records the actual distribution rather than an even
split, and the repository commit history supports it.

---

*Declared under Section 10 of the CSE 4345 CEP brief.*
