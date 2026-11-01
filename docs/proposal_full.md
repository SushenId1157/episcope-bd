# EpiScope - Project Proposal

**Course** CSE 4345 Big Data Analytics - Complex Engineering Problem  
**Institution** Department of CSE, Premier University, Chittagong - Spring 2026  
**Group** <!-- group number, once assigned -->  
**Repository** https://github.com/SushenId1157/episcope-bd

| Member | Student ID | GitHub | Lead role |
|---|---|---|---|
| Sushen Chakraborty | 0222220005101157 | SushenId1157 | Data Engineer - generator, ingestion, lakehouse, quality rules, lineage |
| Rabya Akther | 0222220005101154 | nusaifaa54 | Analytics Engineer - detection, forecasting, optimisation, calendar |
| Prattasha Barua | 0222220005101170 | prattashaaa | Visualisation Engineer - serving layer, privacy in outputs, D3 dashboard |
| Rafiqul Islam | 0222220005101158 | Rafiqul1158 | Engineering Quality - scalability experiment, Spark optimisations, tests, reproducibility |

*The brief specifies groups of three. This group of four was agreed with
the instructor. The fourth lead area covers ENG-1 to ENG-4, which the
three-role split leaves distributed rather than owned by anyone.*

---

## 1. Problem restatement

Bangladesh carries a heavy and strongly seasonal infectious disease burden.
Dengue follows the monsoon, diarrhoeal disease rises before it and after
floods, influenza-like illness peaks in the rainy season, and malaria is
concentrated in the hill districts. The data needed to anticipate these
patterns already exists, but it sits in separate systems that differ in
format, timeliness and granularity, and most analysis happens after the
fact in spreadsheets. Supplies therefore arrive in a district after its
hospitals are already full.

EpiScope is a working prototype of a national platform that turns routine
health, climate and logistics data into early warnings and a defensible
response plan. For every district it must detect unusual rises as early as
possible, forecast one to four weeks ahead, recommend how to allocate a
fixed national stock, and produce a seasonal preparedness calendar.

The engineering difficulty is not any single component. It is that the
components constrain each other: ingestion quality limits what detection
can see, detection and forecasts feed the optimiser, and the optimiser's
output has to reach non-technical decision makers with its uncertainty
intact.

## 2. Stakeholders

| Stakeholder | What they need | What they fear |
|---|---|---|
| Ministry policymakers | A national picture and a defensible split of a fixed budget | Being unable to explain why district X received more than Y |
| Surveillance epidemiologists | Transparent methods and adjustable thresholds | Reporting artefacts treated as outbreaks |
| District health officers | Early local warning and their own forecast | Alert fatigue; supplies arriving late |
| Hospital managers | Bed and consumable demand ahead of time | Forecasts presented without uncertainty |
| Vector-control units | Hotspots and campaign timing | Maps too coarse to act on |
| Patients and the public | A timely response, and no misuse of their data | Re-identification; stigma |
| Privacy regulator | Minimal data, suppressed small counts | Line-level data leaving the secure zone |

## 3. Conflicting requirements and how we resolved them

### C1 - Sensitivity against alert fatigue

A lower threshold detects outbreaks sooner and produces more false alarms.
We swept the alert threshold k across seven values and measured both sides
against independently generated ground truth.

| k | Sensitivity | Outbreaks found | False alerts per district-year | Median days to detect |
|---|---|---|---|---|
| 1.5 | 97.1% | 34/35 | 18.48 | 7 |
| 2.0 | 97.1% | 34/35 | 12.70 | 7 |
| 2.5 | 94.3% | 33/35 | 8.66 | 7 |
| 3.0 | 91.4% | 32/35 | 6.03 | 14 |
| 3.5 | 91.4% | 32/35 | 4.30 | 14 |
| 4.0 | 91.4% | 32/35 | 3.11 | 14 |
| 5.0 | 85.7% | 30/35 | 1.70 | 14 |

Sensitivity is flat between k=3 and k=4 while false alerts fall by half,
so k=4.0 is the operating point: nothing is given up by
moving along the flat part of the curve. The system detects
32 of 35 injected outbreaks
at 3.109 false alerts per district-year,
with a median of 14 days from onset to
first alert.

Two further decisions came from reading the numbers rather than from
theory. Plain EARS raised 12.16 alerts per district-year because
the monsoon climb itself reads as an aberration; scoring against a
leave-one-year-out seasonal expectation cut that sharply at the same
threshold. And the alert table was initially led by ten-case weeks in
January scoring twenty standard deviations against a winter expectation of
one, so a minimum observed-case floor was added: sensitivity stayed at
91.4% and false alerts fell further, meaning none
of the removed alerts had been detecting anything.

### C2 - Spatial granularity against patient privacy

Sub-district maps are more actionable, but small counts identify patients.
We suppress every count between one and four, and because a published
total would give a single suppressed cell away by subtraction, we also
suppress a second cell in the same group. Rates derived from a suppressed
count are withheld alongside it, since incidence multiplied by population
recovers the count just as well as subtraction would. Suppressed cells are
drawn as marked cells, never as zero, so a viewer can tell "withheld"
from "none".

The audit runs over the written JSON rather than over the code, because
suppression that is correct in intent can still fail in serialisation.

### C3 - Efficiency against equity

Minimising weighted unmet demand concentrates supplies where they avert
most harm, and in our scenario that means almost everything goes to one to
five districts. A ministry cannot defend that to the other fifty-nine. We
swept five budget levels against four equity floors and measured the cost
of each.

At a budget equal to the national gap, moving from a pure-efficiency
allocation to an equity floor of alpha=0.25 raises unmet demand by about
six percent and takes coverage from three districts to all sixty-four.
Going further to alpha=0.50 costs twice as much again and buys no
additional coverage. **We recommend alpha=0.25.**

A finding worth stating: equity is cheapest when budgets are tightest, two
percent at a quarter of the gap against seventeen percent at one and a
quarter. The floor should therefore be set early in a response, not added
once resources are plentiful.

### C4 - Freshness against cost

Decisions here are weekly: supplies move on a multi-day lead time and
campaigns take weeks to affect cases. A daily incremental batch matches
that cadence. Streaming would refresh alerts within minutes and change
nothing a district officer could act on sooner, while adding operational
complexity to a platform that must run on modest infrastructure.

### C5 - Accuracy against interpretability

We forecast with a negative-binomial GLM per district rather than a
learned model. Its coefficients can be read aloud: six weeks after a given
rainfall, expect this much more dengue. Against a seasonal-naive baseline
it improves MAE by 6 to 20 percent
depending on disease and horizon, with 80% prediction intervals covering
0.846 of actuals against a nominal 0.80. A gradient-boosted or
recurrent model would likely gain a little accuracy and lose the property
the brief cares about.

## 4. Planned architecture

```
public sources (boundaries, census, ERA5 weather)
synthetic generator (encounters, facilities, stock, ground truth)
        |
        v
   Bronze   raw, immutable, with source, batch id, SHA-256 checksum
        |
        v
   Silver   14 quality rules, quarantine with reason codes,
            salted-hash de-identification before anything else
        |
        v
   Gold     epi-week marts partitioned by epi_year and division,
            registered in a Spark SQL catalog
        |
        +--> detection   EARS C1/C2/C3 on seasonally adjusted counts
        +--> forecast    per-district NB-GLM trained in parallel
        +--> optimiser   linear programme, CBC solver
        +--> calendar    additive seasonal decomposition
                |
                v
          serving layer   suppressed aggregate JSON, 3.3 MB
                |
                v
          D3.js v7 dashboard, five coordinated views
```

### Tool choices and why

| Choice | Reason |
|---|---|
| Apache Spark | Required for heavy transformation (ING-2), and 28 million encounters is past what pandas handles on the available machine |
| Parquet | Columnar, compressed, partition-aware; the gold layer is 10 MB where the raw feed is 500 MB |
| epi_year + division partitioning | 40 partitions of tens of megabytes. epi_week would create two thousand tiny directories; district would skew thirty to one on Dhaka |
| EARS over Farrington | Farrington needs five years of matching history, so it would be blind across the whole of 2021. EARS works from its eighth week |
| NB-GLM over a learned model | Interpretability (C5), and the negative binomial matches the overdispersion the counts actually show |
| PuLP with CBC | Named in the brief; the optimisation is small, the hard part is producing reliable demand at scale |
| D3.js v7 | Required by VIS-1; the prohibited libraries are not loaded anywhere |

## 5. Diseases and methods

Four surveillance diseases: dengue, diarrhoeal disease, influenza-like
illness and malaria, with a background group of other febrile illness.
Forecasting covers dengue and diarrhoeal disease, the two with the largest
burden and the clearest climate signal.

## 6. Role assignment

| Work area | Lead | Support |
|---|---|---|
| Problem analysis, stakeholders, conflict analysis | Sushen | all |
| Synthetic generator and public-data acquisition | Sushen | Rafiqul |
| Ingestion, lakehouse layers, incremental loads | Sushen | Rafiqul |
| Data quality, lineage and privacy controls | Sushen | Prattasha |
| Outbreak detection and evaluation | Rabya | Sushen |
| Forecasting, backtesting and demand conversion | Rabya | Sushen |
| Optimisation model and preparedness calendar | Rabya | Prattasha |
| Serving layer and D3.js dashboard | Prattasha | Sushen |
| Scalability experiment and Spark optimisations | Rafiqul | Sushen |
| Automated tests and one-command reproducibility | Rafiqul | Sushen |
| Report and policy brief | all | - |
| Video production | all | - |

Every member presents their own lead area in the video and must be able to
explain it without a script, as Section 8 requires.

## 7. Risk register

| # | Risk | Likelihood | Impact | Mitigation | Status |
|---|---|---|---|---|---|
| 1 | Ephemeral notebook storage loses the lake | High | Medium | Gold backed up to durable storage; everything else rebuildable from committed code and a fixed seed | Occurred three times; recovery takes four minutes |
| 2 | SF=1 exceeds the 45-minute budget on 2 cores | Medium | High | Spark-native generation with no Python UDFs; AQE and broadcast joins | Measured 41.9 min against a 45-minute limit |
| 3 | A validation check passes while testing nothing | High | High | Checks assert against independently known quantities | Occurred three times and was caught each time |
| 4 | Detection drowns users in false alerts | High | High | Seasonal adjustment, threshold sweep, minimum case floor | 3.109 per district-year |
| 5 | An unpinned dependency changes under us | Medium | Medium | All versions pinned in requirements.txt | PuLP 4.x broke the documented API; pinned to 2.8.0 |
| 6 | Small counts recoverable by arithmetic | Medium | High | Complementary suppression plus withholding derived rates; audit over the shipped JSON | Audit clean |
| 7 | Uneven team contribution | Medium | High | Per-member lead areas and individual video segments | Open |

---

*This is a teaching case for CSE 4345. The epidemiological data is
synthetic and is labelled as such throughout. Public sources are cited
with their licences. No output should be read as an official forecast.*