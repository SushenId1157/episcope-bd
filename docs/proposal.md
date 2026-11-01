# EpiScope - Project Proposal

**CSE 4345 Big Data Analytics - Complex Engineering Problem**  
Department of CSE, Premier University, Chittagong - Spring 2026  
Group <!-- number --> | https://github.com/SushenId1157/episcope-bd

| Member | ID | Lead role |
|---|---|---|
| Sushen Chakraborty | 0222220005101157 | Data Engineer: generator, ingestion, lakehouse, quality, lineage |
| Rabya Akther | 0222220005101154 | Analytics: detection, forecasting, optimisation, calendar |
| Prattasha Barua | 0222220005101170 | Visualisation: serving layer, privacy in outputs, D3 dashboard |
| Rafiqul Islam | 0222220005101158 | Engineering quality: scalability, Spark optimisations, tests |

*Four members, agreed with the instructor. The fourth role covers ENG-1 to
ENG-4, which a three-way split leaves unowned.*

## 1. Problem

Bangladesh's infectious disease burden is heavy and strongly seasonal, and
the data needed to anticipate it already exists across separate systems
that differ in format, timeliness and quality. Because analysis happens
after the fact, supplies reach a district once its hospitals are already
full. EpiScope turns routine health, climate and logistics data into early
warnings, one-to-four-week forecasts, a defensible allocation of a fixed
national stock, and an annual preparedness calendar.

The difficulty is interdependence rather than any single component:
ingestion quality bounds what detection can see, detection and forecasts
feed the optimiser, and the optimiser's output must reach non-technical
decision makers with its uncertainty intact.

## 2. Stakeholders

| Stakeholder | Need | Concern |
|---|---|---|
| Ministry policymakers | Defensible split of a fixed budget | Explaining why district X received more than Y |
| Epidemiologists | Transparent, adjustable methods | Reporting artefacts read as outbreaks |
| District officers | Early local warning | Alert fatigue; late supplies |
| Hospital managers | Demand ahead of time | Forecasts without uncertainty |
| Patients and public | Timely response, data not misused | Re-identification |
| Privacy regulator | Minimal data, small counts suppressed | Line-level data leaving the secure zone |

## 3. Conflicts and resolutions

**C1 Sensitivity against alert fatigue.** We swept the alert threshold and
measured both sides against ground truth the detector never reads.

| k | Sensitivity | False alerts / district-year | Median days |
|---|---|---|---|
| 1.5 | 97.1% | 18.48 | 7 |
| 2.0 | 97.1% | 12.70 | 7 |
| 2.5 | 94.3% | 8.66 | 7 |
| 3.0 | 91.4% | 6.03 | 14 |
| 3.5 | 91.4% | 4.30 | 14 |
| 4.0 **(chosen)** | 91.4% | 3.11 | 14 |
| 5.0 | 85.7% | 1.70 | 14 |

Sensitivity is flat from k=3 to k=4 while false alerts halve, so k=4 is
the operating point: 32/35
outbreaks at 3.109 false alerts per
district-year, median 14 days to detection.
Scoring against a leave-one-year-out seasonal expectation, rather than the
preceding weeks, was what made this workable: plain EARS reads the monsoon
climb itself as an aberration.

**C2 Granularity against privacy.** Counts of one to four are suppressed.
Because a published total reveals a single suppressed cell by subtraction,
a second cell in the group is suppressed too, and rates derived from a
hidden count are withheld with it. Suppressed cells are marked, never
drawn as zero. The audit runs over the shipped JSON, not the code.

**C3 Efficiency against equity.** Pure efficiency sends almost everything
to one to five districts, which no ministry can defend. An equity floor of
alpha=0.25 reaches all 64 districts for about 6% more unmet demand;
alpha=0.50 costs twice as much again and adds no coverage. **Recommended:
alpha=0.25.** Equity is cheapest when budgets are tightest (2% at a
quarter of the gap, 17% at one and a quarter), so the floor belongs early
in a response.

**C4 Freshness against cost.** Decisions are weekly and supplies move on
multi-day lead times, so a daily incremental batch matches the cadence.
Streaming would refresh alerts in minutes and change nothing anyone could
act on sooner.

**C5 Accuracy against interpretability.** A per-district negative-binomial
GLM, whose coefficients read as sentences an official can challenge. It
beats a seasonal-naive baseline by 12-20% MAE with 80% intervals covering
0.846
of actuals. A learned model would gain little and lose that.

## 4. Architecture and tools

Public sources and a seeded synthetic generator feed a medallion lake:
**bronze** (raw, with source, batch id and SHA-256 checksum), **silver**
(14 quality rules, quarantine with reason codes, salted-hash
de-identification before any other step), **gold** (epi-week marts
partitioned by epi_year and division, in a Spark SQL catalog). Analytics
read gold; a serving layer exports suppressed aggregates to a D3.js v7
dashboard.

| Choice | Justification |
|---|---|
| Apache Spark | ING-2 requires it, and 28M encounters exceeds pandas on the target machine |
| Parquet, epi_year + division | 40 partitions of tens of MB. epi_week gives 2,000 tiny directories; district skews 30:1 on Dhaka |
| EARS over Farrington | Farrington needs five years of matching history and would be blind across 2021; EARS works from week eight |
| NB-GLM over a learned model | Interpretability (C5); the negative binomial matches the overdispersion the counts show |
| PuLP with CBC | Named in the brief; the LP is small, the scale problem is producing reliable demand |
| D3.js v7 | VIS-1; no prohibited library is loaded |

## 5. Scope

Four surveillance diseases (dengue, diarrhoeal, influenza-like illness,
malaria) plus a background group. Forecasting covers dengue and
diarrhoeal disease, the two with the largest burden and clearest climate
signal.

## 6. Risk register

| Risk | Impact | Mitigation | Status |
|---|---|---|---|
| Ephemeral storage loses the lake | Medium | Gold backed up; everything else rebuildable from code and a fixed seed | Occurred 3x; recovery takes 4 min |
| SF=1 exceeds the 45-minute budget on 2 cores | High | Spark-native generation, no Python UDFs, AQE and broadcast joins | Measured 41.9 min |
| A check passes while testing nothing | High | Checks assert against independently known quantities | Occurred 3x, caught each time |
| Detection drowns users in false alerts | High | Seasonal adjustment, threshold sweep, minimum case floor | 3.109 per district-year |
| An unpinned dependency changes | Medium | Versions pinned in requirements.txt | PuLP 4.x broke the API; pinned to 2.8.0 |
| Small counts recoverable by arithmetic | High | Complementary suppression, derived rates withheld, audit over shipped JSON | Clean |
| Uneven team contribution | High | Per-member lead areas and individual video segments | Open |

---

*Teaching case for CSE 4345. Epidemiological data is synthetic and
labelled as such; public sources are cited with their licences. No output
is an official forecast.*