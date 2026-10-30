"""
EpiScope - data quality rules for the silver layer (DQ-1, DQ-2).

Each rule is a named predicate that a row must satisfy. A row failing any rule
is routed to the quarantine table with the codes of every rule it broke, so a
single bad row is diagnosed once rather than reappearing load after load.

Rules are declared as data, not as branching code, for three reasons: the
per-load quality report in DQ-2 can enumerate them automatically, the tests in
ENG-4 can assert on each rule individually, and adding a rule later does not
touch the pipeline that applies them.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F


AGE_MIN, AGE_MAX = 0, 120
MAX_REPORTING_DELAY_DAYS = 21
VALID_SEX = ["M", "F", "O"]
VALID_OUTCOME = ["recovered", "died", "unknown"]
VALID_ICD10_PREFIXES = ["A00", "A01", "A02", "A09", "A90", "A91",
                        "B34", "B50", "B51", "B52", "B53", "B54",
                        "J06", "J10", "J11", "R50", "U07"]


def encounter_rules(valid_district_ids: list[str],
                    study_start: str, study_end: str) -> list[dict]:
    """
    The encounter quality rules.

    `passes` is the condition a good row satisfies. Null handling is explicit
    everywhere: a null in a checked field is a failure, not a silent pass,
    because a missing district or date is exactly the defect these rules exist
    to catch.
    """
    icd_ok = None
    for p in VALID_ICD10_PREFIXES:
        c = F.col("icd10_code").startswith(p)
        icd_ok = c if icd_ok is None else (icd_ok | c)

    return [
        {"code": "DQ01_SCHEMA_ENCOUNTER_ID",
         "description": "encounter_id present and non-empty",
         "passes": F.col("encounter_id").isNotNull()
                   & (F.length(F.trim(F.col("encounter_id"))) > 0)},

        {"code": "DQ02_PATIENT_HASH",
         "description": "patient_hash present and a full SHA-256 digest",
         # The rule checks the hash, not the raw identifier. PRV-1 removes
         # patient_id before the rules run, so testing it here would be both
         # impossible and a privacy regression. A null or truncated hash means
         # the source identifier was missing or de-identification failed, which
         # is the defect worth catching.
         "passes": F.col("patient_hash").isNotNull()
                   & (F.length(F.col("patient_hash")) == 64)},

        {"code": "DQ03_AGE_RANGE",
         "description": f"age_years between {AGE_MIN} and {AGE_MAX}",
         "passes": F.col("age_years").isNotNull()
                   & (F.col("age_years") >= AGE_MIN)
                   & (F.col("age_years") <= AGE_MAX)},

        {"code": "DQ04_SEX_DOMAIN",
         "description": f"sex in {VALID_SEX}",
         "passes": F.col("sex").isin(VALID_SEX)},

        {"code": "DQ05_DISTRICT_FK",
         "description": "district_id exists in the district reference",
         "passes": F.col("district_id").isin(valid_district_ids)},

        {"code": "DQ06_FACILITY_FK",
         "description": "facility_id exists in the S5 registry",
         "passes": F.col("_facility_exists")},

        {"code": "DQ07_ONSET_NOT_NULL",
         "description": "onset_date present",
         "passes": F.col("onset_date").isNotNull()},

        {"code": "DQ08_REPORT_NOT_NULL",
         "description": "report_date present",
         "passes": F.col("report_date").isNotNull()},

        {"code": "DQ09_DATE_ORDER",
         "description": "onset_date <= report_date",
         "passes": F.col("onset_date").isNotNull()
                   & F.col("report_date").isNotNull()
                   & (F.col("onset_date") <= F.col("report_date"))},

        {"code": "DQ10_REPORTING_DELAY",
         "description": f"reporting delay at most {MAX_REPORTING_DELAY_DAYS} days",
         "passes": F.datediff(F.col("report_date"), F.col("onset_date"))
                   <= F.lit(MAX_REPORTING_DELAY_DAYS)},

        {"code": "DQ11_ONSET_IN_STUDY_WINDOW",
         "description": f"onset_date within {study_start}..{study_end}",
         "passes": (F.col("onset_date") >= F.to_date(F.lit(study_start)))
                   & (F.col("onset_date") <= F.to_date(F.lit(study_end)))},

        {"code": "DQ12_ICD10_VALID",
         "description": "icd10_code matches an expected prefix",
         "passes": F.col("icd10_code").isNotNull() & icd_ok},

        {"code": "DQ13_OUTCOME_DOMAIN",
         "description": f"outcome in {VALID_OUTCOME}",
         "passes": F.col("outcome").isin(VALID_OUTCOME)},

        {"code": "DQ14_DUPLICATE_ENCOUNTER",
         "description": "encounter_id appears once after de-duplication",
         "passes": F.col("_dup_rank") == 1},
    ]


def apply_rules(df: DataFrame, rules: list[dict]
                ) -> tuple[DataFrame, DataFrame, list[dict]]:
    """
    Split `df` into (clean, quarantined, per-rule counts).

    Every rule is evaluated on every row, so the quarantine record lists all of
    a row's problems at once. Counting is done in a single aggregation rather
    than one count per rule, which matters at 28 million rows on two cores.
    """
    work = df
    for r in rules:
        work = work.withColumn(f"_fail_{r['code']}",
                               (~r["passes"].eqNullSafe(F.lit(True))).cast("int"))

    fail_cols = [f"_fail_{r['code']}" for r in rules]
    work = work.withColumn(
        "_fail_count", sum(F.col(c) for c in fail_cols))
    work = work.withColumn(
        "_quarantine_reasons",
        F.concat_ws(",", *[F.when(F.col(f"_fail_{r['code']}") == 1,
                                  F.lit(r["code"])) for r in rules]))

    agg = work.agg(
        F.count("*").alias("rows_in"),
        *[F.sum(c).alias(c) for c in fail_cols]).collect()[0]

    counts = [{"code": r["code"], "description": r["description"],
               "failed": int(agg[f"_fail_{r['code']}"] or 0),
               "rows_in": int(agg["rows_in"]),
               "pass_rate": round(1 - (agg[f"_fail_{r['code']}"] or 0)
                                  / max(1, agg["rows_in"]), 6)}
              for r in rules]

    clean = work.filter(F.col("_fail_count") == 0).drop(
        *fail_cols, "_fail_count", "_quarantine_reasons")
    quarantined = work.filter(F.col("_fail_count") > 0).drop(*fail_cols)

    return clean, quarantined, counts
