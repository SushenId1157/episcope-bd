"""
EpiScope - incremental, idempotent ingestion (ING-4).

The instructor's hidden 14-day drop arrives in the S4 schema with unannounced
outbreaks and quality defects, and must load without any code change. Three
properties make that possible:

1. Idempotency by content, not by filename. Each incoming file is hashed; if
   that checksum is already in the ingest log the load is a no-op. A file
   renamed or re-delivered is still recognised as the same data.

2. Idempotency by key. Even for genuinely new batches, an encounter_id already
   present is treated as a resubmission rather than a new case. Bronze keeps
   both copies, because bronze is the record of what arrived; silver keeps the
   earliest and quarantines the rest, so counts never double.

3. Selective recomputation. A late report changes the epi-week of its onset,
   not of its arrival. Only the affected (district, disease, epi-year,
   epi-week) cells are rebuilt, so a 14-day drop costs seconds rather than a
   full-history rebuild.

Nothing here hard-codes a date, a file name or a row count: the affected weeks
are derived from the data itself.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F

from pipeline.bronze.ingest import checksum_path, make_batch_id, \
    add_ingestion_metadata


INGEST_LOG = "_ingest_log.json"


def _log_path(bronze_root: str) -> str:
    return os.path.join(bronze_root, INGEST_LOG)


def read_ingest_log(bronze_root: str) -> list[dict]:
    p = _log_path(bronze_root)
    if not os.path.exists(p):
        return []
    try:
        return json.load(open(p))
    except Exception:
        return []


def append_ingest_log(bronze_root: str, entry: dict) -> None:
    log = read_ingest_log(bronze_root)
    log.append(entry)
    os.makedirs(bronze_root, exist_ok=True)
    json.dump(log, open(_log_path(bronze_root), "w"), indent=2)


def already_ingested(bronze_root: str, checksum: str) -> dict | None:
    """Return the prior log entry for this content, if it was loaded before."""
    for e in read_ingest_log(bronze_root):
        if e.get("checksum") == checksum:
            return e
    return None


def detect_format(path: str) -> str:
    """Infer the drop's format from what is actually there, not from its name."""
    if os.path.isdir(path):
        names = os.listdir(path)
        if any(n.endswith(".parquet") for n in names):
            return "parquet"
        if any(n.endswith(".csv") for n in names):
            return "csv"
        if any(n.endswith(".json") or n.endswith(".jsonl") for n in names):
            return "json"
        return "parquet"
    if path.endswith(".parquet"):
        return "parquet"
    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".json") or path.endswith(".jsonl"):
        return "json"
    return "csv"


def read_drop(spark: SparkSession, path: str) -> DataFrame:
    """Read an incremental drop in whichever supported format it arrives in."""
    fmt = detect_format(path)
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt == "json":
        return spark.read.json(path)
    return (spark.read.option("header", True)
            .option("inferSchema", True).csv(path))


def conform_drop(df: DataFrame) -> DataFrame:
    """
    Bring an incoming drop onto the bronze encounter schema.

    Only types and derived partition columns are fixed here; no value is
    corrected and no row is dropped, because that is silver's job and bronze
    must record what arrived. Columns the drop does not carry are added as
    null so the union with existing bronze succeeds whatever optional fields
    the instructor's file happens to include.
    """
    required = {
        "encounter_id": "string", "patient_id": "string",
        "facility_id": "string", "district_id": "string",
        "onset_date": "date", "report_date": "date",
        "age_years": "int", "sex": "string", "icd10_code": "string",
        "disease_group": "string", "lab_confirmed": "boolean",
        "admitted": "boolean", "outcome": "string",
    }
    for col, typ in required.items():
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(typ))
        else:
            df = df.withColumn(col, F.col(col).cast(typ))

    # Partition and helper columns are derived, never taken on trust
    # epi_year must come from the ISO calendar, not the Gregorian one.
    #
    # weekofyear() is ISO (week 1 is the week holding the first Thursday), but
    # year() is the calendar year. Combining them produces impossible pairs:
    # 2025-12-29 became (2025, 1), which reads as January 2025, and the
    # recompute then rebuilt the wrong weeks while the real rows were never
    # written anywhere. The diagnostic showed 1,657 rows claiming (2026, 52).
    #
    # date_format(d, "YYYY") is the ISO week-based year, which pairs correctly
    # with weekofyear and matches the epi-week definition used everywhere else
    # in the lake.
    # ISO week-based year, computed arithmetically rather than through a
    # format pattern.
    #
    # date_format(d, "YYYY") is documented as the ISO week-based year, but
    # under spark.sql.legacy.timeParserPolicy=LEGACY, which this pipeline
    # sets for other reasons, it returns the calendar year for dates in
    # early January. 2021-01-01 came out as (2021, 53) where ISO says
    # (2020, 53), and 2021 has no week 53, so that partition is never
    # created and those rows disappear silently. Late December rolled
    # forward correctly, which is why the defect survived earlier checks.
    #
    # The identity used here holds by definition: the ISO year of a date is
    # the calendar year of the Thursday in its week. Shifting each date to
    # the Thursday of its own ISO week and taking that year is exact in both
    # directions.
    iso_dow = ((F.dayofweek(F.col("onset_date")) + F.lit(5))
               % F.lit(7)) + F.lit(1)              # 1=Monday .. 7=Sunday
    thursday = F.date_add(F.col("onset_date"),
                          (F.lit(4) - iso_dow).cast("int"))

    df = (df
          .withColumn("epi_year", F.year(thursday).cast("int"))
          .withColumn("epi_week", F.weekofyear("onset_date").cast("long"))
          .withColumn("delay_days",
                      F.datediff("report_date", "onset_date").cast("int")))
    if "outbreak_multiplier" not in df.columns:
        df = df.withColumn("outbreak_multiplier", F.lit(1.0))
    if "_artefact_id" not in df.columns:
        df = df.withColumn("_artefact_id", F.lit(None).cast("string"))
    return df


def ingest_incremental(spark: SparkSession, drop_path: str, bronze_root: str,
                       table: str = "encounters",
                       force: bool = False) -> dict:
    """
    Ingest one incremental drop into bronze, idempotently.

    Returns a result describing what happened, including the list of affected
    (epi_year, epi_week) pairs that downstream stages must recompute.
    """
    checksum = checksum_path(drop_path)
    prior = already_ingested(bronze_root, checksum)
    if prior and not force:
        return {"status": "skipped_duplicate_content", "checksum": checksum,
                "previous_batch_id": prior["batch_id"],
                "previously_loaded_at": prior["loaded_at_utc"],
                "rows_added": 0, "affected_weeks": []}

    batch_id = make_batch_id("S4", "incremental")
    raw = conform_drop(read_drop(spark, drop_path))
    rows_in = raw.count()

    stamped = add_ingestion_metadata(
        raw, "S4", batch_id, os.path.basename(drop_path.rstrip("/")),
        checksum, "incremental")

    target = os.path.join(bronze_root, table)
    existing_cols = spark.read.parquet(target).columns
    for c in existing_cols:
        if c not in stamped.columns:
            stamped = stamped.withColumn(c, F.lit(None))
    stamped = stamped.select(*existing_cols)

    # Append, never overwrite: bronze is immutable history (ING-1).
    (stamped.write.mode("append").partitionBy("epi_year")
            .option("compression", "snappy").parquet(target))

    # The weeks to recompute come from the onset dates in the drop itself.
    affected = [(r["epi_year"], r["epi_week"]) for r in
                raw.select("epi_year", "epi_week").distinct()
                   .orderBy("epi_year", "epi_week").collect()]

    entry = {"batch_id": batch_id, "checksum": checksum,
             "source_path": drop_path, "rows": rows_in,
             "ingest_mode": "incremental",
             "loaded_at_utc": datetime.now(timezone.utc).isoformat(),
             "affected_weeks": [list(a) for a in affected]}
    append_ingest_log(bronze_root, entry)

    return {"status": "ingested", "batch_id": batch_id, "checksum": checksum,
            "rows_added": rows_in, "affected_weeks": affected}
