"""
EpiScope - incremental refresh of silver and gold for affected epi-weeks (ING-4).

A 14-day drop touches only a handful of (district, disease, epi-year, epi-week)
cells, so rebuilding all 28 million rows would be both slow and pointless. The
refresh reads only the bronze partitions the drop touched, rebuilds silver for
those encounters, and replaces exactly the affected gold cells.

Idempotency holds at the key level as well as at the file level: silver always
recomputes a week from the full set of bronze rows for that week, so loading the
same batch twice produces the same silver and the same gold. The duplicate rule
keeps the earliest submission of each encounter_id and quarantines the rest, so
a resubmitted record never inflates a count.

Late arrivals are handled by construction. A record reported today for an onset
three weeks ago belongs to the earlier epi-week, so that week is recomputed and
its figures move. This is why onset_date, not report_date, is the time axis of
the gold marts.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from pipeline.quality.rules import encounter_rules, apply_rules
from pipeline.silver.build import deidentify
from pipeline.gold.build import weekly_incidence


def refresh_silver_for_weeks(spark: SparkSession, bronze_root: str,
                             silver_root: str, quarantine_root: str,
                             salt: str, affected_weeks: list[tuple],
                             study_start: str, study_end: str) -> dict:
    """Rebuild silver rows whose onset falls in the affected epi-weeks."""
    if not affected_weeks:
        return {"status": "nothing_to_do", "rows_clean": 0, "rows_quarantined": 0}

    years = sorted({int(y) for y, _ in affected_weeks})
    weeks = {(int(y), int(w)) for y, w in affected_weeks}

    # Partition pruning: only the touched epi_year directories are read.
    # Read the FULL partitions the drop touched, not only its weeks.
    #
    # Dynamic partition overwrite replaces an entire partition directory, so
    # writing back only the affected weeks would delete every other week in
    # that epi_year. An earlier version did exactly that and silently lost
    # 5,091,096 rows - a whole year - while every idempotency check still
    # passed, because the loss was present in each snapshot being compared.
    #
    # The unit of recomputation therefore has to match the unit of overwrite:
    # whole epi_year partitions. That is still far cheaper than a full rebuild,
    # since a 14-day drop touches one or two years rather than five, and it is
    # what keeps the operation genuinely idempotent instead of destructive.
    enc = spark.read.parquet(os.path.join(bronze_root, "encounters")) \
               .filter(F.col("epi_year").isin(years))

    fac = spark.read.parquet(os.path.join(bronze_root, "facilities"))
    dis = spark.read.parquet(os.path.join(bronze_root, "districts"))

    enc = deidentify(enc, salt)

    fac_keys = fac.select(F.col("facility_id").alias("_fk"),
                          F.lit(True).alias("_facility_exists")).distinct()
    enc = (enc.join(F.broadcast(fac_keys), enc.facility_id == F.col("_fk"), "left")
              .drop("_fk")
              .withColumn("_facility_exists",
                          F.coalesce(F.col("_facility_exists"), F.lit(False))))

    district_ids = [r["district_id"] for r in
                    dis.select("district_id").distinct().collect()]

    # Ranking spans every submission of an encounter_id within these weeks,
    # including ones loaded by earlier batches, which is what makes a re-run
    # produce the same result rather than appending a second copy.
    w = Window.partitionBy("encounter_id").orderBy(
        F.col("report_date").asc(), F.col("_batch_id").asc())
    enc = enc.withColumn("_dup_rank", F.row_number().over(w))

    rules = encounter_rules(district_ids, study_start, study_end)
    clean, quarantined, counts = apply_rules(enc, rules)

    clean = (clean
             .withColumn("epi_year_report", F.year("report_date"))
             .withColumn("epi_week_report", F.weekofyear("report_date"))
             .withColumn("reporting_delay_days",
                         F.datediff("report_date", "onset_date"))
             .drop("_dup_rank", "_facility_exists"))

    # replaceWhere would be ideal here; on plain Parquet the equivalent is to
    # rewrite only the affected partitions, which dynamic overwrite does.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    clean.write.mode("overwrite").partitionBy("epi_year") \
         .option("compression", "snappy") \
         .parquet(os.path.join(silver_root, "encounters"))

    (quarantined
     .select("encounter_id", "facility_id", "district_id", "onset_date",
             "report_date", "age_years", "sex", "icd10_code", "disease_group",
             "outcome", "_batch_id", "_source_id", "_quarantine_reasons",
             "_fail_count", "epi_year")
     .write.mode("overwrite").partitionBy("epi_year")
     .option("compression", "snappy")
     .parquet(os.path.join(quarantine_root, "encounters")))

    return {"status": "refreshed", "weeks": len(weeks),
            "years_touched": years, "rules": counts}


def refresh_gold_for_weeks(spark: SparkSession, bronze_root: str,
                           silver_root: str, gold_root: str,
                           affected_weeks: list[tuple]) -> dict:
    """Recompute the gold weekly marts for the affected epi-weeks only."""
    if not affected_weeks:
        return {"status": "nothing_to_do"}

    years = sorted({int(y) for y, _ in affected_weeks})
    weeks = {(int(y), int(w)) for y, w in affected_weeks}

    sil = spark.read.parquet(os.path.join(silver_root, "encounters"))
    dis = spark.read.parquet(os.path.join(bronze_root, "districts"))

    # Same reasoning as above: gold is partitioned by epi_year and division,
    # so the recomputation unit must be the whole epi_year, not single weeks.
    sub = sil.filter(F.col("epi_year").isin(years))

    fresh = weekly_incidence(spark, sub, dis)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Only the epi_year/division partitions present in `fresh` are rewritten;
    # every other partition is left untouched.
    fresh.write.mode("overwrite").partitionBy("epi_year", "division") \
         .option("compression", "snappy") \
         .parquet(os.path.join(gold_root, "weekly_incidence"))

    return {"status": "refreshed", "weeks": len(weeks),
            "rows_written": fresh.count()}
