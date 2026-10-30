"""
EpiScope - silver layer: conformed, de-identified, quality-checked encounters
(DQ-1, DQ-2, DQ-4, PRV-1).

Order of operations matters here and is deliberate:

  1. de-identify first. PRV-1 requires that direct identifiers never travel
     beyond bronze, so patient_id is replaced by a salted hash before any
     other transformation touches the data. Nothing downstream, including the
     quarantine table, ever sees the raw identifier.
  2. resolve foreign keys next, so the facility rule has something to test.
  3. rank duplicates, so the duplicate rule can keep the first submission and
     quarantine the resubmissions rather than dropping both.
  4. apply the rules and split.

The salt is read from the environment, never from the repository (PRV-1, H1).
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from pipeline.quality.rules import encounter_rules, apply_rules


def load_salt(env_path: str) -> str:
    """Read the patient salt from .env. Fails loudly rather than defaulting."""
    if not os.path.exists(env_path):
        raise RuntimeError(f"No .env at {env_path}. PRV-1 requires a salt kept "
                           f"outside the repository; copy .env.example and set "
                           f"EPISCOPE_PATIENT_SALT.")
    for line in open(env_path):
        if line.startswith("EPISCOPE_PATIENT_SALT="):
            salt = line.split("=", 1)[1].strip()
            if not salt or salt.startswith("replace_me"):
                raise RuntimeError("EPISCOPE_PATIENT_SALT is still the "
                                   "placeholder value.")
            return salt
    raise RuntimeError("EPISCOPE_PATIENT_SALT not found in .env")


def deidentify(df: DataFrame, salt: str) -> DataFrame:
    """
    Replace patient_id with a salted SHA-256 hash and reduce age to a band.

    The salt is concatenated with the identifier before hashing, so an attacker
    holding the output cannot test candidate national IDs against it without
    also holding the salt. Age is kept as an integer for the clinical analyses
    that need it, and an age_band is added for anything that will be published,
    which is what PRV-2 suppression operates on.
    """
    hashed = F.sha2(F.concat(F.lit(salt), F.col("patient_id")), 256)
    return (df
            .withColumn("patient_hash", hashed)
            .drop("patient_id")
            .withColumn("age_band",
                        F.when(F.col("age_years") < 5, F.lit("0-4"))
                         .when(F.col("age_years") < 15, F.lit("5-14"))
                         .when(F.col("age_years") < 25, F.lit("15-24"))
                         .when(F.col("age_years") < 45, F.lit("25-44"))
                         .when(F.col("age_years") < 65, F.lit("45-64"))
                         .when(F.col("age_years") <= 120, F.lit("65+"))
                         .otherwise(F.lit("invalid"))))


def build_silver(spark: SparkSession, bronze_root: str, silver_root: str,
                 quarantine_root: str, salt: str,
                 study_start: str, study_end: str) -> dict:
    """Run the full silver build and return a per-load quality report (DQ-2)."""
    enc = spark.read.parquet(os.path.join(bronze_root, "encounters"))
    fac = spark.read.parquet(os.path.join(bronze_root, "facilities"))
    dis = spark.read.parquet(os.path.join(bronze_root, "districts"))

    rows_in = enc.count()

    # 1. de-identify before anything else (PRV-1)
    enc = deidentify(enc, salt)

    # 2. resolve the facility foreign key. The registry is small, so a
    # broadcast join avoids shuffling 28 million rows (ENG-3 optimisation).
    fac_keys = fac.select(F.col("facility_id").alias("_fk"),
                          F.lit(True).alias("_facility_exists")).distinct()
    enc = (enc.join(F.broadcast(fac_keys),
                    enc.facility_id == F.col("_fk"), "left")
              .drop("_fk")
              .withColumn("_facility_exists",
                          F.coalesce(F.col("_facility_exists"), F.lit(False))))

    district_ids = [r["district_id"] for r in
                    dis.select("district_id").distinct().collect()]

    # 3. rank submissions of the same encounter_id. The earliest report_date is
    # the original; later ones are resubmissions. Ranking by report_date then
    # batch id makes the choice deterministic even when two arrive the same
    # day, which matters for the idempotency guarantee in ING-4.
    w = Window.partitionBy("encounter_id").orderBy(
        F.col("report_date").asc(), F.col("_batch_id").asc())
    enc = enc.withColumn("_dup_rank", F.row_number().over(w))

    # 4. apply the rules and split
    rules = encounter_rules(district_ids, study_start, study_end)
    clean, quarantined, counts = apply_rules(enc, rules)

    # Epi-week is derived here so every downstream stage shares one definition.
    clean = (clean
             .withColumn("epi_year_report", F.year(F.col("report_date")))
             .withColumn("epi_week_report", F.weekofyear(F.col("report_date")))
             .withColumn("reporting_delay_days",
                         F.datediff(F.col("report_date"), F.col("onset_date"))))

    clean_out = os.path.join(silver_root, "encounters")
    (clean.drop("_dup_rank", "_facility_exists")
          .write.mode("overwrite").partitionBy("epi_year")
          .option("compression", "snappy").parquet(clean_out))

    q_out = os.path.join(quarantine_root, "encounters")
    (quarantined
     .select("encounter_id", "facility_id", "district_id", "onset_date",
             "report_date", "age_years", "sex", "icd10_code", "disease_group",
             "outcome", "_batch_id", "_source_id", "_quarantine_reasons",
             "_fail_count", "epi_year")
     .write.mode("overwrite").partitionBy("epi_year")
     .option("compression", "snappy").parquet(q_out))

    rows_clean = spark.read.parquet(clean_out).count()
    rows_quar = spark.read.parquet(q_out).count()

    return {"rows_in": rows_in, "rows_clean": rows_clean,
            "rows_quarantined": rows_quar,
            "quarantine_rate": round(rows_quar / max(1, rows_in), 6),
            "rules": counts, "clean_path": clean_out, "quarantine_path": q_out}
