"""
EpiScope - S6 daily stock and bed-occupancy snapshots, plus the reporting
artefacts required by Section 5.2.

Two separate concerns live here:

1. S6: per facility, per day, the quantity of each tracked supply and the
   number of occupied beds. Occupancy responds to the admissions generated in
   S4, so the resource demand conversion in AN-6 has a realistic starting
   stock to work against.

2. Reporting artefacts: a facility stops submitting for several weeks and then
   back-fills everything at once. The underlying illness never changed, only
   the reporting did, so a detector that works on report_date alone sees a
   large artificial spike. AN-3 has to show the pipeline does not alert on it.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


def _unit(col, salt: str, seed: int):
    h = F.xxhash64(F.concat_ws(":", col, F.lit(salt), F.lit(str(seed))))
    return (F.abs(h) % F.lit(1_000_000)) / F.lit(1_000_000.0)


# ------------------------------------------------------- reporting artefacts
def apply_reporting_artefacts(spark: SparkSession, enc: DataFrame,
                              facilities_df: DataFrame, artefacts: list[dict],
                              study_start: str, seed: int
                              ) -> tuple[DataFrame, DataFrame]:
    """
    Simulate a district-wide reporting outage followed by a back-fill.

    An earlier version silenced a single large facility. Measurement showed
    that one facility contributes only 16-20% of its district's records, so
    the other facilities filled the gap and the district weekly series barely
    moved (peak/median 1.07x). Outbreak detection runs on district weekly
    counts, so an artefact invisible at that level gives AN-3 nothing to
    demonstrate.

    Silencing the whole district is also the more realistic failure: when a
    district's DHIS2 server, power or connectivity goes down, every facility
    in it stops submitting, and the held records arrive together when service
    returns. onset_date is never touched, because the illnesses happened when
    they happened; only the reporting moved.
    """
    if not artefacts:
        return enc, None

    # Choose districts with enough volume for the artefact to be unmistakable,
    # deterministically from the registry order rather than at random.
    district_sizes = (facilities_df.groupBy("district_id")
                      .agg(F.sum(F.col("total_beds").cast("int")).alias("beds"))
                      .orderBy(F.desc("beds"))
                      .collect())

    if "_artefact_id" not in enc.columns:
        enc = enc.withColumn("_artefact_id", F.lit(None).cast("string"))

    log_rows = []
    for i, art in enumerate(artefacts):
        if not district_sizes:
            break
        # Avoid the very largest district: an outage in Dhaka would dominate
        # the national series and confound the national novel-respiratory
        # event. Pick from the upper-middle of the distribution instead.
        pick = district_sizes[min(3 + i * 5, len(district_sizes) - 1)]
        did = pick["district_id"]

        start_w = int(art["start_week_index"])
        silent = int(art["silent_weeks"])
        backfill_week = start_w + silent

        base = F.to_date(F.lit(study_start))
        silent_from = F.date_add(base, start_w * 7)
        silent_to = F.date_add(base, backfill_week * 7)
        resume_date = F.date_add(base, backfill_week * 7 + 2)

        c = ((F.col("district_id") == F.lit(did))
             & (F.col("report_date") >= silent_from)
             & (F.col("report_date") < silent_to))

        enc = (enc
               .withColumn("_artefact_id",
                           F.when(c, F.lit(art["artefact_id"]))
                            .otherwise(F.col("_artefact_id")))
               .withColumn("report_date",
                           F.when(c, resume_date)
                            .otherwise(F.col("report_date"))))

        log_rows.append({
            "artefact_id": art["artefact_id"],
            "artefact_type": "district_outage_then_backfill",
            "district_id": did,
            "scope": "whole_district",
            "silent_weeks": silent,
            "start_week_index": start_w,
            "backfill_week_index": backfill_week,
            "note": art["note"],
        })

    log_df = spark.createDataFrame(log_rows) if log_rows else None
    return enc, log_df


# --------------------------------------------------------------------- S6
def build_stock_snapshots(spark: SparkSession, facilities_df: DataFrame,
                          enc: DataFrame, study_start: str, study_end: str,
                          seed: int, sample_days: int = 7) -> DataFrame:
    """
    Daily stock and occupancy per facility.

    Snapshots are produced every `sample_days` days rather than every single
    day: at 640 facilities a full daily series for five years is 1.2 million
    rows of slowly-changing inventory, which adds volume without adding
    information. The cadence is recorded in the data so the gold layer can
    interpolate, and the incremental feed still arrives daily (ING-4).

    Occupied beds are driven by the admissions actually generated in S4, so
    hospital load rises during outbreaks exactly as the encounter data says.
    """
    days = (F.datediff(F.to_date(F.lit(study_end)),
                       F.to_date(F.lit(study_start))))

    # One row per facility per sampled day
    cal = (spark.range(0, 400)                    # upper bound on sample points
           .withColumn("day_offset", (F.col("id") * F.lit(sample_days))
                       .cast("int"))
           .withColumn("snapshot_date",
                       F.date_add(F.to_date(F.lit(study_start)),
                                  F.col("day_offset")))
           .filter(F.col("snapshot_date") <= F.to_date(F.lit(study_end)))
           .select("snapshot_date"))

    fac = facilities_df.select(
        "facility_id", "district_id",
        F.col("total_beds").cast("int").alias("total_beds"),
        F.col("icu_beds").cast("int").alias("icu_beds"))

    snap = fac.crossJoin(F.broadcast(cal))

    # Admissions in the week preceding each snapshot drive occupancy.
    adm = (enc.filter(F.col("admitted"))
              .groupBy("facility_id", F.col("report_date").alias("d"))
              .agg(F.count("*").alias("admissions")))

    snap = (snap.join(adm,
                      (snap.facility_id == adm.facility_id)
                      & (adm.d > F.date_sub(snap.snapshot_date, 7))
                      & (adm.d <= snap.snapshot_date), "left")
                .drop(adm.facility_id)
                .groupBy("facility_id", "district_id", "snapshot_date",
                         "total_beds", "icu_beds")
                .agg(F.coalesce(F.sum("admissions"), F.lit(0))
                      .alias("week_admissions")))

    key = F.concat_ws(":", F.col("facility_id"),
                      F.col("snapshot_date").cast("string"))

    # Baseline occupancy plus pressure from recent admissions, capped at
    # capacity: a hospital cannot hold more patients than it has beds.
    # Baseline occupancy is kept low enough that admissions actually move the
    # number. An earlier version used a 0.45-0.80 baseline, which left national
    # occupancy flat at ~0.69 in every year, including outbreak years. Hospital
    # load that does not respond to disease would make the bed-demand
    # conversion in AN-6 and the allocation view in VIS-2 meaningless.
    # Admitted patients are assumed to occupy a bed for several days, so a
    # week's admissions contribute more than one bed-day each.
    base_occ = _unit(key, "occ", seed) * F.lit(0.18) + F.lit(0.28)
    snap = snap.withColumn(
        "occupied_beds",
        F.least(F.col("total_beds"),
                (F.col("total_beds") * base_occ
                 + F.col("week_admissions") * F.lit(2.2)).cast("int")))
    snap = snap.withColumn(
        "occupied_icu_beds",
        F.least(F.col("icu_beds"),
                (F.col("icu_beds") * (_unit(key, "icu", seed) * F.lit(0.4)
                                      + F.lit(0.45))).cast("int")))

    # Supplies scale with facility size and fluctuate day to day. Stock is
    # drawn down harder when the facility has been busy.
    def supply(name: str, per_bed: float, spread: float):
        u = _unit(key, name, seed)
        return F.greatest(
            F.lit(0),
            (F.col("total_beds") * F.lit(per_bed) * (F.lit(1 - spread) + u
                                                     * F.lit(2 * spread))
             - F.col("week_admissions") * F.lit(per_bed * 0.25)).cast("int"))

    snap = (snap
            .withColumn("iv_saline_units", supply("saline", 9.0, 0.35))
            .withColumn("ns1_kits", supply("ns1", 3.5, 0.45))
            .withColumn("ors_sachets", supply("ors", 14.0, 0.30))
            .withColumn("antimalarial_doses", supply("amal", 2.2, 0.50))
            .withColumn("snapshot_cadence_days", F.lit(sample_days)))

    return snap.select("snapshot_date", "facility_id", "district_id",
                       "iv_saline_units", "ns1_kits", "ors_sachets",
                       "antimalarial_doses", "occupied_beds",
                       "occupied_icu_beds", "total_beds", "icu_beds",
                       "snapshot_cadence_days")
