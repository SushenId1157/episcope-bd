"""
EpiScope - end-to-end pipeline runner (ENG-1, ENG-2).

One entry point rebuilds the whole lake from the committed code plus the public
sources: generator, bronze, silver, gold. It is what `make all` calls, and what
the instructor's clean clone will run.

Every stage is timed and the timings are written to docs/experiments/, so the
scalability table in ENG-2 is a record of measured runs rather than a claim.
Stages are skippable, so a rebuild after a lost session does not have to repeat
work that is still on disk.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone


def run_all(spark, project_root: str, public_root: str, data_root: str,
            scale_factor: float = 1.0, seed: int | None = None,
            skip: tuple[str, ...] = ()) -> dict:
    """Rebuild the lake end to end. Returns per-stage timings."""
    import sys
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    import generator.config as cfg
    import generator.weekly_cases as gwc
    import generator.encounters as genc
    import generator.outbreaks as gob
    import generator.facilities as gfac
    import generator.stock_and_artefacts as gsa
    import pipeline.bronze.ingest as bi
    import pipeline.bronze.weather_features as wf
    import pipeline.silver.build as sb
    import pipeline.gold.build as gb

    from pyspark.sql import functions as F

    seed = seed if seed is not None else cfg.DEFAULT_SEED
    times, t_all = {}, time.time()

    raw_syn = f"{data_root}/raw_synthetic"
    bronze = f"{data_root}/lake/bronze"
    silver = f"{data_root}/lake/silver"
    gold = f"{data_root}/lake/gold"
    quar = f"{data_root}/lake/quarantine"
    for d in (raw_syn, bronze, silver, gold, quar, f"{data_root}/experiments"):
        os.makedirs(d, exist_ok=True)

    import csv
    districts = list(csv.DictReader(
        open(f"{public_root}/S1_district_reference_enriched.csv")))

    # ---------------------------------------------------------- generator
    if "generator" not in skip:
        t = time.time()
        facs = gfac.build_registry(districts, seed=seed)
        gfac.write_registry(facs, f"{raw_syn}/S5_facility_registry.csv")

        plan, truth = gob.build_outbreak_plan(
            districts, cfg.OUTBREAK_SPEC, cfg.DISEASES,
            cfg.SURVEILLANCE_DISEASES, seed, cfg.STUDY_START, cfg.STUDY_END)
        gob.write_plan(plan, f"{raw_syn}/outbreak_injection_plan.csv")
        gob.write_ground_truth(truth, f"{raw_syn}/ground_truth_outbreaks.csv")
        times["generator_setup"] = round(time.time() - t, 1)

        t = time.time()
        weather = wf.build_weather_weekly(f"{public_root}/S3_weather")
        weekly = gwc.build_weekly_cases(districts, weather, cfg.DISEASES,
                                        plan, seed=seed,
                                        scale_factor=scale_factor)
        gwc.write_weekly_cases(weekly, f"{raw_syn}/weekly_cases.csv")
        times["weekly_counts"] = round(time.time() - t, 1)

        t = time.time()
        weekly_df = spark.createDataFrame(weekly).repartition(32, "district_id")
        fac_df = spark.read.option("header", True).csv(
            f"{raw_syn}/S5_facility_registry.csv")
        dist_df = spark.read.option("header", True).csv(
            f"{public_root}/S1_district_reference_enriched.csv")
        enc = genc.build_encounters(spark, weekly_df, fac_df, dist_df,
                                    cfg.DISEASES, cfg.DEFECT_RATES,
                                    cfg.REPORTING, seed=seed)
        enc2, art_log = gsa.apply_reporting_artefacts(
            spark, enc, fac_df, cfg.REPORTING_ARTEFACTS, cfg.STUDY_START, seed)
        enc2.write.mode("overwrite").partitionBy("epi_year") \
            .option("compression", "snappy") \
            .parquet(f"{raw_syn}/S4_encounters_final")
        if art_log is not None:
            art_log.coalesce(1).write.mode("overwrite").option("header", True) \
                   .csv(f"{raw_syn}/reporting_artefacts_log")
        times["encounters"] = round(time.time() - t, 1)

        t = time.time()
        final = spark.read.parquet(f"{raw_syn}/S4_encounters_final")
        stock = gsa.build_stock_snapshots(spark, fac_df, final,
                                          cfg.STUDY_START, cfg.STUDY_END,
                                          seed, sample_days=7)
        stock.write.mode("overwrite").option("compression", "snappy") \
             .parquet(f"{raw_syn}/S6_stock_snapshots")
        times["stock"] = round(time.time() - t, 1)

    # ------------------------------------------------------------- bronze
    if "bronze" not in skip:
        t = time.time()
        csvr = lambda p: spark.read.option("header", True).csv(p)
        pqr = lambda p: spark.read.parquet(p)
        entries = []
        for sid, reader, path, table, parts in [
            ("S1", csvr, f"{public_root}/S1_district_reference_enriched.csv",
             "districts", None),
            ("S2", csvr, f"{public_root}/S2_population_adm2_raw.csv",
             "population", None),
            ("S4", pqr, f"{raw_syn}/S4_encounters_final", "encounters",
             ["epi_year"]),
            ("S5", csvr, f"{raw_syn}/S5_facility_registry.csv", "facilities",
             None),
            ("S6", pqr, f"{raw_syn}/S6_stock_snapshots", "stock_snapshots",
             None),
            ("GT", csvr, f"{raw_syn}/ground_truth_outbreaks.csv",
             "_eval_only_ground_truth", None),
            ("GT", csvr, f"{raw_syn}/reporting_artefacts_log",
             "_eval_only_artefact_log", None),
        ]:
            entries.append(bi.ingest_source(spark, sid, reader, path, bronze,
                                            table, parts))

        weather = wf.build_weather_weekly(f"{public_root}/S3_weather")
        ww = spark.createDataFrame(weather)
        batch = bi.make_batch_id("S3", "full")
        chk = bi.checksum_path(f"{public_root}/S3_weather")
        bi.write_bronze(
            bi.add_ingestion_metadata(ww, "S3", batch, "S3_weather/", chk,
                                      "full"),
            bronze, "weather_weekly", ["epi_year"])
        entries.append({"table": "weather_weekly", "source_id": "S3",
                        "batch_id": batch, "checksum": chk,
                        "rows": len(weather), "size_bytes": 0})
        bi.record_manifest(entries, bronze)
        times["bronze"] = round(time.time() - t, 1)

    # ------------------------------------------------------------- silver
    if "silver" not in skip:
        t = time.time()
        salt = sb.load_salt(f"{project_root}/.env")
        rep = sb.build_silver(spark, bronze, silver, quar, salt,
                              cfg.STUDY_START, cfg.STUDY_END)
        times["silver"] = round(time.time() - t, 1)
        json.dump(rep, open(f"{project_root}/docs/experiments/"
                            f"silver_sf{scale_factor}.json", "w"),
                  indent=2, default=str)

    # --------------------------------------------------------------- gold
    if "gold" not in skip:
        t = time.time()
        sil = spark.read.parquet(f"{silver}/encounters")
        dis = spark.read.parquet(f"{bronze}/districts")
        wx = spark.read.parquet(f"{bronze}/weather_weekly")

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        weekly_g = gb.weekly_incidence(spark, sil, dis)
        gb.write_gold(weekly_g, gold, "weekly_incidence",
                      ["epi_year", "division"])
        wk = spark.read.parquet(f"{gold}/weekly_incidence")

        gb.write_gold(gb.district_disease_stats(wk), gold,
                      "district_disease_stats", ["division"])
        gb.write_gold(gb.weather_joined(
            wk, wx.drop("_source_id", "_batch_id", "_source_file",
                        "_file_checksum", "_load_timestamp", "_ingest_mode")),
            gold, "weekly_features", ["epi_year"])

        silver_rec = json.load(open(f"{project_root}/docs/experiments/"
                                    f"silver_sf{scale_factor}.json"))
        bid = sil.select("_batch_id").first()["_batch_id"]
        gb.write_gold(gb.quality_report(
            spark, silver_rec["rules"], silver_rec["rows_in"],
            silver_rec["rows_clean"], silver_rec["rows_quarantined"], bid,
            str(sil.select("_load_timestamp").first()[0])),
            gold, "data_quality_report")

        q = spark.read.parquet(f"{quar}/encounters")
        gb.write_gold(
            q.groupBy("_quarantine_reasons", "district_id", "epi_year")
             .agg(F.count("*").alias("quarantined_rows"))
             .withColumnRenamed("_quarantine_reasons", "reason_codes"),
            gold, "quarantine_summary", ["epi_year"])

        st = spark.read.parquet(f"{bronze}/stock_snapshots")
        fc = spark.read.parquet(f"{bronze}/facilities").select(
            "facility_id", "district_id", "division")
        gb.write_gold(
            st.drop("district_id").join(F.broadcast(fc), "facility_id", "left")
              .withColumn("epi_year", F.year("snapshot_date"))
              .withColumn("epi_week", F.weekofyear("snapshot_date"))
              .groupBy("district_id", "division", "epi_year", "epi_week")
              .agg(*[F.sum(c).alias(c) for c in
                     ["iv_saline_units", "ns1_kits", "ors_sachets",
                      "antimalarial_doses", "occupied_beds", "total_beds",
                      "occupied_icu_beds", "icu_beds"]]),
            gold, "district_stock_weekly", ["epi_year"])

        gb.register_catalog(spark, gold,
                            ["weekly_incidence", "district_disease_stats",
                             "weekly_features", "data_quality_report",
                             "quarantine_summary", "district_stock_weekly"])
        times["gold"] = round(time.time() - t, 1)

    times["total"] = round(time.time() - t_all, 1)

    rec = {"scale_factor": scale_factor, "seed": seed,
           "stage_times_sec": times,
           "total_min": round(times["total"] / 60, 2),
           "measured_at_utc": datetime.now(timezone.utc).isoformat(),
           "spark_version": spark.version,
           "skipped": list(skip)}
    os.makedirs(f"{project_root}/docs/experiments", exist_ok=True)
    json.dump(rec, open(f"{project_root}/docs/experiments/"
                        f"pipeline_sf{scale_factor}.json", "w"), indent=2)
    return rec
