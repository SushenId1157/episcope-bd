"""
EpiScope - export gold to the suppressed JSON the dashboard reads
(VIS-4, PRV-2).

Two obligations: the browser payload stays under 10 MB and carries only
aggregates, and no count between one and four reaches it. Both are checked
here rather than assumed, and the check runs over the written files, because
suppression that is correct in intent can still fail in serialisation.

Exits non-zero if either obligation is breached, so a broken export fails the
build instead of shipping quietly.
"""
from __future__ import annotations

import argparse
import json
import os
import sys


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="./episcope_data")
    p.add_argument("--public", default="./episcope_public")
    a = p.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)

    import pandas as pd
    from pyspark.sql import functions as F
    from orchestration.run_stage import build_spark, _ship_modules
    import generator.config as cfg
    import serving.export as ex

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    _ship_modules(spark, root)

    gold = os.path.join(a.data, "lake", "gold")
    out = os.path.join(root, "dashboard", "data")
    exp = os.path.join(root, "docs", "experiments")
    os.makedirs(out, exist_ok=True)

    DIS = cfg.SURVEILLANCE_DISEASES
    wk = (spark.read.parquet(os.path.join(gold, "weekly_incidence"))
          .filter(F.col("disease_group").isin(DIS)))
    latest = wk.agg(F.max("epi_year")).first()[0]
    manifest = []

    # ------------------------------------------------------------ view 1
    alerts = spark.read.parquet(os.path.join(gold, "detection_alerts"))
    ac = (alerts.filter(F.col("alert") & (F.col("epi_year") == latest))
          .groupBy("district_id", "disease_group")
          .agg(F.count("*").alias("alert_weeks"),
               F.round(F.max("stat_c2"), 2).alias("max_severity"),
               F.max("epi_week").alias("last_alert_week")))
    m = (wk.filter(F.col("epi_year") == latest)
         .groupBy("district_id", "district_name", "division", "disease_group")
         .agg(F.sum("cases").alias("cases"),
              F.sum("admissions").alias("admissions"),
              F.max("population").alias("population"),
              F.max("lat").alias("lat"), F.max("lon").alias("lon"))
         .join(ac, ["district_id", "disease_group"], "left")
         .fillna({"alert_weeks": 0, "max_severity": 0.0,
                  "last_alert_week": 0}).toPandas())
    m["incidence_per_100k"] = (m["cases"] / m["population"] * 100000).round(1)
    m = ex.suppress_frame(m, ["cases", "admissions"],
                          group_cols=["disease_group"])
    # A rate derived from a suppressed count would reconstruct it, so it goes
    # with the count rather than being published alone.
    m.loc[m["cases"].isna(), "incidence_per_100k"] = None
    manifest.append(ex.write_json(
        {"year": int(latest), "districts": m.to_dict("records")},
        os.path.join(out, "map_districts.json"), places=1))

    # ------------------------------------------------------------ view 2
    nat = (wk.groupBy("disease_group", "epi_year", "epi_week")
           .agg(F.sum("cases").alias("cases"),
                F.sum("admissions").alias("admissions"))
           .orderBy("disease_group", "epi_year", "epi_week").toPandas())
    div = (wk.groupBy("division", "disease_group", "epi_year", "epi_week")
           .agg(F.sum("cases").alias("cases"))
           .orderBy("division", "disease_group", "epi_year", "epi_week")
           .toPandas())
    div = ex.suppress_frame(div, ["cases"])
    fc = spark.read.parquet(os.path.join(gold, "forecast_backtest"))
    fcn = (fc.filter(F.col("horizon") == 1)
           .groupBy("disease_group", "target_year", "target_week")
           .agg(F.round(F.sum("actual"), 0).alias("actual"),
                F.round(F.sum("forecast"), 0).alias("forecast"),
                F.round(F.sum("lower_80"), 0).alias("lower_80"),
                F.round(F.sum("upper_80"), 0).alias("upper_80"),
                F.round(F.sum("baseline_naive"), 0).alias("naive"))
           .orderBy("disease_group", "target_year", "target_week").toPandas())
    manifest.append(ex.write_json(
        {"national": nat.to_dict("records"),
         "divisional": div.to_dict("records"),
         "forecast": fcn.to_dict("records")},
        os.path.join(out, "timeseries.json"), places=0))

    # ------------------------------------------------------------ view 3
    heat = (wk.groupBy("district_id", "district_name", "division",
                       "disease_group", "epi_week")
            .agg(F.round(F.avg("cases"), 1).alias("mean_cases"),
                 F.round(F.avg("incidence_per_100k"), 2).alias("mean_incidence"))
            .orderBy("district_id", "disease_group", "epi_week").toPandas())
    heat = ex.suppress_frame(heat, ["mean_cases"])
    heat.loc[heat["mean_cases"].isna(), "mean_incidence"] = None
    manifest.append(ex.write_json(heat.to_dict("records"),
                                  os.path.join(out, "seasonal_heatmap.json"),
                                  places=2))

    # ------------------------------------------------------------ view 4
    plan_path = os.path.join(a.data, "raw_synthetic", "allocation_plan.csv")
    if os.path.exists(plan_path):
        plan = pd.read_csv(plan_path)
        av = (plan.groupby(["district_id", "district_name", "division",
                            "resource"], as_index=False)
              [["demand", "stock", "allocated", "unmet"]].sum().round(0))
        ee_path = os.path.join(exp, "efficiency_equity.csv")
        ee = pd.read_csv(ee_path) if os.path.exists(ee_path) else pd.DataFrame()
        alloc_meta = {}
        am = os.path.join(exp, "allocation.json")
        if os.path.exists(am):
            alloc_meta = json.load(open(am))
        manifest.append(ex.write_json(
            {"allocation": av.to_dict("records"),
             "efficiency_equity": ee.to_dict("records") if len(ee) else [],
             "recommended_alpha": alloc_meta.get("recommended_alpha", 0.25),
             "solver_status": alloc_meta.get("status")},
            os.path.join(out, "allocation.json"), places=0))

    # ------------------------------------------------------------ view 5
    ew = (alerts.filter(F.col("alert") & (F.col("epi_year") == latest))
          .select("district_id", "district_name", "division", "disease_group",
                  "epi_year", "epi_week", "cases",
                  F.round("seasonal_expected", 0).alias("expected"),
                  F.round("stat_c2", 2).alias("severity"))
          .orderBy(F.desc("severity")).limit(300).toPandas())
    ew = ex.suppress_frame(ew, ["cases", "expected"])
    manifest.append(ex.write_json(ew.to_dict("records"),
                                  os.path.join(out, "early_warning.json"),
                                  places=2))

    # --------------------------------------------------------- supporting
    dq = spark.read.parquet(os.path.join(gold, "data_quality_report")).toPandas()
    manifest.append(ex.write_json({"rules": dq.to_dict("records")},
                                  os.path.join(out, "data_quality.json"),
                                  places=4))
    cal_path = os.path.join(exp, "preparedness_calendar.csv")
    if os.path.exists(cal_path):
        cal = pd.read_csv(cal_path)
        manifest.append(ex.write_json(
            cal.to_dict("records"),
            os.path.join(out, "preparedness_calendar.json"), places=2))

    det_meta = {}
    dp = os.path.join(exp, "detection_eval.json")
    if os.path.exists(dp):
        d = json.load(open(dp))
        o = d["operating_point"]
        det_meta = {"method": "EARS-C2, seasonally adjusted",
                    "k": d.get("operating_k"),
                    "sensitivity": o["sensitivity"],
                    "false_alerts_per_district_year":
                        o["false_alerts_per_district_year"],
                    "median_days_to_detection": o["median_days_to_detection"]}
    manifest.append(ex.write_json(
        {"generated_at_utc": pd.Timestamp.utcnow().isoformat(),
         "latest_year": int(latest), "diseases": DIS,
         "detection": det_meta,
         "privacy": {"rule": "PRV-2 small-cell suppression",
                     "threshold": ex.SUPPRESSION_THRESHOLD,
                     "note": "counts of 1 to 4 are withheld, a second cell is "
                             "withheld where a total would otherwise reveal "
                             "the first, and rates derived from a suppressed "
                             "count are withheld with it"}},
        os.path.join(out, "meta.json")))

    # ------------------------------------------------------------- checks
    geo = os.path.join(out, "districts.geojson")
    if os.path.exists(geo):
        manifest.append({"name": "districts.geojson",
                         "bytes": os.path.getsize(geo),
                         "kb": round(os.path.getsize(geo) / 1024, 1)})

    budget = ex.check_payload_budget(manifest)
    print("\npayload:")
    for mf in sorted(manifest, key=lambda x: -x["bytes"]):
        print(f"   {mf['name']:<32}{mf['kb']:>10,.1f} KB")
    print(f"   {'TOTAL':<32}{budget['total_mb']:>10.3f} MB "
          f"(limit {budget['budget_mb']})")

    violations = 0
    for mf in manifest:
        if not mf["name"].endswith(".json"):
            continue
        obj = json.load(open(os.path.join(out, mf["name"])))
        bad = ex.audit_no_small_counts(obj, mf["name"])
        if bad:
            violations += len(bad)
            print(f"   PRV-2 VIOLATION in {mf['name']}: {bad[:3]}")

    json.dump({"manifest": manifest, "budget": budget,
               "prv2_violations": violations},
              open(os.path.join(exp, "serving_payload.json"), "w"),
              indent=2, default=str)

    if violations:
        print(f"\nFAILED: {violations} PRV-2 violations in the export")
        return 1
    if not budget["within_budget"]:
        print(f"\nFAILED: payload {budget['total_mb']} MB exceeds the limit")
        return 1
    print("\nserving export clean: no small counts, within the payload budget")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
