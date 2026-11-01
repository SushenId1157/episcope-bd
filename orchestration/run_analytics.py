"""
EpiScope - run detection, forecasting, allocation and the calendar
(AN-1 to AN-9).

Reads gold, writes analytic tables back into gold, and records every measured
result in docs/experiments/ so the report draws on runs rather than claims.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import pandas as pd


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="./episcope_data")
    p.add_argument("--public", default="./episcope_public")
    p.add_argument("--skip", nargs="*", default=[],
                   choices=["detection", "forecast", "allocation", "calendar"])
    a = p.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)

    from pyspark.sql import functions as F
    from orchestration.run_stage import build_spark, _ship_modules

    import generator.config as cfg
    import analytics.detection.ears as ears
    import analytics.evaluation.detection_eval as ev
    import analytics.forecast.backtest as bt
    import analytics.optimise.allocation as alloc
    import analytics.calendar.preparedness as prep
    import pipeline.gold.build as gb

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    _ship_modules(spark, root)

    gold = os.path.join(a.data, "lake", "gold")
    exp = os.path.join(root, "docs", "experiments")
    os.makedirs(exp, exist_ok=True)
    times = {}

    wk = spark.read.parquet(os.path.join(gold, "weekly_incidence"))
    all_weeks = wk.select("epi_year", "epi_week").distinct()

    # ---------------------------------------------------- detection AN-1/2/3
    if "detection" not in a.skip:
        t = time.time()
        print("detection ...")
        det = ears.detect_seasonal(wk, all_weeks, "c2", ears.OPERATING_K,
                                   cfg.SURVEILLANCE_DISEASES)
        det.write.mode("overwrite").partitionBy("epi_year") \
           .option("compression", "snappy") \
           .parquet(os.path.join(gold, "detection_alerts"))

        gt_path = os.path.join(a.data, "lake", "bronze",
                               "_eval_only_ground_truth")
        if os.path.isdir(gt_path):
            truth = spark.read.parquet(gt_path)
            d2 = spark.read.parquet(os.path.join(gold, "detection_alerts"))
            m = ev.evaluate(spark, d2, truth, cfg.SURVEILLANCE_DISEASES)
            sweep = ev.threshold_sweep(spark, wk, all_weeks, truth,
                                       ears.detect_seasonal, "c2",
                                       [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0],
                                       cfg.SURVEILLANCE_DISEASES)
            comparison = []
            for v in ["c1", "c2", "c3"]:
                dv = ears.detect_seasonal(wk, all_weeks, v, ears.OPERATING_K,
                                          cfg.SURVEILLANCE_DISEASES)
                mm = ev.evaluate(spark, dv, truth, cfg.SURVEILLANCE_DISEASES)
                mm["variant"] = f"EARS-{v.upper()} seasonal"
                comparison.append(mm)
            dp = ears.detect(wk, all_weeks, "c2", ears.OPERATING_K,
                             cfg.SURVEILLANCE_DISEASES)
            mp = ev.evaluate(spark, dp, truth, cfg.SURVEILLANCE_DISEASES)
            mp["variant"] = "EARS-C2 plain (no seasonal)"
            comparison.append(mp)

            json.dump({"operating_point": m, "operating_k": ears.OPERATING_K,
                       "min_alert_cases": ears.MIN_ALERT_CASES,
                       "threshold_sweep": sweep,
                       "method_comparison": comparison},
                      open(os.path.join(exp, "detection_eval.json"), "w"),
                      indent=2, default=str)
            print(f"  sensitivity {m['sensitivity']*100:.1f}%, "
                  f"{m['false_alerts_per_district_year']} false alerts "
                  f"per district-year")
        else:
            print("  ground truth absent; detection ran, evaluation skipped")
        times["detection"] = round(time.time() - t, 1)

    # ------------------------------------------------------ forecast AN-4/5
    if "forecast" not in a.skip:
        t = time.time()
        print("forecast backtest ...")
        feat = spark.read.parquet(os.path.join(gold, "weekly_features"))
        series = bt.prepare_series(feat, cfg.FORECAST_DISEASES)
        weeks = sorted([(r["epi_year"], r["epi_week"]) for r in
                        series.select("epi_year", "epi_week").distinct()
                        .collect()])
        origins = weeks[-57:-5]
        fc = bt.run_backtest(spark, series, origins, horizons=(1, 2, 3, 4))
        fc.write.mode("overwrite").option("compression", "snappy") \
          .parquet(os.path.join(gold, "forecast_backtest"))
        scored = bt.score(spark.read.parquet(
            os.path.join(gold, "forecast_backtest")))
        summ = bt.summarise(scored)
        json.dump({"origins": len(origins), "horizons": [1, 2, 3, 4],
                   "diseases": cfg.FORECAST_DISEASES,
                   "by_disease_horizon": [r.asDict() for r in summ.collect()]},
                  open(os.path.join(exp, "forecast_backtest.json"), "w"),
                  indent=2, default=str)
        times["forecast"] = round(time.time() - t, 1)
        print(f"  {len(origins)} origins, {fc.count():,} forecasts")

    # ---------------------------------------------------- allocation AN-6/7/8
    if "allocation" not in a.skip:
        t = time.time()
        print("allocation ...")
        res = _run_allocation(spark, gold, a.data, exp, alloc, cfg, F)
        times["allocation"] = round(time.time() - t, 1)
        print(f"  solver {res['status']}, "
              f"{res['improvement_pct']:.1f}% less unmet than the baseline")

    # ------------------------------------------------------- calendar AN-9
    if "calendar" not in a.skip:
        t = time.time()
        print("preparedness calendar ...")
        cal = _run_calendar(spark, gold, root, prep, cfg, F)
        times["calendar"] = round(time.time() - t, 1)
        print(f"  {len(cal)} entries across "
              f"{cal['division'].nunique()} divisions")

    gb.register_catalog(spark, gold,
                        [t for t in os.listdir(gold) if not t.startswith("_")])
    json.dump(times, open(os.path.join(exp, "analytics_timings.json"), "w"),
              indent=2)
    print("\nanalytics complete:", times)
    return 0


def _run_allocation(spark, gold, data, exp, alloc, cfg, F):
    """Demand conversion, the LP, and the efficiency-equity sweep."""
    import pandas as pd

    fc = spark.read.parquet(os.path.join(gold, "forecast_backtest"))
    latest = fc.agg(F.max(F.col("origin_year") * 53
                          + F.col("origin_week"))).first()[0]
    period = (fc.filter((F.col("origin_year") * 53
                         + F.col("origin_week")) == latest)
              .groupBy("district_id", "district_name", "division",
                       "disease_group")
              .agg(F.sum("forecast").alias("forecast"),
                   F.sum("upper_80").alias("upper_80")))
    demand = alloc.convert_to_demand(period.toPandas())
    demand = demand[demand["basis"] == "planning"].copy()

    pops = (spark.read.parquet(os.path.join(gold, "weekly_incidence"))
            .groupBy("district_id")
            .agg(F.max("population").cast("double").alias("population"))
            .toPandas())
    demand = demand.merge(pops, on="district_id", how="left")

    RES = alloc.DECISION_RESOURCES
    for r in RES:
        if r not in demand.columns:
            demand[r] = 0.0

    stock_raw = (spark.read.parquet(os.path.join(gold, "district_stock_weekly"))
                 .groupBy("district_id")
                 .agg(*[F.round(F.avg(c), 0).alias(c) for c in RES])
                 .toPandas())
    stock_raw = demand[["district_id"]].merge(stock_raw, on="district_id",
                                              how="left").fillna(0.0)
    stock = alloc.build_scarcity_scenario(stock_raw, RES)

    sev = demand.set_index("district_id")["admissions"]
    sev = (sev / sev.mean()).clip(0.5, 3.0).to_dict()
    gap = sum(alloc.UNIT_COST[r] * max(0.0, demand[r].sum() - stock[r].sum())
              for r in RES)
    div_need = (demand.groupby("division")
                .apply(lambda g: sum(alloc.TRANSPORT_VOLUME[r] * g[r].sum()
                                     for r in RES), include_groups=False))
    transport = (div_need * 0.85).to_dict()

    RECOMMENDED_ALPHA = 0.25
    res = alloc.optimise_allocation(demand, stock, gap * 0.60, RES,
                                    severity_weight=sev,
                                    equity_alpha=RECOMMENDED_ALPHA,
                                    transport_cap=transport)
    base = alloc.population_proportional_baseline(demand, stock, gap * 0.60, RES)
    a_opt, a_base = res["allocation"], base["allocation"]
    improve = 100 * (1 - a_opt["unmet"].sum() / max(1, a_base["unmet"].sum()))

    plan = a_opt.merge(demand[["district_id", "district_name"]],
                       on="district_id")
    os.makedirs(os.path.join(data, "raw_synthetic"), exist_ok=True)
    plan.to_csv(os.path.join(data, "raw_synthetic", "allocation_plan.csv"),
                index=False)

    rows = []
    for bl in [0.25, 0.50, 0.75, 1.00, 1.25]:
        for al in [0.0, 0.25, 0.50, 0.75]:
            r = alloc.optimise_allocation(demand, stock, gap * bl, RES,
                                          severity_weight=sev, equity_alpha=al,
                                          transport_cap=transport)
            d = r["allocation"]
            served = int((d.groupby("district_id")["allocated"].sum()
                          > 0.5).sum())
            by = d.groupby("district_id")["allocated"].sum().sort_values()
            v = by.to_numpy()
            n = len(v)
            gini = ((2 * pd.Series(v).rank().to_numpy() - n - 1) * v).sum() \
                / (n * max(v.sum(), 1e-9))
            rows.append({"budget_level": bl, "alpha": al,
                         "status": r["status"],
                         "total_unmet": float(d["unmet"].sum()),
                         "districts_served": served, "gini": float(gini),
                         "budget_used_pct": r["budget_utilisation"] * 100})
    pd.DataFrame(rows).to_csv(os.path.join(exp, "efficiency_equity.csv"),
                              index=False)

    out = {"status": res["status"], "solver": res.get("solver"),
           "recommended_alpha": RECOMMENDED_ALPHA,
           "budget_bdt": gap * 0.60, "full_gap_bdt": gap,
           "unmet_optimiser": float(a_opt["unmet"].sum()),
           "unmet_baseline": float(a_base["unmet"].sum()),
           "improvement_pct": float(improve),
           "usable_fraction": alloc.USABLE_FRACTION,
           "conversion_assumptions": alloc.CONVERSION,
           "unit_costs": alloc.UNIT_COST}
    json.dump(out, open(os.path.join(exp, "allocation.json"), "w"),
              indent=2, default=str)
    return out


def _run_calendar(spark, gold, root, prep, cfg, F):
    """Seasonal decomposition into a per-division preparedness calendar."""
    import pandas as pd

    wk = spark.read.parquet(os.path.join(gold, "weekly_incidence"))
    tot = (wk.filter(F.col("disease_group").isin(cfg.SURVEILLANCE_DISEASES))
           .groupBy("division", "disease_group")
           .agg(F.sum("cases").alias("cases")).toPandas())
    pops = (wk.select("division", "district_id", "population").distinct()
            .groupBy("division").agg(F.sum("population").alias("pop"))
            .toPandas())
    tot = tot.merge(pops, on="division", how="left")
    nat = tot.groupby("disease_group")["cases"].sum()
    tot["national_share"] = tot.apply(
        lambda r: r["cases"] / nat[r["disease_group"]], axis=1)
    tot["incidence"] = tot["cases"] / tot["pop"] * 100000
    mean_inc = tot.groupby("disease_group")["incidence"].mean()
    tot["local_salience"] = tot.apply(
        lambda r: r["incidence"] / mean_inc[r["disease_group"]], axis=1)

    eligible = {(r["disease_group"], r["division"]) for _, r in tot.iterrows()
                if r["national_share"] >= 0.08 and r["local_salience"] >= 0.60}

    weekly = (wk.filter(F.col("disease_group").isin(cfg.SURVEILLANCE_DISEASES))
              .select("division", "disease_group", "epi_year", "epi_week",
                      "cases").toPandas())
    cal = prep.build_calendar(weekly, cfg.SURVEILLANCE_DISEASES)
    cal = cal[cal.apply(lambda r: (r["disease_group"], r["division"])
                        in eligible, axis=1)].reset_index(drop=True)
    cal = cal.merge(tot[["division", "disease_group", "national_share",
                         "local_salience"]],
                    on=["division", "disease_group"], how="left")

    cal.to_csv(os.path.join(root, "docs", "experiments",
                            "preparedness_calendar.csv"), index=False)
    if len(cal):
        spark.createDataFrame(cal).write.mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(os.path.join(gold, "preparedness_calendar"))
    return cal


if __name__ == "__main__":
    raise SystemExit(main())
