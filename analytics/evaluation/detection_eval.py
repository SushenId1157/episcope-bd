"""
EpiScope - evaluation of outbreak detection against ground truth (AN-2).

This module is the only place that reads ground_truth_outbreaks. Section 5.2
forbids the detection code from seeing it, so the separation is structural:
analytics/detection/ computes alerts, analytics/evaluation/ scores them.

Three metrics, as the assignment specifies:

  sensitivity              fraction of injected outbreaks with at least one
                           alert inside their window
  false alerts per
  district-year            alerts not attributable to any outbreak, divided by
                           the number of district-years observed
  median days to detection how long after onset the first alert arrived

A detection is credited to an outbreak when an alert falls in the same
district and disease, within the outbreak's window extended by a short grace
period at each end. The grace at the start exists because an outbreak's first
week is often indistinguishable from noise; the grace at the end because a
detector may fire as cases are still falling.
"""
from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F


GRACE_WEEKS_BEFORE = 1
GRACE_WEEKS_AFTER = 2


def _week_to_date(df: DataFrame, ycol: str, wcol: str,
                  out: str = "alert_date") -> DataFrame:
    """Monday of an ISO year/week, computed arithmetically (4 Jan is in W1)."""
    jan4 = F.to_date(F.concat_ws("-", F.col(ycol), F.lit("01"), F.lit("04")),
                     "yyyy-MM-dd")
    iso_dow = ((F.dayofweek(jan4) + F.lit(5)) % F.lit(7)) + F.lit(1)
    monday_w1 = F.date_sub(jan4, (iso_dow - F.lit(1)).cast("int"))
    return df.withColumn(out, F.date_add(
        monday_w1, ((F.col(wcol) - F.lit(1)) * F.lit(7)).cast("int")))


def match_alerts_to_outbreaks(alerts: DataFrame,
                              truth: DataFrame) -> DataFrame:
    """
    Join alerts to the outbreaks they could plausibly be detecting.

    Returns one row per (alert, outbreak) pair that matches on district and
    disease and falls inside the graced window. Alerts that match nothing are
    the false alerts; outbreaks that match nothing were missed.
    """
    a = _week_to_date(alerts, "epi_year", "epi_week")
    t = (truth
         .withColumn("start_date", F.to_date("start_date"))
         .withColumn("end_date", F.to_date("end_date"))
         .withColumn("win_start",
                     F.date_sub("start_date", GRACE_WEEKS_BEFORE * 7))
         .withColumn("win_end",
                     F.date_add("end_date", GRACE_WEEKS_AFTER * 7)))

    # The nationwide event is recorded once with district_id 'ALL', so it has
    # to match an alert in any district rather than one specific district.
    cond = (((t.district_id == a.district_id) | (t.district_id == F.lit("ALL")))
            & (t.disease_group == a.disease_group)
            & (a.alert_date >= t.win_start)
            & (a.alert_date <= t.win_end))

    return (a.join(t.select("outbreak_id", "district_id", "disease_group",
                            "start_date", "end_date", "win_start", "win_end",
                            "outbreak_type", "peak_multiplier",
                            F.col("district_id").alias("_t_district")),
                   cond, "left")
             .withColumn("days_from_onset",
                         F.datediff("alert_date", "start_date")))


def evaluate(spark: SparkSession, detection: DataFrame, truth: DataFrame,
             diseases: list[str] | None = None) -> dict:
    """Score one detection configuration and return its metrics."""
    det = detection
    if diseases:
        det = det.filter(F.col("disease_group").isin(diseases))

    alerts = det.filter(F.col("alert"))
    matched = match_alerts_to_outbreaks(alerts, truth).cache()

    # --- sensitivity: outbreaks with at least one alert in their window ---
    detected = (matched.filter(F.col("outbreak_id").isNotNull())
                       .select("outbreak_id").distinct())
    n_truth = truth.count()
    n_detected = detected.count()

    # --- days to detection: first alert per outbreak ---
    first = (matched.filter(F.col("outbreak_id").isNotNull()
                            & (F.col("days_from_onset") >= 0))
                    .groupBy("outbreak_id")
                    .agg(F.min("days_from_onset").alias("days_to_detect")))
    dd = [r["days_to_detect"] for r in first.collect()]
    dd.sort()
    median_days = dd[len(dd) // 2] if dd else None

    # --- false alerts: alerts matching no outbreak ---
    false_alerts = matched.filter(F.col("outbreak_id").isNull()).count()
    district_years = (det.select("district_id", "epi_year").distinct().count())
    fa_rate = false_alerts / max(1, district_years)

    total_alerts = alerts.count()
    scored_weeks = det.filter(F.col("stat_c2").isNotNull()).count()

    by_type = {r["outbreak_type"]: r["n"] for r in
               truth.join(detected, "outbreak_id", "left_semi")
                    .groupBy("outbreak_type").agg(F.count("*").alias("n"))
                    .collect()}
    truth_by_type = {r["outbreak_type"]: r["n"] for r in
                     truth.groupBy("outbreak_type")
                          .agg(F.count("*").alias("n")).collect()}

    matched.unpersist()
    return {
        "outbreaks_total": n_truth,
        "outbreaks_detected": n_detected,
        "sensitivity": round(n_detected / max(1, n_truth), 4),
        "total_alerts": total_alerts,
        "false_alerts": false_alerts,
        "district_years": district_years,
        "false_alerts_per_district_year": round(fa_rate, 3),
        "median_days_to_detection": median_days,
        "days_to_detection_all": dd,
        "scored_week_count": scored_weeks,
        "alert_rate_pct": round(100 * total_alerts / max(1, scored_weeks), 3),
        "detected_by_type": by_type,
        "truth_by_type": truth_by_type,
    }


def threshold_sweep(spark: SparkSession, gold_weekly: DataFrame,
                    all_weeks: DataFrame, truth: DataFrame, detect_fn,
                    variant: str, ks: list[float],
                    diseases: list[str]) -> list[dict]:
    """
    Sweep the alert threshold to produce the trade-off curve for conflict C1.

    Lowering k detects more outbreaks sooner and raises the false-alert rate.
    There is no single correct k: the curve exists so the operating point can
    be argued for, which is what the assignment asks the report to do.
    """
    out = []
    for k in ks:
        det = detect_fn(gold_weekly, all_weeks, variant=variant, k=k,
                        diseases=diseases).cache()
        m = evaluate(spark, det, truth, diseases)
        m["k"] = k
        m["variant"] = variant
        out.append(m)
        det.unpersist()
    return out
