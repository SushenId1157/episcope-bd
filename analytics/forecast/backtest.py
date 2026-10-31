"""
EpiScope - rolling-origin backtest and forecast scoring (AN-5).

The backtest walks an origin forward one week at a time across the last 52
weeks. At each origin the model is refitted on data up to that week only and
forecasts 1 to 4 weeks ahead. Nothing from after the origin enters the fit,
which is what makes the reported error an estimate of real forecasting skill
rather than of curve fitting.

Refitting at every origin rather than fitting once is the expensive but honest
choice: a model fitted once on all history and then evaluated on part of that
history would report an error it could never achieve in operation.

Metrics, as the assignment specifies:

  MAE                   mean absolute error, in cases
  80% PI coverage       share of actuals inside the 80% interval; a calibrated
                        model scores near 0.80. Over-coverage means intervals
                        are uselessly wide, under-coverage that they understate
                        risk, and for hospital planning the second is worse.
  WIS                   weighted interval score, the proper score recommended
                        by Bracher et al. (2021) for epidemic forecasts. It
                        rewards sharp intervals but penalises missing the
                        actual, so a model cannot win by being vague.

Skill is reported relative to the seasonal-naive baseline, because an absolute
MAE means nothing without knowing what a trivial method would have scored.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F

from analytics.forecast.models import (FORECAST_SCHEMA, forecast_one_series)


def prepare_series(gold_features: DataFrame, diseases: list[str]) -> DataFrame:
    """Select and densify the series the forecaster will train on."""
    df = gold_features.filter(F.col("disease_group").isin(diseases))
    return df.withColumn("week_index",
                         F.col("epi_year") * F.lit(53) + F.col("epi_week"))


def run_backtest(spark: SparkSession, series: DataFrame,
                 origins: list[tuple[int, int]],
                 horizons: tuple[int, ...] = (1, 2, 3, 4)) -> DataFrame:
    """
    Run the rolling-origin backtest across all district-disease series.

    Each (district, disease) group is handed to a pandas function that refits
    the model for every origin. Grouping this way means the 128 series train
    in parallel across the executors, which is what AN-4 asks for, and no
    series ever sees another's data.
    """
    origin_list = list(origins)

    def _run(pdf: pd.DataFrame) -> pd.DataFrame:
        out = []
        for (oy, ow) in origin_list:
            r = forecast_one_series(pdf, horizons, oy, ow)
            if not r.empty:
                out.append(r)
        if not out:
            return pd.DataFrame(columns=[f.name for f in FORECAST_SCHEMA.fields])
        return pd.concat(out, ignore_index=True)

    return (series.groupBy("district_id", "disease_group")
                  .applyInPandas(_run, schema=FORECAST_SCHEMA))


def _wis(actual: np.ndarray, lower: np.ndarray, upper: np.ndarray,
         median: np.ndarray, alpha: float = 0.20) -> np.ndarray:
    """
    Weighted interval score for a single central interval plus the median.

    WIS = (1/(K+0.5)) * [ 0.5*|y - m| + sum_k (alpha_k/2) * IS_k ]

    with the interval score IS = (u - l) + (2/alpha)*(l - y) if y below l,
    + (2/alpha)*(y - u) if y above u. Lower is better, and it is in the same
    units as the data, so it can be read alongside MAE.
    """
    width = upper - lower
    pen_low = (2.0 / alpha) * np.maximum(lower - actual, 0)
    pen_high = (2.0 / alpha) * np.maximum(actual - upper, 0)
    interval_score = width + pen_low + pen_high
    return (0.5 * np.abs(actual - median) + (alpha / 2.0) * interval_score) / 1.5


def score(forecasts: DataFrame) -> DataFrame:
    """Attach per-forecast error terms so they can be aggregated any way."""
    return (forecasts
            .withColumn("abs_error", F.abs(F.col("forecast") - F.col("actual")))
            .withColumn("abs_error_naive",
                        F.abs(F.col("baseline_naive") - F.col("actual")))
            .withColumn("in_interval",
                        ((F.col("actual") >= F.col("lower_80"))
                         & (F.col("actual") <= F.col("upper_80"))).cast("int"))
            .withColumn("interval_width",
                        F.col("upper_80") - F.col("lower_80"))
            .withColumn("pen_low",
                        (F.lit(2.0 / 0.20)
                         * F.greatest(F.col("lower_80") - F.col("actual"),
                                      F.lit(0.0))))
            .withColumn("pen_high",
                        (F.lit(2.0 / 0.20)
                         * F.greatest(F.col("actual") - F.col("upper_80"),
                                      F.lit(0.0))))
            .withColumn("wis",
                        (F.lit(0.5) * F.abs(F.col("actual") - F.col("forecast"))
                         + F.lit(0.20 / 2.0)
                         * (F.col("interval_width") + F.col("pen_low")
                            + F.col("pen_high"))) / F.lit(1.5)))


def summarise(scored: DataFrame, by: list[str] | None = None) -> DataFrame:
    """Aggregate the scored forecasts into the table the report carries."""
    group = by or ["disease_group", "horizon"]
    return (scored.groupBy(*group)
            .agg(F.count("*").alias("n_forecasts"),
                 F.round(F.avg("abs_error"), 2).alias("mae"),
                 F.round(F.avg("abs_error_naive"), 2).alias("mae_naive"),
                 F.round(F.avg("in_interval"), 4).alias("pi80_coverage"),
                 F.round(F.avg("wis"), 2).alias("wis"),
                 F.round(F.avg("interval_width"), 1).alias("mean_pi_width"))
            .withColumn("skill_vs_naive",
                        F.round(F.lit(1.0) - F.col("mae") / F.col("mae_naive"), 4))
            .orderBy(*group))
