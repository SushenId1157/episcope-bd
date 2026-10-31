"""
EpiScope - outbreak detection with the EARS family (AN-1).

Method choice, as the assignment requires it to be justified:

EARS over Farrington. Farrington Flexible fits a GLM on five years of matching
historical weeks. Our study window is 2021-2025, so for every district in 2021
there is no history at all, and detection would be blind over a fifth of the
data. EARS uses a short sliding baseline and produces an alert from its eighth
week onward, which is also why WHO IDSR recommends it where historical
surveillance data is thin.

EARS over CUSUM as the primary method. CUSUM accumulates deviation and is
better at slow drift, but it carries state across weeks and needs a reference
value k chosen per series. EARS C3 provides a comparable accumulating behaviour
with one fewer parameter, and C1/C2/C3 share a baseline so the three can be
compared on equal terms (AN-3).

C2 as the operating variant. C1's baseline is the seven weeks immediately
before the test week, so a slowly growing outbreak contaminates its own
baseline: last week's elevated count raises the threshold this week. C2 leaves
a two-week guard band, which matters here because the injected slow-growing
outbreaks run for 18-26 weeks at modest multipliers.

The formula is deliberately simple enough to explain to a district health
officer in one sentence, which is the interpretability side of conflict C5:
alert when this week exceeds the recent average by k standard deviations.

    C1: baseline weeks t-1 .. t-7
    C2: baseline weeks t-3 .. t-9     (two-week guard band)
    C3: C2's statistic, accumulated over three consecutive weeks

This module must never read ground_truth_outbreaks (Section 5.2). Evaluation
against ground truth lives in analytics/evaluation/.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


# Minimum baseline standard deviation. With small counts the sample standard
# deviation can collapse to near zero, which would make any uptick exceed the
# threshold and flood the system with alerts. Floor it at 1 case, the smallest
# meaningful unit of variation in a count series.
MIN_SD = 1.0

# Minimum baseline mean below which no alert is raised. A district-disease
# series averaging under this is too sparse for the normal approximation EARS
# relies on, and alerting on it would be noise. Stated explicitly rather than
# hidden, because it affects sensitivity and belongs in the evaluation.
MIN_BASELINE_MEAN = 2.0


def _ordered_window(lag_start: int, lag_end: int) -> Window:
    """
    A window over the weeks from `lag_start` to `lag_end` weeks back.

    Rows are ordered by a continuous week index rather than by (year, week),
    so the baseline crosses year boundaries correctly instead of resetting
    every January.
    """
    return (Window
            .partitionBy("district_id", "disease_group")
            .orderBy("week_index")
            .rowsBetween(-lag_end, -lag_start))


def add_week_index(df: DataFrame) -> DataFrame:
    """
    Add a gap-free, strictly increasing week index per series.

    Weeks with no cases are absent from the gold table, and a window defined by
    row offsets would then silently reach further back in time than intended.
    Densifying first is what keeps 'the previous seven weeks' literally true.
    """
    return df.withColumn("week_index",
                         F.col("epi_year") * F.lit(53) + F.col("epi_week"))


def densify(df: DataFrame, all_weeks: DataFrame) -> DataFrame:
    """
    Expand the series so every district-disease has a row for every week.

    all_weeks must carry epi_year, epi_week. Missing weeks become zero cases,
    which is the correct reading: the gold table omits a week only when no
    case was recorded, and a surveillance system treats that as zero, not as
    unknown.
    """
    keys = df.select("district_id", "district_name", "division",
                     "disease_group", "population").distinct()
    grid = keys.crossJoin(F.broadcast(all_weeks))
    out = (grid.join(df.select("district_id", "disease_group", "epi_year",
                               "epi_week", "cases", "admissions"),
                     ["district_id", "disease_group", "epi_year", "epi_week"],
                     "left")
              .withColumn("cases", F.coalesce(F.col("cases"), F.lit(0)))
              .withColumn("admissions",
                          F.coalesce(F.col("admissions"), F.lit(0))))
    return out


def ears_scores(df: DataFrame) -> DataFrame:
    """
    Compute the C1, C2 and C3 statistics for every district-disease-week.

    Everything here is a Spark window expression, so the whole calculation
    runs in the JVM over the 82k-row gold table in seconds, with no Python
    UDF and no per-group collection to the driver (ING-2).
    """
    df = add_week_index(df)

    w1 = _ordered_window(1, 7)    # C1: the seven weeks immediately before
    w2 = _ordered_window(3, 9)    # C2: seven weeks, with a two-week guard band

    df = (df
          .withColumn("baseline_mean_c1", F.avg("cases").over(w1))
          .withColumn("baseline_sd_c1", F.stddev("cases").over(w1))
          .withColumn("baseline_n_c1", F.count("cases").over(w1))
          .withColumn("baseline_mean_c2", F.avg("cases").over(w2))
          .withColumn("baseline_sd_c2", F.stddev("cases").over(w2))
          .withColumn("baseline_n_c2", F.count("cases").over(w2)))

    for v in ("c1", "c2"):
        sd = F.greatest(F.coalesce(F.col(f"baseline_sd_{v}"), F.lit(0.0)),
                        F.lit(MIN_SD))
        df = df.withColumn(f"sd_{v}", sd)
        # The EARS statistic: how many standard deviations above baseline.
        df = df.withColumn(
            f"stat_{v}",
            F.when(F.col(f"baseline_n_{v}") < 5, F.lit(None).cast("double"))
             .when(F.col(f"baseline_mean_{v}") < MIN_BASELINE_MEAN,
                   F.lit(None).cast("double"))
             .otherwise((F.col("cases") - F.col(f"baseline_mean_{v}")) / sd))

    # C3 sums the C2 statistic over this week and the two before it, so a
    # series that stays modestly elevated triggers even when no single week
    # crosses the threshold on its own. Negative contributions are clipped to
    # zero so that quiet weeks cannot cancel out a genuine rise.
    w3 = (Window.partitionBy("district_id", "disease_group")
          .orderBy("week_index").rowsBetween(-2, 0))
    df = df.withColumn("stat_c2_pos",
                       F.greatest(F.coalesce(F.col("stat_c2"), F.lit(0.0)),
                                  F.lit(0.0)))
    df = df.withColumn("stat_c3", F.sum("stat_c2_pos").over(w3))
    df = df.withColumn("stat_c3",
                       F.when(F.col("stat_c2").isNull(),
                              F.lit(None).cast("double"))
                        .otherwise(F.col("stat_c3")))

    return df.drop("stat_c2_pos")


def apply_threshold(df: DataFrame, variant: str = "c2",
                    k: float = 3.0) -> DataFrame:
    """
    Turn a statistic into an alert flag at threshold k.

    k is the operating point of conflict C1 (sensitivity against alert
    fatigue). It is a parameter, not a constant, precisely so the trade-off
    curve in AN-2 can be produced by sweeping it.

    C3 accumulates three weeks, so the equivalent threshold is scaled; without
    this a single k would mean something different for C3 than for C1 and C2.
    """
    stat = F.col(f"stat_{variant}")
    kk = F.lit(k * 2.0 if variant == "c3" else k)
    return (df
            .withColumn("alert_threshold", kk)
            .withColumn("alert",
                        F.when(stat.isNull(), F.lit(False))
                         .otherwise(stat >= kk))
            .withColumn("alert_method", F.lit(f"EARS-{variant.upper()}"))
            .withColumn("alert_k", F.lit(float(k))))


def detect(gold_weekly: DataFrame, all_weeks: DataFrame,
           variant: str = "c2", k: float = 3.0,
           diseases: list[str] | None = None) -> DataFrame:
    """Run detection end to end and return one row per series-week."""
    df = gold_weekly
    if diseases:
        df = df.filter(F.col("disease_group").isin(diseases))
    dense = densify(df, all_weeks)
    scored = ears_scores(dense)
    return apply_threshold(scored, variant, k)


# ---------------------------------------------------------------------------
# Seasonal adjustment
# ---------------------------------------------------------------------------
# Plain EARS compares a week against the seven weeks before it, which assumes
# the series is flat in the absence of an outbreak. Dengue is not flat: cases
# climb roughly twentyfold between week 10 and week 38 every single year. The
# climb itself then reads as an aberration, and at k=3 the detector raised
# 7,502 alerts against 34 injected outbreaks - about 23 per district-year, or
# one every fortnight. That is precisely the alert fatigue district officers
# named as their main concern, and it would make the system unusable.
#
# The fix is to score the deviation from what this week normally looks like,
# not from last month. For each district, disease and week-of-year we take the
# median count in that same week across the other study years, and test the
# ratio of the observed count to that seasonal expectation. A monsoon peak
# that matches previous monsoons is then unremarkable, while the same count
# arriving in February is not.
#
# Using the median rather than the mean matters: an injected outbreak in one
# year would drag a mean upward and raise the bar for every other year, so the
# detector would be partly blinded by the very events it is meant to find.
#
# Leakage: the seasonal profile is computed from the full study period, which
# is legitimate for retrospective detection evaluation but would not be
# available in real time. The forecasting module (AN-4/AN-5) uses strictly
# backward-looking features instead, and the report states this distinction.

def seasonal_baseline(df: DataFrame) -> DataFrame:
    """
    Attach the seasonal expectation for each district, disease and week-of-year.

    Returns the input with `seasonal_median` and `seasonal_ratio` added. The
    ratio is the observed count over the seasonal expectation, floored so that
    very small expectations cannot produce enormous ratios.
    """
    # Leave-one-year-out. Pooling all years lets a week set its own
    # expectation: with five study years the median of five values is often
    # one of them, so the observed week matched its own baseline exactly and
    # scored a ratio of 1.00 however extreme it was. Dhaka week 36 of 2023
    # showed 4,036 cases against an expectation of 4,036, and a genuine
    # outbreak week in Feni scored 1.86 instead of alerting.
    #
    # Excluding the year under test removes that circularity: each week is
    # compared only with the same week in other years. This is the seasonal
    # analogue of not training a model on its own test set.
    others = (df.select(F.col("district_id").alias("_d"),
                        F.col("disease_group").alias("_g"),
                        F.col("epi_week").alias("_w"),
                        F.col("epi_year").alias("_y"),
                        F.col("cases").alias("_c")))

    out = (df.join(others,
                   (F.col("district_id") == F.col("_d"))
                   & (F.col("disease_group") == F.col("_g"))
                   & (F.col("epi_week") == F.col("_w"))
                   & (F.col("epi_year") != F.col("_y")), "left")
             .groupBy("district_id", "district_name", "division",
                      "disease_group", "epi_year", "epi_week", "population",
                      "cases", "admissions", "week_index")
             .agg(F.expr("percentile_approx(_c, 0.5)").alias("seasonal_median"),
                  F.avg("_c").alias("seasonal_mean"),
                  F.count("_c").alias("seasonal_n_years")))

    # Floor the expectation at 1 case: below that the ratio stops being
    # informative and starts amplifying noise.
    expected = F.greatest(F.coalesce(F.col("seasonal_median"), F.lit(0.0)),
                          F.lit(1.0))
    return out.withColumn("seasonal_expected", expected) \
              .withColumn("seasonal_ratio", F.col("cases") / expected)


def ears_scores_seasonal(df: DataFrame) -> DataFrame:
    """
    EARS computed on the seasonally adjusted series.

    The same C1/C2/C3 machinery is applied to the ratio of observed cases to
    the seasonal expectation, so the statistic answers 'is this week unusual
    for this week of the year', rather than 'is this week higher than last
    month'.
    """
    df = add_week_index(df)
    df = seasonal_baseline(df)

    w1 = _ordered_window(1, 7)
    w2 = _ordered_window(3, 9)

    df = (df
          .withColumn("baseline_mean_c1", F.avg("seasonal_ratio").over(w1))
          .withColumn("baseline_sd_c1", F.stddev("seasonal_ratio").over(w1))
          .withColumn("baseline_n_c1", F.count("seasonal_ratio").over(w1))
          .withColumn("baseline_mean_c2", F.avg("seasonal_ratio").over(w2))
          .withColumn("baseline_sd_c2", F.stddev("seasonal_ratio").over(w2))
          .withColumn("baseline_n_c2", F.count("seasonal_ratio").over(w2)))

    for v in ("c1", "c2"):
        # The ratio series sits around 1.0, so its natural scale is much
        # smaller than a count series; the floor is set accordingly.
        sd = F.greatest(F.coalesce(F.col(f"baseline_sd_{v}"), F.lit(0.0)),
                        F.lit(0.15))
        df = df.withColumn(f"sd_{v}", sd)
        df = df.withColumn(
            f"stat_{v}",
            F.when(F.col(f"baseline_n_{v}") < 5, F.lit(None).cast("double"))
             .when(F.col("seasonal_expected") < MIN_BASELINE_MEAN,
                   F.lit(None).cast("double"))
             .otherwise((F.col("seasonal_ratio")
                         - F.col(f"baseline_mean_{v}")) / sd))

    w3 = (Window.partitionBy("district_id", "disease_group")
          .orderBy("week_index").rowsBetween(-2, 0))
    df = df.withColumn("stat_c2_pos",
                       F.greatest(F.coalesce(F.col("stat_c2"), F.lit(0.0)),
                                  F.lit(0.0)))
    df = df.withColumn("stat_c3", F.sum("stat_c2_pos").over(w3))
    df = df.withColumn("stat_c3",
                       F.when(F.col("stat_c2").isNull(),
                              F.lit(None).cast("double"))
                        .otherwise(F.col("stat_c3")))
    return df.drop("stat_c2_pos")


def detect_seasonal(gold_weekly: DataFrame, all_weeks: DataFrame,
                    variant: str = "c2", k: float = 3.0,
                    diseases: list[str] | None = None) -> DataFrame:
    """Seasonally adjusted detection, the operating configuration."""
    df = gold_weekly
    if diseases:
        df = df.filter(F.col("disease_group").isin(diseases))
    dense = densify(df, all_weeks)
    scored = ears_scores_seasonal(dense)
    return apply_threshold(scored, variant, k)
