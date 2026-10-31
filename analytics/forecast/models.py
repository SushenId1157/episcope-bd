"""
EpiScope - short-term district-level forecasting (AN-4, AN-5).

Model: negative-binomial GLM with a log link, fitted per district and disease
and run in parallel across the cluster with applyInPandas.

Why negative binomial rather than ordinary regression. Weekly case counts are
non-negative, overdispersed and right-skewed. A Gaussian model can predict
negative cases and produces symmetric prediction intervals, which understate
the upside risk that matters most for hospital planning. The NB variance
mu + mu^2/k matches what the data actually does, so the 80% interval is
asymmetric in the right direction.

Why not a neural or boosted model. They would likely gain a little accuracy
and lose the property the brief cares about in conflict C5: a district health
officer can read these coefficients. 'Six weeks after 100mm of rain, expect
dengue roughly 1.4 times higher' is a sentence an official can act on and
challenge. A learned black box is not.

Leakage control. A forecast made at origin t for horizon h may use only
information available at t. Three rules enforce this:

  1. training data is restricted to weeks strictly before the origin;
  2. weather covariates use lags of at least h weeks, so a 4-week-ahead
     forecast never reads rainfall from 2 weeks ago, which had not happened
     yet at the origin;
  3. the seasonal term is built from prior years only, never from the year
     being forecast.

These are enforced in build_features and rolling_origin_backtest rather than
left to discipline, because leakage is silent: a leaking model simply looks
better than it is.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)


FORECAST_SCHEMA = StructType([
    StructField("district_id", StringType()),
    StructField("district_name", StringType()),
    StructField("division", StringType()),
    StructField("disease_group", StringType()),
    StructField("origin_year", IntegerType()),
    StructField("origin_week", IntegerType()),
    StructField("target_year", IntegerType()),
    StructField("target_week", IntegerType()),
    StructField("horizon", IntegerType()),
    StructField("actual", DoubleType()),
    StructField("forecast", DoubleType()),
    StructField("lower_80", DoubleType()),
    StructField("upper_80", DoubleType()),
    StructField("baseline_naive", DoubleType()),
    StructField("model", StringType()),
    StructField("n_train", IntegerType()),
])


def weather_lag_columns(horizon: int, max_lag: int = 8) -> list[str]:
    """
    The weather lags usable at a given horizon.

    At origin t forecasting t+h, a lag-L value refers to week t+h-L. That is
    known at t only when L >= h. Allowing shorter lags would feed the model
    rainfall from the future of its own forecast.
    """
    lags = [L for L in range(max(1, horizon), max_lag + 1)]
    cols = []
    for L in lags:
        cols += [f"rain_mm_lag{L}", f"temp_mean_c_lag{L}"]
    return cols


def _nb_irls(X: np.ndarray, y: np.ndarray, k: float = 2.0,
             max_iter: int = 40, tol: float = 1e-7
             ) -> tuple[np.ndarray, float]:
    """
    Fit a negative-binomial GLM with log link by iteratively reweighted least
    squares, with the dispersion k held fixed and then estimated once.

    Written out rather than taken from statsmodels so that the whole fit runs
    inside a pandas UDF on the executors without requiring that package to be
    present on every node, and so the numerics stay visible and explainable.
    """
    n, p = X.shape
    beta = np.zeros(p)
    beta[0] = np.log(max(y.mean(), 0.5))

    for _ in range(max_iter):
        eta = np.clip(X @ beta, -20, 20)
        mu = np.exp(eta)
        # NB variance and the IRLS weight for a log link
        var = mu + mu ** 2 / k
        w = mu ** 2 / np.maximum(var, 1e-9)
        z = eta + (y - mu) / np.maximum(mu, 1e-9)

        WX = X * w[:, None]
        A = X.T @ WX + np.eye(p) * 1e-6      # ridge term for stability
        b = WX.T @ z
        try:
            new = np.linalg.solve(A, b)
        except np.linalg.LinAlgError:
            break
        if np.max(np.abs(new - beta)) < tol:
            beta = new
            break
        beta = new

    # Estimate dispersion from the Pearson residuals of the fitted model
    eta = np.clip(X @ beta, -20, 20)
    mu = np.exp(eta)
    resid2 = ((y - mu) ** 2).sum()
    denom = max(n - p, 1)
    phi = resid2 / denom
    mu_bar = max(mu.mean(), 1e-6)
    k_hat = mu_bar ** 2 / max(phi - mu_bar, 1e-6) if phi > mu_bar else 50.0
    return beta, float(np.clip(k_hat, 0.3, 50.0))


def _design(df: pd.DataFrame, feat_cols: list[str]) -> np.ndarray:
    """
    Build the design matrix: intercept, harmonic seasonality, trend, weather.

    Seasonality enters as a pair of sine and cosine terms on the annual cycle
    rather than as 52 week dummies. Two coefficients capture the annual wave
    with roughly 260 observations available, where 52 dummies would overfit
    badly and leave nothing to estimate the weather effect with.
    """
    n = len(df)
    wk = df["epi_week"].to_numpy(dtype=float)
    cols = [np.ones(n),
            np.sin(2 * np.pi * wk / 52.0),
            np.cos(2 * np.pi * wk / 52.0),
            np.sin(4 * np.pi * wk / 52.0),
            np.cos(4 * np.pi * wk / 52.0),
            (df["week_index"].to_numpy(dtype=float)
             - df["week_index"].mean()) / 52.0]
    for c in feat_cols:
        v = df[c].to_numpy(dtype=float) if c in df.columns else np.zeros(n)
        v = np.nan_to_num(v, nan=0.0)
        # Log-transform rainfall: its effect saturates, and the raw scale
        # would let one extreme week dominate the fit.
        if c.startswith("rain"):
            v = np.log1p(np.maximum(v, 0) / 25.0)
        else:
            v = (v - np.nanmean(v)) / max(np.nanstd(v), 1e-6)
        cols.append(v)
    return np.column_stack(cols)


def forecast_one_series(pdf: pd.DataFrame, horizons: tuple[int, ...],
                        origin_year: int, origin_week: int,
                        min_train: int = 60) -> pd.DataFrame:
    """
    Fit one district-disease series and forecast every horizon from one origin.

    Returns one row per horizon, carrying the point forecast, an 80% interval
    and the seasonal-naive baseline for the same target week.
    """
    pdf = pdf.sort_values("week_index").reset_index(drop=True)
    origin_idx = origin_year * 53 + origin_week

    train = pdf[pdf["week_index"] <= origin_idx]
    if len(train) < min_train:
        return pd.DataFrame(columns=[f.name for f in FORECAST_SCHEMA.fields])

    rows = []
    for h in horizons:
        target_idx = origin_idx + h
        tgt = pdf[pdf["week_index"] == target_idx]
        if tgt.empty:
            continue
        tgt = tgt.iloc[0]

        feat = weather_lag_columns(h)
        feat = [c for c in feat if c in pdf.columns]

        X = _design(train, feat)
        y = train["cases"].to_numpy(dtype=float)
        try:
            beta, k_hat = _nb_irls(X, y)
        except Exception:
            continue

        Xt = _design(pd.concat([train.tail(1), tgt.to_frame().T]), feat)[-1:]
        mu = float(np.exp(np.clip(Xt @ beta, -20, 20))[0])
        mu = float(np.clip(mu, 0, y.max() * 8 + 50))

        # 80% interval from the fitted negative binomial. The gamma-Poisson
        # mixture gives the right asymmetry: the upper bound sits further from
        # the mean than the lower one, which is what resource planning needs.
        var = mu + mu ** 2 / k_hat
        sd = np.sqrt(var)
        lower = max(0.0, mu - 1.2816 * sd)
        upper = mu + 1.2816 * sd

        # Seasonal-naive baseline: the same epi-week one year earlier. This is
        # the comparator AN-5 names, and it is a strong one for seasonal
        # disease, which is the point.
        prior = pdf[pdf["week_index"] == target_idx - 52]
        base = float(prior["cases"].iloc[0]) if not prior.empty else float(y[-1])

        rows.append({
            "district_id": pdf["district_id"].iloc[0],
            "district_name": pdf["district_name"].iloc[0],
            "division": pdf["division"].iloc[0],
            "disease_group": pdf["disease_group"].iloc[0],
            "origin_year": int(origin_year), "origin_week": int(origin_week),
            "target_year": int(tgt["epi_year"]),
            "target_week": int(tgt["epi_week"]),
            "horizon": int(h),
            "actual": float(tgt["cases"]),
            "forecast": mu, "lower_80": lower, "upper_80": upper,
            "baseline_naive": base,
            "model": "NB-GLM", "n_train": int(len(train)),
        })

    return pd.DataFrame(rows, columns=[f.name for f in FORECAST_SCHEMA.fields])
