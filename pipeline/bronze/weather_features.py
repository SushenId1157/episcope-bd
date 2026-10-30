"""
EpiScope - convert the raw S3 daily weather JSON into weekly district features
with the lags the generator and the forecaster need.

Epidemiological weeks follow ISO 8601 (Monday start). epi_year is taken from
the ISO calendar, not the calendar year, so the last days of December fall in
the correct week-1 of the following epi-year instead of being misfiled.

Output: one Parquet table, 64 districts x ~261 weeks, written with Spark so the
same table serves the generator, the forecaster (AN-4 lagged covariates) and
the gold layer.
"""
from __future__ import annotations

import csv
import glob
import json
import os
from datetime import date, timedelta

WEATHER_VARS = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
                "precipitation_sum", "relative_humidity_2m_mean"]


def iso_epi_week(d: date) -> tuple[int, int]:
    """Return (epi_year, epi_week) for a date, using the ISO 8601 calendar."""
    iso = d.isocalendar()
    return iso[0], iso[1]


def week_start(epi_year: int, epi_week: int) -> date:
    """Monday of the given ISO year/week."""
    return date.fromisocalendar(epi_year, epi_week, 1)


def load_daily_records(weather_dir: str) -> list[dict]:
    """Flatten every district's daily JSON into one list of daily records."""
    out = []
    for path in sorted(glob.glob(os.path.join(weather_dir, "*.json"))):
        pay = json.load(open(path))
        meta = pay.get("_episcope", {})
        did = meta.get("district_id") or os.path.basename(path).split(".")[0]
        daily = pay["daily"]
        times = daily["time"]
        for i, t in enumerate(times):
            y, m, dd = (int(x) for x in t.split("-"))
            rec = {"district_id": did, "obs_date": date(y, m, dd)}
            for v in WEATHER_VARS:
                vals = daily.get(v)
                rec[v] = vals[i] if vals is not None and i < len(vals) else None
            out.append(rec)
    return out


def aggregate_weekly(daily: list[dict]) -> list[dict]:
    """
    Collapse daily records to district x epi-week.

    Rainfall is summed (total water that week drives vector breeding and
    contamination); temperature and humidity are averaged. Extreme-rain days are
    counted separately because the brief ties diarrhoeal spikes to extreme
    rainfall, not to the weekly mean.
    """
    EXTREME_RAIN_MM = 50.0        # a heavy-rain day threshold for this project
    buckets: dict[tuple, dict] = {}

    for r in daily:
        ey, ew = iso_epi_week(r["obs_date"])
        key = (r["district_id"], ey, ew)
        b = buckets.setdefault(key, {
            "district_id": r["district_id"], "epi_year": ey, "epi_week": ew,
            "_rain": [], "_tmean": [], "_tmax": [], "_tmin": [], "_rh": [],
            "extreme_rain_days": 0, "n_days": 0,
        })
        b["n_days"] += 1
        rain = r.get("precipitation_sum")
        if rain is not None:
            b["_rain"].append(rain)
            if rain >= EXTREME_RAIN_MM:
                b["extreme_rain_days"] += 1
        for src, dst in [("temperature_2m_mean", "_tmean"),
                         ("temperature_2m_max", "_tmax"),
                         ("temperature_2m_min", "_tmin"),
                         ("relative_humidity_2m_mean", "_rh")]:
            if r.get(src) is not None:
                b[dst].append(r[src])

    def mean(xs):
        return round(sum(xs) / len(xs), 3) if xs else None

    rows = []
    for b in buckets.values():
        rows.append({
            "district_id": b["district_id"],
            "epi_year": b["epi_year"],
            "epi_week": b["epi_week"],
            "week_start_date": week_start(b["epi_year"], b["epi_week"]),
            "n_days": b["n_days"],
            "rain_mm": round(sum(b["_rain"]), 2) if b["_rain"] else None,
            "rain_max_day_mm": round(max(b["_rain"]), 2) if b["_rain"] else None,
            "extreme_rain_days": b["extreme_rain_days"],
            "temp_mean_c": mean(b["_tmean"]),
            "temp_max_c": mean(b["_tmax"]),
            "temp_min_c": mean(b["_tmin"]),
            "humidity_pct": mean(b["_rh"]),
        })
    rows.sort(key=lambda r: (r["district_id"], r["epi_year"], r["epi_week"]))
    return rows


def add_lags(weekly: list[dict], lags: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8)
             ) -> list[dict]:
    """
    Attach lagged rainfall, temperature and humidity to each district-week.

    A lag of k means "the value k weeks earlier in the same district". These are
    the covariates the generator uses for the climate response in Section 5.2
    and the forecaster uses as lagged weather covariates (AN-4). Because the
    series is strictly ordered per district, a lag can only ever look backwards,
    which is what prevents future-data leakage in the backtest (AN-5).
    """
    by_district: dict[str, list[dict]] = {}
    for r in weekly:
        by_district.setdefault(r["district_id"], []).append(r)

    out = []
    for did, rows in by_district.items():
        rows.sort(key=lambda r: (r["epi_year"], r["epi_week"]))
        for i, r in enumerate(rows):
            rec = dict(r)
            for k in lags:
                prev = rows[i - k] if i - k >= 0 else None
                rec[f"rain_mm_lag{k}"] = prev["rain_mm"] if prev else None
                rec[f"temp_mean_c_lag{k}"] = prev["temp_mean_c"] if prev else None
                rec[f"humidity_pct_lag{k}"] = prev["humidity_pct"] if prev else None
                rec[f"extreme_rain_days_lag{k}"] = (
                    prev["extreme_rain_days"] if prev else None)
            # Rolling totals give a smoother climate signal than a single week
            win4 = [x["rain_mm"] for x in rows[max(0, i - 3): i + 1]
                    if x["rain_mm"] is not None]
            win8 = [x["rain_mm"] for x in rows[max(0, i - 7): i + 1]
                    if x["rain_mm"] is not None]
            rec["rain_mm_roll4"] = round(sum(win4), 2) if win4 else None
            rec["rain_mm_roll8"] = round(sum(win8), 2) if win8 else None
            out.append(rec)

    out.sort(key=lambda r: (r["district_id"], r["epi_year"], r["epi_week"]))
    return out


def build_weather_weekly(weather_dir: str) -> list[dict]:
    """Full pipeline: daily JSON -> weekly aggregates -> lagged features."""
    return add_lags(aggregate_weekly(load_daily_records(weather_dir)))
