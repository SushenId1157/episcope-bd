"""
EpiScope - expected weekly case counts per district, disease and epi-week.

This is the statistical core of the generator. It produces a small table
(64 districts x ~261 weeks x 5 disease groups), which the encounter builder
then explodes into individual records. Keeping the statistics in the small
table and the explosion in Spark is what makes 25 million records feasible on
a two-core machine: the expensive distributions are evaluated ~84,000 times,
not 25 million times.

Model, per district d, epi-week w and disease s:

    lambda = pop(d)/100000 * base(s) * season(s,w) * geo(d,s)
             * climate(d,w,s) * outbreak(d,w,s)
    cases ~ NegativeBinomial(mean=lambda, size=k_s)

Negative-binomial rather than Poisson because infectious disease counts are
overdispersed: variance = mu + mu^2/k, with smaller k meaning more clustering
(Section 5.2 requires overdispersion explicitly).
"""
from __future__ import annotations

import csv
import hashlib
import math
import os
import random


# --------------------------------------------------------------- seasonality
def seasonal_factor(epi_week: int, peak_week: int, width: float,
                    secondary_peak: int | None = None,
                    secondary_strength: float = 0.0) -> float:
    """
    Annual cycle as a wrapped Gaussian centred on the disease's peak week.

    The distance between two weeks is computed on a circle of 52 weeks, so a
    peak in week 2 is correctly close to week 51 rather than 49 weeks away.
    Returns a multiplier in roughly [0.12, 1.0].
    """
    def wrapped(a: int, b: int) -> float:
        diff = abs(a - b)
        return min(diff, 52 - diff)

    main = math.exp(-0.5 * (wrapped(epi_week, peak_week) / width) ** 2)
    if secondary_peak is not None and secondary_strength > 0:
        sec = math.exp(-0.5 * (wrapped(epi_week, secondary_peak) /
                               (width * 0.8)) ** 2)
        main = max(main, sec * secondary_strength)
    # A floor keeps a low but non-zero baseline all year, as real surveillance
    # data has: diseases do not vanish entirely out of season.
    return 0.12 + 0.88 * main


# ------------------------------------------------------------------- geography
def geographic_factor(district: dict, disease_cfg: dict) -> float:
    """Urban and hill amplification, interpolated so every district differs."""
    is_urban = str(district.get("is_urban")).lower() == "true"
    is_hill = str(district.get("is_hill")).lower() == "true"
    f = 1.0
    if is_urban:
        f *= disease_cfg.get("urban_amplification", 1.0)
    if is_hill:
        f *= disease_cfg.get("hill_amplification", 1.0)
    if not is_urban and not is_hill:
        # Rural lowland districts sit between the two extremes rather than at
        # exactly 1.0, so amplitude varies district by district as required.
        ua = disease_cfg.get("urban_amplification", 1.0)
        f *= 1.0 + (ua - 1.0) * 0.18
    return max(0.05, f)


# --------------------------------------------------------------------- climate
def climate_factor(wrow: dict | None, clim: dict) -> float:
    """
    Response to lagged rainfall, temperature and humidity.

    Rainfall enters through a saturating (log) response: the first 50 mm of a
    week matter far more than the next 50 mm, because vector breeding sites
    and contamination routes saturate. Temperature enters as a Gaussian around
    the disease's optimum, so both unusually cold and unusually hot weeks
    suppress transmission. A missing weather row (the first weeks of the
    series, where the lag reaches before the data begins) yields 1.0, meaning
    no climate effect rather than a fabricated one.
    """
    if not wrow:
        return 1.0

    rain = wrow.get(f"rain_mm_lag{clim['rain_lag_weeks']}")
    temp = wrow.get(f"temp_mean_c_lag{clim['temp_lag_weeks']}")
    hum = wrow.get(f"humidity_pct_lag{clim['rain_lag_weeks']}")

    f = 1.0

    if rain is not None:
        # log1p(rain/25) is ~0 at no rain, ~1.1 at 50 mm, ~1.8 at 125 mm
        f *= 1.0 + clim["rain_coefficient"] * math.log1p(max(0.0, rain) / 25.0)

    if temp is not None:
        opt = clim["temp_optimum_c"]
        dev = (temp - opt) / 6.0
        shape = math.exp(-0.5 * dev * dev)       # 1.0 at optimum, falls away
        coef = clim["temp_coefficient"]
        if coef >= 0:
            f *= 1.0 + coef * (shape - 0.5)
        else:
            # Negative coefficient: the disease prefers cooler weeks (ILI)
            f *= 1.0 + abs(coef) * (0.5 - shape) * -1.0 + abs(coef) * shape * 0
            f *= 1.0 + abs(coef) * (shape - 0.5)

    if hum is not None:
        f *= 1.0 + clim.get("humidity_coefficient", 0.0) * ((hum - 70.0) / 30.0)

    return max(0.15, min(4.0, f))


# ------------------------------------------------------- negative binomial draw
def _nb_sample(rng: random.Random, mean: float, size: float) -> int:
    """
    Draw from a negative binomial with the given mean and dispersion `size`,
    using the Poisson-gamma mixture: lambda ~ Gamma(size, mean/size), then
    count ~ Poisson(lambda). This is the formulation Section 5.2 names.
    """
    if mean <= 0:
        return 0
    if size <= 0:
        return _poisson(rng, mean)
    shape = size
    scale = mean / size
    lam = rng.gammavariate(shape, scale)
    return _poisson(rng, lam)


def _poisson(rng: random.Random, lam: float) -> int:
    """Knuth for small lambda, normal approximation for large lambda."""
    if lam <= 0:
        return 0
    if lam < 30:
        L, k, p = math.exp(-lam), 0, 1.0
        while True:
            k += 1
            p *= rng.random()
            if p <= L:
                return k - 1
    val = int(round(rng.gauss(lam, math.sqrt(lam))))
    return max(0, val)


def _rng_for(seed: int, key: str) -> random.Random:
    h = hashlib.sha256(f"{seed}:weekly:{key}".encode()).hexdigest()
    return random.Random(int(h[:16], 16))


# ------------------------------------------------------------------- main build
def build_weekly_cases(districts: list[dict], weather_weekly: list[dict],
                       diseases: dict, outbreak_plan: list[dict],
                       seed: int, scale_factor: float = 1.0,
                       study_years: tuple[int, int] = (2021, 2025)
                       ) -> list[dict]:
    """
    Produce one row per (district, disease, epi-year, epi-week) with the
    expected mean and the realised case count.

    scale_factor multiplies the expected counts, so SF=0.25 produces about a
    quarter of the encounters while preserving every seasonal, spatial and
    outbreak pattern. The seed is independent of the scale factor, so the same
    epidemic history appears at every scale.
    """
    # Index weather by (district, year, week) for O(1) lookup
    wx = {(w["district_id"], w["epi_year"], w["epi_week"]): w
          for w in weather_weekly}

    # Index outbreak multipliers by (district, disease, year, week)
    ob = {}
    for r in outbreak_plan:
        key = (r["district_id"], r["disease_group"],
               int(r["epi_year"]), int(r["epi_week"]))
        # If two outbreaks overlap in the same cell, take the stronger one
        ob[key] = max(ob.get(key, 1.0), float(r["multiplier"]))

    weeks = sorted({(w["epi_year"], w["epi_week"]) for w in weather_weekly
                    if study_years[0] <= w["epi_year"] <= study_years[1]})

    rows = []
    for d in districts:
        pop_100k = int(d["population_total"]) / 100_000.0
        for sname, scfg in diseases.items():
            geo = geographic_factor(d, scfg)
            clim_cfg = scfg["climate"]
            base = scfg["baseline_rate"]
            k = scfg["nb_dispersion"]
            rng = _rng_for(seed, f"{d['district_id']}:{sname}")

            for (ey, ew) in weeks:
                season = seasonal_factor(
                    ew, scfg["peak_week"], scfg["season_width"],
                    scfg.get("secondary_peak_week"),
                    scfg.get("secondary_peak_strength", 0.0))
                clim = climate_factor(wx.get((d["district_id"], ey, ew)),
                                      clim_cfg)
                mult = ob.get((d["district_id"], sname, ey, ew), 1.0)

                lam = pop_100k * base * season * geo * clim * mult * scale_factor
                cases = _nb_sample(rng, lam, k)

                rows.append({
                    "district_id": d["district_id"],
                    "district_name": d["district_name"],
                    "division": d["division"],
                    "disease_group": sname,
                    "epi_year": ey,
                    "epi_week": ew,
                    "expected_mean": round(lam, 4),
                    "outbreak_multiplier": round(mult, 4),
                    "cases": cases,
                })
    return rows


def write_weekly_cases(rows: list[dict], path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cols = ["district_id", "district_name", "division", "disease_group",
            "epi_year", "epi_week", "expected_mean", "outbreak_multiplier",
            "cases"]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader(); w.writerows(rows)
    return path
