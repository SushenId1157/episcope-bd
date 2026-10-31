"""
EpiScope - annual seasonal preparedness calendar (AN-9).

The calendar answers one question per division and disease group: when does
this disease reliably rise, and therefore when must supplies be in place and
campaigns be running for them to matter.

Method. A classical additive decomposition on the weekly series:

    observed = trend + seasonal + residual

The trend is a centred 52-week rolling median, chosen over a mean because a
single injected outbreak would drag a mean and distort the trend for a whole
year around it. The seasonal component is the median detrended value for each
week of the year, again median rather than mean for the same reason. The
seasonal index is that component expressed relative to the annual average, so
an index of 1.8 means a week typically runs 80% above the yearly norm.

A week is called high-risk when its index exceeds HIGH_RISK_INDEX and the
pattern is consistent, measured as the share of study years in which that week
was above average. Consistency matters: a week that was extreme once and
ordinary four times is not a seasonal pattern, it is a past outbreak, and
pre-positioning against it would waste supplies.

Lead times differ by intervention, not by disease severity. Larval source
reduction needs weeks to break the mosquito breeding cycle before cases fall,
so dengue campaigns must start well ahead of the rise. Oral rehydration salts
can be moved and used immediately, so diarrhoeal pre-positioning can be later
and tighter. These are stated as assumptions of this teaching case.

Everything here is derived from the gold weekly table. No projection is
presented as an official forecast; the output is labelled as a planning
guide built from synthetic surveillance data.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# A week counts as seasonally elevated when its index exceeds this.
HIGH_RISK_INDEX = 1.25

# And when at least this share of study years had that week above the annual
# average, so a single outbreak year cannot create a false seasonal signal.
MIN_CONSISTENCY = 0.60

# Weeks of lead time between starting an intervention and it affecting cases.
INTERVENTION_LEAD = {
    "dengue": {
        "supply_lead_weeks": 4,
        "campaign_lead_weeks": 8,
        "campaign": "Larval source reduction and community clean-up. Breaking "
                    "the breeding cycle takes several weeks to show in case "
                    "counts, so campaigns must begin well before the rise.",
        "supplies": "NS1 rapid tests, IV fluids, platelet capacity",
    },
    "diarrhoeal": {
        "supply_lead_weeks": 2,
        "campaign_lead_weeks": 4,
        "campaign": "Water quality monitoring, ORS distribution points and "
                    "hygiene messaging, concentrated before the pre-monsoon "
                    "rise and after flooding.",
        "supplies": "ORS sachets, IV fluids, water purification tablets",
    },
    "ili": {
        "supply_lead_weeks": 3,
        "campaign_lead_weeks": 6,
        "campaign": "Respiratory hygiene messaging and vaccination of priority "
                    "groups where available.",
        "supplies": "Oxygen capacity, antivirals, PPE",
    },
    "malaria": {
        "supply_lead_weeks": 4,
        "campaign_lead_weeks": 10,
        "campaign": "Bed net distribution and indoor residual spraying in hill "
                    "districts, timed to precede the wet season.",
        "supplies": "RDTs, artemisinin-based combination therapy",
    },
}

MONTHS = ["January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December"]


def _week_to_month(week: int) -> str:
    """Approximate calendar month for an ISO week, for a readable calendar."""
    return MONTHS[min(11, max(0, int((week - 1) * 12 / 52)))]


def decompose(series: pd.DataFrame) -> pd.DataFrame:
    """
    Additive decomposition of one weekly series.

    `series` carries epi_year, epi_week and cases, ordered in time. Returns
    the same rows with trend, seasonal and residual attached.
    """
    s = series.sort_values(["epi_year", "epi_week"]).reset_index(drop=True)
    y = s["cases"].astype(float)

    # Centred 52-week rolling median. Median resists the injected outbreaks,
    # which a rolling mean would smear across a whole year of trend.
    trend = y.rolling(window=53, center=True, min_periods=27).median()
    trend = trend.bfill().ffill()
    detrended = y - trend

    seasonal_by_week = detrended.groupby(s["epi_week"]).median()
    s["trend"] = trend
    s["seasonal"] = s["epi_week"].map(seasonal_by_week)
    s["residual"] = y - s["trend"] - s["seasonal"]
    return s


def seasonal_profile(series: pd.DataFrame) -> pd.DataFrame:
    """
    Build the week-of-year profile a calendar is read from.

    Returns one row per epi_week with the seasonal index and the consistency
    of the pattern across study years.
    """
    d = decompose(series)
    annual_mean = max(float(d["cases"].mean()), 1e-6)

    rows = []
    for wk, g in d.groupby("epi_week"):
        med = float(g["cases"].median())
        above = float((g["cases"] > annual_mean).mean())
        rows.append({
            "epi_week": int(wk),
            "median_cases": med,
            "seasonal_index": med / annual_mean,
            "consistency": above,
            "n_years": int(len(g)),
        })
    return pd.DataFrame(rows).sort_values("epi_week").reset_index(drop=True)


def _contiguous_runs(weeks: list[int], bridge_gap: int = 3,
                    min_length: int = 3) -> list[tuple[int, int]]:
    """
    Group week numbers into seasonal windows.

    Two corrections over a naive run-length grouping, both found by reading
    the first calendar:

    Bridging. A season does not stop because one week dipped below the
    threshold. Khulna's ILI season came out as three separate entries - June,
    June to August, and September - which is one season with two dips in it.
    Runs separated by `bridge_gap` weeks or fewer are joined.

    Minimum length. A single isolated week above threshold is noise, not a
    season worth pre-positioning against, so runs shorter than `min_length`
    are dropped.
    """
    if not weeks:
        return []
    weeks = sorted(set(weeks))
    runs, start, prev = [], weeks[0], weeks[0]
    for w in weeks[1:]:
        if w - prev <= bridge_gap:
            prev = w
            continue
        runs.append((start, prev))
        start = prev = w
    runs.append((start, prev))

    # Join a run ending near week 52 with one starting near week 1
    if len(runs) > 1 and runs[0][0] <= bridge_gap and runs[-1][1] >= 53 - bridge_gap:
        runs[0] = (runs[-1][0], runs[0][1])
        runs.pop()

    def length(r):
        return (r[1] - r[0] + 1) if r[1] >= r[0] else (52 - r[0] + r[1] + 1)

    return [r for r in runs if length(r) >= min_length]


def build_calendar(weekly: pd.DataFrame, diseases: list[str]) -> pd.DataFrame:
    """
    Produce the preparedness calendar per division and disease group.

    `weekly` carries division, disease_group, epi_year, epi_week, cases.
    """
    rows = []
    for (div, dis), g in weekly.groupby(["division", "disease_group"]):
        if dis not in diseases:
            continue
        agg = (g.groupby(["epi_year", "epi_week"], as_index=False)["cases"]
                .sum())
        prof = seasonal_profile(agg)

        high = prof[(prof["seasonal_index"] >= HIGH_RISK_INDEX)
                    & (prof["consistency"] >= MIN_CONSISTENCY)]
        if high.empty:
            continue

        lead = INTERVENTION_LEAD.get(dis, {"supply_lead_weeks": 3,
                                           "campaign_lead_weeks": 6,
                                           "campaign": "", "supplies": ""})

        for (w_start, w_end) in _contiguous_runs(sorted(high["epi_week"])):
            # The peak must be the peak OF THIS WINDOW. Taking the year's
            # global maximum put a September peak inside a November window in
            # the first version, which is incoherent on its face.
            if w_end >= w_start:
                band = prof[(prof["epi_week"] >= w_start)
                            & (prof["epi_week"] <= w_end)]
            else:                                # window wraps the year end
                band = prof[(prof["epi_week"] >= w_start)
                            | (prof["epi_week"] <= w_end)]
            if band.empty:
                continue
            peak = band.loc[band["seasonal_index"].idxmax()]

            # Lead times count backwards from the start of the window and wrap
            # into the previous year rather than clamping at week 1, so a
            # January season is prepared for in the preceding November.
            supply_week = ((w_start - lead["supply_lead_weeks"] - 1) % 52) + 1
            campaign_week = ((w_start - lead["campaign_lead_weeks"] - 1) % 52) + 1

            rows.append({
                "division": div,
                "disease_group": dis,
                "high_risk_week_start": int(w_start),
                "high_risk_week_end": int(w_end),
                "high_risk_months": (f"{_week_to_month(w_start)} to "
                                     f"{_week_to_month(w_end)}"),
                "peak_week": int(peak["epi_week"]),
                "peak_month": _week_to_month(int(peak["epi_week"])),
                "expected_rise_vs_annual_mean": round(
                    float(band["seasonal_index"].mean()), 2),
                "peak_index": round(float(peak["seasonal_index"]), 2),
                "pattern_consistency": round(
                    float(band["consistency"].mean()), 2),
                "position_supplies_by_week": int(supply_week),
                "position_supplies_by_month": _week_to_month(supply_week),
                "start_campaign_week": int(campaign_week),
                "start_campaign_month": _week_to_month(campaign_week),
                "recommended_campaign": lead["campaign"],
                "key_supplies": lead["supplies"],
                "years_of_evidence": int(prof["n_years"].max()),
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["division", "disease_group",
                            "high_risk_week_start"]).reset_index(drop=True)
