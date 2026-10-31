"""
EpiScope - gold to browser-ready JSON with privacy suppression
(VIS-4, PRV-2).

Two obligations shape this module.

Small-cell suppression (PRV-2). No count between 1 and 4 may appear in any
dashboard output, and totals must not let a suppressed value be recovered by
subtraction. Primary suppression alone does not achieve the second part: if
63 districts are published and one is hidden, the national total gives the
hidden one away exactly. Complementary suppression is therefore applied, a
second cell in the same row is hidden so the arithmetic no longer resolves to
a single value. Where even that is not enough, the total itself is withheld.

Payload budget (VIS-4). The browser must load at most 10 MB in total and
never line-level data. Each export below is an aggregate, and the sizes are
measured and reported rather than assumed; the budget check fails loudly
instead of shipping an oversized dashboard.
"""
from __future__ import annotations

import json
import math
import os
from typing import Iterable

import pandas as pd


SUPPRESSION_THRESHOLD = 5      # counts of 1..4 inclusive are suppressed
SUPPRESSED = None              # what a suppressed value becomes in JSON
PAYLOAD_BUDGET_MB = 10.0


def _needs_suppression(v) -> bool:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return False
    return 0 < float(v) < SUPPRESSION_THRESHOLD


def suppress_series(values: list, total: float | None = None
                    ) -> tuple[list, float | None, dict]:
    """
    Apply primary and complementary suppression to one row of counts.

    Returns (values, total, audit). A suppressed value is None. The audit
    records why each cell was hidden, which the dashboard shows and the
    report cites.

    The complementary rule: if exactly one cell is suppressed and a total is
    published, the smallest remaining cell is suppressed as well. With two
    unknowns and one equation the original is no longer recoverable. If the
    row is too short for that to work, the total is withheld instead.
    """
    out = list(values)
    primary = [i for i, v in enumerate(out) if _needs_suppression(v)]
    complementary = []

    for i in primary:
        out[i] = SUPPRESSED

    if primary and total is not None:
        remaining = [(i, v) for i, v in enumerate(out)
                     if v is not None and float(v) > 0]
        if len(primary) == 1 and remaining:
            # One hidden cell plus a published total is exactly recoverable,
            # so a second cell must go. The smallest is chosen to lose the
            # least information.
            j = min(remaining, key=lambda t: float(t[1]))[0]
            out[j] = SUPPRESSED
            complementary.append(j)
        elif not remaining:
            total = SUPPRESSED

    return out, total, {"primary": primary, "complementary": complementary}


def suppress_frame(df: pd.DataFrame, count_cols: Iterable[str],
                   group_cols: Iterable[str] | None = None) -> pd.DataFrame:
    """
    Suppress small counts across a dataframe.

    Rows are treated independently unless `group_cols` is given, in which case
    complementary suppression is applied within each group, since that is the
    level at which a viewer could sum the published values.
    """
    d = df.copy()
    count_cols = list(count_cols)

    for c in count_cols:
        d[c] = d[c].apply(lambda v: SUPPRESSED if _needs_suppression(v) else v)

    if group_cols:
        group_cols = list(group_cols)
        for _, idx in d.groupby(group_cols).groups.items():
            block = d.loc[idx, count_cols]
            hidden = int(block.isna().sum().sum())
            if hidden == 1:
                # Hide the smallest published value in the group as well
                stacked = block.stack()
                if not stacked.empty:
                    ri, ci = stacked.idxmin()
                    d.loc[ri, ci] = SUPPRESSED
    return d


def round_floats(obj, places: int = 2):
    """Shrink the payload by not shipping more precision than is meaningful."""
    if isinstance(obj, float):
        return None if math.isnan(obj) else round(obj, places)
    if isinstance(obj, dict):
        return {k: round_floats(v, places) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_floats(v, places) for v in obj]
    if hasattr(obj, "item"):
        try:
            return round_floats(obj.item(), places)
        except Exception:
            return obj
    return obj


def write_json(obj, path: str, places: int = 2) -> dict:
    """Write one export and report its size."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = round_floats(obj, places)
    # separators drop the spaces json.dump adds by default, which is a few
    # percent of payload for no benefit over the wire
    with open(path, "w") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    size = os.path.getsize(path)
    return {"path": path, "name": os.path.basename(path), "bytes": size,
            "kb": round(size / 1024, 1)}


def check_payload_budget(manifest: list[dict],
                         budget_mb: float = PAYLOAD_BUDGET_MB) -> dict:
    """Verify the total browser payload against the VIS-4 limit."""
    total = sum(m["bytes"] for m in manifest)
    return {"files": len(manifest),
            "total_bytes": total,
            "total_mb": round(total / 1024 / 1024, 3),
            "budget_mb": budget_mb,
            "within_budget": total / 1024 / 1024 <= budget_mb,
            "largest": sorted(manifest, key=lambda m: -m["bytes"])[:5]}


# Fields that hold counts of people, and so fall under PRV-2. Everything
# else in the exports is an identifier, an index, a rate or a model output:
# epi_week 3 is the third week of the year, alert_weeks 2 means a district
# alerted twice, and neither discloses anything about a patient.
#
# An earlier version of this audit flagged every integer between 1 and 4
# anywhere in the payload and returned 1,966 findings, almost all of them week
# numbers. A check that cannot distinguish a week number from a case count
# provides no assurance, so the field list is explicit and is the thing a
# reviewer should challenge.
COUNT_FIELDS = {
    "cases", "admissions", "deaths", "lab_confirmed", "distinct_patients",
    "mean_cases", "expected", "actual", "total_cases", "total_admissions",
    "total_deaths", "peak_weekly_cases", "quarantined_rows",
}

# Fields that are derived from a count and would reconstruct it if published
# while the count itself is suppressed.
DERIVED_FROM_COUNT = {
    "incidence_per_100k", "mean_incidence", "admission_rate",
    "case_fatality_rate",
}


def audit_no_small_counts(obj, path: str = "",
                          fields: set[str] | None = None) -> list[str]:
    """
    Walk an exported structure and report any disclosed count in 1..4.

    Only fields named in COUNT_FIELDS are examined, because those are the ones
    PRV-2 governs. The audit runs over the written JSON rather than over the
    dataframes, so a fault anywhere between aggregation and serialisation is
    still caught.
    """
    fields = fields if fields is not None else COUNT_FIELDS
    bad = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in fields and isinstance(v, (int, float)) \
                    and not isinstance(v, bool):
                if float(v).is_integer() and 0 < float(v) < SUPPRESSION_THRESHOLD:
                    bad.append(f"{path}.{k} = {v}")
            else:
                bad += audit_no_small_counts(v, f"{path}.{k}", fields)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            bad += audit_no_small_counts(v, f"{path}[{i}]", fields)
    return bad


def audit_derivable_counts(records: list[dict]) -> list[str]:
    """
    Report rows where a count is suppressed but a value derived from it is not.

    Publishing incidence_per_100k alongside the population while hiding cases
    would let any reader multiply the two back. Suppression has to propagate
    to everything computed from the hidden value.
    """
    bad = []
    for i, r in enumerate(records):
        if not isinstance(r, dict):
            continue
        for cf in COUNT_FIELDS & set(r):
            if r.get(cf) is None:
                leaked = [d for d in DERIVED_FROM_COUNT & set(r)
                          if r.get(d) is not None]
                if leaked:
                    bad.append(f"[{i}] {cf} suppressed but {leaked} published")
    return bad
