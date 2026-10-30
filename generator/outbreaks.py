"""
EpiScope - outbreak injection plan and ground truth (Section 5.2).

Two artefacts come out of this module:

  1. an injection plan: per district, disease and epi-week, a multiplier applied
     to the expected case count when the encounter data is generated;
  2. a ground truth table: one row per injected outbreak, written to its own
     file. AN-1 detection code must never read it. Only AN-2 evaluation does.

Outbreaks are drawn deterministically from the seed, so the same seed always
produces the same epidemic history.
"""
from __future__ import annotations

import csv
import hashlib
import math
import os
import random
from datetime import date, timedelta


def _rng_for(seed: int, key: str) -> random.Random:
    h = hashlib.sha256(f"{seed}:outbreak:{key}".encode()).hexdigest()
    return random.Random(int(h[:16], 16))


def _week_index_to_date(week_index: int, start: date) -> date:
    """Week 0 is the ISO Monday on or before `start`."""
    monday = start - timedelta(days=start.isoweekday() - 1)
    return monday + timedelta(weeks=week_index)


def _epi(d: date) -> tuple[int, int]:
    iso = d.isocalendar()
    return iso[0], iso[1]


def _epidemic_curve(duration: int, peak_mult: float, shape: str) -> list[float]:
    """
    Multiplier for each week of an outbreak, 1.0 meaning no excess.

    'point'  : sharp rise, slower decay - the classic single-source epidemic.
    'slow'   : near-linear creep to a modest peak, deliberately hard to detect.
    'national': broad logistic wave, like a novel pathogen spreading nationwide.
    """
    out = []
    for w in range(duration):
        t = w / max(1, duration - 1)
        if shape == "slow":
            # Gentle, almost linear growth then a short plateau
            f = t ** 0.85
        elif shape == "national":
            # Logistic rise, peak around 45% through, long tail
            f = 1.0 / (1.0 + math.exp(-10 * (t - 0.45)))
            f *= math.exp(-1.2 * max(0.0, t - 0.55))
        else:
            # Point source: fast up (gamma-like), slower down
            f = (t ** 1.4) * math.exp(-3.0 * t) * math.e ** 1.4 * 2.4
        out.append(1.0 + (peak_mult - 1.0) * max(0.0, min(1.0, f)))
    return out


def build_outbreak_plan(districts: list[dict], spec: dict, diseases: dict,
                        surveillance_diseases: list[str], seed: int,
                        study_start: str, study_end: str) -> tuple[list[dict],
                                                                   list[dict]]:
    """
    Return (injection_plan, ground_truth).

    injection_plan rows: district_id, disease_group, epi_year, epi_week,
                         multiplier, outbreak_id
    ground_truth rows  : one per outbreak, matching GROUND_TRUTH_SCHEMA
    """
    sy, sm, sd = (int(x) for x in study_start.split("-"))
    ey, em, ed = (int(x) for x in study_end.split("-"))
    start_date, end_date = date(sy, sm, sd), date(ey, em, ed)
    total_weeks = ((end_date - start_date).days // 7) - 1

    plan, truth = [], []

    # ------------------------------------------------ 1. district outbreaks
    n = spec["n_district_outbreaks"]
    rng_pick = _rng_for(seed, "pick")
    dur_lo, dur_hi = spec["duration_weeks"]
    mult_lo, mult_hi = spec["peak_multiplier"]

    # Spread outbreaks across districts, diseases and the whole study window.
    # Larger districts are somewhat more likely to be chosen, as they have more
    # importation opportunities, but every district remains eligible.
    weights = [max(1.0, math.log10(int(d["population_total"]))) for d in districts]

    # Disease mix reflects the national burden described in the brief: dengue
    # dominates, diarrhoeal disease is common, ILI is seasonal and malaria is
    # geographically confined to the hill districts and therefore rarer.
    DISEASE_MIX = {"dengue": 0.38, "diarrhoeal": 0.28, "ili": 0.22, "malaria": 0.12}
    mix_names = [d for d in DISEASE_MIX if d in surveillance_diseases]
    mix_weights = [DISEASE_MIX[d] for d in mix_names]

    hills = [x for x in districts if str(x.get("is_hill")).lower() == "true"]
    urban = [x for x in districts if str(x.get("is_urban")).lower() == "true"]

    # Spread outbreaks over as many distinct districts as possible: a district
    # already carrying an outbreak of the same disease is only reused once the
    # unused pool is exhausted. This keeps the detection evaluation (AN-2)
    # informative, since false-alert rates are measured per district-year.
    used: set[tuple[str, str]] = set()

    def choose_district(pool, rng, disease):
        """
        Prefer a district not yet used for this disease, but never leave the
        pool: geography is a hard constraint. Malaria must stay in the hill
        districts even if that means a district carries two outbreaks, because
        a malaria epidemic in the northern plains would be epidemiologically
        false and would discredit the whole dataset.
        """
        fresh = [x for x in pool if (x["district_id"], disease) not in used]
        chosen = rng.choice(fresh) if fresh else rng.choice(pool)
        used.add((chosen["district_id"], disease))
        return chosen

    def seasonal_start_week(disease, rng, total_weeks, duration):
        """
        Place the outbreak so that its growth phase coincides with the season
        in which that disease actually circulates. The brief states dengue
        follows the monsoon, diarrhoeal disease rises before the monsoon and
        after floods, ILI peaks in the rainy season and malaria peaks in the
        wet months in the hill tracts. An outbreak injected outside its season
        would sit on a near-zero baseline, so no surveillance method could
        detect it and the evaluation in AN-2 would be measuring nothing.

        The chosen week is the ISO week in which the outbreak starts; a small
        random offset keeps the outbreaks from lining up with each other.
        """
        windows = {
            "dengue":     (27, 40),   # July to early October
            "diarrhoeal": (12, 24),   # pre-monsoon, late March to mid June
            "ili":        (22, 34),   # rainy season
            "malaria":    (20, 32),   # wet months in the hill tracts
        }
        lo, hi = windows.get(disease, (10, 40))
        target_week = rng.randint(lo, hi)

        # Pick a study year that leaves room for the full outbreak duration
        max_year_offset = max(0, (total_weeks - duration - 4) // 52)
        year_offset = rng.randint(0, max_year_offset) if max_year_offset else 0

        start_w = year_offset * 52 + (target_week - 1)
        return max(4, min(start_w, total_weeks - duration - 2))

    for i in range(n):
        rng = _rng_for(seed, f"district:{i}")
        disease = rng_pick.choices(mix_names, weights=mix_weights, k=1)[0]

        if disease == "malaria":
            pool = hills or districts
        elif disease == "dengue":
            # Dengue concentrates in large cities but is not confined to them
            pool = urban if (urban and rng.random() < 0.70) else districts
        else:
            pool = districts
        d = choose_district(pool, rng, disease)

        duration = rng.randint(dur_lo, dur_hi)
        peak_mult = round(rng.uniform(mult_lo, mult_hi), 2)
        start_w = seasonal_start_week(disease, rng, total_weeks, duration)

        oid = f"OB-{i+1:03d}"
        curve = _epidemic_curve(duration, peak_mult, "point")
        _append(plan, truth, oid, d, disease, start_w, curve, peak_mult,
                "point", False, start_date)

    # ------------------------------------------------ 2. slow-growing outbreaks
    sg = spec["slow_growing"]
    for j in range(sg["count"]):
        rng = _rng_for(seed, f"slow:{j}")
        disease = rng.choice(surveillance_diseases)
        # Slow-growing outbreaks obey the same geography as point-source ones:
        # the disease decides which districts are even eligible.
        if disease == "malaria":
            slow_pool = hills or districts
        elif disease == "dengue":
            slow_pool = urban if (urban and rng.random() < 0.70) else districts
        else:
            slow_pool = districts
        d = choose_district(slow_pool, rng, disease)
        duration = rng.randint(*sg["duration_weeks"])
        peak_mult = round(rng.uniform(*sg["peak_multiplier"]), 2)
        # A slow outbreak must begin earlier so that its peak still lands in
        # season, since it takes most of its duration to build up.
        start_w = max(4, seasonal_start_week(disease, rng, total_weeks, duration)
                      - int(duration * 0.55))

        oid = f"OB-SLOW-{j+1:02d}"
        curve = _epidemic_curve(duration, peak_mult, "slow")
        _append(plan, truth, oid, d, disease, start_w, curve, peak_mult,
                "slow_growing", False, start_date)

    # ------------------------------------- 3. nationwide novel respiratory surge
    nr = spec["novel_respiratory"]
    duration = nr["duration_weeks"]
    curve = _epidemic_curve(duration, nr["peak_multiplier"], "national")
    for d in districts:
        rng = _rng_for(seed, f"national:{d['district_id']}")
        # The wave reaches districts at slightly different times and strengths
        offset = rng.randint(0, 3)
        local = rng.uniform(0.75, 1.25)
        local_curve = [1.0 + (m - 1.0) * local for m in curve]
        _append(plan, truth, "OB-NATIONAL-01", d, nr["disease"],
                nr["start_week_index"] + offset, local_curve,
                nr["peak_multiplier"], "national_novel_respiratory", True,
                start_date, emit_truth=False)

    # One ground truth row represents the national event as a whole.
    nat_start = _week_index_to_date(nr["start_week_index"], start_date)
    nat_end = nat_start + timedelta(weeks=duration)
    peak_off = max(range(duration), key=lambda w: curve[w])
    truth.append({
        "outbreak_id": "OB-NATIONAL-01",
        "district_id": "ALL",
        "district_name": "All districts",
        "disease_group": nr["disease"],
        "start_date": nat_start,
        "end_date": nat_end,
        "peak_date": nat_start + timedelta(weeks=peak_off),
        "peak_multiplier": float(nr["peak_multiplier"]),
        "outbreak_type": "national_novel_respiratory",
        "is_national": True,
        "excess_cases": None,          # measured after generation, not assumed
    })

    return plan, truth


def _append(plan, truth, oid, d, disease, start_w, curve, peak_mult,
            otype, is_national, start_date, emit_truth=True):
    for w, mult in enumerate(curve):
        wd = _week_index_to_date(start_w + w, start_date)
        ey, ew = _epi(wd)
        plan.append({
            "outbreak_id": oid,
            "district_id": d["district_id"],
            "disease_group": disease,
            "epi_year": ey,
            "epi_week": ew,
            "multiplier": round(mult, 4),
        })
    if emit_truth:
        s = _week_index_to_date(start_w, start_date)
        e = _week_index_to_date(start_w + len(curve), start_date)
        peak_off = max(range(len(curve)), key=lambda i: curve[i])
        truth.append({
            "outbreak_id": oid,
            "district_id": d["district_id"],
            "district_name": d["district_name"],
            "disease_group": disease,
            "start_date": s,
            "end_date": e,
            "peak_date": _week_index_to_date(start_w + peak_off, start_date),
            "peak_multiplier": float(peak_mult),
            "outbreak_type": otype,
            "is_national": is_national,
            "excess_cases": None,
        })


def write_plan(plan: list[dict], path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cols = ["outbreak_id", "district_id", "disease_group", "epi_year",
            "epi_week", "multiplier"]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader(); w.writerows(plan)
    return path


def write_ground_truth(truth: list[dict], path: str) -> str:
    """
    Write the ground truth outbreaks to their own file.

    IMPORTANT (Section 5.2): no module under analytics/detection/ may read this
    file. It exists only so analytics/evaluation can score the detector.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cols = ["outbreak_id", "district_id", "district_name", "disease_group",
            "start_date", "end_date", "peak_date", "peak_multiplier",
            "outbreak_type", "is_national", "excess_cases"]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader(); w.writerows(truth)
    return path
