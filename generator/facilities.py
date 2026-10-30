"""
EpiScope - synthetic facility registry (S5) and stock snapshot helper (S6).

The registry mirrors the real structure of the Bangladesh health system: one
district hospital per district, medical colleges in the largest districts, and
upazila health complexes plus clinics allocated in proportion to population.
That proportionality is deliberate: it reproduces the Dhaka-driven data skew
the assignment asks us to handle (ENG-3) instead of inventing a uniform
distribution that would hide it.

Deterministic for a given seed (Section 5.2).
"""
from __future__ import annotations

import csv
import hashlib
import math
import os
import random

FACILITY_PROFILES = {
    "medical_college":  {"beds": (500, 1200), "icu_ratio": (0.04, 0.08)},
    "district_hospital": {"beds": (100, 400), "icu_ratio": (0.02, 0.05)},
    "upazila_hc":       {"beds": (31, 60),   "icu_ratio": (0.0, 0.0)},
    "clinic":           {"beds": (8, 30),    "icu_ratio": (0.0, 0.0)},
    "private_clinic":   {"beds": (10, 50),   "icu_ratio": (0.0, 0.02)},
}

# Districts large enough to host a medical college in this scenario
MEDICAL_COLLEGE_MIN_POP = 2_500_000

TARGET_FACILITIES = 640          # exceeds the required minimum of 600


def _rng_for(seed: int, key: str) -> random.Random:
    """A separate, reproducible RNG stream per key, so adding a district later
    does not shift the numbers already drawn for other districts."""
    h = hashlib.sha256(f"{seed}:{key}".encode()).hexdigest()
    return random.Random(int(h[:16], 16))


def _jitter_point(rng: random.Random, lat: float, lon: float,
                  spread_deg: float = 0.18) -> tuple[float, float]:
    """Place a facility near the district centroid, not exactly on it."""
    return (round(lat + rng.uniform(-spread_deg, spread_deg), 5),
            round(lon + rng.uniform(-spread_deg, spread_deg), 5))


def build_registry(districts: list[dict], seed: int,
                   target_total: int = TARGET_FACILITIES) -> list[dict]:
    """
    Build the facility registry.

    districts: rows from S1_district_reference_enriched.csv, each carrying
    district_id, district_name, division, centroid_lat/lon, population_total,
    is_urban, is_hill.
    """
    total_pop = sum(int(d["population_total"]) for d in districts)

    # Every district gets a district hospital first.
    fixed = len(districts)
    # Medical colleges in the largest districts.
    mc_districts = [d for d in districts
                    if int(d["population_total"]) >= MEDICAL_COLLEGE_MIN_POP]
    fixed += len(mc_districts)

    remaining = max(0, target_total - fixed)

    rows = []
    seq = 0

    def add(d, ftype, rng):
        nonlocal seq
        seq += 1
        prof = FACILITY_PROFILES[ftype]
        beds = rng.randint(*prof["beds"])
        lo, hi = prof["icu_ratio"]
        icu = int(round(beds * rng.uniform(lo, hi))) if hi > 0 else 0
        lat, lon = _jitter_point(rng, float(d["centroid_lat"]),
                                 float(d["centroid_lon"]))
        rows.append({
            "facility_id": f"F{seq:04d}",
            "facility_name": f"{d['district_name']} "
                             f"{ftype.replace('_', ' ').title()} {seq:04d}",
            "facility_type": ftype,
            "district_id": d["district_id"],
            "district_name": d["district_name"],
            "division": d["division"],
            "latitude": lat,
            "longitude": lon,
            "total_beds": beds,
            "icu_beds": icu,
            "opened_year": rng.randint(1972, 2019),
        })

    # Pass 1: one district hospital per district
    for d in sorted(districts, key=lambda x: x["district_id"]):
        add(d, "district_hospital", _rng_for(seed, f"dh:{d['district_id']}"))

    # Pass 2: medical colleges in large districts
    for d in sorted(mc_districts, key=lambda x: x["district_id"]):
        add(d, "medical_college", _rng_for(seed, f"mc:{d['district_id']}"))

    # Pass 3: distribute the rest in proportion to population, using the
    # largest-remainder method so the total lands exactly on target.
    quotas = []
    for d in sorted(districts, key=lambda x: x["district_id"]):
        share = int(d["population_total"]) / total_pop
        exact = share * remaining
        quotas.append([d, int(math.floor(exact)), exact - math.floor(exact)])

    assigned = sum(q[1] for q in quotas)
    for q in sorted(quotas, key=lambda x: -x[2])[:remaining - assigned]:
        q[1] += 1

    for d, n, _ in quotas:
        rng = _rng_for(seed, f"extra:{d['district_id']}")
        urban = str(d.get("is_urban")).lower() == "true"
        for _ in range(n):
            # Urban districts skew towards clinics; rural towards upazila HCs.
            r = rng.random()
            if urban:
                ftype = ("private_clinic" if r < 0.35 else
                         "clinic" if r < 0.70 else "upazila_hc")
            else:
                ftype = ("upazila_hc" if r < 0.60 else
                         "clinic" if r < 0.88 else "private_clinic")
            add(d, ftype, rng)

    rows.sort(key=lambda r: r["facility_id"])
    return rows


def write_registry(rows: list[dict], out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    cols = ["facility_id", "facility_name", "facility_type", "district_id",
            "district_name", "division", "latitude", "longitude",
            "total_beds", "icu_beds", "opened_year"]
    with open(out_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    return out_path
