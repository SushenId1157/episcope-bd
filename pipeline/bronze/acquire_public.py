"""
EpiScope - acquisition of the real public sources S1 and S3 (ING-1).

Design note: public source data is small (tens of MB) and expensive to refetch,
so it is kept in a durable location, while the derived lake stays on fast local
disk. Everything here is idempotent: already-complete files are verified and
skipped, so the module can be re-run after an interrupted session or on a clean
clone without refetching what is already present.
"""
from __future__ import annotations

import csv
import json
import os
import time
import unicodedata
import urllib.parse
from datetime import datetime, timezone

from pipeline.bronze.fetch_public import http_get, download_to_bronze

GEOBOUNDARIES_API = "https://www.geoboundaries.org/api/current/gbOpen/BGD/ADM2"
OPENMETEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"

START_DATE, END_DATE = "2021-01-01", "2025-12-31"
EXPECTED_DAYS = 1826

DAILY_PRIMARY = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
                 "precipitation_sum", "relative_humidity_2m_mean"]
DAILY_FALLBACK = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
                  "precipitation_sum"]


# ----------------------------------------------------------------- utilities
def normalise_name(s: str) -> str:
    """Lower-case, accent-stripped, alphanumeric-only form used for matching."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return "".join(c for c in s.lower() if c.isalnum())


def _pick(props: dict, keys: list[str]):
    for k in keys:
        if props.get(k):
            return props[k]
    return None


# --------------------------------------------------------------- S1 boundaries
def acquire_boundaries(raw_dir: str, force: bool = False) -> dict:
    """Download Bangladesh ADM2 boundaries via the geoBoundaries API."""
    os.makedirs(raw_dir, exist_ok=True)
    dest = os.path.join(raw_dir, "S1_bgd_adm2_districts.geojson")

    if os.path.exists(dest) and not force:
        try:
            gj = json.load(open(dest))
            if gj.get("type") == "FeatureCollection" and gj.get("features"):
                return {"status": "cached", "features": len(gj["features"]),
                        "path": dest}
        except Exception:
            pass

    api = json.loads(http_get(GEOBOUNDARIES_API, timeout=90).decode())
    if isinstance(api, list):
        api = api[0]

    licence = (f"{api.get('licenseDetail')} | {api.get('licenseSource')} | "
               f"geoBoundaries (Runfola et al. 2020, PLOS ONE 15(4):e0231866)")

    for url in [api.get("simplifiedGeometryGeoJSON"), api.get("gjDownloadURL")]:
        if not url:
            continue
        try:
            meta = download_to_bronze(
                url=url, dest_path=dest, source_id="S1",
                description="Bangladesh district (ADM2) administrative boundaries",
                licence=licence, overwrite=True)
            if open(dest, "rb").read(32).startswith(b"version https://git-lfs"):
                continue                     # LFS pointer, not geometry
            gj = json.load(open(dest))
            if gj.get("type") == "FeatureCollection" and gj.get("features"):
                json.dump(api, open(
                    os.path.join(raw_dir, "S1_geoboundaries_api_record.json"), "w"),
                    indent=2)
                return {"status": "downloaded", "features": len(gj["features"]),
                        "path": dest, "meta": meta,
                        "admUnitCount": api.get("admUnitCount")}
        except Exception:
            continue

    raise RuntimeError("Could not obtain S1 boundaries from any geoBoundaries link")


def build_district_reference(raw_dir: str, divisions: dict,
                             urban: list, hill: list,
                             aliases: dict | None = None) -> dict:
    """
    Derive the canonical district table from S1: stable id, centroid and the
    division/urban/hill attributes the generator and gold partitioning need.
    """
    from shapely.geometry import shape

    gj = json.load(open(os.path.join(raw_dir, "S1_bgd_adm2_districts.geojson")))
    name_keys = ["shapeName", "ADM2_EN", "NAME_2", "district", "name"]
    id_keys = ["shapeID", "ADM2_PCODE", "shapeISO", "GID_2"]

    rows = []
    for feat in gj["features"]:
        geom = shape(feat["geometry"])
        pt = geom.representative_point()      # always inside the polygon
        rows.append({
            "district_name": _pick(feat["properties"], name_keys),
            "source_shape_id": _pick(feat["properties"], id_keys),
            "centroid_lat": round(pt.y, 4),
            "centroid_lon": round(pt.x, 4),
            "area_deg2": round(geom.area, 6),
        })

    rows.sort(key=lambda r: r["district_name"] or "")
    for i, r in enumerate(rows, start=1):
        r["district_id"] = f"D{i:02d}"

    # S1 may use historical spellings; resolve them to the canonical name
    # before any lookup, so one district never appears under two names.
    aliases = aliases or {}
    for r in rows:
        canon = aliases.get(normalise_name(r["district_name"]))
        if canon:
            r["source_district_name"] = r["district_name"]   # keep provenance
            r["district_name"] = canon

    div_lookup = {normalise_name(n): d for d, names in divisions.items()
                  for n in names}
    urban_n = {normalise_name(x) for x in urban}
    hill_n = {normalise_name(x) for x in hill}

    unmatched = []
    for r in rows:
        key = normalise_name(r["district_name"])
        r["division"] = div_lookup.get(key)
        r["is_urban"] = key in urban_n
        r["is_hill"] = key in hill_n
        if r["division"] is None:
            unmatched.append(r["district_name"])

    for r in rows:
        r.setdefault("source_district_name", r["district_name"])

    cols = ["district_id", "district_name", "source_district_name", "division",
            "source_shape_id", "centroid_lat", "centroid_lon", "area_deg2",
            "is_urban", "is_hill"]

    plain = os.path.join(raw_dir, "S1_district_reference.csv")
    with open(plain, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

    if not unmatched:
        enriched = os.path.join(raw_dir, "S1_district_reference_enriched.csv")
        with open(enriched, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader(); w.writerows(rows)

    return {"districts": len(rows), "unmatched": unmatched, "path": plain}


# ------------------------------------------------------------------ S3 weather
def _weather_complete(path: str) -> bool:
    try:
        pay = json.load(open(path))
        d = pay["daily"]
        return (len(d["time"]) >= EXPECTED_DAYS
                and d["time"][0] == START_DATE and d["time"][-1] == END_DATE
                and all(v in d for v in DAILY_FALLBACK))
    except Exception:
        return False


def acquire_weather(raw_dir: str, max_rounds: int = 12,
                    verbose: bool = True) -> dict:
    """
    Fetch the five-year daily series for every district centroid, retrying with
    increasing delay until nothing is missing. Files are written atomically, so
    an interrupted run never leaves a half-written series behind.
    """
    weather_dir = os.path.join(raw_dir, "S3_weather")
    os.makedirs(weather_dir, exist_ok=True)

    districts = list(csv.DictReader(
        open(os.path.join(raw_dir, "S1_district_reference.csv"))))
    by_id = {d["district_id"]: d for d in districts}

    def missing():
        return [d["district_id"] for d in districts
                if not _weather_complete(
                    os.path.join(weather_dir, f"{d['district_id']}.json"))]

    def fetch(d, daily_vars):
        params = {"latitude": d["centroid_lat"], "longitude": d["centroid_lon"],
                  "start_date": START_DATE, "end_date": END_DATE,
                  "daily": ",".join(daily_vars), "timezone": "Asia/Dhaka"}
        url = f"{OPENMETEO_ARCHIVE}?{urllib.parse.urlencode(params)}"
        pay = json.loads(http_get(url, timeout=240, retries=4).decode())
        if "daily" not in pay:
            raise RuntimeError(f"no daily block: {str(pay)[:160]}")
        return pay, url

    delay = 4.0
    for rnd in range(1, max_rounds + 1):
        todo = missing()
        if not todo:
            break
        if verbose:
            print(f"  round {rnd}: {len(todo)} remaining (delay {delay:.1f}s)")
        failed = 0
        for did in todo:
            d = by_id[did]
            try:
                try:
                    pay, url = fetch(d, DAILY_PRIMARY); used = DAILY_PRIMARY
                except Exception:
                    pay, url = fetch(d, DAILY_FALLBACK); used = DAILY_FALLBACK
                pay["_episcope"] = {
                    "source_id": "S3", "district_id": did,
                    "district_name": d["district_name"],
                    "centroid_lat": float(d["centroid_lat"]),
                    "centroid_lon": float(d["centroid_lon"]),
                    "request_url": url, "daily_variables": used,
                    "start_date": START_DATE, "end_date": END_DATE,
                    "licence": "Open-Meteo (CC-BY 4.0); ERA5, Copernicus CDS",
                    "load_timestamp_utc": datetime.now(timezone.utc).isoformat(),
                }
                tmp = os.path.join(weather_dir, f"{did}.json.tmp")
                json.dump(pay, open(tmp, "w"))
                os.replace(tmp, os.path.join(weather_dir, f"{did}.json"))
                time.sleep(delay)
            except Exception as err:
                failed += 1
                if verbose:
                    print(f"    FAIL {did}: {str(err)[:80]}")
                time.sleep(delay * 2)
        if failed:
            delay = min(delay * 1.8, 25.0)
            time.sleep(min(30 + rnd * 20, 120))

    still = missing()
    return {"complete": len(districts) - len(still), "total": len(districts),
            "missing": still, "dir": weather_dir}
