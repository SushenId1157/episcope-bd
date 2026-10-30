"""
EpiScope - acquisition of S2, district population (ING-1).

Source: HDX Common Operational Dataset for Population Statistics (COD-PS) for
Bangladesh, which republishes Bangladesh Bureau of Statistics census figures
with age and sex disaggregation at admin level 2 (district / zila).

The HDX CKAN API is queried for the dataset's resource list rather than hard-
coding a file URL, so the code keeps working when HDX republishes the file.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re

from pipeline.bronze.fetch_public import http_get, download_to_bronze

HDX_PACKAGE_API = "https://data.humdata.org/api/3/action/package_show?id=cod-ps-bgd"


def discover_resources() -> list[dict]:
    """Return the CKAN resource records for the Bangladesh COD-PS dataset."""
    pkg = json.loads(http_get(HDX_PACKAGE_API, timeout=90).decode())
    if not pkg.get("success"):
        raise RuntimeError("HDX package_show did not succeed")
    return pkg["result"].get("resources", [])


def pick_admin2_csv(resources: list[dict]) -> dict | None:
    """
    Choose the district-level (admin 2) CSV. COD-PS names resources with an
    'adm2' or 'admin2' marker; we prefer CSV over XLSX for simple parsing.
    """
    def score(r):
        name = (r.get("name") or "").lower()
        fmt = (r.get("format") or "").lower()
        s = 0
        if re.search(r"adm(in)?[_\-\s]?2", name):
            s += 10
        if "csv" in fmt:
            s += 5
        if "xlsx" in fmt:
            s += 2
        if any(k in name for k in ("2022", "2023", "2024")):
            s += 3
        if "adm3" in name or "adm1" in name or "adm0" in name:
            s -= 8
        return s

    ranked = sorted(resources, key=score, reverse=True)
    return ranked[0] if ranked and score(ranked[0]) > 0 else None


def _find_column(header: list[str], patterns: list[str]) -> str | None:
    for p in patterns:
        for h in header:
            if re.fullmatch(p, h.strip(), flags=re.I):
                return h
    return None


def parse_population_csv(raw_bytes: bytes) -> tuple[list[dict], dict]:
    """
    Parse a COD-PS admin2 CSV into one row per district.

    COD-PS files commonly carry a HXL tag row directly under the header; it is
    detected and skipped. Column names vary between releases, so the district
    name, pcode and total columns are located by pattern rather than position.
    """
    text = raw_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(c.strip() for c in r)]
    if not rows:
        raise RuntimeError("population CSV is empty")

    header = rows[0]
    body = rows[1:]
    # HXL tag rows start every populated cell with '#'
    if body and sum(1 for c in body[0] if c.strip().startswith("#")) >= 2:
        body = body[1:]

    name_col = _find_column(header, [r"ADM2_EN", r"ADM2_NAME", r"admin2Name_en",
                                     r"district", r"zila", r"ADM2"])
    pcode_col = _find_column(header, [r"ADM2_PCODE", r"admin2Pcode", r"ADM2_CODE"])
    total_col = _find_column(header, [r"T_TL", r"TOTAL", r"total", r"T",
                                      r"population", r"POP"])
    div_col = _find_column(header, [r"ADM1_EN", r"ADM1_NAME", r"admin1Name_en",
                                    r"division"])

    if not name_col:
        raise RuntimeError(f"no district-name column found in: {header}")

    idx = {h: i for i, h in enumerate(header)}

    # Age-sex columns look like T_00_04 / M_05_09 / F_80plus
    agesex_cols = [h for h in header
                   if re.fullmatch(r"[TMF]_\d{2}_\d{2}", h.strip(), flags=re.I)
                   or re.fullmatch(r"[TMF]_\d{2}plus", h.strip(), flags=re.I)]

    def num(v):
        v = (v or "").replace(",", "").strip()
        try:
            return int(float(v))
        except ValueError:
            return None

    out = []
    for r in body:
        if len(r) <= idx[name_col]:
            continue
        name = r[idx[name_col]].strip()
        if not name:
            continue
        rec = {
            "district_name_source": name,
            "adm2_pcode": r[idx[pcode_col]].strip() if pcode_col else None,
            "division_source": r[idx[div_col]].strip() if div_col else None,
            "population_total": num(r[idx[total_col]]) if total_col else None,
        }
        for c in agesex_cols:
            if len(r) > idx[c]:
                rec[c.upper()] = num(r[idx[c]])
        out.append(rec)

    # If no explicit total column, derive it from the T_* age bands
    if out and out[0]["population_total"] is None and agesex_cols:
        t_cols = [c.upper() for c in agesex_cols if c.upper().startswith("T_")]
        for rec in out:
            vals = [rec.get(c) for c in t_cols if rec.get(c) is not None]
            rec["population_total"] = sum(vals) if vals else None

    meta = {"name_col": name_col, "pcode_col": pcode_col,
            "total_col": total_col, "division_col": div_col,
            "agesex_cols": len(agesex_cols), "rows": len(out)}
    return out, meta


def acquire_population(raw_dir: str, force: bool = False) -> dict:
    """Download and parse the COD-PS district population table."""
    os.makedirs(raw_dir, exist_ok=True)
    dest = os.path.join(raw_dir, "S2_population_adm2_raw.csv")

    if os.path.exists(dest) and not force:
        rows, meta = parse_population_csv(open(dest, "rb").read())
        return {"status": "cached", "rows": rows, "parse": meta, "path": dest}

    resources = discover_resources()
    chosen = pick_admin2_csv(resources)
    if chosen is None:
        return {"status": "no_resource", "available": [
            {"name": r.get("name"), "format": r.get("format")} for r in resources]}

    meta_file = download_to_bronze(
        url=chosen["url"], dest_path=dest, source_id="S2",
        description="Bangladesh district population, BBS census via HDX COD-PS",
        licence="HDX COD-PS, Bangladesh Bureau of Statistics (BBS). "
                "See https://data.humdata.org/dataset/cod-ps-bgd",
        overwrite=True)

    rows, parse_meta = parse_population_csv(open(dest, "rb").read())
    return {"status": "downloaded", "resource_name": chosen.get("name"),
            "resource_format": chosen.get("format"), "rows": rows,
            "parse": parse_meta, "ingest_meta": meta_file, "path": dest,
            "available": [{"name": r.get("name"), "format": r.get("format")}
                          for r in resources]}
