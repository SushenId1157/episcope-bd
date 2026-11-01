"""
EpiScope - fetch the real public sources S1, S2 and S3 (ING-1).

Cached by default. S3 alone is 64 API calls against Open-Meteo, so
re-fetching on every pipeline run would be both slow and impolite to the
provider. Each download records its source URL, licence and SHA-256 checksum
alongside the file.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--public", default="./episcope_public")
    p.add_argument("--force", action="store_true",
                   help="refetch even if the files are already present")
    a = p.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)

    import generator.config as cfg
    import pipeline.bronze.acquire_public as ap
    import pipeline.bronze.acquire_population as apop

    os.makedirs(a.public, exist_ok=True)
    enriched = os.path.join(a.public, "S1_district_reference_enriched.csv")

    if os.path.exists(enriched) and not a.force:
        rows = list(csv.DictReader(open(enriched)))
        weather = os.path.join(a.public, "S3_weather")
        n_weather = len(os.listdir(weather)) if os.path.isdir(weather) else 0
        if len(rows) == 64 and n_weather >= 64:
            print(f"public sources already present: {len(rows)} districts, "
                  f"{n_weather} weather series")
            print("use --force to refetch")
            return 0

    print("S1 administrative boundaries ...")
    r1 = ap.acquire_boundaries(a.public, force=a.force)
    print(f"  {r1['status']}, {r1['features']} features")

    print("district reference ...")
    r2 = ap.build_district_reference(a.public, cfg.DIVISIONS,
                                     cfg.URBAN_DISTRICTS, cfg.HILL_DISTRICTS,
                                     aliases=cfg.DISTRICT_NAME_ALIASES)
    print(f"  {r2['districts']} districts, {len(r2['unmatched'])} unmatched")
    for u in r2["unmatched"]:
        print(f"    UNMATCHED: {u}")

    print("S2 population ...")
    rp_ = apop.acquire_population(a.public, force=a.force)
    print(f"  {rp_['status']}, {len(rp_.get('rows', []))} rows")

    # Attach population to the district reference, matching through the same
    # alias map the boundaries used so a renamed district still resolves.
    if rp_.get("rows"):
        alias = cfg.DISTRICT_NAME_ALIASES

        def canon(n):
            k = ap.normalise_name(n)
            return ap.normalise_name(alias.get(k, n))

        lookup = {canon(r["district_name_source"]): r
                  for r in rp_["rows"] if r.get("population_total")}
        districts = list(csv.DictReader(
            open(os.path.join(a.public, "S1_district_reference.csv"))))
        matched = 0
        for d in districts:
            hit = lookup.get(canon(d["district_name"]))
            if hit:
                d["population_total"] = hit["population_total"]
                d["adm2_pcode"] = hit.get("adm2_pcode")
                matched += 1
        print(f"  population attached to {matched}/64 districts")
        if matched == 64:
            cols = list(districts[0].keys())
            with open(enriched, "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=cols)
                w.writeheader()
                w.writerows(districts)

    print("S3 weather (64 district centroids, 2021-2025) ...")
    r3 = ap.acquire_weather(a.public)
    print(f"  {r3['complete']}/{r3['total']} complete")
    if r3["missing"]:
        print(f"  missing: {r3['missing']}")
        print("  rerun this target; completed districts are skipped")
        return 1

    print("\npublic sources ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
